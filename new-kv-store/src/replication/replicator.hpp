/**
 * replicator.hpp — Replication logic for the distributed KV store.
 *
 * Supports three modes (configurable per experiment):
 *
 *   "none"  — replicate() is a no-op; writes only affect the local node.
 *   "sync"  — replicate() sends REPL_SET to secondary and blocks until
 *             REPL_SET_ACK is received. Write latency includes replication.
 *   "async" — replicate() enqueues the replication message on a background
 *             thread and returns immediately. Primary responds before the
 *             replica is updated.
 *
 * Note on async replication and data loss:
 *   In async mode, the primary acknowledges the write before the secondary
 *   has confirmed it. If the primary crashes after acknowledging but before
 *   the background replication completes, the write is lost on the secondary.
 *   This is intentional and is one of the trade-offs we want to measure.
 */

#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <vector>

#include "../common/logger.hpp"
#include "../common/message.hpp"
#include "../common/net.hpp"

namespace kvs {

class Replicator {
public:
    /**
     * @param mode       "none", "sync", or "async"
     * @param my_id      This node's ID
     * @param peer_id    The secondary node's ID
     * @param logger     Logger reference for event logging
     */
    Replicator(const std::string& mode,
               const std::string& my_id,
               const std::string& peer_id,
               Logger& logger)
        : mode_(mode)
        , my_id_(my_id)
        , peer_id_(peer_id)
        , logger_(logger)
        , peer_fd_(net::INVALID_FD)
        , running_(false)
        , seq_(0)
        , peer_alive_(true)
    {}

    ~Replicator() {
        stop();
    }

    /**
     * Connect to the secondary node for replication.
     * Must be called before replicate() in sync/async modes.
     * Returns true if connected (or mode is "none").
     */
    bool connect(const std::string& host, int port) {
        if (mode_ == "none") return true;

        peer_fd_ = net::tcp_connect(host, port);
        if (peer_fd_ == net::INVALID_FD) {
            logger_.log("repl_connect_fail", peer_id_);
            return false;
        }
        logger_.log("repl_connected", peer_id_);

        if (mode_ == "async") {
            // Start background sender thread
            running_.store(true);
            async_thread_ = std::thread([this]() { run_async_sender(); });
        }

        return true;
    }

    /**
     * Replicate a SET operation to the secondary.
     *
     * - "none":  returns true immediately.
     * - "sync":  sends REPL_SET (with version), waits for REPL_SET_ACK.
     * - "async": enqueues and returns true immediately.
     *
     * @param version_json  Serialized version vector, e.g. {"node0":5}
     */
    bool replicate(const std::string& key, const std::string& value,
                   const std::string& version_json = "{}") {
        if (mode_ == "none") return true;
        if (!peer_alive_.load()) {
            logger_.log("repl_skipped", peer_id_,
                        "{\"reason\":\"peer_dead\",\"key\":\"" +
                        msg::json_escape(key) + "\"}");
            return false;
        }

        uint64_t s = seq_.fetch_add(1);

        if (mode_ == "sync")  return replicate_sync(key, value, s, version_json);
        if (mode_ == "async") return replicate_async(key, value, s, version_json);
        return false;
    }

    // Notify the replicator that the peer is dead (from failure detector).
    void set_peer_dead() {
        peer_alive_.store(false);
    }

    // Stop the replicator (join async thread if running).
    void stop() {
        running_.store(false);
        cv_.notify_all();
        if (async_thread_.joinable()) async_thread_.join();
    }

    // Get the replication mode string.
    const std::string& mode() const { return mode_; }

private:
    // Synchronous replication: send with version vector and wait for ack.
    bool replicate_sync(const std::string& key, const std::string& value,
                        uint64_t s, const std::string& version_json) {
        std::string m = msg::make_repl_set(key, value, s, version_json);
        logger_.log("repl_start", peer_id_,
                    "{\"key\":\"" + msg::json_escape(key) +
                    "\",\"seq\":" + std::to_string(s) +
                    ",\"mode\":\"sync\",\"version\":" + version_json + "}");

        std::lock_guard<std::mutex> lock(io_mu_);
        if (!net::send_msg(peer_fd_, m)) {
            logger_.log("repl_send_fail", peer_id_);
            return false;
        }

        // Wait for ack (blocking read with timeout)
        std::vector<char> buf;
        std::string line;
        // Use a short select timeout and retry a few times
        for (int attempt = 0; attempt < 50; ++attempt) {
            if (net::wait_readable(peer_fd_, 100)) {
                char tmp[512];
                ssize_t n = recv(peer_fd_, tmp, sizeof(tmp), 0);
                if (n > 0) {
                    buf.insert(buf.end(), tmp, tmp + n);
                    auto it = std::find(buf.begin(), buf.end(), '\n');
                    if (it != buf.end()) {
                        line.assign(buf.begin(), it);
                        buf.erase(buf.begin(), it + 1);
                        if (msg::extract_type(line) == msg::REPL_SET_ACK) {
                            logger_.log("repl_ack", peer_id_,
                                        "{\"seq\":" + std::to_string(s) +
                                        ",\"mode\":\"sync\"}");
                            return true;
                        }
                    }
                } else if (n == 0) {
                    // EOF
                    break;
                }
            }
        }
        logger_.log("repl_ack_timeout", peer_id_);
        return false;
    }

    // Asynchronous replication: enqueue for background sending.
    bool replicate_async(const std::string& key, const std::string& value,
                         uint64_t s, const std::string& version_json) {
        logger_.log("repl_start", peer_id_,
                    "{\"key\":\"" + msg::json_escape(key) +
                    "\",\"seq\":" + std::to_string(s) +
                    ",\"mode\":\"async\",\"version\":" + version_json + "}");
        {
            std::lock_guard<std::mutex> lock(queue_mu_);
            queue_.push({key, value, s, version_json});
        }
        cv_.notify_one();
        return true;
    }

    // Background thread for async replication.
    void run_async_sender() {
        while (running_.load() || !queue_.empty()) {
            ReplItem item;
            {
                std::unique_lock<std::mutex> lock(queue_mu_);
                cv_.wait_for(lock, std::chrono::milliseconds(50),
                             [this]() { return !queue_.empty() || !running_.load(); });
                if (queue_.empty()) continue;
                item = queue_.front();
                queue_.pop();
            }

            if (!peer_alive_.load()) {
                logger_.log("repl_skipped", peer_id_,
                            "{\"reason\":\"peer_dead\",\"key\":\"" +
                            msg::json_escape(item.key) + "\"}");
                continue;
            }

            std::string m = msg::make_repl_set(item.key, item.value, item.seq,
                                                item.version_json);
            {
                std::lock_guard<std::mutex> lock(io_mu_);
                if (!net::send_msg(peer_fd_, m)) {
                    logger_.log("repl_send_fail", peer_id_);
                    continue;
                }
            }
            logger_.log("repl_sent_async", peer_id_,
                        "{\"key\":\"" + msg::json_escape(item.key) +
                        "\",\"seq\":" + std::to_string(item.seq) +
                        ",\"version\":" + item.version_json + "}");
        }
    }

    struct ReplItem {
        std::string key;
        std::string value;
        uint64_t    seq;
        std::string version_json = "{}";
    };

    std::string mode_;
    std::string my_id_;
    std::string peer_id_;
    Logger& logger_;

    int peer_fd_;
    std::atomic<bool> running_;
    std::atomic<uint64_t> seq_;
    std::atomic<bool> peer_alive_;

    // I/O mutex for sync replication (protects peer_fd_ read/write)
    std::mutex io_mu_;

    // Async replication queue
    std::mutex queue_mu_;
    std::condition_variable cv_;
    std::queue<ReplItem> queue_;
    std::thread async_thread_;
};

} // namespace kvs
