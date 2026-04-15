/**
 * node.hpp — Main node logic for the distributed KV store.
 *
 * Research additions over the baseline:
 *
 *   1. WAL support: NodeConfig.wal_path → KVStore.open_wal()
 *      On startup, recovers state from WAL if the file already exists.
 *
 *   2. Vector clock propagation: every SET response carries the assigned
 *      version vector; GET responses carry the stored version. Replication
 *      messages include the version for secondary-side consistency tracking.
 *
 *   3. Pluggable failure detector: NodeConfig.fd_algo selects between
 *      "fixed" (FailureDetector, heartbeat.hpp) and "phi" (PhiAccrualDetector,
 *      phi_accrual.hpp). Both implement the same on_peer_dead() callback.
 *
 * Architecture (2-node, single machine):
 *
 *   Node 0 (primary):
 *     - Accepts client SET/GET requests
 *     - On SET: increments local_seq, writes to WAL, writes to KVStore,
 *               replicates (with version) to Node 1, responds to client
 *     - Monitors Node 1 via selected FD algorithm
 *
 *   Node 1 (secondary):
 *     - Receives REPL_SET → applies with version → WAL → replies REPL_SET_ACK
 *     - Responds to HEARTBEAT_PING with HEARTBEAT_ACK (phi: timestamps used)
 *     - Can accept client GET (returns version for RYW staleness checks)
 */

#pragma once

#include <atomic>
#include <chrono>
#include <csignal>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "../common/logger.hpp"
#include "../common/message.hpp"
#include "../common/net.hpp"
#include "../failure_detector/heartbeat.hpp"
#include "../failure_detector/phi_accrual.hpp"
#include "../replication/replicator.hpp"
#include "kv_store.hpp"

namespace kvs {

/**
 * Configuration for a Node, parsed from CLI arguments.
 */
struct NodeConfig {
    std::string id;
    int port = 9100;
    std::string peer_addr;
    std::string log_path;
    std::string wal_path;           // NEW: WAL file path (empty = disabled)
    std::string run_id = "default_run";
    int hb_interval_ms = 100;
    int hb_timeout_ms  = 400;
    std::string repl_mode = "none"; // "none", "sync", "async"
    std::string fd_algo   = "fixed";// NEW: "fixed" or "phi"
    double phi_threshold  = 8.0;   // NEW: φ threshold for phi accrual
    int phi_window        = 200;   // NEW: sliding window size for phi
    bool is_primary       = false;
};

class Node {
public:
    explicit Node(const NodeConfig& cfg)
        : cfg_(cfg)
        , running_(false)
        , injected_delay_ms_(0)
    {}

    void run() {
        if (!logger_.open(cfg_.log_path)) {
            std::cerr << cfg_.id << ": failed to open log " << cfg_.log_path << "\n";
            return;
        }
        logger_.set_metadata(cfg_.id, cfg_.run_id);

        // WAL recovery (if WAL file already exists from a previous run)
        int recovered = 0;
        if (!cfg_.wal_path.empty()) {
            recovered = store_.recover_from_wal(cfg_.wal_path);
        }

        // Open WAL for future writes
        if (!cfg_.wal_path.empty()) {
            if (!store_.open_wal(cfg_.wal_path, cfg_.id)) {
                std::cerr << cfg_.id << ": warning: failed to open WAL "
                          << cfg_.wal_path << "\n";
            }
        }

        logger_.log("node_start", "",
                    "{\"port\":"          + std::to_string(cfg_.port) +
                    ",\"repl_mode\":\""   + cfg_.repl_mode + "\"" +
                    ",\"fd_algo\":\""     + cfg_.fd_algo   + "\"" +
                    ",\"is_primary\":"    + (cfg_.is_primary ? "true" : "false") +
                    ",\"hb_interval_ms\":" + std::to_string(cfg_.hb_interval_ms) +
                    ",\"hb_timeout_ms\":" + std::to_string(cfg_.hb_timeout_ms) +
                    ",\"phi_threshold\":" + std::to_string(cfg_.phi_threshold) +
                    ",\"wal_enabled\":"   + (cfg_.wal_path.empty() ? "false" : "true") +
                    ",\"wal_recovered\":" + std::to_string(recovered) + "}");

        if (recovered > 0) {
            logger_.log("wal_recovered", "",
                        "{\"entries\":" + std::to_string(recovered) + "}");
        }

        listen_fd_ = net::tcp_listen(cfg_.port);
        if (listen_fd_ == net::INVALID_FD) {
            std::cerr << cfg_.id << ": failed to bind port " << cfg_.port << "\n";
            return;
        }
        std::cerr << cfg_.id << ": listening on port " << cfg_.port << "\n";
        running_.store(true);

        if (cfg_.is_primary && !cfg_.peer_addr.empty()) {
            start_primary_connections();
        }

        run_accept_loop();
        logger_.log("node_stop");
    }

    void stop() {
        running_.store(false);
        net::close_fd(listen_fd_);
        listen_fd_ = net::INVALID_FD;

        if (fd_fixed_) fd_fixed_->stop();
        if (fd_phi_)   fd_phi_->stop();
        if (replicator_) replicator_->stop();
    }

    static std::atomic<bool>& global_shutdown() {
        static std::atomic<bool> flag{false};
        return flag;
    }

private:
    // ─── Primary initialization ───────────────────────────────────────────────

    void start_primary_connections() {
        std::string host;
        int port;
        if (!net::parse_addr(cfg_.peer_addr, host, port)) {
            std::cerr << cfg_.id << ": invalid peer_addr: " << cfg_.peer_addr << "\n";
            return;
        }

        // Heartbeat connection
        int hb_fd = net::tcp_connect(host, port);
        if (hb_fd == net::INVALID_FD) {
            std::cerr << cfg_.id << ": failed to connect for heartbeat\n";
            return;
        }
        std::cerr << cfg_.id << ": connected to peer " << cfg_.peer_addr
                  << " for heartbeat (fd_algo=" << cfg_.fd_algo << ")\n";

        const std::string peer_id = "peer";

        auto notify_dead = [this]() {
            if (replicator_) replicator_->set_peer_dead();
        };

        if (cfg_.fd_algo == "phi") {
            fd_phi_ = std::make_unique<PhiAccrualDetector>(
                cfg_.id, peer_id,
                cfg_.hb_interval_ms,
                cfg_.phi_threshold,
                cfg_.phi_window,
                logger_);
            fd_phi_->on_peer_dead(notify_dead);
            fd_phi_->start(hb_fd);
        } else {
            // Default: fixed-threshold
            fd_fixed_ = std::make_unique<FailureDetector>(
                cfg_.id, peer_id,
                cfg_.hb_interval_ms, cfg_.hb_timeout_ms,
                logger_);
            fd_fixed_->on_peer_dead(notify_dead);
            fd_fixed_->start(hb_fd);
        }

        // Replication connection (separate TCP socket)
        if (cfg_.repl_mode != "none") {
            replicator_ = std::make_unique<Replicator>(
                cfg_.repl_mode, cfg_.id, peer_id, logger_);
            if (!replicator_->connect(host, port)) {
                std::cerr << cfg_.id << ": failed to connect for replication\n";
            }
        }
    }

    // ─── Accept loop ──────────────────────────────────────────────────────────

    void run_accept_loop() {
        while (running_.load() && !global_shutdown().load()) {
            struct sockaddr_in peer_addr{};
            socklen_t peer_len = sizeof(peer_addr);
            int client_fd = accept(listen_fd_,
                                   reinterpret_cast<struct sockaddr*>(&peer_addr),
                                   &peer_len);
            if (client_fd < 0) {
                if (!running_.load()) break;
                continue;
            }
            int one = 1;
            setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
            std::thread([this, client_fd]() {
                handle_connection(client_fd);
            }).detach();
        }
    }

    // ─── Connection handler ───────────────────────────────────────────────────

    void handle_connection(int fd) {
        std::vector<char> buf;
        std::string line;

        while (running_.load() && net::recv_line(fd, &line, buf)) {
            std::string type = msg::extract_type(line);

            int delay = injected_delay_ms_.load();
            if (delay > 0) {
                std::this_thread::sleep_for(std::chrono::milliseconds(delay));
            }

            if      (type == msg::KV_SET)         handle_kv_set(fd, line);
            else if (type == msg::KV_GET)         handle_kv_get(fd, line);
            else if (type == msg::HEARTBEAT_PING) handle_heartbeat_ping(fd, line);
            else if (type == msg::REPL_SET)       handle_repl_set(fd, line);
            else if (type == msg::FAULT_DELAY)    handle_fault_delay(fd, line);
        }

        net::close_fd(fd);
    }

    // ─── KV handlers ─────────────────────────────────────────────────────────

    void handle_kv_set(int fd, const std::string& line) {
        std::string key    = msg::extract_string(line, "key");
        std::string value  = msg::extract_string(line, "value");
        std::string req_id = msg::extract_string(line, "req_id");

        logger_.log("client_req_recv", "",
                    "{\"op\":\"SET\",\"key\":\"" + msg::json_escape(key) +
                    "\",\"req_id\":\"" + msg::json_escape(req_id) + "\"}");

        // Primary: assign a new version, write to WAL + store
        VersionVector version;
        if (cfg_.is_primary) {
            version = store_.set_local(key, value);
        } else {
            // Secondary accepts direct SET without version bump
            store_.set(key, value, {});
        }

        bool repl_ok = true;
        if (cfg_.is_primary && replicator_) {
            repl_ok = replicator_->replicate(key, value, version_to_json(version));
        }

        std::string resp = msg::make_kv_set_resp(key, true, req_id);
        net::send_msg(fd, resp);

        logger_.log("client_req_done", "",
                    "{\"op\":\"SET\",\"key\":\"" + msg::json_escape(key) +
                    "\",\"req_id\":\"" + msg::json_escape(req_id) +
                    "\",\"version\":" + version_to_json(version) +
                    ",\"repl_ok\":" + (repl_ok ? "true" : "false") + "}");
    }

    void handle_kv_get(int fd, const std::string& line) {
        std::string key    = msg::extract_string(line, "key");
        std::string req_id = msg::extract_string(line, "req_id");

        logger_.log("client_req_recv", "",
                    "{\"op\":\"GET\",\"key\":\"" + msg::json_escape(key) +
                    "\",\"req_id\":\"" + msg::json_escape(req_id) + "\"}");

        auto [entry, found] = store_.get(key);
        std::string ver_json = found ? version_to_json(entry.version) : "{}";
        std::string resp = msg::make_kv_get_resp(
            key, found, found ? entry.value : "", req_id, ver_json);
        net::send_msg(fd, resp);

        logger_.log("client_req_done", "",
                    "{\"op\":\"GET\",\"key\":\"" + msg::json_escape(key) +
                    "\",\"req_id\":\"" + msg::json_escape(req_id) +
                    "\",\"found\":" + (found ? "true" : "false") +
                    ",\"version\":" + ver_json + "}");
    }

    // ─── Peer message handlers ────────────────────────────────────────────────

    void handle_heartbeat_ping(int fd, const std::string& line) {
        uint64_t seq     = static_cast<uint64_t>(msg::extract_int(line, "seq", 0));
        int64_t  ts      = wall_ms();
        std::string ack  = msg::make_heartbeat_ack(seq, ts, cfg_.id);
        net::send_msg(fd, ack);
    }

    void handle_repl_set(int fd, const std::string& line) {
        std::string key   = msg::extract_string(line, "key");
        std::string value = msg::extract_string(line, "value");
        uint64_t    seq   = static_cast<uint64_t>(msg::extract_int(line, "seq", 0));

        // Extract version vector from replication message
        std::string ver_field = extract_object_field(line, "version");
        VersionVector version = version_from_json(ver_field.empty() ? "{}" : ver_field);

        logger_.log("repl_recv", "",
                    "{\"key\":\"" + msg::json_escape(key) +
                    "\",\"seq\":"  + std::to_string(seq) +
                    ",\"version\":" + version_to_json(version) + "}");

        store_.set(key, value, version);

        std::string ack = msg::make_repl_set_ack(seq, true);
        net::send_msg(fd, ack);

        logger_.log("repl_ack_sent", "",
                    "{\"key\":\"" + msg::json_escape(key) +
                    "\",\"seq\":"  + std::to_string(seq) + "}");
    }

    // ─── Fault injection handler ──────────────────────────────────────────────

    void handle_fault_delay(int fd, const std::string& line) {
        int delay = static_cast<int>(msg::extract_int(line, "delay_ms", 0));
        injected_delay_ms_.store(delay);
        logger_.log("fault_injected", "",
                    "{\"type\":\"delay\",\"delay_ms\":" + std::to_string(delay) + "}");
        std::string ack = msg::make_fault_delay_ack(true);
        net::send_msg(fd, ack);
    }

    // Extract a JSON object-valued field as a string, e.g. "version":{...}
    static std::string extract_object_field(const std::string& line,
                                            const std::string& field) {
        std::string key_str = "\"" + field + "\":{";
        auto i = line.find(key_str);
        if (i == std::string::npos) return "";
        i += key_str.size() - 1;  // point to '{'
        int depth = 0;
        size_t start = i;
        for (; i < line.size(); ++i) {
            if (line[i] == '{') ++depth;
            else if (line[i] == '}') { --depth; if (depth == 0) { ++i; break; } }
        }
        return line.substr(start, i - start);
    }

    // ─── Member variables ─────────────────────────────────────────────────────

    NodeConfig   cfg_;
    Logger       logger_;
    KVStore      store_;

    int listen_fd_ = net::INVALID_FD;
    std::atomic<bool> running_;
    std::atomic<int>  injected_delay_ms_;

    // Failure detector (exactly one is active)
    std::unique_ptr<FailureDetector>    fd_fixed_;
    std::unique_ptr<PhiAccrualDetector> fd_phi_;

    std::unique_ptr<Replicator> replicator_;
};

}  // namespace kvs
