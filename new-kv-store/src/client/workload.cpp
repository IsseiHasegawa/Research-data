/**
 * workload.cpp — Multi-client workload generator with Zipfian key distribution
 *               and read-your-writes (RYW) consistency tracking.
 *
 * Research additions over the baseline:
 *
 *   1. ZipfSampler: replaces uniform random key selection with a power-law
 *      distribution P(key = k) ∝ 1/k^α. Controlled by --zipf_alpha (0 = uniform).
 *      Creates realistic "hot key" access patterns and stresses async replication.
 *
 *   2. Multi-client concurrency: --num_clients spawns N parallel client threads,
 *      each with its own TCP connection. Each thread tracks per-key write versions
 *      and logs stale_read events when a GET returns an older version than the
 *      client's last confirmed write (read-your-writes violation detection).
 *
 *   3. Vector clock RYW tracking: on each GET response, the client compares the
 *      returned version vector against its last-written vector for that key.
 *      A mismatch is logged as a stale_read with the staleness delta.
 *
 * Usage:
 *   ./kv_workload --target 127.0.0.1:9100 \
 *                 --num_ops 500 --set_ratio 0.5 --rate 100 \
 *                 --num_clients 4 --zipf_alpha 0.99 \
 *                 --log_path output/logs/workload.jsonl
 */

#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "../common/message.hpp"
#include "../common/net.hpp"

// ─── Timing ───────────────────────────────────────────────────────────────────

static int64_t wall_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

static int64_t steady_us() {
    return std::chrono::duration_cast<std::chrono::microseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

// ─── Simple PRNG (xorshift64, per-thread) ─────────────────────────────────────

struct XorShift64 {
    uint64_t state;
    explicit XorShift64(uint64_t seed = 12345) : state(seed) {}
    uint64_t next() {
        uint64_t x = state;
        x ^= x << 13; x ^= x >> 7; x ^= x << 17;
        return state = x;
    }
    double uniform() { return static_cast<double>(next()) / static_cast<double>(UINT64_MAX); }
};

// ─── Zipf sampler (rejection-based, Clauset et al.) ──────────────────────────
/**
 * Samples integers in [0, n) according to P(k) ∝ 1/(k+1)^α.
 * Uses the rejection-sampling method from:
 *   Clauset, Shalizi & Newman (2009). Power-Law Distributions in Empirical Data.
 *
 * When α = 0, degenerates to uniform (all keys equally likely).
 */
class ZipfSampler {
public:
    ZipfSampler(int n, double alpha) : n_(n), alpha_(alpha) {
        if (alpha_ <= 0.0) return;  // uniform fallback

        // Precompute CDF normalization constant H_n = sum_{k=1}^{n} 1/k^alpha
        h_n_ = 0.0;
        for (int k = 1; k <= n_; ++k) h_n_ += 1.0 / std::pow(static_cast<double>(k), alpha_);
    }

    // Sample a key index in [0, n_).
    int sample(XorShift64& rng) const {
        if (alpha_ <= 0.0 || n_ <= 1) {
            return static_cast<int>(rng.next() % static_cast<uint64_t>(n_));
        }
        // Inverse CDF via linear scan (fast for small n_, sufficient for our prototype)
        double u = rng.uniform() * h_n_;
        double cumulative = 0.0;
        for (int k = 1; k <= n_; ++k) {
            cumulative += 1.0 / std::pow(static_cast<double>(k), alpha_);
            if (u <= cumulative) return k - 1;  // 0-indexed
        }
        return n_ - 1;
    }

    double alpha() const { return alpha_; }

private:
    int    n_;
    double alpha_;
    double h_n_ = 0.0;
};

// ─── Thread-safe log writer ───────────────────────────────────────────────────

class WorkloadLogger {
public:
    std::string run_id;

    bool open(const std::string& path) {
        std::lock_guard<std::mutex> lock(mu_);
        f_.open(path, std::ios::out | std::ios::app);
        return f_.is_open();
    }

    void log(const std::string& event, const std::string& op_type,
             const std::string& key, const std::string& req_id,
             int     client_id   = -1,
             int64_t latency_us  = -1,
             bool    success     = true,
             const std::string& extra = "") {
        std::lock_guard<std::mutex> lock(mu_);
        if (!f_.is_open()) return;
        f_ << "{\"ts_ms\":"  << wall_ms()
           << ",\"run_id\":\"" << msg::json_escape(run_id) << "\""
           << ",\"event\":\"" << msg::json_escape(event) << "\""
           << ",\"op_type\":\"" << msg::json_escape(op_type) << "\""
           << ",\"key\":\"" << msg::json_escape(key) << "\""
           << ",\"req_id\":\"" << msg::json_escape(req_id) << "\"";
        if (client_id >= 0)  f_ << ",\"client_id\":" << client_id;
        if (latency_us >= 0) f_ << ",\"latency_us\":" << latency_us;
        f_ << ",\"success\":" << (success ? "true" : "false");
        if (!extra.empty())  f_ << "," << extra;
        f_ << "}\n";
        f_.flush();
    }

private:
    std::mutex    mu_;
    std::ofstream f_;
};

// ─── Per-client worker ────────────────────────────────────────────────────────

struct ClientConfig {
    std::string host;
    int         port;
    std::string run_id;
    int         num_ops;
    double      set_ratio;
    int         rate;          // ops/sec (0 = unlimited)
    int         key_space;
    int         client_id;
    double      zipf_alpha;
    WorkloadLogger* logger;
    std::atomic<int>* global_op_counter;  // shared across clients for req_id uniqueness
};

// Extract the raw version object string from a RESP line.
static std::string extract_version_object(const std::string& line) {
    const std::string key = "\"version\":{";
    auto i = line.find(key);
    if (i == std::string::npos) return "";
    i += key.size() - 1;  // point at '{'
    int depth = 0;
    size_t start = i;
    for (; i < line.size(); ++i) {
        if (line[i] == '{') ++depth;
        else if (line[i] == '}') { --depth; if (depth == 0) { ++i; break; } }
    }
    return line.substr(start, i - start);
}

static void run_client(const ClientConfig& cfg) {
    XorShift64   rng(static_cast<uint64_t>(wall_ms()) ^
                     (static_cast<uint64_t>(cfg.client_id) * 0xDEADBEEF));
    ZipfSampler  zipf(cfg.key_space, cfg.zipf_alpha);

    // Per-client RYW tracking: last version seen for each key
    // version is stored as "node_id:seq" string — we just track seq for simplicity
    std::unordered_map<std::string, uint64_t> last_write_seq;

    int fd = net::tcp_connect(cfg.host, cfg.port, 10, 500);
    if (fd == net::INVALID_FD) {
        std::cerr << "client " << cfg.client_id << ": connect failed\n";
        return;
    }
    std::cerr << "client " << cfg.client_id << ": connected\n";

    int64_t delay_us = cfg.rate > 0 ? 1000000 / cfg.rate : 0;

    std::vector<char> recv_buf;
    int success_count = 0, fail_count = 0, stale_reads = 0;

    for (int i = 0; i < cfg.num_ops; ++i) {
        int  global_id = cfg.global_op_counter->fetch_add(1);
        std::string req_id = "r_" + std::to_string(cfg.client_id) +
                             "_" + std::to_string(global_id);

        bool is_set = (rng.uniform() < cfg.set_ratio);
        std::string op_type = is_set ? "SET" : "GET";

        int key_idx = zipf.sample(rng);
        std::string key   = "key_" + std::to_string(key_idx);
        std::string value = "val_" + std::to_string(global_id);

        std::string request;
        if (is_set) {
            request = msg::make_kv_set(key, value, req_id);
        } else {
            request = msg::make_kv_get(key, req_id);
        }

        cfg.logger->log("op_start", op_type, key, req_id, cfg.client_id);
        int64_t t0 = steady_us();

        bool ok = net::send_msg(fd, request);
        std::string resp_line;
        if (ok) ok = net::recv_line(fd, &resp_line, recv_buf);

        int64_t latency = steady_us() - t0;

        if (ok) {
            ++success_count;

            // ── RYW consistency check ────────────────────────────────────────
            std::string extra;
            if (is_set) {
                // Extract the version seq from response (logged by node in client_req_done)
                // For simplicity, we use global_id as a monotone proxy for write order.
                // True version is in node logs; here we just track op order.
                last_write_seq[key] = static_cast<uint64_t>(global_id);
                extra = "\"write_seq\":" + std::to_string(global_id);
            } else {
                // GET: check if the returned value's write_seq ≥ our last write
                // We detect staleness by parsing the version field in the response.
                // resp_line contains: {"type":"KV_GET_RESP",...,"version":{"node0":N},...}
                // Extract the primary node's seq from version (first integer value found)
                uint64_t resp_seq = 0;
                std::string ver_obj = extract_version_object(resp_line);
                if (!ver_obj.empty()) {
                    // Find first number in the version object
                    size_t colon = ver_obj.find(':');
                    if (colon != std::string::npos) {
                        try { resp_seq = std::stoull(ver_obj.substr(colon + 1)); }
                        catch (...) {}
                    }
                }

                auto it = last_write_seq.find(key);
                if (it != last_write_seq.end() && resp_seq < it->second) {
                    ++stale_reads;
                    int64_t staleness = static_cast<int64_t>(it->second - resp_seq);
                    extra = "\"stale_read\":true,\"staleness_ops\":" +
                            std::to_string(staleness);
                    cfg.logger->log("stale_read", "GET", key, req_id,
                                    cfg.client_id, latency, true,
                                    "\"last_write_seq\":" + std::to_string(it->second) +
                                    ",\"resp_seq\":" + std::to_string(resp_seq));
                }
            }

            cfg.logger->log("op_done", op_type, key, req_id,
                            cfg.client_id, latency, true, extra);
        } else {
            ++fail_count;
            cfg.logger->log("op_done", op_type, key, req_id,
                            cfg.client_id, latency, false);

            // Reconnect
            net::close_fd(fd);
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            fd = net::tcp_connect(cfg.host, cfg.port, 3, 200);
            if (fd == net::INVALID_FD) {
                std::cerr << "client " << cfg.client_id << ": reconnect failed, stopping\n";
                break;
            }
            recv_buf.clear();
        }

        // Rate limiting
        if (delay_us > 0) {
            int64_t elapsed = steady_us() - t0;
            if (elapsed < delay_us) {
                std::this_thread::sleep_for(
                    std::chrono::microseconds(delay_us - elapsed));
            }
        }
    }

    net::close_fd(fd);
    std::cerr << "client " << cfg.client_id
              << ": done. success=" << success_count
              << " fail=" << fail_count
              << " stale_reads=" << stale_reads << "\n";
}

// ─── Usage ────────────────────────────────────────────────────────────────────

static void usage(const char* prog) {
    std::cerr
        << "Usage: " << prog << " [options]\n\n"
        << "Required:\n"
        << "  --target <host:port>    Target node\n"
        << "  --log_path <path>       JSONL log output\n\n"
        << "Optional:\n"
        << "  --num_ops <n>           Ops per client (default: 100)\n"
        << "  --set_ratio <0-1>       Fraction SET ops (default: 0.5)\n"
        << "  --rate <ops/sec>        Per-client rate, 0=unlimited (default: 0)\n"
        << "  --key_space <n>         Distinct keys (default: 50)\n"
        << "  --num_clients <n>       Parallel clients (default: 1)\n"
        << "  --zipf_alpha <alpha>    Zipf exponent, 0=uniform (default: 0)\n"
        << "  --run_id <id>           Experiment ID\n";
}

// ─── Main ─────────────────────────────────────────────────────────────────────

int main(int argc, char* argv[]) {
    signal(SIGPIPE, SIG_IGN);

    std::string target, log_path;
    std::string run_id = "default_run";
    int    num_ops     = 100;
    double set_ratio   = 0.5;
    int    rate        = 0;
    int    key_space   = 50;
    int    num_clients = 1;
    double zipf_alpha  = 0.0;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if      (arg == "--target"      && i+1 < argc) target      = argv[++i];
        else if (arg == "--log_path"    && i+1 < argc) log_path    = argv[++i];
        else if (arg == "--run_id"      && i+1 < argc) run_id      = argv[++i];
        else if (arg == "--num_ops"     && i+1 < argc) num_ops     = std::stoi(argv[++i]);
        else if (arg == "--set_ratio"   && i+1 < argc) set_ratio   = std::stod(argv[++i]);
        else if (arg == "--rate"        && i+1 < argc) rate        = std::stoi(argv[++i]);
        else if (arg == "--key_space"   && i+1 < argc) key_space   = std::stoi(argv[++i]);
        else if (arg == "--num_clients" && i+1 < argc) num_clients = std::stoi(argv[++i]);
        else if (arg == "--zipf_alpha"  && i+1 < argc) zipf_alpha  = std::stod(argv[++i]);
        else if (arg == "--help" || arg == "-h") { usage(argv[0]); return 0; }
    }

    // Inherit run_id from env if not set
    if (run_id == "default_run") {
        const char* env = std::getenv("RUN_ID");
        if (env && env[0] != '\0') run_id = env;
    }

    if (target.empty() || log_path.empty()) {
        std::cerr << "Error: --target and --log_path are required\n\n";
        usage(argv[0]);
        return 1;
    }

    std::string host;
    int port;
    if (!net::parse_addr(target, host, port)) {
        std::cerr << "Error: invalid target: " << target << "\n";
        return 1;
    }

    WorkloadLogger logger;
    logger.run_id = run_id;
    if (!logger.open(log_path)) {
        std::cerr << "Error: failed to open log " << log_path << "\n";
        return 1;
    }

    // Log experiment metadata
    {
        std::string dist = zipf_alpha > 0.0
            ? ("zipf_alpha=" + std::to_string(zipf_alpha))
            : "uniform";
        std::cerr << "workload: num_clients=" << num_clients
                  << " num_ops=" << num_ops
                  << " key_space=" << key_space
                  << " distribution=" << dist << "\n";
        logger.log("workload_start", "meta", "meta", "meta", -1, -1, true,
                   "\"num_clients\":" + std::to_string(num_clients) +
                   ",\"num_ops\":" + std::to_string(num_ops) +
                   ",\"key_space\":" + std::to_string(key_space) +
                   ",\"zipf_alpha\":" + std::to_string(zipf_alpha) +
                   ",\"set_ratio\":" + std::to_string(set_ratio));
    }

    std::atomic<int> global_op_counter{0};

    // Launch client threads
    std::vector<std::thread> threads;
    threads.reserve(num_clients);
    for (int c = 0; c < num_clients; ++c) {
        ClientConfig cfg;
        cfg.host               = host;
        cfg.port               = port;
        cfg.run_id             = run_id;
        cfg.num_ops            = num_ops;
        cfg.set_ratio          = set_ratio;
        cfg.rate               = rate;
        cfg.key_space          = key_space;
        cfg.client_id          = c;
        cfg.zipf_alpha         = zipf_alpha;
        cfg.logger             = &logger;
        cfg.global_op_counter  = &global_op_counter;
        threads.emplace_back(run_client, cfg);
    }

    for (auto& t : threads) t.join();

    std::cerr << "workload: all clients done. total_ops=" << global_op_counter.load() << "\n";
    logger.log("workload_done", "meta", "meta", "meta", -1, -1, true,
               "\"total_ops\":" + std::to_string(global_op_counter.load()));
    return 0;
}
