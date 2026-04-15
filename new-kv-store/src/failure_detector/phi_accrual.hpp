/**
 * phi_accrual.hpp — Phi Accrual Failure Detector (Hayashibara et al., 2004).
 *
 * Overview
 * --------
 * Instead of comparing a fixed timeout against the last-ack time, the Phi
 * Accrual detector models the distribution of heartbeat inter-arrival times
 * as a Gaussian and computes a continuous suspicion level φ(t):
 *
 *   φ(t) = -log₁₀( P_late(t) )
 *
 * where P_late(t) is the probability that the *next* heartbeat arrives later
 * than t milliseconds after the last observed arrival, under the model.
 *
 * When φ(t) exceeds a configuration threshold (typically 8–16), the detector
 * declares the peer dead. Higher thresholds → fewer false positives but
 * slower detection.
 *
 * Research value
 * -------------
 * Compared to the fixed-threshold detector in heartbeat.hpp, this detector
 * self-adapts to the measured network/OS-scheduling jitter on the loopback
 * interface. The φ value is logged with every heartbeat ACK, enabling
 * post-hoc analysis of how suspicion evolves through transient slowdowns vs.
 * actual failures.
 *
 * Implementation notes
 * --------------------
 * - Uses a fixed-size circular window of inter-arrival samples (default 200).
 * - Falls back to fixed-threshold detection until MIN_SAMPLES have been
 *   collected (the Gaussian model is unreliable with too few points).
 * - Uses the same thread model as FailureDetector: sender, receiver, checker.
 * - Provides the same on_peer_dead() callback interface for drop-in use.
 *
 * Reference
 * ---------
 * Hayashibara, N., Défago, X., Yared, R., & Katayama, T. (2004).
 * The φ accrual failure detector. SRDS 2004.
 */

#pragma once

#include <atomic>
#include <chrono>
#include <cmath>
#include <deque>
#include <functional>
#include <limits>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "../common/logger.hpp"
#include "../common/message.hpp"
#include "../common/net.hpp"

namespace kvs {

class PhiAccrualDetector {
public:
    /**
     * @param my_id           This node's ID
     * @param peer_id         Peer node ID
     * @param hb_interval_ms  Heartbeat send interval (ms)
     * @param phi_threshold   φ value above which peer is declared dead (default 8.0)
     * @param window_size     Number of inter-arrival samples to keep (default 200)
     * @param logger          Logger reference
     */
    PhiAccrualDetector(const std::string& my_id,
                       const std::string& peer_id,
                       int   hb_interval_ms,
                       double phi_threshold,
                       int   window_size,
                       Logger& logger)
        : my_id_(my_id)
        , peer_id_(peer_id)
        , hb_interval_ms_(hb_interval_ms)
        , phi_threshold_(phi_threshold)
        , window_size_(window_size)
        , logger_(logger)
        , running_(false)
        , dead_declared_(false)
        , last_arrival_mono_(0)
    {}

    ~PhiAccrualDetector() { stop(); }

    /** Start sender, receiver and checker threads on the given socket. */
    void start(int peer_fd) {
        peer_fd_ = peer_fd;
        running_.store(true);

        int64_t now = monotonic_ms();
        last_arrival_mono_.store(now);
        start_wall_ = wall_ms();

        sender_thread_   = std::thread([this] { run_sender(); });
        receiver_thread_ = std::thread([this] { run_receiver(); });
        checker_thread_  = std::thread([this] { run_checker(); });
    }

    void stop() {
        running_.store(false);
        if (sender_thread_.joinable())   sender_thread_.join();
        if (receiver_thread_.joinable()) receiver_thread_.join();
        if (checker_thread_.joinable())  checker_thread_.join();
    }

    bool is_peer_dead() const { return dead_declared_.load(); }

    void on_peer_dead(std::function<void()> cb) { on_dead_cb_ = std::move(cb); }

private:
    // ── Sender thread ─────────────────────────────────────────────────────────

    void run_sender() {
        uint64_t seq = 0;
        while (running_.load() && !dead_declared_.load()) {
            int64_t ts  = wall_ms();
            std::string ping = msg::make_heartbeat_ping(seq++, ts, my_id_);
            if (!net::send_msg(peer_fd_, ping)) break;
            logger_.log("hb_ping_sent", peer_id_);

            for (int e = 0;
                 e < hb_interval_ms_ && running_.load() && !dead_declared_.load();
                 e += 10) {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
        }
    }

    // ── Receiver thread ───────────────────────────────────────────────────────

    void run_receiver() {
        net::set_nonblocking(peer_fd_, true);
        std::vector<char> buf;
        std::string line;
        int wait_ms = hb_interval_ms_ * 4 + 50;

        while (running_.load()) {
            if (!net::recv_line(peer_fd_, &line, buf, true, wait_ms)) break;
            if (dead_declared_.load()) continue;

            if (msg::extract_type(line) == msg::HEARTBEAT_ACK) {
                int64_t now = monotonic_ms();
                record_arrival(now);
                logger_.log("hb_ack_recv", peer_id_);
            }
        }
    }

    // Called on each heartbeat ACK arrival.
    void record_arrival(int64_t now_mono) {
        std::lock_guard<std::mutex> lock(samples_mu_);
        int64_t last = last_arrival_mono_.exchange(now_mono);
        if (last > 0) {
            double interval = static_cast<double>(now_mono - last);
            if (interval > 0.0) {
                arrivals_.push_back(interval);
                if (static_cast<int>(arrivals_.size()) > window_size_) {
                    arrivals_.pop_front();
                }
            }
        }
    }

    // ── Checker thread ────────────────────────────────────────────────────────

    void run_checker() {
        const int check_ms   = 10;
        const int64_t grace  = 2000;  // ms: warm-up before checking

        while (running_.load() && !dead_declared_.load()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(check_ms));

            int64_t now_mono = monotonic_ms();
            int64_t now_wall = wall_ms();

            if (now_wall - start_wall_ < grace) continue;

            int64_t last = last_arrival_mono_.load();
            double  t    = static_cast<double>(now_mono - last);  // silence duration (ms)

            double phi = compute_phi(t);

            // Log φ periodically (every ~500ms) for post-hoc analysis
            if ((now_wall % 500) < check_ms) {
                logger_.log("phi_value", peer_id_,
                            "{\"phi\":" + format_double(phi, 3) +
                            ",\"silence_ms\":" + std::to_string(static_cast<int>(t)) +
                            ",\"samples\":" + std::to_string(sample_count()) + "}");
            }

            if (phi >= phi_threshold_) {
                dead_declared_.store(true);
                logger_.log("declared_dead", peer_id_,
                            "{\"fd_algo\":\"phi_accrual\",\"phi\":" +
                            format_double(phi, 3) +
                            ",\"phi_threshold\":" + format_double(phi_threshold_, 1) + "}");
                if (on_dead_cb_) on_dead_cb_();
                break;
            }
        }
    }

    // ── φ computation (Gaussian model) ────────────────────────────────────────

    /**
     * Compute φ(t) = -log₁₀(P_late(t)) under a Gaussian model of
     * inter-arrival times.
     *
     * P_late(t) = 1 - Φ_cdf((t - μ) / σ)
     *
     * where Φ_cdf is the standard normal CDF, μ and σ² are the sample
     * mean and variance of observed inter-arrival times.
     *
     * Before MIN_SAMPLES are collected, falls back to:
     *   φ(t) = t / (hb_interval_ms * phi_threshold) * phi_threshold
     * which is equivalent to a scaled fixed-threshold approach.
     */
    double compute_phi(double t) const {
        std::lock_guard<std::mutex> lock(samples_mu_);

        if (static_cast<int>(arrivals_.size()) < MIN_SAMPLES) {
            // Fallback: linear ramp from 0 to phi_threshold over 3× interval
            double ratio = t / (static_cast<double>(hb_interval_ms_) * 3.0);
            return phi_threshold_ * std::min(ratio, 2.0);
        }

        // Compute mean and variance
        double sum = 0.0, sum2 = 0.0;
        for (double x : arrivals_) { sum += x; sum2 += x * x; }
        double n  = static_cast<double>(arrivals_.size());
        double mu = sum / n;
        double var = std::max(sum2 / n - mu * mu, 1.0);  // clamp variance ≥ 1
        double sigma = std::sqrt(var);

        // Standard normal CDF via erfc approximation
        double z   = (t - mu) / (sigma * std::sqrt(2.0));
        double p_late = 0.5 * std::erfc(z);  // P(X > t) under Gaussian

        if (p_late < 1e-300) p_late = 1e-300;  // avoid log(0)
        return -std::log10(p_late);
    }

    int sample_count() const {
        std::lock_guard<std::mutex> lock(samples_mu_);
        return static_cast<int>(arrivals_.size());
    }

    static std::string format_double(double v, int precision) {
        char buf[64];
        snprintf(buf, sizeof(buf), "%.*f", precision, v);
        return buf;
    }

    // ── Member data ───────────────────────────────────────────────────────────

    static constexpr int MIN_SAMPLES = 30;  // Minimum before Gaussian model activates

    std::string my_id_;
    std::string peer_id_;
    int         hb_interval_ms_;
    double      phi_threshold_;
    int         window_size_;
    Logger&     logger_;

    int  peer_fd_ = net::INVALID_FD;
    std::atomic<bool>    running_;
    std::atomic<bool>    dead_declared_;
    std::atomic<int64_t> last_arrival_mono_;
    int64_t              start_wall_ = 0;

    mutable std::mutex  samples_mu_;
    std::deque<double>  arrivals_;  // inter-arrival times in ms

    std::thread sender_thread_;
    std::thread receiver_thread_;
    std::thread checker_thread_;

    std::function<void()> on_dead_cb_;
};

}  // namespace kvs
