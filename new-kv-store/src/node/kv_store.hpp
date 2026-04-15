/**
 * kv_store.hpp — Thread-safe in-memory key-value store with WAL and vector clocks.
 *
 * Research additions over the baseline:
 *
 *   1. Write-Ahead Log (WAL): every SET is durably appended to a JSONL file
 *      before the in-memory write returns. On node restart, call recover_from_wal()
 *      to reconstruct state. This enables precise measurement of confirmed data loss
 *      after a crash by comparing primary vs. secondary WAL files.
 *
 *   2. Vector Clocks: each key stores a version vector {node_id → logical_seq}
 *      alongside its value. Versions are propagated through replication messages
 *      and returned in GET responses, enabling the workload client to detect
 *      read-your-writes (RYW) violations.
 *
 * WAL format (one JSON object per line):
 *   {"wal_seq":0,"ts_ms":..,"node_id":"node0","key":"k","value":"v","version":{"node0":1}}
 */

#pragma once

#include <chrono>
#include <fstream>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "../common/message.hpp"

namespace kvs {

// ── Version vector ────────────────────────────────────────────────────────────
// Maps node_id → logical sequence number. Serialized as a JSON object string.

using VersionVector = std::unordered_map<std::string, uint64_t>;

inline std::string version_to_json(const VersionVector& vv) {
    if (vv.empty()) return "{}";
    std::string s = "{";
    bool first = true;
    for (const auto& [k, v] : vv) {
        if (!first) s += ',';
        s += '"' + msg::json_escape(k) + "\":" + std::to_string(v);
        first = false;
    }
    s += '}';
    return s;
}

// Merge two version vectors (element-wise max).
inline VersionVector version_merge(const VersionVector& a, const VersionVector& b) {
    VersionVector result = a;
    for (const auto& [k, v] : b) {
        auto it = result.find(k);
        if (it == result.end() || it->second < v) result[k] = v;
    }
    return result;
}

// Parse a version vector from a JSON string like {"node0":3,"node1":1}
inline VersionVector version_from_json(const std::string& s) {
    VersionVector vv;
    // Simple hand-rolled parser sufficient for our controlled format
    size_t i = 0;
    while (i < s.size() && s[i] != '{') ++i;
    ++i;  // skip '{'
    while (i < s.size() && s[i] != '}') {
        // Skip whitespace and commas
        while (i < s.size() && (s[i] == ' ' || s[i] == ',')) ++i;
        if (i >= s.size() || s[i] == '}') break;
        // Key (quoted)
        if (s[i] != '"') break;
        ++i;
        std::string key;
        while (i < s.size() && s[i] != '"') { key += s[i++]; }
        ++i;  // closing quote
        // Skip ':'
        while (i < s.size() && s[i] != ':') ++i;
        ++i;
        // Value (integer)
        uint64_t val = 0;
        while (i < s.size() && s[i] >= '0' && s[i] <= '9') {
            val = val * 10 + (s[i++] - '0');
        }
        vv[key] = val;
    }
    return vv;
}

// ── Per-key versioned entry ───────────────────────────────────────────────────

struct KVEntry {
    std::string    value;
    VersionVector  version;   // which node wrote this, at what logical time
};

// ── KVStore ───────────────────────────────────────────────────────────────────

class KVStore {
public:
    KVStore() = default;

    /**
     * Open the WAL file for appending.
     * If path is empty, WAL is disabled.
     * Returns true on success (or if WAL is disabled).
     */
    bool open_wal(const std::string& path, const std::string& node_id) {
        if (path.empty()) return true;
        node_id_ = node_id;
        wal_file_.open(path, std::ios::out | std::ios::app);
        wal_enabled_ = wal_file_.is_open();
        return wal_enabled_;
    }

    /**
     * Recover state from an existing WAL file.
     * Replays all entries in order. Returns the number of entries replayed.
     * Call before open_wal() if you want to replay before further writes.
     */
    int recover_from_wal(const std::string& path) {
        std::ifstream f(path);
        if (!f.is_open()) return 0;

        int count = 0;
        std::string line;
        while (std::getline(f, line)) {
            if (line.empty()) continue;
            std::string key   = msg::extract_string(line, "key");
            std::string value = msg::extract_string(line, "value");
            if (key.empty()) continue;

            // Extract version vector
            std::string ver_str = extract_object(line, "version");
            VersionVector vv = version_from_json(ver_str);

            // Apply directly (no WAL write during recovery)
            std::lock_guard<std::mutex> lock(mu_);
            data_[key] = {value, vv};
            ++count;
        }
        return count;
    }

    /**
     * Set a key-value pair with a version vector.
     * Appends to WAL before in-memory write (crash safety).
     */
    void set(const std::string& key, const std::string& value,
             const VersionVector& version) {
        // WAL write first (durability before visibility)
        if (wal_enabled_) {
            write_wal(key, value, version);
        }
        std::lock_guard<std::mutex> lock(mu_);
        data_[key] = {value, version};
    }

    /**
     * Convenience set with a single-origin version (for primary writes).
     * version = {node_id: ++local_seq}
     */
    VersionVector set_local(const std::string& key, const std::string& value) {
        uint64_t seq;
        {
            std::lock_guard<std::mutex> lock(mu_);
            seq = ++local_seq_;
        }
        VersionVector vv{{node_id_.empty() ? "node" : node_id_, seq}};
        set(key, value, vv);
        return vv;
    }

    /**
     * Get value + version for a key.
     * Returns {entry, true} if found, {{}, false} otherwise.
     */
    std::pair<KVEntry, bool> get(const std::string& key) const {
        std::lock_guard<std::mutex> lock(mu_);
        auto it = data_.find(key);
        if (it != data_.end()) return {it->second, true};
        return {{}, false};
    }

    /** Return the number of stored keys. */
    size_t size() const {
        std::lock_guard<std::mutex> lock(mu_);
        return data_.size();
    }

    /** Return a snapshot of all entries (for WAL comparison / data-loss analysis). */
    std::unordered_map<std::string, KVEntry> snapshot() const {
        std::lock_guard<std::mutex> lock(mu_);
        return data_;
    }

    /** Return all keys. */
    std::vector<std::string> keys() const {
        std::lock_guard<std::mutex> lock(mu_);
        std::vector<std::string> result;
        result.reserve(data_.size());
        for (const auto& kv : data_) result.push_back(kv.first);
        return result;
    }

    /** Number of WAL entries written so far. */
    uint64_t wal_seq() const { return wal_seq_.load(); }

private:
    // Append one WAL entry (called before in-memory write).
    void write_wal(const std::string& key, const std::string& value,
                   const VersionVector& version) {
        uint64_t seq = wal_seq_.fetch_add(1);
        int64_t  ts  = static_cast<int64_t>(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count());

        std::lock_guard<std::mutex> lock(wal_mu_);
        wal_file_ << "{\"wal_seq\":" << seq
                  << ",\"ts_ms\":"   << ts
                  << ",\"node_id\":\"" << msg::json_escape(node_id_) << "\""
                  << ",\"key\":\""   << msg::json_escape(key) << "\""
                  << ",\"value\":\"" << msg::json_escape(value) << "\""
                  << ",\"version\":" << version_to_json(version)
                  << "}\n";
        wal_file_.flush();
    }

    // Extract a JSON object value for a given key.
    // e.g. extract_object(line, "version") from {"version":{"node0":1},...}
    static std::string extract_object(const std::string& line, const std::string& field) {
        std::string key_str = "\"" + field + "\":{";
        auto i = line.find(key_str);
        if (i == std::string::npos) return "{}";
        i += key_str.size() - 1;  // point to '{'
        int depth = 0;
        size_t start = i;
        for (; i < line.size(); ++i) {
            if (line[i] == '{') ++depth;
            else if (line[i] == '}') { --depth; if (depth == 0) { ++i; break; } }
        }
        return line.substr(start, i - start);
    }

    mutable std::mutex mu_;
    std::unordered_map<std::string, KVEntry> data_;
    uint64_t local_seq_ = 0;   // Guarded by mu_
    std::string node_id_;

    // WAL state
    std::mutex     wal_mu_;
    std::ofstream  wal_file_;
    bool           wal_enabled_ = false;
    std::atomic<uint64_t> wal_seq_{0};
};

}  // namespace kvs
