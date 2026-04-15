/**
 * message.hpp — Message types and JSON serialization for the distributed KV store.
 *
 * All messages are newline-delimited JSON objects with a "type" field.
 * This header provides:
 *   - String constants for each message type
 *   - Builder functions that produce single-line JSON strings (with trailing \n)
 *   - Lightweight parser helpers to extract fields from JSON lines
 *
 * Research additions:
 *   - "version" field added to KV_SET, KV_GET_RESP, REPL_SET for vector clock
 *     propagation. Enables measurement of read-your-writes consistency violations.
 *
 * Design note: We intentionally avoid pulling in a full JSON library.
 * The message format is simple enough that hand-rolled serialization is
 * clearer and has zero external dependencies.
 */

#pragma once

#include <cstdint>
#include <iomanip>
#include <sstream>
#include <string>

namespace msg {

// ─── Message type constants ───────────────────────────────────────────────────

// Client ↔ Node
constexpr const char* KV_SET          = "KV_SET";
constexpr const char* KV_SET_RESP     = "KV_SET_RESP";
constexpr const char* KV_GET          = "KV_GET";
constexpr const char* KV_GET_RESP     = "KV_GET_RESP";

// Node ↔ Node: heartbeat
constexpr const char* HEARTBEAT_PING  = "HEARTBEAT_PING";
constexpr const char* HEARTBEAT_ACK   = "HEARTBEAT_ACK";

// Node ↔ Node: replication
constexpr const char* REPL_SET        = "REPL_SET";
constexpr const char* REPL_SET_ACK    = "REPL_SET_ACK";

// Fault injection control (injector → node)
constexpr const char* FAULT_DELAY     = "FAULT_DELAY";
constexpr const char* FAULT_DELAY_ACK = "FAULT_DELAY_ACK";

// ─── JSON escape ──────────────────────────────────────────────────────────────

inline std::string json_escape(const std::string& s) {
    std::ostringstream out;
    for (char c : s) {
        switch (c) {
            case '"':  out << "\\\""; break;
            case '\\': out << "\\\\"; break;
            case '\n': out << "\\n";  break;
            case '\r': out << "\\r";  break;
            case '\t': out << "\\t";  break;
            default:
                if (static_cast<unsigned char>(c) < 0x20) {
                    out << "\\u" << std::hex << std::setw(4)
                        << std::setfill('0') << static_cast<int>(static_cast<unsigned char>(c))
                        << std::dec;
                } else {
                    out << c;
                }
        }
    }
    return out.str();
}

// ─── Field extraction (lightweight JSON parsing) ──────────────────────────────

// Extract the value of "type":"..." from a JSON line.
inline std::string extract_type(const std::string& line) {
    const std::string key = "\"type\":\"";
    auto i = line.find(key);
    if (i == std::string::npos) return "";
    i += key.size();
    auto j = line.find('"', i);
    if (j == std::string::npos) return "";
    return line.substr(i, j - i);
}

// Extract a string field: "field_name":"value"
inline std::string extract_string(const std::string& line, const std::string& field) {
    std::string key = "\"" + field + "\":\"";
    auto i = line.find(key);
    if (i == std::string::npos) return "";
    i += key.size();
    // Walk to closing quote, handling escaped chars
    std::string val;
    for (size_t k = i; k < line.size() && line[k] != '"'; ++k) {
        if (line[k] == '\\' && k + 1 < line.size()) {
            ++k;
            val += line[k];
        } else {
            val += line[k];
        }
    }
    return val;
}

// Extract an integer field: "field_name":12345
inline int64_t extract_int(const std::string& line, const std::string& field, int64_t def = -1) {
    std::string key = "\"" + field + "\":";
    auto i = line.find(key);
    if (i == std::string::npos) return def;
    i += key.size();
    // Skip whitespace
    while (i < line.size() && (line[i] == ' ' || line[i] == '\t')) ++i;
    if (i >= line.size()) return def;
    bool neg = false;
    if (line[i] == '-') { neg = true; ++i; }
    int64_t v = 0;
    bool found = false;
    while (i < line.size() && line[i] >= '0' && line[i] <= '9') {
        v = v * 10 + (line[i] - '0');
        ++i;
        found = true;
    }
    if (!found) return def;
    return neg ? -v : v;
}

// Extract a boolean field: "field_name":true or "field_name":false
inline bool extract_bool(const std::string& line, const std::string& field, bool def = false) {
    std::string key_true = "\"" + field + "\":true";
    std::string key_false = "\"" + field + "\":false";
    if (line.find(key_true) != std::string::npos) return true;
    if (line.find(key_false) != std::string::npos) return false;
    return def;
}

// ─── Message builders ─────────────────────────────────────────────────────────
// Each returns a single-line JSON string with trailing newline.

// Client → Node: SET request (version is the client's last-written version for this key)
inline std::string make_kv_set(const std::string& key, const std::string& value,
                               const std::string& req_id,
                               const std::string& version_json = "{}") {
    return "{\"type\":\"KV_SET\",\"key\":\"" + json_escape(key) +
           "\",\"value\":\"" + json_escape(value) +
           "\",\"req_id\":\"" + json_escape(req_id) +
           "\",\"version\":" + version_json + "}\n";
}

// Node → Client: SET response
inline std::string make_kv_set_resp(const std::string& key, bool ok,
                                    const std::string& req_id) {
    return "{\"type\":\"KV_SET_RESP\",\"key\":\"" + json_escape(key) +
           "\",\"ok\":" + (ok ? "true" : "false") +
           ",\"req_id\":\"" + json_escape(req_id) + "\"}\n";
}

// Client → Node: GET request
inline std::string make_kv_get(const std::string& key, const std::string& req_id) {
    return "{\"type\":\"KV_GET\",\"key\":\"" + json_escape(key) +
           "\",\"req_id\":\"" + json_escape(req_id) + "\"}\n";
}

// Node → Client: GET response (includes version vector of the returned value)
inline std::string make_kv_get_resp(const std::string& key, bool ok,
                                    const std::string& value,
                                    const std::string& req_id,
                                    const std::string& version_json = "{}") {
    std::string v_part = ok ? ("\"" + json_escape(value) + "\"") : "null";
    return "{\"type\":\"KV_GET_RESP\",\"key\":\"" + json_escape(key) +
           "\",\"value\":" + v_part +
           ",\"ok\":" + (ok ? "true" : "false") +
           ",\"version\":" + version_json +
           ",\"req_id\":\"" + json_escape(req_id) + "\"}\n";
}

// Node → Node: heartbeat ping
inline std::string make_heartbeat_ping(uint64_t seq, int64_t ts_ms,
                                       const std::string& sender_id) {
    return "{\"type\":\"HEARTBEAT_PING\",\"seq\":" + std::to_string(seq) +
           ",\"ts_ms\":" + std::to_string(ts_ms) +
           ",\"sender_id\":\"" + json_escape(sender_id) + "\"}\n";
}

// Node → Node: heartbeat ack
inline std::string make_heartbeat_ack(uint64_t seq, int64_t ts_ms,
                                      const std::string& sender_id) {
    return "{\"type\":\"HEARTBEAT_ACK\",\"seq\":" + std::to_string(seq) +
           ",\"ts_ms\":" + std::to_string(ts_ms) +
           ",\"sender_id\":\"" + json_escape(sender_id) + "\"}\n";
}

// Primary → Secondary: replicate a SET (includes version for causality tracking)
inline std::string make_repl_set(const std::string& key, const std::string& value,
                                 uint64_t seq,
                                 const std::string& version_json = "{}") {
    return "{\"type\":\"REPL_SET\",\"key\":\"" + json_escape(key) +
           "\",\"value\":\"" + json_escape(value) +
           "\",\"seq\":" + std::to_string(seq) +
           ",\"version\":" + version_json + "}\n";
}

// Secondary → Primary: replication ack
inline std::string make_repl_set_ack(uint64_t seq, bool ok) {
    return "{\"type\":\"REPL_SET_ACK\",\"seq\":" + std::to_string(seq) +
           ",\"ok\":" + (ok ? "true" : "false") + "}\n";
}

// Injector → Node: inject artificial delay
inline std::string make_fault_delay(int delay_ms) {
    return "{\"type\":\"FAULT_DELAY\",\"delay_ms\":" + std::to_string(delay_ms) + "}\n";
}

// Node → Injector: delay ack
inline std::string make_fault_delay_ack(bool ok) {
    return "{\"type\":\"FAULT_DELAY_ACK\",\"ok\":" + (ok ? std::string("true") : std::string("false")) + "}\n";
}

} // namespace msg
