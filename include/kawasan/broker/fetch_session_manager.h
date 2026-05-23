#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <unordered_map>

namespace kawasan::broker {

// Phase 1.4: KIP-227 FetchSessionManager (basic).
//
// A "fetch session" lets a consumer maintain a long-running fetch
// subscription with the broker. Instead of resending the full
// (topic, partition, fetch_offset) tuple every poll, the consumer
// references a session_id and the broker remembers the partitions it
// last saw. Incremental fetches then only need to mention partitions
// whose state changed.
//
// This minimal implementation supports the wire-level handshake:
//   - session_id == 0 in request → allocate a new session
//   - session_id != 0 → look up; validate session_epoch
//   - epoch on response = epoch in request
//   - INVALID_FETCH_SESSION_ID / INVALID_FETCH_SESSION_EPOCH on mismatch
//
// We do NOT yet implement incremental delivery — every fetch returns a
// "full" payload. The session_id round-trip means clients don't get
// stuck retrying with session_id=0; they get a stable session that
// they can use for their lifetime.
class FetchSessionManager {
public:
    /// @brief Per-partition tracked offset in a session.
    struct SessionPartition {
        std::string topic;
        int32_t partition;
        int64_t last_offset = -1;
    };

    struct Session {
        int32_t session_id;
        int32_t session_epoch;
        std::chrono::steady_clock::time_point last_used;
        std::set<std::pair<std::string, int32_t>> partitions;
        // Phase 1.4: per-partition last-seen fetch offset (the offset
        // the broker last returned to this session). Used to decide
        // whether a partition has fresh data on a subsequent fetch.
        std::unordered_map<std::string, int64_t> last_seen_offset;
    };

    /// @brief Result of validating an incoming Fetch session reference.
    struct ValidationResult {
        int32_t session_id;       // What the response should echo.
        int32_t session_epoch;    // What the response should echo.
        bool is_new_session;      // True if we just allocated this.
        bool is_full_fetch;       // True → broker should send full payload
        bool is_error;            // True → caller emits the error code in `error`
        int16_t error;            // 70 = INVALID_FETCH_SESSION_ID, 71 = INVALID_FETCH_SESSION_EPOCH
    };

    /// @brief Validates a Fetch request's (session_id, session_epoch).
    ValidationResult validate(int32_t requested_session_id,
                              int32_t requested_session_epoch);

    /// @brief Updates a session's tracked partition list after a fetch.
    void recordFetch(int32_t session_id,
                     const std::set<std::pair<std::string, int32_t>>& partitions);

    /// @brief Phase 1.4: records the last offset the broker served for a
    /// (session, topic, partition). Used to decide whether subsequent
    /// fetches should include the partition or omit it (incremental).
    void recordPartitionOffset(int32_t session_id, const std::string& topic,
                               int32_t partition, int64_t last_offset);

    /// @brief Returns the last-served offset for a (session, topic,
    /// partition), or -1 if not tracked. Used by the Fetch handler to
    /// decide whether to include the partition in the response.
    int64_t lastSeenOffset(int32_t session_id, const std::string& topic,
                           int32_t partition) const;

    /// @brief Removes sessions idle for more than `max_idle_ms`.
    size_t evictIdle(int64_t max_idle_ms);

    /// @brief Returns the active session count.
    size_t size() const;

    /// @brief Phase EX-1 (§6.3): Prometheus metrics snapshot.
    struct Metrics {
        int64_t session_count;              // gauge
        int64_t evictions_total;            // counter
        double incremental_hit_ratio;       // gauge in [0, 1]
        int64_t incremental_hits_total;     // counter
        int64_t incremental_misses_total;   // counter
    };
    Metrics getMetrics() const;

    /// @brief Hook called by the Fetch handler to track incremental
    /// vs. full-fetch outcomes for the hit-ratio metric.
    void recordHitOrMiss(bool hit);

private:
    mutable std::mutex mutex_;
    std::unordered_map<int32_t, Session> sessions_;
    std::atomic<int32_t> next_session_id_{1};
    // Phase EX-1 metrics.
    std::atomic<int64_t> evictions_total_{0};
    std::atomic<int64_t> incremental_hits_total_{0};
    std::atomic<int64_t> incremental_misses_total_{0};
};

}  // namespace kawasan::broker
