#pragma once

#include <atomic>
#include <cstdint>
#include <deque>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "kawasan/common/types.h"

namespace kawasan::broker {

// Phase EX-10: read_committed isolation tracker.
//
// Tracks per-(topic, partition):
//   - in-flight transactional producers (producer_id → first_offset)
//   - aborted-transactions ring buffer (producer_id, first_offset)
//   - last-stable-offset (LSO): min(first_offset over in-flight) or HWM
//
// Hooks called by the request handlers:
//
//   handleAddPartitionsToTxn → recordInFlightTxn(pid, topic, part, log_end)
//      Records that a new transaction is starting on this partition.
//      first_offset is captured *before* the first batch of this txn is
//      appended, which means LSO will correctly hold consumers behind
//      the in-flight transactional records.
//
//   handleEndTxn(committed=true)  → commitInFlightTxns(transactional_id)
//      For each partition the txn touched, remove from in-flight and
//      bump LSO past the committed boundary.
//
//   handleEndTxn(committed=false) → abortInFlightTxns(transactional_id)
//      For each partition, record (pid, first_offset) in the aborted
//      ring buffer so read_committed consumers can filter the bad
//      records on their next Fetch.
//
// Two-stage LSO cache: the cached `last_stable_offset_` per partition
// is invalidated on every in-flight change and recomputed lazily on
// next query. This keeps the Fetch hot path cheap (read-only of an
// atomic) and only does the O(k) re-scan on transactional state
// transitions which are rare relative to Fetch.
//
// The aborted-transactions ring is bounded (default 1000 entries per
// partition); on overflow the oldest entries are evicted. A real
// implementation would persist these to `__transaction_state` and
// rebuild on restart. Our single-broker scaffolding accepts the
// memory bound — restart loses the abort history but no committed
// records, which is the same posture as our group-coordinator
// in-memory state.
class IsolationTracker {
public:
    struct AbortedTxn {
        int64_t producer_id;
        Offset first_offset;
    };

    /// @brief Records that a transactional producer is now in-flight on
    /// `(topic, partition)` with its first record at offset
    /// `current_log_end`. Idempotent — repeat calls for the same
    /// (pid, topic, partition) are no-ops (the first_offset doesn't
    /// move within a single transaction).
    void recordInFlightTxn(int64_t producer_id, const std::string& topic,
                           PartitionId partition, Offset current_log_end);

    /// @brief Commits all in-flight txns for `transactional_id` across
    /// every partition. The caller has already advanced the partition's
    /// log to commit-record offsets; we just need to release the LSO
    /// hold.
    void commitInFlightTxns(
        const std::vector<std::pair<std::string, int32_t>>& partitions,
        int64_t producer_id);

    /// @brief Aborts all in-flight txns. For each partition, the txn's
    /// first_offset is moved to the aborted-transactions ring so
    /// read_committed consumers skip those records.
    void abortInFlightTxns(
        const std::vector<std::pair<std::string, int32_t>>& partitions,
        int64_t producer_id);

    /// @brief Returns the LSO for a partition. If no in-flight txns,
    /// returns the high_watermark argument unchanged.
    Offset lastStableOffset(const std::string& topic, PartitionId partition,
                            Offset high_watermark) const;

    /// @brief Returns the aborted-transactions list for the partition,
    /// filtered to only those with `first_offset >= fetch_offset`. This
    /// is what the Fetch v4+ response should populate.
    std::vector<AbortedTxn> abortedTransactions(const std::string& topic,
                                                PartitionId partition,
                                                Offset fetch_offset) const;

    /// @brief Maximum aborted entries retained per partition. Default
    /// 1000. Set via config in production deployments.
    void setMaxAbortedPerPartition(size_t n) { max_aborted_per_partition_ = n; }

    /// @brief For unit testing.
    size_t inFlightCount() const;

private:
    struct PartitionKey {
        std::string topic;
        PartitionId partition;
        bool operator==(const PartitionKey& other) const {
            return partition == other.partition && topic == other.topic;
        }
    };
    struct PartitionKeyHash {
        size_t operator()(const PartitionKey& k) const noexcept {
            return std::hash<std::string>{}(k.topic) ^
                   (std::hash<int32_t>{}(k.partition) << 1);
        }
    };

    struct PartitionState {
        // pid → first_offset of in-flight transactional batch
        std::unordered_map<int64_t, Offset> in_flight;
        // Aborted (pid, first_offset) ring (oldest first).
        std::deque<AbortedTxn> aborted;
    };

    mutable std::mutex mutex_;
    std::unordered_map<PartitionKey, PartitionState, PartitionKeyHash> partitions_;
    size_t max_aborted_per_partition_ = 1000;
};

}  // namespace kawasan::broker
