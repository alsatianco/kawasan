#pragma once

#include <atomic>
#include <cstdint>
#include <limits>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "kawasan/common/types.h"

namespace kawasan::storage {
class Log;
}

namespace kawasan::broker {

// Tracks in-flight transaction starts (for LSO) and retained aborted ranges.
// Abort history is rebuilt from durable data/control batches at startup, then
// updated by EndTxn. Retention, rather than an arbitrary entry count, bounds it.
class IsolationTracker {
public:
    struct AbortedTxn {
        int64_t producer_id;
        Offset first_offset;
        Offset last_offset;
    };

    /// @brief Records that a transactional producer is now in-flight on
    /// `(topic, partition)` with its first record at offset
    /// `current_log_end`. Idempotent — repeat calls for the same
    /// (pid, topic, partition) are no-ops (the first_offset doesn't
    /// move within a single transaction).
    void recordInFlightTxn(int64_t producer_id, const std::string& topic, PartitionId partition,
                           Offset current_log_end);

    /// @brief Commits all in-flight txns for `transactional_id` across
    /// every partition. The caller has already advanced the partition's
    /// log to commit-record offsets; we just need to release the LSO
    /// hold.
    void commitInFlightTxns(const std::vector<std::pair<std::string, int32_t>>& partitions,
                            int64_t producer_id);

    /// @brief Aborts all in-flight txns. For each partition, the txn's
    /// first_offset is retained with the marker offset so
    /// read_committed consumers skip those records.
    void abortInFlightTxns(const std::vector<std::pair<std::string, int32_t>>& partitions,
                           int64_t producer_id,
                           Offset last_offset = std::numeric_limits<Offset>::max());

    /// @brief Rebuild abort history from retained transactional batches and
    /// durable ABORT markers. Does not restore in-flight LSO holds; those come
    /// from the coordinator log. Call before serving traffic.
    void recoverAbortedTransactions(const std::string& topic, PartitionId partition,
                                    storage::Log& log);

    /// @brief Returns the LSO for a partition. If no in-flight txns,
    /// returns the high_watermark argument unchanged.
    Offset lastStableOffset(const std::string& topic, PartitionId partition,
                            Offset high_watermark) const;

    /// @brief Returns the aborted-transactions list for the partition,
    /// including transactions whose marker is at/after `fetch_offset`. This
    /// is what the Fetch v4+ response should populate.
    std::vector<AbortedTxn> abortedTransactions(const std::string& topic, PartitionId partition,
                                                Offset fetch_offset, Offset log_start_offset = 0);

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
            return std::hash<std::string>{}(k.topic) ^ (std::hash<int32_t>{}(k.partition) << 1);
        }
    };

    struct PartitionState {
        // pid → first_offset of in-flight transactional batch
        std::unordered_map<int64_t, Offset> in_flight;
        // Retained aborted ranges, including the durable marker offset.
        std::vector<AbortedTxn> aborted;
    };

    mutable std::mutex mutex_;
    std::unordered_map<PartitionKey, PartitionState, PartitionKeyHash> partitions_;
};

}  // namespace kawasan::broker
