#pragma once

#include <atomic>
#include <cstdint>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>

#include "kawasan/common/error.h"
#include "kawasan/common/types.h"

namespace kawasan::broker {

// Phase 2.1: ProducerStateManager.
//
// Tracks the last seen (producer_epoch, base_sequence, last_offset) per
// (partition, producer_id) for idempotent producer dedup. The state is
// in-memory only — restart resets it, which matches a server that has
// never seen the producer before. Persistence belongs to the
// `__producer_snapshot` (Kafka calls it the producer-state snapshot file)
// follow-up.
//
// Decisions match Kafka's `ProducerStateManager` semantics:
//
//   - producer_id < 0          → non-idempotent. accept.
//   - epoch < last_epoch       → INVALID_PRODUCER_EPOCH (a newer epoch
//                                 has already been seen; the writer is
//                                 fenced).
//   - epoch > last_epoch       → accept, reset sequence tracking for
//                                 this producer_id.
//   - epoch == last_epoch and
//         seq <= last_seq      → duplicate. return DUPLICATE_SEQUENCE_NUMBER
//                                 with the previously-stored base_offset
//                                 (the produce response will look like a
//                                 success replay).
//   - epoch == last_epoch and
//         seq != last_seq + 1  → OUT_OF_ORDER_SEQUENCE_NUMBER.
//   - epoch == last_epoch and
//         seq == last_seq + 1  → accept, advance state.
//
// Wraparound at INT32_MAX is handled the same way Kafka handles it:
// numericLast = last_seq, numericNew = new_seq; the "next" check is
// modulo INT32_MAX so a producer can sequence forever without resetting.
class ProducerStateManager {
public:
    struct CheckResult {
        ErrorCode error = ErrorCode::NONE;
        // For DUPLICATE_SEQUENCE_NUMBER: the offset of the original
        // (already-written) batch. Producer libraries treat the duplicate
        // as a successful retry and update their internal state.
        Offset duplicate_offset = -1;
    };

    /// @brief Checks whether a produce batch is acceptable.
    /// @param topic        Topic the batch belongs to.
    /// @param partition    Partition id.
    /// @param producer_id  Batch's producer_id (-1 = non-idempotent).
    /// @param producer_epoch
    ///                     Batch's producer_epoch.
    /// @param base_sequence
    ///                     Batch's base sequence (first record's sequence).
    /// @param record_count Number of records in the batch (used to compute
    ///                     the next expected sequence).
    /// @return ErrorCode::NONE if the batch is acceptable; otherwise a
    ///         specific error. The caller is responsible for actually
    ///         appending the batch and then calling `recordAppend()`.
    CheckResult check(const std::string& topic, PartitionId partition, int64_t producer_id,
                      int16_t producer_epoch, int32_t base_sequence, int32_t record_count) const;

    /// @brief Records that a batch was successfully appended.
    void recordAppend(const std::string& topic, PartitionId partition, int64_t producer_id,
                      int16_t producer_epoch, int32_t base_sequence, int32_t record_count,
                      Offset base_offset);

    /// @brief Discard a partition's cached state before rebuilding its retained log.
    void clearPartition(const std::string& topic, PartitionId partition);

    /// @brief Clears all state. Test-only.
    void clear();

    /// @brief Snapshot of an active producer for DescribeProducers.
    struct ActiveProducer {
        int64_t producer_id;
        int16_t producer_epoch;
        int32_t last_sequence;
        Offset last_base_offset;
    };

    /// @brief Returns all active producers for a (topic, partition).
    std::vector<ActiveProducer> listProducers(const std::string& topic,
                                              PartitionId partition) const;

    /// @brief M3: a fully-reconstructable producer-state record for one
    /// (topic, partition, producer_id) — everything `check()` needs. Written to
    /// a per-partition producer-state snapshot so startup replay only has to
    /// scan the log TAIL after the snapshot offset, not the whole log.
    struct SnapshotEntry {
        int64_t producer_id = -1;
        int16_t last_epoch = -1;
        int32_t last_sequence = -1;
        int32_t last_base_sequence = -1;
        int32_t last_record_count = 0;
        Offset last_base_offset = -1;
    };

    /// @brief M3: all producer-state entries for a (topic, partition).
    std::vector<SnapshotEntry> snapshotEntries(const std::string& topic,
                                               PartitionId partition) const;

    /// @brief M3: restore producer-state entries for a (topic, partition) from a
    /// snapshot (startup). Overwrites any existing state for those keys.
    void restoreEntries(const std::string& topic, PartitionId partition,
                        const std::vector<SnapshotEntry>& entries);

    /// @brief Phase EX-1 (§6.3): Prometheus metrics snapshot.
    struct Metrics {
        int64_t entries;            // gauge: tracked (topic, partition, producer_id) keys
        int64_t evictions_total;    // counter: keys evicted (expiry / pid reuse)
        int64_t producer_id_count;  // gauge: unique producer_ids currently tracked
    };
    Metrics getMetrics() const;

private:
    struct State {
        int16_t last_epoch = -1;
        // last_sequence is the *last* sequence in the most recent batch,
        // i.e. base_sequence + record_count - 1. This is what determines
        // "next expected" = last_sequence + 1.
        int32_t last_sequence = -1;
        int32_t last_base_sequence = -1;
        int32_t last_record_count = 0;
        Offset last_base_offset = -1;
    };

    struct Key {
        std::string topic;
        PartitionId partition;
        int64_t producer_id;
        bool operator==(const Key& other) const {
            return producer_id == other.producer_id && partition == other.partition &&
                   topic == other.topic;
        }
    };

    struct KeyHash {
        size_t operator()(const Key& k) const noexcept {
            // Combine producer_id with topic-name hash and partition.
            size_t h = std::hash<std::string>{}(k.topic);
            h ^= std::hash<int32_t>{}(k.partition) + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2);
            h ^= std::hash<int64_t>{}(k.producer_id) + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2);
            return h;
        }
    };

    mutable std::mutex mutex_;
    std::unordered_map<Key, State, KeyHash> states_;
    // Phase EX-1 metrics.
    std::atomic<int64_t> evictions_total_{0};
};

}  // namespace kawasan::broker
