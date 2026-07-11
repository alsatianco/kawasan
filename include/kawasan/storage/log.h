#pragma once

#include <chrono>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <tuple>
#include <vector>

#include "kawasan/common/types.h"
#include "kawasan/storage/log_segment.h"
#include "kawasan/storage/record_batch.h"

namespace kawasan::storage {

/// @brief Durability mode for partition-log writes.
/// kSync issues an fsync (RocksDB WriteOptions.sync=true) before a produce
/// append returns, so an acknowledged record survives a power loss / OS
/// crash. kAsync relies on the RocksDB WAL in the OS page cache (lower
/// latency, but a machine crash before the next flush loses the tail).
/// Default is kSync: the broker's at-least-once guarantee must be true.
enum class FlushMode { kSync, kAsync };

/// @brief Configuration for a log
struct LogConfig {
    size_t segment_size = 1024 * 1024 * 1024;        // 1GB
    int64_t segment_ms = -1;                         // Time-based rolling (disabled by default)
    int64_t retention_bytes = -1;                    // -1 = unlimited
    int64_t retention_ms = 7 * 24 * 60 * 60 * 1000;  // 7 days
    bool cleanup_policy_compact = false;
    bool cleanup_policy_delete = true;
    FlushMode flush_mode = FlushMode::kSync;

    /// @brief 0A.4: derive a LogConfig by applying Kafka-style topic configs on
    /// top of a base. Honored keys: cleanup.policy, retention.ms,
    /// retention.bytes, segment.bytes, segment.ms. Unknown keys are ignored
    /// for forward-compat with future admin APIs.
    static LogConfig fromMap(const std::map<std::string, std::string>& configs,
                             const LogConfig& base);
    /// @brief Convenience overload using a default-constructed base.
    static LogConfig fromMap(const std::map<std::string, std::string>& configs);
};

/// @brief Manages a log (collection of segments) for a topic-partition
class Log {
public:
    Log(const std::string& topic, PartitionId partition, const std::string& log_dir,
        const LogConfig& config = LogConfig());
    ~Log();

    // Disable copy, allow move
    Log(const Log&) = delete;
    Log& operator=(const Log&) = delete;
    Log(Log&&) noexcept;
    Log& operator=(Log&&) noexcept;

    /// @brief Appends records to the log
    /// @param records The records to append
    /// @param force_sync If true, fsync this append regardless of the log's
    ///        flush mode — used for durability-critical internal state such as
    ///        __transaction_state, which must survive a power loss even when
    ///        the broker's data durability is `async`.
    /// @return The offset of the first appended record
    Offset append(const std::vector<Record>& records, bool force_sync = false);

    /// @brief Appends a fully-formed RecordBatch, preserving its
    /// V2 header attributes (isControl, isTransactional, producer_id,
    /// producer_epoch, baseSequence). Used for control records emitted
    /// by EndTxn — these MUST keep the isControl bit so read_committed
    /// consumers can identify transaction boundaries (KIP-98).
    /// The base offset is assigned by the log.
    /// @param advance_high_watermark When true (default) the high watermark is
    ///        advanced to the new log-end-offset in the same step, matching the
    ///        single-node invariant HW==LEO. A leader of a replicated partition
    ///        (RF>1) passes false so the high watermark is instead advanced by
    ///        the replication layer once the ISR has the record
    ///        (`ReplicaManager::maybeAdvanceHighWatermark`), keeping
    ///        un-replicated records below the watermark and thus invisible to
    ///        consumers.
    Offset appendBatch(RecordBatch batch, bool advance_high_watermark = true);

    /// @brief Reads records from the log
    /// @param start_offset Starting offset
    /// @param max_bytes Maximum bytes to read
    /// @return Vector of record batches
    std::vector<RecordBatch> read(Offset start_offset, size_t max_bytes);

    /// @brief Phase 5.1: raw-bytes fetch path. Returns serialized
    /// record-batch bytes concatenated, suitable for direct write into
    /// the Fetch response body. Saves a deserialize-then-serialize cycle
    /// on the hot path. Honors max_bytes by stopping after a batch that
    /// would exceed the cap (always returns at least one batch if any
    /// exist starting at start_offset).
    std::vector<uint8_t> readRaw(Offset start_offset, size_t max_bytes);

    /// @brief Returns the log start offset (oldest available)
    Offset logStartOffset() const;

    /// @brief Returns the log end offset (next offset to be written)
    Offset logEndOffset() const;

    /// @brief Returns the high watermark
    Offset highWatermark() const { return high_watermark_; }

    /// @brief Total on-disk size of this partition's log in bytes (sum of all
    /// segment sizes). Used by DescribeLogDirs to report real per-partition
    /// storage usage.
    size_t sizeBytes() const;

    /// @brief Sets the high watermark
    void setHighWatermark(Offset offset);

    /// @brief Flushes all segments to disk
    void flush();

    /// @brief Closes the log
    void close();

    /// @brief Returns the topic name
    const std::string& topic() const { return topic_; }

    /// @brief Returns the partition ID
    PartitionId partition() const { return partition_; }

    /// @brief Backend for ListOffsets MAX_TIMESTAMP (-3): the base offset and
    /// max timestamp of the data batch with the highest timestamp. Control
    /// batches (transaction COMMIT/ABORT markers) are excluded — their
    /// wall-clock timestamps must never win the scan. Returns nullopt when
    /// the log holds no data batches. O(n) over batches until a timestamp
    /// index (.timeindex analog) lands.
    std::optional<std::pair<Offset, int64_t>> maxTimestampOffset();

    /// @brief Rolls a new segment if needed
    void maybeRoll();

    /// @brief Performs log cleanup (deletion or compaction)
    void cleanup();

    /// @brief Phase 4.1d: truncates the log prefix, advancing logStartOffset
    /// to at least `new_start_offset`. Segments whose end_offset is strictly
    /// below the new start are deleted immediately; the active segment is
    /// preserved even if its data is older. Returns the new effective
    /// log_start_offset (clamped to logEndOffset).
    Offset truncatePrefix(Offset new_start_offset);

private:
    void loadSegments();
    void rollNewSegment();
    LogSegment* activeSegment();
    const LogSegment* activeSegment() const;
    bool shouldRollForTime() const;
    std::chrono::steady_clock::time_point now() const;
    Offset endOffsetUnlocked() const;
    Offset startOffsetUnlocked() const;
    void loadCheckpoint();
    void persistCheckpointLocked() const;
    std::optional<std::tuple<Offset, Offset, Offset>> readCheckpointFromDisk() const;
    std::string checkpointPath() const;

    std::string topic_;
    PartitionId partition_;
    std::string log_dir_;
    LogConfig config_;
    std::vector<std::unique_ptr<LogSegment>> segments_;
    mutable std::mutex mutex_;
    Offset high_watermark_ = 0;
    std::chrono::steady_clock::time_point last_roll_time_;
    bool last_roll_time_initialized_ = false;
    bool closed_ = false;
};

}  // namespace kawasan::storage
