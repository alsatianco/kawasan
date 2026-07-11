#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "kawasan/common/types.h"
#include "kawasan/storage/record_batch.h"

namespace rocksdb {
class DB;
}

namespace kawasan::storage {

/// @brief Manages a single log segment
class LogSegment {
public:
    LogSegment(Offset base_offset, const std::string& path);
    ~LogSegment();

    // Disable copy, allow move
    LogSegment(const LogSegment&) = delete;
    LogSegment& operator=(const LogSegment&) = delete;
    LogSegment(LogSegment&&) noexcept;
    LogSegment& operator=(LogSegment&&) noexcept;

    /// @brief Appends a record batch to the segment
    /// @param batch The record batch to append
    /// @param sync If true, fsync the write (RocksDB WriteOptions.sync) before
    ///        returning so the record is durable across a power loss. If false,
    ///        the write is WAL-buffered only. Defaults to true (durability-first).
    /// @return The offset of the first record in the batch
    Offset append(const RecordBatch& batch, bool sync = true);

    /// @brief Reads a record batch at the given offset
    /// @param offset The offset to read from
    /// @return The record batch, or nullopt if not found
    std::optional<RecordBatch> read(Offset offset);

    /// @brief Reads multiple record batches starting from offset
    /// @param start_offset Starting offset
    /// @param max_bytes Maximum bytes to read
    /// @return Vector of record batches
    std::vector<RecordBatch> read(Offset start_offset, size_t max_bytes);

    /// @brief Phase 5.1: raw-bytes fetch path. Reads serialized record-batch
    /// bytes directly from RocksDB without deserializing into RecordBatch
    /// objects. The bytes can be written straight to a Fetch response,
    /// avoiding the deserialize-then-serialize round-trip on hot paths.
    /// Returns the concatenated serialized bytes of all batches starting
    /// at or after `start_offset`, capped at `max_bytes`.
    std::vector<uint8_t> readRaw(Offset start_offset, size_t max_bytes);

    /// @brief Returns the base offset of this segment
    Offset baseOffset() const { return base_offset_; }

    /// @brief Returns the next offset to be written
    Offset nextOffset() const;

    /// @brief Returns the size of this segment in bytes
    size_t size() const;

    /// @brief Flushes any pending writes to disk
    void flush();

    /// @brief Closes the segment
    void close();

    /// @brief Returns whether this segment is closed
    bool isClosed() const { return closed_; }

    /// @brief Returns the path to this segment
    const std::string& path() const { return path_; }

    /// @brief Phase 3.2: streaming compaction support.
    /// Delete the batch keyed at `base` (compaction's "drop this batch" op).
    /// Refuses (returns false) while this segment is marked active — the
    /// active segment is append-only until rolled, so no caller can compact
    /// live produce data regardless of its own loop bounds.
    /// Returns true if a batch was actually removed.
    bool deleteBatchAt(Offset base);

    /// @brief M7: truncate this segment's tail — remove every batch at or after
    /// `target` (and a batch that straddles `target`, since a batch cannot be
    /// split), then reset the next-offset to the highest remaining batch end
    /// (or the segment base if it becomes empty). Unlike deleteBatchAt this is
    /// permitted on the active segment — truncation always targets the tail.
    /// Returns the segment's next-offset after truncation.
    Offset truncateTo(Offset target);

    /// @brief Marks this segment as the log's active (append) segment.
    /// The owning Log keeps exactly the newest segment active; destructive
    /// per-batch ops (deleteBatchAt) refuse while the flag is set.
    void setActive(bool active);

    /// @brief Returns whether this segment is the active (append) segment.
    bool isActive() const;

private:
    void open();

    Offset base_offset_;
    std::string path_;
    std::unique_ptr<rocksdb::DB> db_;
    mutable std::mutex mutex_;
    Offset next_offset_;
    size_t size_bytes_ = 0;
    bool closed_ = false;
    bool active_ = false;
};

}  // namespace kawasan::storage
