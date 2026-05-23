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
    /// @return The offset of the first record in the batch
    Offset append(const RecordBatch& batch);

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
    /// Safe to call while the active segment is held by another writer;
    /// callers must coordinate so the active segment isn't compacted.
    /// Returns true if a batch was actually removed.
    bool deleteBatchAt(Offset base);

private:
    void open();

    Offset base_offset_;
    std::string path_;
    std::unique_ptr<rocksdb::DB> db_;
    mutable std::mutex mutex_;
    Offset next_offset_;
    size_t size_bytes_ = 0;
    bool closed_ = false;
};

}  // namespace kawasan::storage
