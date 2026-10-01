#pragma once

#include <cstdint>
#include <mutex>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "kawasan/common/types.h"

namespace kawasan::storage {

/// @brief One leader epoch and the first offset written in it.
struct EpochEntry {
    int32_t epoch;
    Offset start_offset;
    bool operator==(const EpochEntry& other) const = default;
};

/// @brief M8-F1: KIP-101 leader-epoch cache for one partition — the ordered
/// history of (epoch, start offset). It answers "where does epoch E end?" so a
/// follower can truncate exactly at its divergence point from a new leader.
/// Every mutation is persisted crash-atomically to `checkpoint_path` (Kafka's
/// `leader-epoch-checkpoint` text format: version, count, "epoch offset" lines).
/// A missing or unreadable checkpoint starts empty. Thread-safe.
class LeaderEpochCache {
public:
    explicit LeaderEpochCache(std::string checkpoint_path);

    /// @brief Records that `epoch` starts at `start_offset`. Ignored for an
    /// undefined (< 0), stale (< latest) or duplicate (== latest) epoch. Epochs
    /// that start at or after `start_offset` (they never wrote anything) are
    /// replaced.
    void assign(int32_t epoch, Offset start_offset);

    std::optional<int32_t> latestEpoch() const;

    /// @brief KIP-101 lookup for `requested` given the log end offset: the
    /// largest cached epoch <= requested and where it ends (the next cached
    /// epoch's start, or `log_end_offset` for the latest). A requested epoch
    /// older than every cached one ends where the earliest starts. Undefined
    /// (-1, -1) for -1, an empty cache, or an epoch newer than the latest.
    std::pair<int32_t, Offset> endOffsetFor(int32_t requested, Offset log_end_offset) const;

    /// @brief After truncating the log to `end_offset`: drops epochs that start
    /// at or after it.
    void truncateFromEnd(Offset end_offset);

    /// @brief After deleting records below `start_offset`: drops epochs that
    /// lie wholly below it; the epoch containing it now starts there.
    void truncateFromStart(Offset start_offset);

    std::vector<EpochEntry> entries() const;

private:
    void persistLocked() const;

    std::string path_;
    mutable std::mutex mutex_;
    std::vector<EpochEntry> entries_;  // strictly increasing epoch and offset
};

}  // namespace kawasan::storage
