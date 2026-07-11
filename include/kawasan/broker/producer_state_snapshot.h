#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "kawasan/broker/producer_state_manager.h"

namespace kawasan::broker {

/// @brief M3: on-disk producer-state snapshot for one partition. Serializes the
/// ProducerStateManager entries for a (topic, partition) plus the log offset the
/// snapshot is valid up to, so startup replay only rescans the log TAIL after
/// that offset instead of the whole log (bounded restart time).
///
/// File layout per partition dir: `producer-<offset>.psnap`, written atomically
/// (temp + fsync + rename). A CRC-32C header lets a torn/corrupt snapshot be
/// detected and skipped (loadNewest falls back to the next-older, then to a
/// full log replay). Old snapshots are pruned to the newest few.
class ProducerStateSnapshot {
public:
    struct Loaded {
        Offset snapshot_offset = 0;
        std::vector<ProducerStateManager::SnapshotEntry> entries;
    };

    /// @brief Serialize entries + the snapshot offset (versioned, CRC-32C).
    static std::vector<uint8_t> serialize(
        Offset snapshot_offset, const std::vector<ProducerStateManager::SnapshotEntry>& entries);

    /// @brief Inverse of serialize(). Returns nullopt on a CRC/format error.
    static std::optional<Loaded> deserialize(const std::vector<uint8_t>& bytes);

    /// @brief Atomically write a snapshot for `snapshot_offset` into
    /// `partition_dir` (temp + rename) and prune old snapshots to `keep` newest.
    /// No-op if the directory can't be written.
    static void write(const std::string& partition_dir, Offset snapshot_offset,
                      const std::vector<ProducerStateManager::SnapshotEntry>& entries,
                      int keep = 2);

    /// @brief Load the newest VALID snapshot in `partition_dir` (highest offset
    /// whose CRC checks out), skipping corrupt ones. nullopt if none.
    static std::optional<Loaded> loadNewest(const std::string& partition_dir);
};

}  // namespace kawasan::broker
