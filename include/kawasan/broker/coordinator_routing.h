#pragma once

#include <algorithm>
#include <cstdint>
#include <string>

namespace kawasan::broker {

/// @brief The single routing function from a group id / transactional id to its
/// coordinator partition of `__consumer_offsets` / `__transaction_state`.
///
/// Java `String.hashCode()` over the key's bytes (h = 31*h + c, 32-bit wrap),
/// reduced as an unsigned 32-bit value modulo the partition count. Every
/// placement decision — offset-commit mirroring, AddOffsetsToTxn, txn-state
/// persistence and FindCoordinator — MUST use this one function so they agree
/// on the partition; data already on disk was placed by exactly this formula.
inline int32_t coordinatorPartitionFor(const std::string& key, int32_t num_partitions) {
    uint32_t h = 0;
    for (unsigned char c : key) {
        h = 31u * h + c;
    }
    return static_cast<int32_t>(h % static_cast<uint32_t>(std::max<int32_t>(1, num_partitions)));
}

}  // namespace kawasan::broker
