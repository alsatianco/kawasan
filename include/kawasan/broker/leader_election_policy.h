#pragma once

#include <optional>
#include <set>
#include <string>
#include <vector>

#include "kawasan/common/types.h"

namespace kawasan::broker {

/// @brief One partition's leadership change decided by the controller sweep.
struct PartitionLeadershipChange {
    std::string topic;
    PartitionId partition = 0;
    /// Set → commit UPDATE_LEADER with this leader (-1 = offline, no leader).
    std::optional<BrokerId> new_leader;
    /// Set → commit UPDATE_ISR with this ISR (after the leader change, if any).
    std::optional<std::vector<BrokerId>> new_isr;
    /// The new leader was NOT in the ISR (acked records may be lost).
    bool unclean = false;
};

/// @brief M8-C: the controller's failover policy, a pure function.
///
/// For each partition:
///  - leader dead (or offline, -1): elect the first assigned replica that is a
///    live ISR member; the ISR becomes the live ISR members. With no live ISR
///    member, elect the first live assigned replica only if `unclean_enabled`
///    (ISR = {it}); otherwise mark the partition offline (leader -1, ISR kept so
///    its members stay eligible when they return).
///  - leader alive: drop dead brokers from the ISR.
/// Returns only partitions that need a change, in metadata order.
std::vector<PartitionLeadershipChange> computeLeadershipChanges(
    const std::vector<TopicMetadata>& topics, const std::set<BrokerId>& dead,
    bool unclean_enabled);

}  // namespace kawasan::broker
