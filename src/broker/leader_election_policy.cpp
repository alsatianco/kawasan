#include "kawasan/broker/leader_election_policy.h"

#include <algorithm>

namespace kawasan::broker {

std::vector<PartitionLeadershipChange> computeLeadershipChanges(
    const std::vector<TopicMetadata>& topics, const std::set<BrokerId>& dead,
    bool unclean_enabled) {
    std::vector<PartitionLeadershipChange> changes;
    const auto is_dead = [&](BrokerId id) { return dead.count(id) > 0; };
    for (const auto& tm : topics) {
        for (const auto& pm : tm.partitions) {
            PartitionLeadershipChange change;
            change.topic = tm.name;
            change.partition = pm.partition;
            change.expected_partition_epoch = pm.partition_epoch;

            if (pm.leader < 0 || is_dead(pm.leader)) {
                std::vector<BrokerId> live_isr;
                for (BrokerId r : pm.replicas) {
                    const bool in_isr = std::find(pm.isr.begin(), pm.isr.end(), r) != pm.isr.end();
                    if (in_isr && !is_dead(r)) {
                        live_isr.push_back(r);
                    }
                }
                if (!live_isr.empty()) {
                    change.new_leader = live_isr.front();
                    if (live_isr != pm.isr) {
                        change.new_isr = live_isr;
                    }
                } else {
                    std::optional<BrokerId> live_replica;
                    for (BrokerId r : pm.replicas) {
                        if (!is_dead(r)) {
                            live_replica = r;
                            break;
                        }
                    }
                    if (unclean_enabled && live_replica) {
                        change.new_leader = *live_replica;
                        change.new_isr = std::vector<BrokerId>{*live_replica};
                        change.unclean = true;
                    } else if (pm.leader >= 0) {
                        change.new_leader = -1;
                    } else {
                        continue;  // already offline, still no eligible leader
                    }
                }
                changes.push_back(std::move(change));
                continue;
            }

            std::vector<BrokerId> shrunk;
            for (BrokerId r : pm.isr) {
                if (!is_dead(r)) {
                    shrunk.push_back(r);
                }
            }
            if (shrunk != pm.isr) {
                change.new_isr = std::move(shrunk);
                changes.push_back(std::move(change));
            }
        }
    }
    return changes;
}

ErrorCode checkLeaderEpoch(int32_t requested, int32_t current) {
    if (requested < 0 || requested == current) {
        return ErrorCode::NONE;
    }
    return requested < current ? ErrorCode::FENCED_LEADER_EPOCH : ErrorCode::UNKNOWN_LEADER_EPOCH;
}

}  // namespace kawasan::broker
