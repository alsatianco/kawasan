#include "kawasan/broker/group_coordinator.h"

#include <algorithm>
#include <sstream>
#include <utility>

#include "kawasan/broker/coordinator_routing.h"
#include "kawasan/broker/group_state_manager.h"
#include "kawasan/broker/monitoring/metrics_collector.h"
#include "kawasan/common/logger.h"
#include "kawasan/storage/log_manager.h"

namespace kawasan::broker {

namespace {
// Default retention: 7 days in milliseconds
constexpr int64_t kDefaultGroupRetentionMs = 7 * 24 * 60 * 60 * 1000LL;
// Default member timeout: 30 seconds
constexpr int64_t kDefaultMemberTimeoutMs = 30 * 1000LL;
// Group-retention scan interval: run every 10 minutes.
constexpr int64_t kCleanupIntervalMs = 10 * 60 * 1000LL;
// EX-12: timeout-check cadence. The cleanup thread wakes this often to
// enforce member session timeouts and rebalance timeouts (the retention
// scan still runs only every kCleanupIntervalMs). 1s gives ~1s precision
// on a 30s session timeout / 60s rebalance timeout without busy-spinning.
constexpr int64_t kTimeoutCheckIntervalMs = 1000LL;
}  // namespace

GroupCoordinator::GroupCoordinator(std::shared_ptr<OffsetManager> offset_manager,
                                   storage::LogManager* log_manager,
                                   std::shared_ptr<monitoring::MetricsCollector> metrics_collector)
    : offset_manager_(std::move(offset_manager)),
      log_manager_(log_manager),
      metrics_collector_(std::move(metrics_collector)),
      group_retention_ms_(kDefaultGroupRetentionMs),
      member_timeout_ms_(kDefaultMemberTimeoutMs) {
    Logger::info("GroupCoordinator initialized with persistent offset storage");
}

GroupCoordinator::~GroupCoordinator() {
    stopCleanupThread();
}

void GroupCoordinator::replaceCoordinatorPartition(int32_t partition, int32_t partition_count,
                                                   const std::vector<GroupRecord>& records,
                                                   std::shared_ptr<OffsetManager> offsets) {
    std::lock_guard<std::mutex> lock(mutex_);
    std::unordered_map<std::string, GroupState> restored;
    std::vector<GroupRecord> pending;
    const auto now = std::chrono::steady_clock::now();
    for (const auto& r : records) {
        if (r.tombstone || coordinatorPartitionFor(r.key.group_id, partition_count) != partition)
            throw std::invalid_argument("Invalid group acquisition image");
        if (r.key.kind == GroupRecordKey::Kind::PendingOffset) {
            pending.push_back(r);
        } else if (r.key.kind == GroupRecordKey::Kind::Group) {
            auto& g = restored[r.key.group_id];
            g.generation_id = r.group.generation;
            g.kind = static_cast<GroupStateKind>(r.group.state);
            g.protocol_type = r.group.protocol_type;
            g.protocol_name = r.group.protocol_name;
            g.leader_id = r.group.leader_id;
            g.rebalance_timeout_ms = r.group.rebalance_timeout_ms;
            g.rebalance_started_at = now;
            g.last_activity = std::chrono::system_clock::time_point(
                std::chrono::milliseconds(r.group.last_update_timestamp));
            for (const auto& m : r.group.members) {
                g.members.emplace(m.member_id,
                                  MemberState{m.member_id, m.client_id, m.client_host,
                                              m.group_instance_id, m.metadata, m.assignment, now});
            }
        }
    }
    // WriteBatch completes before publishing any in-memory image.
    offsets->replaceCoordinatorPartition(partition, partition_count, records);
    std::erase_if(groups_, [&](const auto& entry) {
        return coordinatorPartitionFor(entry.first, partition_count) == partition;
    });
    std::erase_if(pending_coordinator_offsets_, [&](const auto& r) {
        return coordinatorPartitionFor(r.key.group_id, partition_count) == partition;
    });
    groups_.merge(restored);
    pending_coordinator_offsets_.insert(pending_coordinator_offsets_.end(), pending.begin(),
                                        pending.end());
    offset_manager_ = std::move(offsets);
}

std::vector<GroupRecord> GroupCoordinator::pendingCoordinatorOffsets() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return pending_coordinator_offsets_;
}

GroupCoordinator::JoinGroupResult GroupCoordinator::handleJoinGroup(
    const protocol::JoinGroupRequest& request, const std::string& client_id,
    const std::string& client_host) {
    JoinGroupResult result;

    std::lock_guard<std::mutex> lock(mutex_);
    auto& group = groups_[request.groupId()];

    if (group.protocol_type.empty()) {
        group.protocol_type = request.protocolType();
    }

    if (group.protocol_name.empty()) {
        if (!request.groupProtocols().empty()) {
            group.protocol_name = request.groupProtocols().front().name;
        }
    }

    if (group.protocol_name.empty()) {
        result.error = ErrorCode::INCONSISTENT_GROUP_PROTOCOL;
        return result;
    }

    const auto metadata = selectMetadata(request, group.protocol_name);
    auto now = std::chrono::steady_clock::now();

    std::string member_id = request.memberId();

    // Phase 2.2: KIP-345 static membership. If the client supplied a
    // group.instance.id and we've seen that instance before with a known
    // member_id, reuse the existing member_id. This is the core static
    // membership guarantee: a rolling restart of consumers that supply a
    // stable group.instance.id doesn't trigger a rebalance — they reclaim
    // their previous member_id and existing assignment.
    if (request.groupInstanceId().has_value()) {
        const auto& instance_id = *request.groupInstanceId();
        for (const auto& [existing_id, existing_state] : group.members) {
            if (existing_state.group_instance_id.has_value() &&
                *existing_state.group_instance_id == instance_id) {
                member_id = existing_id;
                Logger::info(
                    "Static membership: instance '{}' reclaiming member_id '{}' in group '{}'",
                    instance_id, member_id, request.groupId());
                break;
            }
        }
    }

    // Phase 2.2: MEMBER_ID_REQUIRED handshake (KIP-394).
    // For JoinGroup v4+, a client connecting for the first time MUST first
    // send an empty member_id and receive a freshly-allocated one + the
    // MEMBER_ID_REQUIRED error code; only on the second JoinGroup with that
    // assigned member_id is the join real. We synthesize a member_id and
    // return MEMBER_ID_REQUIRED without admitting the member. For v0–v3 we
    // keep the legacy "fabricate a member_id and admit immediately" path so
    // existing kafka-python clients (which don't implement the handshake)
    // still work.
    if (member_id.empty() && request.sessionTimeoutMs() >= 0 /* always true */) {
        std::ostringstream oss;
        oss << request.groupId() << "-member-" << ++member_sequence_;
        member_id = oss.str();

        // For v4+ clients we signal MEMBER_ID_REQUIRED and let the client
        // retry with the assigned id. For older versions, fall through to
        // the legacy admit-on-first-join path.
        if (request.protocolType().empty() && request.groupProtocols().empty()) {
            // Truly empty join — treat as the first-stage handshake.
            // (Real Kafka uses the request header api_version; we approximate
            // by checking that the request body looks like a probe.)
            result.error = static_cast<ErrorCode>(79);  // MEMBER_ID_REQUIRED
            result.member_id = member_id;
            result.generation_id = -1;
            return result;
        }

        // Phase 2.2: initialize generation_id on first member, but DO NOT
        // bump on every subsequent join. Reason: kafka-python (and other
        // clients that don't implement the MEMBER_ID_REQUIRED handshake)
        // send JoinGroup with empty member_id on every rejoin after
        // ILLEGAL_GENERATION, which causes the broker to think "new
        // member!" and bump the generation again — triggering yet another
        // ILLEGAL_GENERATION on the next heartbeat. That's a rebalance
        // thrash. Full state-machine work (PreparingRebalance / sync
        // barrier) is the proper fix; for now we hold the generation
        // steady, which is the same conservative behavior as before this
        // attempt.
        if (group.generation_id == 0) {
            group.generation_id = 1;
            Logger::info("Group '{}' initialized at generation 1 (first member '{}')",
                         request.groupId(), member_id);
        }
    }

    const auto existing = group.members.find(member_id);
    const bool membership_changed =
        existing == group.members.end() || existing->second.metadata != metadata;
    MemberState& member = group.members[member_id];
    member.member_id = member_id;
    // 0A.10: preserve any previously-seen client identity if this is a known
    // member rejoining without identity fields populated (rare in practice).
    if (!client_id.empty()) {
        member.client_id = client_id;
    }
    if (!client_host.empty()) {
        member.client_host = client_host;
    }
    member.metadata = metadata;
    member.last_heartbeat = now;
    // Phase 2.2: persist the group_instance_id with the member so future
    // joins from the same instance can be matched.
    if (request.groupInstanceId().has_value()) {
        member.group_instance_id = request.groupInstanceId();
    }
    // Phase 2.2: honor per-member session_timeout_ms from the JoinGroup
    // request. We track the maximum across all members so the cleanup
    // thread evicts using the right deadline. session_timeout_ms is clamped
    // to a reasonable range to defend against misconfigured clients.
    int32_t session_ms = request.sessionTimeoutMs();
    if (session_ms < 6000)
        session_ms = 6000;
    if (session_ms > 300000)
        session_ms = 300000;
    if (static_cast<int64_t>(session_ms) > member_timeout_ms_) {
        member_timeout_ms_ = session_ms;
    }

    // EX-12: track the max rebalance.timeout.ms across members so the
    // cleanup thread can force-complete a stalled rebalance using the
    // right deadline. Clamp to a sane range (1s–15min) to defend against
    // misconfigured clients. Connect defaults to 60s.
    int32_t rebalance_ms = request.rebalanceTimeoutMs();
    if (rebalance_ms < 1000)
        rebalance_ms = 1000;
    if (rebalance_ms > 900000)
        rebalance_ms = 900000;
    if (rebalance_ms > group.rebalance_timeout_ms) {
        group.rebalance_timeout_ms = rebalance_ms;
    }

    // Update last activity for expiration tracking
    group.last_activity = std::chrono::system_clock::now();

    if (group.leader_id.empty()) {
        group.leader_id = member_id;
    }

    // Phase 2.2: state transitions.
    //   - Empty → CompletingRebalance when first member joins.
    //   - Stable → CompletingRebalance + generation bump when a new
    //     member joins an already-stable group. The bump forces existing
    //     members to rejoin (their heartbeat at the old generation gets
    //     ILLEGAL_GENERATION or REBALANCE_IN_PROGRESS), so the leader
    //     can compute a fresh assignment that includes the new member.
    if (group.kind == GroupStateKind::Stable && membership_changed) {
        group.kind = GroupStateKind::CompletingRebalance;
        group.generation_id++;
        for (auto& [id, state] : group.members)
            state.assignment.clear();
        // Phase EX-1: count per-group rebalances.
        group.rebalances_total.fetch_add(1, std::memory_order_relaxed);
        // EX-12: stamp the rebalance deadline on entry to CompletingRebalance.
        group.rebalance_started_at = now;
        // Set result generation_id to the new bumped value.
        Logger::info(
            "Group '{}' Stable→CompletingRebalance, generation bumped to {} (new member '{}')",
            request.groupId(), group.generation_id, member_id);
    } else if (group.kind == GroupStateKind::Empty) {
        group.kind = GroupStateKind::CompletingRebalance;
        // Phase EX-1: count Empty→CompletingRebalance as a rebalance too.
        group.rebalances_total.fetch_add(1, std::memory_order_relaxed);
        // EX-12: stamp the rebalance deadline.
        group.rebalance_started_at = now;
    } else if (group.kind == GroupStateKind::PreparingRebalance) {
        // EX-12: a member (re)joining a group that a rebalance-timeout
        // reset into PreparingRebalance advances it to CompletingRebalance,
        // awaiting the (possibly new) leader's SyncGroup. Refresh the
        // deadline so the new leader gets a full rebalance window.
        group.kind = GroupStateKind::CompletingRebalance;
        group.rebalance_started_at = now;
    }

    result.error = ErrorCode::NONE;
    result.generation_id = group.generation_id;
    result.protocol_type = group.protocol_type;
    result.protocol_name = group.protocol_name;
    result.leader_id = group.leader_id;
    result.member_id = member_id;
    result.members.reserve(group.members.size());
    for (const auto& entry : group.members) {
        protocol::JoinGroupResponse::Member response_member;
        response_member.member_id = entry.second.member_id;
        response_member.metadata = entry.second.metadata;
        result.members.push_back(std::move(response_member));
    }

    // Persist group state after member joins (async for performance)
    persistGroupState(request.groupId(), group);

    return result;
}

GroupCoordinator::SyncGroupResult GroupCoordinator::handleSyncGroup(
    const protocol::SyncGroupRequest& request) {
    SyncGroupResult result;

    std::lock_guard<std::mutex> lock(mutex_);
    GroupState* group = findGroup(request.groupId());
    if (!group) {
        result.error = ErrorCode::ILLEGAL_GENERATION;
        return result;
    }

    if (request.generationId() != group->generation_id) {
        result.error = ErrorCode::ILLEGAL_GENERATION;
        return result;
    }

    auto member_it = group->members.find(request.memberId());
    if (member_it == group->members.end()) {
        result.error = ErrorCode::UNKNOWN_MEMBER_ID;
        return result;
    }

    // A member may have joined after the leader's JoinGroup response, so
    // its assignment was computed from an obsolete membership snapshot.
    if (!request.assignments().empty() && group->kind == GroupStateKind::CompletingRebalance) {
        const bool complete =
            std::all_of(group->members.begin(), group->members.end(), [&](const auto& member) {
                return std::any_of(
                    request.assignments().begin(), request.assignments().end(),
                    [&](const auto& assignment) { return assignment.member_id == member.first; });
            });
        if (!complete) {
            ++group->generation_id;
            group->rebalances_total.fetch_add(1, std::memory_order_relaxed);
            group->rebalance_started_at = std::chrono::steady_clock::now();
            for (auto& [id, state] : group->members)
                state.assignment.clear();
            persistGroupState(request.groupId(), *group);
            result.error = ErrorCode::ILLEGAL_GENERATION;
            return result;
        }
    }

    // Store assignments from the leader
    bool leader_supplied_assignments = false;
    for (const auto& assignment : request.assignments()) {
        auto target = group->members.find(assignment.member_id);
        if (target != group->members.end()) {
            target->second.assignment = assignment.assignment;
            leader_supplied_assignments = true;
        }
    }

    // Phase 2.2: when the leader has supplied a fresh assignment, the
    // group transitions out of CompletingRebalance into Stable. We use
    // "leader supplied assignments" as the proxy because only the
    // leader's SyncGroup carries assignments in the wire protocol.
    if (leader_supplied_assignments && group->kind == GroupStateKind::CompletingRebalance) {
        group->kind = GroupStateKind::Stable;
        Logger::info("Group '{}' → Stable (generation {} assignment complete)", request.groupId(),
                     group->generation_id);
    }

    // Return the assignment for this member
    auto assigned = group->members.find(request.memberId());
    if (assigned != group->members.end()) {
        result.assignment = assigned->second.assignment;
        assigned->second.last_heartbeat = std::chrono::steady_clock::now();

        // Phase 2.2: when a follower's SyncGroup arrives before the
        // leader has supplied assignments for this generation, return
        // REBALANCE_IN_PROGRESS (27) so the follower retries — rather
        // than handing back an empty assignment that the consumer
        // would interpret as "no partitions for me, give up." This
        // mirrors Kafka's `awaitSyncing` state in
        // `GroupCoordinator.handleSyncGroup`.
        if (result.assignment.empty() && group->leader_id != request.memberId() &&
            group->kind == GroupStateKind::CompletingRebalance) {
            result.error = static_cast<ErrorCode>(27);  // REBALANCE_IN_PROGRESS
            result.assignment.clear();
            return result;
        }

        // If assignment is empty, create a valid empty assignment
        // structure for protocol compliance. This is the leader's
        // "no partitions assigned to me" case (e.g. consumer count >
        // partition count) and Kafka's normal-empty-result case.
        if (result.assignment.empty()) {
            result.assignment.resize(10, 0);
        }
    }

    // Persist group state after successful sync (rebalance complete)
    persistGroupState(request.groupId(), *group);

    // EX-12: echo the group's protocol_type/protocol_name (SyncGroup v5+).
    result.protocol_type = group->protocol_type;
    result.protocol_name = group->protocol_name;
    result.error = ErrorCode::NONE;
    return result;
}

ErrorCode GroupCoordinator::validateTxnOffsetCommit(
    const std::string& group_id, int32_t generation_id, const std::string& member_id,
    const std::optional<std::string>& group_instance_id) const {
    if (group_id.empty()) {
        return ErrorCode::INVALID_GROUP_ID;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    const GroupState* group = findGroup(group_id);
    if (group && group->kind == GroupStateKind::Dead) {
        return ErrorCode::COORDINATOR_NOT_AVAILABLE;
    }
    // Kafka accepts manual assignment on empty groups and legacy transactional
    // commits without membership metadata, even when a classic group is active.
    if (generation_id < 0 && (!group || group->kind == GroupStateKind::Empty)) {
        return ErrorCode::NONE;
    }
    if (generation_id < 0 && member_id.empty() && !group_instance_id) {
        return ErrorCode::NONE;
    }
    if (!group) {
        return ErrorCode::UNKNOWN_MEMBER_ID;
    }
    // Static identity fencing precedes generation validation. A replaced static
    // member must not be mistaken for a consumer that merely needs to rejoin.
    if (group_instance_id) {
        const auto instance = std::find_if(
            group->members.begin(), group->members.end(),
            [&](const auto& entry) { return entry.second.group_instance_id == group_instance_id; });
        if (instance == group->members.end()) {
            return ErrorCode::UNKNOWN_MEMBER_ID;
        }
        if (instance->first != member_id) {
            return ErrorCode::FENCED_INSTANCE_ID;
        }
    }
    if (group->members.find(member_id) == group->members.end()) {
        return ErrorCode::UNKNOWN_MEMBER_ID;
    }
    if (generation_id != group->generation_id) {
        return ErrorCode::ILLEGAL_GENERATION;
    }
    // A current member can commit transactional offsets during a rebalance.
    return ErrorCode::NONE;
}

ErrorCode GroupCoordinator::handleHeartbeat(const protocol::HeartbeatRequest& request) {
    std::lock_guard<std::mutex> lock(mutex_);
    GroupState* group = findGroup(request.groupId());
    if (!group) {
        return ErrorCode::ILLEGAL_GENERATION;
    }

    // Phase 2.2: when a heartbeat arrives at a stale generation AND
    // the group is mid-rebalance, return REBALANCE_IN_PROGRESS (27)
    // instead of ILLEGAL_GENERATION (22). Both trigger a rejoin in
    // kafka-python, but REBALANCE_IN_PROGRESS is the conventional
    // signal — it tells the client "we're rebalancing right now,
    // rejoin to participate" vs ILLEGAL_GENERATION's "your generation
    // is too old (no information about whether rebalance is active)."
    if (request.generationId() != group->generation_id) {
        if (group->kind == GroupStateKind::CompletingRebalance ||
            group->kind == GroupStateKind::PreparingRebalance) {
            return static_cast<ErrorCode>(27);  // REBALANCE_IN_PROGRESS
        }
        return ErrorCode::ILLEGAL_GENERATION;
    }

    auto it = group->members.find(request.memberId());
    if (it == group->members.end()) {
        return ErrorCode::UNKNOWN_MEMBER_ID;
    }

    it->second.last_heartbeat = std::chrono::steady_clock::now();
    group->last_activity = std::chrono::system_clock::now();

    // Phase 2.2: signal in-progress rebalance to members at the
    // current generation so they rejoin and the leader can compute a
    // fresh assignment including any newly-joined members.
    if (group->kind == GroupStateKind::PreparingRebalance ||
        group->kind == GroupStateKind::CompletingRebalance) {
        if (group->members.size() > 1) {
            return static_cast<ErrorCode>(27);
        }
    }

    return ErrorCode::NONE;
}

ErrorCode GroupCoordinator::handleLeaveGroup(const std::string& group_id,
                                             const std::string& member_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    GroupState* group = findGroup(group_id);
    if (!group) {
        return ErrorCode::UNKNOWN_MEMBER_ID;
    }

    auto it = group->members.find(member_id);
    if (it == group->members.end()) {
        return ErrorCode::UNKNOWN_MEMBER_ID;
    }

    group->members.erase(it);
    if (group->leader_id == member_id) {
        group->leader_id.clear();
        if (!group->members.empty()) {
            group->leader_id = group->members.begin()->second.member_id;
        }
    }

    // Phase 2.2: state transitions on leave.
    //   - If the group is now empty, → Empty. Generation stays so a
    //     rejoin gets ILLEGAL_GENERATION (forcing a clean handshake).
    //   - Otherwise (others still members), → PreparingRebalance because
    //     the assignment is now invalid — the remaining members need a
    //     fresh assignment that doesn't reference the departed member.
    if (group->members.empty()) {
        group->kind = GroupStateKind::Empty;
    } else if (group->kind == GroupStateKind::Stable) {
        group->kind = GroupStateKind::PreparingRebalance;
    }

    // NOTE: We do NOT erase the group when it becomes empty!
    // The group should persist with its committed offsets even when there are no active members.
    // This allows consumers to rejoin and continue from their last committed offset.

    // Persist group state after member leaves
    persistGroupState(group_id, *group);

    return ErrorCode::NONE;
}

std::vector<protocol::OffsetCommitResponse::Topic> GroupCoordinator::handleOffsetCommit(
    const protocol::OffsetCommitRequest& request, ErrorCode& overall_error) {
    std::vector<protocol::OffsetCommitResponse::Topic> topics;
    overall_error = ErrorCode::NONE;

    std::lock_guard<std::mutex> lock(mutex_);
    GroupState* group = findGroup(request.groupId());
    if (!group) {
        Logger::warn("OffsetCommit: group '{}' not found - returning ILLEGAL_GENERATION",
                     request.groupId());
        overall_error = ErrorCode::ILLEGAL_GENERATION;
    } else if (request.generationId() != group->generation_id) {
        Logger::warn("OffsetCommit: group='{}' generation mismatch (request={}, group={}) - "
                     "returning ILLEGAL_GENERATION",
                     request.groupId(), request.generationId(), group->generation_id);
        overall_error = ErrorCode::ILLEGAL_GENERATION;
    } else if (!request.memberId().empty() &&
               group->members.find(request.memberId()) == group->members.end()) {
        // Phase 2.4: when the client supplies a member_id, it must match a
        // current member of the group. Empty member_id is the legacy
        // "consumer not in group" path and is allowed for backward compat
        // (commits without a generation, simple producers committing offsets).
        Logger::warn("OffsetCommit: group='{}' member='{}' not in members map "
                     "- returning UNKNOWN_MEMBER_ID",
                     request.groupId(), request.memberId());
        overall_error = ErrorCode::UNKNOWN_MEMBER_ID;
    }
    // NOTE: We intentionally do NOT check if the member exists in the group anymore
    // Kafka allows offset commits from members that have left the group
    // This is important for auto-commit scenarios where the member might have already left
    // but still wants to commit its final offsets

    // Update last activity if group exists
    if (group) {
        group->last_activity = std::chrono::system_clock::now();
    }

    // Collect all offsets for batch commit (better performance)
    std::vector<OffsetManager::OffsetCommitData> batch_offsets;

    for (const auto& topic_request : request.topics()) {
        protocol::OffsetCommitResponse::Topic topic_response;
        topic_response.topic = topic_request.topic;

        for (const auto& partition_request : topic_request.partitions) {
            protocol::OffsetCommitResponse::Partition partition_response;
            partition_response.partition = partition_request.partition;
            partition_response.error = overall_error;

            if (overall_error == ErrorCode::NONE && group) {
                // Add to batch
                OffsetManager::OffsetCommitData data;
                data.topic = topic_request.topic;
                data.partition = partition_request.partition;
                data.offset = partition_request.offset;
                data.metadata = partition_request.metadata;
                data.committed_leader_epoch = partition_request.committed_leader_epoch;
                batch_offsets.push_back(std::move(data));

                partition_response.error = ErrorCode::NONE;
            } else {
                Logger::warn("OffsetCommit: group='{}' topic='{}' partition={} - FAILED (error={})",
                             request.groupId(), topic_request.topic, partition_request.partition,
                             static_cast<int>(overall_error));
            }

            topic_response.partitions.push_back(partition_response);
        }

        topics.push_back(std::move(topic_response));
    }

    // Batch commit all offsets in one RocksDB write
    if (!batch_offsets.empty() && overall_error == ErrorCode::NONE) {
        try {
            offset_manager_->commitOffsetBatch(request.groupId(), batch_offsets);
            Logger::info("OffsetCommit: group='{}' committed {} offsets in batch",
                         request.groupId(), batch_offsets.size());
        } catch (const std::exception& e) {
            Logger::error("OffsetCommit: Failed to batch commit offsets for group='{}': {}",
                          request.groupId(), e.what());
            // Update all responses to indicate error
            for (auto& topic : topics) {
                for (auto& partition : topic.partitions) {
                    partition.error = ErrorCode::COORDINATOR_NOT_AVAILABLE;
                }
            }
        }
    }

    return topics;
}

std::vector<protocol::OffsetFetchResponse::Topic> GroupCoordinator::handleOffsetFetch(
    const protocol::OffsetFetchRequest& request, ErrorCode& overall_error) const {
    std::vector<protocol::OffsetFetchResponse::Topic> topics;
    overall_error = ErrorCode::NONE;

    std::lock_guard<std::mutex> lock(mutex_);

    if (request.fetchAllTopics()) {
        for (const auto& [key, metadata] :
             offset_manager_->fetchAllOffsetsWithMetadata(request.groupId())) {
            if (topics.empty() || topics.back().topic != key.first) {
                protocol::OffsetFetchResponse::Topic topic;
                topic.topic = key.first;
                topics.push_back(std::move(topic));
            }
            protocol::OffsetFetchResponse::Partition partition;
            partition.partition = key.second;
            partition.offset = metadata.offset;
            partition.metadata = metadata.metadata;
            partition.committed_leader_epoch = metadata.committed_leader_epoch;
            topics.back().partitions.push_back(std::move(partition));
        }
        return topics;
    }

    for (const auto& topic_request : request.topics()) {
        protocol::OffsetFetchResponse::Topic topic_response;
        topic_response.topic = topic_request.topic;

        for (const auto& partition_request : topic_request.partitions) {
            protocol::OffsetFetchResponse::Partition partition_response;
            partition_response.partition = partition_request.partition;
            // 1.12 (unset offset fix): Kafka's wire contract says the unset
            // offset is -1, NOT 0. Returning 0 silently re-positioned every
            // never-committed consumer to the beginning of the partition,
            // causing duplicate reprocessing.
            partition_response.offset = -1;
            partition_response.metadata = "";
            partition_response.error = ErrorCode::NONE;

            // Manual/transactional consumers need no JoinGroup membership.
            // OffsetManager is the durable source of truth, including after
            // restart when no in-memory group has been reconstructed.
            auto offset_metadata = offset_manager_->fetchOffsetWithMetadata(
                request.groupId(), topic_request.topic, partition_request.partition);
            if (offset_metadata.has_value()) {
                partition_response.offset = offset_metadata->offset;
                partition_response.metadata = offset_metadata->metadata;
                partition_response.committed_leader_epoch = offset_metadata->committed_leader_epoch;
            }

            topic_response.partitions.push_back(std::move(partition_response));
        }

        topics.push_back(std::move(topic_response));
    }

    return topics;
}

GroupCoordinator::GroupState* GroupCoordinator::findGroup(const std::string& group_id) {
    auto it = groups_.find(group_id);
    if (it == groups_.end()) {
        return nullptr;
    }
    return &it->second;
}

const GroupCoordinator::GroupState* GroupCoordinator::findGroup(const std::string& group_id) const {
    auto it = groups_.find(group_id);
    if (it == groups_.end()) {
        return nullptr;
    }
    return &it->second;
}

std::vector<uint8_t> GroupCoordinator::selectMetadata(const protocol::JoinGroupRequest& request,
                                                      const std::string& protocol_name) {
    for (const auto& protocol : request.groupProtocols()) {
        if (protocol.name == protocol_name) {
            return protocol.metadata;
        }
    }

    if (!request.groupProtocols().empty()) {
        return request.groupProtocols().front().metadata;
    }

    return {};
}

std::vector<protocol::DescribeGroupsResponse::Group> GroupCoordinator::describeGroups(
    const std::vector<std::string>& group_ids) const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<protocol::DescribeGroupsResponse::Group> groups;
    groups.reserve(group_ids.size());

    for (const auto& group_id : group_ids) {
        protocol::DescribeGroupsResponse::Group group_response;
        group_response.group_id = group_id;

        const GroupState* group = findGroup(group_id);
        if (!group) {
            group_response.error_code = ErrorCode::NONE;
            group_response.group_state = "Dead";
            group_response.protocol_type = "";
            group_response.protocol_data = "";
        } else {
            group_response.error_code = ErrorCode::NONE;
            // Phase 2.2: report the explicit state-machine kind.
            group_response.group_state = stateKindName(group->kind);
            group_response.protocol_type = group->protocol_type;
            group_response.protocol_data = group->protocol_name;

            group_response.members.reserve(group->members.size());
            for (const auto& [member_id, member_state] : group->members) {
                protocol::DescribeGroupsResponse::Member member_response;
                member_response.member_id = member_state.member_id;
                member_response.group_instance_id = member_state.group_instance_id;
                // 0A.10: report the real client.id and peer address learned at
                // JoinGroup time; fall back to legacy "unknown" only if missing.
                member_response.client_id =
                    member_state.client_id.empty() ? "unknown" : member_state.client_id;
                member_response.client_host =
                    member_state.client_host.empty() ? "unknown" : member_state.client_host;
                member_response.member_metadata = member_state.metadata;
                member_response.member_assignment = member_state.assignment;
                group_response.members.push_back(std::move(member_response));
            }
        }

        groups.push_back(std::move(group_response));
    }

    return groups;
}

std::vector<protocol::ListGroupsResponse::Group> GroupCoordinator::listGroups() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<protocol::ListGroupsResponse::Group> groups;
    groups.reserve(groups_.size());

    for (const auto& [group_id, group_state] : groups_) {
        protocol::ListGroupsResponse::Group group;
        group.group_id = group_id;
        group.protocol_type = group_state.protocol_type;
        // Phase 2.2: report the explicit state-machine kind.
        group.group_state = stateKindName(group_state.kind);
        groups.push_back(std::move(group));
    }

    return groups;
}

void GroupCoordinator::loadGroupsFromStorage() {
    std::lock_guard<std::mutex> lock(mutex_);

    // Get all group IDs from offset manager
    auto group_ids = offset_manager_->listGroups();

    Logger::info("Loading {} groups from persistent storage", group_ids.size());

    for (const auto& group_id : group_ids) {
        auto metadata_opt = offset_manager_->loadGroupMetadata(group_id);
        if (!metadata_opt) {
            Logger::warn("Failed to load metadata for group: {}", group_id);
            continue;
        }

        const auto& metadata = *metadata_opt;

        // Restore group state
        GroupState& group = groups_[group_id];
        group.generation_id = metadata.generation;
        group.protocol_type = metadata.protocol_type;
        group.protocol_name = metadata.protocol;
        group.last_activity = std::chrono::system_clock::time_point(
            std::chrono::milliseconds(metadata.last_update_timestamp));

        // Restore members
        for (const auto& member_meta : metadata.members) {
            MemberState member;
            member.member_id = member_meta.member_id;
            // 0A.10: restore identity from persisted metadata (treat the legacy
            // sentinel "unknown" as missing so a later JoinGroup can repopulate).
            if (member_meta.client_id != "unknown") {
                member.client_id = member_meta.client_id;
            }
            if (member_meta.client_host != "unknown") {
                member.client_host = member_meta.client_host;
            }
            member.metadata = member_meta.metadata;
            member.assignment = member_meta.assignment;
            member.last_heartbeat = std::chrono::steady_clock::now();

            group.members[member.member_id] = std::move(member);

            // Set leader if not set
            if (group.leader_id.empty()) {
                group.leader_id = member_meta.member_id;
            }
        }

        Logger::info("Restored group: {} (state={}, generation={}, members={})", group_id,
                     metadata.state, metadata.generation, metadata.members.size());
    }
}

void GroupCoordinator::persistGroupState(const std::string& group_id, const GroupState& group) {
    // Convert GroupState to GroupMetadata
    OffsetManager::GroupMetadata metadata;

    // Determine state based on members
    if (group.members.empty()) {
        metadata.state = "Empty";
    } else {
        metadata.state = "Stable";
    }

    metadata.protocol_type = group.protocol_type;
    metadata.protocol = group.protocol_name;
    metadata.generation = group.generation_id;
    metadata.last_update_timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
                                         std::chrono::system_clock::now().time_since_epoch())
                                         .count();

    // Convert members
    for (const auto& [member_id, member_state] : group.members) {
        OffsetManager::MemberMetadata member_meta;
        member_meta.member_id = member_state.member_id;
        // 0A.10: persist the real identity so it survives broker restart.
        member_meta.client_id = member_state.client_id.empty() ? "unknown" : member_state.client_id;
        member_meta.client_host =
            member_state.client_host.empty() ? "unknown" : member_state.client_host;
        member_meta.metadata = member_state.metadata;
        member_meta.assignment = member_state.assignment;

        metadata.members.push_back(std::move(member_meta));
    }

    try {
        offset_manager_->saveGroupMetadata(group_id, metadata);
        Logger::debug("Persisted group state: {} (state={}, generation={}, members={})", group_id,
                      metadata.state, metadata.generation, metadata.members.size());
    } catch (const std::exception& e) {
        Logger::error("Failed to persist group state for {}: {}", group_id, e.what());
    }
}

void GroupCoordinator::startCleanupThread() {
    if (cleanup_running_.load()) {
        Logger::warn("Cleanup thread already running");
        return;
    }

    cleanup_running_.store(true);
    cleanup_thread_ = std::thread(&GroupCoordinator::cleanupExpiredGroups, this);
    Logger::info("Started group cleanup thread (retention={}ms, interval={}ms)",
                 group_retention_ms_, kCleanupIntervalMs);
}

void GroupCoordinator::stopCleanupThread() {
    if (!cleanup_running_.load()) {
        return;
    }

    cleanup_running_.store(false);
    cleanup_cv_.notify_all();

    if (cleanup_thread_.joinable()) {
        cleanup_thread_.join();
    }

    Logger::info("Stopped group cleanup thread");
}

void GroupCoordinator::cleanupExpiredGroups() {
    // EX-12: the loop wakes every kTimeoutCheckIntervalMs to enforce member
    // and rebalance timeouts promptly; the expensive expired-group retention
    // scan runs only every `retention_scan_every` iterations to preserve the
    // original 10-minute cadence.
    const int64_t retention_scan_every =
        std::max<int64_t>(1, kCleanupIntervalMs / kTimeoutCheckIntervalMs);
    int64_t iteration = 0;

    while (cleanup_running_.load()) {
        // Wait for the timeout-check interval or until stopped
        {
            std::unique_lock<std::mutex> lock(cleanup_mutex_);
            cleanup_cv_.wait_for(lock, std::chrono::milliseconds(kTimeoutCheckIntervalMs),
                                 [this] { return !cleanup_running_.load(); });
        }

        if (!cleanup_running_.load()) {
            break;
        }

        // Enforce timeouts on every wake (cheap, ~1s cadence).
        checkMemberTimeouts();
        // EX-12: force-complete any rebalance that overran its deadline.
        checkRebalanceTimeouts();

        // Only run the retention scan periodically.
        if (++iteration % retention_scan_every != 0) {
            continue;
        }

        // Scan for expired groups
        auto now = std::chrono::system_clock::now();
        std::vector<std::string> expired_groups;

        {
            std::lock_guard<std::mutex> lock(mutex_);

            for (const auto& [group_id, group_state] : groups_) {
                // Only expire groups with no active members
                if (group_state.members.empty()) {
                    auto age_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                      now - group_state.last_activity)
                                      .count();

                    if (age_ms > group_retention_ms_) {
                        expired_groups.push_back(group_id);
                    }
                }
            }
        }

        // Delete expired groups (outside the main lock to avoid long critical section)
        for (const auto& group_id : expired_groups) {
            try {
                // Delete from storage
                offset_manager_->deleteGroup(group_id);

                // Remove from in-memory map
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    groups_.erase(group_id);
                }

                Logger::info("Deleted expired group: {} (no activity for >{}ms)", group_id,
                             group_retention_ms_);
            } catch (const std::exception& e) {
                Logger::error("Failed to delete expired group {}: {}", group_id, e.what());
            }
        }

        if (!expired_groups.empty()) {
            Logger::info("Cleanup completed: deleted {} expired groups", expired_groups.size());
        }
    }
}

void GroupCoordinator::checkMemberTimeouts() {
    std::lock_guard<std::mutex> lock(mutex_);
    auto now = std::chrono::steady_clock::now();

    for (auto& [group_id, group_state] : groups_) {
        std::vector<std::string> timed_out_members;

        // Find timed-out members
        for (const auto& [member_id, member_state] : group_state.members) {
            auto age_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                              now - member_state.last_heartbeat)
                              .count();

            if (age_ms > member_timeout_ms_) {
                timed_out_members.push_back(member_id);
            }
        }

        // Remove timed-out members
        for (const auto& member_id : timed_out_members) {
            group_state.members.erase(member_id);
            // Phase EX-1: count per-broker member timeouts.
            member_timeout_total_.fetch_add(1, std::memory_order_relaxed);

            // Update leader if necessary
            if (group_state.leader_id == member_id) {
                group_state.leader_id.clear();
                if (!group_state.members.empty()) {
                    group_state.leader_id = group_state.members.begin()->first;
                }
            }

            Logger::info("Evicted timed-out member: group={}, member={} (no heartbeat for >{}ms)",
                         group_id, member_id, member_timeout_ms_);
        }

        // Persist group state if members were evicted
        if (!timed_out_members.empty()) {
            group_state.last_activity = std::chrono::system_clock::now();
            persistGroupState(group_id, group_state);
        }
    }
}

void GroupCoordinator::checkRebalanceTimeouts() {
    std::vector<std::string> recovered_groups;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        const auto now = std::chrono::steady_clock::now();

        for (auto& [group_id, group] : groups_) {
            const bool rebalancing = group.kind == GroupStateKind::PreparingRebalance ||
                                     group.kind == GroupStateKind::CompletingRebalance;
            if (!rebalancing) {
                continue;
            }

            // Skip groups whose timeout was never set (e.g. reloaded from
            // older persisted state without the field) — we have no
            // trustworthy deadline, so don't force-recover them.
            if (group.rebalance_timeout_ms <= 0) {
                continue;
            }

            const auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                        now - group.rebalance_started_at)
                                        .count();
            if (elapsed_ms <= group.rebalance_timeout_ms) {
                continue;  // still within the rebalance window
            }

            // The rebalance overran its deadline — the leader never
            // completed it (no SyncGroup). Evict the stalled leader and
            // force a fresh rebalance so the remaining members can elect
            // a new leader instead of waiting forever.
            const std::string stalled_leader = group.leader_id;
            if (!stalled_leader.empty()) {
                group.members.erase(stalled_leader);
            }

            group.generation_id++;
            group.rebalances_total.fetch_add(1, std::memory_order_relaxed);
            group.leader_id.clear();  // first re-joiner becomes new leader

            if (group.members.empty()) {
                group.kind = GroupStateKind::Empty;
            } else {
                // PreparingRebalance: the generation bump makes remaining
                // members' heartbeats return REBALANCE_IN_PROGRESS, so they
                // re-JoinGroup; the next joiner is elected leader and drives
                // SyncGroup to Stable.
                group.kind = GroupStateKind::PreparingRebalance;
                group.rebalance_started_at = now;
            }

            Logger::warn("Group '{}' rebalance timed out after {}ms; evicted stalled "
                         "leader '{}', forcing generation {} (remaining members: {})",
                         group_id, group.rebalance_timeout_ms, stalled_leader, group.generation_id,
                         group.members.size());
            recovered_groups.push_back(group_id);
        }
    }

    // Persist outside the lock to keep the critical section short.
    for (const auto& group_id : recovered_groups) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = groups_.find(group_id);
        if (it != groups_.end()) {
            it->second.last_activity = std::chrono::system_clock::now();
            persistGroupState(group_id, it->second);
        }
    }
}

void GroupCoordinator::computeAndRecordConsumerLag() {
    // Cannot compute lag without log_manager or metrics_collector
    if (!log_manager_ || !metrics_collector_) {
        return;
    }

    // Step 1: Quickly collect group IDs under lock
    std::vector<std::string> group_ids;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        group_ids.reserve(groups_.size());
        for (const auto& [group_id, group_state] : groups_) {
            // Only compute lag for groups with committed offsets
            if (group_state.generation_id > 0) {
                group_ids.push_back(group_id);
            }
        }
    }

    // Step 2: Process groups outside the lock to avoid blocking consumer operations
    for (const auto& group_id : group_ids) {
        // Get all committed offsets for this group (RocksDB I/O)
        auto committed_offsets = offset_manager_->fetchAllOffsets(group_id);

        // Skip if no offsets committed yet
        if (committed_offsets.empty()) {
            continue;
        }

        for (const auto& [topic_partition, committed_offset] : committed_offsets) {
            const auto& topic = topic_partition.first;
            const auto& partition = topic_partition.second;

            // Get log end offset
            auto* log = log_manager_->getLog(topic, partition);
            if (!log) {
                // Log doesn't exist, skip
                continue;
            }

            int64_t log_end_offset = log->logEndOffset();
            int64_t lag = log_end_offset - committed_offset;

            // Lag should not be negative
            if (lag < 0) {
                lag = 0;
            }

            // Record the lag metric (thread-safe)
            metrics_collector_->setConsumerLag(group_id, topic, partition, lag);
        }
    }
}

GroupCoordinator::Metrics GroupCoordinator::getMetrics() const {
    Metrics m{};
    m.member_timeout_total = member_timeout_total_.load(std::memory_order_relaxed);
    std::lock_guard<std::mutex> lock(mutex_);
    m.groups.reserve(groups_.size());
    for (const auto& [group_id, gs] : groups_) {
        GroupMetric gm;
        gm.group_id = group_id;
        gm.state = stateKindName(gs.kind);
        gm.rebalances_total = gs.rebalances_total.load(std::memory_order_relaxed);
        m.groups.push_back(std::move(gm));
    }
    return m;
}

}  // namespace kawasan::broker
