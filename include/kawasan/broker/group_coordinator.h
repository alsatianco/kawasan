#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "kawasan/broker/group_state_manager.h"
#include "kawasan/broker/offset_manager.h"
#include "kawasan/common/error.h"
#include "kawasan/protocol/describe_groups_request.h"
#include "kawasan/protocol/heartbeat_request.h"
#include "kawasan/protocol/join_group_request.h"
#include "kawasan/protocol/list_groups_request.h"
#include "kawasan/protocol/offset_commit_request.h"
#include "kawasan/protocol/offset_fetch_request.h"
#include "kawasan/protocol/sync_group_request.h"

namespace kawasan {
namespace storage {
class LogManager;
}  // namespace storage

namespace broker {
namespace monitoring {
class MetricsCollector;
}  // namespace monitoring
}  // namespace broker
}  // namespace kawasan

namespace kawasan::broker {

/// @brief Minimal in-memory group coordinator to support basic consumer groups.
/// Offsets are now persisted to disk via OffsetManager.
class GroupCoordinator {
public:
    class GroupProposal;
    explicit GroupCoordinator(
        std::shared_ptr<OffsetManager> offset_manager, storage::LogManager* log_manager = nullptr,
        std::shared_ptr<monitoring::MetricsCollector> metrics_collector = nullptr);
    ~GroupCoordinator();
    void replaceCoordinatorPartition(int32_t partition, int32_t partition_count,
                                     const std::vector<GroupRecord>& records,
                                     std::shared_ptr<OffsetManager> offsets);
    // Same committed image installation, but retain live clocks and counters
    // for surviving identities. Acquisition deliberately rebuilds those clocks.
    void publishCoordinatorPartition(int32_t partition, int32_t partition_count,
                                     const std::vector<GroupRecord>& records,
                                     std::shared_ptr<OffsetManager> offsets,
                                     const GroupProposal* proposal = nullptr);
    std::vector<GroupRecord> pendingCoordinatorOffsets() const;
    // Detached state-machine execution: durable fields come from the source,
    // live clocks/counters from this owner. Only committed publication installs
    // the touched group's runtime changes. Caller serializes this partition.
    std::unique_ptr<GroupProposal> proposeGroup(const std::string& group_id,
                                                const std::optional<GroupRecord>& committed);

    GroupCoordinator(const GroupCoordinator&) = delete;
    GroupCoordinator& operator=(const GroupCoordinator&) = delete;
    GroupCoordinator(GroupCoordinator&&) = delete;
    GroupCoordinator& operator=(GroupCoordinator&&) = delete;

    struct JoinGroupResult {
        ErrorCode error = ErrorCode::NONE;
        int32_t generation_id = 0;
        std::string protocol_type;  // EX-12: echoed in JoinGroup v7+ response
        std::string protocol_name;
        std::string leader_id;
        std::string member_id;
        std::vector<protocol::JoinGroupResponse::Member> members;
    };

    struct SyncGroupResult {
        ErrorCode error = ErrorCode::NONE;
        std::string protocol_type;  // EX-12: echoed in SyncGroup v5+ response
        std::string protocol_name;
        std::vector<uint8_t> assignment;
    };

    /// @brief Handle a JoinGroup request.
    /// @param request The decoded JoinGroup request.
    /// @param client_id The client.id from the request header (KIP-511; "" if unknown).
    /// @param client_host The remote peer endpoint string (e.g. "192.168.1.5:54321";
    ///                    "" if unknown). 0A.10: populate this through to DescribeGroups
    ///                    instead of the hardcoded "unknown" placeholder.
    JoinGroupResult handleJoinGroup(const protocol::JoinGroupRequest& request,
                                    const std::string& client_id = "",
                                    const std::string& client_host = "");
    SyncGroupResult handleSyncGroup(const protocol::SyncGroupRequest& request);
    ErrorCode handleHeartbeat(const protocol::HeartbeatRequest& request);
    ErrorCode validateTxnOffsetCommit(const std::string& group_id, int32_t generation_id,
                                      const std::string& member_id,
                                      const std::optional<std::string>& group_instance_id) const;
    ErrorCode handleLeaveGroup(const std::string& group_id, const std::string& member_id);

    std::vector<protocol::OffsetCommitResponse::Topic> handleOffsetCommit(
        const protocol::OffsetCommitRequest& request, ErrorCode& overall_error);

    std::vector<protocol::OffsetFetchResponse::Topic> handleOffsetFetch(
        const protocol::OffsetFetchRequest& request, ErrorCode& overall_error) const;

    std::vector<protocol::DescribeGroupsResponse::Group> describeGroups(
        const std::vector<std::string>& group_ids) const;

    std::vector<protocol::ListGroupsResponse::Group> listGroups() const;

    /// @brief Loads group state from persistent storage on startup.
    /// Should be called after initialization to restore groups.
    void loadGroupsFromStorage();

    /// @brief Starts the background cleanup thread for expired groups.
    void startCleanupThread();

    /// @brief Stops the background cleanup thread.
    void stopCleanupThread();

    /// @brief Sets the group retention period in milliseconds.
    /// Groups with no activity for this period will be deleted.
    /// @param retention_ms Retention period (default: 7 days)
    void setGroupRetentionMs(int64_t retention_ms) { group_retention_ms_ = retention_ms; }

    /// @brief Gets the configured group retention period.
    int64_t getGroupRetentionMs() const { return group_retention_ms_; }

    /// @brief Sets the member timeout period in milliseconds.
    /// Members that haven't sent a heartbeat for this period will be evicted.
    /// @param timeout_ms Timeout period (default: 30 seconds)
    void setMemberTimeoutMs(int64_t timeout_ms) { member_timeout_ms_ = timeout_ms; }

    /// @brief Gets the configured member timeout period.
    int64_t getMemberTimeoutMs() const { return member_timeout_ms_; }

    /// @brief Computes and records consumer lag for all groups.
    /// Should be called periodically (e.g., every 10 seconds).
    void computeAndRecordConsumerLag();

    /// @brief EX-12: forces a fresh rebalance for any group stuck in
    /// Preparing/CompletingRebalance past its rebalance_timeout deadline.
    /// Evicts the unresponsive leader so a stalled (e.g. multi-worker
    /// Connect) leader cannot hang followers indefinitely. Called from the
    /// cleanup thread; public so unit tests can drive it deterministically.
    void checkRebalanceTimeouts();

    /// @brief Phase EX-1 (§6.3): Prometheus metrics snapshot.
    struct GroupMetric {
        std::string group_id;
        std::string state;
        int64_t rebalances_total;
    };
    struct Metrics {
        std::vector<GroupMetric> groups;
        int64_t member_timeout_total;
    };
    Metrics getMetrics() const;

private:
    friend struct CoordinatorAcquisitionProbe;
    void installCoordinatorPartition(int32_t partition, int32_t partition_count,
                                     const std::vector<GroupRecord>& records,
                                     std::shared_ptr<OffsetManager> offsets, bool preserve_runtime,
                                     const GroupProposal* proposal = nullptr);
    struct MemberState {
        std::string member_id;
        // 0A.10: track real client identity instead of the "unknown" placeholder
        // that previously appeared in DescribeGroups responses.
        std::string client_id;
        std::string client_host;
        // Phase 2.2: KIP-345 static membership. When a client supplies a
        // group.instance.id, the coordinator remembers the (instance_id →
        // member_id) mapping. On rejoin with the same instance_id, the
        // member_id is preserved across short disconnects so a rolling
        // restart doesn't trigger a rebalance.
        std::optional<std::string> group_instance_id;
        std::vector<uint8_t> metadata;
        std::vector<uint8_t> assignment;
        std::chrono::steady_clock::time_point last_heartbeat;
    };

    struct OffsetKey {
        std::string topic;
        int32_t partition = 0;

        bool operator==(const OffsetKey& other) const {
            return partition == other.partition && topic == other.topic;
        }
    };

    struct OffsetKeyHasher {
        size_t operator()(const OffsetKey& key) const {
            std::hash<std::string> hash_str;
            std::hash<int32_t> hash_int;
            return hash_str(key.topic) ^ (hash_int(key.partition) << 1);
        }
    };

    struct OffsetValue {
        int64_t offset = -1;
        std::string metadata;
    };

    // Phase 2.2: explicit consumer group state machine. Mirrors Kafka's
    // `GroupState` enum so DescribeGroups can report a meaningful state
    // instead of always "Stable", and so rebalance decisions can be
    // gated on transitions.
    enum class GroupStateKind {
        Empty,                // No members; created or all evicted
        PreparingRebalance,   // At least one member is rejoining
        CompletingRebalance,  // Members have rejoined, awaiting leader sync
        Stable,               // Steady state: leader has synced an assignment
        Dead                  // Marked for cleanup (no recovery)
    };

    static const char* stateKindName(GroupStateKind k) {
        switch (k) {
            case GroupStateKind::Empty:
                return "Empty";
            case GroupStateKind::PreparingRebalance:
                return "PreparingRebalance";
            case GroupStateKind::CompletingRebalance:
                return "CompletingRebalance";
            case GroupStateKind::Stable:
                return "Stable";
            case GroupStateKind::Dead:
                return "Dead";
        }
        return "Unknown";
    }

    struct GroupState {
        int32_t generation_id = 0;  // 0 means uninitialized, will be set to 1 on first member join
        GroupStateKind kind = GroupStateKind::Empty;
        std::string protocol_type;
        std::string protocol_name;
        std::string leader_id;
        std::unordered_map<std::string, MemberState> members;
        std::chrono::system_clock::time_point last_activity;  // For expiration tracking
        // EX-12: rebalance-timeout enforcement. rebalance_timeout_ms is the
        // max client-supplied rebalance.timeout.ms across members; the
        // deadline is (rebalance_started_at + rebalance_timeout_ms). If a
        // group lingers in Preparing/CompletingRebalance past the deadline
        // (e.g. a stalled leader never sends SyncGroup), the cleanup thread
        // evicts the leader and forces a fresh rebalance. steady_clock so
        // it is immune to wall-clock adjustments.
        int32_t rebalance_timeout_ms = 0;  // 0 = unset; first joiner sets it
        std::chrono::steady_clock::time_point rebalance_started_at;
        // Phase EX-1: per-group rebalance counter (incremented on every
        // generation bump). Atomic so getMetrics() can read without
        // holding the coordinator mutex.
        std::atomic<int64_t> rebalances_total{0};

        GroupState() = default;
        GroupState(const GroupState& other)
            : generation_id(other.generation_id),
              kind(other.kind),
              protocol_type(other.protocol_type),
              protocol_name(other.protocol_name),
              leader_id(other.leader_id),
              members(other.members),
              last_activity(other.last_activity),
              rebalance_timeout_ms(other.rebalance_timeout_ms),
              rebalance_started_at(other.rebalance_started_at),
              rebalances_total(other.rebalances_total.load()) {}
        GroupState& operator=(const GroupState& other) {
            if (this != &other) {
                generation_id = other.generation_id;
                kind = other.kind;
                protocol_type = other.protocol_type;
                protocol_name = other.protocol_name;
                leader_id = other.leader_id;
                members = other.members;
                last_activity = other.last_activity;
                rebalance_timeout_ms = other.rebalance_timeout_ms;
                rebalance_started_at = other.rebalance_started_at;
                rebalances_total.store(other.rebalances_total.load());
            }
            return *this;
        }
        // Note: committed_offsets removed - now stored in OffsetManager
    };

    explicit GroupCoordinator(bool proposal_only);
    static GroupState restoreGroup(const GroupRecord& record, const GroupState* previous);
    static GroupRecord snapshotGroup(const std::string& group_id, const GroupState& group);

    GroupState* findGroup(const std::string& group_id);
    const GroupState* findGroup(const std::string& group_id) const;

    /// @brief Persists group state to storage.
    void persistGroupState(const std::string& group_id, const GroupState& group);

    /// @brief Cleanup thread function that periodically removes expired groups.
    void cleanupExpiredGroups();

    /// @brief Checks and removes timed-out members from groups.
    void checkMemberTimeouts();

    static std::vector<uint8_t> selectMetadata(const protocol::JoinGroupRequest& request,
                                               const std::string& protocol_name);

    std::shared_ptr<OffsetManager> offset_manager_;
    storage::LogManager* log_manager_;  ///< For accessing log end offsets (optional)
    std::shared_ptr<monitoring::MetricsCollector>
        metrics_collector_;  ///< For recording metrics (optional)
    mutable std::mutex mutex_;
    std::unordered_map<std::string, GroupState> groups_;
    std::vector<GroupRecord> pending_coordinator_offsets_;
    // IDs may be reserved by failed proposals; sharing the allocator prevents
    // concurrent proposals for different partitions from reusing an ID.
    std::shared_ptr<std::atomic<int64_t>> member_sequence_ =
        std::make_shared<std::atomic<int64_t>>(0);
    bool proposal_only_ = false;

    // Group expiration
    int64_t group_retention_ms_;  ///< Group retention period (default: 7 days)
    int64_t member_timeout_ms_;   ///< Member timeout period (default: 30 seconds)
    std::atomic<bool> cleanup_running_{false};
    std::thread cleanup_thread_;
    std::condition_variable cleanup_cv_;
    std::mutex cleanup_mutex_;

    // Phase EX-1 metrics.
    std::atomic<int64_t> member_timeout_total_{0};
};

// Restricted facade: proposals can only run membership/timeout transitions for
// their selected group. They have no storage, cleanup thread or live cache access.
class GroupCoordinator::GroupProposal {
public:
    GroupProposal(const GroupProposal&) = delete;
    GroupProposal& operator=(const GroupProposal&) = delete;
    GroupProposal(GroupProposal&&) = delete;
    GroupProposal& operator=(GroupProposal&&) = delete;
    JoinGroupResult handleJoinGroup(const protocol::JoinGroupRequest& request,
                                    const std::string& client_id = {},
                                    const std::string& client_host = {});
    SyncGroupResult handleSyncGroup(const protocol::SyncGroupRequest& request);
    ErrorCode handleHeartbeat(const protocol::HeartbeatRequest& request);
    ErrorCode handleLeaveGroup(const std::string& member_id);
    void checkTimeouts();
    std::optional<GroupRecord> record() const;

private:
    friend class GroupCoordinator;
    GroupProposal(const GroupCoordinator* owner, std::string group_id,
                  std::unique_ptr<GroupCoordinator> draft);
    void requireGroup(const std::string& group_id) const;
    const GroupCoordinator* owner_;
    std::string group_id_;
    std::unique_ptr<GroupCoordinator> draft_;
    mutable bool published_ = false;
};

}  // namespace kawasan::broker
