#pragma once

#include <atomic>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "kawasan/common/types.h"
#include "kawasan/storage/log.h"

namespace kawasan::broker {

// Forward declarations
class KawasanBroker;

/// @brief Manages replicas for partitions on this broker
/// @details In single-node mode, this broker is always the leader for all replicas.
///          In multi-broker mode, tracks leader/follower status, ISR membership,
///          and coordinates replication via follower fetching.
class ReplicaManager {
public:
    ReplicaManager();
    ~ReplicaManager();

    // Non-copyable/movable
    ReplicaManager(const ReplicaManager&) = delete;
    ReplicaManager& operator=(const ReplicaManager&) = delete;
    ReplicaManager(ReplicaManager&&) = delete;
    ReplicaManager& operator=(ReplicaManager&&) = delete;

    /// @brief Adds a replica (topic-partition) to be managed
    /// @param tp The topic-partition
    /// @param log The log for this partition
    void addReplica(const TopicPartition& tp, std::shared_ptr<storage::Log> log);

    /// @brief Removes a replica from management
    /// @param tp The topic-partition
    void removeReplica(const TopicPartition& tp);

    /// @brief Checks if this broker is the leader for a partition
    /// @param tp The topic-partition
    /// @return true if this broker is the leader (always true in single-node mode)
    bool isLeader(const TopicPartition& tp) const;

    /// @brief Gets the leader broker ID for a partition
    /// @param tp The topic-partition
    /// @return The leader broker ID, or nullopt if partition not found
    std::optional<BrokerId> getLeader(const TopicPartition& tp) const;

    /// @brief Gets the high watermark for a partition
    /// @param tp The topic-partition
    /// @return The high watermark offset, or nullopt if partition not found
    std::optional<Offset> getHighWatermark(const TopicPartition& tp) const;

    /// @brief Updates the high watermark for a partition
    /// @param tp The topic-partition
    /// @param hw The new high watermark
    void updateHighWatermark(const TopicPartition& tp, Offset hw);

    /// @brief Gets the leader epoch for a partition (KIP-101). Starts at 0 when
    /// the replica is first added and increments on each leadership change, so
    /// clients can detect stale leadership and request log truncation points.
    /// @return the current leader epoch, or nullopt if the partition isn't managed
    std::optional<int32_t> getLeaderEpoch(const TopicPartition& tp) const;

    /// @brief Increments the leader epoch for a partition (call on a leadership
    /// change — election win or reassignment). Returns the new epoch, or nullopt
    /// if the partition isn't managed.
    std::optional<int32_t> bumpLeaderEpoch(const TopicPartition& tp);

    /// @brief Recomputes and advances the high watermark from the ISR: the HW is
    /// the minimum of the leader's log-end-offset and every in-sync follower's
    /// last fetched offset. With only the leader in the ISR (single-node) this is
    /// exactly the leader's LEO, so single-node behavior is unchanged. The HW
    /// never moves backward. Returns the (possibly advanced) high watermark.
    Offset maybeAdvanceHighWatermark(const TopicPartition& tp);

    /// @brief The offset replicated to all in-sync replicas: min(leader LEO,
    /// every in-sync follower's last fetched offset). This is the offset an
    /// acks=all produce must wait for. With only the leader in the ISR it equals
    /// the leader's LEO (so acks=all returns immediately). Returns nullopt if the
    /// partition isn't managed. Unlike the high watermark, this is computed live
    /// from ISR state and is not affected by the log's own HW bookkeeping.
    std::optional<Offset> isrCommittedOffset(const TopicPartition& tp) const;

    /// @brief Gets the In-Sync Replicas for a partition
    /// @param tp The topic-partition
    /// @return Vector of broker IDs in the ISR
    std::vector<BrokerId> getISR(const TopicPartition& tp) const;

    /// @brief Updates the ISR for a partition
    /// @param tp The topic-partition
    /// @param isr Vector of broker IDs that should be in the ISR
    void updateISR(const TopicPartition& tp, const std::vector<BrokerId>& isr);

    /// @brief Gets the log for a partition
    /// @param tp The topic-partition
    /// @return Shared pointer to the log, or nullptr if not found
    std::shared_ptr<storage::Log> getLog(const TopicPartition& tp) const;

    /// @brief Gets all managed topic-partitions
    /// @return Vector of all topic-partitions
    std::vector<TopicPartition> getAllReplicas() const;

    /// @brief Sets the broker reference for sending fetch requests
    /// @param broker Pointer to the KawasanBroker instance
    void setBroker(KawasanBroker* broker);

    /// @brief M4: sets this broker's own id. Must be called before any replica is
    /// registered, so `addReplica`'s self-leader/self-ISR seeding and the
    /// leader-side follower-offset check (`leader == local_broker_id_`) use the
    /// real `broker.id` rather than the hardcoded 0. Single-node behavior is
    /// unchanged because leader and local id stay equal.
    void setLocalBrokerId(BrokerId id) { local_broker_id_ = id; }

    /// @brief M4: returns this broker's own id (the ISR/leader identity).
    BrokerId localBrokerId() const { return local_broker_id_; }

    /// @brief Starts the follower fetch thread
    /// @details Begins background fetching from leaders for follower replicas
    void start();

    /// @brief Stops the follower fetch thread
    /// @details Gracefully stops the background thread
    void stop();

    /// @brief Checks if the replica manager is running
    /// @return true if the follower fetch thread is running
    bool isRunning() const { return running_; }

    /// @brief Sets the maximum allowed lag in messages before a replica is removed from ISR
    /// @param max_lag Maximum lag in number of messages (default: 10000)
    void setMaxReplicaLag(int64_t max_lag) { max_replica_lag_messages_ = max_lag; }

    /// @brief Gets the maximum allowed replica lag
    /// @return Maximum lag in messages
    int64_t getMaxReplicaLag() const { return max_replica_lag_messages_; }

    /// @brief Updates the last fetched offset for a follower replica
    /// @param tp The topic-partition
    /// @param broker_id The follower broker ID
    /// @param offset The last fetched offset
    void updateFollowerFetchOffset(const TopicPartition& tp, BrokerId broker_id, Offset offset);

    /// @brief Checks and updates ISR based on replica lag
    /// @details Called by leader to check if followers are keeping up
    /// @return true if ISR was modified
    bool checkAndUpdateISR(const TopicPartition& tp);

    /// @brief Gets the lag for a follower replica
    /// @param tp The topic-partition
    /// @param broker_id The follower broker ID
    /// @return The lag in messages, or nullopt if not found
    std::optional<int64_t> getFollowerLag(const TopicPartition& tp, BrokerId broker_id) const;

private:
    /// @brief Information about a follower replica's replication state
    struct FollowerState {
        Offset last_caught_up_offset = 0;  // Last offset where follower was caught up
        Offset last_fetched_offset = 0;    // Last offset fetched by this follower
        int64_t last_update_time_ms = 0;   // Timestamp of last update
    };

    /// @brief Information about a replica
    struct ReplicaInfo {
        std::shared_ptr<storage::Log> log;
        BrokerId leader;            // Leader broker ID (always this broker in single-node mode)
        std::vector<BrokerId> isr;  // In-Sync Replicas
        Offset fetch_offset = 0;    // Last fetched offset (for follower replicas)
        int32_t leader_epoch = 0;   // KIP-101: bumped on each leadership change

        // Leader-side tracking of follower states (only used when this broker is leader)
        std::map<BrokerId, FollowerState> follower_states;
    };

    /// @brief Computes the ISR-derived high watermark for a replica (caller holds
    /// mutex_). HW = min(leader LEO, min in-sync follower fetch offset). Followers
    /// in the ISR with no recorded fetch state hold the HW at the log start until
    /// they report progress, matching Kafka's "HW only advances past offsets all
    /// in-sync replicas have" rule.
    Offset computeHighWatermarkLocked(const ReplicaInfo& info) const;

    /// @brief Background thread for follower fetching
    void fetcherThreadLoop();

    /// @brief Fetch from leader for a single partition
    /// @param tp The topic-partition to fetch
    /// @param info The replica info
    void fetchFromLeader(const TopicPartition& tp, ReplicaInfo& info);

    mutable std::mutex mutex_;
    std::map<TopicPartition, ReplicaInfo> replicas_;
    BrokerId local_broker_id_ = 0;  // This broker's ID

    KawasanBroker* broker_ = nullptr;  // Reference to broker for sending fetch requests
    std::atomic<bool> running_{false};
    std::thread fetcher_thread_;

    // ISR management configuration
    int64_t max_replica_lag_messages_ = 10000;  // Max lag before removing from ISR
};

}  // namespace kawasan::broker
