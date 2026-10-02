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
class PeerClient;

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

    /// @brief A consumer-visible HW only after the current leadership's ISR
    /// confirms the inherited log tail. Until then a stale follower/checkpoint
    /// HW must not be exposed as a successful committed-offset observation.
    std::optional<Offset> readableHighWatermark(const TopicPartition& tp,
                                                int32_t leader_epoch) const;

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
    /// On a partition this broker leads, a shrink may advance the high watermark.
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

    /// @brief M5: upsert a replica's leader/ISR/epoch from the Raft-committed
    /// metadata. This is the single registration path used by the broker's
    /// metadata reconciliation: it registers the partition (if new) with the
    /// given role and, if it already exists, updates leader/ISR/epoch. Fetch
    /// progress is preserved unless leadership changed (M8-D): a demoted leader
    /// re-seeds fetch_offset to its log end, and any role flip or new epoch on
    /// the leader clears follower_states. A new partition we lead has its HW
    /// recomputed from the ISR (a sole replica commits its whole log). When
    /// `leader != local id` the partition is a FOLLOWER and the fetcher thread
    /// will replicate it from the leader. For a single-node RF=1 partition this
    /// registers leader=self / ISR={self}, identical to the lazy path.
    /// Returns true if the partition is new or its leader/ISR/epoch changed (the
    /// caller wakes requests parked on it). A changed ISR on a partition this
    /// broker leads may advance the high watermark.
    bool reconcileReplica(const TopicPartition& tp, std::shared_ptr<storage::Log> log,
                          BrokerId leader, const std::vector<BrokerId>& isr, int32_t leader_epoch);

    /// @brief The offset a follower replica will fetch from next (unused, 0, on
    /// a leader); nullopt if the partition is unmanaged.
    std::optional<Offset> getFetchOffset(const TopicPartition& tp) const;

    /// @brief M6: compute a proposed ISR for a partition THIS broker leads, from
    /// tracked follower progress. A current ISR member is dropped only once its
    /// last fetch has gone stale (older than `lag_ms`) — a member we have not yet
    /// heard from is kept (avoids churn right after assignment; a never-started
    /// broker is M8's concern). A follower not in the ISR is (re-)added once it is
    /// fetching recently AND has caught up to the leader's high watermark. Returns
    /// the proposed ISR (sorted, always including the leader) if it differs from
    /// the current ISR, else nullopt. nullopt too if this broker is not the leader
    /// or the partition is unmanaged. The caller commits the change through the
    /// controller (AlterPartition / UPDATE_ISR).
    std::optional<std::vector<BrokerId>> computeIsrUpdate(const TopicPartition& tp, int64_t lag_ms,
                                                          int64_t now_ms) const;

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
        // M8-E3: before fetching from this leader, reconcile the log tail with it
        // via OffsetForLeaderEpoch (set on becoming a follower, on a leader or
        // epoch change, and on an out-of-range / gap fetch).
        bool epoch_check_pending = false;
        Offset leadership_read_floor = 0;
        bool watermark_ready = false;

        // Leader-side tracking of follower states (only used when this broker is leader)
        std::map<BrokerId, FollowerState> follower_states;
    };

    /// @brief Computes the ISR-derived high watermark for a replica (caller holds
    /// mutex_). HW = min(leader LEO, min in-sync follower fetch offset). Followers
    /// in the ISR with no recorded fetch state hold the HW at the log start until
    /// they report progress, matching Kafka's "HW only advances past offsets all
    /// in-sync replicas have" rule.
    Offset computeHighWatermarkLocked(const ReplicaInfo& info) const;
    /// Raises the log's HW to computeHighWatermarkLocked (never lowers it).
    Offset maybeAdvanceHighWatermarkLocked(ReplicaInfo& info);

    /// @brief Background thread for follower fetching
    void fetcherThreadLoop();

    /// @brief M5: a follower-fetch unit of work snapshotted from replicas_ under
    /// the lock, so the blocking network fetch runs WITHOUT holding mutex_.
    struct FetchTask {
        TopicPartition tp;
        BrokerId leader;
        Offset fetch_offset;
        std::shared_ptr<storage::Log> log;
        int32_t leader_epoch = -1;
        bool epoch_check = false;
    };

    /// @brief M8-E3: truncate the follower's tail to where it diverges from the
    /// leader (KIP-101 via OffsetForLeaderEpoch, HW fallback without epoch
    /// history). Returns false if the leader could not answer (retry later);
    /// `reachable` is cleared if it could not be reached at all.
    bool reconcileWithLeader(const FetchTask& task, PeerClient& client, bool& reachable);

    /// @brief Marks/clears the epoch check for `task`'s partition — clearing only
    /// if the leader and epoch still match what was checked.
    void setEpochCheckPending(const FetchTask& task, bool pending);

    /// @brief M5: replicate one follower partition from its leader (network I/O,
    /// runs outside mutex_). Appends fetched batches offset-preserved, adopts the
    /// leader's high watermark, and writes the advanced fetch offset back into
    /// replicas_ under the lock.
    /// Returns false if the leader could not be reached (skip it this cycle).
    bool fetchPartitionFromLeader(const FetchTask& task);

    /// @brief M5: get (creating if needed) the cached PeerClient for a leader.
    /// Called only from the fetcher thread, so peer_clients_ needs no lock.
    PeerClient* peerClientFor(BrokerId leader, const std::string& host, int32_t port);

    mutable std::mutex mutex_;
    std::map<TopicPartition, ReplicaInfo> replicas_;
    BrokerId local_broker_id_ = 0;  // This broker's ID

    KawasanBroker* broker_ = nullptr;  // Reference to broker for sending fetch requests
    std::atomic<bool> running_{false};
    std::thread fetcher_thread_;
    int64_t fetcher_interval_ms_ = 100;  // follower fetch cadence
    // M5: one persistent connection per leader broker; fetcher-thread-only.
    std::map<BrokerId, std::unique_ptr<PeerClient>> peer_clients_;

    // ISR management configuration
    int64_t max_replica_lag_messages_ = 10000;  // Max lag before removing from ISR
};

}  // namespace kawasan::broker
