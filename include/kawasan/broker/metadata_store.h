#pragma once

#include <map>
#include <mutex>
#include <string>
#include <vector>

#include "kawasan/broker/metadata_types.h"
#include "kawasan/storage/log_manager.h"

namespace kawasan::broker {

/// @brief M8-A2: deterministic replica assignment for partitions
/// [first_partition, first_partition + count). Broker ids are sorted; partition
/// p's replicas start at index p % N and take the next RF brokers (RF clamped to
/// [1, N]); the first replica is the preferred leader. Depends only on its
/// inputs, so every broker computes the same assignment. Empty if no brokers.
std::vector<std::vector<BrokerId>> roundRobinAssignments(std::vector<BrokerId> broker_ids,
                                                         int32_t first_partition, int32_t count,
                                                         int16_t replication_factor);

/// @brief Owns the authoritative metadata cache and on-disk persistence.
class MetadataStore {
public:
    MetadataStore(std::string metadata_dir, std::string cluster_id,
                  const BrokerMetadata& local_broker, storage::LogManager* log_manager);

    MetadataStore(const MetadataStore&) = delete;
    MetadataStore& operator=(const MetadataStore&) = delete;

    /// @brief Loads metadata from disk (creates empty store if missing).
    void load();

    /// @brief Applies a create topic command.
    TopicOperationResult applyCreate(const TopicSpecification& spec);

    /// @brief Applies a delete topic command.
    TopicOperationResult applyDelete(const std::string& topic_name);

    /// @brief Applies an ISR update command.
    TopicOperationResult applyUpdateISR(const std::string& topic_name, PartitionId partition_id,
                                        const std::vector<BrokerId>& isr);

    /// @brief M7: applies a leader-election command — sets the partition leader
    /// (must be an assigned replica, or -1 = offline, M8) and bumps its
    /// leader_epoch. The ISR is untouched.
    TopicOperationResult applyUpdateLeader(const std::string& topic_name, PartitionId partition_id,
                                           BrokerId leader);

    /// @brief Phase 4.1c: increases a topic's partition count.
    /// @param new_total_count The new total partition count (Kafka semantics:
    ///        this is the absolute target, not a delta). Must be strictly
    ///        greater than the current count.
    /// @return INVALID_PARTITIONS if new_total_count is not greater than
    ///         the current count; UNKNOWN_TOPIC_OR_PARTITION if the topic
    ///         doesn't exist.
    TopicOperationResult applyIncreasePartitions(const std::string& topic_name,
                                                 int32_t new_total_count);

    /// @brief Returns topic metadata for the requested topics (all if empty).
    std::vector<TopicMetadata> describeTopics(const std::vector<std::string>& topic_names) const;

    /// @brief Returns known brokers.
    std::vector<BrokerMetadata> brokers() const;

    /// @brief Returns the stored cluster ID.
    std::string clusterId() const;

    /// @brief Updates the broker entry for this node (persists change).
    void updateLocalBroker(const BrokerMetadata& broker);

    /// @brief Registers (or updates) a cluster peer broker, keyed by id. Used to
    /// seed cluster membership from raft.peers so replica assignment can spread
    /// partitions across brokers and Metadata responses advertise the full
    /// cluster. Idempotent.
    void registerBroker(const BrokerMetadata& broker);

    /// @brief Number of brokers currently known to the cluster membership.
    size_t brokerCount() const;

    /// @brief Computes a checksum of all topics/partitions/configs.
    std::string computeChecksum() const;

    /// @brief Highest Raft log index whose command is reflected in this store
    /// (persisted atomically with the state; 0 = none / pre-tracking file).
    int64_t appliedIndex() const;

    /// @brief Records that the command at `index` is being applied. The next
    /// persist (the command's own, or persistAppliedIndex) writes it together
    /// with the resulting state, so a crash never separates the two.
    void setAppliedIndex(int64_t index);

    /// @brief Lowers the applied index (and persists it) — only for a replaced
    /// Raft log whose indexes restarted.
    void resetAppliedIndex(int64_t index);

    /// @brief Persists the applied index if the command did not persist itself
    /// (e.g. it was rejected by validation).
    void persistAppliedIndex();

private:
    struct TopicState {
        TopicMetadata metadata;
        std::map<std::string, std::string> configs;
    };

    TopicOperationResult validateCreateLocked(const TopicSpecification& spec) const;
    void ensureLocalBrokerLocked(const BrokerMetadata& broker);
    void persistLocked() const;

    std::string metadata_dir_;
    std::string metadata_file_;
    std::string cluster_id_;
    BrokerId local_broker_id_;
    BrokerMetadata local_broker_;
    storage::LogManager* log_manager_;

    mutable std::mutex mutex_;
    int64_t applied_index_ = 0;
    mutable int64_t persisted_applied_index_ = 0;
    std::vector<BrokerMetadata> brokers_;
    std::map<std::string, TopicState, std::less<>> topics_;
};

}  // namespace kawasan::broker
