#pragma once

#include <map>
#include <mutex>
#include <string>
#include <vector>

#include "kawasan/broker/metadata_types.h"
#include "kawasan/storage/log_manager.h"

namespace kawasan::broker {

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
    TopicOperationResult applyUpdateISR(const std::string& topic_name,
                                       PartitionId partition_id,
                                       const std::vector<BrokerId>& isr);

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
    std::vector<TopicMetadata> describeTopics(
        const std::vector<std::string>& topic_names) const;

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
    std::vector<BrokerMetadata> brokers_;
    std::map<std::string, TopicState, std::less<>> topics_;
};

}  // namespace kawasan::broker
