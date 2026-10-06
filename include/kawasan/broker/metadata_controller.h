#pragma once

#include <future>
#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "kawasan/broker/metadata_store.h"
#include "kawasan/raft/raft_node.h"

namespace kawasan::broker {

/// @brief Coordinates metadata mutations via the Raft log and serves cached state.
class MetadataController {
public:
    MetadataController(std::string metadata_dir, std::string cluster_id,
                       const BrokerMetadata& local_broker, storage::LogManager* log_manager,
                       raft::RaftNode* raft_node);
    ~MetadataController();

    MetadataController(const MetadataController&) = delete;
    MetadataController& operator=(const MetadataController&) = delete;

    void configureCoordinatorFormat(const CoordinatorFormat& format, const std::string& log_dir) {
        store_.configureCoordinatorFormat(format, log_dir);
        format_configured_ = true;
    }
    TopicOperationResult declareCoordinatorFormat(const CoordinatorFormat& format);
    std::optional<CoordinatorFormat> coordinatorFormat() const {
        return store_.coordinatorFormat();
    }

    storage::Log* openCoordinatorReplica(const std::string& topic, PartitionId partition) {
        return store_.openCoordinatorReplica(topic, partition);
    }
    ErrorCode withPartitionLeadership(
        const TopicPartition& tp, BrokerId owner, int32_t epoch,
        const std::function<ErrorCode(const PartitionMetadata&)>& action) {
        return store_.withPartitionLeadership(tp, owner, epoch, action);
    }

    /// @brief Loads persisted metadata and installs commit hooks.
    void start();

    /// @brief Flushes pending promises so shutdown can proceed cleanly.
    void stop();

    TopicOperationResult createTopic(const TopicSpecification& spec);
    TopicOperationResult deleteTopic(const std::string& topic_name);

    TopicOperationResult alterTopicConfigs(const std::string& name,
                                           const std::vector<TopicConfigChange>& changes,
                                           bool replace, bool validate_only);
    std::optional<std::map<std::string, std::string>> topicConfigs(const std::string& name) const {
        return store_.topicConfigs(name);
    }

    /// @brief Updates the ISR for a partition (replicated via Raft).
    /// @param topic Topic name
    /// @param partition Partition ID
    /// @param isr New ISR list
    /// @return Operation result
    TopicOperationResult updatePartitionISR(const std::string& topic, PartitionId partition,
                                            const std::vector<BrokerId>& isr,
                                            int32_t expected_partition_epoch = -1);

    /// @brief M7: elect a new leader for a partition (Raft-replicated). Bumps the
    /// partition's leader_epoch. Only the active controller can commit.
    TopicOperationResult updatePartitionLeader(const std::string& topic, PartitionId partition,
                                               BrokerId leader,
                                               int32_t expected_partition_epoch = -1);

    /// @brief Phase 4.1c: increases a topic's partition count to a new total.
    TopicOperationResult increasePartitions(const std::string& topic_name, int32_t new_total_count);

    std::vector<TopicMetadata> describeTopics(const std::vector<std::string>& names) const {
        return store_.describeTopics(names);
    }

    std::optional<TopicMetadata> topicById(const std::array<uint8_t, 16>& id) const {
        return store_.topicById(id);
    }

    std::vector<BrokerMetadata> brokers() const { return store_.brokers(); }
    std::string clusterId() const { return store_.clusterId(); }

    /// @brief Updates the metadata entry for this broker (host/port changes).
    void updateLocalBroker(const BrokerMetadata& broker) { store_.updateLocalBroker(broker); }

    /// @brief Registers a cluster peer broker (idempotent), seeding membership
    /// from raft.peers so replica assignment can spread across brokers.
    void registerBroker(const BrokerMetadata& broker) { store_.registerBroker(broker); }

    /// @brief Number of brokers known to the cluster membership.
    size_t brokerCount() const { return store_.brokerCount(); }

    /// @brief Computes and returns a checksum of the current metadata state.
    std::string computeMetadataChecksum() const { return store_.computeChecksum(); }

private:
    TopicOperationResult replicateAndAwait(const MetadataCommand& command);
    TopicOperationResult applyCommand(const MetadataCommand& command);
    static std::vector<uint8_t> serializeCommand(const MetadataCommand& command);
    static MetadataCommand deserializeCommand(const std::vector<uint8_t>& bytes);
    void installCommitCallback();
    void handleCommit(const raft::LogEntry& entry);
    void fulfillPending(int64_t index, TopicOperationResult result);

    MetadataStore store_;
    raft::RaftNode* raft_node_;
    bool commit_callback_installed_ = false;

    mutable std::mutex pending_mutex_;
    std::unordered_map<int64_t, std::promise<TopicOperationResult>> pending_;
    std::unordered_map<int64_t, TopicOperationResult> completed_;
    // Indices whose waiter timed out; their eventual commit is dropped.
    std::unordered_set<int64_t> abandoned_;
    bool stopped_ = false;
    bool format_configured_ = false;
    std::atomic<bool> format_failed_{false};
};

}  // namespace kawasan::broker
