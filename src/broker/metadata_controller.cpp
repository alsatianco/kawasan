#include "kawasan/broker/metadata_controller.h"

#include <chrono>
#include <nlohmann/json.hpp>

#include "kawasan/common/error.h"
#include "kawasan/common/logger.h"

namespace kawasan::broker {

namespace {
constexpr std::chrono::seconds REPLICATE_TIMEOUT{30};
}  // namespace

MetadataController::MetadataController(std::string metadata_dir, std::string cluster_id,
                                       const BrokerMetadata& local_broker,
                                       storage::LogManager* log_manager, raft::RaftNode* raft_node)
    : store_(std::move(metadata_dir), std::move(cluster_id), local_broker, log_manager),
      raft_node_(raft_node) {}

MetadataController::~MetadataController() {
    stop();
}

void MetadataController::start() {
    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        stopped_ = false;
    }
    store_.load();
    if (raft_node_ && raft_node_->lastLogIndex() < store_.appliedIndex()) {
        // The Raft log was replaced (e.g. its directory was wiped): its indexes
        // restart, so the store's applied index no longer refers to it.
        Logger::warn("Raft log ends at index {} but metadata was applied up to {}; resetting the "
                     "applied index (Raft state was lost)",
                     raft_node_->lastLogIndex(), store_.appliedIndex());
        store_.resetAppliedIndex(raft_node_->lastLogIndex());
    }
    installCommitCallback();
}

void MetadataController::stop() {
    std::lock_guard<std::mutex> lock(pending_mutex_);
    stopped_ = true;
    for (auto& [_, promise] : pending_) {
        promise.set_value(
            TopicOperationResult::failure(ErrorCode::BROKER_NOT_AVAILABLE, "Broker shutting down"));
    }
    pending_.clear();
    completed_.clear();
    abandoned_.clear();
    if (raft_node_ && commit_callback_installed_) {
        raft_node_->setCommitCallback(nullptr);
        commit_callback_installed_ = false;
    }
}

TopicOperationResult MetadataController::createTopic(const TopicSpecification& spec) {
    MetadataCommand command;
    command.type = MetadataCommandType::CREATE_TOPIC;
    command.topic_spec = spec;
    if (command.topic_spec.topic_id == std::array<uint8_t, 16>{}) {
        command.topic_spec.topic_id = newTopicId();
    }
    // M8-A2: in a cluster the controller decides the assignment and ships it in
    // the replicated command, so every broker applies identical leadership even
    // if their broker lists ever differ. Single-node commands stay unchanged.
    auto& stamped = command.topic_spec;
    const auto known = store_.brokers();
    if (stamped.assignments.empty() && known.size() > 1 && stamped.replication_factor > 0 &&
        stamped.replication_factor <= static_cast<int16_t>(known.size())) {
        std::vector<BrokerId> ids;
        ids.reserve(known.size());
        for (const auto& broker : known) {
            ids.push_back(broker.id);
        }
        stamped.assignments = roundRobinAssignments(std::move(ids), 0, stamped.num_partitions,
                                                    stamped.replication_factor);
    }
    return replicateAndAwait(command);
}

TopicOperationResult MetadataController::deleteTopic(const std::string& topic_name) {
    MetadataCommand command;
    command.type = MetadataCommandType::DELETE_TOPIC;
    command.topic_name = topic_name;
    return replicateAndAwait(command);
}

TopicOperationResult MetadataController::updatePartitionISR(const std::string& topic,
                                                            PartitionId partition,
                                                            const std::vector<BrokerId>& isr,
                                                            int32_t expected_partition_epoch) {
    MetadataCommand command;
    command.type = MetadataCommandType::UPDATE_ISR;
    command.topic_name = topic;
    command.partition_id = partition;
    command.isr = isr;
    command.expected_partition_epoch = expected_partition_epoch;
    return replicateAndAwait(command);
}

TopicOperationResult MetadataController::updatePartitionLeader(const std::string& topic,
                                                               PartitionId partition,
                                                               BrokerId leader,
                                                               int32_t expected_partition_epoch) {
    MetadataCommand command;
    command.type = MetadataCommandType::UPDATE_LEADER;
    command.topic_name = topic;
    command.partition_id = partition;
    command.leader = leader;
    command.expected_partition_epoch = expected_partition_epoch;
    return replicateAndAwait(command);
}

TopicOperationResult MetadataController::increasePartitions(const std::string& topic_name,
                                                            int32_t new_total_count) {
    MetadataCommand command;
    command.type = MetadataCommandType::INCREASE_PARTITIONS;
    command.topic_name = topic_name;
    command.new_partition_count = new_total_count;
    return replicateAndAwait(command);
}

TopicOperationResult MetadataController::replicateAndAwait(const MetadataCommand& command) {
    if (!raft_node_) {
        return applyCommand(command);
    }

    if (!raft_node_->isLeader()) {
        return TopicOperationResult::failure(ErrorCode::NOT_CONTROLLER,
                                             "This broker is not the active controller");
    }

    int64_t log_index = 0;
    try {
        auto payload = serializeCommand(command);
        std::future<int64_t> append_future = raft_node_->appendCommand(payload, "metadata");
        log_index = append_future.get();
    } catch (const std::exception& ex) {
        TopicOperationResult result;
        result.error_code = ErrorCode::NOT_CONTROLLER;
        result.error_message = ex.what();
        return result;
    }

    std::promise<TopicOperationResult> promise;
    auto future = promise.get_future();
    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        auto completed = completed_.find(log_index);
        if (completed != completed_.end()) {
            auto result = completed->second;
            completed_.erase(completed);
            promise.set_value(result);
            return future.get();
        }
        // stop() may have already run while we were waiting for the append;
        // a promise registered after it would never be fulfilled and would
        // hang shutdown (the bootstrap thread is joined in KawasanBroker::stop).
        if (stopped_) {
            return TopicOperationResult::failure(ErrorCode::BROKER_NOT_AVAILABLE,
                                                 "Broker shutting down");
        }
        pending_.emplace(log_index, std::move(promise));
    }
    // Bounded: without a quorum the entry may never commit, and callers run on
    // request-handling threads.
    if (future.wait_for(REPLICATE_TIMEOUT) == std::future_status::ready) {
        return future.get();
    }
    std::lock_guard<std::mutex> lock(pending_mutex_);
    auto it = pending_.find(log_index);
    if (it == pending_.end()) {
        // Fulfilled between the timeout and taking the lock.
        return future.get();
    }
    pending_.erase(it);
    abandoned_.insert(log_index);
    return TopicOperationResult::failure(ErrorCode::REQUEST_TIMED_OUT,
                                         "Timed out waiting for metadata commit");
}

TopicOperationResult MetadataController::applyCommand(const MetadataCommand& command) {
    switch (command.type) {
        case MetadataCommandType::CREATE_TOPIC:
            return store_.applyCreate(command.topic_spec);
        case MetadataCommandType::DELETE_TOPIC:
            return store_.applyDelete(command.topic_name);
        case MetadataCommandType::UPDATE_ISR:
            return store_.applyUpdateISR(command.topic_name, command.partition_id, command.isr,
                                         command.expected_partition_epoch);
        case MetadataCommandType::INCREASE_PARTITIONS:
            return store_.applyIncreasePartitions(command.topic_name, command.new_partition_count);
        case MetadataCommandType::UPDATE_LEADER:
            return store_.applyUpdateLeader(command.topic_name, command.partition_id,
                                            command.leader, command.expected_partition_epoch);
    }
    TopicOperationResult unknown;
    unknown.error_code = ErrorCode::INVALID_REQUEST;
    unknown.error_message = "Unknown metadata command";
    return unknown;
}

std::vector<uint8_t> MetadataController::serializeCommand(const MetadataCommand& command) {
    nlohmann::json json;
    if (command.type == MetadataCommandType::CREATE_TOPIC) {
        json["type"] = "create";
        json["topic"]["name"] = command.topic_spec.name;
        json["topic"]["topic_id"] = command.topic_spec.topic_id;
        json["topic"]["num_partitions"] = command.topic_spec.num_partitions;
        json["topic"]["replication_factor"] = command.topic_spec.replication_factor;
        json["topic"]["assignments"] = command.topic_spec.assignments;
        json["topic"]["configs"] = command.topic_spec.configs;
    } else if (command.type == MetadataCommandType::DELETE_TOPIC) {
        json["type"] = "delete";
        json["topic_name"] = command.topic_name;
    } else if (command.type == MetadataCommandType::UPDATE_ISR) {
        json["type"] = "update_isr";
        json["topic_name"] = command.topic_name;
        json["partition_id"] = command.partition_id;
        json["isr"] = command.isr;
    } else if (command.type == MetadataCommandType::INCREASE_PARTITIONS) {
        json["type"] = "increase_partitions";
        json["topic_name"] = command.topic_name;
        json["new_partition_count"] = command.new_partition_count;
    } else if (command.type == MetadataCommandType::UPDATE_LEADER) {
        json["type"] = "update_leader";
        json["topic_name"] = command.topic_name;
        json["partition_id"] = command.partition_id;
        json["leader"] = command.leader;
    }
    if (command.type == MetadataCommandType::UPDATE_ISR ||
        command.type == MetadataCommandType::UPDATE_LEADER) {
        json["expected_partition_epoch"] = command.expected_partition_epoch;
    }
    auto dump = json.dump();
    return std::vector<uint8_t>(dump.begin(), dump.end());
}

MetadataCommand MetadataController::deserializeCommand(const std::vector<uint8_t>& bytes) {
    auto json = nlohmann::json::parse(bytes.begin(), bytes.end());
    MetadataCommand command;
    auto type = json.at("type").get<std::string>();
    if (type == "create") {
        command.type = MetadataCommandType::CREATE_TOPIC;
        auto topic = json.at("topic");
        command.topic_spec.name = topic.at("name").get<std::string>();
        if (topic.contains("topic_id")) {
            const auto& id = topic.at("topic_id");
            if (!id.is_array() || id.size() != 16) {
                throw std::runtime_error("Invalid topic ID in Raft create command");
            }
            for (size_t i = 0; i < 16; ++i) {
                if (!id[i].is_number_integer() || id[i].get<int64_t>() < 0 ||
                    id[i].get<int64_t>() > 255) {
                    throw std::runtime_error("Invalid topic ID byte in Raft create command");
                }
                command.topic_spec.topic_id[i] = id[i].get<uint8_t>();
            }
            if (command.topic_spec.topic_id == std::array<uint8_t, 16>{}) {
                throw std::runtime_error("Zero topic ID in Raft create command");
            }
        }
        command.topic_spec.num_partitions = topic.at("num_partitions").get<int32_t>();
        command.topic_spec.replication_factor = topic.at("replication_factor").get<int16_t>();
        command.topic_spec.assignments =
            topic.value("assignments", std::vector<std::vector<BrokerId>>{});
        command.topic_spec.configs = topic.value("configs", std::map<std::string, std::string>{});
    } else if (type == "delete") {
        command.type = MetadataCommandType::DELETE_TOPIC;
        command.topic_name = json.at("topic_name").get<std::string>();
    } else if (type == "update_isr") {
        command.type = MetadataCommandType::UPDATE_ISR;
        command.topic_name = json.at("topic_name").get<std::string>();
        command.partition_id = json.at("partition_id").get<PartitionId>();
        command.isr = json.at("isr").get<std::vector<BrokerId>>();
    } else if (type == "increase_partitions") {
        command.type = MetadataCommandType::INCREASE_PARTITIONS;
        command.topic_name = json.at("topic_name").get<std::string>();
        command.new_partition_count = json.at("new_partition_count").get<int32_t>();
    } else if (type == "update_leader") {
        command.type = MetadataCommandType::UPDATE_LEADER;
        command.topic_name = json.at("topic_name").get<std::string>();
        command.partition_id = json.at("partition_id").get<PartitionId>();
        command.leader = json.at("leader").get<BrokerId>();
    }
    command.expected_partition_epoch = json.value("expected_partition_epoch", -1);
    return command;
}

void MetadataController::installCommitCallback() {
    if (!raft_node_ || commit_callback_installed_) {
        return;
    }
    raft_node_->setCommitCallback([this](const raft::LogEntry& entry) { handleCommit(entry); });
    commit_callback_installed_ = true;
}

void MetadataController::handleCommit(const raft::LogEntry& entry) {
    // A new Raft leader's no-op carries no command. Raft does not persist its
    // commit/applied index, so after a restart every
    // committed entry is delivered again. Entries already reflected in the
    // persisted store must not re-apply (UPDATE_LEADER is not idempotent, and a
    // replayed DELETE_TOPIC would delete a re-created topic's data).
    if (entry.command_type == "noop" || entry.index <= store_.appliedIndex()) {
        fulfillPending(entry.index, TopicOperationResult{});
        return;
    }
    try {
        auto command = deserializeCommand(entry.data);
        store_.setAppliedIndex(entry.index);
        auto result = applyCommand(command);
        store_.persistAppliedIndex();

        // Compute and log metadata checksum after applying command
        std::string checksum = store_.computeChecksum();
        Logger::info("Metadata commit at index {}: checksum={}", entry.index, checksum);

        fulfillPending(entry.index, result);
    } catch (const std::exception& ex) {
        Logger::error("Failed to apply metadata command at index {}: {}", entry.index, ex.what());
        TopicOperationResult failure;
        failure.error_code = ErrorCode::KAFKA_STORAGE_ERROR;
        failure.error_message = ex.what();
        fulfillPending(entry.index, failure);
    }
}

void MetadataController::fulfillPending(int64_t index, TopicOperationResult result) {
    std::lock_guard<std::mutex> lock(pending_mutex_);
    auto it = pending_.find(index);
    if (it != pending_.end()) {
        it->second.set_value(result);
        pending_.erase(it);
        return;
    }
    if (abandoned_.erase(index) > 0) {
        return;
    }
    completed_[index] = std::move(result);
}

}  // namespace kawasan::broker
