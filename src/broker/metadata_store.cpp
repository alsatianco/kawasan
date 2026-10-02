#include "kawasan/broker/metadata_store.h"

#include <algorithm>
#include <array>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <nlohmann/json.hpp>
#include <optional>
#include <random>
#include <sstream>
#include <utility>

#include "kawasan/common/error.h"
#include "kawasan/common/file_util.h"
#include "kawasan/common/logger.h"

namespace fs = std::filesystem;

namespace kawasan::broker {

std::vector<std::vector<BrokerId>> roundRobinAssignments(std::vector<BrokerId> broker_ids,
                                                         int32_t first_partition, int32_t count,
                                                         int16_t replication_factor) {
    std::vector<std::vector<BrokerId>> result;
    if (broker_ids.empty() || count <= 0) {
        return result;
    }
    std::sort(broker_ids.begin(), broker_ids.end());
    const size_t n = broker_ids.size();
    const size_t rf =
        std::clamp<size_t>(static_cast<size_t>(std::max<int16_t>(1, replication_factor)), 1, n);
    result.reserve(static_cast<size_t>(count));
    for (int32_t partition = first_partition; partition < first_partition + count; ++partition) {
        const size_t start = static_cast<size_t>(partition) % n;
        std::vector<BrokerId> replicas;
        replicas.reserve(rf);
        for (size_t i = 0; i < rf; ++i) {
            replicas.push_back(broker_ids[(start + i) % n]);
        }
        result.push_back(std::move(replicas));
    }
    return result;
}

namespace {

constexpr const char* kMetadataFileName = "topics.json";

// Phase 1.4 / 1.16: generate a deterministic per-process UUID for a new
// topic. Production deployments should persist this so the same topic
// re-creates with the same UUID, but for a single-process broker this
// suffices to give clients a stable handle within the lifetime of the
// process. RFC 4122 §4.1.2 type 4 (random) variant — we use a simple
// PRNG seeded once per process.
std::array<uint8_t, 16> generateTopicId() {
    static thread_local std::mt19937_64 rng{std::random_device{}()};
    std::array<uint8_t, 16> id{};
    for (int i = 0; i < 16; i += 8) {
        const uint64_t r = rng();
        for (int j = 0; j < 8; ++j) {
            id[i + j] = static_cast<uint8_t>((r >> (8 * j)) & 0xFF);
        }
    }
    // Set version (4) and variant (RFC 4122) bits per spec.
    id[6] = (id[6] & 0x0F) | 0x40;
    id[8] = (id[8] & 0x3F) | 0x80;
    return id;
}

TopicMetadata buildTopicMetadata(const TopicSpecification& spec,
                                 const std::vector<BrokerMetadata>& brokers,
                                 BrokerId local_broker_id) {
    TopicMetadata metadata;
    metadata.error_code = ErrorCode::NONE;
    metadata.name = spec.name;
    metadata.topic_id = generateTopicId();
    metadata.is_internal = false;
    metadata.partitions.reserve(static_cast<size_t>(spec.num_partitions));

    // If manual assignments are provided, use them
    if (!spec.assignments.empty()) {
        for (int32_t partition = 0; partition < spec.num_partitions; ++partition) {
            PartitionMetadata pm;
            pm.error_code = ErrorCode::NONE;
            pm.partition = partition;

            if (partition < static_cast<int32_t>(spec.assignments.size())) {
                pm.replicas = spec.assignments[partition];
            } else {
                // Fallback if assignments incomplete
                pm.replicas = {local_broker_id};
            }

            // First replica is the leader
            pm.leader = pm.replicas.empty() ? local_broker_id : pm.replicas[0];
            pm.leader_epoch = 0;
            // Initially, all replicas are in-sync
            pm.isr = pm.replicas;
            pm.offline_replicas.clear();
            metadata.partitions.push_back(std::move(pm));
        }
        return metadata;
    }

    // Single broker: everything is local (single-node stays byte-identical).
    if (brokers.size() <= 1) {
        for (int32_t partition = 0; partition < spec.num_partitions; ++partition) {
            PartitionMetadata pm;
            pm.error_code = ErrorCode::NONE;
            pm.partition = partition;
            pm.leader = local_broker_id;
            pm.leader_epoch = 0;
            pm.replicas = {local_broker_id};
            pm.isr = {local_broker_id};
            pm.offline_replicas.clear();
            metadata.partitions.push_back(std::move(pm));
        }
        return metadata;
    }

    // Multi-broker: deterministic round-robin (M8-A2), including RF=1 — the
    // result must not depend on which broker applies the command.
    std::vector<BrokerId> broker_ids;
    broker_ids.reserve(brokers.size());
    for (const auto& broker : brokers) {
        broker_ids.push_back(broker.id);
    }
    const auto assignments = roundRobinAssignments(std::move(broker_ids), 0, spec.num_partitions,
                                                   spec.replication_factor);
    for (int32_t partition = 0; partition < spec.num_partitions; ++partition) {
        PartitionMetadata pm;
        pm.error_code = ErrorCode::NONE;
        pm.partition = partition;
        pm.replicas = assignments[static_cast<size_t>(partition)];
        // First replica is the leader
        pm.leader = pm.replicas.front();
        pm.leader_epoch = 0;
        // Initially, all replicas are in-sync
        pm.isr = pm.replicas;
        pm.offline_replicas.clear();
        metadata.partitions.push_back(std::move(pm));
    }

    return metadata;
}

nlohmann::json partitionToJson(const PartitionMetadata& partition) {
    nlohmann::json j;
    j["partition"] = partition.partition;
    j["leader"] = partition.leader;
    j["leader_epoch"] = partition.leader_epoch;
    j["partition_epoch"] = partition.partition_epoch;
    j["replicas"] = partition.replicas;
    j["isr"] = partition.isr;
    j["offline_replicas"] = partition.offline_replicas;
    return j;
}

PartitionMetadata partitionFromJson(const nlohmann::json& j) {
    PartitionMetadata metadata;
    metadata.error_code = ErrorCode::NONE;
    metadata.partition = j.at("partition").get<int32_t>();
    metadata.leader = j.at("leader").get<int32_t>();
    metadata.leader_epoch = j.value("leader_epoch", 0);
    metadata.partition_epoch = j.value("partition_epoch", 0);
    metadata.replicas = j.at("replicas").get<std::vector<int32_t>>();
    metadata.isr = j.at("isr").get<std::vector<int32_t>>();
    metadata.offline_replicas = j.value("offline_replicas", std::vector<int32_t>{});
    return metadata;
}

}  // namespace

MetadataStore::MetadataStore(std::string metadata_dir, std::string cluster_id,
                             const BrokerMetadata& local_broker, storage::LogManager* log_manager)
    : metadata_dir_(std::move(metadata_dir)),
      metadata_file_(metadata_dir_ + "/" + kMetadataFileName),
      cluster_id_(std::move(cluster_id)),
      local_broker_id_(local_broker.id),
      local_broker_(local_broker),
      log_manager_(log_manager) {}

void MetadataStore::load() {
    std::lock_guard<std::mutex> lock(mutex_);
    fs::create_directories(metadata_dir_);

    if (!fs::exists(metadata_file_)) {
        brokers_.push_back(local_broker_);
        persistLocked();
        Logger::info("Bootstrapped empty metadata store at {}", metadata_file_);
        return;
    }

    std::ifstream in(metadata_file_);
    if (!in.is_open()) {
        throw KawasanException(ErrorCode::KAFKA_STORAGE_ERROR,
                               "Failed to open metadata file " + metadata_file_);
    }

    nlohmann::json json;
    in >> json;

    cluster_id_ = json.value("cluster_id", cluster_id_);
    applied_index_ = json.value("applied_index", int64_t{0});
    persisted_applied_index_ = applied_index_;
    brokers_.clear();
    if (json.contains("brokers")) {
        for (const auto& broker_json : json["brokers"]) {
            BrokerMetadata broker;
            broker.id = broker_json.at("id").get<int32_t>();
            broker.host = broker_json.at("host").get<std::string>();
            broker.port = broker_json.at("port").get<int32_t>();
            if (broker_json.contains("rack") && !broker_json["rack"].is_null()) {
                broker.rack = broker_json["rack"].get<std::string>();
            }
            brokers_.push_back(broker);
        }
    }
    ensureLocalBrokerLocked(local_broker_);

    topics_.clear();
    if (json.contains("topics")) {
        for (const auto& topic_json : json["topics"]) {
            TopicState state;
            state.metadata.error_code = ErrorCode::NONE;
            state.metadata.name = topic_json.at("name").get<std::string>();
            state.metadata.is_internal = topic_json.value("is_internal", false);
            state.metadata.partitions.clear();
            for (const auto& partition_json : topic_json.at("partitions")) {
                state.metadata.partitions.push_back(partitionFromJson(partition_json));
            }
            if (topic_json.contains("configs")) {
                state.configs = topic_json.at("configs").get<std::map<std::string, std::string>>();
            }
            topics_[state.metadata.name] = std::move(state);
        }
    }

    // 0A.4: re-register per-topic LogConfig overrides so logs created lazily
    // after restart inherit the same cleanup.policy that was used at create
    // time. Without this, a broker restart silently downgrades compacted topics
    // to delete-only retention.
    if (log_manager_) {
        for (const auto& [name, state] : topics_) {
            if (!state.configs.empty()) {
                log_manager_->setTopicConfig(name, storage::LogConfig::fromMap(state.configs));
            }
        }
    }

    Logger::info("Loaded {} topics from {}", topics_.size(), metadata_file_);
}

TopicOperationResult MetadataStore::applyCreate(const TopicSpecification& spec) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto validation = validateCreateLocked(spec);
    if (validation.error_code != ErrorCode::NONE) {
        return validation;
    }

    TopicState state;
    state.metadata = buildTopicMetadata(spec, brokers_, local_broker_id_);
    state.configs = spec.configs;
    topics_[spec.name] = state;

    // 0A.4: register the per-topic LogConfig (cleanup.policy, retention, etc.)
    // BEFORE creating the partition logs so the per-topic config takes effect.
    if (log_manager_) {
        log_manager_->setTopicConfig(spec.name, storage::LogConfig::fromMap(spec.configs));
        for (const auto& partition : state.metadata.partitions) {
            log_manager_->getOrCreateLog(spec.name, partition.partition);
        }
    }

    persistLocked();
    Logger::info("Created topic {} with {} partitions", spec.name, spec.num_partitions);

    TopicOperationResult result;
    result.error_code = ErrorCode::NONE;
    result.topic_metadata = state.metadata;
    result.has_metadata = true;
    return result;
}

TopicOperationResult MetadataStore::applyDelete(const std::string& topic_name) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = topics_.find(topic_name);
    if (it == topics_.end()) {
        return TopicOperationResult::failure(ErrorCode::UNKNOWN_TOPIC_OR_PARTITION,
                                             "Topic does not exist");
    }

    for (const auto& partition : it->second.metadata.partitions) {
        if (log_manager_) {
            log_manager_->deleteLog(topic_name, partition.partition);
        }
    }

    topics_.erase(it);
    persistLocked();
    Logger::info("Deleted topic {}", topic_name);

    TopicOperationResult result;
    result.error_code = ErrorCode::NONE;
    return result;
}

TopicOperationResult MetadataStore::applyIncreasePartitions(const std::string& topic_name,
                                                            int32_t new_total_count) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = topics_.find(topic_name);
    if (it == topics_.end()) {
        return TopicOperationResult::failure(ErrorCode::UNKNOWN_TOPIC_OR_PARTITION,
                                             "Topic does not exist");
    }

    auto& metadata = it->second.metadata;
    const int32_t current_count = static_cast<int32_t>(metadata.partitions.size());
    if (new_total_count <= current_count) {
        return TopicOperationResult::failure(
            ErrorCode::INVALID_PARTITIONS,
            "Topic already has at least the requested partition count");
    }

    // Build assignments for the new partitions. We follow Kafka semantics:
    // the new partitions get round-robin assignment starting from where the
    // existing assignments left off.
    std::vector<BrokerId> broker_ids;
    broker_ids.reserve(brokers_.size());
    for (const auto& broker : brokers_) {
        broker_ids.push_back(broker.id);
    }
    std::sort(broker_ids.begin(), broker_ids.end());

    const int16_t replication_factor =
        metadata.partitions.empty()
            ? static_cast<int16_t>(1)
            : static_cast<int16_t>(metadata.partitions.front().replicas.size());
    // Deterministic across brokers (M8-A2); single broker stays local.
    const auto assignments =
        broker_ids.size() <= 1
            ? std::vector<std::vector<BrokerId>>{}
            : roundRobinAssignments(broker_ids, current_count, new_total_count - current_count,
                                    replication_factor);

    for (int32_t partition = current_count; partition < new_total_count; ++partition) {
        PartitionMetadata pm;
        pm.error_code = ErrorCode::NONE;
        pm.partition = partition;
        pm.leader_epoch = 0;
        if (assignments.empty()) {
            pm.replicas = {local_broker_id_};
        } else {
            pm.replicas = assignments[static_cast<size_t>(partition - current_count)];
        }
        pm.leader = pm.replicas.empty() ? local_broker_id_ : pm.replicas[0];
        pm.isr = pm.replicas;
        metadata.partitions.push_back(std::move(pm));

        if (log_manager_) {
            log_manager_->getOrCreateLog(topic_name, partition);
        }
    }

    persistLocked();
    Logger::info("Increased partitions for topic {} from {} to {}", topic_name, current_count,
                 new_total_count);

    TopicOperationResult result;
    result.error_code = ErrorCode::NONE;
    result.topic_metadata = metadata;
    result.has_metadata = true;
    return result;
}

TopicOperationResult MetadataStore::applyUpdateISR(const std::string& topic_name,
                                                   PartitionId partition_id,
                                                   const std::vector<BrokerId>& isr,
                                                   int32_t expected_partition_epoch) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = topics_.find(topic_name);
    if (it == topics_.end()) {
        return TopicOperationResult::failure(ErrorCode::UNKNOWN_TOPIC_OR_PARTITION,
                                             "Topic does not exist");
    }

    bool partition_found = false;
    for (auto& partition : it->second.metadata.partitions) {
        if (partition.partition == partition_id) {
            if (expected_partition_epoch >= 0 &&
                expected_partition_epoch != partition.partition_epoch) {
                return TopicOperationResult::failure(ErrorCode::INVALID_UPDATE_VERSION,
                                                     "Partition metadata changed since planning");
            }
            if (partition.isr != isr) {
                partition.isr = isr;
                ++partition.partition_epoch;
            }
            partition_found = true;
            break;
        }
    }

    if (!partition_found) {
        return TopicOperationResult::failure(ErrorCode::UNKNOWN_TOPIC_OR_PARTITION,
                                             "Partition does not exist");
    }

    persistLocked();
    Logger::info("Updated ISR for {}-{}, new ISR size: {}", topic_name, partition_id, isr.size());

    TopicOperationResult result;
    result.error_code = ErrorCode::NONE;
    return result;
}

TopicOperationResult MetadataStore::applyUpdateLeader(const std::string& topic_name,
                                                      PartitionId partition_id, BrokerId leader,
                                                      int32_t expected_partition_epoch) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = topics_.find(topic_name);
    if (it == topics_.end()) {
        return TopicOperationResult::failure(ErrorCode::UNKNOWN_TOPIC_OR_PARTITION,
                                             "Topic does not exist");
    }

    for (auto& partition : it->second.metadata.partitions) {
        if (partition.partition == partition_id) {
            if (expected_partition_epoch >= 0 &&
                expected_partition_epoch != partition.partition_epoch) {
                return TopicOperationResult::failure(ErrorCode::INVALID_UPDATE_VERSION,
                                                     "Partition metadata changed since planning");
            }
            // The new leader must be an assigned replica (M7 elects among the
            // partition's replicas), or -1 to mark the partition offline (M8:
            // no eligible leader). Bump the leader epoch on every change so
            // followers can detect stale leadership (KIP-101).
            if (leader != -1 && std::find(partition.replicas.begin(), partition.replicas.end(),
                                          leader) == partition.replicas.end()) {
                return TopicOperationResult::failure(ErrorCode::INVALID_REPLICA_ASSIGNMENT,
                                                     "New leader is not an assigned replica");
            }
            partition.leader = leader;
            partition.leader_epoch += 1;
            ++partition.partition_epoch;
            persistLocked();
            Logger::info("Elected leader {} for {}-{} (leader_epoch now {})", leader, topic_name,
                         partition_id, partition.leader_epoch);
            TopicOperationResult result;
            result.error_code = ErrorCode::NONE;
            result.topic_metadata = it->second.metadata;
            result.has_metadata = true;
            return result;
        }
    }
    return TopicOperationResult::failure(ErrorCode::UNKNOWN_TOPIC_OR_PARTITION,
                                         "Partition does not exist");
}

std::vector<TopicMetadata> MetadataStore::describeTopics(
    const std::vector<std::string>& topic_names) const {
    std::lock_guard<std::mutex> lock(mutex_);

    std::vector<TopicMetadata> result;
    if (topic_names.empty()) {
        result.reserve(topics_.size());
        for (const auto& [_, state] : topics_) {
            result.push_back(state.metadata);
        }
        return result;
    }

    result.reserve(topic_names.size());
    for (const auto& name : topic_names) {
        auto it = topics_.find(name);
        if (it == topics_.end()) {
            TopicMetadata missing;
            missing.error_code = ErrorCode::UNKNOWN_TOPIC_OR_PARTITION;
            missing.name = name;
            missing.is_internal = false;
            result.push_back(std::move(missing));
        } else {
            result.push_back(it->second.metadata);
        }
    }
    return result;
}

std::vector<BrokerMetadata> MetadataStore::brokers() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return brokers_;
}

std::string MetadataStore::clusterId() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return cluster_id_;
}

void MetadataStore::updateLocalBroker(const BrokerMetadata& broker) {
    std::lock_guard<std::mutex> lock(mutex_);
    ensureLocalBrokerLocked(broker);
    persistLocked();
}

void MetadataStore::registerBroker(const BrokerMetadata& broker) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = std::find_if(brokers_.begin(), brokers_.end(),
                           [&](const BrokerMetadata& b) { return b.id == broker.id; });
    if (it == brokers_.end()) {
        brokers_.push_back(broker);
    } else {
        *it = broker;  // refresh address if it changed
    }
}

size_t MetadataStore::brokerCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return brokers_.size();
}

TopicOperationResult MetadataStore::validateCreateLocked(const TopicSpecification& spec) const {
    if (spec.name.empty()) {
        return TopicOperationResult::failure(ErrorCode::INVALID_TOPIC_EXCEPTION,
                                             "Topic name must not be empty");
    }
    if (topics_.contains(spec.name)) {
        return TopicOperationResult::failure(ErrorCode::TOPIC_ALREADY_EXISTS,
                                             "Topic already exists");
    }
    if (spec.num_partitions <= 0) {
        return TopicOperationResult::failure(ErrorCode::INVALID_PARTITIONS,
                                             "num_partitions must be > 0");
    }
    if (spec.replication_factor <= 0) {
        return TopicOperationResult::failure(ErrorCode::INVALID_REPLICATION_FACTOR,
                                             "replication_factor must be > 0");
    }
    if (spec.replication_factor > static_cast<int16_t>(brokers_.size())) {
        return TopicOperationResult::failure(
            ErrorCode::INVALID_REPLICATION_FACTOR,
            "replication_factor cannot be greater than the number of available brokers");
    }
    // Manual assignments are now supported
    if (!spec.assignments.empty()) {
        // Validate manual assignments
        if (static_cast<int32_t>(spec.assignments.size()) != spec.num_partitions) {
            return TopicOperationResult::failure(ErrorCode::INVALID_REPLICA_ASSIGNMENT,
                                                 "assignments size must match num_partitions");
        }
        for (const auto& replicas : spec.assignments) {
            if (replicas.empty()) {
                return TopicOperationResult::failure(
                    ErrorCode::INVALID_REPLICA_ASSIGNMENT,
                    "each partition must have at least one replica");
            }
            for (BrokerId broker_id : replicas) {
                bool found = false;
                for (const auto& broker : brokers_) {
                    if (broker.id == broker_id) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    return TopicOperationResult::failure(
                        ErrorCode::INVALID_REPLICA_ASSIGNMENT,
                        "replica assignment references unknown broker ID: " +
                            std::to_string(broker_id));
                }
            }
        }
    }
    return TopicOperationResult{};
}

void MetadataStore::ensureLocalBrokerLocked(const BrokerMetadata& broker) {
    auto it = std::find_if(brokers_.begin(), brokers_.end(),
                           [&](const BrokerMetadata& b) { return b.id == broker.id; });
    if (it == brokers_.end()) {
        brokers_.push_back(broker);
    } else {
        it->host = broker.host;
        it->port = broker.port;
        it->rack = broker.rack;
    }
}

void MetadataStore::persistLocked() const {
    nlohmann::json json;
    json["cluster_id"] = cluster_id_;
    json["brokers"] = nlohmann::json::array();
    for (const auto& broker : brokers_) {
        nlohmann::json broker_json;
        broker_json["id"] = broker.id;
        broker_json["host"] = broker.host;
        broker_json["port"] = broker.port;
        if (broker.rack) {
            broker_json["rack"] = *broker.rack;
        } else {
            broker_json["rack"] = nullptr;
        }
        json["brokers"].push_back(std::move(broker_json));
    }

    json["topics"] = nlohmann::json::array();
    for (const auto& [name, state] : topics_) {
        nlohmann::json topic_json;
        topic_json["name"] = name;
        topic_json["is_internal"] = state.metadata.is_internal;
        topic_json["configs"] = state.configs;
        topic_json["partitions"] = nlohmann::json::array();
        for (const auto& partition : state.metadata.partitions) {
            topic_json["partitions"].push_back(partitionToJson(partition));
        }
        json["topics"].push_back(std::move(topic_json));
    }

    json["applied_index"] = applied_index_;

    // Crash-atomic: a crash leaves either the previous or the new state, never a
    // torn file.
    if (!writeFileAtomically(metadata_file_, json.dump(2))) {
        throw KawasanException(ErrorCode::KAFKA_STORAGE_ERROR,
                               "Failed to persist metadata to " + metadata_file_);
    }
    persisted_applied_index_ = applied_index_;
}

int64_t MetadataStore::appliedIndex() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return applied_index_;
}

void MetadataStore::setAppliedIndex(int64_t index) {
    std::lock_guard<std::mutex> lock(mutex_);
    applied_index_ = std::max(applied_index_, index);
}

void MetadataStore::resetAppliedIndex(int64_t index) {
    std::lock_guard<std::mutex> lock(mutex_);
    applied_index_ = index;
    persistLocked();
}

void MetadataStore::persistAppliedIndex() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (persisted_applied_index_ < applied_index_) {
        persistLocked();
    }
}

std::string MetadataStore::computeChecksum() const {
    std::lock_guard<std::mutex> lock(mutex_);

    // Build a canonical JSON representation for checksum computation
    nlohmann::json json;
    json["cluster_id"] = cluster_id_;

    // Add topics in sorted order for deterministic checksum
    json["topics"] = nlohmann::json::array();
    std::vector<std::string> topic_names;
    topic_names.reserve(topics_.size());
    for (const auto& [name, _] : topics_) {
        topic_names.push_back(name);
    }
    std::sort(topic_names.begin(), topic_names.end());

    for (const auto& name : topic_names) {
        const auto& state = topics_.at(name);
        nlohmann::json topic_json;
        topic_json["name"] = name;
        topic_json["is_internal"] = state.metadata.is_internal;

        // Add partitions in sorted order
        topic_json["partitions"] = nlohmann::json::array();
        for (const auto& partition : state.metadata.partitions) {
            nlohmann::json part_json;
            part_json["partition"] = partition.partition;
            part_json["leader"] = partition.leader;
            part_json["leader_epoch"] = partition.leader_epoch;
            part_json["partition_epoch"] = partition.partition_epoch;
            part_json["replicas"] = partition.replicas;
            part_json["isr"] = partition.isr;
            part_json["offline_replicas"] = partition.offline_replicas;
            topic_json["partitions"].push_back(std::move(part_json));
        }

        // Add configs in sorted order
        if (!state.configs.empty()) {
            topic_json["configs"] = state.configs;
        }

        json["topics"].push_back(std::move(topic_json));
    }

    // Compute SHA-256 hash of the canonical JSON
    std::string canonical = json.dump();

    // Simple hash computation using std::hash for now
    // In production, should use proper SHA-256
    std::hash<std::string> hasher;
    size_t hash_value = hasher(canonical);

    // Convert to hex string
    std::stringstream ss;
    ss << std::hex << std::setfill('0') << std::setw(16) << hash_value;
    return ss.str();
}

}  // namespace kawasan::broker
