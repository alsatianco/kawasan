#pragma once

#include <array>
#include <optional>
#include <string>
#include <vector>

#include "kawasan/common/buffer.h"
#include "kawasan/common/types.h"

namespace kawasan::protocol {

/// @brief Metadata request for discovering brokers, topics, and partitions
///
/// Phase 1.2: supports v0–v12.
///   v0–v3: topics ARRAY<STRING> (null = "all")
///   v4+:   + allow_auto_topic_creation
///   v8:    + include_cluster_authorized_operations + include_topic_authorized_operations
///   v9:    flexible
///   v10:   topics ARRAY<{ topic_id UUID, name COMPACT_NULLABLE_STRING }>
///   v11:   − include_cluster_authorized_operations
class MetadataRequest {
public:
    MetadataRequest() = default;
    explicit MetadataRequest(const std::vector<std::string>& topics) : topics_(topics) {}

    const std::vector<std::string>& topics() const { return topics_; }
    bool allowAutoTopicCreation() const { return allow_auto_topic_creation_; }
    bool includeClusterAuthorizedOperations() const { return include_cluster_authorized_operations_; }
    bool includeTopicAuthorizedOperations() const { return include_topic_authorized_operations_; }

    void setTopics(const std::vector<std::string>& topics) { topics_ = topics; }
    void setAllowAutoTopicCreation(bool allow) { allow_auto_topic_creation_ = allow; }
    void setIncludeClusterAuthorizedOperations(bool v) { include_cluster_authorized_operations_ = v; }
    void setIncludeTopicAuthorizedOperations(bool v) { include_topic_authorized_operations_ = v; }
    void addTopic(const std::string& topic) { topics_.push_back(topic); }

    void encode(Buffer& buffer, int16_t api_version) const;
    void decode(Buffer& buffer, int16_t api_version);
    size_t size(int16_t api_version) const;

private:
    std::vector<std::string> topics_;
    bool allow_auto_topic_creation_ = true;
    bool include_cluster_authorized_operations_ = false;  // v8–v10
    bool include_topic_authorized_operations_ = false;    // v8+
};

/// @brief Metadata response containing cluster metadata
class MetadataResponse {
public:
    MetadataResponse() = default;

    // Getters
    int32_t throttleTimeMs() const { return throttle_time_ms_; }
    const std::vector<BrokerMetadata>& brokers() const { return brokers_; }
    const std::string& clusterId() const { return cluster_id_; }
    BrokerId controllerId() const { return controller_id_; }
    const std::vector<TopicMetadata>& topics() const { return topics_; }

    // Setters
    void setThrottleTimeMs(int32_t time) { throttle_time_ms_ = time; }
    void setBrokers(const std::vector<BrokerMetadata>& brokers) { brokers_ = brokers; }
    void setClusterId(const std::string& id) { cluster_id_ = id; }
    void setControllerId(BrokerId id) { controller_id_ = id; }
    void setTopics(const std::vector<TopicMetadata>& topics) { topics_ = topics; }
    void addBroker(const BrokerMetadata& broker) { brokers_.push_back(broker); }
    void addTopic(const TopicMetadata& topic) { topics_.push_back(topic); }

    // Serialization
    void encode(Buffer& buffer, int16_t api_version) const;
    void decode(Buffer& buffer, int16_t api_version);

    // Size calculation
    size_t size(int16_t api_version) const;

private:
    int32_t throttle_time_ms_ = 0;
    std::vector<BrokerMetadata> brokers_;
    std::string cluster_id_;
    BrokerId controller_id_ = -1;
    std::vector<TopicMetadata> topics_;
};

}  // namespace kawasan::protocol

