#pragma once

#include <array>
#include <string>
#include <vector>

#include "kawasan/broker/metadata_types.h"
#include "kawasan/common/buffer.h"
#include "kawasan/common/types.h"

namespace kawasan::protocol {

struct CreatableReplicaAssignment {
    PartitionId partition_index = 0;
    std::vector<BrokerId> broker_ids;
};

struct CreatableTopicConfig {
    std::string name;
    std::string value;
};

struct CreatableTopic {
    std::string name;
    int32_t num_partitions = -1;
    int16_t replication_factor = -1;
    std::vector<CreatableReplicaAssignment> assignments;
    std::vector<CreatableTopicConfig> configs;
};

class CreateTopicsRequest {
public:
    CreateTopicsRequest() = default;

    void setTimeoutMs(int32_t timeout) { timeout_ms_ = timeout; }
    void setValidateOnly(bool validate_only) { validate_only_ = validate_only; }
    void addTopic(const CreatableTopic& topic) { topics_.push_back(topic); }

    int32_t timeoutMs() const { return timeout_ms_; }
    bool validateOnly() const { return validate_only_; }
    const std::vector<CreatableTopic>& topics() const { return topics_; }

    void encode(Buffer& buffer, int16_t api_version) const;
    void decode(Buffer& buffer, int16_t api_version);

private:
    int32_t timeout_ms_ = 60000;
    bool validate_only_ = false;
    std::vector<CreatableTopic> topics_;
};

struct CreatableTopicResult {
    std::string name;
    // v7+: topic_id is a 16-byte UUID. We use all-zeros for synthetic topics
    // until the full topic-id assignment lands in a later phase.
    std::array<uint8_t, 16> topic_id{};
    ErrorCode error_code = ErrorCode::NONE;
    std::string error_message;
    // v5+: numerical config-value reporting (we leave it empty until
    // DescribeConfigs is fully wired into the response).
    int32_t num_partitions = -1;
    int16_t replication_factor = -1;
};

class CreateTopicsResponse {
public:
    void setThrottleTimeMs(int32_t throttle) { throttle_time_ms_ = throttle; }
    void addTopicResult(const CreatableTopicResult& result) { topics_.push_back(result); }
    const std::vector<CreatableTopicResult>& results() const { return topics_; }

    void encode(Buffer& buffer, int16_t api_version) const;
    void decode(Buffer& buffer, int16_t api_version);

private:
    int32_t throttle_time_ms_ = 0;
    std::vector<CreatableTopicResult> topics_;
};

/// @brief Utility to convert protocol structures into broker specifications.
broker::TopicSpecification toTopicSpecification(const CreatableTopic& topic);

}  // namespace kawasan::protocol

