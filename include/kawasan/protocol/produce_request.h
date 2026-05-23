#pragma once

#include <map>
#include <string>
#include <vector>

#include "kawasan/common/buffer.h"
#include "kawasan/common/types.h"

namespace kawasan::protocol {

/// @brief Partition data for produce request
struct ProducePartitionData {
    PartitionId partition;
    std::vector<uint8_t> record_batch;  // Serialized record batch
};

/// @brief Topic data for produce request
struct ProduceTopicData {
    std::string topic;
    std::vector<ProducePartitionData> partitions;
};

/// @brief Produce request for sending records to Kafka
class ProduceRequest {
public:
    ProduceRequest() = default;

    // Getters
    const std::string& transactionalId() const { return transactional_id_; }
    int16_t acks() const { return acks_; }
    int32_t timeoutMs() const { return timeout_ms_; }
    const std::vector<ProduceTopicData>& topics() const { return topics_; }

    // Setters
    void setTransactionalId(const std::string& id) { transactional_id_ = id; }
    void setAcks(int16_t acks) { acks_ = acks; }
    void setTimeoutMs(int32_t timeout) { timeout_ms_ = timeout; }
    void addTopic(const ProduceTopicData& topic) { topics_.push_back(topic); }

    // Serialization
    void encode(Buffer& buffer, int16_t api_version) const;
    void decode(Buffer& buffer, int16_t api_version);

    // Size calculation
    size_t size(int16_t api_version) const;

private:
    std::string transactional_id_;
    int16_t acks_ = -1;  // -1 = all, 0 = none, 1 = leader
    int32_t timeout_ms_ = 30000;
    std::vector<ProduceTopicData> topics_;
};

/// @brief Per-record-batch error detail (Produce response v8+).
struct ProduceRecordError {
    int32_t batch_index = -1;
    std::string error_message;
};

/// @brief Partition response for produce request
struct ProducePartitionResponse {
    PartitionId partition;
    ErrorCode error_code;
    Offset base_offset;
    Timestamp log_append_time;
    Timestamp log_start_offset;
    // v8+ — populated only when partition.error_code != NONE.
    std::vector<ProduceRecordError> record_errors;
    std::string error_message;
};

/// @brief Topic response for produce request
struct ProduceTopicResponse {
    std::string topic;
    std::vector<ProducePartitionResponse> partitions;
};

/// @brief Produce response
class ProduceResponse {
public:
    ProduceResponse() = default;

    // Getters
    const std::vector<ProduceTopicResponse>& topics() const { return topics_; }
    int32_t throttleTimeMs() const { return throttle_time_ms_; }

    // Setters
    void addTopic(const ProduceTopicResponse& topic) { topics_.push_back(topic); }
    void setThrottleTimeMs(int32_t time) { throttle_time_ms_ = time; }

    // Serialization
    void encode(Buffer& buffer, int16_t api_version) const;
    void decode(Buffer& buffer, int16_t api_version);

    // Size calculation
    size_t size(int16_t api_version) const;

private:
    std::vector<ProduceTopicResponse> topics_;
    int32_t throttle_time_ms_ = 0;
};

}  // namespace kawasan::protocol

