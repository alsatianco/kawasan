#pragma once

#include <string>
#include <vector>

#include "kawasan/common/buffer.h"
#include "kawasan/common/types.h"

namespace kawasan::protocol {

/// @brief Partition data for list offsets request
struct ListOffsetsPartition {
    PartitionId partition;
    int32_t current_leader_epoch = -1;  // v4+: current leader epoch, -1 if unknown
    int64_t timestamp;  // -1 = earliest, -2 = latest, or timestamp in ms
    int32_t max_num_offsets = 1;  // Used in v0 only
};

/// @brief Topic data for list offsets request
struct ListOffsetsTopic {
    std::string topic;
    std::vector<ListOffsetsPartition> partitions;
};

/// @brief ListOffsets request for querying partition offsets
class ListOffsetsRequest {
public:
    ListOffsetsRequest() = default;

    // Getters
    int32_t replicaId() const { return replica_id_; }
    int8_t isolationLevel() const { return isolation_level_; }
    const std::vector<ListOffsetsTopic>& topics() const { return topics_; }

    // Setters
    void setReplicaId(int32_t id) { replica_id_ = id; }
    void setIsolationLevel(int8_t level) { isolation_level_ = level; }
    void addTopic(const ListOffsetsTopic& topic) { topics_.push_back(topic); }

    // Serialization
    void encode(Buffer& buffer, int16_t api_version) const;
    void decode(Buffer& buffer, int16_t api_version);

private:
    int32_t replica_id_ = -1;  // -1 for consumer
    int8_t isolation_level_ = 0;  // 0 = READ_UNCOMMITTED, 1 = READ_COMMITTED
    std::vector<ListOffsetsTopic> topics_;
};

/// @brief Partition response for list offsets request
struct ListOffsetsPartitionResponse {
    PartitionId partition;
    ErrorCode error_code;
    int64_t timestamp;  // -1 if not available
    int64_t offset;     // Offset corresponding to timestamp
    int32_t leader_epoch = -1;  // Leader epoch (v4+)
    
    // For v0 compatibility (multiple offsets)
    std::vector<int64_t> old_style_offsets;
};

/// @brief Topic response for list offsets request
struct ListOffsetsTopicResponse {
    std::string topic;
    std::vector<ListOffsetsPartitionResponse> partitions;
};

/// @brief ListOffsets response
class ListOffsetsResponse {
public:
    ListOffsetsResponse() = default;

    void setThrottleTimeMs(int32_t throttle) { throttle_time_ms_ = throttle; }
    void addTopic(const ListOffsetsTopicResponse& topic) { topics_.push_back(topic); }
    
    int32_t throttleTimeMs() const { return throttle_time_ms_; }
    const std::vector<ListOffsetsTopicResponse>& topics() const { return topics_; }

    // Serialization
    void encode(Buffer& buffer, int16_t api_version) const;
    void decode(Buffer& buffer, int16_t api_version);

private:
    int32_t throttle_time_ms_ = 0;
    std::vector<ListOffsetsTopicResponse> topics_;
};

}  // namespace kawasan::protocol

