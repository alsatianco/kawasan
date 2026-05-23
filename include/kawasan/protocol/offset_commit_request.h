#pragma once

#include <optional>
#include <string>
#include <vector>

#include "kawasan/common/buffer.h"
#include "kawasan/common/error.h"

namespace kawasan::protocol {

/// @brief OffsetCommit request
///
/// Phase 1.11: supports v0–v8.
///   v0: group_id, topics[{ partition, offset, metadata }]
///   v1: + generation_id, member_id, partitions.timestamp INT64
///   v2: + retention_time_ms; − partitions.timestamp
///   v5: − retention_time_ms
///   v6: + partitions.committed_leader_epoch INT32
///   v7: + group_instance_id NULLABLE_STRING
///   v8: flexible
class OffsetCommitRequest {
public:
    struct PartitionData {
        int32_t partition = 0;
        int64_t offset = 0;
        int32_t committed_leader_epoch = -1;  // v6+
        int64_t timestamp = -1;               // v1 only
        std::string metadata;
    };

    struct TopicData {
        std::string topic;
        std::vector<PartitionData> partitions;
    };

    OffsetCommitRequest() = default;

    const std::string& groupId() const { return group_id_; }
    int32_t generationId() const { return generation_id_; }
    const std::string& memberId() const { return member_id_; }
    const std::optional<std::string>& groupInstanceId() const { return group_instance_id_; }
    int64_t retentionTimeMs() const { return retention_time_ms_; }
    const std::vector<TopicData>& topics() const { return topics_; }

    void setGroupId(const std::string& id) { group_id_ = id; }
    void setGenerationId(int32_t generation) { generation_id_ = generation; }
    void setMemberId(const std::string& member) { member_id_ = member; }
    void setGroupInstanceId(std::optional<std::string> v) { group_instance_id_ = std::move(v); }
    void setRetentionTimeMs(int64_t retention) { retention_time_ms_ = retention; }
    void setTopics(const std::vector<TopicData>& topics) { topics_ = topics; }

    void encode(Buffer& buffer, int16_t api_version) const;
    void decode(Buffer& buffer, int16_t api_version);

private:
    std::string group_id_;
    int32_t generation_id_ = -1;
    std::string member_id_;
    std::optional<std::string> group_instance_id_;  // v7+
    int64_t retention_time_ms_ = -1;                // v2–v4
    std::vector<TopicData> topics_;
};

/// @brief OffsetCommit response
///
///   v0–v2: topics[{ partition, error }]
///   v3+:   + throttle_time_ms (first)
///   v8:    flexible
class OffsetCommitResponse {
public:
    struct Partition {
        int32_t partition = 0;
        ErrorCode error = ErrorCode::NONE;
    };

    struct Topic {
        std::string topic;
        std::vector<Partition> partitions;
    };

    OffsetCommitResponse() = default;

    void setThrottleTimeMs(int32_t v) { throttle_time_ms_ = v; }
    void setTopics(const std::vector<Topic>& topics) { topics_ = topics; }
    int32_t throttleTimeMs() const { return throttle_time_ms_; }
    const std::vector<Topic>& topics() const { return topics_; }

    void encode(Buffer& buffer, int16_t api_version) const;
    void decode(Buffer& buffer, int16_t api_version);

private:
    int32_t throttle_time_ms_ = 0;
    std::vector<Topic> topics_;
};

}  // namespace kawasan::protocol
