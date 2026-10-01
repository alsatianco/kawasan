#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "kawasan/common/buffer.h"
#include "kawasan/common/types.h"

namespace kawasan::protocol {

// Phase 1.19: OffsetForLeaderEpoch (API 23). Single-broker correctness.
//
// Schema (Kafka 4.2):
//   Request:
//     v0: topics ARRAY<{ name STRING, partitions ARRAY<{
//           partition INT32, leader_epoch INT32 }> }>
//     v1: + current_leader_epoch INT32 (replaces leader_epoch in v2+)
//     v2: + current_leader_epoch on each partition
//     v3: + replica_id INT32 (top-level, "-1 means consumer")
//     v4: flexible
//   Response:
//     topics ARRAY<{ name STRING, partitions ARRAY<{
//           error_code INT16, partition INT32,
//           leader_epoch INT32 (v1+), end_offset INT64 }> }>
//     v3: + throttle_time_ms INT32
//     v4: flexible
//
// Single-broker behavior: we only ever have one leader epoch (= 0). For any
// requested epoch, return (epoch=0, end_offset=log_end_offset). Consumers
// use this to detect truncation; with a single leader and persistent log
// there is never a truncation to detect.
class OffsetForLeaderEpochRequest {
public:
    struct PartitionQuery {
        int32_t partition;
        int32_t current_leader_epoch;  // v2+; -1 for v0/v1
        int32_t leader_epoch;          // the epoch consumer is requesting end_offset for
    };
    struct TopicQuery {
        std::string name;
        std::vector<PartitionQuery> partitions;
    };

    void decode(Buffer& buffer, int16_t api_version);
    void encode(Buffer& buffer, int16_t api_version) const;

    int32_t replicaId() const { return replica_id_; }
    const std::vector<TopicQuery>& topics() const { return topics_; }
    void setReplicaId(int32_t id) { replica_id_ = id; }
    void addTopic(TopicQuery topic) { topics_.push_back(std::move(topic)); }

private:
    int32_t replica_id_ = -1;  // v3+; "-1 = consumer"
    std::vector<TopicQuery> topics_;
};

class OffsetForLeaderEpochResponse {
public:
    struct PartitionResult {
        ErrorCode error_code = ErrorCode::NONE;
        int32_t partition = 0;
        int32_t leader_epoch = 0;
        int64_t end_offset = -1;
    };
    struct TopicResult {
        std::string name;
        std::vector<PartitionResult> partitions;
    };

    void setThrottleTimeMs(int32_t v) { throttle_time_ms_ = v; }
    void addTopic(TopicResult t) { topics_.push_back(std::move(t)); }

    void encode(Buffer& buffer, int16_t api_version) const;
    void decode(Buffer& buffer, int16_t api_version);

    const std::vector<TopicResult>& topics() const { return topics_; }

private:
    int32_t throttle_time_ms_ = 0;
    std::vector<TopicResult> topics_;
};

}  // namespace kawasan::protocol
