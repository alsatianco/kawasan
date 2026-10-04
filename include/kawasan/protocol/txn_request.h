#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "kawasan/common/buffer.h"
#include "kawasan/common/error.h"

namespace kawasan::protocol {

// Transactional wire codecs through v3. The broker implements single-node
// commit/abort visibility and durable replay; clustered coordinator failover
// and multi-broker EOS are M10. Advertisement is gated on handler semantics.

// ---- AddPartitionsToTxn (24) ----
class AddPartitionsToTxnRequest {
public:
    struct PartitionList {
        std::string topic;
        std::vector<int32_t> partitions;
    };
    const std::string& transactionalId() const { return transactional_id_; }
    int64_t producerId() const { return producer_id_; }
    int16_t producerEpoch() const { return producer_epoch_; }
    const std::vector<PartitionList>& topics() const { return topics_; }
    void encode(Buffer& buf, int16_t v) const;
    void decode(Buffer& buf, int16_t v);

private:
    std::string transactional_id_;
    int64_t producer_id_ = -1;
    int16_t producer_epoch_ = -1;
    std::vector<PartitionList> topics_;
};

class AddPartitionsToTxnResponse {
public:
    struct PartitionResult {
        int32_t partition;
        ErrorCode error_code = ErrorCode::NONE;
    };
    struct TopicResult {
        std::string topic;
        std::vector<PartitionResult> partitions;
    };
    void setThrottleTimeMs(int32_t v) { throttle_time_ms_ = v; }
    void addTopic(TopicResult t) { topics_.push_back(std::move(t)); }
    void encode(Buffer& buf, int16_t v) const;
    void decode(Buffer& buf, int16_t v);

private:
    int32_t throttle_time_ms_ = 0;
    std::vector<TopicResult> topics_;
};

// ---- AddOffsetsToTxn (25) ----
class AddOffsetsToTxnRequest {
public:
    const std::string& transactionalId() const { return transactional_id_; }
    int64_t producerId() const { return producer_id_; }
    int16_t producerEpoch() const { return producer_epoch_; }
    const std::string& groupId() const { return group_id_; }
    void encode(Buffer& buf, int16_t v) const;
    void decode(Buffer& buf, int16_t v);

private:
    std::string transactional_id_;
    int64_t producer_id_ = -1;
    int16_t producer_epoch_ = -1;
    std::string group_id_;
};

class AddOffsetsToTxnResponse {
public:
    void setThrottleTimeMs(int32_t v) { throttle_time_ms_ = v; }
    void setErrorCode(ErrorCode v) { error_code_ = v; }
    void encode(Buffer& buf, int16_t v) const;
    void decode(Buffer& buf, int16_t v);

private:
    int32_t throttle_time_ms_ = 0;
    ErrorCode error_code_ = ErrorCode::NONE;
};

// ---- EndTxn (26) ----
class EndTxnRequest {
public:
    const std::string& transactionalId() const { return transactional_id_; }
    int64_t producerId() const { return producer_id_; }
    int16_t producerEpoch() const { return producer_epoch_; }
    bool committed() const { return committed_; }
    void encode(Buffer& buf, int16_t v) const;
    void decode(Buffer& buf, int16_t v);

private:
    std::string transactional_id_;
    int64_t producer_id_ = -1;
    int16_t producer_epoch_ = -1;
    bool committed_ = false;
};

class EndTxnResponse {
public:
    void setThrottleTimeMs(int32_t v) { throttle_time_ms_ = v; }
    void setErrorCode(ErrorCode v) { error_code_ = v; }
    void encode(Buffer& buf, int16_t v) const;
    void decode(Buffer& buf, int16_t v);

private:
    int32_t throttle_time_ms_ = 0;
    ErrorCode error_code_ = ErrorCode::NONE;
};

// ---- TxnOffsetCommit (28) ----
class TxnOffsetCommitRequest {
public:
    struct PartitionOffset {
        int32_t partition;
        int64_t offset;
        std::string metadata;
        int32_t committed_leader_epoch = -1;  // v2+
    };
    struct TopicOffsets {
        std::string topic;
        std::vector<PartitionOffset> partitions;
    };
    const std::string& transactionalId() const { return transactional_id_; }
    const std::string& groupId() const { return group_id_; }
    int64_t producerId() const { return producer_id_; }
    int16_t producerEpoch() const { return producer_epoch_; }
    int32_t generationId() const { return generation_id_; }
    const std::string& memberId() const { return member_id_; }
    const std::optional<std::string>& groupInstanceId() const { return group_instance_id_; }
    const std::vector<TopicOffsets>& topics() const { return topics_; }
    void setTransactionalId(std::string id) { transactional_id_ = std::move(id); }
    void setGroupId(std::string id) { group_id_ = std::move(id); }
    void setProducerId(int64_t id) { producer_id_ = id; }
    void setProducerEpoch(int16_t epoch) { producer_epoch_ = epoch; }
    void setGenerationId(int32_t generation) { generation_id_ = generation; }
    void setMemberId(std::string member) { member_id_ = std::move(member); }
    void setGroupInstanceId(std::optional<std::string> instance) {
        group_instance_id_ = std::move(instance);
    }
    void addTopic(TopicOffsets topic) { topics_.push_back(std::move(topic)); }
    void encode(Buffer& buf, int16_t v) const;
    void decode(Buffer& buf, int16_t v);

private:
    std::string transactional_id_;
    std::string group_id_;
    int64_t producer_id_ = -1;
    int16_t producer_epoch_ = -1;
    int32_t generation_id_ = -1;
    std::string member_id_;
    std::optional<std::string> group_instance_id_;
    std::vector<TopicOffsets> topics_;
};

class TxnOffsetCommitResponse {
public:
    struct PartitionResult {
        int32_t partition;
        ErrorCode error_code = ErrorCode::NONE;
    };
    struct TopicResult {
        std::string topic;
        std::vector<PartitionResult> partitions;
    };
    void setThrottleTimeMs(int32_t v) { throttle_time_ms_ = v; }
    void addTopic(TopicResult t) { topics_.push_back(std::move(t)); }
    void encode(Buffer& buf, int16_t v) const;
    void decode(Buffer& buf, int16_t v);

private:
    int32_t throttle_time_ms_ = 0;
    std::vector<TopicResult> topics_;
};

}  // namespace kawasan::protocol
