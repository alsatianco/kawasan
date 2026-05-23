#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "kawasan/common/buffer.h"
#include "kawasan/common/error.h"

namespace kawasan::protocol {

// Phase 3.3 scaffolding: wire-level handlers for the transactional APIs.
// The bodies are decoded enough to extract the transactional_id and
// producer_id/epoch so the broker can return a coherent response, but
// the actual transactional state-machine work (LSO, control records,
// commit/abort markers, read_committed isolation) is deferred. Returning
// a structured response — even one that effectively says "no
// transaction was actually started" — is better than returning
// UNSUPPORTED_VERSION because Java/librdkafka clients then have a
// graceful no-op path instead of crashing.

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
    };
    struct TopicOffsets {
        std::string topic;
        std::vector<PartitionOffset> partitions;
    };
    const std::string& transactionalId() const { return transactional_id_; }
    const std::string& groupId() const { return group_id_; }
    int64_t producerId() const { return producer_id_; }
    int16_t producerEpoch() const { return producer_epoch_; }
    const std::vector<TopicOffsets>& topics() const { return topics_; }
    void encode(Buffer& buf, int16_t v) const;
    void decode(Buffer& buf, int16_t v);
private:
    std::string transactional_id_;
    std::string group_id_;
    int64_t producer_id_ = -1;
    int16_t producer_epoch_ = -1;
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
