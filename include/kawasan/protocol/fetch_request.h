#pragma once

#include <array>
#include <map>
#include <string>
#include <vector>

#include "kawasan/common/buffer.h"
#include "kawasan/common/types.h"

namespace kawasan::protocol {

/// @brief Partition data for fetch request
struct FetchPartition {
    PartitionId partition;
    int32_t current_leader_epoch = -1;  // v9+
    Offset fetch_offset;
    int32_t last_fetched_epoch = -1;    // v12+ (we accept-and-ignore)
    Offset log_start_offset = -1;       // v5+ — replica only; -1 = unset
    int32_t partition_max_bytes;
};

/// @brief Topic data for fetch request
struct FetchTopic {
    std::string topic;
    // Phase 1.4: v13 swaps `topic` (STRING) for `topic_id` (UUID). The
    // handler resolves topic_id → name; for older versions only `topic`
    // is populated.
    std::array<uint8_t, 16> topic_id{};
    bool has_topic_id = false;
    std::vector<FetchPartition> partitions;
};

/// @brief Topic-partition pair for forgotten_topics_data (v7+).
struct FetchForgottenTopic {
    std::string topic;
    std::vector<int32_t> partitions;
};

/// @brief Fetch request for consuming records
///
/// Phase 1.4 (partial): supports v0–v11. v12 flexible + v13 topic_id deferred
/// alongside the full FetchSessionManager (KIP-227) backend.
class FetchRequest {
public:
    FetchRequest() = default;

    int32_t replicaId() const { return replica_id_; }
    int32_t maxWaitMs() const { return max_wait_ms_; }
    int32_t minBytes() const { return min_bytes_; }
    int32_t maxBytes() const { return max_bytes_; }
    int8_t isolationLevel() const { return isolation_level_; }
    int32_t sessionId() const { return session_id_; }
    int32_t sessionEpoch() const { return session_epoch_; }
    const std::string& rackId() const { return rack_id_; }
    const std::vector<FetchTopic>& topics() const { return topics_; }
    const std::vector<FetchForgottenTopic>& forgottenTopics() const { return forgotten_topics_; }

    void setReplicaId(int32_t id) { replica_id_ = id; }
    void setMaxWaitMs(int32_t wait) { max_wait_ms_ = wait; }
    void setMinBytes(int32_t bytes) { min_bytes_ = bytes; }
    void setMaxBytes(int32_t bytes) { max_bytes_ = bytes; }
    void setIsolationLevel(int8_t level) { isolation_level_ = level; }
    void setSessionId(int32_t id) { session_id_ = id; }
    void setSessionEpoch(int32_t epoch) { session_epoch_ = epoch; }
    void setRackId(const std::string& rack) { rack_id_ = rack; }
    void addTopic(const FetchTopic& topic) { topics_.push_back(topic); }
    void addForgottenTopic(const FetchForgottenTopic& t) { forgotten_topics_.push_back(t); }

    void encode(Buffer& buffer, int16_t api_version) const;
    void decode(Buffer& buffer, int16_t api_version);
    size_t size(int16_t api_version) const;

private:
    int32_t replica_id_ = -1;
    int32_t max_wait_ms_ = 500;
    int32_t min_bytes_ = 1;
    int32_t max_bytes_ = 52428800;
    int8_t isolation_level_ = 0;       // v4+; 0 = READ_UNCOMMITTED, 1 = READ_COMMITTED
    int32_t session_id_ = 0;            // v7+
    int32_t session_epoch_ = -1;        // v7+
    std::string rack_id_;               // v11+
    std::vector<FetchTopic> topics_;
    std::vector<FetchForgottenTopic> forgotten_topics_;
};

/// @brief Aborted transaction entry (Fetch response v4+).
struct FetchAbortedTransaction {
    int64_t producer_id = -1;
    int64_t first_offset = -1;
};

/// @brief Partition response for fetch request
struct FetchPartitionResponse {
    PartitionId partition;
    ErrorCode error_code;
    Offset high_watermark;
    Offset last_stable_offset;
    Offset log_start_offset;
    std::vector<FetchAbortedTransaction> aborted_transactions;  // v4+
    int32_t preferred_read_replica = -1;                        // v11+; -1 = no preference
    std::vector<uint8_t> record_batches;
};

/// @brief Topic response for fetch request
struct FetchTopicResponse {
    std::string topic;
    // Phase 1.4: v13 response echoes topic_id instead of topic name.
    std::array<uint8_t, 16> topic_id{};
    std::vector<FetchPartitionResponse> partitions;
};

/// @brief Fetch response
class FetchResponse {
public:
    FetchResponse() = default;

    int32_t throttleTimeMs() const { return throttle_time_ms_; }
    ErrorCode errorCode() const { return error_code_; }
    int32_t sessionId() const { return session_id_; }
    const std::vector<FetchTopicResponse>& topics() const { return topics_; }

    void setThrottleTimeMs(int32_t time) { throttle_time_ms_ = time; }
    void setErrorCode(ErrorCode code) { error_code_ = code; }
    void setSessionId(int32_t id) { session_id_ = id; }
    void addTopic(const FetchTopicResponse& topic) { topics_.push_back(topic); }

    void encode(Buffer& buffer, int16_t api_version) const;
    void decode(Buffer& buffer, int16_t api_version);
    size_t size(int16_t api_version) const;

private:
    int32_t throttle_time_ms_ = 0;
    ErrorCode error_code_ = ErrorCode::NONE;  // v7+
    int32_t session_id_ = 0;                  // v7+
    std::vector<FetchTopicResponse> topics_;
};

}  // namespace kawasan::protocol

