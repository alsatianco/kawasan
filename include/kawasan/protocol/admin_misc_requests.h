// Phase 4.1 miscellaneous admin APIs.
//
// This header batches several single-broker-stub APIs:
//   - DescribeLogDirs (35)        — returns per-log-dir size info
//   - AlterReplicaLogDirs (34)    — single-broker: no-op success
//   - ElectLeaders (43)            — single-broker: always already-elected
//   - AlterPartition (56)          — single-broker: no-op
//   - DeleteRecords (21)           — advances log_start_offset
//   - DeleteGroups (42)            — deletes consumer group state
//   - OffsetDelete (47)            — deletes specific (group,topic,partition) offsets
//   - CreatePartitions (37)        — adds partitions to existing topic
//
// Each request/response is implemented inline for brevity. Where the doc
// targets a specific Kafka-spec version, we support v0 only for the stub
// APIs and the full lower-version ladder for the active ones.

#pragma once

#include <optional>
#include <string>
#include <vector>

#include "kawasan/common/buffer.h"
#include "kawasan/common/error.h"
#include "kawasan/common/types.h"

namespace kawasan::protocol {

// ---- DescribeLogDirs (API 35) ----
class DescribeLogDirsRequest {
public:
    struct Topic {
        std::string topic;
        std::vector<int32_t> partitions;
    };
    // null topics list (v0+) means "all topics".
    bool fetchAll() const { return fetch_all_; }
    const std::vector<Topic>& topics() const { return topics_; }
    // Client-side builders (symmetric with encode()).
    void setFetchAll(bool v) { fetch_all_ = v; }
    void addTopic(Topic t) {
        fetch_all_ = false;
        topics_.push_back(std::move(t));
    }
    void encode(Buffer& buf, int16_t v) const;
    void decode(Buffer& buf, int16_t v);

private:
    bool fetch_all_ = true;
    std::vector<Topic> topics_;
};

class DescribeLogDirsResponse {
public:
    struct PartitionInfo {
        int32_t partition = 0;
        int64_t size_bytes = 0;
        int64_t offset_lag = 0;
        bool is_future = false;
    };
    struct TopicInfo {
        std::string topic;
        std::vector<PartitionInfo> partitions;
    };
    struct LogDirInfo {
        ErrorCode error_code = ErrorCode::NONE;
        std::string log_dir;
        std::vector<TopicInfo> topics;
    };
    void setThrottleTimeMs(int32_t v) { throttle_time_ms_ = v; }
    void addLogDir(LogDirInfo d) { log_dirs_.push_back(std::move(d)); }
    const std::vector<LogDirInfo>& logDirs() const { return log_dirs_; }
    void encode(Buffer& buf, int16_t v) const;
    void decode(Buffer& buf, int16_t v);

private:
    int32_t throttle_time_ms_ = 0;
    std::vector<LogDirInfo> log_dirs_;
};

// ---- AlterReplicaLogDirs (API 34) ----
// Single-broker: we have one log dir; accept all requests and return success.
class AlterReplicaLogDirsRequest {
public:
    struct PartitionRef {
        std::string topic;
        int32_t partition;
    };
    struct DirSpec {
        std::string log_dir;
        std::vector<PartitionRef> partitions;
    };
    const std::vector<DirSpec>& dirs() const { return dirs_; }
    void encode(Buffer& buf, int16_t v) const;
    void decode(Buffer& buf, int16_t v);

private:
    std::vector<DirSpec> dirs_;
};

class AlterReplicaLogDirsResponse {
public:
    struct PartitionResult {
        std::string topic;
        int32_t partition = 0;
        ErrorCode error_code = ErrorCode::NONE;
    };
    void setThrottleTimeMs(int32_t v) { throttle_time_ms_ = v; }
    void addResult(PartitionResult r) { results_.push_back(std::move(r)); }
    void encode(Buffer& buf, int16_t v) const;
    void decode(Buffer& buf, int16_t v);

private:
    int32_t throttle_time_ms_ = 0;
    std::vector<PartitionResult> results_;
};

// ---- ElectLeaders (API 43) ----
// Single-broker: we're always the leader; report NONE for known partitions.
class ElectLeadersRequest {
public:
    struct TopicPartitionsList {
        std::string topic;
        std::vector<int32_t> partitions;
    };
    int8_t electionType() const { return election_type_; }
    const std::vector<TopicPartitionsList>& topics() const { return topics_; }
    int32_t timeoutMs() const { return timeout_ms_; }
    void encode(Buffer& buf, int16_t v) const;
    void decode(Buffer& buf, int16_t v);

private:
    int8_t election_type_ = 0;
    std::vector<TopicPartitionsList> topics_;
    int32_t timeout_ms_ = 60000;
};

class ElectLeadersResponse {
public:
    struct PartitionResult {
        int32_t partition = 0;
        ErrorCode error_code = ErrorCode::NONE;
        std::string error_message;
    };
    struct TopicResult {
        std::string topic;
        std::vector<PartitionResult> partitions;
    };
    void setThrottleTimeMs(int32_t v) { throttle_time_ms_ = v; }
    void setErrorCode(ErrorCode v) { error_code_ = v; }
    void addTopic(TopicResult t) { topics_.push_back(std::move(t)); }
    void encode(Buffer& buf, int16_t v) const;
    void decode(Buffer& buf, int16_t v);

private:
    int32_t throttle_time_ms_ = 0;
    ErrorCode error_code_ = ErrorCode::NONE;
    std::vector<TopicResult> topics_;
};

// ---- DeleteRecords (API 21) ----
// Advances log_start_offset to a given offset per partition.
class DeleteRecordsRequest {
public:
    struct PartitionSpec {
        int32_t partition = 0;
        int64_t offset = 0;
    };
    struct TopicSpec {
        std::string topic;
        std::vector<PartitionSpec> partitions;
    };
    const std::vector<TopicSpec>& topics() const { return topics_; }
    int32_t timeoutMs() const { return timeout_ms_; }
    void encode(Buffer& buf, int16_t v) const;
    void decode(Buffer& buf, int16_t v);

private:
    std::vector<TopicSpec> topics_;
    int32_t timeout_ms_ = 60000;
};

class DeleteRecordsResponse {
public:
    struct PartitionResult {
        int32_t partition = 0;
        int64_t low_watermark = 0;
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

// ---- DeleteGroups (API 42) ----
class DeleteGroupsRequest {
public:
    const std::vector<std::string>& groups() const { return groups_; }
    void encode(Buffer& buf, int16_t v) const;
    void decode(Buffer& buf, int16_t v);

private:
    std::vector<std::string> groups_;
};

class DeleteGroupsResponse {
public:
    struct Result {
        std::string group_id;
        ErrorCode error_code = ErrorCode::NONE;
    };
    void setThrottleTimeMs(int32_t v) { throttle_time_ms_ = v; }
    void addResult(Result r) { results_.push_back(std::move(r)); }
    void encode(Buffer& buf, int16_t v) const;
    void decode(Buffer& buf, int16_t v);

private:
    int32_t throttle_time_ms_ = 0;
    std::vector<Result> results_;
};

// ---- OffsetDelete (API 47) ----
class OffsetDeleteRequest {
public:
    struct PartitionSpec {
        int32_t partition = 0;
    };
    struct TopicSpec {
        std::string topic;
        std::vector<PartitionSpec> partitions;
    };
    const std::string& groupId() const { return group_id_; }
    const std::vector<TopicSpec>& topics() const { return topics_; }
    void encode(Buffer& buf, int16_t v) const;
    void decode(Buffer& buf, int16_t v);

private:
    std::string group_id_;
    std::vector<TopicSpec> topics_;
};

class OffsetDeleteResponse {
public:
    struct PartitionResult {
        int32_t partition = 0;
        ErrorCode error_code = ErrorCode::NONE;
    };
    struct TopicResult {
        std::string topic;
        std::vector<PartitionResult> partitions;
    };
    void setErrorCode(ErrorCode v) { error_code_ = v; }
    void setThrottleTimeMs(int32_t v) { throttle_time_ms_ = v; }
    void addTopic(TopicResult t) { topics_.push_back(std::move(t)); }
    void encode(Buffer& buf, int16_t v) const;
    void decode(Buffer& buf, int16_t v);

private:
    ErrorCode error_code_ = ErrorCode::NONE;
    int32_t throttle_time_ms_ = 0;
    std::vector<TopicResult> topics_;
};

// ---- CreatePartitions (API 37) ----
class CreatePartitionsRequest {
public:
    struct TopicSpec {
        std::string topic;
        int32_t count = 0;                              // new total partition count
        std::vector<std::vector<int32_t>> assignments;  // optional explicit
    };
    const std::vector<TopicSpec>& topics() const { return topics_; }
    int32_t timeoutMs() const { return timeout_ms_; }
    bool validateOnly() const { return validate_only_; }
    // Client-side builders (symmetric with encode()).
    void addTopic(TopicSpec t) { topics_.push_back(std::move(t)); }
    void setTimeoutMs(int32_t v) { timeout_ms_ = v; }
    void setValidateOnly(bool v) { validate_only_ = v; }
    void encode(Buffer& buf, int16_t v) const;
    void decode(Buffer& buf, int16_t v);

private:
    std::vector<TopicSpec> topics_;
    int32_t timeout_ms_ = 60000;
    bool validate_only_ = false;
};

class CreatePartitionsResponse {
public:
    struct Result {
        std::string topic;
        ErrorCode error_code = ErrorCode::NONE;
        std::string error_message;
    };
    void setThrottleTimeMs(int32_t v) { throttle_time_ms_ = v; }
    void addResult(Result r) { results_.push_back(std::move(r)); }
    const std::vector<Result>& results() const { return results_; }
    void encode(Buffer& buf, int16_t v) const;
    void decode(Buffer& buf, int16_t v);

private:
    int32_t throttle_time_ms_ = 0;
    std::vector<Result> results_;
};

}  // namespace kawasan::protocol
