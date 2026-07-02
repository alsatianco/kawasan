// Phase 4.1 (j, k, l, m) + 4.2c — minimal stubs.
//
// These APIs are wired so clients (especially Kafka UI and AdminClient
// surveys) get a valid empty response instead of UNSUPPORTED_VERSION. Real
// behavior will arrive with:
//   - ProducerStateManager (Phase 2.1) → DescribeProducers populates entries
//   - TransactionCoordinator (Phase 3.3) → Describe/ListTransactions populate
//   - Authorizer (Phase 4.2 follow-up) → ACL APIs persist + enforce

#pragma once

#include <optional>
#include <string>
#include <vector>

#include "kawasan/common/buffer.h"
#include "kawasan/common/error.h"

namespace kawasan::protocol {

// ---- DescribeProducers (61) ----
class DescribeProducersRequest {
public:
    struct TopicSpec {
        std::string topic;
        std::vector<int32_t> partitions;
    };
    const std::vector<TopicSpec>& topics() const { return topics_; }
    void encode(Buffer& buf, int16_t v) const;
    void decode(Buffer& buf, int16_t v);

private:
    std::vector<TopicSpec> topics_;
};

class DescribeProducersResponse {
public:
    struct ActiveProducer {
        int64_t producer_id = -1;
        int32_t producer_epoch = -1;
        int32_t last_sequence = -1;
        int64_t last_timestamp = -1;
        int32_t coordinator_epoch = -1;
        int64_t current_txn_start_offset = -1;
    };
    struct PartitionResult {
        int32_t partition = 0;
        ErrorCode error_code = ErrorCode::NONE;
        std::string error_message;
        std::vector<ActiveProducer> active_producers;
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

// ---- ListTransactions (66) ----
class ListTransactionsRequest {
public:
    const std::vector<std::string>& stateFilters() const { return state_filters_; }
    const std::vector<int64_t>& producerIdFilters() const { return producer_id_filters_; }
    void encode(Buffer& buf, int16_t v) const;
    void decode(Buffer& buf, int16_t v);

private:
    std::vector<std::string> state_filters_;
    std::vector<int64_t> producer_id_filters_;
};

class ListTransactionsResponse {
public:
    struct TxnState {
        std::string transactional_id;
        int64_t producer_id = -1;
        std::string state;
    };
    void setThrottleTimeMs(int32_t v) { throttle_time_ms_ = v; }
    void setErrorCode(ErrorCode v) { error_code_ = v; }
    void addState(TxnState s) { states_.push_back(std::move(s)); }
    void encode(Buffer& buf, int16_t v) const;
    void decode(Buffer& buf, int16_t v);

private:
    int32_t throttle_time_ms_ = 0;
    ErrorCode error_code_ = ErrorCode::NONE;
    std::vector<std::string> unknown_state_filters_;
    std::vector<TxnState> states_;
};

// ---- DescribeTransactions (65) ----
class DescribeTransactionsRequest {
public:
    const std::vector<std::string>& transactionalIds() const { return ids_; }
    void encode(Buffer& buf, int16_t v) const;
    void decode(Buffer& buf, int16_t v);

private:
    std::vector<std::string> ids_;
};

class DescribeTransactionsResponse {
public:
    struct TopicData {
        std::string topic;
        std::vector<int32_t> partitions;
    };
    struct State {
        ErrorCode error_code = ErrorCode::NONE;
        std::string transactional_id;
        std::string state;
        int32_t transaction_timeout_ms = 0;
        int64_t transaction_start_time_ms = -1;
        int64_t producer_id = -1;
        int16_t producer_epoch = -1;  // INT16 on the wire (Kafka schema)
        std::vector<TopicData> topics;
    };
    void setThrottleTimeMs(int32_t v) { throttle_time_ms_ = v; }
    void addState(State s) { states_.push_back(std::move(s)); }
    void encode(Buffer& buf, int16_t v) const;
    void decode(Buffer& buf, int16_t v);

private:
    int32_t throttle_time_ms_ = 0;
    std::vector<State> states_;
};

// ---- AlterPartition (56) ----
// Full v0 wire codec (KIP-497). The handler is still a single-broker no-op;
// the controller-authoritative ISR path (roadmap M6) drives these fields.
class AlterPartitionRequest {
public:
    struct PartitionData {
        int32_t partition_index = 0;
        int32_t leader_epoch = -1;
        std::vector<int32_t> new_isr;
        int32_t partition_epoch = 0;
    };
    struct TopicData {
        std::string topic_name;
        std::vector<PartitionData> partitions;
    };
    int32_t broker_id = -1;
    int64_t broker_epoch = -1;
    std::vector<TopicData> topics;

    void encode(Buffer& buf, int16_t v) const;
    void decode(Buffer& buf, int16_t v);
};
class AlterPartitionResponse {
public:
    struct PartitionResult {
        int32_t partition_index = 0;
        ErrorCode error_code = ErrorCode::NONE;
        int32_t leader_id = -1;
        int32_t leader_epoch = -1;
        std::vector<int32_t> isr;
        int32_t partition_epoch = 0;
    };
    struct TopicResult {
        std::string topic_name;
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

// ---- DescribeAcls (29) / CreateAcls (30) / DeleteAcls (31) — stubs ----
// All three return empty/success — single-broker ALLOW-ALL until authorizer
// lands (Phase 4 follow-up). Wire format kept minimal but valid so clients
// don't crash when surveying the cluster.

// ---- ACL types (Phase 4.2c) ----
//
// Kafka ACL constants:
//   ResourceType:        UNKNOWN(0), ANY(1), TOPIC(2), GROUP(3),
//                        CLUSTER(4), TRANSACTIONAL_ID(5),
//                        DELEGATION_TOKEN(6), USER(7)
//   ResourcePatternType: UNKNOWN(0), ANY(1), MATCH(2), LITERAL(3),
//                        PREFIXED(4)
//   AclOperation:        UNKNOWN(0), ANY(1), ALL(2), READ(3), WRITE(4),
//                        CREATE(5), DELETE(6), ALTER(7), DESCRIBE(8),
//                        CLUSTER_ACTION(9), DESCRIBE_CONFIGS(10),
//                        ALTER_CONFIGS(11), IDEMPOTENT_WRITE(12)
//   AclPermissionType:   UNKNOWN(0), ANY(1), DENY(2), ALLOW(3)
struct AclBinding {
    int8_t resource_type = 0;
    std::string resource_name;
    int8_t pattern_type = 3;  // LITERAL
    std::string principal;
    std::string host;
    int8_t operation = 0;
    int8_t permission_type = 0;
};

class DescribeAclsRequest {
public:
    // Filter fields (matching Kafka v0+).
    int8_t resource_type = 1;          // ANY by default
    std::string resource_name_filter;  // empty = ANY
    int8_t pattern_type = 1;           // ANY (v1+)
    std::string principal_filter;
    std::string host_filter;
    int8_t operation = 1;        // ANY
    int8_t permission_type = 1;  // ANY

    void encode(Buffer& buf, int16_t v) const;
    void decode(Buffer& buf, int16_t v);
};

class DescribeAclsResponse {
public:
    struct Resource {
        int8_t resource_type = 0;
        std::string resource_name;
        int8_t pattern_type = 3;  // LITERAL (v1+)
        struct Acl {
            std::string principal;
            std::string host;
            int8_t operation = 0;
            int8_t permission_type = 0;
        };
        std::vector<Acl> acls;
    };
    void setThrottleTimeMs(int32_t v) { throttle_time_ms_ = v; }
    void setErrorCode(ErrorCode v) { error_code_ = v; }
    void setErrorMessage(const std::string& v) { error_message_ = v; }
    void addResource(Resource r) { resources_.push_back(std::move(r)); }
    void encode(Buffer& buf, int16_t v) const;
    void decode(Buffer& buf, int16_t v);

private:
    int32_t throttle_time_ms_ = 0;
    ErrorCode error_code_ = ErrorCode::NONE;
    std::string error_message_;
    std::vector<Resource> resources_;
};

class CreateAclsRequest {
public:
    std::vector<AclBinding> creations;
    void encode(Buffer&, int16_t) const;
    void decode(Buffer&, int16_t);
};

class CreateAclsResponse {
public:
    struct Result {
        ErrorCode error_code = ErrorCode::NONE;
        std::string error_message;
    };
    void setThrottleTimeMs(int32_t v) { throttle_time_ms_ = v; }
    void addResult(Result r) { results_.push_back(std::move(r)); }
    void encode(Buffer& buf, int16_t v) const;
    void decode(Buffer& buf, int16_t v);

private:
    int32_t throttle_time_ms_ = 0;
    std::vector<Result> results_;
};

class DeleteAclsRequest {
public:
    // Filters — each is the same as DescribeAclsRequest fields.
    struct Filter {
        int8_t resource_type = 1;
        std::string resource_name_filter;
        int8_t pattern_type = 1;
        std::string principal_filter;
        std::string host_filter;
        int8_t operation = 1;
        int8_t permission_type = 1;
    };
    std::vector<Filter> filters;
    void encode(Buffer&, int16_t) const;
    void decode(Buffer&, int16_t);
};

class DeleteAclsResponse {
public:
    struct MatchingAcl {
        ErrorCode error_code = ErrorCode::NONE;
        std::string error_message;
        AclBinding binding;
    };
    struct FilterResult {
        ErrorCode error_code = ErrorCode::NONE;
        std::string error_message;
        std::vector<MatchingAcl> matches;
    };
    void setThrottleTimeMs(int32_t v) { throttle_time_ms_ = v; }
    void addFilterResult(FilterResult r) { filter_results_.push_back(std::move(r)); }
    void encode(Buffer& buf, int16_t v) const;
    void decode(Buffer& buf, int16_t v);

private:
    int32_t throttle_time_ms_ = 0;
    std::vector<FilterResult> filter_results_;
};

}  // namespace kawasan::protocol
