#pragma once

#include <array>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace kawasan {

// Basic types
using Offset = int64_t;
using Timestamp = int64_t;
using PartitionId = int32_t;
using BrokerId = int32_t;
using CorrelationId = int32_t;

// Error codes (matching Kafka error codes)
enum class ErrorCode : int16_t {
    NONE = 0,
    OFFSET_OUT_OF_RANGE = 1,
    CORRUPT_MESSAGE = 2,
    UNKNOWN_TOPIC_OR_PARTITION = 3,
    INVALID_FETCH_SIZE = 4,
    LEADER_NOT_AVAILABLE = 5,
    NOT_LEADER_FOR_PARTITION = 6,
    REQUEST_TIMED_OUT = 7,
    BROKER_NOT_AVAILABLE = 8,
    REPLICA_NOT_AVAILABLE = 9,
    MESSAGE_TOO_LARGE = 10,
    STALE_CONTROLLER_EPOCH = 11,
    OFFSET_METADATA_TOO_LARGE = 12,
    NETWORK_EXCEPTION = 13,
    COORDINATOR_LOAD_IN_PROGRESS = 14,
    COORDINATOR_NOT_AVAILABLE = 15,
    NOT_COORDINATOR = 16,
    INVALID_TOPIC_EXCEPTION = 17,
    RECORD_LIST_TOO_LARGE = 18,
    NOT_ENOUGH_REPLICAS = 19,
    NOT_ENOUGH_REPLICAS_AFTER_APPEND = 20,
    INVALID_REQUIRED_ACKS = 21,
    ILLEGAL_GENERATION = 22,
    INCONSISTENT_GROUP_PROTOCOL = 23,
    INVALID_GROUP_ID = 24,
    UNKNOWN_MEMBER_ID = 25,
    INVALID_SESSION_TIMEOUT = 26,
    REBALANCE_IN_PROGRESS = 27,
    INVALID_COMMIT_OFFSET_SIZE = 28,
    TOPIC_AUTHORIZATION_FAILED = 29,
    GROUP_AUTHORIZATION_FAILED = 30,
    CLUSTER_AUTHORIZATION_FAILED = 31,
    INVALID_TIMESTAMP = 32,
    UNSUPPORTED_SASL_MECHANISM = 33,
    ILLEGAL_SASL_STATE = 34,
    UNSUPPORTED_VERSION = 35,
    TOPIC_ALREADY_EXISTS = 36,
    INVALID_PARTITIONS = 37,
    INVALID_REPLICATION_FACTOR = 38,
    INVALID_REPLICA_ASSIGNMENT = 39,
    INVALID_CONFIG = 40,
    NOT_CONTROLLER = 41,
    INVALID_REQUEST = 42,
    UNSUPPORTED_FOR_MESSAGE_FORMAT = 43,
    POLICY_VIOLATION = 44,
    OUT_OF_ORDER_SEQUENCE_NUMBER = 45,
    DUPLICATE_SEQUENCE_NUMBER = 46,
    INVALID_PRODUCER_EPOCH = 47,
    INVALID_TXN_STATE = 48,
    INVALID_PRODUCER_ID_MAPPING = 49,
    INVALID_TRANSACTION_TIMEOUT = 50,
    CONCURRENT_TRANSACTIONS = 51,
    TRANSACTION_COORDINATOR_FENCED = 52,
    TRANSACTIONAL_ID_AUTHORIZATION_FAILED = 53,
    SECURITY_DISABLED = 54,
    OPERATION_NOT_ATTEMPTED = 55,
    KAFKA_STORAGE_ERROR = 56,
    LOG_DIR_NOT_FOUND = 57,
    SASL_AUTHENTICATION_FAILED = 58,
    UNKNOWN_PRODUCER_ID = 59,
    REASSIGNMENT_IN_PROGRESS = 60,
    DELEGATION_TOKEN_AUTH_DISABLED = 61,
    DELEGATION_TOKEN_NOT_FOUND = 62,
    DELEGATION_TOKEN_OWNER_MISMATCH = 63,
    DELEGATION_TOKEN_REQUEST_NOT_ALLOWED = 64,
    DELEGATION_TOKEN_AUTHORIZATION_FAILED = 65,
    DELEGATION_TOKEN_EXPIRED = 66,
    INVALID_PRINCIPAL_TYPE = 67,
    NON_EMPTY_GROUP = 68,
    GROUP_ID_NOT_FOUND = 69,
    FETCH_SESSION_ID_NOT_FOUND = 70,
    INVALID_FETCH_SESSION_EPOCH = 71,
    LISTENER_NOT_FOUND = 72,
    TOPIC_DELETION_DISABLED = 73,
    FENCED_LEADER_EPOCH = 74,
    UNKNOWN_LEADER_EPOCH = 75,
    UNSUPPORTED_COMPRESSION_TYPE = 76,
    STALE_BROKER_EPOCH = 77,
    OFFSET_NOT_AVAILABLE = 78,
    MEMBER_ID_REQUIRED = 79,
    PREFERRED_LEADER_NOT_AVAILABLE = 80,
    GROUP_MAX_SIZE_REACHED = 81,
    FENCED_INSTANCE_ID = 82,
    UNSTABLE_OFFSET_COMMIT = 88,
    INVALID_UPDATE_VERSION = 95,
    UNKNOWN_TOPIC_ID = 100,
};

// Compression types
enum class CompressionType : int8_t {
    NONE = 0,
    GZIP = 1,
    SNAPPY = 2,
    LZ4 = 3,
    ZSTD = 4,
};

// Record batch attributes
struct RecordBatchAttributes {
    CompressionType compression;
    bool is_transactional;
    bool is_control_batch;

    static constexpr int16_t encode(CompressionType comp, bool transactional, bool control) {
        return static_cast<int16_t>(comp) | (transactional ? (1 << 4) : 0) |
               (control ? (1 << 5) : 0);
    }
};

// Topic partition
struct TopicPartition {
    std::string topic;
    PartitionId partition;

    bool operator==(const TopicPartition& other) const {
        return topic == other.topic && partition == other.partition;
    }

    bool operator<(const TopicPartition& other) const {
        if (topic != other.topic) {
            return topic < other.topic;
        }
        return partition < other.partition;
    }
};

// Record header
struct RecordHeader {
    std::string key;
    std::vector<uint8_t> value;
};

// Record
struct Record {
    Timestamp timestamp;
    std::optional<std::vector<uint8_t>> key;
    std::optional<std::vector<uint8_t>> value;
    std::vector<RecordHeader> headers;
    // 0A.3: preserve the producer-supplied offset delta so that on re-encode
    // the wire bytes stay byte-identical (mod CRC of the surrounding batch).
    // Idempotent producer sequence-vs-offset checks rely on this. A sentinel
    // of -1 means "not set" — encoders should fall back to the insertion index
    // in that case, preserving today's behavior for freshly-constructed records.
    int32_t offset_delta = -1;

    Record() : timestamp(0) {}

    Record(const std::string& k, const std::string& v)
        : timestamp(0),
          key(std::vector<uint8_t>(k.begin(), k.end())),
          value(std::vector<uint8_t>(v.begin(), v.end())) {}
};

// Broker metadata
struct BrokerMetadata {
    BrokerId id;
    std::string host;
    int32_t port;
    std::optional<std::string> rack;
};

// Partition metadata
struct PartitionMetadata {
    ErrorCode error_code;
    PartitionId partition;
    BrokerId leader;
    int32_t leader_epoch;
    std::vector<BrokerId> replicas;
    std::vector<BrokerId> isr;  // In-sync replicas
    std::vector<BrokerId> offline_replicas;
    int32_t partition_epoch = 0;  // Version of leader and ISR metadata mutations.
};

// Topic metadata
struct TopicMetadata {
    ErrorCode error_code;
    std::string name;
    // Phase 1.2: topic_id is a 16-byte UUID (KIP-516, Metadata v10+). Leaves
    // all-zeros until the metadata controller assigns IDs.
    std::array<uint8_t, 16> topic_id{};
    bool is_internal;
    // Phase 1.2: kafka-python's ACLOperation parser is known-buggy when this
    // is INT32_MIN (the real Kafka "unset" sentinel) — it iterates the bit
    // positions and chokes on bit 31. Until the authorizer interface is wired
    // (Phase 4), return 0 (no operations granted), which both kafka-python and
    // Java client tolerate. Producer/consumer paths don't consult this field.
    int32_t topic_authorized_operations = 0;  // v8+
    std::vector<PartitionMetadata> partitions;
};

}  // namespace kawasan
