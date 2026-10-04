#include "kawasan/common/error.h"

namespace kawasan {

std::string KawasanException::toString(ErrorCode code) {
    switch (code) {
        case ErrorCode::NONE:
            return "No error";
        case ErrorCode::OFFSET_OUT_OF_RANGE:
            return "Offset out of range";
        case ErrorCode::CORRUPT_MESSAGE:
            return "Corrupt message";
        case ErrorCode::UNKNOWN_TOPIC_OR_PARTITION:
            return "Unknown topic or partition";
        case ErrorCode::INVALID_FETCH_SIZE:
            return "Invalid fetch size";
        case ErrorCode::LEADER_NOT_AVAILABLE:
            return "Leader not available";
        case ErrorCode::NOT_LEADER_FOR_PARTITION:
            return "Not leader for partition";
        case ErrorCode::REQUEST_TIMED_OUT:
            return "Request timed out";
        case ErrorCode::BROKER_NOT_AVAILABLE:
            return "Broker not available";
        case ErrorCode::REPLICA_NOT_AVAILABLE:
            return "Replica not available";
        case ErrorCode::MESSAGE_TOO_LARGE:
            return "Message too large";
        case ErrorCode::STALE_CONTROLLER_EPOCH:
            return "Stale controller epoch";
        case ErrorCode::OFFSET_METADATA_TOO_LARGE:
            return "Offset metadata too large";
        case ErrorCode::NETWORK_EXCEPTION:
            return "Network exception";
        case ErrorCode::COORDINATOR_LOAD_IN_PROGRESS:
            return "Coordinator load in progress";
        case ErrorCode::COORDINATOR_NOT_AVAILABLE:
            return "Coordinator not available";
        case ErrorCode::NOT_COORDINATOR:
            return "Not coordinator";
        case ErrorCode::INVALID_TOPIC_EXCEPTION:
            return "Invalid topic";
        case ErrorCode::RECORD_LIST_TOO_LARGE:
            return "Record list too large";
        case ErrorCode::NOT_ENOUGH_REPLICAS:
            return "Not enough replicas";
        case ErrorCode::NOT_ENOUGH_REPLICAS_AFTER_APPEND:
            return "Not enough replicas after append";
        case ErrorCode::INVALID_REQUIRED_ACKS:
            return "Invalid required acks";
        case ErrorCode::ILLEGAL_GENERATION:
            return "Illegal generation";
        case ErrorCode::INCONSISTENT_GROUP_PROTOCOL:
            return "Inconsistent group protocol";
        case ErrorCode::INVALID_GROUP_ID:
            return "Invalid group ID";
        case ErrorCode::UNKNOWN_MEMBER_ID:
            return "Unknown member ID";
        case ErrorCode::INVALID_SESSION_TIMEOUT:
            return "Invalid session timeout";
        case ErrorCode::REBALANCE_IN_PROGRESS:
            return "Rebalance in progress";
        case ErrorCode::INVALID_COMMIT_OFFSET_SIZE:
            return "Invalid commit offset size";
        case ErrorCode::TOPIC_AUTHORIZATION_FAILED:
            return "Topic authorization failed";
        case ErrorCode::GROUP_AUTHORIZATION_FAILED:
            return "Group authorization failed";
        case ErrorCode::CLUSTER_AUTHORIZATION_FAILED:
            return "Cluster authorization failed";
        case ErrorCode::INVALID_TIMESTAMP:
            return "Invalid timestamp";
        case ErrorCode::UNSUPPORTED_SASL_MECHANISM:
            return "Unsupported SASL mechanism";
        case ErrorCode::ILLEGAL_SASL_STATE:
            return "Illegal SASL state";
        case ErrorCode::UNSUPPORTED_VERSION:
            return "Unsupported version";
        case ErrorCode::TOPIC_ALREADY_EXISTS:
            return "Topic already exists";
        case ErrorCode::INVALID_PARTITIONS:
            return "Invalid partitions";
        case ErrorCode::INVALID_REPLICATION_FACTOR:
            return "Invalid replication factor";
        case ErrorCode::INVALID_REPLICA_ASSIGNMENT:
            return "Invalid replica assignment";
        case ErrorCode::INVALID_CONFIG:
            return "Invalid config";
        case ErrorCode::NOT_CONTROLLER:
            return "Not controller";
        case ErrorCode::INVALID_REQUEST:
            return "Invalid request";
        case ErrorCode::UNSUPPORTED_FOR_MESSAGE_FORMAT:
            return "Unsupported for message format";
        case ErrorCode::POLICY_VIOLATION:
            return "Policy violation";
        case ErrorCode::OUT_OF_ORDER_SEQUENCE_NUMBER:
            return "Out of order sequence number";
        case ErrorCode::DUPLICATE_SEQUENCE_NUMBER:
            return "Duplicate sequence number";
        case ErrorCode::INVALID_PRODUCER_EPOCH:
            return "Invalid producer epoch";
        case ErrorCode::INVALID_TXN_STATE:
            return "Invalid transaction state";
        case ErrorCode::INVALID_PRODUCER_ID_MAPPING:
            return "Invalid producer ID mapping";
        case ErrorCode::INVALID_TRANSACTION_TIMEOUT:
            return "Invalid transaction timeout";
        case ErrorCode::CONCURRENT_TRANSACTIONS:
            return "Concurrent transactions";
        case ErrorCode::TRANSACTION_COORDINATOR_FENCED:
            return "Transaction coordinator fenced";
        case ErrorCode::TRANSACTIONAL_ID_AUTHORIZATION_FAILED:
            return "Transactional ID authorization failed";
        case ErrorCode::SECURITY_DISABLED:
            return "Security disabled";
        case ErrorCode::OPERATION_NOT_ATTEMPTED:
            return "Operation not attempted";
        case ErrorCode::KAFKA_STORAGE_ERROR:
            return "Kafka storage error";
        case ErrorCode::LOG_DIR_NOT_FOUND:
            return "Log directory not found";
        case ErrorCode::SASL_AUTHENTICATION_FAILED:
            return "SASL authentication failed";
        case ErrorCode::UNKNOWN_PRODUCER_ID:
            return "Unknown producer ID";
        case ErrorCode::REASSIGNMENT_IN_PROGRESS:
            return "Reassignment in progress";
        case ErrorCode::DELEGATION_TOKEN_AUTH_DISABLED:
            return "Delegation token auth disabled";
        case ErrorCode::DELEGATION_TOKEN_NOT_FOUND:
            return "Delegation token not found";
        case ErrorCode::DELEGATION_TOKEN_OWNER_MISMATCH:
            return "Delegation token owner mismatch";
        case ErrorCode::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED:
            return "Delegation token request not allowed";
        case ErrorCode::DELEGATION_TOKEN_AUTHORIZATION_FAILED:
            return "Delegation token authorization failed";
        case ErrorCode::DELEGATION_TOKEN_EXPIRED:
            return "Delegation token expired";
        case ErrorCode::INVALID_PRINCIPAL_TYPE:
            return "Invalid principal type";
        case ErrorCode::NON_EMPTY_GROUP:
            return "Non-empty group";
        case ErrorCode::GROUP_ID_NOT_FOUND:
            return "Group ID not found";
        case ErrorCode::FETCH_SESSION_ID_NOT_FOUND:
            return "Fetch session ID not found";
        case ErrorCode::INVALID_FETCH_SESSION_EPOCH:
            return "Invalid fetch session epoch";
        case ErrorCode::LISTENER_NOT_FOUND:
            return "Listener not found";
        case ErrorCode::TOPIC_DELETION_DISABLED:
            return "Topic deletion disabled";
        case ErrorCode::FENCED_LEADER_EPOCH:
            return "Fenced leader epoch";
        case ErrorCode::UNKNOWN_LEADER_EPOCH:
            return "Unknown leader epoch";
        case ErrorCode::UNSUPPORTED_COMPRESSION_TYPE:
            return "Unsupported compression type";
        case ErrorCode::STALE_BROKER_EPOCH:
            return "Stale broker epoch";
        case ErrorCode::OFFSET_NOT_AVAILABLE:
            return "Offset not available";
        case ErrorCode::MEMBER_ID_REQUIRED:
            return "Member ID required";
        case ErrorCode::PREFERRED_LEADER_NOT_AVAILABLE:
            return "Preferred leader not available";
        case ErrorCode::GROUP_MAX_SIZE_REACHED:
            return "Group max size reached";
        case ErrorCode::FENCED_INSTANCE_ID:
            return "Fenced instance ID";
        case ErrorCode::UNSTABLE_OFFSET_COMMIT:
            return "Unstable offset commit";
        case ErrorCode::UNKNOWN_TOPIC_ID:
            return "Unknown topic ID";
        case ErrorCode::INVALID_UPDATE_VERSION:
            return "Invalid partition update version";
        default:
            return "Unknown error";
    }
}

}  // namespace kawasan
