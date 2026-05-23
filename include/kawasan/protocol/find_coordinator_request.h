#pragma once

#include <string>
#include <vector>

#include "kawasan/common/buffer.h"
#include "kawasan/common/types.h"

namespace kawasan::protocol {

/// @brief Coordinator key type
enum class CoordinatorType : int8_t {
    GROUP = 0,
    TRANSACTION = 1,
};

/// @brief FindCoordinator request
///
/// Phase 1.6: supports v0–v4.
///   v0:    key STRING                                (group only)
///   v1+:   + key_type INT8 (group / transaction)
///   v3:    flexible (compact strings + tagged fields)
///   v4:    + coordinator_keys ARRAY<COMPACT_STRING>; legacy `key` removed
class FindCoordinatorRequest {
public:
    FindCoordinatorRequest() = default;

    // Legacy v0–v3 accessors (return the first/only key).
    const std::string& key() const {
        static const std::string kEmpty;
        return keys_.empty() ? kEmpty : keys_.front();
    }
    CoordinatorType keyType() const { return key_type_; }
    const std::vector<std::string>& keys() const { return keys_; }

    void setKey(const std::string& key) {
        keys_.clear();
        keys_.push_back(key);
    }
    void setKeyType(CoordinatorType type) { key_type_ = type; }
    void addKey(std::string key) { keys_.push_back(std::move(key)); }

    void encode(Buffer& buffer, int16_t api_version) const;
    void decode(Buffer& buffer, int16_t api_version);

private:
    std::vector<std::string> keys_;
    CoordinatorType key_type_ = CoordinatorType::GROUP;
};

/// @brief FindCoordinator response
///
///   v0:    error_code, node_id, host, port (single coordinator)
///   v1+:   + throttle_time_ms (first) + error_message NULLABLE_STRING
///   v3:    flexible
///   v4:    + coordinators ARRAY<{ key, node_id, host, port, error_code, error_message }>
class FindCoordinatorResponse {
public:
    struct Coordinator {
        std::string key;
        BrokerId node_id = -1;
        std::string host;
        int32_t port = 0;
        ErrorCode error_code = ErrorCode::NONE;
        std::string error_message;
    };

    FindCoordinatorResponse() = default;

    void setThrottleTimeMs(int32_t throttle) { throttle_time_ms_ = throttle; }
    void setErrorCode(ErrorCode code) { error_code_ = code; }
    void setErrorMessage(const std::string& message) { error_message_ = message; }
    void setNodeId(BrokerId id) { node_id_ = id; }
    void setHost(const std::string& host) { host_ = host; }
    void setPort(int32_t port) { port_ = port; }
    void addCoordinator(Coordinator c) { coordinators_.push_back(std::move(c)); }

    int32_t throttleTimeMs() const { return throttle_time_ms_; }
    ErrorCode errorCode() const { return error_code_; }
    const std::string& errorMessage() const { return error_message_; }
    BrokerId nodeId() const { return node_id_; }
    const std::string& host() const { return host_; }
    int32_t port() const { return port_; }
    const std::vector<Coordinator>& coordinators() const { return coordinators_; }

    void encode(Buffer& buffer, int16_t api_version) const;
    void decode(Buffer& buffer, int16_t api_version);

private:
    int32_t throttle_time_ms_ = 0;
    ErrorCode error_code_ = ErrorCode::NONE;
    std::string error_message_;
    BrokerId node_id_ = -1;
    std::string host_;
    int32_t port_ = 0;
    std::vector<Coordinator> coordinators_;  // v4+
};

}  // namespace kawasan::protocol
