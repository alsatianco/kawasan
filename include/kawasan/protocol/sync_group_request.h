#pragma once

#include <optional>
#include <string>
#include <vector>

#include "kawasan/common/buffer.h"
#include "kawasan/common/error.h"

namespace kawasan::protocol {

/// @brief SyncGroup request
///
/// Phase 1.8: supports v0–v5.
///   v0–v2: group_id, generation_id, member_id, assignments[]
///   v3:    + group_instance_id NULLABLE_STRING (KIP-345 static membership)
///   v4:    flexible (compact strings + tagged fields)
///   v5:    + protocol_type NULLABLE_STRING + protocol_name NULLABLE_STRING (KIP-559)
class SyncGroupRequest {
public:
    struct Assignment {
        std::string member_id;
        std::vector<uint8_t> assignment;
    };

    SyncGroupRequest() = default;

    const std::string& groupId() const { return group_id_; }
    int32_t generationId() const { return generation_id_; }
    const std::string& memberId() const { return member_id_; }
    const std::optional<std::string>& groupInstanceId() const { return group_instance_id_; }
    const std::optional<std::string>& protocolType() const { return protocol_type_; }
    const std::optional<std::string>& protocolName() const { return protocol_name_; }
    const std::vector<Assignment>& assignments() const { return assignments_; }

    void setGroupId(const std::string& id) { group_id_ = id; }
    void setGenerationId(int32_t generation) { generation_id_ = generation; }
    void setMemberId(const std::string& member) { member_id_ = member; }
    void setGroupInstanceId(std::optional<std::string> v) { group_instance_id_ = std::move(v); }
    void setProtocolType(std::optional<std::string> v) { protocol_type_ = std::move(v); }
    void setProtocolName(std::optional<std::string> v) { protocol_name_ = std::move(v); }
    void setAssignments(const std::vector<Assignment>& assignments) {
        assignments_ = assignments;
    }

    void encode(Buffer& buffer, int16_t api_version) const;
    void decode(Buffer& buffer, int16_t api_version);

private:
    std::string group_id_;
    int32_t generation_id_ = 0;
    std::string member_id_;
    std::optional<std::string> group_instance_id_;  // v3+
    std::optional<std::string> protocol_type_;       // v5+
    std::optional<std::string> protocol_name_;       // v5+
    std::vector<Assignment> assignments_;
};

/// @brief SyncGroup response
///
///   v0:    error_code, assignment
///   v1+:   + throttle_time_ms (first)
///   v4:    flexible
///   v5:    + protocol_type NULLABLE_STRING + protocol_name NULLABLE_STRING
class SyncGroupResponse {
public:
    SyncGroupResponse() = default;

    void setErrorCode(ErrorCode code) { error_code_ = code; }
    void setThrottleTimeMs(int32_t v) { throttle_time_ms_ = v; }
    void setProtocolType(std::optional<std::string> v) { protocol_type_ = std::move(v); }
    void setProtocolName(std::optional<std::string> v) { protocol_name_ = std::move(v); }
    void setAssignment(const std::vector<uint8_t>& assignment) {
        assignment_ = assignment;
    }

    ErrorCode errorCode() const { return error_code_; }
    int32_t throttleTimeMs() const { return throttle_time_ms_; }
    const std::optional<std::string>& protocolType() const { return protocol_type_; }
    const std::optional<std::string>& protocolName() const { return protocol_name_; }
    const std::vector<uint8_t>& assignment() const { return assignment_; }

    void encode(Buffer& buffer, int16_t api_version) const;
    void decode(Buffer& buffer, int16_t api_version);

private:
    ErrorCode error_code_ = ErrorCode::NONE;
    int32_t throttle_time_ms_ = 0;
    std::optional<std::string> protocol_type_;
    std::optional<std::string> protocol_name_;
    std::vector<uint8_t> assignment_;
};

}  // namespace kawasan::protocol
