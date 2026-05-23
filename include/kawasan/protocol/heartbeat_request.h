#pragma once

#include <optional>
#include <string>

#include "kawasan/common/buffer.h"
#include "kawasan/common/error.h"

namespace kawasan::protocol {

/// @brief Heartbeat request
///
/// Phase 1.9: supports v0–v4.
///   v0–v2: group_id, generation_id, member_id (legacy STRING)
///   v3:    + group_instance_id NULLABLE_STRING (KIP-345 static membership)
///   v4:    flexible — compact strings + tagged fields
class HeartbeatRequest {
public:
    HeartbeatRequest() = default;

    const std::string& groupId() const { return group_id_; }
    int32_t generationId() const { return generation_id_; }
    const std::string& memberId() const { return member_id_; }
    const std::optional<std::string>& groupInstanceId() const { return group_instance_id_; }

    void setGroupId(const std::string& id) { group_id_ = id; }
    void setGenerationId(int32_t generation) { generation_id_ = generation; }
    void setMemberId(const std::string& member) { member_id_ = member; }
    void setGroupInstanceId(std::optional<std::string> v) { group_instance_id_ = std::move(v); }

    void encode(Buffer& buffer, int16_t api_version) const;
    void decode(Buffer& buffer, int16_t api_version);

private:
    std::string group_id_;
    int32_t generation_id_ = 0;
    std::string member_id_;
    std::optional<std::string> group_instance_id_;
};

/// @brief Heartbeat response
///
/// Phase 1.9:
///   v0:    error_code
///   v1+:   + throttle_time_ms (placed first)
///   v4:    flexible (tagged fields trailer)
class HeartbeatResponse {
public:
    HeartbeatResponse() = default;

    void setErrorCode(ErrorCode code) { error_code_ = code; }
    void setThrottleTimeMs(int32_t v) { throttle_time_ms_ = v; }
    ErrorCode errorCode() const { return error_code_; }
    int32_t throttleTimeMs() const { return throttle_time_ms_; }

    void encode(Buffer& buffer, int16_t api_version) const;
    void decode(Buffer& buffer, int16_t api_version);

private:
    ErrorCode error_code_ = ErrorCode::NONE;
    int32_t throttle_time_ms_ = 0;
};

}  // namespace kawasan::protocol
