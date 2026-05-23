#pragma once

#include <optional>
#include <string>
#include <vector>

#include "kawasan/common/buffer.h"
#include "kawasan/common/error.h"

namespace kawasan::protocol {

/// @brief JoinGroup request
///
/// Phase 1.7: supports v0–v9.
///   v0:    group_id, session_timeout_ms, member_id, protocol_type, group_protocols[]
///   v1+:   + rebalance_timeout_ms
///   v4+:   MEMBER_ID_REQUIRED handshake supported
///   v5+:   + group_instance_id NULLABLE_STRING (KIP-345 static membership)
///   v6+:   flexible (compact strings + tagged fields)
///   v8+:   + reason NULLABLE_STRING (KIP-800)
class JoinGroupRequest {
public:
    struct GroupProtocol {
        std::string name;
        std::vector<uint8_t> metadata;
    };

    JoinGroupRequest() = default;

    const std::string& groupId() const { return group_id_; }
    int32_t sessionTimeoutMs() const { return session_timeout_ms_; }
    int32_t rebalanceTimeoutMs() const { return rebalance_timeout_ms_; }
    const std::string& memberId() const { return member_id_; }
    const std::optional<std::string>& groupInstanceId() const { return group_instance_id_; }
    const std::string& protocolType() const { return protocol_type_; }
    const std::vector<GroupProtocol>& groupProtocols() const { return group_protocols_; }
    const std::optional<std::string>& reason() const { return reason_; }

    void setGroupId(const std::string& id) { group_id_ = id; }
    void setSessionTimeoutMs(int32_t timeout) { session_timeout_ms_ = timeout; }
    void setRebalanceTimeoutMs(int32_t timeout) { rebalance_timeout_ms_ = timeout; }
    void setMemberId(const std::string& member) { member_id_ = member; }
    void setGroupInstanceId(std::optional<std::string> v) { group_instance_id_ = std::move(v); }
    void setProtocolType(const std::string& type) { protocol_type_ = type; }
    void setGroupProtocols(const std::vector<GroupProtocol>& protocols) {
        group_protocols_ = protocols;
    }
    void setReason(std::optional<std::string> v) { reason_ = std::move(v); }

    void encode(Buffer& buffer, int16_t api_version) const;
    void decode(Buffer& buffer, int16_t api_version);

private:
    std::string group_id_;
    int32_t session_timeout_ms_ = 10000;
    int32_t rebalance_timeout_ms_ = 10000;
    std::string member_id_;
    std::optional<std::string> group_instance_id_;  // v5+
    std::string protocol_type_;
    std::vector<GroupProtocol> group_protocols_;
    std::optional<std::string> reason_;  // v8+
};

/// @brief JoinGroup response
///
///   v0:    error_code, generation_id, group_protocol(STRING), leader_id, member_id, members[]
///   v2+:   + throttle_time_ms (first)
///   v5+:   members.group_instance_id NULLABLE_STRING
///   v6+:   flexible
///   v7+:   group_protocol replaced by protocol_type NULLABLE_STRING + protocol_name NULLABLE_STRING
class JoinGroupResponse {
public:
    struct Member {
        std::string member_id;
        std::optional<std::string> group_instance_id;  // v5+
        std::vector<uint8_t> metadata;
    };

    JoinGroupResponse() = default;

    void setErrorCode(ErrorCode code) { error_code_ = code; }
    void setThrottleTimeMs(int32_t v) { throttle_time_ms_ = v; }
    void setGenerationId(int32_t id) { generation_id_ = id; }
    void setGroupProtocol(const std::string& protocol) { group_protocol_ = protocol; }
    void setProtocolType(std::optional<std::string> v) { protocol_type_ = std::move(v); }
    void setProtocolName(std::optional<std::string> v) { protocol_name_ = std::move(v); }
    void setLeaderId(const std::string& leader) { leader_id_ = leader; }
    void setMemberId(const std::string& member) { member_id_ = member; }
    void setMembers(const std::vector<Member>& members) { members_ = members; }

    ErrorCode errorCode() const { return error_code_; }
    int32_t throttleTimeMs() const { return throttle_time_ms_; }
    int32_t generationId() const { return generation_id_; }
    const std::string& groupProtocol() const { return group_protocol_; }
    const std::optional<std::string>& protocolType() const { return protocol_type_; }
    const std::optional<std::string>& protocolName() const { return protocol_name_; }
    const std::string& leaderId() const { return leader_id_; }
    const std::string& memberId() const { return member_id_; }
    const std::vector<Member>& members() const { return members_; }

    void encode(Buffer& buffer, int16_t api_version) const;
    void decode(Buffer& buffer, int16_t api_version);

private:
    ErrorCode error_code_ = ErrorCode::NONE;
    int32_t throttle_time_ms_ = 0;
    int32_t generation_id_ = 0;
    std::string group_protocol_;
    std::optional<std::string> protocol_type_;  // v7+ — replaces group_protocol
    std::optional<std::string> protocol_name_;  // v7+
    std::string leader_id_;
    std::string member_id_;
    std::vector<Member> members_;
};

}  // namespace kawasan::protocol
