#pragma once

#include <optional>
#include <string>
#include <vector>

#include "kawasan/common/buffer.h"
#include "kawasan/common/error.h"

namespace kawasan::protocol {

/// @brief LeaveGroup request
///
/// Phase 1.10: supports v0–v5.
///   v0–v2: group_id + single member_id (legacy single-member form)
///   v3+:   group_id + array of {member_id, group_instance_id} (KIP-345)
///   v4:    flexible (compact strings + tagged fields)
///   v5:    + reason NULLABLE_STRING per member (KIP-800)
///
/// `memberId()` returns the first member from `members_` for legacy access.
class LeaveGroupRequest {
public:
    struct Member {
        std::string member_id;
        std::optional<std::string> group_instance_id;
        std::optional<std::string> reason;  // v5+
    };

    LeaveGroupRequest() = default;

    const std::string& groupId() const { return group_id_; }
    const std::string& memberId() const {
        static const std::string kEmpty;
        return members_.empty() ? kEmpty : members_.front().member_id;
    }
    const std::vector<Member>& members() const { return members_; }

    void setGroupId(const std::string& id) { group_id_ = id; }
    void setMemberId(const std::string& member) {
        members_.clear();
        Member m;
        m.member_id = member;
        members_.push_back(std::move(m));
    }
    void addMember(Member m) { members_.push_back(std::move(m)); }

    void encode(Buffer& buffer, int16_t api_version) const;
    void decode(Buffer& buffer, int16_t api_version);

private:
    std::string group_id_;
    std::vector<Member> members_;
};

/// @brief LeaveGroup response
///
///   v0:    error_code
///   v1+:   + throttle_time_ms (placed first)
///   v3+:   + members ARRAY<{ member_id, group_instance_id, error_code }>
///   v4:    flexible
class LeaveGroupResponse {
public:
    struct MemberResult {
        std::string member_id;
        std::optional<std::string> group_instance_id;
        ErrorCode error_code = ErrorCode::NONE;
    };

    LeaveGroupResponse() = default;

    void setErrorCode(ErrorCode code) { error_code_ = code; }
    void setThrottleTimeMs(int32_t v) { throttle_time_ms_ = v; }
    void addMember(MemberResult m) { members_.push_back(std::move(m)); }

    ErrorCode errorCode() const { return error_code_; }
    int32_t throttleTimeMs() const { return throttle_time_ms_; }
    const std::vector<MemberResult>& members() const { return members_; }

    void encode(Buffer& buffer, int16_t api_version) const;
    void decode(Buffer& buffer, int16_t api_version);

private:
    ErrorCode error_code_ = ErrorCode::NONE;
    int32_t throttle_time_ms_ = 0;
    std::vector<MemberResult> members_;
};

}  // namespace kawasan::protocol
