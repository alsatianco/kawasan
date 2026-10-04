#pragma once

#include <optional>
#include <string>
#include <vector>

#include "kawasan/common/buffer.h"
#include "kawasan/common/error.h"

namespace kawasan::protocol {

/// @brief DescribeGroups request
///
/// Phase 1.13: supports v0–v5.
///   v0–v2: groups ARRAY<STRING>
///   v3+:   + include_authorized_operations BOOL
///   v5:    flexible
class DescribeGroupsRequest {
public:
    DescribeGroupsRequest() = default;

    const std::vector<std::string>& groups() const { return groups_; }
    bool includeAuthorizedOperations() const { return include_authorized_operations_; }

    void setGroups(const std::vector<std::string>& groups) { groups_ = groups; }
    void setIncludeAuthorizedOperations(bool v) { include_authorized_operations_ = v; }

    void encode(Buffer& buffer, int16_t api_version) const;
    void decode(Buffer& buffer, int16_t api_version);

private:
    std::vector<std::string> groups_;
    bool include_authorized_operations_ = false;
};

/// @brief DescribeGroups response
class DescribeGroupsResponse {
public:
    struct Member {
        std::string member_id;
        std::optional<std::string> group_instance_id;  // v4+
        std::string client_id;
        std::string client_host;
        std::vector<uint8_t> member_metadata;
        std::vector<uint8_t> member_assignment;
    };

    struct Group {
        ErrorCode error_code = ErrorCode::NONE;
        std::string group_id;
        std::string group_state;
        std::string protocol_type;
        std::string protocol_data;
        std::vector<Member> members;
        int32_t authorized_operations = -2147483648;  // v3+; bitmask, sentinel = unset
    };

    DescribeGroupsResponse() = default;

    void setThrottleTimeMs(int32_t throttle_time_ms) { throttle_time_ms_ = throttle_time_ms; }
    void setGroups(const std::vector<Group>& groups) { groups_ = groups; }

    int32_t throttleTimeMs() const { return throttle_time_ms_; }
    const std::vector<Group>& groups() const { return groups_; }

    void encode(Buffer& buffer, int16_t api_version) const;
    void decode(Buffer& buffer, int16_t api_version);

private:
    int32_t throttle_time_ms_ = 0;
    std::vector<Group> groups_;
};

}  // namespace kawasan::protocol
