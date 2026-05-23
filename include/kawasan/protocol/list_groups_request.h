#pragma once

#include <string>
#include <vector>

#include "kawasan/common/buffer.h"
#include "kawasan/common/error.h"

namespace kawasan::protocol {

/// @brief ListGroups request
///
/// Phase 1.14: supports v0–v4.
///   v0–v2: empty body
///   v3:    flexible (tagged fields only)
///   v4:    + states_filter ARRAY<COMPACT_STRING>
class ListGroupsRequest {
public:
    ListGroupsRequest() = default;

    const std::vector<std::string>& statesFilter() const { return states_filter_; }
    void setStatesFilter(std::vector<std::string> v) { states_filter_ = std::move(v); }

    void encode(Buffer& buffer, int16_t api_version) const;
    void decode(Buffer& buffer, int16_t api_version);

private:
    std::vector<std::string> states_filter_;  // v4+
};

/// @brief ListGroups response
class ListGroupsResponse {
public:
    struct Group {
        std::string group_id;
        std::string protocol_type;
        std::string group_state;  // v4+
    };

    ListGroupsResponse() = default;

    void setErrorCode(ErrorCode code) { error_code_ = code; }
    void setThrottleTimeMs(int32_t throttle_time_ms) {
        throttle_time_ms_ = throttle_time_ms;
    }
    void setGroups(const std::vector<Group>& groups) { groups_ = groups; }

    ErrorCode errorCode() const { return error_code_; }
    int32_t throttleTimeMs() const { return throttle_time_ms_; }
    const std::vector<Group>& groups() const { return groups_; }

    void encode(Buffer& buffer, int16_t api_version) const;
    void decode(Buffer& buffer, int16_t api_version);

private:
    ErrorCode error_code_ = ErrorCode::NONE;
    int32_t throttle_time_ms_ = 0;
    std::vector<Group> groups_;
};

}  // namespace kawasan::protocol
