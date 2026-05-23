#include "kawasan/protocol/list_groups_request.h"

#include <stdexcept>

#include "kawasan/common/error.h"

namespace kawasan::protocol {

void ListGroupsRequest::encode(Buffer& buffer, int16_t api_version) const {
    const bool flex = api_version >= 3;
    if (api_version >= 4) {
        buffer.writeCompactArrayLen(static_cast<int32_t>(states_filter_.size()));
        for (const auto& s : states_filter_) {
            buffer.writeCompactString(s);
        }
    }
    if (flex) buffer.writeEmptyTaggedFields();
}

void ListGroupsRequest::decode(Buffer& buffer, int16_t api_version) {
    const bool flex = api_version >= 3;
    states_filter_.clear();
    if (api_version >= 4) {
        int32_t n = buffer.readCompactArrayLen();
        states_filter_.reserve(n < 0 ? 0 : n);
        for (int32_t i = 0; i < n; ++i) {
            states_filter_.push_back(buffer.readCompactString());
        }
    }
    if (flex) buffer.skipTaggedFields();
}

void ListGroupsResponse::encode(Buffer& buffer, int16_t api_version) const {
    const bool flex = api_version >= 3;
    if (api_version >= 1) {
        buffer.writeInt32(throttle_time_ms_);
    }
    buffer.writeInt16(static_cast<int16_t>(error_code_));

    if (flex) {
        buffer.writeCompactArrayLen(static_cast<int32_t>(groups_.size()));
    } else {
        buffer.writeInt32(static_cast<int32_t>(groups_.size()));
    }
    for (const auto& group : groups_) {
        if (flex) {
            buffer.writeCompactString(group.group_id);
            buffer.writeCompactString(group.protocol_type);
        } else {
            buffer.writeString(group.group_id);
            buffer.writeString(group.protocol_type);
        }
        if (api_version >= 4) {
            buffer.writeCompactString(group.group_state);
        }
        if (flex) buffer.writeEmptyTaggedFields();
    }
    if (flex) buffer.writeEmptyTaggedFields();
}

void ListGroupsResponse::decode(Buffer& buffer, int16_t api_version) {
    const bool flex = api_version >= 3;
    if (api_version >= 1) {
        throttle_time_ms_ = buffer.readInt32();
    }
    error_code_ = static_cast<ErrorCode>(buffer.readInt16());

    int32_t n = flex ? buffer.readCompactArrayLen() : buffer.readInt32();
    groups_.clear();
    groups_.reserve(n < 0 ? 0 : n);
    for (int32_t i = 0; i < n; ++i) {
        Group group;
        group.group_id = flex ? buffer.readCompactString() : buffer.readString();
        group.protocol_type = flex ? buffer.readCompactString() : buffer.readString();
        if (api_version >= 4) {
            group.group_state = buffer.readCompactString();
        }
        if (flex) buffer.skipTaggedFields();
        groups_.push_back(std::move(group));
    }
    if (flex) buffer.skipTaggedFields();
}

}  // namespace kawasan::protocol
