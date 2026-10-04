#include "kawasan/protocol/describe_groups_request.h"

#include <stdexcept>

#include "kawasan/common/error.h"

namespace kawasan::protocol {

void DescribeGroupsRequest::encode(Buffer& buffer, int16_t api_version) const {
    const bool flex = api_version >= 5;
    if (flex) {
        buffer.writeCompactArrayLen(static_cast<int32_t>(groups_.size()));
    } else {
        buffer.writeInt32(static_cast<int32_t>(groups_.size()));
    }
    for (const auto& group : groups_) {
        if (flex)
            buffer.writeCompactString(group);
        else
            buffer.writeString(group);
    }
    if (api_version >= 3) {
        buffer.writeInt8(include_authorized_operations_ ? 1 : 0);
    }
    if (flex)
        buffer.writeEmptyTaggedFields();
}

void DescribeGroupsRequest::decode(Buffer& buffer, int16_t api_version) {
    const bool flex = api_version >= 5;
    const int32_t group_count = flex ? buffer.readCompactArrayLen() : buffer.readInt32();
    groups_.clear();
    groups_.reserve(group_count < 0 ? 0 : group_count);
    for (int32_t i = 0; i < group_count; ++i) {
        groups_.push_back(flex ? buffer.readCompactString() : buffer.readString());
    }
    if (api_version >= 3) {
        include_authorized_operations_ = (buffer.readInt8() != 0);
    } else {
        include_authorized_operations_ = false;
    }
    if (flex)
        buffer.skipTaggedFields();
}

void DescribeGroupsResponse::encode(Buffer& buffer, int16_t api_version) const {
    const bool flex = api_version >= 5;
    if (api_version >= 1) {
        buffer.writeInt32(throttle_time_ms_);
    }
    if (flex) {
        buffer.writeCompactArrayLen(static_cast<int32_t>(groups_.size()));
    } else {
        buffer.writeInt32(static_cast<int32_t>(groups_.size()));
    }
    for (const auto& group : groups_) {
        buffer.writeInt16(static_cast<int16_t>(group.error_code));
        if (flex) {
            buffer.writeCompactString(group.group_id);
            buffer.writeCompactString(group.group_state);
            buffer.writeCompactString(group.protocol_type);
            buffer.writeCompactString(group.protocol_data);
        } else {
            buffer.writeString(group.group_id);
            buffer.writeString(group.group_state);
            buffer.writeString(group.protocol_type);
            buffer.writeString(group.protocol_data);
        }

        if (flex) {
            buffer.writeCompactArrayLen(static_cast<int32_t>(group.members.size()));
        } else {
            buffer.writeInt32(static_cast<int32_t>(group.members.size()));
        }
        for (const auto& member : group.members) {
            if (flex) {
                buffer.writeCompactString(member.member_id);
                buffer.writeCompactNullableString(member.group_instance_id);
                buffer.writeCompactString(member.client_id);
                buffer.writeCompactString(member.client_host);
                buffer.writeCompactBytes(member.member_metadata);
                buffer.writeCompactBytes(member.member_assignment);
                buffer.writeEmptyTaggedFields();
            } else {
                buffer.writeString(member.member_id);
                if (api_version >= 4) {
                    buffer.writeNullableString(member.group_instance_id);
                }
                buffer.writeString(member.client_id);
                buffer.writeString(member.client_host);
                buffer.writeBytes(member.member_metadata);
                buffer.writeBytes(member.member_assignment);
            }
        }

        if (api_version >= 3) {
            buffer.writeInt32(group.authorized_operations);
        }
        if (flex)
            buffer.writeEmptyTaggedFields();
    }
    if (flex)
        buffer.writeEmptyTaggedFields();
}

void DescribeGroupsResponse::decode(Buffer& buffer, int16_t api_version) {
    const bool flex = api_version >= 5;
    if (api_version >= 1) {
        throttle_time_ms_ = buffer.readInt32();
    }
    const int32_t group_count = flex ? buffer.readCompactArrayLen() : buffer.readInt32();
    groups_.clear();
    groups_.reserve(group_count < 0 ? 0 : group_count);

    for (int32_t i = 0; i < group_count; ++i) {
        Group group;
        group.error_code = static_cast<ErrorCode>(buffer.readInt16());
        if (flex) {
            group.group_id = buffer.readCompactString();
            group.group_state = buffer.readCompactString();
            group.protocol_type = buffer.readCompactString();
            group.protocol_data = buffer.readCompactString();
        } else {
            group.group_id = buffer.readString();
            group.group_state = buffer.readString();
            group.protocol_type = buffer.readString();
            group.protocol_data = buffer.readString();
        }

        const int32_t member_count = flex ? buffer.readCompactArrayLen() : buffer.readInt32();
        group.members.reserve(member_count < 0 ? 0 : member_count);
        for (int32_t j = 0; j < member_count; ++j) {
            Member member;
            if (flex) {
                member.member_id = buffer.readCompactString();
                member.group_instance_id = buffer.readCompactNullableString();
                member.client_id = buffer.readCompactString();
                member.client_host = buffer.readCompactString();
                member.member_metadata = buffer.readCompactBytes();
                member.member_assignment = buffer.readCompactBytes();
                buffer.skipTaggedFields();
            } else {
                member.member_id = buffer.readString();
                if (api_version >= 4) {
                    member.group_instance_id = buffer.readNullableString();
                }
                member.client_id = buffer.readString();
                member.client_host = buffer.readString();
                member.member_metadata = buffer.readBytesWithLength();
                member.member_assignment = buffer.readBytesWithLength();
            }
            group.members.push_back(std::move(member));
        }

        if (api_version >= 3) {
            group.authorized_operations = buffer.readInt32();
        }
        if (flex)
            buffer.skipTaggedFields();
        groups_.push_back(std::move(group));
    }
    if (flex)
        buffer.skipTaggedFields();
}

}  // namespace kawasan::protocol
