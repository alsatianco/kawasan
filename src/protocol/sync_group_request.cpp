#include "kawasan/protocol/sync_group_request.h"

#include "kawasan/common/error.h"

namespace kawasan::protocol {

void SyncGroupRequest::encode(Buffer& buffer, int16_t api_version) const {
    if (api_version < 0 || api_version > 5) {
        throw ProtocolException("Unsupported SyncGroup request version");
    }
    const bool flex = api_version >= 4;

    if (flex) {
        buffer.writeCompactString(group_id_);
    } else {
        buffer.writeString(group_id_);
    }
    buffer.writeInt32(generation_id_);
    if (flex) {
        buffer.writeCompactString(member_id_);
    } else {
        buffer.writeString(member_id_);
    }
    if (api_version >= 3) {
        if (flex) buffer.writeCompactNullableString(group_instance_id_);
        else buffer.writeNullableString(group_instance_id_);
    }
    if (api_version >= 5) {
        buffer.writeCompactNullableString(protocol_type_);
        buffer.writeCompactNullableString(protocol_name_);
    }

    if (flex) {
        buffer.writeCompactArrayLen(static_cast<int32_t>(assignments_.size()));
    } else {
        buffer.writeInt32(static_cast<int32_t>(assignments_.size()));
    }
    for (const auto& assignment : assignments_) {
        if (flex) {
            buffer.writeCompactString(assignment.member_id);
            buffer.writeCompactBytes(assignment.assignment);
            buffer.writeEmptyTaggedFields();
        } else {
            buffer.writeString(assignment.member_id);
            buffer.writeBytes(assignment.assignment);
        }
    }
    if (flex) buffer.writeEmptyTaggedFields();
}

void SyncGroupRequest::decode(Buffer& buffer, int16_t api_version) {
    if (api_version < 0 || api_version > 5) {
        throw ProtocolException("Unsupported SyncGroup request version");
    }
    const bool flex = api_version >= 4;

    group_id_ = flex ? buffer.readCompactString() : buffer.readString();
    generation_id_ = buffer.readInt32();
    member_id_ = flex ? buffer.readCompactString() : buffer.readString();

    group_instance_id_.reset();
    if (api_version >= 3) {
        group_instance_id_ =
            flex ? buffer.readCompactNullableString() : buffer.readNullableString();
    }
    protocol_type_.reset();
    protocol_name_.reset();
    if (api_version >= 5) {
        protocol_type_ = buffer.readCompactNullableString();
        protocol_name_ = buffer.readCompactNullableString();
    }

    const int32_t assignment_count =
        flex ? buffer.readCompactArrayLen() : buffer.readInt32();
    assignments_.clear();
    assignments_.reserve(assignment_count < 0 ? 0 : assignment_count);
    for (int32_t i = 0; i < assignment_count; ++i) {
        Assignment a;
        if (flex) {
            a.member_id = buffer.readCompactString();
            a.assignment = buffer.readCompactBytes();
            buffer.skipTaggedFields();
        } else {
            a.member_id = buffer.readString();
            a.assignment = buffer.readBytesWithLength();
        }
        assignments_.push_back(std::move(a));
    }
    if (flex) buffer.skipTaggedFields();
}

void SyncGroupResponse::encode(Buffer& buffer, int16_t api_version) const {
    if (api_version < 0 || api_version > 5) {
        throw ProtocolException("Unsupported SyncGroup response version");
    }
    const bool flex = api_version >= 4;

    if (api_version >= 1) {
        buffer.writeInt32(throttle_time_ms_);
    }
    buffer.writeInt16(static_cast<int16_t>(error_code_));
    if (api_version >= 5) {
        buffer.writeCompactNullableString(protocol_type_);
        buffer.writeCompactNullableString(protocol_name_);
    }
    if (flex) buffer.writeCompactBytes(assignment_);
    else buffer.writeBytes(assignment_);

    if (flex) buffer.writeEmptyTaggedFields();
}

void SyncGroupResponse::decode(Buffer& buffer, int16_t api_version) {
    if (api_version < 0 || api_version > 5) {
        throw ProtocolException("Unsupported SyncGroup response version");
    }
    const bool flex = api_version >= 4;

    if (api_version >= 1) {
        throttle_time_ms_ = buffer.readInt32();
    }
    error_code_ = static_cast<ErrorCode>(buffer.readInt16());
    if (api_version >= 5) {
        protocol_type_ = buffer.readCompactNullableString();
        protocol_name_ = buffer.readCompactNullableString();
    }
    assignment_ = flex ? buffer.readCompactBytes() : buffer.readBytesWithLength();
    if (flex) buffer.skipTaggedFields();
}

}  // namespace kawasan::protocol
