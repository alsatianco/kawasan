#include "kawasan/protocol/join_group_request.h"

#include <stdexcept>

#include "kawasan/common/error.h"

namespace kawasan::protocol {

void JoinGroupRequest::encode(Buffer& buffer, int16_t api_version) const {
    if (api_version < 0 || api_version > 9) {
        throw ProtocolException("Unsupported JoinGroup request version");
    }
    const bool flex = api_version >= 6;

    if (flex) buffer.writeCompactString(group_id_);
    else buffer.writeString(group_id_);

    buffer.writeInt32(session_timeout_ms_);
    if (api_version >= 1) {
        buffer.writeInt32(rebalance_timeout_ms_);
    }

    if (flex) buffer.writeCompactString(member_id_);
    else buffer.writeString(member_id_);

    if (api_version >= 5) {
        if (flex) buffer.writeCompactNullableString(group_instance_id_);
        else buffer.writeNullableString(group_instance_id_);
    }

    if (flex) buffer.writeCompactString(protocol_type_);
    else buffer.writeString(protocol_type_);

    if (flex) {
        buffer.writeCompactArrayLen(static_cast<int32_t>(group_protocols_.size()));
    } else {
        buffer.writeInt32(static_cast<int32_t>(group_protocols_.size()));
    }
    for (const auto& protocol : group_protocols_) {
        if (flex) {
            buffer.writeCompactString(protocol.name);
            buffer.writeCompactBytes(protocol.metadata);
            buffer.writeEmptyTaggedFields();
        } else {
            buffer.writeString(protocol.name);
            buffer.writeBytes(protocol.metadata);
        }
    }

    if (api_version >= 8) {
        buffer.writeCompactNullableString(reason_);
    }

    if (flex) buffer.writeEmptyTaggedFields();
}

void JoinGroupRequest::decode(Buffer& buffer, int16_t api_version) {
    if (api_version < 0 || api_version > 9) {
        throw ProtocolException("Unsupported JoinGroup request version");
    }
    const bool flex = api_version >= 6;

    group_id_ = flex ? buffer.readCompactString() : buffer.readString();
    session_timeout_ms_ = buffer.readInt32();
    if (api_version >= 1) {
        rebalance_timeout_ms_ = buffer.readInt32();
    } else {
        rebalance_timeout_ms_ = session_timeout_ms_;
    }
    member_id_ = flex ? buffer.readCompactString() : buffer.readString();

    group_instance_id_.reset();
    if (api_version >= 5) {
        group_instance_id_ =
            flex ? buffer.readCompactNullableString() : buffer.readNullableString();
    }

    protocol_type_ = flex ? buffer.readCompactString() : buffer.readString();

    const int32_t protocol_count =
        flex ? buffer.readCompactArrayLen() : buffer.readInt32();
    group_protocols_.clear();
    group_protocols_.reserve(protocol_count < 0 ? 0 : protocol_count);
    for (int32_t i = 0; i < protocol_count; ++i) {
        GroupProtocol p;
        if (flex) {
            p.name = buffer.readCompactString();
            p.metadata = buffer.readCompactBytes();
            buffer.skipTaggedFields();
        } else {
            p.name = buffer.readString();
            p.metadata = buffer.readBytesWithLength();
        }
        group_protocols_.push_back(std::move(p));
    }

    reason_.reset();
    if (api_version >= 8) {
        reason_ = buffer.readCompactNullableString();
    }

    if (flex) buffer.skipTaggedFields();
}

void JoinGroupResponse::encode(Buffer& buffer, int16_t api_version) const {
    if (api_version < 0 || api_version > 9) {
        throw ProtocolException("Unsupported JoinGroup response version");
    }
    const bool flex = api_version >= 6;

    if (api_version >= 2) {
        buffer.writeInt32(throttle_time_ms_);
    }
    buffer.writeInt16(static_cast<int16_t>(error_code_));
    buffer.writeInt32(generation_id_);

    if (api_version >= 7) {
        // v7+ replaced group_protocol (STRING) with protocol_type +
        // protocol_name (both NULLABLE_STRING).
        buffer.writeCompactNullableString(protocol_type_);
        buffer.writeCompactNullableString(protocol_name_);
    } else {
        if (flex) buffer.writeCompactString(group_protocol_);
        else buffer.writeString(group_protocol_);
    }

    if (flex) {
        buffer.writeCompactString(leader_id_);
    } else {
        buffer.writeString(leader_id_);
    }
    // Phase 1.7 fix: v9 added skip_assignment BOOL between leader_id
    // and member_id. KIP-792 (the static-membership-rejoin polish).
    // Schema Registry's Java client expects this field at v9; omitting
    // it shifts every following field by 1 byte and the client decodes
    // garbage.
    if (api_version >= 9) {
        buffer.writeInt8(0);  // skip_assignment = false
    }
    if (flex) {
        buffer.writeCompactString(member_id_);
    } else {
        buffer.writeString(member_id_);
    }

    if (flex) {
        buffer.writeCompactArrayLen(static_cast<int32_t>(members_.size()));
    } else {
        buffer.writeInt32(static_cast<int32_t>(members_.size()));
    }
    for (const auto& member : members_) {
        if (flex) {
            buffer.writeCompactString(member.member_id);
            if (api_version >= 5) {
                buffer.writeCompactNullableString(member.group_instance_id);
            }
            buffer.writeCompactBytes(member.metadata);
            buffer.writeEmptyTaggedFields();
        } else {
            buffer.writeString(member.member_id);
            if (api_version >= 5) {
                buffer.writeNullableString(member.group_instance_id);
            }
            buffer.writeBytes(member.metadata);
        }
    }

    if (flex) buffer.writeEmptyTaggedFields();
}

void JoinGroupResponse::decode(Buffer& buffer, int16_t api_version) {
    if (api_version < 0 || api_version > 9) {
        throw ProtocolException("Unsupported JoinGroup response version");
    }
    const bool flex = api_version >= 6;

    if (api_version >= 2) {
        throttle_time_ms_ = buffer.readInt32();
    }
    error_code_ = static_cast<ErrorCode>(buffer.readInt16());
    generation_id_ = buffer.readInt32();

    if (api_version >= 7) {
        protocol_type_ = buffer.readCompactNullableString();
        protocol_name_ = buffer.readCompactNullableString();
        if (protocol_name_) group_protocol_ = *protocol_name_;
    } else {
        group_protocol_ = flex ? buffer.readCompactString() : buffer.readString();
    }

    leader_id_ = flex ? buffer.readCompactString() : buffer.readString();
    if (api_version >= 9) {
        // Phase 1.7: skip_assignment BOOL (KIP-792).
        (void)buffer.readInt8();
    }
    member_id_ = flex ? buffer.readCompactString() : buffer.readString();

    const int32_t member_count =
        flex ? buffer.readCompactArrayLen() : buffer.readInt32();
    members_.clear();
    members_.reserve(member_count < 0 ? 0 : member_count);
    for (int32_t i = 0; i < member_count; ++i) {
        Member m;
        m.member_id = flex ? buffer.readCompactString() : buffer.readString();
        if (api_version >= 5) {
            m.group_instance_id =
                flex ? buffer.readCompactNullableString() : buffer.readNullableString();
        }
        m.metadata = flex ? buffer.readCompactBytes() : buffer.readBytesWithLength();
        if (flex) buffer.skipTaggedFields();
        members_.push_back(std::move(m));
    }

    if (flex) buffer.skipTaggedFields();
}

}  // namespace kawasan::protocol
