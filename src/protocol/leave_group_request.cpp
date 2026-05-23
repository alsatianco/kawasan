#include "kawasan/protocol/leave_group_request.h"

#include "kawasan/common/error.h"

namespace kawasan::protocol {

namespace {
constexpr int16_t kMaxVersion = 5;
}

void LeaveGroupRequest::encode(Buffer& buffer, int16_t api_version) const {
    if (api_version < 0 || api_version > kMaxVersion) {
        throw ProtocolException("Unsupported LeaveGroup request version");
    }
    const bool flex = api_version >= 4;

    if (flex) {
        buffer.writeCompactString(group_id_);
    } else {
        buffer.writeString(group_id_);
    }

    if (api_version <= 2) {
        // Legacy single-member form.
        const std::string& mid = memberId();
        if (flex) buffer.writeCompactString(mid);
        else buffer.writeString(mid);
    } else {
        if (flex) {
            buffer.writeCompactArrayLen(static_cast<int32_t>(members_.size()));
        } else {
            buffer.writeInt32(static_cast<int32_t>(members_.size()));
        }
        for (const auto& m : members_) {
            if (flex) {
                buffer.writeCompactString(m.member_id);
                buffer.writeCompactNullableString(m.group_instance_id);
            } else {
                buffer.writeString(m.member_id);
                buffer.writeNullableString(m.group_instance_id);
            }
            if (api_version >= 5) {
                if (flex) buffer.writeCompactNullableString(m.reason);
                else buffer.writeNullableString(m.reason);
            }
            if (flex) buffer.writeEmptyTaggedFields();
        }
    }

    if (flex) buffer.writeEmptyTaggedFields();
}

void LeaveGroupRequest::decode(Buffer& buffer, int16_t api_version) {
    if (api_version < 0 || api_version > kMaxVersion) {
        throw ProtocolException("Unsupported LeaveGroup request version");
    }
    const bool flex = api_version >= 4;

    if (flex) {
        group_id_ = buffer.readCompactString();
    } else {
        group_id_ = buffer.readString();
    }

    members_.clear();
    if (api_version <= 2) {
        // Legacy single-member form.
        Member m;
        m.member_id = flex ? buffer.readCompactString() : buffer.readString();
        members_.push_back(std::move(m));
    } else {
        int32_t count = flex ? buffer.readCompactArrayLen() : buffer.readInt32();
        members_.reserve(count < 0 ? 0 : count);
        for (int32_t i = 0; i < count; ++i) {
            Member m;
            m.member_id = flex ? buffer.readCompactString() : buffer.readString();
            m.group_instance_id =
                flex ? buffer.readCompactNullableString() : buffer.readNullableString();
            if (api_version >= 5) {
                m.reason =
                    flex ? buffer.readCompactNullableString() : buffer.readNullableString();
            }
            if (flex) buffer.skipTaggedFields();
            members_.push_back(std::move(m));
        }
    }

    if (flex) buffer.skipTaggedFields();
}

void LeaveGroupResponse::encode(Buffer& buffer, int16_t api_version) const {
    if (api_version < 0 || api_version > kMaxVersion) {
        throw ProtocolException("Unsupported LeaveGroup response version");
    }
    const bool flex = api_version >= 4;

    if (api_version >= 1) {
        buffer.writeInt32(throttle_time_ms_);
    }
    buffer.writeInt16(static_cast<int16_t>(error_code_));

    if (api_version >= 3) {
        if (flex) {
            buffer.writeCompactArrayLen(static_cast<int32_t>(members_.size()));
        } else {
            buffer.writeInt32(static_cast<int32_t>(members_.size()));
        }
        for (const auto& m : members_) {
            if (flex) {
                buffer.writeCompactString(m.member_id);
                buffer.writeCompactNullableString(m.group_instance_id);
            } else {
                buffer.writeString(m.member_id);
                buffer.writeNullableString(m.group_instance_id);
            }
            buffer.writeInt16(static_cast<int16_t>(m.error_code));
            if (flex) buffer.writeEmptyTaggedFields();
        }
    }

    if (flex) buffer.writeEmptyTaggedFields();
}

void LeaveGroupResponse::decode(Buffer& buffer, int16_t api_version) {
    if (api_version < 0 || api_version > kMaxVersion) {
        throw ProtocolException("Unsupported LeaveGroup response version");
    }
    const bool flex = api_version >= 4;

    if (api_version >= 1) {
        throttle_time_ms_ = buffer.readInt32();
    }
    error_code_ = static_cast<ErrorCode>(buffer.readInt16());

    members_.clear();
    if (api_version >= 3) {
        int32_t count = flex ? buffer.readCompactArrayLen() : buffer.readInt32();
        members_.reserve(count < 0 ? 0 : count);
        for (int32_t i = 0; i < count; ++i) {
            MemberResult m;
            m.member_id = flex ? buffer.readCompactString() : buffer.readString();
            m.group_instance_id =
                flex ? buffer.readCompactNullableString() : buffer.readNullableString();
            m.error_code = static_cast<ErrorCode>(buffer.readInt16());
            if (flex) buffer.skipTaggedFields();
            members_.push_back(std::move(m));
        }
    }

    if (flex) buffer.skipTaggedFields();
}

}  // namespace kawasan::protocol
