#include "kawasan/protocol/heartbeat_request.h"

#include "kawasan/common/error.h"

namespace kawasan::protocol {

namespace {
constexpr int16_t kMaxVersion = 4;
}

void HeartbeatRequest::encode(Buffer& buffer, int16_t api_version) const {
    if (api_version < 0 || api_version > kMaxVersion) {
        throw ProtocolException("Unsupported Heartbeat request version");
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
        if (flex) {
            buffer.writeCompactNullableString(group_instance_id_);
        } else {
            buffer.writeNullableString(group_instance_id_);
        }
    }
    if (flex) {
        buffer.writeEmptyTaggedFields();
    }
}

void HeartbeatRequest::decode(Buffer& buffer, int16_t api_version) {
    if (api_version < 0 || api_version > kMaxVersion) {
        throw ProtocolException("Unsupported Heartbeat request version");
    }
    const bool flex = api_version >= 4;
    if (flex) {
        group_id_ = buffer.readCompactString();
    } else {
        group_id_ = buffer.readString();
    }
    generation_id_ = buffer.readInt32();
    if (flex) {
        member_id_ = buffer.readCompactString();
    } else {
        member_id_ = buffer.readString();
    }
    if (api_version >= 3) {
        if (flex) {
            group_instance_id_ = buffer.readCompactNullableString();
        } else {
            group_instance_id_ = buffer.readNullableString();
        }
    }
    if (flex) {
        buffer.skipTaggedFields();
    }
}

void HeartbeatResponse::encode(Buffer& buffer, int16_t api_version) const {
    if (api_version < 0 || api_version > kMaxVersion) {
        throw ProtocolException("Unsupported Heartbeat response version");
    }
    const bool flex = api_version >= 4;
    if (api_version >= 1) {
        buffer.writeInt32(throttle_time_ms_);
    }
    buffer.writeInt16(static_cast<int16_t>(error_code_));
    if (flex) {
        buffer.writeEmptyTaggedFields();
    }
}

void HeartbeatResponse::decode(Buffer& buffer, int16_t api_version) {
    if (api_version < 0 || api_version > kMaxVersion) {
        throw ProtocolException("Unsupported Heartbeat response version");
    }
    const bool flex = api_version >= 4;
    if (api_version >= 1) {
        throttle_time_ms_ = buffer.readInt32();
    }
    error_code_ = static_cast<ErrorCode>(buffer.readInt16());
    if (flex) {
        buffer.skipTaggedFields();
    }
}

}  // namespace kawasan::protocol
