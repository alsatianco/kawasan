#include "kawasan/protocol/find_coordinator_request.h"

namespace kawasan::protocol {

void FindCoordinatorRequest::encode(Buffer& buffer, int16_t api_version) const {
    const bool flex = api_version >= 3;
    if (api_version <= 3) {
        if (flex) {
            buffer.writeCompactString(key());
        } else {
            buffer.writeString(key());
        }
        if (api_version >= 1) {
            buffer.writeInt8(static_cast<int8_t>(key_type_));
        }
    } else {
        // v4: key_type first, then coordinator_keys array.
        buffer.writeInt8(static_cast<int8_t>(key_type_));
        buffer.writeCompactArrayLen(static_cast<int32_t>(keys_.size()));
        for (const auto& k : keys_) {
            buffer.writeCompactString(k);
        }
    }
    if (flex) buffer.writeEmptyTaggedFields();
}

void FindCoordinatorRequest::decode(Buffer& buffer, int16_t api_version) {
    const bool flex = api_version >= 3;
    keys_.clear();
    if (api_version <= 3) {
        std::string k = flex ? buffer.readCompactString() : buffer.readString();
        keys_.push_back(std::move(k));
        if (api_version >= 1) {
            key_type_ = static_cast<CoordinatorType>(buffer.readInt8());
        } else {
            key_type_ = CoordinatorType::GROUP;
        }
    } else {
        key_type_ = static_cast<CoordinatorType>(buffer.readInt8());
        int32_t count = buffer.readCompactArrayLen();
        keys_.reserve(count < 0 ? 0 : count);
        for (int32_t i = 0; i < count; ++i) {
            keys_.push_back(buffer.readCompactString());
        }
    }
    if (flex) buffer.skipTaggedFields();
}

void FindCoordinatorResponse::encode(Buffer& buffer, int16_t api_version) const {
    const bool flex = api_version >= 3;
    if (api_version >= 1) {
        buffer.writeInt32(throttle_time_ms_);
    }

    if (api_version <= 3) {
        buffer.writeInt16(static_cast<int16_t>(error_code_));
        if (api_version >= 1) {
            const auto msg_opt = error_message_.empty()
                                     ? std::optional<std::string>{}
                                     : std::optional<std::string>(error_message_);
            if (flex) buffer.writeCompactNullableString(msg_opt);
            else buffer.writeNullableString(msg_opt);
        }
        buffer.writeInt32(node_id_);
        if (flex) buffer.writeCompactString(host_);
        else buffer.writeString(host_);
        buffer.writeInt32(port_);
    } else {
        // v4: array of {key, node_id, host, port, error_code, error_message}.
        buffer.writeCompactArrayLen(static_cast<int32_t>(coordinators_.size()));
        for (const auto& c : coordinators_) {
            buffer.writeCompactString(c.key);
            buffer.writeInt32(c.node_id);
            buffer.writeCompactString(c.host);
            buffer.writeInt32(c.port);
            buffer.writeInt16(static_cast<int16_t>(c.error_code));
            const auto msg_opt = c.error_message.empty()
                                     ? std::optional<std::string>{}
                                     : std::optional<std::string>(c.error_message);
            buffer.writeCompactNullableString(msg_opt);
            buffer.writeEmptyTaggedFields();
        }
    }
    if (flex) buffer.writeEmptyTaggedFields();
}

void FindCoordinatorResponse::decode(Buffer& buffer, int16_t api_version) {
    const bool flex = api_version >= 3;
    if (api_version >= 1) {
        throttle_time_ms_ = buffer.readInt32();
    }
    coordinators_.clear();
    if (api_version <= 3) {
        error_code_ = static_cast<ErrorCode>(buffer.readInt16());
        if (api_version >= 1) {
            auto msg = flex ? buffer.readCompactNullableString() : buffer.readNullableString();
            error_message_ = msg.value_or("");
        }
        node_id_ = buffer.readInt32();
        host_ = flex ? buffer.readCompactString() : buffer.readString();
        port_ = buffer.readInt32();
    } else {
        int32_t count = buffer.readCompactArrayLen();
        coordinators_.reserve(count < 0 ? 0 : count);
        for (int32_t i = 0; i < count; ++i) {
            Coordinator c;
            c.key = buffer.readCompactString();
            c.node_id = buffer.readInt32();
            c.host = buffer.readCompactString();
            c.port = buffer.readInt32();
            c.error_code = static_cast<ErrorCode>(buffer.readInt16());
            auto msg = buffer.readCompactNullableString();
            c.error_message = msg.value_or("");
            buffer.skipTaggedFields();
            coordinators_.push_back(std::move(c));
        }
    }
    if (flex) buffer.skipTaggedFields();
}

}  // namespace kawasan::protocol
