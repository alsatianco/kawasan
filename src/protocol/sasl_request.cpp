#include "kawasan/protocol/sasl_request.h"

namespace kawasan::protocol {

void SaslHandshakeRequest::encode(Buffer& buf, int16_t /*v*/) const {
    buf.writeString(mechanism_);
}
void SaslHandshakeRequest::decode(Buffer& buf, int16_t /*v*/) {
    mechanism_ = buf.readString();
}
void SaslHandshakeResponse::encode(Buffer& buf, int16_t /*v*/) const {
    buf.writeInt16(static_cast<int16_t>(error_code_));
    buf.writeInt32(static_cast<int32_t>(enabled_mechanisms_.size()));
    for (const auto& m : enabled_mechanisms_)
        buf.writeString(m);
}
void SaslHandshakeResponse::decode(Buffer& buf, int16_t /*v*/) {
    error_code_ = static_cast<ErrorCode>(buf.readInt16());
    int32_t n = buf.readInt32();
    enabled_mechanisms_.resize(n < 0 ? 0 : n);
    for (int32_t i = 0; i < n; ++i)
        enabled_mechanisms_[i] = buf.readString();
}

void SaslAuthenticateRequest::encode(Buffer& buf, int16_t v) const {
    if (v >= 2) {
        buf.writeCompactBytes(auth_bytes_);
        buf.writeEmptyTaggedFields();
    } else {
        buf.writeBytes(auth_bytes_);
    }
}
void SaslAuthenticateRequest::decode(Buffer& buf, int16_t v) {
    if (v >= 2) {
        auth_bytes_ = buf.readCompactBytes();
        buf.skipTaggedFields();
    } else {
        const int32_t n = buf.readInt32();
        auth_bytes_ = n > 0 ? buf.readBytes(n) : std::vector<uint8_t>{};
    }
}
void SaslAuthenticateResponse::encode(Buffer& buf, int16_t v) const {
    const bool flex = v >= 2;
    buf.writeInt16(static_cast<int16_t>(error_code_));
    const auto msg = error_message_.empty() ? std::optional<std::string>{}
                                            : std::optional<std::string>(error_message_);
    if (flex) {
        buf.writeCompactNullableString(msg);
        buf.writeCompactBytes(auth_bytes_);
    } else {
        buf.writeNullableString(msg);
        buf.writeBytes(auth_bytes_);
    }
    if (v >= 1) {
        buf.writeInt64(session_lifetime_ms_);
    }
    if (flex) {
        buf.writeEmptyTaggedFields();
    }
}
void SaslAuthenticateResponse::decode(Buffer& buf, int16_t v) {
    const bool flex = v >= 2;
    error_code_ = static_cast<ErrorCode>(buf.readInt16());
    const auto msg = flex ? buf.readCompactNullableString() : buf.readNullableString();
    error_message_ = msg.value_or("");
    if (flex) {
        auth_bytes_ = buf.readCompactBytes();
    } else {
        const int32_t n = buf.readInt32();
        auth_bytes_ = n > 0 ? buf.readBytes(n) : std::vector<uint8_t>{};
    }
    session_lifetime_ms_ = v >= 1 ? buf.readInt64() : 0;
    if (flex) {
        buf.skipTaggedFields();
    }
}

}  // namespace kawasan::protocol
