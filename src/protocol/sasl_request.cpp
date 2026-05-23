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
    for (const auto& m : enabled_mechanisms_) buf.writeString(m);
}
void SaslHandshakeResponse::decode(Buffer& buf, int16_t /*v*/) {
    error_code_ = static_cast<ErrorCode>(buf.readInt16());
    int32_t n = buf.readInt32();
    enabled_mechanisms_.resize(n < 0 ? 0 : n);
    for (int32_t i = 0; i < n; ++i) enabled_mechanisms_[i] = buf.readString();
}

void SaslAuthenticateRequest::encode(Buffer& buf, int16_t /*v*/) const {
    buf.writeInt32(static_cast<int32_t>(auth_bytes_.size()));
    if (!auth_bytes_.empty()) {
        buf.writeBytes(auth_bytes_.data(), auth_bytes_.size());
    }
}
void SaslAuthenticateRequest::decode(Buffer& buf, int16_t /*v*/) {
    int32_t n = buf.readInt32();
    if (n > 0) auth_bytes_ = buf.readBytes(n);
}
void SaslAuthenticateResponse::encode(Buffer& buf, int16_t v) const {
    buf.writeInt16(static_cast<int16_t>(error_code_));
    const auto msg = error_message_.empty()
                         ? std::optional<std::string>{}
                         : std::optional<std::string>(error_message_);
    buf.writeNullableString(msg);
    buf.writeInt32(static_cast<int32_t>(auth_bytes_.size()));
    if (!auth_bytes_.empty()) {
        buf.writeBytes(auth_bytes_.data(), auth_bytes_.size());
    }
    if (v >= 1) {
        buf.writeInt64(session_lifetime_ms_);
    }
}
void SaslAuthenticateResponse::decode(Buffer& buf, int16_t v) {
    error_code_ = static_cast<ErrorCode>(buf.readInt16());
    auto msg = buf.readNullableString();
    error_message_ = msg.value_or("");
    int32_t n = buf.readInt32();
    if (n > 0) auth_bytes_ = buf.readBytes(n);
    if (v >= 1) {
        session_lifetime_ms_ = buf.readInt64();
    }
}

}  // namespace kawasan::protocol
