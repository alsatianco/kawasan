#pragma once

#include <optional>
#include <string>
#include <vector>

#include "kawasan/common/buffer.h"
#include "kawasan/common/error.h"

namespace kawasan::protocol {

// Phase 4.2a: SaslHandshake (API 17).
//
// Schema (Kafka 4.2):
//   Request:
//     v0: mechanism STRING
//     v1: same body; transition to SASL_AUTHENTICATE messaging follows
//   Response:
//     error_code, enabled_mechanisms ARRAY<STRING>
class SaslHandshakeRequest {
public:
    const std::string& mechanism() const { return mechanism_; }
    void setMechanism(const std::string& m) { mechanism_ = m; }
    void encode(Buffer& buf, int16_t v) const;
    void decode(Buffer& buf, int16_t v);

private:
    std::string mechanism_;
};

class SaslHandshakeResponse {
public:
    void setErrorCode(ErrorCode v) { error_code_ = v; }
    void setEnabledMechanisms(std::vector<std::string> v) { enabled_mechanisms_ = std::move(v); }
    void encode(Buffer& buf, int16_t v) const;
    void decode(Buffer& buf, int16_t v);

private:
    ErrorCode error_code_ = ErrorCode::NONE;
    std::vector<std::string> enabled_mechanisms_;
};

// SaslAuthenticate (API 36).
//
//   Request: auth_bytes BYTES
//   Response: error_code, error_message NULLABLE_STRING, auth_bytes BYTES,
//             session_lifetime_ms INT64 (v1+)
//   v2: flexible compact bytes/nullable message and tagged fields.
class SaslAuthenticateRequest {
public:
    const std::vector<uint8_t>& authBytes() const { return auth_bytes_; }
    void setAuthBytes(std::vector<uint8_t> b) { auth_bytes_ = std::move(b); }
    void encode(Buffer& buf, int16_t v) const;
    void decode(Buffer& buf, int16_t v);

private:
    std::vector<uint8_t> auth_bytes_;
};

class SaslAuthenticateResponse {
public:
    void setErrorCode(ErrorCode v) { error_code_ = v; }
    void setErrorMessage(const std::string& v) { error_message_ = v; }
    void setAuthBytes(std::vector<uint8_t> b) { auth_bytes_ = std::move(b); }
    void setSessionLifetimeMs(int64_t v) { session_lifetime_ms_ = v; }
    void encode(Buffer& buf, int16_t v) const;
    void decode(Buffer& buf, int16_t v);

private:
    ErrorCode error_code_ = ErrorCode::NONE;
    std::string error_message_;
    std::vector<uint8_t> auth_bytes_;
    int64_t session_lifetime_ms_ = 0;
};

}  // namespace kawasan::protocol
