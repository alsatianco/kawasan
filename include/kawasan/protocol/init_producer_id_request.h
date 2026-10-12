#pragma once

#include <cstdint>
#include <optional>
#include <string>

#include "kawasan/common/buffer.h"
#include "kawasan/common/types.h"

namespace kawasan::protocol {

// Kafka InitProducerId v0-4: v2+ uses compact strings and tagged fields;
// v3+ carries the expected producer ID/epoch for KIP-360 retry fencing;
// v4 adds PRODUCER_FENCED (older versions use INVALID_PRODUCER_EPOCH).
class InitProducerIdRequest {
public:
    void decode(Buffer& buffer, int16_t api_version);

    const std::optional<std::string>& transactionalId() const { return transactional_id_; }
    int32_t transactionTimeoutMs() const { return transaction_timeout_ms_; }
    int64_t producerId() const { return producer_id_; }
    int16_t producerEpoch() const { return producer_epoch_; }

private:
    std::optional<std::string> transactional_id_;
    int32_t transaction_timeout_ms_ = 0;
    int64_t producer_id_ = -1;
    int16_t producer_epoch_ = -1;
};

class InitProducerIdResponse {
public:
    void setThrottleTimeMs(int32_t v) { throttle_time_ms_ = v; }
    void setErrorCode(ErrorCode v) { error_code_ = v; }
    void setProducerId(int64_t v) { producer_id_ = v; }
    void setProducerEpoch(int16_t v) { producer_epoch_ = v; }

    void encode(Buffer& buffer, int16_t api_version) const;

private:
    int32_t throttle_time_ms_ = 0;
    ErrorCode error_code_ = ErrorCode::NONE;
    int64_t producer_id_ = -1;
    int16_t producer_epoch_ = -1;
};

}  // namespace kawasan::protocol
