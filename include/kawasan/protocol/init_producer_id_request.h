#pragma once

#include <cstdint>
#include <optional>
#include <string>

#include "kawasan/common/buffer.h"
#include "kawasan/common/types.h"

namespace kawasan::protocol {

// Phase 1.18: InitProducerId (API 22). Minimal viable implementation.
//
// Schema (Kafka 4.2):
//   Request:
//     v0–v1: transactional_id NULLABLE_STRING + transaction_timeout_ms INT32
//     v2:    same (no new fields)
//     v3:    + producer_id INT64 + producer_epoch INT16   (KIP-360)
//     v4:    flexible (compact strings + tagged fields)
//   Response:
//     throttle_time_ms INT32, error_code INT16,
//     producer_id INT64, producer_epoch INT16
//     v4: flexible
//
// This minimal handler ignores transactional_id semantics — it allocates a
// fresh producer_id from a durable monotonic counter and always returns
// epoch=0. Producer-state-manager dedup arrives in Phase 2.1.
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
