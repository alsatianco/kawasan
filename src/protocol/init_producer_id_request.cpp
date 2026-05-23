#include "kawasan/protocol/init_producer_id_request.h"

namespace kawasan::protocol {

void InitProducerIdRequest::decode(Buffer& buffer, int16_t api_version) {
    const bool flex = api_version >= 4;
    if (flex) {
        transactional_id_ = buffer.readCompactNullableString();
    } else {
        transactional_id_ = buffer.readNullableString();
    }
    transaction_timeout_ms_ = buffer.readInt32();
    if (api_version >= 3) {
        producer_id_ = buffer.readInt64();
        producer_epoch_ = buffer.readInt16();
    }
    if (flex) {
        buffer.skipTaggedFields();
    }
}

void InitProducerIdResponse::encode(Buffer& buffer, int16_t api_version) const {
    const bool flex = api_version >= 4;
    buffer.writeInt32(throttle_time_ms_);
    buffer.writeInt16(static_cast<int16_t>(error_code_));
    buffer.writeInt64(producer_id_);
    buffer.writeInt16(producer_epoch_);
    if (flex) {
        buffer.writeEmptyTaggedFields();
    }
}

}  // namespace kawasan::protocol
