#pragma once

#include <cstdint>
#include <vector>

#include "kawasan/storage/record_batch.h"

namespace kawasan::test_support {
// Construct a Kafka v2 compacted batch whose offset span exceeds record count.
// Patch the fixed header's lastOffsetDelta (bytes 23..26) and compute CRC-32C
// independently over attributes through the end of the batch. Production append APIs cannot choose
// this field, so the fixture specifies the wire span independently.
inline storage::RecordBatch batchWithWireSpan(const storage::RecordBatch& batch,
                                              int32_t last_delta) {
    auto bytes = batch.serialize();
    const auto delta = static_cast<uint32_t>(last_delta);
    for (size_t i = 0; i < 4; ++i)
        bytes[23 + i] = static_cast<uint8_t>(delta >> (24 - 8 * i));
    uint32_t crc = 0xFFFFFFFFu;
    for (size_t i = 21; i < bytes.size(); ++i) {
        crc ^= bytes[i];
        for (int bit = 0; bit < 8; ++bit)
            crc = (crc >> 1) ^ ((crc & 1) ? 0x82F63B78u : 0u);
    }
    crc = ~crc;
    for (size_t i = 0; i < 4; ++i)
        bytes[17 + i] = static_cast<uint8_t>(crc >> (24 - 8 * i));
    return storage::RecordBatch::deserialize(bytes);
}
inline storage::RecordBatch sparseBatch(Offset base = 0, int32_t last_delta = 9) {
    storage::RecordBatch batch;
    batch.setBaseOffset(base);
    Record first("first", "kept-2");
    first.offset_delta = 2;
    Record last("last", "kept-9");
    last.offset_delta = 9;
    batch.addRecord(first);
    batch.addRecord(last);
    return batchWithWireSpan(batch, last_delta);
}

}  // namespace kawasan::test_support
