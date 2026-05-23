// Phase EX-2 (§6.2): libFuzzer driver for RecordBatch::deserialize.
//
// Catches CRC-validation bypass, varint overflow in record headers,
// compression-codec dispatch bugs, and ZSTD frame-error paths.

#include <cstdint>
#include <cstddef>
#include <vector>

#include "kawasan/storage/record_batch.h"

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size == 0) return 0;
    try {
        std::vector<uint8_t> bytes(data, data + size);
        auto batch = kawasan::storage::RecordBatch::deserialize(bytes);
        (void)batch.baseOffset();
        (void)batch.records();
    } catch (const std::exception&) {
        // Expected for malformed input.
    }
    return 0;
}
