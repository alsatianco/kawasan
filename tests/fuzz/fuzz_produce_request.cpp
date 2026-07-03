// P13: libFuzzer driver for ProduceRequest::decode across every advertised
// version, plus the legacy-MessageSet down-conversion entry point
// (RecordBatch::deserializeFromProduceRequest) that hostile producers reach
// directly.
//
// The first input byte selects the api_version so one corpus covers the
// whole version matrix (including a couple of not-yet-advertised versions,
// which must fail cleanly rather than crash).

#include <cstddef>
#include <cstdint>
#include <vector>

#include "kawasan/common/buffer.h"
#include "kawasan/protocol/produce_request.h"
#include "kawasan/storage/record_batch.h"

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size < 2)
        return 0;
    const int16_t api_version = static_cast<int16_t>(data[0] % 12);  // v0..v11
    std::vector<uint8_t> bytes(data + 1, data + size);

    try {
        kawasan::Buffer buf(bytes);
        kawasan::protocol::ProduceRequest request;
        request.decode(buf, api_version);
        for (const auto& topic : request.topics()) {
            (void)topic;
        }
    } catch (const std::exception&) {
        // Expected for malformed input.
    }

    try {
        auto batch = kawasan::storage::RecordBatch::deserializeFromProduceRequest(bytes);
        (void)batch.records();
    } catch (const std::exception&) {
        // Expected for malformed input.
    }
    return 0;
}
