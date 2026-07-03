// P13: libFuzzer driver for MetadataRequest::decode across every version
// (flexible from v9, topic-ID form at v10+).

#include <cstddef>
#include <cstdint>
#include <vector>

#include "kawasan/common/buffer.h"
#include "kawasan/protocol/metadata_request.h"

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size < 2)
        return 0;
    const int16_t api_version = static_cast<int16_t>(data[0] % 13);  // v0..v12
    std::vector<uint8_t> bytes(data + 1, data + size);

    try {
        kawasan::Buffer buf(bytes);
        kawasan::protocol::MetadataRequest request;
        request.decode(buf, api_version);
        (void)request.topics();
    } catch (const std::exception&) {
        // Expected for malformed input.
    }
    return 0;
}
