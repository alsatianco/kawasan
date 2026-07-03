// P13: libFuzzer driver for FetchRequest::decode across every version,
// including the flexible v12 encoding and the wire-ready (not yet
// advertised) v13+ topic-ID form — both must reject garbage cleanly.

#include <cstddef>
#include <cstdint>
#include <vector>

#include "kawasan/common/buffer.h"
#include "kawasan/protocol/fetch_request.h"

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size < 2)
        return 0;
    const int16_t api_version = static_cast<int16_t>(data[0] % 17);  // v0..v16
    std::vector<uint8_t> bytes(data + 1, data + size);

    try {
        kawasan::Buffer buf(bytes);
        kawasan::protocol::FetchRequest request;
        request.decode(buf, api_version);
        for (const auto& topic : request.topics()) {
            (void)topic;
        }
    } catch (const std::exception&) {
        // Expected for malformed input.
    }
    return 0;
}
