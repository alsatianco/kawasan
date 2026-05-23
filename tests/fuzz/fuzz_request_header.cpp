// Phase EX-2 (§6.2): libFuzzer driver for RequestHeader::decode.
//
// Catches malformed-header bugs: negative lengths, integer overflows,
// truncated tagged-fields varints, header-v1-vs-v2 confusion.

#include <cstdint>
#include <cstddef>

#include "kawasan/common/buffer.h"
#include "kawasan/protocol/request_header.h"

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size == 0) return 0;
    try {
        std::vector<uint8_t> bytes(data, data + size);
        kawasan::Buffer buf(std::move(bytes));
        kawasan::protocol::RequestHeader header;
        header.decode(buf);
        // Sanity probes — exercising getters shouldn't crash.
        (void)header.apiKey();
        (void)header.apiVersion();
        (void)header.correlationId();
        (void)header.clientId();
    } catch (const std::exception&) {
        // Decoder throws on malformed input — that's the contract; not a crash.
    }
    return 0;
}
