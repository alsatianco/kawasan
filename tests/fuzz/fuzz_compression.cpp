// P13: libFuzzer driver for the record-batch decompression paths (GZIP,
// SNAPPY, LZ4, ZSTD) plus the uncompressed record parser.
//
// RecordBatch::decode validates CRC-32C before touching the payload, so raw
// random bytes would die at the CRC gate and never reach a codec. This
// harness wraps the fuzz input in a structurally valid v2 batch header with
// a CORRECT CRC — the codec sees attacker-controlled compressed frames, the
// exact production scenario for a hostile producer.

#include <cstddef>
#include <cstdint>
#include <vector>

#include "kawasan/storage/record_batch.h"

namespace {

// CRC-32C (Castagnoli, reflected 0x1EDC6F41) — mirrors the implementation
// the decoder validates against.
uint32_t crc32c(const uint8_t* data, size_t size) {
    static uint32_t table[256];
    static bool init = false;
    if (!init) {
        for (uint32_t i = 0; i < 256; ++i) {
            uint32_t c = i;
            for (int k = 0; k < 8; ++k) {
                c = (c & 1) ? (0x82F63B78u ^ (c >> 1)) : (c >> 1);
            }
            table[i] = c;
        }
        init = true;
    }
    uint32_t crc = 0xFFFFFFFFu;
    for (size_t i = 0; i < size; ++i) {
        crc = table[(crc ^ data[i]) & 0xFF] ^ (crc >> 8);
    }
    return crc ^ 0xFFFFFFFFu;
}

void putBE16(std::vector<uint8_t>& out, uint16_t v) {
    out.push_back(static_cast<uint8_t>(v >> 8));
    out.push_back(static_cast<uint8_t>(v));
}

void putBE32(std::vector<uint8_t>& out, uint32_t v) {
    out.push_back(static_cast<uint8_t>(v >> 24));
    out.push_back(static_cast<uint8_t>(v >> 16));
    out.push_back(static_cast<uint8_t>(v >> 8));
    out.push_back(static_cast<uint8_t>(v));
}

void putBE64(std::vector<uint8_t>& out, uint64_t v) {
    putBE32(out, static_cast<uint32_t>(v >> 32));
    putBE32(out, static_cast<uint32_t>(v));
}

}  // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size < 3)
        return 0;
    // Codec 0..4: NONE, GZIP, SNAPPY, LZ4, ZSTD — NONE exercises the varint
    // record parser on raw attacker bytes.
    const uint16_t attributes = data[0] % 5;
    const uint32_t record_count = data[1] % 4;
    const uint8_t* payload = data + 2;
    const size_t payload_size = size - 2;

    // v2 layout: base_offset(8) batch_length(4) ple(4) magic(1) crc(4)
    // attributes(2) last_offset_delta(4) first_ts(8) max_ts(8) pid(8)
    // pepoch(2) base_seq(4) record_count(4) records...
    // batch_length counts from ple onward; CRC covers attributes onward.
    std::vector<uint8_t> batch;
    batch.reserve(61 + payload_size);
    putBE64(batch, 0);                                         // base_offset
    putBE32(batch, static_cast<uint32_t>(49 + payload_size));  // batch_length
    putBE32(batch, 0);                                         // partition_leader_epoch
    batch.push_back(2);                                        // magic
    const size_t crc_pos = batch.size();
    putBE32(batch, 0);  // crc placeholder
    const size_t crc_start = batch.size();
    putBE16(batch, attributes);
    putBE32(batch, 0);                          // last_offset_delta
    putBE64(batch, 0);                          // first_timestamp
    putBE64(batch, 0);                          // max_timestamp
    putBE64(batch, static_cast<uint64_t>(-1));  // producer_id
    putBE16(batch, 0xFFFF);                     // producer_epoch
    putBE32(batch, 0xFFFFFFFFu);                // base_sequence
    putBE32(batch, record_count);
    batch.insert(batch.end(), payload, payload + payload_size);

    const uint32_t crc = crc32c(batch.data() + crc_start, batch.size() - crc_start);
    batch[crc_pos] = static_cast<uint8_t>(crc >> 24);
    batch[crc_pos + 1] = static_cast<uint8_t>(crc >> 16);
    batch[crc_pos + 2] = static_cast<uint8_t>(crc >> 8);
    batch[crc_pos + 3] = static_cast<uint8_t>(crc);

    try {
        auto decoded = kawasan::storage::RecordBatch::deserialize(batch);
        (void)decoded.records();
    } catch (const std::exception&) {
        // Expected for malformed compressed frames.
    }
    return 0;
}
