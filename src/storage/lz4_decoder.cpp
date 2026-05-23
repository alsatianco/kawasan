#include "kawasan/storage/lz4_decoder.h"

#include <cstring>
#include <sstream>
#include <stdexcept>
#include <vector>

#include <lz4.h>

namespace kawasan::storage {

namespace {

constexpr uint32_t kLz4FrameMagic = 0x184D2204;
constexpr uint32_t kLz4FrameIncompressibleMask = 0x80000000;
constexpr size_t kFrameDescriptorOverhead = 1;  // HC byte

struct Reader {
    const uint8_t* data;
    size_t size;
    size_t pos = 0;

    size_t remaining() const { return size - pos; }

    uint8_t readU8() {
        if (remaining() < 1) {
            throw std::runtime_error("Truncated LZ4 frame");
        }
        return data[pos++];
    }

    uint32_t readU32() {
        if (remaining() < 4) {
            throw std::runtime_error("Truncated LZ4 frame");
        }
        uint32_t value;
        std::memcpy(&value, data + pos, sizeof(value));
        pos += sizeof(value);
        return value;
    }

    void skip(size_t n) {
        if (remaining() < n) {
            throw std::runtime_error("Truncated LZ4 frame");
        }
        pos += n;
    }

    const uint8_t* currentPtr() const { return data + pos; }
};

int decodeMaxBlockSize(uint8_t bd) {
    if ((bd & 0x0F) != 0 || (bd & 0x80) != 0) {
        throw std::runtime_error("Invalid LZ4 block descriptor");
    }
    const int block_value = (bd >> 4) & 0x07;
    if (block_value < 4 || block_value > 7) {
        throw std::runtime_error("Unsupported LZ4 block size value");
    }
    return 1 << ((2 * block_value) + 8);
}

struct FrameFlags {
    bool content_checksum = false;
    bool content_size = false;
    bool block_checksum = false;
    bool block_independence = false;
    int version = 0;
};

FrameFlags parseFlags(uint8_t flg) {
    FrameFlags flags;
    const int reserved = flg & 0x03;
    flags.content_checksum = (flg >> 2) & 0x01;
    flags.content_size = (flg >> 3) & 0x01;
    flags.block_checksum = (flg >> 4) & 0x01;
    flags.block_independence = (flg >> 5) & 0x01;
    flags.version = (flg >> 6) & 0x03;

    if (reserved != 0) {
        throw std::runtime_error("Invalid LZ4 frame flags");
    }
    if (flags.version != 1) {
        throw std::runtime_error("Unsupported LZ4 frame version");
    }
    if (!flags.block_independence) {
        throw std::runtime_error("Dependent LZ4 blocks are unsupported");
    }
    return flags;
}

}  // namespace

std::vector<uint8_t> decodeKafkaLz4Frame(const std::vector<uint8_t>& payload) {
    if (payload.size() < 7) {
        throw std::runtime_error("LZ4 frame too short");
    }

    Reader reader{payload.data(), payload.size()};

    const uint32_t magic = reader.readU32();
    if (magic != kLz4FrameMagic) {
        std::ostringstream oss;
        oss << "Unsupported LZ4 frame magic: 0x" << std::hex << magic;
        throw std::runtime_error(oss.str());
    }

    const FrameFlags flags = parseFlags(reader.readU8());
    const int max_block_size = decodeMaxBlockSize(reader.readU8());

    if (flags.content_size) {
        reader.skip(sizeof(uint64_t));
    }

    if (reader.remaining() < kFrameDescriptorOverhead) {
        throw std::runtime_error("Truncated LZ4 descriptor");
    }
    reader.skip(kFrameDescriptorOverhead);  // descriptor checksum (ignored)

    std::vector<uint8_t> scratch(static_cast<size_t>(max_block_size));
    std::vector<uint8_t> output;
    output.reserve(payload.size() * 2);

    while (true) {
        if (reader.remaining() < sizeof(uint32_t)) {
            throw std::runtime_error("Truncated LZ4 block header");
        }
        uint32_t block_header = reader.readU32();
        const bool compressed = (block_header & kLz4FrameIncompressibleMask) == 0;
        block_header &= ~kLz4FrameIncompressibleMask;

        if (block_header == 0) {
            if (flags.content_checksum) {
                reader.skip(sizeof(uint32_t));  // skip checksum
            }
            break;
        }

        if (block_header > static_cast<uint32_t>(max_block_size)) {
            throw std::runtime_error("LZ4 block exceeds negotiated size");
        }

        if (reader.remaining() < block_header) {
            throw std::runtime_error("Truncated LZ4 block payload");
        }

        if (compressed) {
            const int decoded =
                LZ4_decompress_safe(reinterpret_cast<const char*>(reader.currentPtr()),
                                    reinterpret_cast<char*>(scratch.data()),
                                    static_cast<int>(block_header), max_block_size);
            if (decoded < 0) {
                throw std::runtime_error("LZ4 decompression failed");
            }
            output.insert(output.end(), scratch.begin(), scratch.begin() + decoded);
        } else {
            output.insert(output.end(), reader.currentPtr(),
                          reader.currentPtr() + block_header);
        }
        reader.skip(block_header);

        if (flags.block_checksum) {
            reader.skip(sizeof(uint32_t));
        }
    }

    return output;
}

}  // namespace kawasan::storage
