#pragma once

#include <arpa/inet.h>

#include <cstdint>
#include <cstring>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace kawasan {

/// @brief Binary buffer for reading/writing protocol data
class Buffer {
public:
    Buffer() = default;
    explicit Buffer(size_t capacity) { data_.reserve(capacity); }
    explicit Buffer(std::vector<uint8_t> data) : data_(std::move(data)) {}

    // Write operations
    void writeInt8(int8_t value) { data_.push_back(static_cast<uint8_t>(value)); }

    void writeInt16(int16_t value) {
        uint16_t net_value = htons(static_cast<uint16_t>(value));
        writeBytes(reinterpret_cast<const uint8_t*>(&net_value), sizeof(net_value));
    }

    void writeInt32(int32_t value) {
        uint32_t net_value = htonl(static_cast<uint32_t>(value));
        writeBytes(reinterpret_cast<const uint8_t*>(&net_value), sizeof(net_value));
    }

    void writeInt64(int64_t value) {
        // Network byte order for 64-bit
        uint64_t net_value = htobe64(static_cast<uint64_t>(value));
        writeBytes(reinterpret_cast<const uint8_t*>(&net_value), sizeof(net_value));
    }

    void writeString(const std::string& str) {
        writeInt16(static_cast<int16_t>(str.size()));
        writeBytes(reinterpret_cast<const uint8_t*>(str.data()), str.size());
    }

    void writeNullableString(const std::optional<std::string>& str) {
        if (str) {
            writeString(*str);
        } else {
            writeInt16(-1);
        }
    }

    void writeBytes(const uint8_t* data, size_t size) {
        data_.insert(data_.end(), data, data + size);
    }

    void writeBytes(const std::vector<uint8_t>& bytes) {
        writeInt32(static_cast<int32_t>(bytes.size()));
        data_.insert(data_.end(), bytes.begin(), bytes.end());
    }

    void writeNullableBytes(const std::optional<std::vector<uint8_t>>& bytes) {
        if (bytes) {
            writeBytes(*bytes);
        } else {
            writeInt32(-1);
        }
    }

    void writeUnsignedVarInt(uint32_t value) {
        while (value > 0x7F) {
            writeInt8(static_cast<int8_t>((value & 0x7F) | 0x80));
            value >>= 7;
        }
        writeInt8(static_cast<int8_t>(value & 0x7F));
    }

    void writeVarInt(int32_t value) { writeUnsignedVarInt(encodeZigZag32(value)); }

    void writeUnsignedVarLong(uint64_t value) {
        while (value > 0x7F) {
            writeInt8(static_cast<int8_t>((value & 0x7F) | 0x80));
            value >>= 7;
        }
        writeInt8(static_cast<int8_t>(value & 0x7F));
    }

    void writeVarLong(int64_t value) { writeUnsignedVarLong(encodeZigZag64(value)); }

    // ============================================================
    // 0A.5: Flexible-versions encoding (KIP-482).
    //
    // Starting with Kafka 2.4, "flexible versions" of each protocol API
    // replaced int16/int32 length prefixes with unsigned-varints (compact
    // form) and added an optional tagged-fields trailer to every header
    // and body. Helpers here are the building blocks for every flexible
    // encoder/decoder in `src/protocol/`.
    //
    // Wire encoding:
    //  - Compact string:    UNSIGNED_VARINT(len + 1) | bytes
    //  - Compact nullable:  UNSIGNED_VARINT(0)              if null
    //                        UNSIGNED_VARINT(len + 1) | bytes  otherwise
    //  - Compact array len: UNSIGNED_VARINT(count + 1)
    //  - Tagged fields:     UNSIGNED_VARINT(count)
    //                        for each: UNSIGNED_VARINT(tag) | UNSIGNED_VARINT(len) | bytes
    // ============================================================

    void writeCompactString(const std::string& str) {
        writeUnsignedVarInt(static_cast<uint32_t>(str.size()) + 1);
        if (!str.empty()) {
            writeBytes(reinterpret_cast<const uint8_t*>(str.data()), str.size());
        }
    }

    void writeCompactNullableString(const std::optional<std::string>& str) {
        if (!str.has_value()) {
            writeUnsignedVarInt(0);
        } else {
            writeCompactString(*str);
        }
    }

    void writeCompactBytes(const std::vector<uint8_t>& bytes) {
        writeUnsignedVarInt(static_cast<uint32_t>(bytes.size()) + 1);
        if (!bytes.empty()) {
            writeBytes(bytes.data(), bytes.size());
        }
    }

    void writeCompactNullableBytes(const std::optional<std::vector<uint8_t>>& bytes) {
        if (!bytes.has_value()) {
            writeUnsignedVarInt(0);
        } else {
            writeCompactBytes(*bytes);
        }
    }

    /// @brief Write the length prefix of a compact array. Caller writes elements.
    /// Pass -1 for a null array (encoded as UNSIGNED_VARINT(0)).
    void writeCompactArrayLen(int32_t count) {
        if (count < 0) {
            writeUnsignedVarInt(0);
        } else {
            writeUnsignedVarInt(static_cast<uint32_t>(count) + 1);
        }
    }

    /// @brief Write an empty tagged-fields trailer (count=0). Most encoders
    /// will only ever need this form; the broker does not currently produce
    /// tag-bearing responses.
    void writeEmptyTaggedFields() { writeUnsignedVarInt(0); }

    std::string readCompactString() {
        const uint32_t raw = readUnsignedVarInt();
        if (raw == 0) {
            throw std::runtime_error("Null compact string encountered (expected non-null)");
        }
        const size_t len = raw - 1;
        checkAvailable(len);
        std::string result(reinterpret_cast<const char*>(&data_[read_pos_]), len);
        read_pos_ += len;
        return result;
    }

    std::optional<std::string> readCompactNullableString() {
        const uint32_t raw = readUnsignedVarInt();
        if (raw == 0) {
            return std::nullopt;
        }
        const size_t len = raw - 1;
        checkAvailable(len);
        std::string result(reinterpret_cast<const char*>(&data_[read_pos_]), len);
        read_pos_ += len;
        return result;
    }

    std::vector<uint8_t> readCompactBytes() {
        const uint32_t raw = readUnsignedVarInt();
        if (raw == 0) {
            throw std::runtime_error("Null compact bytes encountered (expected non-null)");
        }
        return readBytes(raw - 1);
    }

    std::optional<std::vector<uint8_t>> readCompactNullableBytes() {
        const uint32_t raw = readUnsignedVarInt();
        if (raw == 0) {
            return std::nullopt;
        }
        return readBytes(raw - 1);
    }

    /// @brief Read the length prefix of a compact array. Returns -1 for null.
    int32_t readCompactArrayLen() {
        const uint32_t raw = readUnsignedVarInt();
        if (raw == 0) {
            return -1;
        }
        return static_cast<int32_t>(raw - 1);
    }

    /// @brief Read an array length (compact or classic INT32) and validate it
    /// against the bytes left in the buffer. Every array element occupies at
    /// least one wire byte, so a declared count larger than `remaining()` is
    /// provably malformed — rejecting it here prevents a hostile length field
    /// from driving an unbounded reserve()/resize() (a trivial OOM DoS).
    /// Returns a non-negative element count (a null/absent array yields 0).
    int32_t readArrayLength(bool flexible) {
        const int32_t count = flexible ? readCompactArrayLen() : readInt32();
        if (count <= 0) {
            return 0;
        }
        if (static_cast<size_t>(count) > remaining()) {
            throw std::runtime_error("Array length " + std::to_string(count) +
                                     " exceeds remaining buffer (" + std::to_string(remaining()) +
                                     " bytes)");
        }
        return count;
    }

    /// @brief Read and discard the tagged-fields trailer. We do not currently
    /// surface tagged-field values to handlers; any future per-API tag handling
    /// should specialize this call.
    void skipTaggedFields() {
        const uint32_t count = readUnsignedVarInt();
        for (uint32_t i = 0; i < count; ++i) {
            (void)readUnsignedVarInt();  // tag
            const uint32_t len = readUnsignedVarInt();
            checkAvailable(len);
            read_pos_ += len;
        }
    }

    // Read operations
    int8_t readInt8() {
        checkAvailable(1);
        return static_cast<int8_t>(data_[read_pos_++]);
    }

    int16_t readInt16() {
        checkAvailable(2);
        uint16_t net_value;
        std::memcpy(&net_value, &data_[read_pos_], sizeof(net_value));
        read_pos_ += 2;
        return static_cast<int16_t>(ntohs(net_value));
    }

    int32_t readInt32() {
        checkAvailable(4);
        uint32_t net_value;
        std::memcpy(&net_value, &data_[read_pos_], sizeof(net_value));
        read_pos_ += 4;
        return static_cast<int32_t>(ntohl(net_value));
    }

    int64_t readInt64() {
        checkAvailable(8);
        uint64_t net_value;
        std::memcpy(&net_value, &data_[read_pos_], sizeof(net_value));
        read_pos_ += 8;
        return static_cast<int64_t>(be64toh(net_value));
    }

    std::string readString() {
        int16_t size = readInt16();
        if (size < 0) {
            throw std::runtime_error("Null string encountered");
        }
        checkAvailable(size);
        std::string result(reinterpret_cast<const char*>(&data_[read_pos_]), size);
        read_pos_ += size;
        return result;
    }

    std::optional<std::string> readNullableString() {
        int16_t size = readInt16();
        if (size < 0) {
            return std::nullopt;
        }
        checkAvailable(size);
        std::string result(reinterpret_cast<const char*>(&data_[read_pos_]), size);
        read_pos_ += size;
        return result;
    }

    std::vector<uint8_t> readBytes(size_t size) {
        checkAvailable(size);
        std::vector<uint8_t> result(data_.begin() + read_pos_, data_.begin() + read_pos_ + size);
        read_pos_ += size;
        return result;
    }

    std::vector<uint8_t> readBytesWithLength() {
        int32_t size = readInt32();
        if (size < 0) {
            throw std::runtime_error("Null bytes encountered");
        }
        return readBytes(size);
    }

    std::optional<std::vector<uint8_t>> readNullableBytes() {
        int32_t size = readInt32();
        if (size < 0) {
            return std::nullopt;
        }
        return readBytes(size);
    }

    uint32_t readUnsignedVarInt() {
        uint32_t value = 0;
        int shift = 0;
        while (true) {
            const uint8_t byte = static_cast<uint8_t>(readInt8());
            value |= static_cast<uint32_t>(byte & 0x7F) << shift;
            if ((byte & 0x80) == 0) {
                break;
            }
            shift += 7;
            if (shift > 28) {
                throw std::runtime_error("VarInt too long");
            }
        }
        return value;
    }

    int32_t readVarInt() { return decodeZigZag32(readUnsignedVarInt()); }

    uint64_t readUnsignedVarLong() {
        uint64_t value = 0;
        int shift = 0;
        while (true) {
            const uint8_t byte = static_cast<uint8_t>(readInt8());
            value |= static_cast<uint64_t>(byte & 0x7F) << shift;
            if ((byte & 0x80) == 0) {
                break;
            }
            shift += 7;
            if (shift > 63) {
                throw std::runtime_error("VarLong too long");
            }
        }
        return value;
    }

    int64_t readVarLong() { return decodeZigZag64(readUnsignedVarLong()); }

    // Buffer management
    size_t size() const { return data_.size(); }
    size_t remaining() const { return data_.size() - read_pos_; }
    size_t position() const { return read_pos_; }
    void setPosition(size_t pos) { read_pos_ = pos; }
    void reset() { read_pos_ = 0; }
    void clear() {
        data_.clear();
        read_pos_ = 0;
    }

    const uint8_t* data() const { return data_.data(); }
    uint8_t* data() { return data_.data(); }
    const std::vector<uint8_t>& vector() const { return data_; }
    std::vector<uint8_t> takeVector() {
        read_pos_ = 0;
        return std::move(data_);
    }

private:
    void checkAvailable(size_t size) const {
        if (read_pos_ + size > data_.size()) {
            throw std::runtime_error("Buffer underflow");
        }
    }

    static uint64_t htobe64(uint64_t value) {
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
        return __builtin_bswap64(value);
#else
        return value;
#endif
    }

    static uint64_t be64toh(uint64_t value) { return htobe64(value); }

    static uint32_t encodeZigZag32(int32_t value) {
        return (static_cast<uint32_t>(value) << 1) ^ static_cast<uint32_t>(value >> 31);
    }

    static int32_t decodeZigZag32(uint32_t value) {
        return static_cast<int32_t>((value >> 1) ^ static_cast<uint32_t>(0 - (value & 1)));
    }

    static uint64_t encodeZigZag64(int64_t value) {
        return (static_cast<uint64_t>(value) << 1) ^ static_cast<uint64_t>(value >> 63);
    }

    static int64_t decodeZigZag64(uint64_t value) {
        return static_cast<int64_t>((value >> 1) ^ static_cast<uint64_t>(0 - (value & 1)));
    }

    std::vector<uint8_t> data_;
    size_t read_pos_ = 0;
};

}  // namespace kawasan
