#include "kawasan/storage/record_batch.h"

#include <arpa/inet.h>
#include <snappy.h>
#include <zlib.h>
#include <zstd.h>

#include <cstring>
#include <stdexcept>

#include "kawasan/common/logger.h"
#include "kawasan/storage/lz4_decoder.h"

namespace kawasan::storage {

namespace {

void writeVarBytes(Buffer& buffer, const std::optional<std::vector<uint8_t>>& bytes) {
    if (!bytes) {
        buffer.writeVarInt(-1);
        return;
    }
    buffer.writeVarInt(static_cast<int32_t>(bytes->size()));
    if (!bytes->empty()) {
        buffer.writeBytes(bytes->data(), bytes->size());
    }
}

void writeVarBytes(Buffer& buffer, const std::vector<uint8_t>& bytes) {
    buffer.writeVarInt(static_cast<int32_t>(bytes.size()));
    if (!bytes.empty()) {
        buffer.writeBytes(bytes.data(), bytes.size());
    }
}

std::optional<std::vector<uint8_t>> readVarBytes(Buffer& buffer) {
    const int32_t length = buffer.readVarInt();
    if (length < 0) {
        return std::nullopt;
    }
    return buffer.readBytes(static_cast<size_t>(length));
}

// 0A.1: Kafka RecordBatch v2 uses CRC-32C (Castagnoli polynomial 0x1EDC6F41),
// NOT IEEE CRC-32. Zlib's crc32 implements the IEEE polynomial — wire-
// incompatible with every real Kafka client. The previous encoder used it
// and worked only because the decoder never validated the CRC. We provide
// a portable table-driven CRC-32C here (hardware intrinsics are a Phase 5
// performance follow-up).
class Crc32cTable {
public:
    Crc32cTable() {
        constexpr uint32_t kPoly = 0x82F63B78u;  // bit-reversed 0x1EDC6F41
        for (uint32_t i = 0; i < 256; ++i) {
            uint32_t c = i;
            for (int j = 0; j < 8; ++j) {
                c = (c & 1) ? (kPoly ^ (c >> 1)) : (c >> 1);
            }
            table_[i] = c;
        }
    }
    uint32_t lookup(uint8_t b) const { return table_[b]; }

private:
    uint32_t table_[256]{};
};

uint32_t crc32c(const uint8_t* data, size_t n) {
    static const Crc32cTable kTable;
    uint32_t c = 0xFFFFFFFFu;
    for (size_t i = 0; i < n; ++i) {
        c = kTable.lookup(static_cast<uint8_t>((c ^ data[i]) & 0xff)) ^ (c >> 8);
    }
    return c ^ 0xFFFFFFFFu;
}

}  // namespace

void RecordBatch::addRecord(const Record& record) {
    records_.push_back(record);
    updateDerivedFields();
}

CompressionType RecordBatch::compressionType() const {
    return static_cast<CompressionType>(attributes_ & 0x07);
}

bool RecordBatch::isTransactional() const {
    return (attributes_ & (1 << 4)) != 0;
}

bool RecordBatch::isControlBatch() const {
    return (attributes_ & (1 << 5)) != 0;
}

void RecordBatch::encode(Buffer& buffer) const {
    const auto record_payload = encodeRecords();

    Buffer crc_buffer;
    crc_buffer.writeInt16(attributes_);
    crc_buffer.writeInt32(last_offset_delta_);
    crc_buffer.writeInt64(first_timestamp_);
    crc_buffer.writeInt64(max_timestamp_);
    crc_buffer.writeInt64(producer_id_);
    crc_buffer.writeInt16(producer_epoch_);
    crc_buffer.writeInt32(base_sequence_);
    crc_buffer.writeInt32(static_cast<int32_t>(records_.size()));  // records count
    if (!record_payload.empty()) {
        crc_buffer.writeBytes(record_payload.data(), record_payload.size());
    }

    // 0A.1: Kafka v2 RecordBatch uses CRC-32C (Castagnoli). Previously we used
    // zlib's CRC-32 here — wire-incompatible with real Kafka clients, which
    // worked only because the decoder never validated the CRC.
    const uint32_t crc = crc32c(crc_buffer.data(), crc_buffer.size());

    Buffer local;
    local.writeInt64(base_offset_);
    const size_t length_offset = local.size();
    local.writeInt32(0);  // batch_length placeholder
    const size_t body_start = local.size();

    local.writeInt32(partition_leader_epoch_);
    local.writeInt8(magic_);
    const size_t crc_offset = local.size();
    local.writeInt32(0);  // CRC placeholder
    local.writeInt16(attributes_);
    local.writeInt32(last_offset_delta_);
    local.writeInt64(first_timestamp_);
    local.writeInt64(max_timestamp_);
    local.writeInt64(producer_id_);
    local.writeInt16(producer_epoch_);
    local.writeInt32(base_sequence_);
    local.writeInt32(static_cast<int32_t>(records_.size()));  // records count
    if (!record_payload.empty()) {
        local.writeBytes(record_payload.data(), record_payload.size());
    }

    const int32_t batch_length = static_cast<int32_t>(local.size() - body_start);

    auto* raw = local.data();
    const int32_t net_length = htonl(batch_length);
    std::memcpy(raw + length_offset, &net_length, sizeof(net_length));
    const uint32_t net_crc = htonl(crc);
    std::memcpy(raw + crc_offset, &net_crc, sizeof(net_crc));

    batch_length_ = batch_length;
    crc_ = crc;

    buffer.writeBytes(local.data(), local.size());
}

void RecordBatch::decode(Buffer& buffer) {
    const size_t start_pos = buffer.position();
    base_offset_ = buffer.readInt64();
    batch_length_ = buffer.readInt32();

    // Peek at the magic byte to determine format
    // For magic v2: partition_leader_epoch (4) + magic (1) = offset 5
    // For magic v1: crc (4) + magic (1) = offset 5
    const size_t magic_offset = start_pos + 8 + 4 + 4;  // base_offset + batch_length + 4 bytes
    if (buffer.size() < magic_offset + 1) {
        throw std::runtime_error("Buffer too small to read magic byte");
    }
    const int8_t peek_magic = static_cast<int8_t>(buffer.data()[magic_offset]);

    size_t records_bytes = 0;

    if (peek_magic == 2) {
        // Magic v2: Record Batch format (Kafka 0.11+)
        partition_leader_epoch_ = buffer.readInt32();
        magic_ = buffer.readInt8();
        crc_ = static_cast<uint32_t>(buffer.readInt32());

        // 0A.1: validate the CRC over the bytes from `attributes` to end-of-batch.
        // batch_length_ measures from `partition_leader_epoch` onwards, so the
        // CRC-covered region is (batch_length_ - 9) bytes starting at offset 21
        // from `start_pos` (skipping base_offset=8, batch_length=4, ple=4,
        // magic=1, crc=4 → 21).
        const size_t crc_data_start = start_pos + 21;
        if (batch_length_ < 9) {
            throw std::runtime_error("Invalid batch_length=" + std::to_string(batch_length_) +
                                     " (too short for CRC)");
        }
        const size_t crc_data_length = static_cast<size_t>(batch_length_) - 9;
        if (crc_data_start + crc_data_length > buffer.size()) {
            throw std::runtime_error("Batch truncated: CRC-covered region extends past buffer");
        }
        // 0A.1: validate with CRC-32C (Kafka v2 standard).
        const uint32_t computed_crc = crc32c(buffer.data() + crc_data_start, crc_data_length);
        if (computed_crc != crc_) {
            Logger::warn("RecordBatch CRC mismatch (wire={:#x}, computed={:#x}, batch_length={})",
                         crc_, computed_crc, batch_length_);
            throw std::runtime_error("RecordBatch CRC check failed");
        }

        attributes_ = buffer.readInt16();
        last_offset_delta_ = buffer.readInt32();
        first_timestamp_ = buffer.readInt64();
        max_timestamp_ = buffer.readInt64();
        producer_id_ = buffer.readInt64();
        producer_epoch_ = buffer.readInt16();
        base_sequence_ = buffer.readInt32();

        // Read the records count (INT32)
        const int32_t records_count = buffer.readInt32();
        Logger::debug("RecordBatch has {} records", records_count);
        if (records_count < 0) {
            throw std::runtime_error("Invalid negative records count");
        }

        const size_t header_bytes =
            buffer.position() - (start_pos + sizeof(int64_t) + sizeof(int32_t));
        if (static_cast<int64_t>(header_bytes) > batch_length_) {
            throw std::runtime_error("Invalid batch length");
        }
        records_bytes = static_cast<size_t>(batch_length_) - header_bytes;
    } else if (peek_magic == 1 || peek_magic == 0) {
        // Legacy MessageSet format — decode() only handles a single message entry,
        // but a MessageSet may contain many.  Throw here so that callers like
        // deserializeFromProduceRequest() fall through to the correct MessageSet
        // loop that handles multiple messages.
        throw std::runtime_error("Legacy MessageSet format (magic=" + std::to_string(peek_magic) +
                                 ") requires MessageSet parser");
    } else {
        Logger::error("Unsupported magic byte version: {}", static_cast<int>(peek_magic));
        throw std::runtime_error("Unsupported magic byte version");
    }

    // For v2, continue reading the records from the batch
    Logger::debug("Before reading records: buffer.remaining()={}, records_bytes={}, "
                  "buffer.position()={}, buffer.size()={}",
                  buffer.remaining(), records_bytes, buffer.position(), buffer.size());
    if (buffer.remaining() < records_bytes) {
        Logger::error("Buffer underflow: remaining={} < needed={}", buffer.remaining(),
                      records_bytes);
        throw std::runtime_error("Buffer underflow");
    }

    // Extract records payload (may be compressed)
    std::vector<uint8_t> records_payload = buffer.readBytes(records_bytes);

    // Decompress if needed
    const auto compression = static_cast<CompressionType>(attributes_ & 0x07);
    Logger::debug("RecordBatch compression type: {}, attributes: {:#x}, payload size: {}",
                  static_cast<int>(compression), attributes_, records_payload.size());
    std::vector<uint8_t> decompressed;
    if (compression == CompressionType::NONE) {
        // Use payload as-is
        decompressed = std::move(records_payload);
    } else if (compression == CompressionType::GZIP) {
        // Inflate using zlib (auto-detect zlib/gzip header)
        z_stream strm{};
        strm.next_in = const_cast<Bytef*>(reinterpret_cast<const Bytef*>(records_payload.data()));
        strm.avail_in = static_cast<uInt>(records_payload.size());
        if (inflateInit2(&strm, 15 + 32) != Z_OK) {
            throw std::runtime_error("Failed to initialize zlib inflater");
        }
        constexpr size_t kChunk = 64 * 1024;
        int ret = Z_OK;
        std::vector<uint8_t> out;
        out.reserve(records_payload.size() * 2 + kChunk);
        do {
            size_t prev_size = out.size();
            out.resize(prev_size + kChunk);
            strm.next_out = reinterpret_cast<Bytef*>(out.data() + prev_size);
            strm.avail_out = static_cast<uInt>(kChunk);
            ret = inflate(&strm, Z_NO_FLUSH);
            if (ret == Z_STREAM_ERROR) {
                inflateEnd(&strm);
                throw std::runtime_error("zlib stream error during inflate");
            }
            const size_t produced = kChunk - strm.avail_out;
            out.resize(prev_size + produced);
        } while (ret != Z_STREAM_END);
        inflateEnd(&strm);
        decompressed = std::move(out);
    } else if (compression == CompressionType::SNAPPY) {
        size_t uncompressed_len = 0;
        if (!snappy::GetUncompressedLength(reinterpret_cast<const char*>(records_payload.data()),
                                           records_payload.size(), &uncompressed_len)) {
            throw std::runtime_error("Invalid Snappy payload");
        }
        std::vector<char> out(uncompressed_len);
        if (!snappy::RawUncompress(reinterpret_cast<const char*>(records_payload.data()),
                                   records_payload.size(), out.data())) {
            throw std::runtime_error("Snappy decompression failed");
        }
        decompressed.assign(out.begin(), out.end());
    } else if (compression == CompressionType::LZ4) {
        decompressed = decodeKafkaLz4Frame(records_payload);
    } else if (compression == CompressionType::ZSTD) {
        // 0A.2: previously this path treated frame errors as "compression bug,
        // data is actually uncompressed" and silently fell through. That
        // converted real on-the-wire corruption into silently malformed
        // records on disk. Now we report CORRUPT_MESSAGE explicitly.
        unsigned long long content_size =
            ZSTD_getFrameContentSize(records_payload.data(), records_payload.size());
        if (content_size == ZSTD_CONTENTSIZE_ERROR) {
            throw std::runtime_error("Invalid ZSTD frame (CORRUPT_MESSAGE)");
        }
        size_t out_cap = content_size == ZSTD_CONTENTSIZE_UNKNOWN
                             ? records_payload.size() * 8 + 65536
                             : static_cast<size_t>(content_size);
        std::vector<uint8_t> out(out_cap);
        size_t dsize =
            ZSTD_decompress(out.data(), out.size(), records_payload.data(), records_payload.size());
        if (ZSTD_isError(dsize)) {
            throw std::runtime_error(std::string("ZSTD decompression failed: ") +
                                     ZSTD_getErrorName(dsize));
        }
        out.resize(dsize);
        decompressed = std::move(out);
    } else {
        throw std::runtime_error("Unsupported compression type");
    }

    // Parse records from the (decompressed) payload
    Buffer rec_buffer(std::move(decompressed));
    records_.clear();
    Logger::debug("Parsing records from payload: rec_buffer.size()={}", rec_buffer.size());
    // Print first 16 bytes
    if (rec_buffer.size() >= 16) {
        std::string hex;
        for (size_t i = 0; i < 16; ++i) {
            char buf[8];
            snprintf(buf, sizeof(buf), "%02x ", rec_buffer.data()[i]);
            hex += buf;
        }
        Logger::debug("First 16 bytes of records payload: {}", hex);
    }
    while (rec_buffer.remaining() > 0) {
        Logger::debug("Reading record: rec_buffer.remaining()={}, position={}",
                      rec_buffer.remaining(), rec_buffer.position());
        const int32_t record_length = rec_buffer.readVarInt();
        Logger::debug("Read record_length={}, rec_buffer.remaining()={}", record_length,
                      rec_buffer.remaining());
        if (record_length < 0) {
            throw std::runtime_error("Negative record length");
        }
        if (rec_buffer.remaining() < static_cast<size_t>(record_length)) {
            Logger::error("Buffer underflow: rec_buffer.remaining()={} < record_length={}",
                          rec_buffer.remaining(), record_length);
            throw std::runtime_error("Buffer underflow");
        }

        Buffer record_buffer(rec_buffer.readBytes(static_cast<size_t>(record_length)));
        Record record;
        (void)record_buffer.readInt8();  // record attributes
        const int64_t timestamp_delta = record_buffer.readVarLong();
        record.timestamp = first_timestamp_ + timestamp_delta;

        // 0A.3: capture the producer-supplied offset delta. We need this on
        // re-encode so that the broker round-trip is byte-identical and so the
        // upcoming ProducerStateManager can validate sequence-vs-offset.
        record.offset_delta = record_buffer.readVarInt();

        record.key = readVarBytes(record_buffer);
        record.value = readVarBytes(record_buffer);

        const int32_t header_count = record_buffer.readVarInt();
        if (header_count < 0) {
            throw std::runtime_error("Invalid header count");
        }
        record.headers.clear();
        record.headers.reserve(static_cast<size_t>(header_count));
        for (int32_t i = 0; i < header_count; ++i) {
            RecordHeader header;
            const int32_t key_length = record_buffer.readVarInt();
            if (key_length < 0) {
                throw std::runtime_error("Header key cannot be null");
            }
            auto key_bytes = record_buffer.readBytes(static_cast<size_t>(key_length));
            header.key.assign(key_bytes.begin(), key_bytes.end());

            auto header_value = readVarBytes(record_buffer);
            if (header_value) {
                header.value = std::move(*header_value);
            } else {
                header.value.clear();
            }
            record.headers.push_back(std::move(header));
        }

        records_.push_back(std::move(record));
    }
}

std::vector<uint8_t> RecordBatch::serialize() const {
    Buffer buffer;
    encode(buffer);
    return buffer.takeVector();
}

RecordBatch RecordBatch::deserialize(const std::vector<uint8_t>& data) {
    Buffer buffer(data);
    RecordBatch batch;
    batch.decode(buffer);
    return batch;
}

RecordBatch RecordBatch::deserialize(Buffer& buffer) {
    RecordBatch batch;
    batch.decode(buffer);
    return batch;
}

RecordBatch RecordBatch::deserializeFromProduceRequest(const std::vector<uint8_t>& data) {
    if (data.empty()) {
        throw std::runtime_error("Empty record batch in produce request");
    }

    // Modern producers (Produce v3+) already send the on-disk record batch layout, so
    // attempt to parse it directly first. This covers the common case and avoids
    // mistakenly treating a full RecordBatch as a legacy MessageSet.
    try {
        Logger::debug("Attempting to parse as v2 RecordBatch directly");
        return deserialize(data);
    } catch (const std::exception& ex) {
        Logger::debug("Produce payload did not decode as a full RecordBatch: {}. Falling back to "
                      "MessageSet parsing.",
                      ex.what());
    }

    Buffer buffer(data);
    RecordBatch batch;

    // MessageSet format (legacy wrapper) - wraps individual messages
    // Each entry: offset (8) + message_size (4) + [message content]
    // Message content can be:
    //   - magic 0/1: simple message (crc + magic + attributes + key + value)
    //   - magic 2: complete v2 RecordBatch (crc + magic + attributes + ... + records)
    Logger::info("Parsing MessageSet format from produce request");

    // Debug: print first 64 bytes
    if (data.size() >= 64) {
        std::string hex;
        for (size_t i = 0; i < 64 && i < data.size(); ++i) {
            char buf[8];
            snprintf(buf, sizeof(buf), "%02x ", data[i]);
            hex += buf;
        }
        Logger::debug("First 64 bytes of MessageSet: {}", hex);
    }

    // For compatibility, set default values for v2 fields
    batch.partition_leader_epoch_ = -1;
    batch.magic_ = 2;  // Convert to v2 internally
    batch.producer_id_ = -1;
    batch.producer_epoch_ = -1;
    batch.base_sequence_ = -1;
    batch.base_offset_ = 0;

    // Parse all messages in the message set
    batch.records_.clear();
    while (buffer.remaining() > 0) {
        if (buffer.remaining() < 12) {
            // Not enough bytes for offset + size
            break;
        }

        const int64_t msg_offset = buffer.readInt64();
        (void)msg_offset;  // Ignore offset from wire, will be reassigned

        const int32_t message_size = buffer.readInt32();
        if (message_size < 0) {
            throw std::runtime_error("Invalid negative message size in MessageSet format");
        }
        if (buffer.remaining() < static_cast<size_t>(message_size)) {
            throw std::runtime_error("Buffer underflow: message_size exceeds remaining bytes");
        }

        // Message content starts here (message_size bytes)
        const size_t msg_start = buffer.position();

        const uint32_t msg_crc = static_cast<uint32_t>(buffer.readInt32());
        (void)msg_crc;  // TODO: validate CRC

        const int8_t msg_magic = buffer.readInt8();
        const int8_t msg_attributes = buffer.readInt8();

        if (msg_magic == 2) {
            // This is a v2 RecordBatch wrapped in MessageSet format
            // Structure after crc+magic+attributes:
            // last_offset_delta (4) + first_timestamp (8) + max_timestamp (8) +
            // producer_id (8) + producer_epoch (2) + base_sequence (4) + record_count (4) + records

            // Debug logging
            Logger::debug(
                "Parsing v2 RecordBatch: message_size={}, position after crc+magic+attr={}",
                message_size, buffer.position() - msg_start);

            batch.last_offset_delta_ = buffer.readInt32();
            batch.first_timestamp_ = buffer.readInt64();
            batch.max_timestamp_ = buffer.readInt64();
            batch.producer_id_ = buffer.readInt64();
            batch.producer_epoch_ = buffer.readInt16();
            batch.base_sequence_ = buffer.readInt32();

            const int32_t record_count = buffer.readInt32();
            Logger::debug("Read record_count={}, position={}, bytes_consumed={}", record_count,
                          buffer.position(), buffer.position() - msg_start);
            if (record_count < 0) {
                throw std::runtime_error("Invalid record count in v2 message");
            }

            // Calculate how many bytes are left for records
            const size_t bytes_consumed_so_far = buffer.position() - msg_start;
            const size_t records_bytes = static_cast<size_t>(message_size) - bytes_consumed_so_far;

            // Extract records payload (may be compressed)
            std::vector<uint8_t> records_payload = buffer.readBytes(records_bytes);

            // Decompress if needed and parse records
            const auto compression = static_cast<CompressionType>(msg_attributes & 0x07);

            std::vector<uint8_t> decompressed;
            if (compression == CompressionType::NONE) {
                decompressed = std::move(records_payload);
            } else if (compression == CompressionType::GZIP) {
                z_stream strm{};
                strm.next_in =
                    const_cast<Bytef*>(reinterpret_cast<const Bytef*>(records_payload.data()));
                strm.avail_in = static_cast<uInt>(records_payload.size());
                if (inflateInit2(&strm, 15 + 32) != Z_OK) {
                    throw std::runtime_error("Failed to initialize zlib inflater");
                }
                constexpr size_t kChunk = 64 * 1024;
                int ret = Z_OK;
                std::vector<uint8_t> out;
                out.reserve(records_payload.size() * 2 + kChunk);
                do {
                    size_t prev_size = out.size();
                    out.resize(prev_size + kChunk);
                    strm.next_out = reinterpret_cast<Bytef*>(out.data() + prev_size);
                    strm.avail_out = static_cast<uInt>(kChunk);
                    ret = inflate(&strm, Z_NO_FLUSH);
                    if (ret == Z_STREAM_ERROR) {
                        inflateEnd(&strm);
                        throw std::runtime_error("zlib stream error during inflate");
                    }
                    const size_t produced = kChunk - strm.avail_out;
                    out.resize(prev_size + produced);
                } while (ret != Z_STREAM_END);
                inflateEnd(&strm);
                decompressed = std::move(out);
            } else if (compression == CompressionType::SNAPPY) {
                size_t uncompressed_len = 0;
                if (!snappy::GetUncompressedLength(
                        reinterpret_cast<const char*>(records_payload.data()),
                        records_payload.size(), &uncompressed_len)) {
                    throw std::runtime_error("Invalid Snappy payload");
                }
                std::vector<char> out(uncompressed_len);
                if (!snappy::RawUncompress(reinterpret_cast<const char*>(records_payload.data()),
                                           records_payload.size(), out.data())) {
                    throw std::runtime_error("Snappy decompression failed");
                }
                decompressed.assign(out.begin(), out.end());
            } else if (compression == CompressionType::LZ4) {
                decompressed = decodeKafkaLz4Frame(records_payload);
            } else if (compression == CompressionType::ZSTD) {
                // 0A.2: same hardening as the v2 path — no silent fallback.
                unsigned long long content_size =
                    ZSTD_getFrameContentSize(records_payload.data(), records_payload.size());
                if (content_size == ZSTD_CONTENTSIZE_ERROR) {
                    throw std::runtime_error("Invalid ZSTD frame (CORRUPT_MESSAGE)");
                }
                size_t out_cap = content_size == ZSTD_CONTENTSIZE_UNKNOWN
                                     ? records_payload.size() * 8 + 65536
                                     : static_cast<size_t>(content_size);
                std::vector<uint8_t> out(out_cap);
                size_t dsize = ZSTD_decompress(out.data(), out.size(), records_payload.data(),
                                               records_payload.size());
                if (ZSTD_isError(dsize)) {
                    throw std::runtime_error(std::string("ZSTD decompression failed: ") +
                                             ZSTD_getErrorName(dsize));
                }
                out.resize(dsize);
                decompressed = std::move(out);
            } else {
                throw std::runtime_error("Unsupported compression type");
            }

            // Parse records from the (decompressed) payload
            Buffer rec_buffer(std::move(decompressed));
            while (rec_buffer.remaining() > 0) {
                const int32_t record_length = rec_buffer.readVarInt();
                if (record_length < 0) {
                    throw std::runtime_error("Negative record length");
                }
                if (rec_buffer.remaining() < static_cast<size_t>(record_length)) {
                    throw std::runtime_error("Buffer underflow in record parsing");
                }

                Buffer record_buffer(rec_buffer.readBytes(static_cast<size_t>(record_length)));
                Record record;
                (void)record_buffer.readInt8();  // record attributes
                const int64_t timestamp_delta = record_buffer.readVarLong();
                record.timestamp = batch.first_timestamp_ + timestamp_delta;

                // 0A.3: preserve producer-supplied offset_delta on the
                // legacy MessageSet path too.
                record.offset_delta = record_buffer.readVarInt();

                record.key = readVarBytes(record_buffer);
                record.value = readVarBytes(record_buffer);

                const int32_t header_count = record_buffer.readVarInt();
                if (header_count < 0) {
                    throw std::runtime_error("Invalid header count");
                }
                record.headers.clear();
                record.headers.reserve(static_cast<size_t>(header_count));
                for (int32_t i = 0; i < header_count; ++i) {
                    RecordHeader header;
                    const int32_t key_length = record_buffer.readVarInt();
                    if (key_length < 0) {
                        throw std::runtime_error("Header key cannot be null");
                    }
                    auto key_bytes = record_buffer.readBytes(static_cast<size_t>(key_length));
                    header.key.assign(key_bytes.begin(), key_bytes.end());

                    auto header_value = readVarBytes(record_buffer);
                    if (header_value) {
                        header.value = std::move(*header_value);
                    } else {
                        header.value.clear();
                    }
                    record.headers.push_back(std::move(header));
                }

                batch.records_.push_back(std::move(record));
            }

            batch.attributes_ = static_cast<int16_t>(msg_attributes);

        } else {
            // Legacy message format (magic 0 or 1)
            Timestamp timestamp = 0;
            if (msg_magic >= 1) {
                timestamp = buffer.readInt64();
            }

            if (batch.first_timestamp_ == 0) {
                batch.first_timestamp_ = timestamp;
            }
            if (timestamp > batch.max_timestamp_) {
                batch.max_timestamp_ = timestamp;
            }

            // Read key
            std::optional<std::vector<uint8_t>> key;
            const int32_t key_length = buffer.readInt32();
            if (key_length >= 0) {
                if (buffer.remaining() < static_cast<size_t>(key_length)) {
                    throw std::runtime_error("Buffer underflow reading key");
                }
                key = buffer.readBytes(static_cast<size_t>(key_length));
            }

            // Read value
            std::optional<std::vector<uint8_t>> value;
            const int32_t value_length = buffer.readInt32();
            if (value_length >= 0) {
                if (buffer.remaining() < static_cast<size_t>(value_length)) {
                    throw std::runtime_error("Buffer underflow reading value");
                }
                value = buffer.readBytes(static_cast<size_t>(value_length));
            }

            // Verify we consumed exactly message_size bytes
            const size_t bytes_consumed = buffer.position() - msg_start;
            if (bytes_consumed != static_cast<size_t>(message_size)) {
                Logger::warn("Message size mismatch: expected {}, consumed {} bytes", message_size,
                             bytes_consumed);
            }

            // Create record
            Record record;
            record.timestamp = timestamp;
            record.key = key;
            record.value = value;
            batch.records_.push_back(std::move(record));

            batch.attributes_ = static_cast<int16_t>(msg_attributes);
        }
    }

    if (!batch.records_.empty()) {
        batch.last_offset_delta_ = static_cast<int32_t>(batch.records_.size() - 1);
    } else {
        batch.last_offset_delta_ = 0;
    }

    return batch;
}

size_t RecordBatch::size() const {
    return sizeof(int64_t) + sizeof(int32_t) + static_cast<size_t>(batch_length_);
}

bool RecordBatch::isValid() const {
    return crc_ == computeCrc();
}

uint32_t RecordBatch::computeCrc() const {
    Buffer buffer;
    buffer.writeInt16(attributes_);
    buffer.writeInt32(last_offset_delta_);
    buffer.writeInt64(first_timestamp_);
    buffer.writeInt64(max_timestamp_);
    buffer.writeInt64(producer_id_);
    buffer.writeInt16(producer_epoch_);
    buffer.writeInt32(base_sequence_);
    // 0A.1: record count INT32 was missing here previously; CRC-32C with the
    // missing field would never match the wire. Add it so isValid() agrees
    // with the encoder.
    buffer.writeInt32(static_cast<int32_t>(records_.size()));
    const auto record_payload = encodeRecords();
    if (!record_payload.empty()) {
        buffer.writeBytes(record_payload.data(), record_payload.size());
    }
    return crc32c(buffer.data(), buffer.size());
}

void RecordBatch::updateDerivedFields() {
    if (records_.empty()) {
        last_offset_delta_ = 0;
        max_timestamp_ = first_timestamp_;
        return;
    }

    last_offset_delta_ = static_cast<int32_t>(records_.size() - 1);
    first_timestamp_ = records_.front().timestamp;
    max_timestamp_ = records_.front().timestamp;
    for (const auto& record : records_) {
        if (record.timestamp > max_timestamp_) {
            max_timestamp_ = record.timestamp;
        }
    }
}

namespace {

// Compress a raw records payload per the batch codec. Mirrors the decoders in
// RecordBatch::decode so a re-serialized batch's payload matches the
// compression bits in its attributes (an uncompressed payload under a
// compressed attribute makes strict clients like librdkafka drop the batch).
std::vector<uint8_t> compressPayload(CompressionType codec, const std::vector<uint8_t>& raw) {
    switch (codec) {
        case CompressionType::NONE:
            return raw;
        case CompressionType::GZIP: {
            z_stream strm{};
            if (deflateInit2(&strm, Z_DEFAULT_COMPRESSION, Z_DEFLATED, 15 + 16, 8,
                             Z_DEFAULT_STRATEGY) != Z_OK) {
                throw std::runtime_error("Failed to initialize zlib deflater");
            }
            strm.next_in = const_cast<Bytef*>(raw.data());
            strm.avail_in = static_cast<uInt>(raw.size());
            std::vector<uint8_t> out;
            constexpr size_t kChunk = 64 * 1024;
            int ret = Z_OK;
            do {
                size_t prev = out.size();
                out.resize(prev + kChunk);
                strm.next_out = reinterpret_cast<Bytef*>(out.data() + prev);
                strm.avail_out = static_cast<uInt>(kChunk);
                ret = deflate(&strm, Z_FINISH);
                if (ret == Z_STREAM_ERROR) {
                    deflateEnd(&strm);
                    throw std::runtime_error("zlib stream error during deflate");
                }
                out.resize(prev + (kChunk - strm.avail_out));
            } while (ret != Z_STREAM_END);
            deflateEnd(&strm);
            return out;
        }
        case CompressionType::SNAPPY: {
            std::string out;
            snappy::Compress(reinterpret_cast<const char*>(raw.data()), raw.size(), &out);
            return std::vector<uint8_t>(out.begin(), out.end());
        }
        case CompressionType::LZ4:
            return encodeKafkaLz4Frame(raw);
        case CompressionType::ZSTD: {
            const size_t bound = ZSTD_compressBound(raw.size());
            std::vector<uint8_t> out(bound);
            const size_t written =
                ZSTD_compress(out.data(), out.size(), raw.data(), raw.size(), /*level=*/3);
            if (ZSTD_isError(written)) {
                throw std::runtime_error(std::string("ZSTD compression failed: ") +
                                         ZSTD_getErrorName(written));
            }
            out.resize(written);
            return out;
        }
    }
    throw std::runtime_error("Unsupported compression type on encode");
}

}  // namespace

std::vector<uint8_t> RecordBatch::encodeRecords() const {
    Buffer buffer;
    if (records_.empty()) {
        return buffer.takeVector();
    }

    for (size_t i = 0; i < records_.size(); ++i) {
        const auto& record = records_[i];
        Buffer record_buffer;
        record_buffer.writeInt8(0);  // record attributes placeholder
        const auto timestamp_delta = record.timestamp - first_timestamp_;
        record_buffer.writeVarLong(timestamp_delta);
        // 0A.3: honor a captured offset_delta from the producer wire; only
        // fall back to insertion index for freshly-constructed records.
        const int32_t effective_delta =
            record.offset_delta >= 0 ? record.offset_delta : static_cast<int32_t>(i);
        record_buffer.writeVarInt(effective_delta);
        writeVarBytes(record_buffer, record.key);
        writeVarBytes(record_buffer, record.value);
        record_buffer.writeVarInt(static_cast<int32_t>(record.headers.size()));
        for (const auto& header : record.headers) {
            record_buffer.writeVarInt(static_cast<int32_t>(header.key.size()));
            if (!header.key.empty()) {
                record_buffer.writeBytes(reinterpret_cast<const uint8_t*>(header.key.data()),
                                         header.key.size());
            }
            writeVarBytes(record_buffer, header.value);
        }

        buffer.writeVarInt(static_cast<int32_t>(record_buffer.size()));
        if (record_buffer.size() > 0) {
            buffer.writeBytes(record_buffer.data(), record_buffer.size());
        }
    }

    // Compress the assembled records payload to match the batch's codec bits;
    // NONE returns the bytes unchanged.
    return compressPayload(compressionType(), buffer.takeVector());
}

// Phase EX-10: build a control batch (commit/abort marker for a txn).
RecordBatch RecordBatch::makeControlBatch(int64_t producer_id, int16_t producer_epoch,
                                          Offset base_offset, bool committed,
                                          Timestamp timestamp_ms) {
    RecordBatch batch;
    batch.setBaseOffset(base_offset);
    batch.setMagic(2);
    // Attributes: bit 5 = isControl, bit 4 = isTransactional. We set
    // both because control records are always transactional.
    batch.setAttributes(static_cast<int16_t>((1 << 5) | (1 << 4)));
    batch.setFirstTimestamp(timestamp_ms);
    batch.setMaxTimestamp(timestamp_ms);
    batch.setProducerId(producer_id);
    batch.setProducerEpoch(producer_epoch);
    batch.setBaseSequence(-1);  // Control records don't participate in idempotence.

    // The single control record:
    //   key   = INT16 version=0 + INT16 type (0=ABORT, 1=COMMIT)
    //   value = INT32 coordinator_epoch (we use 0)
    // Per KIP-98, the key encodes the marker type and version, value
    // encodes the coordinator epoch (which we treat as 0 in
    // single-broker mode).
    Record r;
    r.timestamp = timestamp_ms;
    std::vector<uint8_t> key(4);
    key[0] = 0;  // version high byte
    key[1] = 0;  // version low byte
    key[2] = 0;
    key[3] = committed ? 1 : 0;  // type
    r.key = std::move(key);

    std::vector<uint8_t> value(4, 0);  // coordinator_epoch = 0
    r.value = std::move(value);

    batch.addRecord(r);
    return batch;
}

}  // namespace kawasan::storage
