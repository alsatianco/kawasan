#include <arpa/inet.h>
#include <cstring>

#include <gtest/gtest.h>
#include <lz4frame.h>
#include <zlib.h>

#include "kawasan/common/buffer.h"
#include "kawasan/storage/record_batch.h"

namespace kawasan::storage {

namespace {

constexpr size_t kRecordBatchHeaderBytes = 49;
constexpr size_t kRecordsOffset = sizeof(int64_t) + sizeof(int32_t) + kRecordBatchHeaderBytes;

void writeInt32BE(std::vector<uint8_t>& buffer, size_t offset, uint32_t value) {
    const uint32_t net_value = htonl(value);
    std::memcpy(buffer.data() + offset, &net_value, sizeof(net_value));
}

void writeInt16BE(std::vector<uint8_t>& buffer, size_t offset, int16_t value) {
    const uint16_t net_value = htons(static_cast<uint16_t>(value));
    std::memcpy(buffer.data() + offset, &net_value, sizeof(net_value));
}

// 0A.1: Kafka v2 RecordBatch uses CRC-32C (Castagnoli), not IEEE CRC-32.
// Local copy of the same algorithm the production code uses, so this test
// helper computes the bytes the new decoder expects.
uint32_t crc32cTest(const uint8_t* data, size_t n) {
    static uint32_t table[256];
    static bool initialized = false;
    if (!initialized) {
        constexpr uint32_t kPoly = 0x82F63B78u;
        for (uint32_t i = 0; i < 256; ++i) {
            uint32_t c = i;
            for (int j = 0; j < 8; ++j) {
                c = (c & 1) ? (kPoly ^ (c >> 1)) : (c >> 1);
            }
            table[i] = c;
        }
        initialized = true;
    }
    uint32_t c = 0xFFFFFFFFu;
    for (size_t i = 0; i < n; ++i) {
        c = table[(c ^ data[i]) & 0xff] ^ (c >> 8);
    }
    return c ^ 0xFFFFFFFFu;
}

uint32_t computeCompressedCrc(const RecordBatch& batch, int16_t attributes,
                              const std::vector<uint8_t>& payload) {
    Buffer crc_buffer;
    crc_buffer.writeInt16(attributes);
    crc_buffer.writeInt32(batch.lastOffsetDelta());
    crc_buffer.writeInt64(batch.firstTimestamp());
    crc_buffer.writeInt64(batch.maxTimestamp());
    crc_buffer.writeInt64(batch.producerId());
    crc_buffer.writeInt16(batch.producerEpoch());
    crc_buffer.writeInt32(batch.baseSequence());
    crc_buffer.writeInt32(static_cast<int32_t>(batch.records().size()));
    if (!payload.empty()) {
        crc_buffer.writeBytes(payload.data(), payload.size());
    }
    return crc32cTest(crc_buffer.data(), crc_buffer.size());
}

}  // namespace

TEST(RecordBatchTest, SerializeAndDeserialize) {
    Record record;
    record.timestamp = 123456;
    record.key = std::vector<uint8_t>{'k', '1'};
    record.value = std::vector<uint8_t>{'v', '1'};
    record.headers.push_back({"header", {'a', 'b', 'c'}});

    RecordBatch batch;
    batch.setBaseOffset(10);
    batch.addRecord(record);

    auto bytes = batch.serialize();
    auto restored = RecordBatch::deserialize(bytes);

    ASSERT_EQ(restored.records().size(), 1);
    const auto& restored_record = restored.records().front();
    ASSERT_TRUE(restored_record.key.has_value());
    ASSERT_TRUE(restored_record.value.has_value());

    EXPECT_EQ(restored.baseOffset(), 10);
    EXPECT_EQ(restored_record.key->at(0), 'k');
    EXPECT_EQ(restored_record.value->at(0), 'v');
    ASSERT_EQ(restored_record.headers.size(), 1);
    EXPECT_EQ(restored_record.headers[0].key, "header");
    EXPECT_EQ(restored_record.headers[0].value.size(), 3);
}

TEST(RecordBatchTest, PreservesProducerSuppliedOffsetDelta) {
    // 0A.3: when a batch is decoded and re-encoded, the explicit per-record
    // offset_delta must round-trip. Previously the decode discarded the value
    // (`(void)offset_delta;`) and the re-encode wrote the insertion index.
    //
    // We construct three records whose deltas are NOT sequential (0, 2, 5) —
    // this simulates a producer batching after retries or skipping sequence
    // numbers. After serialize → deserialize → re-serialize → deserialize,
    // the deltas must come back unchanged.
    RecordBatch original;
    original.setBaseOffset(100);
    original.setProducerId(42);
    original.setProducerEpoch(7);
    original.setBaseSequence(10);
    original.setFirstTimestamp(1000);

    Record r0;
    r0.timestamp = 1000;
    r0.value = std::vector<uint8_t>{'a'};
    r0.offset_delta = 0;
    original.addRecord(r0);

    Record r2;
    r2.timestamp = 1001;
    r2.value = std::vector<uint8_t>{'b'};
    r2.offset_delta = 2;  // non-sequential — gap of 1
    original.addRecord(r2);

    Record r5;
    r5.timestamp = 1002;
    r5.value = std::vector<uint8_t>{'c'};
    r5.offset_delta = 5;  // another gap
    original.addRecord(r5);

    // First round-trip
    auto bytes1 = original.serialize();
    auto round1 = RecordBatch::deserialize(bytes1);
    ASSERT_EQ(round1.records().size(), 3);
    EXPECT_EQ(round1.records()[0].offset_delta, 0);
    EXPECT_EQ(round1.records()[1].offset_delta, 2);
    EXPECT_EQ(round1.records()[2].offset_delta, 5);

    // Second round-trip — verifies re-encode preserved the deltas
    auto bytes2 = round1.serialize();
    auto round2 = RecordBatch::deserialize(bytes2);
    ASSERT_EQ(round2.records().size(), 3);
    EXPECT_EQ(round2.records()[0].offset_delta, 0);
    EXPECT_EQ(round2.records()[1].offset_delta, 2);
    EXPECT_EQ(round2.records()[2].offset_delta, 5);
}

TEST(RecordBatchTest, DetectsCrcCorruption) {
    // 0A.1: a bit flip in the records payload must be detected by CRC
    // validation on decode. Previously decode() read crc_ from the wire but
    // never compared it against the computed value, so corruption flowed
    // silently into the log.
    Record record;
    record.timestamp = 100;
    record.value = std::vector<uint8_t>{'h', 'e', 'l', 'l', 'o'};

    RecordBatch batch;
    batch.setBaseOffset(0);
    batch.addRecord(record);

    auto bytes = batch.serialize();
    ASSERT_GT(bytes.size(), 30u);

    // Flip the second-to-last byte (which lives inside the value payload, not
    // in the CRC field itself).
    bytes[bytes.size() - 2] ^= 0xFF;

    EXPECT_THROW({ RecordBatch::deserialize(bytes); }, std::exception);
}

TEST(RecordBatchTest, FreshRecordsDefaultToInsertionIndex) {
    // 0A.3: records created without an explicit offset_delta (the default
    // sentinel -1) should fall back to insertion index on encode. This is the
    // path used by Log::append when assembling a fresh batch from app records.
    RecordBatch batch;
    batch.setBaseOffset(0);
    batch.setFirstTimestamp(100);

    Record r0;
    r0.timestamp = 100;
    r0.value = std::vector<uint8_t>{'x'};
    batch.addRecord(r0);

    Record r1;
    r1.timestamp = 100;
    r1.value = std::vector<uint8_t>{'y'};
    batch.addRecord(r1);

    auto bytes = batch.serialize();
    auto restored = RecordBatch::deserialize(bytes);
    ASSERT_EQ(restored.records().size(), 2);
    EXPECT_EQ(restored.records()[0].offset_delta, 0);
    EXPECT_EQ(restored.records()[1].offset_delta, 1);
}

TEST(RecordBatchTest, SizeMatchesEncodedBytes) {
    Record record;
    record.timestamp = 42;
    record.value = std::vector<uint8_t>{'x', 'y', 'z'};

    RecordBatch batch;
    batch.addRecord(record);

    const size_t encoded_size = batch.serialize().size();
    EXPECT_EQ(batch.size(), encoded_size);
}

TEST(RecordBatchTest, DecodesLz4CompressedPayloads) {
    Record record;
    record.timestamp = 7;
    record.value = std::vector<uint8_t>{'L', 'Z', '4'};

    RecordBatch batch;
    batch.setBaseOffset(99);
    batch.addRecord(record);

    auto uncompressed_bytes = batch.serialize();
    ASSERT_GT(uncompressed_bytes.size(), kRecordsOffset);
    std::vector<uint8_t> records(uncompressed_bytes.begin() + kRecordsOffset,
                                 uncompressed_bytes.end());

    LZ4F_preferences_t prefs{};
    prefs.compressionLevel = 0;
    const size_t bound = LZ4F_compressFrameBound(records.size(), &prefs);
    std::vector<uint8_t> compressed(bound);
    const size_t written = LZ4F_compressFrame(compressed.data(), compressed.size(),
                                              records.data(), records.size(), &prefs);
    ASSERT_EQ(LZ4F_isError(written), 0U) << LZ4F_getErrorName(written);
    ASSERT_GT(written, 0U);
    compressed.resize(written);

    std::vector<uint8_t> compressed_batch(uncompressed_bytes.begin(),
                                          uncompressed_bytes.begin() + kRecordsOffset);
    compressed_batch.insert(compressed_batch.end(), compressed.begin(), compressed.end());

    const int32_t new_batch_length =
        static_cast<int32_t>(kRecordBatchHeaderBytes + compressed.size());
    writeInt32BE(compressed_batch, sizeof(int64_t), static_cast<uint32_t>(new_batch_length));

    const int16_t attributes =
        RecordBatchAttributes::encode(CompressionType::LZ4, false, false);
    writeInt16BE(compressed_batch, sizeof(int64_t) + sizeof(int32_t) + sizeof(int32_t) +
                                        sizeof(int8_t) + sizeof(int32_t),
                 attributes);

    const uint32_t crc = computeCompressedCrc(batch, attributes, compressed);
    writeInt32BE(compressed_batch, sizeof(int64_t) + sizeof(int32_t) + sizeof(int32_t) +
                                       sizeof(int8_t),
                 crc);

    auto restored = RecordBatch::deserialize(compressed_batch);
    ASSERT_EQ(restored.records().size(), 1);
    ASSERT_TRUE(restored.records()[0].value.has_value());
    EXPECT_EQ(restored.records()[0].value->at(0), 'L');
    EXPECT_EQ(restored.baseOffset(), 99);
}

}  // namespace kawasan::storage

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
