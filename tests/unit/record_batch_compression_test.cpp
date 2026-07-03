// Compressed-batch round-trip. Kawasan decodes a produced batch into records
// (decompressing) and re-serializes it for storage; the re-serialized batch
// must carry a payload that actually matches its compression attribute.
// Regression for the interop bug where encodeRecords() wrote PLAINTEXT while
// the header still advertised snappy/lz4/zstd/gzip — librdkafka then read the
// header, tried to decompress plaintext, and dropped every record (consumed 0
// from a compressed topic; uncompressed worked).

#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "kawasan/storage/record_batch.h"

namespace kawasan::storage {
namespace {

std::vector<uint8_t> bytesOf(const std::string& s) {
    return std::vector<uint8_t>(s.begin(), s.end());
}

RecordBatch makeBatch(CompressionType codec, int n) {
    RecordBatch batch;
    batch.setBaseOffset(0);
    batch.setCompressionType(codec);
    for (int i = 0; i < n; ++i) {
        Record r;
        r.timestamp = 1000 + i;
        r.key = bytesOf("key-" + std::to_string(i));
        // Compressible, distinct values so a mislabeled plaintext payload is
        // provably wrong when a codec tries to decode it.
        r.value = bytesOf(std::string(200, 'a' + (i % 20)) + std::to_string(i));
        batch.addRecord(r);
    }
    return batch;
}

void expectRoundTrip(CompressionType codec) {
    RecordBatch original = makeBatch(codec, 16);

    Buffer buf;
    original.encode(buf);
    std::vector<uint8_t> wire(buf.data(), buf.data() + buf.size());

    // Deserialize must succeed and recover every record byte-for-byte. This
    // fails if the payload was written uncompressed while the attribute says
    // compressed (the decoder throws trying to decompress plaintext).
    RecordBatch decoded = RecordBatch::deserialize(wire);
    ASSERT_EQ(decoded.compressionType(), codec);
    ASSERT_EQ(decoded.records().size(), original.records().size());
    for (size_t i = 0; i < original.records().size(); ++i) {
        EXPECT_EQ(decoded.records()[i].key, original.records()[i].key) << "codec=" << (int)codec;
        EXPECT_EQ(decoded.records()[i].value, original.records()[i].value)
            << "codec=" << (int)codec << " record=" << i;
    }
}

TEST(RecordBatchCompression, NoneRoundTrips) {
    expectRoundTrip(CompressionType::NONE);
}
TEST(RecordBatchCompression, GzipRoundTrips) {
    expectRoundTrip(CompressionType::GZIP);
}
TEST(RecordBatchCompression, SnappyRoundTrips) {
    expectRoundTrip(CompressionType::SNAPPY);
}
TEST(RecordBatchCompression, Lz4RoundTrips) {
    expectRoundTrip(CompressionType::LZ4);
}
TEST(RecordBatchCompression, ZstdRoundTrips) {
    expectRoundTrip(CompressionType::ZSTD);
}

// The compressed payload must genuinely be smaller than the raw records for
// highly compressible input — proves compression actually happened rather
// than the codec bits being set on a plaintext payload.
TEST(RecordBatchCompression, SnappyActuallyShrinksPayload) {
    RecordBatch compressed = makeBatch(CompressionType::SNAPPY, 64);
    RecordBatch plain = makeBatch(CompressionType::NONE, 64);
    Buffer cbuf, pbuf;
    compressed.encode(cbuf);
    plain.encode(pbuf);
    EXPECT_LT(cbuf.size(), pbuf.size())
        << "snappy batch should be smaller than the equivalent uncompressed batch";
}

}  // namespace
}  // namespace kawasan::storage
