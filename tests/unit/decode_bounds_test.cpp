// P13: decoders must reject an array length larger than the bytes that could
// possibly hold that many elements, instead of reserve()/resize()-ing to an
// attacker-controlled count (a ~10-byte packet otherwise drives a multi-GB
// allocation — a trivial OOM DoS the fuzzers found).

#include <gtest/gtest.h>

#include <vector>

#include "kawasan/common/buffer.h"
#include "kawasan/protocol/fetch_request.h"
#include "kawasan/protocol/metadata_request.h"
#include "kawasan/protocol/produce_request.h"

using kawasan::Buffer;

namespace {

TEST(DecodeBounds, ReadArrayLengthRejectsOversizedClassicCount) {
    Buffer buf;
    buf.writeInt32(0x7FFFFFFF);  // 2^31-1 elements claimed, 0 bytes follow
    EXPECT_THROW(buf.readArrayLength(/*flexible=*/false), std::runtime_error);
}

TEST(DecodeBounds, ReadArrayLengthRejectsOversizedCompactCount) {
    Buffer buf;
    buf.writeCompactArrayLen(0x7FFFFFF0);
    EXPECT_THROW(buf.readArrayLength(/*flexible=*/true), std::runtime_error);
}

TEST(DecodeBounds, ReadArrayLengthAcceptsPlausibleCount) {
    Buffer buf;
    buf.writeInt32(3);
    buf.writeInt32(0);
    buf.writeInt32(0);
    buf.writeInt32(0);
    EXPECT_EQ(buf.readArrayLength(/*flexible=*/false), 3);
}

// Metadata v0: topic array count claims 2^31 topics in a 5-byte packet.
TEST(DecodeBounds, MetadataRequestRejectsHugeTopicCount) {
    Buffer buf;
    buf.writeInt32(0x7FFFFFFF);  // topic_count
    kawasan::protocol::MetadataRequest req;
    EXPECT_THROW(req.decode(buf, /*api_version=*/0), std::runtime_error);
}

// Produce v0: topic array count claims a huge number with no bytes to back it.
TEST(DecodeBounds, ProduceRequestRejectsHugeTopicCount) {
    Buffer buf;
    buf.writeInt16(-1);          // transactional_id = null
    buf.writeInt16(1);           // acks
    buf.writeInt32(30000);       // timeout_ms
    buf.writeInt32(0x7FFFFFFF);  // topic_count
    kawasan::protocol::ProduceRequest req;
    EXPECT_THROW(req.decode(buf, /*api_version=*/3), std::runtime_error);
}

// Fetch v0: topic array count claims a huge number.
TEST(DecodeBounds, FetchRequestRejectsHugeTopicCount) {
    Buffer buf;
    buf.writeInt32(-1);          // replica_id
    buf.writeInt32(500);         // max_wait_ms
    buf.writeInt32(1);           // min_bytes
    buf.writeInt32(0x7FFFFFFF);  // topic_count
    kawasan::protocol::FetchRequest req;
    EXPECT_THROW(req.decode(buf, /*api_version=*/0), std::runtime_error);
}

}  // namespace
