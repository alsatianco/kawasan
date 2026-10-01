// OffsetForLeaderEpoch (API 23) codec round-trips across v0-v4 — the follower
// (PeerClient, M8-F4) encodes requests and decodes responses that the broker
// decodes/encodes.
#include <gtest/gtest.h>

#include "kawasan/common/buffer.h"
#include "kawasan/protocol/offset_for_leader_epoch_request.h"

using kawasan::Buffer;
using kawasan::ErrorCode;
using kawasan::protocol::OffsetForLeaderEpochRequest;
using kawasan::protocol::OffsetForLeaderEpochResponse;

class OffsetForLeaderEpochCodecTest : public ::testing::TestWithParam<int16_t> {};

TEST_P(OffsetForLeaderEpochCodecTest, RequestRoundTrip) {
    const int16_t v = GetParam();
    OffsetForLeaderEpochRequest req;
    req.setReplicaId(7);
    req.addTopic({"t", {{0, 4, 3}, {2, 4, 1}}});
    Buffer buf;
    req.encode(buf, v);
    OffsetForLeaderEpochRequest out;
    out.decode(buf, v);
    ASSERT_EQ(out.topics().size(), 1u);
    EXPECT_EQ(out.topics()[0].name, "t");
    ASSERT_EQ(out.topics()[0].partitions.size(), 2u);
    EXPECT_EQ(out.topics()[0].partitions[1].partition, 2);
    EXPECT_EQ(out.topics()[0].partitions[1].leader_epoch, 1);
    EXPECT_EQ(out.topics()[0].partitions[1].current_leader_epoch, v >= 2 ? 4 : -1);
    EXPECT_EQ(out.replicaId(), v >= 3 ? 7 : -1);
    EXPECT_EQ(buf.remaining(), 0u);
}

TEST_P(OffsetForLeaderEpochCodecTest, ResponseRoundTrip) {
    const int16_t v = GetParam();
    OffsetForLeaderEpochResponse resp;
    OffsetForLeaderEpochResponse::TopicResult t;
    t.name = "t";
    t.partitions.push_back({ErrorCode::NONE, 0, 3, 42});
    t.partitions.push_back({ErrorCode::FENCED_LEADER_EPOCH, 1, -1, -1});
    resp.addTopic(t);
    Buffer buf;
    resp.encode(buf, v);
    OffsetForLeaderEpochResponse out;
    out.decode(buf, v);
    ASSERT_EQ(out.topics().size(), 1u);
    const auto& p = out.topics()[0].partitions;
    ASSERT_EQ(p.size(), 2u);
    EXPECT_EQ(p[0].end_offset, 42);
    EXPECT_EQ(p[0].leader_epoch, v >= 1 ? 3 : -1);
    EXPECT_EQ(p[1].error_code, ErrorCode::FENCED_LEADER_EPOCH);
    EXPECT_EQ(buf.remaining(), 0u);
}

INSTANTIATE_TEST_SUITE_P(AllVersions, OffsetForLeaderEpochCodecTest,
                         ::testing::Values(0, 1, 2, 3, 4));
