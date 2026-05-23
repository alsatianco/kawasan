#include <gtest/gtest.h>

#include "kawasan/raft/raft_protocol.h"

using kawasan::raft::InstallSnapshotRequest;
using kawasan::raft::InstallSnapshotResponse;

namespace {

TEST(InstallSnapshotTest, RequestRoundTrip) {
    InstallSnapshotRequest req;
    req.term = 7;
    req.leader_id = 2;
    req.last_included_index = 100;
    req.last_included_term = 5;
    req.offset = 0;
    req.data = {0x01, 0x02, 0x03, 0x04};
    req.done = true;

    auto encoded = req.encode();
    auto decoded = InstallSnapshotRequest::decode(encoded);

    EXPECT_EQ(decoded.term, 7);
    EXPECT_EQ(decoded.leader_id, 2);
    EXPECT_EQ(decoded.last_included_index, 100);
    EXPECT_EQ(decoded.last_included_term, 5);
    EXPECT_EQ(decoded.offset, 0);
    EXPECT_EQ(decoded.data, std::vector<uint8_t>({0x01, 0x02, 0x03, 0x04}));
    EXPECT_TRUE(decoded.done);
}

TEST(InstallSnapshotTest, ResponseRoundTrip) {
    InstallSnapshotResponse resp;
    resp.term = 42;
    auto encoded = resp.encode();
    auto decoded = InstallSnapshotResponse::decode(encoded);
    EXPECT_EQ(decoded.term, 42);
}

TEST(InstallSnapshotTest, ChunkedSnapshot) {
    // Phase 5.2: verify a multi-chunk snapshot round-trips correctly.
    // The chunking strategy isn't implemented at the leader-send side
    // yet, but the wire format supports it via the `offset` field.
    InstallSnapshotRequest req;
    req.term = 1;
    req.leader_id = 0;
    req.last_included_index = 1000;
    req.last_included_term = 1;
    req.offset = 4096;  // Middle chunk
    req.data = std::vector<uint8_t>(4096, 0xAB);
    req.done = false;

    auto encoded = req.encode();
    auto decoded = InstallSnapshotRequest::decode(encoded);

    EXPECT_EQ(decoded.offset, 4096);
    EXPECT_EQ(decoded.data.size(), 4096u);
    EXPECT_EQ(decoded.data[0], 0xAB);
    EXPECT_FALSE(decoded.done);
}

}  // namespace
