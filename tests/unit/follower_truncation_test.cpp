// M8-E3/F4: where a follower truncates before replicating from a (new) leader.
// KIP-101: ask the leader where the follower's latest epoch ends; without epoch
// history on either side fall back to the follower's high watermark (M7 rule).
#include "kawasan/broker/follower_truncation.h"

#include <gtest/gtest.h>

using kawasan::broker::EpochAnswer;
using kawasan::broker::followerTruncationOffset;

// Old leader wrote 10..14 in epoch 1 (never replicated); the new leader's
// epoch 2 started at 10. The returning follower must cut back to 10.
TEST(FollowerTruncationTest, SameEpochTruncatesToLeadersEndOfIt) {
    EXPECT_EQ(followerTruncationOffset(1, EpochAnswer{1, 10}, EpochAnswer{1, 15}, 15, 8), 10);
}

TEST(FollowerTruncationTest, NothingToCutWhenLeaderHasMore) {
    EXPECT_EQ(followerTruncationOffset(2, EpochAnswer{2, 40}, EpochAnswer{2, 30}, 30, 30), 30);
}

// The leader never saw our latest epoch 3: it answers with its largest epoch
// below it (2, ending at 20). We cut to the earlier of where epoch 2 ends on
// either side.
TEST(FollowerTruncationTest, OlderEpochUsesTheEarlierEndOfBothSides) {
    EXPECT_EQ(followerTruncationOffset(3, EpochAnswer{2, 20}, EpochAnswer{2, 18}, 25, 10), 18);
    EXPECT_EQ(followerTruncationOffset(3, EpochAnswer{2, 20}, EpochAnswer{2, 22}, 25, 10), 20);
    // We have no record of the leader's epoch: trust the leader's end.
    EXPECT_EQ(followerTruncationOffset(3, EpochAnswer{2, 20}, EpochAnswer{-1, -1}, 25, 10), 20);
}

TEST(FollowerTruncationTest, FallsBackToHighWatermarkWithoutHistory) {
    // Follower has no epoch history (pre-M8 log).
    EXPECT_EQ(followerTruncationOffset(std::nullopt, std::nullopt, EpochAnswer{-1, -1}, 15, 9), 9);
    // Leader has none / does not know our epoch (UNDEFINED).
    EXPECT_EQ(followerTruncationOffset(4, EpochAnswer{-1, -1}, EpochAnswer{-1, -1}, 15, 9), 9);
    // Never "truncates" upward.
    EXPECT_EQ(followerTruncationOffset(std::nullopt, std::nullopt, EpochAnswer{-1, -1}, 5, 9), 5);
}
