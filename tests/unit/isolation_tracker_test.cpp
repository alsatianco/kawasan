// Phase EX-10: IsolationTracker correctness tests. Validates LSO
// computation, aborted-transactions ring, and the commit/abort
// state-machine transitions.

#include <gtest/gtest.h>

#include "kawasan/broker/isolation_tracker.h"

using kawasan::broker::IsolationTracker;

namespace {

TEST(IsolationTrackerTest, NoInFlightReturnsHwm) {
    IsolationTracker tr;
    EXPECT_EQ(tr.lastStableOffset("t", 0, 100), 100);
}

TEST(IsolationTrackerTest, InFlightHoldsLso) {
    IsolationTracker tr;
    tr.recordInFlightTxn(/*pid=*/1, "t", 0, /*log_end=*/50);
    // LSO should be the txn's first_offset, not HWM.
    EXPECT_EQ(tr.lastStableOffset("t", 0, 100), 50);
}

TEST(IsolationTrackerTest, MinOfMultipleInFlight) {
    IsolationTracker tr;
    tr.recordInFlightTxn(/*pid=*/1, "t", 0, /*log_end=*/50);
    tr.recordInFlightTxn(/*pid=*/2, "t", 0, /*log_end=*/30);
    tr.recordInFlightTxn(/*pid=*/3, "t", 0, /*log_end=*/70);
    EXPECT_EQ(tr.lastStableOffset("t", 0, 100), 30);
}

TEST(IsolationTrackerTest, CommitReleasesHold) {
    IsolationTracker tr;
    tr.recordInFlightTxn(1, "t", 0, 50);
    tr.commitInFlightTxns({{"t", 0}}, 1);
    EXPECT_EQ(tr.lastStableOffset("t", 0, 100), 100);
    EXPECT_EQ(tr.inFlightCount(), 0u);
}

TEST(IsolationTrackerTest, AbortMovesToAbortedRing) {
    IsolationTracker tr;
    tr.recordInFlightTxn(1, "t", 0, 50);
    tr.abortInFlightTxns({{"t", 0}}, 1);
    EXPECT_EQ(tr.inFlightCount(), 0u);
    auto aborts = tr.abortedTransactions("t", 0, /*fetch_offset=*/0);
    ASSERT_EQ(aborts.size(), 1u);
    EXPECT_EQ(aborts[0].producer_id, 1);
    EXPECT_EQ(aborts[0].first_offset, 50);
}

TEST(IsolationTrackerTest, AbortedTxnsFilteredByFetchOffset) {
    IsolationTracker tr;
    tr.recordInFlightTxn(1, "t", 0, 10);
    tr.abortInFlightTxns({{"t", 0}}, 1);
    tr.recordInFlightTxn(2, "t", 0, 50);
    tr.abortInFlightTxns({{"t", 0}}, 2);
    // fetch_offset=20 → only the abort at offset 50 is relevant.
    auto aborts = tr.abortedTransactions("t", 0, 20);
    ASSERT_EQ(aborts.size(), 1u);
    EXPECT_EQ(aborts[0].first_offset, 50);
    // fetch_offset=0 → both aborts visible.
    auto all = tr.abortedTransactions("t", 0, 0);
    EXPECT_EQ(all.size(), 2u);
}

TEST(IsolationTrackerTest, AbortedRingBounded) {
    IsolationTracker tr;
    tr.setMaxAbortedPerPartition(3);
    for (int i = 0; i < 10; ++i) {
        tr.recordInFlightTxn(i, "t", 0, i * 10);
        tr.abortInFlightTxns({{"t", 0}}, i);
    }
    auto aborts = tr.abortedTransactions("t", 0, 0);
    EXPECT_EQ(aborts.size(), 3u);
    // FIFO eviction → oldest 7 dropped, only 7,8,9 remain.
    EXPECT_EQ(aborts[0].producer_id, 7);
    EXPECT_EQ(aborts[1].producer_id, 8);
    EXPECT_EQ(aborts[2].producer_id, 9);
}

TEST(IsolationTrackerTest, IdempotentRecordingForSamePidAndPartition) {
    IsolationTracker tr;
    tr.recordInFlightTxn(1, "t", 0, 10);
    tr.recordInFlightTxn(1, "t", 0, 50);  // Should NOT update first_offset
    EXPECT_EQ(tr.lastStableOffset("t", 0, 100), 10);
}

TEST(IsolationTrackerTest, PerPartitionIsolation) {
    IsolationTracker tr;
    tr.recordInFlightTxn(1, "t", 0, 10);
    tr.recordInFlightTxn(1, "t", 1, 50);
    EXPECT_EQ(tr.lastStableOffset("t", 0, 100), 10);
    EXPECT_EQ(tr.lastStableOffset("t", 1, 100), 50);
    EXPECT_EQ(tr.lastStableOffset("other", 0, 100), 100);  // not tracked
}

}  // namespace
