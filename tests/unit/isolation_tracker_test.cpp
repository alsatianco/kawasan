// Phase EX-10: IsolationTracker correctness tests. Validates LSO
// computation, aborted-transactions ring, and the commit/abort
// state-machine transitions.

#include "kawasan/broker/isolation_tracker.h"

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>

#include "kawasan/common/logger.h"
#include "kawasan/storage/log.h"

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
    tr.abortInFlightTxns({{"t", 0}}, 1, 19);
    tr.recordInFlightTxn(2, "t", 0, 50);
    tr.abortInFlightTxns({{"t", 0}}, 2, 59);
    // fetch_offset=20 → only the abort at offset 50 is relevant.
    auto aborts = tr.abortedTransactions("t", 0, 20);
    ASSERT_EQ(aborts.size(), 1u);
    EXPECT_EQ(aborts[0].first_offset, 50);
    // fetch_offset=0 → both aborts visible.
    auto all = tr.abortedTransactions("t", 0, 0);
    EXPECT_EQ(all.size(), 2u);
}

TEST(IsolationTrackerTest, RetainsHistoricalAbortsBeyondFormerRingLimit) {
    IsolationTracker tr;
    for (int i = 0; i < 1005; ++i) {
        tr.recordInFlightTxn(i, "t", 0, i * 10);
        tr.abortInFlightTxns({{"t", 0}}, i, i * 10 + 9);
    }
    auto aborts = tr.abortedTransactions("t", 0, 0);
    ASSERT_EQ(aborts.size(), 1005u);
    EXPECT_EQ(aborts.front().producer_id, 0);
}

TEST(IsolationTrackerTest, FetchInsideAbortedTransactionIncludesItsEarlierStart) {
    IsolationTracker tr;
    tr.recordInFlightTxn(1, "t", 0, 10);
    tr.abortInFlightTxns({{"t", 0}}, 1, 30);
    auto aborts = tr.abortedTransactions("t", 0, 20);
    ASSERT_EQ(aborts.size(), 1u);
    EXPECT_EQ(aborts[0].first_offset, 10);
    EXPECT_TRUE(tr.abortedTransactions("t", 0, 31).empty());
}

TEST(IsolationTrackerTest, RestoresAbortRangesFromDurableMarkersAcrossProducerReuse) {
    kawasan::Logger::init("warn");
    const auto dir = std::filesystem::temp_directory_path() /
                     ("kawasan-abort-recovery-" +
                      std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    struct Cleanup {
        std::filesystem::path path;
        ~Cleanup() { std::filesystem::remove_all(path); }
    } cleanup{dir};
    auto data = [](int16_t epoch, int count) {
        kawasan::storage::RecordBatch batch;
        batch.setProducerId(7);
        batch.setProducerEpoch(epoch);
        batch.setAttributes(0x10);
        for (int i = 0; i < count; ++i) {
            kawasan::Record record;
            record.value = std::vector<uint8_t>{'x'};
            batch.addRecord(record);
        }
        return batch;
    };
    {
        kawasan::storage::Log log("t", 0, dir.string());
        log.appendBatch(data(0, 2));  // 0,1 aborted; marker at 2
        log.appendBatch(kawasan::storage::RecordBatch::makeControlBatch(7, 0, 2, false, 0));
        log.appendBatch(data(0, 1));  // 3 committed; marker at 4
        log.appendBatch(kawasan::storage::RecordBatch::makeControlBatch(7, 0, 4, true, 0));
        log.appendBatch(data(1, 3));  // 5,6,7 aborted after epoch bump; marker at 8
        log.appendBatch(kawasan::storage::RecordBatch::makeControlBatch(7, 1, 8, false, 0));
        log.appendBatch(data(1, 1));  // 9 has no decision: coordinator restores its LSO hold
        log.close();
    }
    kawasan::storage::Log reopened("t", 0, dir.string());
    IsolationTracker restored;
    restored.recoverAbortedTransactions("t", 0, reopened);
    const auto all = restored.abortedTransactions("t", 0, 0);
    ASSERT_EQ(all.size(), 2u);
    EXPECT_EQ(all[0].first_offset, 0);
    EXPECT_EQ(all[0].last_offset, 2);
    EXPECT_EQ(all[1].first_offset, 5);
    EXPECT_EQ(all[1].last_offset, 8);
    const auto inside = restored.abortedTransactions("t", 0, 6);
    ASSERT_EQ(inside.size(), 1u);
    EXPECT_EQ(inside[0].first_offset, 5);
    EXPECT_TRUE(restored.abortedTransactions("t", 0, 9).empty());
    EXPECT_EQ(restored.inFlightCount(), 0u);
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
