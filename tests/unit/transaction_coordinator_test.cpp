#include "kawasan/broker/transaction_coordinator.h"

#include <gtest/gtest.h>

using kawasan::broker::TransactionCoordinator;
using State = TransactionCoordinator::State;

namespace {

TEST(TransactionCoordinatorTest, InitProducerIdRegistersTxn) {
    TransactionCoordinator tc;
    tc.recordInitProducerId("txn-1", /*pid=*/100, /*epoch=*/0,
                            /*timeout=*/60000);
    auto snap = tc.describe("txn-1");
    ASSERT_TRUE(snap.has_value());
    EXPECT_EQ(snap->producer_id, 100);
    EXPECT_EQ(snap->state, State::Empty);
    EXPECT_TRUE(snap->partitions.empty());
}

TEST(TransactionCoordinatorTest, AddPartitionsTransitionsToOngoing) {
    TransactionCoordinator tc;
    tc.recordInitProducerId("txn-1", 100, 0, 60000);
    tc.addPartitions("txn-1", {{"topic-a", 0}, {"topic-a", 1}});
    auto snap = tc.describe("txn-1");
    ASSERT_TRUE(snap.has_value());
    EXPECT_EQ(snap->state, State::Ongoing);
    EXPECT_EQ(snap->partitions.size(), 2u);
}

TEST(TransactionCoordinatorTest, AddPartitionsDedupes) {
    TransactionCoordinator tc;
    tc.recordInitProducerId("txn-1", 100, 0, 60000);
    tc.addPartitions("txn-1", {{"topic-a", 0}});
    tc.addPartitions("txn-1", {{"topic-a", 0}, {"topic-b", 0}});
    auto snap = tc.describe("txn-1");
    EXPECT_EQ(snap->partitions.size(), 2u);
}

TEST(TransactionCoordinatorTest, CommitTxnTransitionsToCompleteCommit) {
    TransactionCoordinator tc;
    tc.recordInitProducerId("txn-1", 100, 0, 60000);
    tc.addPartitions("txn-1", {{"topic-a", 0}, {"topic-a", 1}});
    auto participating = tc.prepareCommit("txn-1");
    tc.completeCommit("txn-1");
    EXPECT_EQ(participating.size(), 2u);
    auto snap = tc.describe("txn-1");
    EXPECT_EQ(snap->state, State::CompleteCommit);
    // Partition list is cleared on commit.
    EXPECT_TRUE(snap->partitions.empty());
}

TEST(TransactionCoordinatorTest, AbortTxnTransitionsToCompleteAbort) {
    TransactionCoordinator tc;
    tc.recordInitProducerId("txn-1", 100, 0, 60000);
    tc.addPartitions("txn-1", {{"topic-a", 0}});
    auto participating = tc.prepareAbort("txn-1");
    tc.completeAbort("txn-1");
    EXPECT_EQ(participating.size(), 1u);
    auto snap = tc.describe("txn-1");
    EXPECT_EQ(snap->state, State::CompleteAbort);
}

TEST(TransactionCoordinatorTest, NewTxnAfterCommit) {
    // Phase 3.3: after CompleteCommit, AddPartitions starts a fresh txn.
    TransactionCoordinator tc;
    tc.recordInitProducerId("txn-1", 100, 0, 60000);
    tc.addPartitions("txn-1", {{"topic-a", 0}});
    tc.prepareCommit("txn-1");
    tc.completeCommit("txn-1");

    tc.addPartitions("txn-1", {{"topic-b", 0}, {"topic-b", 1}});
    auto snap = tc.describe("txn-1");
    EXPECT_EQ(snap->state, State::Ongoing);
    EXPECT_EQ(snap->partitions.size(), 2u);
}

TEST(TransactionCoordinatorTest, StateFilterInList) {
    TransactionCoordinator tc;
    tc.recordInitProducerId("txn-empty", 1, 0, 60000);

    tc.recordInitProducerId("txn-ongoing", 2, 0, 60000);
    tc.addPartitions("txn-ongoing", {{"t", 0}});

    tc.recordInitProducerId("txn-commit", 3, 0, 60000);
    tc.addPartitions("txn-commit", {{"t", 1}});
    tc.prepareCommit("txn-commit");
    tc.completeCommit("txn-commit");

    auto ongoing = tc.list({"Ongoing"}, {});
    EXPECT_EQ(ongoing.size(), 1u);
    EXPECT_EQ(ongoing[0].transactional_id, "txn-ongoing");

    auto commits = tc.list({"CompleteCommit"}, {});
    EXPECT_EQ(commits.size(), 1u);
    EXPECT_EQ(commits[0].transactional_id, "txn-commit");
}

// Phase EX-6: staged offsets — KIP-447 transactional consumer-group
// offset commits. The offsets are buffered in the txn snapshot and
// applied only on EndTxn(commit=true), discarded on abort.
TEST(TransactionCoordinatorTest, StagedOffsetsCommitOnCommit) {
    TransactionCoordinator tc;
    tc.recordInitProducerId("txn-1", 1, 0, 60000);
    tc.addPartitions("txn-1", {{"input", 0}});

    std::vector<TransactionCoordinator::PendingOffset> offs;
    offs.push_back({"grp-1", "input", 0, 42, ""});
    offs.push_back({"grp-1", "input", 1, 43, ""});
    tc.stagePendingOffsets("txn-1", std::move(offs));

    auto drained = tc.drainPendingOffsets("txn-1");
    ASSERT_EQ(drained.size(), 2u);
    EXPECT_EQ(drained[0].group_id, "grp-1");
    EXPECT_EQ(drained[0].offset, 42);
    EXPECT_EQ(drained[1].offset, 43);

    // Idempotent: second drain returns empty.
    auto empty = tc.drainPendingOffsets("txn-1");
    EXPECT_TRUE(empty.empty());
}

TEST(TransactionCoordinatorTest, StagedOffsetsClearedOnAbort) {
    TransactionCoordinator tc;
    tc.recordInitProducerId("txn-1", 1, 0, 60000);
    tc.addPartitions("txn-1", {{"input", 0}});

    std::vector<TransactionCoordinator::PendingOffset> offs;
    offs.push_back({"grp-1", "input", 0, 99, "meta"});
    tc.stagePendingOffsets("txn-1", std::move(offs));

    tc.prepareAbort("txn-1");
    tc.completeAbort("txn-1");

    // Abort must have discarded the staged offsets.
    auto drained = tc.drainPendingOffsets("txn-1");
    EXPECT_TRUE(drained.empty()) << "abort should discard staged offsets";
}

TEST(TransactionCoordinatorTest, StagedOffsetsAccumulateAcrossCommitCalls) {
    TransactionCoordinator tc;
    tc.recordInitProducerId("txn-1", 1, 0, 60000);
    tc.addPartitions("txn-1", {{"input", 0}});

    std::vector<TransactionCoordinator::PendingOffset> batch1;
    batch1.push_back({"grp-1", "topic-a", 0, 10, ""});
    tc.stagePendingOffsets("txn-1", std::move(batch1));

    std::vector<TransactionCoordinator::PendingOffset> batch2;
    batch2.push_back({"grp-1", "topic-b", 1, 20, ""});
    batch2.push_back({"grp-2", "topic-c", 0, 30, ""});
    tc.stagePendingOffsets("txn-1", std::move(batch2));

    auto drained = tc.drainPendingOffsets("txn-1");
    EXPECT_EQ(drained.size(), 3u);
}

TEST(TransactionCoordinatorTest, StagePendingOffsetsForUnknownTxnIsNoop) {
    TransactionCoordinator tc;
    std::vector<TransactionCoordinator::PendingOffset> offs;
    offs.push_back({"grp-1", "t", 0, 1, ""});
    // Should not throw or crash.
    tc.stagePendingOffsets("does-not-exist", std::move(offs));
    auto drained = tc.drainPendingOffsets("does-not-exist");
    EXPECT_TRUE(drained.empty());
}

}  // namespace
