// M1: TransactionStateManager serialization round-trip + routing, and the
// TransactionCoordinator two-phase / restore state machine. These are the
// pure, I/O-free cores of durable transactions; the crash-recovery
// integration test drives the log path.

#include "kawasan/broker/transaction_state_manager.h"

#include <gtest/gtest.h>

#include <string>

#include "kawasan/broker/transaction_coordinator.h"

namespace kawasan::broker {
namespace {

using TSM = TransactionStateManager;
using Snapshot = TransactionCoordinator::TxnSnapshot;
using State = TransactionCoordinator::State;

Snapshot makeSnapshot() {
    Snapshot s;
    s.transactional_id = "txn-app-1";
    s.producer_id = 4242;
    s.producer_epoch = 7;
    s.transaction_timeout_ms = 60000;
    s.state = State::PrepareCommit;
    s.state_start_time_ms = 1234567890123LL;
    s.partitions = {{"orders", 3, 100}, {"orders", 5, 250}, {"__consumer_offsets", 12, -1}};
    s.pending_offsets = {
        {"grp-A", "input", 0, 500, "meta"},
        {"grp-A", "input", 1, 900, ""},
    };
    return s;
}

TEST(TxnStateSerialization, RoundTripsEveryField) {
    const Snapshot original = makeSnapshot();
    const auto bytes = TSM::serialize(original);
    const Snapshot decoded = TSM::deserialize(bytes);

    EXPECT_EQ(decoded.transactional_id, original.transactional_id);
    EXPECT_EQ(decoded.producer_id, original.producer_id);
    EXPECT_EQ(decoded.producer_epoch, original.producer_epoch);
    EXPECT_EQ(decoded.transaction_timeout_ms, original.transaction_timeout_ms);
    EXPECT_EQ(decoded.state, original.state);
    EXPECT_EQ(decoded.state_start_time_ms, original.state_start_time_ms);

    ASSERT_EQ(decoded.partitions.size(), original.partitions.size());
    for (size_t i = 0; i < original.partitions.size(); ++i) {
        EXPECT_EQ(decoded.partitions[i].topic, original.partitions[i].topic);
        EXPECT_EQ(decoded.partitions[i].partition, original.partitions[i].partition);
        EXPECT_EQ(decoded.partitions[i].first_offset, original.partitions[i].first_offset);
    }
    ASSERT_EQ(decoded.pending_offsets.size(), original.pending_offsets.size());
    for (size_t i = 0; i < original.pending_offsets.size(); ++i) {
        EXPECT_EQ(decoded.pending_offsets[i].group_id, original.pending_offsets[i].group_id);
        EXPECT_EQ(decoded.pending_offsets[i].topic, original.pending_offsets[i].topic);
        EXPECT_EQ(decoded.pending_offsets[i].partition, original.pending_offsets[i].partition);
        EXPECT_EQ(decoded.pending_offsets[i].offset, original.pending_offsets[i].offset);
        EXPECT_EQ(decoded.pending_offsets[i].metadata, original.pending_offsets[i].metadata);
    }
}

TEST(TxnStateSerialization, EmptySnapshotRoundTrips) {
    Snapshot s;
    s.transactional_id = "t";
    s.state = State::Empty;
    const Snapshot decoded = TSM::deserialize(TSM::serialize(s));
    EXPECT_EQ(decoded.transactional_id, "t");
    EXPECT_EQ(decoded.state, State::Empty);
    EXPECT_TRUE(decoded.partitions.empty());
    EXPECT_TRUE(decoded.pending_offsets.empty());
}

TEST(TxnStateSerialization, RejectsGarbage) {
    EXPECT_THROW(TSM::deserialize({0x00, 0x01, 0x02}), std::runtime_error);
}

TEST(TxnStateRouting, IsDeterministicAndInRange) {
    for (int parts : {1, 16, 50}) {
        const int p = TSM::partitionFor("my-transactional-id", parts);
        EXPECT_GE(p, 0);
        EXPECT_LT(p, parts);
        EXPECT_EQ(p, TSM::partitionFor("my-transactional-id", parts)) << "must be stable";
    }
}

// The two-phase state machine: prepare keeps state recoverable; complete is
// terminal. restore() must load an exact persisted state (including Prepare*),
// which recordInitProducerId cannot do (it forces Empty).
TEST(TxnCoordinatorTwoPhase, CommitPreparesThenCompletes) {
    TransactionCoordinator tc;
    tc.recordInitProducerId("t", 1, 0, 60000);
    tc.addPartitions("t", {{"topic", 0, 42}});

    auto participating = tc.prepareCommit("t");
    ASSERT_EQ(participating.size(), 1u);
    EXPECT_EQ(participating[0].first_offset, 42);
    auto snap = tc.describe("t");
    ASSERT_TRUE(snap.has_value());
    EXPECT_EQ(snap->state, State::PrepareCommit);
    EXPECT_FALSE(snap->partitions.empty()) << "prepare keeps partitions for recovery";

    tc.completeCommit("t");
    snap = tc.describe("t");
    ASSERT_TRUE(snap.has_value());
    EXPECT_EQ(snap->state, State::CompleteCommit);
    EXPECT_TRUE(snap->partitions.empty());
    EXPECT_EQ(tc.getMetrics().commits_total, 1);
}

TEST(TxnCoordinatorTwoPhase, AbortPreparesThenCompletesAndDropsOffsets) {
    TransactionCoordinator tc;
    tc.recordInitProducerId("t", 1, 0, 60000);
    tc.addPartitions("t", {{"topic", 0, 10}});
    tc.stagePendingOffsets("t", {{"g", "in", 0, 5, ""}});

    tc.prepareAbort("t");
    auto snap = tc.describe("t");
    ASSERT_TRUE(snap.has_value());
    EXPECT_EQ(snap->state, State::PrepareAbort);
    EXPECT_EQ(snap->pending_offsets.size(), 1u) << "prepare keeps offsets for recovery accuracy";

    tc.completeAbort("t");
    snap = tc.describe("t");
    ASSERT_TRUE(snap.has_value());
    EXPECT_EQ(snap->state, State::CompleteAbort);
    EXPECT_TRUE(snap->pending_offsets.empty()) << "abort discards staged offsets";
    EXPECT_EQ(tc.getMetrics().aborts_total, 1);
}

TEST(TxnCoordinatorRestore, LoadsPrepareStateVerbatim) {
    TransactionCoordinator tc;
    Snapshot s = makeSnapshot();  // PrepareCommit with partitions + offsets
    tc.restore(s);

    auto got = tc.describe(s.transactional_id);
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(got->state, State::PrepareCommit);
    EXPECT_EQ(got->producer_id, s.producer_id);
    EXPECT_EQ(got->partitions.size(), s.partitions.size());
    EXPECT_EQ(got->pending_offsets.size(), s.pending_offsets.size());
    // restore must not inflate metric counters.
    EXPECT_EQ(tc.getMetrics().state_loads_total, 0);
}

}  // namespace
}  // namespace kawasan::broker
