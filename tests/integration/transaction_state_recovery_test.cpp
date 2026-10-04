// M1: durable transactions survive a broker restart. This drives the real
// TransactionStateManager against a real LogManager, reopening the manager to
// simulate a restart, then rebuilds the TransactionCoordinator + IsolationTracker
// exactly as KawasanBroker::replayTransactionStateFromLog does. Each test
// mirrors a crash at a distinct point in the transaction lifecycle and asserts
// the recovery invariants: no applied-offsets-from-aborted-txn, no frozen LSO,
// state restored faithfully.
#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <iostream>
#include <memory>
#include <string>

#include "kawasan/broker/isolation_tracker.h"
#include "kawasan/broker/transaction_coordinator.h"
#include "kawasan/broker/transaction_state_manager.h"
#include "kawasan/common/logger.h"
#include "kawasan/storage/log.h"
#include "kawasan/storage/log_manager.h"
#include "kawasan/storage/record_batch.h"

namespace fs = std::filesystem;
using kawasan::broker::IsolationTracker;
using kawasan::broker::TransactionCoordinator;
using kawasan::broker::TransactionStateManager;
using kawasan::storage::LogManager;
using kawasan::storage::RecordBatch;
using State = TransactionCoordinator::State;

namespace {

void ensureLogger() {
    static bool init = false;
    if (!init) {
        kawasan::Logger::init("warn");
        init = true;
    }
}

std::string makeDir() {
    const auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
    auto p = fs::temp_directory_path() / ("kawasan-txn-recovery-" + std::to_string(ts));
    fs::create_directories(p);
    return p.string();
}

constexpr int kTxnPartitions = 16;

// Rebuild coordinator + isolation tracker from a reopened log manager, mirroring
// KawasanBroker::replayTransactionStateFromLog's restore + in-flight re-arm.
// (Re-drive of Prepare* is exercised separately since it needs the broker.)
struct Recovered {
    std::unique_ptr<TransactionCoordinator> coordinator;
    std::unique_ptr<IsolationTracker> isolation;
};

Recovered recover(LogManager& lm) {
    Recovered r;
    r.coordinator = std::make_unique<TransactionCoordinator>();
    r.isolation = std::make_unique<IsolationTracker>();
    TransactionStateManager tsm(&lm, kTxnPartitions);
    for (const auto& snap : tsm.loadAll()) {
        r.coordinator->restore(snap);
        const bool in_flight = snap.state == State::Ongoing || snap.state == State::PrepareCommit ||
                               snap.state == State::PrepareAbort;
        if (in_flight) {
            for (const auto& tp : snap.partitions) {
                if (tp.first_offset >= 0) {
                    r.isolation->recordInFlightTxn(snap.producer_id, tp.topic, tp.partition,
                                                   tp.first_offset);
                }
            }
        }
    }
    return r;
}

class TxnRecoveryTest : public ::testing::Test {
protected:
    void SetUp() override {
        ensureLogger();
        dir_ = makeDir();
    }
    void TearDown() override {
        if (HasFailure()) {
            std::cerr << "Retained transaction recovery data: " << dir_ << '\n';
            return;
        }
        if (!dir_.empty() && fs::exists(dir_))
            fs::remove_all(dir_);
    }
    std::string dir_;
};

// Crash after AddPartitions (Ongoing persisted), before EndTxn. On restart the
// txn must be Ongoing and the LSO must hold at the recorded first_offset — the
// "no frozen LSO" flip: state is recovered rather than lost.
TEST_F(TxnRecoveryTest, OngoingTxnRestoresAndHoldsLso) {
    {
        LogManager lm(dir_);
        TransactionStateManager tsm(&lm, kTxnPartitions);
        TransactionCoordinator tc;
        tc.recordInitProducerId("txn-1", 1000, 3, 60000);
        tc.addPartitions("txn-1", {{"orders", 2, 40}});
        tsm.persist(*tc.describe("txn-1"));
    }  // LogManager destructs → flush → "crash"

    LogManager lm(dir_);
    auto r = recover(lm);
    auto snap = r.coordinator->describe("txn-1");
    ASSERT_TRUE(snap.has_value());
    EXPECT_EQ(snap->state, State::Ongoing);
    EXPECT_EQ(snap->producer_id, 1000);
    ASSERT_EQ(snap->partitions.size(), 1u);
    EXPECT_EQ(snap->partitions[0].first_offset, 40);
    // LSO holds at first_offset (40), well below a hypothetical HWM of 100.
    EXPECT_EQ(r.isolation->lastStableOffset("orders", 2, /*hwm=*/100), 40);
}

// Staged TxnOffsetCommit offsets must survive restart so a later commit applies
// them and an abort discards them.
TEST_F(TxnRecoveryTest, StagedOffsetsSurviveRestart) {
    {
        LogManager lm(dir_);
        TransactionStateManager tsm(&lm, kTxnPartitions);
        TransactionCoordinator tc;
        tc.recordInitProducerId("txn-2", 2000, 0, 60000);
        tc.addPartitions("txn-2", {{"data", 0, 10}});
        tc.stagePendingOffsets("txn-2", {{"grp", "input", 0, 777, "m"}});
        tsm.persist(*tc.describe("txn-2"));
    }
    LogManager lm(dir_);
    auto r = recover(lm);
    auto snap = r.coordinator->describe("txn-2");
    ASSERT_TRUE(snap.has_value());
    ASSERT_EQ(snap->pending_offsets.size(), 1u);
    EXPECT_EQ(snap->pending_offsets[0].group_id, "grp");
    EXPECT_EQ(snap->pending_offsets[0].offset, 777);
    EXPECT_EQ(snap->pending_offsets[0].metadata, "m");
}

// last-writer-wins: the newest snapshot per transactional_id is recovered.
TEST_F(TxnRecoveryTest, LastSnapshotWins) {
    {
        LogManager lm(dir_);
        TransactionStateManager tsm(&lm, kTxnPartitions);
        TransactionCoordinator tc;
        tc.recordInitProducerId("txn-3", 3000, 0, 60000);
        tsm.persist(*tc.describe("txn-3"));  // Empty
        tc.addPartitions("txn-3", {{"t", 1, 5}});
        tsm.persist(*tc.describe("txn-3"));  // Ongoing
        tc.prepareCommit("txn-3");
        tsm.persist(*tc.describe("txn-3"));  // PrepareCommit
    }
    LogManager lm(dir_);
    auto r = recover(lm);
    auto snap = r.coordinator->describe("txn-3");
    ASSERT_TRUE(snap.has_value());
    EXPECT_EQ(snap->state, State::PrepareCommit);
}

// A completed (CompleteAbort) txn must NOT re-hold the LSO after restart, and a
// committed txn's staged offsets are cleared — no applied-offsets-from-aborted.
TEST_F(TxnRecoveryTest, CompletedTxnReleasesLso) {
    {
        LogManager lm(dir_);
        TransactionStateManager tsm(&lm, kTxnPartitions);
        TransactionCoordinator tc;
        tc.recordInitProducerId("txn-4", 4000, 0, 60000);
        tc.addPartitions("txn-4", {{"t", 0, 20}});
        tc.stagePendingOffsets("txn-4", {{"g", "in", 0, 9, ""}});
        tc.prepareAbort("txn-4");
        tc.completeAbort("txn-4");
        tsm.persist(*tc.describe("txn-4"));  // terminal CompleteAbort
    }
    LogManager lm(dir_);
    auto r = recover(lm);
    auto snap = r.coordinator->describe("txn-4");
    ASSERT_TRUE(snap.has_value());
    EXPECT_EQ(snap->state, State::CompleteAbort);
    EXPECT_TRUE(snap->pending_offsets.empty());
    // No in-flight hold: LSO returns the HWM unchanged.
    EXPECT_EQ(r.isolation->lastStableOffset("t", 0, /*hwm=*/50), 50);
}

// Two transactional_ids (which may route to different __transaction_state
// partitions) both recover independently.
TEST_F(TxnRecoveryTest, MultipleTxnsRecover) {
    {
        LogManager lm(dir_);
        TransactionStateManager tsm(&lm, kTxnPartitions);
        TransactionCoordinator tc;
        tc.recordInitProducerId("alpha", 1, 0, 60000);
        tc.addPartitions("alpha", {{"t", 0, 1}});
        tsm.persist(*tc.describe("alpha"));
        tc.recordInitProducerId("beta", 2, 0, 60000);
        tc.addPartitions("beta", {{"t", 1, 2}});
        tsm.persist(*tc.describe("beta"));
    }
    LogManager lm(dir_);
    auto r = recover(lm);
    EXPECT_TRUE(r.coordinator->describe("alpha").has_value());
    EXPECT_TRUE(r.coordinator->describe("beta").has_value());
    EXPECT_EQ(r.isolation->inFlightCount(), 2u);
}

// The idempotency guard's underlying read: a control batch appended to a data
// partition is discoverable by producer_id on reopen — this is what stops
// crash-recovery re-drive from double-emitting a marker.
TEST_F(TxnRecoveryTest, ControlMarkerDiscoverableAfterReopen) {
    {
        LogManager lm(dir_);
        auto* log = lm.getOrCreateLog("orders", 0);
        // A data batch, then a COMMIT control marker for producer 5.
        RecordBatch data;
        data.setMagic(2);
        kawasan::Record rec;
        rec.value = std::vector<uint8_t>{'x'};
        data.addRecord(rec);
        log->appendBatch(std::move(data));
        const kawasan::Offset marker_off = log->logEndOffset();
        log->appendBatch(RecordBatch::makeControlBatch(5, 0, marker_off, true, 0));
    }
    LogManager lm(dir_);
    auto* log = lm.getOrCreateLog("orders", 0);
    bool found = false;
    for (const auto& batch : log->read(0, 1 << 20)) {
        if (batch.isControlBatch() && batch.producerId() == 5)
            found = true;
    }
    EXPECT_TRUE(found) << "a persisted control marker must be discoverable on restart";
}

}  // namespace

// M10: acquisition must consume only the committed prefix of its own partition.
TEST_F(TxnRecoveryTest, ScopedReplayExcludesUncommittedAndOtherPartitions) {
    LogManager lm(dir_);
    TransactionStateManager tsm(&lm, 2);
    TransactionCoordinator tc;
    std::vector<std::string> ids(2);
    for (int i = 0; ids[0].empty() || ids[1].empty(); ++i) {
        const std::string id = "scoped-" + std::to_string(i);
        ids[TransactionStateManager::partitionFor(id, 2)] = id;
    }
    for (int partition : {0, 1}) {
        tc.recordInitProducerId(ids[partition], 100 + partition, 0, 60000);
        tsm.persist(*tc.describe(ids[partition]));
    }
    tc.addPartitions(ids[0], {{"input", 0, 9}});
    tc.stagePendingOffsets(ids[0], {{"group", "input", 0, 42, "checkpoint", 17}});
    tc.prepareCommit(ids[0]);
    tsm.persist(*tc.describe(ids[0]));
    auto* log = lm.getLog(TransactionStateManager::kTopic, 0);
    ASSERT_EQ(log->logEndOffset(), 2);
    log->setHighWatermark(1);
    auto prefix = tsm.loadCommittedPartition(0);
    ASSERT_EQ(prefix.size(), 1u);
    EXPECT_EQ(prefix[0].transactional_id, ids[0]);
    EXPECT_EQ(prefix[0].state, State::Empty);
    EXPECT_TRUE(prefix[0].pending_offsets.empty());
    log->setHighWatermark(2);
    prefix = tsm.loadCommittedPartition(0);
    ASSERT_EQ(prefix.size(), 1u);
    EXPECT_EQ(prefix[0].state, State::PrepareCommit);
    ASSERT_EQ(prefix[0].pending_offsets.size(), 1u);
    EXPECT_EQ(prefix[0].pending_offsets[0].committed_leader_epoch, 17);
    const auto other = tsm.loadCommittedPartition(1);
    ASSERT_EQ(other.size(), 1u);
    EXPECT_EQ(other[0].transactional_id, ids[1]);
    EXPECT_EQ(other[0].state, State::Empty);
}

TEST_F(TxnRecoveryTest, ScopedReplayRejectsMalformedCommittedStateAndKeyMismatch) {
    LogManager lm(dir_);
    TransactionStateManager tsm(&lm, 1);
    auto* log = lm.getOrCreateLog(TransactionStateManager::kTopic, 0);
    kawasan::Record malformed("broken", "invalid snapshot");
    log->append({malformed}, true);
    log->setHighWatermark(0);
    EXPECT_TRUE(tsm.loadCommittedPartition(0).empty());
    log->setHighWatermark(1);
    EXPECT_THROW(tsm.loadCommittedPartition(0), std::runtime_error);
    log->truncateSuffix(0);
    TransactionCoordinator tc;
    tc.recordInitProducerId("actual", 1, 0, 60000);
    kawasan::Record mismatch;
    mismatch.key = std::vector<uint8_t>{'w', 'r', 'o', 'n', 'g'};
    mismatch.value = TransactionStateManager::serialize(*tc.describe("actual"));
    log->append({mismatch}, true);
    EXPECT_THROW(tsm.loadCommittedPartition(0), std::runtime_error);
}

TEST_F(TxnRecoveryTest, ScopedReplayAppliesTxnTombstones) {
    LogManager lm(dir_);
    TransactionStateManager tsm(&lm, 1);
    TransactionCoordinator tc;
    tc.recordInitProducerId("removed", 9, 0, 60000);
    tsm.persist(*tc.describe("removed"));
    auto* log = lm.getLog(TransactionStateManager::kTopic, 0);
    kawasan::Record tombstone;
    tombstone.key = std::vector<uint8_t>{'r', 'e', 'm', 'o', 'v', 'e', 'd'};
    log->append({tombstone}, true);
    EXPECT_TRUE(tsm.loadCommittedPartition(0).empty());
}

TEST_F(TxnRecoveryTest, ScopedReplayDoesNotCreateMissingLogsOrAcceptInvalidPartitions) {
    LogManager lm(dir_);
    TransactionStateManager tsm(&lm, 2);
    EXPECT_THROW(tsm.loadCommittedPartition(0), std::runtime_error);
    EXPECT_EQ(lm.getLog(TransactionStateManager::kTopic, 0), nullptr);
    EXPECT_THROW(tsm.loadCommittedPartition(-1), std::out_of_range);
    EXPECT_THROW(tsm.loadCommittedPartition(2), std::out_of_range);
}

TEST_F(TxnRecoveryTest, ScopedReplayBoundsRecordsInsideOneBatch) {
    LogManager lm(dir_);
    TransactionStateManager tsm(&lm, 1);
    TransactionCoordinator tc;
    tc.recordInitProducerId("committed", 15, 0, 60000);
    kawasan::Record valid;
    const std::string key = "committed";
    valid.key = std::vector<uint8_t>(key.begin(), key.end());
    valid.value = TransactionStateManager::serialize(*tc.describe(key));
    auto* log = lm.getOrCreateLog(TransactionStateManager::kTopic, 0);
    log->append({valid, kawasan::Record("uncommitted", "malformed snapshot")}, true);
    log->setHighWatermark(1);
    const auto snapshots = tsm.loadCommittedPartition(0);
    ASSERT_EQ(snapshots.size(), 1u);
    EXPECT_EQ(snapshots[0].transactional_id, "committed");
    log->setHighWatermark(2);
    EXPECT_THROW(tsm.loadCommittedPartition(0), std::runtime_error);
}

TEST_F(TxnRecoveryTest, ScopedReplayRejectsKeysInTheWrongPartition) {
    LogManager lm(dir_);
    TransactionStateManager tsm(&lm, 2);
    std::string id;
    for (int i = 0; id.empty(); ++i) {
        const auto candidate = "wrong-route-" + std::to_string(i);
        if (TransactionStateManager::partitionFor(candidate, 2) == 1)
            id = candidate;
    }
    TransactionCoordinator tc;
    tc.recordInitProducerId(id, 20, 0, 60000);
    kawasan::Record record;
    record.key = std::vector<uint8_t>(id.begin(), id.end());
    record.value = TransactionStateManager::serialize(*tc.describe(id));
    lm.getOrCreateLog(TransactionStateManager::kTopic, 0)->append({record}, true);
    EXPECT_THROW(tsm.loadCommittedPartition(0), std::runtime_error);
}
