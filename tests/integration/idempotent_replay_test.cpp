// B3: idempotent-producer state must survive a broker restart. Producer state is
// not stored in a separate snapshot; instead it is rebuilt at startup by
// replaying the persisted V2 record-batch headers (Kafka's no-snapshot
// fallback). This test exercises that exact path: append an idempotent batch to
// a Log, reopen it (simulating restart), replay into a fresh
// ProducerStateManager, and assert the producer's sequence state is restored so
// a retried batch is detected as a duplicate (not silently re-appended). The
// no-replay control shows the bug this fixes.
#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <string>
#include <vector>

#include "kawasan/broker/producer_state_manager.h"
#include "kawasan/common/logger.h"
#include "kawasan/storage/log.h"
#include "kawasan/storage/record_batch.h"

namespace fs = std::filesystem;
using kawasan::broker::ProducerStateManager;
using kawasan::storage::Log;
using kawasan::storage::RecordBatch;

namespace {

std::string makeDir() {
    const auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
    auto p = fs::temp_directory_path() / ("kawasan-idem-replay-" + std::to_string(ts));
    fs::create_directories(p);
    return p.string();
}

void ensureLogger() {
    static bool init = false;
    if (!init) {
        kawasan::Logger::init("warn");
        init = true;
    }
}

RecordBatch makeIdempotentBatch(int64_t pid, int16_t epoch, int32_t base_seq,
                                const std::vector<std::string>& values) {
    RecordBatch b;
    b.setMagic(2);
    b.setProducerId(pid);
    b.setProducerEpoch(epoch);
    b.setBaseSequence(base_seq);
    b.setFirstTimestamp(0);
    for (const auto& v : values) {
        kawasan::Record r;
        r.timestamp = 0;
        r.value = std::vector<uint8_t>(v.begin(), v.end());
        b.addRecord(r);
    }
    return b;
}

// Mirrors KawasanBroker::replayProducerStateFromLog.
void replay(ProducerStateManager& psm, const std::string& topic,
            kawasan::PartitionId partition, Log& log) {
    for (const auto& batch : log.read(0, 16 * 1024 * 1024)) {
        const int32_t count = static_cast<int32_t>(batch.records().size());
        if (batch.producerId() >= 0 && count > 0) {
            psm.recordAppend(topic, partition, batch.producerId(), batch.producerEpoch(),
                             batch.baseSequence(), count, batch.baseOffset());
        }
    }
}

class IdempotentReplayTest : public ::testing::Test {
protected:
    void SetUp() override { ensureLogger(); dir_ = makeDir(); }
    void TearDown() override { if (fs::exists(dir_)) fs::remove_all(dir_); }
    std::string dir_;
};

}  // namespace

TEST_F(IdempotentReplayTest, ReplayRebuildsDedupStateAcrossRestart) {
    // Write an idempotent batch (pid=5, epoch=0, seq 0..2) and close the log.
    {
        Log log("idem-topic", 0, dir_);
        log.appendBatch(makeIdempotentBatch(5, 0, /*base_seq=*/0, {"a", "b", "c"}));
        log.close();
    }

    // Reopen (== restart) and rebuild producer state from the persisted batches.
    Log reopened("idem-topic", 0, dir_);
    ProducerStateManager psm;
    replay(psm, "idem-topic", 0, reopened);

    // A retry of the same batch (same pid/epoch/base_sequence) is now a duplicate.
    auto dup = psm.check("idem-topic", 0, /*pid=*/5, /*epoch=*/0, /*base_seq=*/0,
                         /*count=*/3);
    EXPECT_EQ(dup.error, kawasan::ErrorCode::DUPLICATE_SEQUENCE_NUMBER);

    // The next in-order sequence is accepted.
    auto next = psm.check("idem-topic", 0, 5, 0, /*base_seq=*/3, /*count=*/2);
    EXPECT_EQ(next.error, kawasan::ErrorCode::NONE);
}

// Control: WITHOUT the replay, a fresh manager (an empty post-restart state, the
// pre-fix behavior) fails to recognize the retried batch — it would be appended
// again, producing a silent duplicate. This is exactly what B3 fixes.
TEST_F(IdempotentReplayTest, WithoutReplayDuplicateIsNotDetected) {
    {
        Log log("idem-topic", 0, dir_);
        log.appendBatch(makeIdempotentBatch(5, 0, 0, {"a", "b", "c"}));
        log.close();
    }
    ProducerStateManager fresh;  // no replay — simulates the old in-memory-only restart
    auto wouldDup = fresh.check("idem-topic", 0, 5, 0, /*base_seq=*/0, /*count=*/3);
    EXPECT_EQ(wouldDup.error, kawasan::ErrorCode::NONE)
        << "without replay the duplicate is undetected (the bug B3 fixes)";
}
