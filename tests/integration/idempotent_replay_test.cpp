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
#include <fstream>
#include <string>
#include <vector>

#include "kawasan/broker/producer_state_manager.h"
#include "kawasan/broker/producer_state_snapshot.h"
#include "kawasan/common/logger.h"
#include "kawasan/storage/log.h"
#include "kawasan/storage/record_batch.h"

namespace fs = std::filesystem;
using kawasan::broker::ProducerStateManager;
using kawasan::broker::ProducerStateSnapshot;
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

// Mirrors KawasanBroker::replayProducerStateFromLog with a start offset. Only
// batches whose base offset is at/after `start_offset` are applied — the "tail"
// the snapshot bounds the replay to. start_offset=0 is a full replay.
void replayFrom(ProducerStateManager& psm, const std::string& topic, kawasan::PartitionId partition,
                Log& log, kawasan::Offset start_offset) {
    for (const auto& batch : log.read(start_offset, 16 * 1024 * 1024)) {
        if (batch.baseOffset() < start_offset)
            continue;
        const int32_t count = static_cast<int32_t>(batch.records().size());
        if (batch.producerId() >= 0 && count > 0) {
            psm.recordAppend(topic, partition, batch.producerId(), batch.producerEpoch(),
                             batch.baseSequence(), count, batch.baseOffset());
        }
    }
}

void replay(ProducerStateManager& psm, const std::string& topic, kawasan::PartitionId partition,
            Log& log) {
    replayFrom(psm, topic, partition, log, 0);
}

class IdempotentReplayTest : public ::testing::Test {
protected:
    void SetUp() override {
        ensureLogger();
        dir_ = makeDir();
    }
    void TearDown() override {
        if (fs::exists(dir_))
            fs::remove_all(dir_);
    }
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

// M3: the on-disk snapshot format round-trips exactly, and a single flipped byte
// is rejected by the CRC-32C frame (so a torn snapshot can never be trusted).
TEST_F(IdempotentReplayTest, SnapshotSerializeRoundTripAndCrcRejection) {
    std::vector<ProducerStateManager::SnapshotEntry> entries = {
        {/*producer_id=*/7, /*last_epoch=*/0, /*last_sequence=*/2, /*last_base_sequence=*/0,
         /*last_record_count=*/3, /*last_base_offset=*/0},
        {/*producer_id=*/8, /*last_epoch=*/1, /*last_sequence=*/1, /*last_base_sequence=*/0,
         /*last_record_count=*/2, /*last_base_offset=*/3},
    };
    auto bytes = ProducerStateSnapshot::serialize(/*snapshot_offset=*/5, entries);
    auto loaded = ProducerStateSnapshot::deserialize(bytes);
    ASSERT_TRUE(loaded.has_value());
    EXPECT_EQ(loaded->snapshot_offset, 5);
    ASSERT_EQ(loaded->entries.size(), entries.size());
    for (size_t i = 0; i < entries.size(); ++i) {
        EXPECT_EQ(loaded->entries[i].producer_id, entries[i].producer_id);
        EXPECT_EQ(loaded->entries[i].last_epoch, entries[i].last_epoch);
        EXPECT_EQ(loaded->entries[i].last_sequence, entries[i].last_sequence);
        EXPECT_EQ(loaded->entries[i].last_base_sequence, entries[i].last_base_sequence);
        EXPECT_EQ(loaded->entries[i].last_record_count, entries[i].last_record_count);
        EXPECT_EQ(loaded->entries[i].last_base_offset, entries[i].last_base_offset);
    }
    // Flip a byte inside the body — the CRC prefix must reject it.
    bytes[bytes.size() - 1] ^= 0xFF;
    EXPECT_FALSE(ProducerStateSnapshot::deserialize(bytes).has_value());
}

// M3: restoring the newest snapshot and replaying only the log TAIL after its
// offset rebuilds the exact same dedup state as a full-log replay — and the
// snapshot is load-bearing: producer 7's state exists ONLY in the snapshot (it
// is not in the replayed tail), so a correct duplicate verdict for it can come
// from nowhere else.
TEST_F(IdempotentReplayTest, SnapshotPlusTailReplayEqualsFullReplay) {
    // Producer 7 writes offsets 0..2; producer 8 writes offsets 3..4.
    {
        Log log("idem-topic", 0, dir_);
        log.appendBatch(makeIdempotentBatch(/*pid=*/7, 0, /*base_seq=*/0, {"a", "b", "c"}));
        log.appendBatch(makeIdempotentBatch(/*pid=*/8, 0, /*base_seq=*/0, {"d", "e"}));
        log.close();
    }

    // Build a checkpoint as of offset 3 (producer 7 only) and write a snapshot.
    const kawasan::Offset kSnapOffset = 3;
    {
        Log ck("idem-topic", 0, dir_);
        ProducerStateManager checkpoint;
        // Replay producer 7's prefix (offsets [0,3)) into the checkpoint state.
        for (const auto& b : ck.read(0, 16 * 1024 * 1024)) {
            if (b.baseOffset() >= kSnapOffset)
                continue;
            checkpoint.recordAppend("idem-topic", 0, b.producerId(), b.producerEpoch(),
                                    b.baseSequence(), static_cast<int32_t>(b.records().size()),
                                    b.baseOffset());
        }
        ProducerStateSnapshot::write(dir_, kSnapOffset,
                                     checkpoint.snapshotEntries("idem-topic", 0));
    }

    // Restart path: restore newest snapshot + replay only the tail from its offset.
    Log reopened("idem-topic", 0, dir_);
    ProducerStateManager restored;
    auto snap = ProducerStateSnapshot::loadNewest(dir_);
    ASSERT_TRUE(snap.has_value());
    EXPECT_EQ(snap->snapshot_offset, kSnapOffset);
    restored.restoreEntries("idem-topic", 0, snap->entries);
    replayFrom(restored, "idem-topic", 0, reopened, snap->snapshot_offset);

    // Oracle: a full replay from offset 0.
    ProducerStateManager full;
    replay(full, "idem-topic", 0, reopened);

    // Producer 7's dedup state came ONLY from the snapshot (the tail replay never
    // saw it) yet a retry is still detected as a duplicate — proof the snapshot
    // is load-bearing, matching the full-replay oracle.
    EXPECT_EQ(restored.check("idem-topic", 0, 7, 0, 0, 3).error,
              kawasan::ErrorCode::DUPLICATE_SEQUENCE_NUMBER);
    EXPECT_EQ(full.check("idem-topic", 0, 7, 0, 0, 3).error,
              kawasan::ErrorCode::DUPLICATE_SEQUENCE_NUMBER);
    // Producer 8's state came from the tail replay in both.
    EXPECT_EQ(restored.check("idem-topic", 0, 8, 0, 0, 2).error,
              kawasan::ErrorCode::DUPLICATE_SEQUENCE_NUMBER);
    EXPECT_EQ(full.check("idem-topic", 0, 8, 0, 0, 2).error,
              kawasan::ErrorCode::DUPLICATE_SEQUENCE_NUMBER);
    // The next in-order sequence for producer 8 is accepted in both.
    EXPECT_EQ(restored.check("idem-topic", 0, 8, 0, /*base_seq=*/2, 1).error,
              kawasan::ErrorCode::NONE);
    EXPECT_EQ(full.check("idem-topic", 0, 8, 0, /*base_seq=*/2, 1).error, kawasan::ErrorCode::NONE);
}

// M3: a corrupt snapshot must be skipped so recovery falls back to a full-log
// replay — a bad checkpoint can never poison or truncate dedup state.
TEST_F(IdempotentReplayTest, CorruptSnapshotFallsBackToFullReplay) {
    {
        Log log("idem-topic", 0, dir_);
        log.appendBatch(makeIdempotentBatch(/*pid=*/7, 0, 0, {"a", "b", "c"}));
        log.appendBatch(makeIdempotentBatch(/*pid=*/8, 0, 0, {"d", "e"}));
        log.close();
    }
    // Write a valid snapshot at offset 3, then corrupt every .psnap on disk.
    {
        ProducerStateManager checkpoint;
        checkpoint.recordAppend("idem-topic", 0, 7, 0, 0, 3, 0);
        ProducerStateSnapshot::write(dir_, 3, checkpoint.snapshotEntries("idem-topic", 0));
    }
    size_t corrupted = 0;
    for (const auto& e : fs::directory_iterator(dir_)) {
        if (e.path().extension() == ".psnap") {
            std::ofstream f(e.path(), std::ios::binary | std::ios::app);
            const char junk[] = {0x00, 0x01, 0x02, 0x03};
            f.write(junk, sizeof(junk));
            ++corrupted;
        }
    }
    ASSERT_GT(corrupted, 0u);

    // loadNewest rejects the corrupt file -> nullopt -> full replay from 0.
    EXPECT_FALSE(ProducerStateSnapshot::loadNewest(dir_).has_value());

    Log reopened("idem-topic", 0, dir_);
    ProducerStateManager restored;
    auto snap = ProducerStateSnapshot::loadNewest(dir_);
    const kawasan::Offset start = snap ? snap->snapshot_offset : 0;
    if (snap)
        restored.restoreEntries("idem-topic", 0, snap->entries);
    replayFrom(restored, "idem-topic", 0, reopened, start);

    // Both producers' state was rebuilt by the fallback full replay.
    EXPECT_EQ(restored.check("idem-topic", 0, 7, 0, 0, 3).error,
              kawasan::ErrorCode::DUPLICATE_SEQUENCE_NUMBER);
    EXPECT_EQ(restored.check("idem-topic", 0, 8, 0, 0, 2).error,
              kawasan::ErrorCode::DUPLICATE_SEQUENCE_NUMBER);
}
