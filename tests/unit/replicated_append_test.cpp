// M5: Log::appendReplicatedBatch — the offset-preserving, HW-neutral append a
// follower uses to mirror its leader. Unlike appendBatch (which reassigns the
// base offset to the local LEO and advances the HW), this MUST preserve the
// leader's base offsets exactly and MUST NOT advance the high watermark, and it
// must detect gaps/duplicates rather than silently relabel them.
#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <iostream>
#include <string>
#include <vector>

#include "../sparse_record_batch.h"
#include "kawasan/common/buffer.h"
#include "kawasan/common/error.h"
#include "kawasan/common/logger.h"
#include "kawasan/storage/log.h"
#include "kawasan/storage/record_batch.h"

namespace fs = std::filesystem;
using kawasan::Buffer;
using kawasan::storage::Log;
using kawasan::storage::RecordBatch;
using Result = kawasan::storage::Log::ReplicaAppendResult;

namespace {

void ensureLogger() {
    static bool init = false;
    if (!init) {
        kawasan::Logger::init("warn");
        init = true;
    }
}

std::string makeDir(const std::string& tag) {
    const auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
    auto p = fs::temp_directory_path() / ("kawasan-repl-append-" + tag + "-" + std::to_string(ts));
    fs::create_directories(p);
    return p.string();
}

RecordBatch makeBatch(const std::vector<std::string>& values) {
    RecordBatch b;
    b.setMagic(2);
    b.setFirstTimestamp(0);
    for (const auto& v : values) {
        kawasan::Record r;
        r.timestamp = 0;
        r.value = std::vector<uint8_t>(v.begin(), v.end());
        b.addRecord(r);
    }
    return b;
}

// Split a concatenation of serialized batches (as returned by Log::readRaw)
// into individual RecordBatches — the leader→follower wire payload.
std::vector<RecordBatch> splitBatches(const std::vector<uint8_t>& raw) {
    std::vector<RecordBatch> out;
    Buffer buf(raw);
    while (buf.remaining() >= 12) {
        out.push_back(RecordBatch::deserialize(buf));
    }
    return out;
}

class ReplicatedAppendTest : public ::testing::Test {
protected:
    void SetUp() override {
        ensureLogger();
        leader_dir_ = makeDir("leader");
        follower_dir_ = makeDir("follower");
    }
    void TearDown() override {
        if (HasFailure()) {
            std::cerr << "Retained sparse replica data: " << follower_dir_ << '\n';
            return;
        }
        for (const auto& d : {leader_dir_, follower_dir_}) {
            if (fs::exists(d))
                fs::remove_all(d);
        }
    }
    std::string leader_dir_;
    std::string follower_dir_;
};

}  // namespace

// A follower mirrors the leader: offsets are preserved and records match, but
// the follower's high watermark is NOT advanced by the append (it stays 0 until
// the follower explicitly adopts the leader's HW).
TEST_F(ReplicatedAppendTest, PreservesOffsetsAndDoesNotAdvanceHighWatermark) {
    // Leader produces three batches at offsets 0,1,2 / 3,4 / 5.
    std::vector<uint8_t> wire;
    {
        Log leader("t", 0, leader_dir_);
        leader.appendBatch(makeBatch({"a", "b", "c"}));
        leader.appendBatch(makeBatch({"d", "e"}));
        leader.appendBatch(makeBatch({"f"}));
        ASSERT_EQ(leader.logEndOffset(), 6);
        ASSERT_EQ(leader.highWatermark(), 6);  // single-node leader: HW==LEO
        wire = leader.readRaw(0, 16 * 1024 * 1024);
        leader.close();
    }

    Log follower("t", 0, follower_dir_);
    for (auto& b : splitBatches(wire)) {
        EXPECT_EQ(follower.appendReplicatedBatch(b), Result::kAppended);
    }

    // Offsets preserved: follower LEO == leader LEO, and read-back base offsets match.
    EXPECT_EQ(follower.logEndOffset(), 6);
    // HW is leader-driven: appendReplicatedBatch must NOT have advanced it.
    EXPECT_EQ(follower.highWatermark(), 0);

    auto batches = follower.read(0, 16 * 1024 * 1024);
    std::vector<kawasan::Offset> bases;
    std::vector<std::string> values;
    for (const auto& b : batches) {
        bases.push_back(b.baseOffset());
        for (const auto& r : b.records()) {
            if (r.value)
                values.emplace_back(r.value->begin(), r.value->end());
        }
    }
    EXPECT_EQ(bases, (std::vector<kawasan::Offset>{0, 3, 5}));
    EXPECT_EQ(values, (std::vector<std::string>{"a", "b", "c", "d", "e", "f"}));

    // Adopting the leader's HW is a separate, explicit step.
    follower.setHighWatermark(6);
    EXPECT_EQ(follower.highWatermark(), 6);
}

// Re-delivering an already-held batch is a no-op (kDuplicate), so a reconnecting
// follower that re-fetches from an older offset does not double-append.
TEST_F(ReplicatedAppendTest, DuplicateBatchIsSkipped) {
    std::vector<uint8_t> wire;
    {
        Log leader("t", 0, leader_dir_);
        leader.appendBatch(makeBatch({"a", "b"}));
        wire = leader.readRaw(0, 16 * 1024 * 1024);
        leader.close();
    }
    auto batches = splitBatches(wire);
    ASSERT_EQ(batches.size(), 1u);

    Log follower("t", 0, follower_dir_);
    EXPECT_EQ(follower.appendReplicatedBatch(batches[0]), Result::kAppended);
    EXPECT_EQ(follower.logEndOffset(), 2);
    // Re-append the same batch (base offset 0 < LEO 2) -> duplicate, no growth.
    EXPECT_EQ(follower.appendReplicatedBatch(batches[0]), Result::kDuplicate);
    EXPECT_EQ(follower.logEndOffset(), 2);
}

// A batch whose base offset is beyond the follower's LEO is a gap: it must be
// REFUSED (not relabeled to the local LEO, which would corrupt offsets).
TEST_F(ReplicatedAppendTest, GapBatchIsRefused) {
    // Build a leader with two batches, then hand the follower ONLY the second
    // (base offset 2) — simulating a missing prefix.
    std::vector<RecordBatch> batches;
    {
        Log leader("t", 0, leader_dir_);
        leader.appendBatch(makeBatch({"a", "b"}));  // offsets 0,1
        leader.appendBatch(makeBatch({"c", "d"}));  // offsets 2,3
        batches = splitBatches(leader.readRaw(0, 16 * 1024 * 1024));
        leader.close();
    }
    ASSERT_EQ(batches.size(), 2u);
    ASSERT_EQ(batches[1].baseOffset(), 2);

    Log follower("t", 0, follower_dir_);
    // Follower LEO is 0; a batch at base 2 leaves a hole -> kGap, nothing written.
    EXPECT_EQ(follower.appendReplicatedBatch(batches[1]), Result::kGap);
    EXPECT_EQ(follower.logEndOffset(), 0);
    // The contiguous batch at base 0 is accepted.
    EXPECT_EQ(follower.appendReplicatedBatch(batches[0]), Result::kAppended);
    EXPECT_EQ(follower.logEndOffset(), 2);
    // Now the previously-gapped batch fits.
    EXPECT_EQ(follower.appendReplicatedBatch(batches[1]), Result::kAppended);
    EXPECT_EQ(follower.logEndOffset(), 4);
}

TEST_F(ReplicatedAppendTest, SparseWireSpanDefinesReplicaLeoDuplicateAndOverlap) {
    {
        Log follower("sparse", 0, follower_dir_);
        const auto sparse = kawasan::test_support::sparseBatch();
        ASSERT_EQ(follower.appendReplicatedBatch(sparse), Result::kAppended);
        EXPECT_EQ(follower.logEndOffset(), 10);
        EXPECT_EQ(follower.highWatermark(), 0);
        EXPECT_EQ(follower.appendReplicatedBatch(sparse), Result::kDuplicate);
        EXPECT_EQ(follower.appendReplicatedBatch(kawasan::test_support::sparseBatch(0, 11)),
                  Result::kOverlap);
        auto next = makeBatch({"next"});
        next.setBaseOffset(10);
        EXPECT_EQ(follower.appendReplicatedBatch(next), Result::kAppended);
        EXPECT_EQ(follower.logEndOffset(), 11);
        follower.close();
    }
    Log reopened("sparse", 0, follower_dir_);
    EXPECT_EQ(reopened.logEndOffset(), 11);
    EXPECT_EQ(reopened.highWatermark(), 0);
}

TEST_F(ReplicatedAppendTest, RejectsInvalidSparseSpanWithoutRegressingLeo) {
    for (int32_t delta : {-2, -1, 1, 8}) {
        SCOPED_TRACE(delta);
        Log follower("invalid", 0, follower_dir_ + "/invalid-" + std::to_string(delta));
        EXPECT_THROW(follower.appendReplicatedBatch(kawasan::test_support::sparseBatch(0, delta)),
                     kawasan::StorageException);
        EXPECT_EQ(follower.logEndOffset(), 0);
        EXPECT_EQ(follower.highWatermark(), 0);
    }
}
