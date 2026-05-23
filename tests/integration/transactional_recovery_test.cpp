// Phase EX-10 / opus2 Item 1: lock the invariant that a fully-formed
// transactional RecordBatch's V2 header (producer_id, producer_epoch,
// base_sequence, isTransactional bit, isControl bit) survives the
// full lifecycle:
//
//     produce-side build → Log::appendBatch → RocksDB persist
//     → Log close → Log re-open → Log::read → consume-side
//
// Before EX-10, handleProduce called `Log::append(records)` which
// rebuilt a default-attributes batch from just the records, silently
// dropping producer_id/epoch/baseSequence/attributes. read_committed
// isolation cannot function if these don't round-trip — clients use
// producer_id to match records against the aborted_transactions list.
//
// This test would have failed before the EX-10 fix. It must pass on
// every commit that touches the storage layer.
#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <string>
#include <vector>

#include "kawasan/common/types.h"
#include "kawasan/storage/log.h"
#include "kawasan/storage/record_batch.h"

namespace fs = std::filesystem;

namespace {

std::string makeLogDir(const std::string& prefix) {
    const auto timestamp =
        std::chrono::steady_clock::now().time_since_epoch().count();
    auto path = fs::temp_directory_path() /
                ("kawasan-txn-recovery-" + prefix + "-" +
                 std::to_string(timestamp));
    fs::create_directories(path);
    return path.string();
}

kawasan::Record makeRecord(const std::string& key,
                           const std::string& value) {
    kawasan::Record r;
    r.key = std::vector<uint8_t>(key.begin(), key.end());
    r.value = std::vector<uint8_t>(value.begin(), value.end());
    r.timestamp = 1700000000000LL;
    return r;
}

TEST(TransactionalRecoveryTest, V2HeaderSurvivesAppendCloseReopenRead) {
    const std::string log_dir = makeLogDir("v2-header");

    // Phase 1: open log, append a transactional batch with a populated
    // V2 header, close.
    constexpr int64_t kProducerId = 42;
    constexpr int16_t kProducerEpoch = 7;
    constexpr int32_t kBaseSequence = 100;

    {
        kawasan::storage::Log log("test-topic", /*partition=*/0, log_dir);

        kawasan::storage::RecordBatch batch;
        batch.setMagic(2);
        batch.setProducerId(kProducerId);
        batch.setProducerEpoch(kProducerEpoch);
        batch.setBaseSequence(kBaseSequence);
        // Set isTransactional (bit 4). Production transactional Produce
        // requests come in with this bit set; the broker is responsible
        // for not stripping it.
        batch.setAttributes(static_cast<int16_t>(1 << 4));
        batch.setFirstTimestamp(1700000000000LL);
        batch.setMaxTimestamp(1700000000000LL);
        batch.addRecord(makeRecord("k1", "v1"));
        batch.addRecord(makeRecord("k2", "v2"));
        batch.addRecord(makeRecord("k3", "v3"));

        log.appendBatch(std::move(batch));
    }  // ~Log() flushes RocksDB

    // Phase 2: re-open the log from the same dir, read the batch back,
    // assert every V2 header field survived.
    {
        kawasan::storage::Log log("test-topic", /*partition=*/0, log_dir);

        auto batches = log.read(/*start_offset=*/0, /*max_bytes=*/1 << 20);
        ASSERT_EQ(batches.size(), 1u) << "expected exactly one batch after restart";

        const auto& b = batches[0];
        EXPECT_EQ(b.producerId(), kProducerId)
            << "producer_id was lost across the restart — read_committed will break";
        EXPECT_EQ(b.producerEpoch(), kProducerEpoch);
        EXPECT_EQ(b.baseSequence(), kBaseSequence);
        EXPECT_TRUE(b.isTransactional())
            << "isTransactional bit was stripped — client-side aborted-txn "
               "filtering depends on this bit being preserved";
        ASSERT_EQ(b.records().size(), 3u);
        ASSERT_TRUE(b.records()[0].key.has_value());
        ASSERT_TRUE(b.records()[2].value.has_value());
        EXPECT_EQ(std::string(b.records()[0].key->begin(), b.records()[0].key->end()), "k1");
        EXPECT_EQ(std::string(b.records()[2].value->begin(), b.records()[2].value->end()), "v3");
    }

    fs::remove_all(log_dir);
}

TEST(TransactionalRecoveryTest, ControlBatchIsControlBitSurvivesRecovery) {
    const std::string log_dir = makeLogDir("control");

    // makeControlBatch sets the V2 attributes to (isControl | isTransactional)
    // = (1<<5) | (1<<4). Persist + reopen must preserve this — clients
    // identify the COMMIT/ABORT markers by the isControl bit and skip
    // them from delivery.
    {
        kawasan::storage::Log log("test-topic", /*partition=*/0, log_dir);
        auto ctrl = kawasan::storage::RecordBatch::makeControlBatch(
            /*producer_id=*/99,
            /*producer_epoch=*/3,
            /*base_offset=*/0,
            /*committed=*/false,  // ABORT marker
            /*timestamp_ms=*/1700000000000LL);
        log.appendBatch(std::move(ctrl));
    }

    {
        kawasan::storage::Log log("test-topic", /*partition=*/0, log_dir);
        auto batches = log.read(0, 1 << 20);
        ASSERT_EQ(batches.size(), 1u);

        const auto& b = batches[0];
        EXPECT_TRUE(b.isControlBatch())
            << "isControl bit lost on recovery — read_committed clients would "
               "deliver COMMIT/ABORT markers to user code as data";
        EXPECT_TRUE(b.isTransactional());
        EXPECT_EQ(b.producerId(), 99);
        EXPECT_EQ(b.producerEpoch(), 3);
    }

    fs::remove_all(log_dir);
}

TEST(TransactionalRecoveryTest, NonTransactionalBatchAlsoRoundTrips) {
    // Baseline: a regular non-txn batch (producer_id=-1) must continue
    // to round-trip exactly as before. Regression check that our
    // appendBatch path didn't pessimize the non-transactional case.
    const std::string log_dir = makeLogDir("non-txn");

    {
        kawasan::storage::Log log("test-topic", /*partition=*/0, log_dir);
        kawasan::storage::RecordBatch batch;
        batch.setMagic(2);
        batch.setAttributes(0);
        batch.setFirstTimestamp(1700000000000LL);
        batch.setMaxTimestamp(1700000000000LL);
        batch.addRecord(makeRecord("hello", "world"));
        log.appendBatch(std::move(batch));
    }

    {
        kawasan::storage::Log log("test-topic", /*partition=*/0, log_dir);
        auto batches = log.read(0, 1 << 20);
        ASSERT_EQ(batches.size(), 1u);
        const auto& b = batches[0];
        EXPECT_EQ(b.producerId(), -1);
        EXPECT_FALSE(b.isTransactional());
        EXPECT_FALSE(b.isControlBatch());
        ASSERT_EQ(b.records().size(), 1u);
    }

    fs::remove_all(log_dir);
}

TEST(TransactionalRecoveryTest, MixedTxnAndNonTxnBatchesInSameLog) {
    // Real workload: a partition typically interleaves transactional
    // and non-transactional records. Both must round-trip with their
    // own header intact — no leakage of one batch's metadata into the
    // next.
    const std::string log_dir = makeLogDir("mixed");

    {
        kawasan::storage::Log log("test-topic", /*partition=*/0, log_dir);

        // Non-txn batch
        {
            kawasan::storage::RecordBatch b;
            b.setMagic(2);
            b.setAttributes(0);
            b.setFirstTimestamp(1700000000000LL);
            b.setMaxTimestamp(1700000000000LL);
            b.addRecord(makeRecord("plain", "1"));
            log.appendBatch(std::move(b));
        }
        // Txn batch
        {
            kawasan::storage::RecordBatch b;
            b.setMagic(2);
            b.setProducerId(55);
            b.setProducerEpoch(2);
            b.setBaseSequence(0);
            b.setAttributes(static_cast<int16_t>(1 << 4));
            b.setFirstTimestamp(1700000000000LL);
            b.setMaxTimestamp(1700000000000LL);
            b.addRecord(makeRecord("txn", "1"));
            log.appendBatch(std::move(b));
        }
        // Another non-txn batch
        {
            kawasan::storage::RecordBatch b;
            b.setMagic(2);
            b.setAttributes(0);
            b.setFirstTimestamp(1700000000000LL);
            b.setMaxTimestamp(1700000000000LL);
            b.addRecord(makeRecord("plain", "2"));
            log.appendBatch(std::move(b));
        }
    }

    {
        kawasan::storage::Log log("test-topic", /*partition=*/0, log_dir);
        auto batches = log.read(0, 1 << 20);
        ASSERT_EQ(batches.size(), 3u);

        EXPECT_EQ(batches[0].producerId(), -1);
        EXPECT_FALSE(batches[0].isTransactional());

        EXPECT_EQ(batches[1].producerId(), 55);
        EXPECT_EQ(batches[1].producerEpoch(), 2);
        EXPECT_TRUE(batches[1].isTransactional());

        EXPECT_EQ(batches[2].producerId(), -1);
        EXPECT_FALSE(batches[2].isTransactional());
    }

    fs::remove_all(log_dir);
}

}  // namespace
