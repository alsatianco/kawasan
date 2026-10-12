#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <climits>
#include <filesystem>
#include <future>
#include <iostream>
#include <memory>

#include "../sparse_record_batch.h"
#include "kawasan/common/error.h"
#include "kawasan/common/logger.h"
#include "kawasan/storage/log_manager.h"

namespace kawasan::storage {
namespace {
namespace fs = std::filesystem;

RecordBatch data(Offset base, int64_t producer, int16_t epoch, int32_t sequence,
                 Timestamp timestamp, bool transactional = false, size_t bytes = 1) {
    RecordBatch batch;
    batch.setBaseOffset(base);
    batch.setProducerId(producer);
    batch.setProducerEpoch(epoch);
    batch.setBaseSequence(sequence);
    batch.setAttributes(transactional ? 0x10 : 0);
    Record record("key", std::string(bytes, 'v'));
    record.timestamp = timestamp;
    batch.addRecord(record);
    batch.setFirstTimestamp(timestamp);
    batch.setMaxTimestamp(timestamp);
    return batch;
}

// Specify Kafka's versioned marker independently of makeControlBatch.
RecordBatch marker(Offset base, int64_t producer, int16_t epoch, Timestamp timestamp,
                   int32_t coordinator, bool commit = true) {
    RecordBatch batch;
    batch.setBaseOffset(base);
    batch.setProducerId(producer);
    batch.setProducerEpoch(epoch);
    batch.setBaseSequence(-1);
    batch.setAttributes(0x30);
    Record record;
    record.timestamp = timestamp;
    record.key = std::vector<uint8_t>{0, 0, 0, static_cast<uint8_t>(commit)};
    Buffer value;
    value.writeInt16(0);
    value.writeInt32(coordinator);
    record.value = value.vector();
    batch.addRecord(record);
    batch.setFirstTimestamp(timestamp);
    batch.setMaxTimestamp(timestamp);
    return batch;
}

class LogProducerInspectionTest : public ::testing::Test {
protected:
    void SetUp() override {
        static const bool initialized = [] {
            Logger::init("warn");
            return true;
        }();
        (void)initialized;
        dir_ = fs::temp_directory_path() /
               ("kawasan-producer-inspection-" +
                std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        config_.flush_mode = FlushMode::kAsync;
    }
    void TearDown() override {
        if (HasFailure())
            std::cerr << "Retained producer inspection data: " << dir_ << '\n';
        else
            fs::remove_all(dir_);
    }
    void append(Log& log, const RecordBatch& batch) {
        ASSERT_EQ(log.appendReplicatedBatch(batch), Log::ReplicaAppendResult::kAppended);
    }
    void expectError(Log& log, ErrorCode expected) {
        try {
            (void)log.inspectProducerState();
            FAIL() << "Incomplete/unreadable history must refuse inspection";
        } catch (const KawasanException& error) {
            EXPECT_EQ(error.code(), expected) << error.what();
        }
    }
    fs::path dir_;
    LogConfig config_;
};

TEST_F(LogProducerInspectionTest, EmptyAndNonIdempotentLogsHaveNoProducerState) {
    Log log("data", 0, dir_.string(), config_);
    EXPECT_TRUE(log.inspectProducerState().producers.empty());
    append(log, data(0, -1, -1, -1, 99));
    const auto image = log.inspectProducerState();
    EXPECT_EQ(image.log_end_offset, 1);
    EXPECT_TRUE(image.producers.empty());
}

TEST_F(LogProducerInspectionTest, FollowerInspectsUncommittedTransactionalTailThroughLeo) {
    Log log("data", 0, dir_.string(), config_);
    append(log, data(0, 9, 3, 40, 500, true));
    append(log, data(1, 9, 3, 41, 300, true));
    ASSERT_EQ(log.highWatermark(), 0);
    const auto image = log.inspectProducerState();
    EXPECT_EQ(image.log_end_offset, 2);
    ASSERT_EQ(image.producers.size(), 1u);
    EXPECT_EQ(image.producers[0], (Log::ProducerState{9, 3, 41, 300, -1, 0}));
}

TEST_F(LogProducerInspectionTest, CommitAndAbortPreserveDataSequenceAndEndTransaction) {
    for (bool commit : {false, true}) {
        Log log("data", 0, (dir_ / (commit ? "commit" : "abort")).string(), config_);
        append(log, data(0, 9, 3, 40, 500, true));
        append(log, marker(1, 9, 3, 800, 7, commit));
        auto image = log.inspectProducerState();
        ASSERT_EQ(image.producers.size(), 1u);
        EXPECT_EQ(image.producers[0], (Log::ProducerState{9, 3, 40, 800, 7, -1}));
        append(log, data(2, 9, 3, 41, 900, true));
        image = log.inspectProducerState();
        EXPECT_EQ(image.producers[0], (Log::ProducerState{9, 3, 41, 900, 7, 2}));
    }
}

TEST_F(LogProducerInspectionTest, HigherEpochMarkerClearsSequenceAndControlOnlyProducerIsVisible) {
    Log log("data", 0, dir_.string(), config_);
    append(log, data(0, 9, 3, 40, 500, true));
    append(log, marker(1, 9, 4, 800, 7));
    append(log, marker(2, 2, 1, 900, 12, false));
    const auto image = log.inspectProducerState();
    ASSERT_EQ(image.producers.size(), 2u);
    // Stable producer-ID order also makes independent replica images comparable.
    EXPECT_EQ(image.producers[0], (Log::ProducerState{2, 1, -1, 900, 12, -1}));
    EXPECT_EQ(image.producers[1], (Log::ProducerState{9, 4, -1, 800, 7, -1}));
}

TEST_F(LogProducerInspectionTest,
       SparseWrappedAndEmptyBatchHeadersSurviveChunkAndSegmentBoundaries) {
    config_.segment_size = 1;
    Log log("data", 0, dir_.string(), config_);
    auto first = data(0, 9, 3, INT32_MAX - 3, 500, true, 8 * 1024 * 1024);
    append(log, test_support::batchWithWireSpan(first, 9));
    RecordBatch empty;
    empty.setBaseOffset(10);
    empty.setProducerId(9);
    empty.setProducerEpoch(3);
    empty.setBaseSequence(6);
    empty.setAttributes(0x10);
    empty.setFirstTimestamp(600);
    empty.setMaxTimestamp(600);
    append(log, test_support::batchWithWireSpan(empty, 9));
    append(log, data(20, 2, 1, 20, 900));
    const auto image = log.inspectProducerState();
    EXPECT_EQ(image.log_end_offset, 21);
    ASSERT_EQ(image.producers.size(), 2u);
    EXPECT_EQ(image.producers[0], (Log::ProducerState{2, 1, 20, 900, -1, -1}));
    EXPECT_EQ(image.producers[1], (Log::ProducerState{9, 3, 15, 600, -1, 0}));
}

TEST_F(LogProducerInspectionTest, DuplicateOverlapAndDivergenceTruncationCannotLeakOldState) {
    Log log("data", 0, dir_.string(), config_);
    append(log, data(0, 9, 3, 40, 500, true));
    append(log, data(1, 2, 1, 0, 600));
    EXPECT_EQ(log.appendReplicatedBatch(data(0, 9, 3, 40, 500, true)),
              Log::ReplicaAppendResult::kDuplicate);
    auto replacement = test_support::batchWithWireSpan(data(1, 9, 3, 41, 700, true), 3);
    ASSERT_EQ(log.appendReplicatedBatch(replacement), Log::ReplicaAppendResult::kOverlap);
    ASSERT_EQ(log.truncateSuffix(1), 1);
    append(log, replacement);
    auto image = log.inspectProducerState();
    ASSERT_EQ(image.producers.size(), 1u);
    EXPECT_EQ(image.producers[0], (Log::ProducerState{9, 3, 44, 700, -1, 0}));
    ASSERT_EQ(log.truncateSuffix(1), 1);
    image = log.inspectProducerState();
    EXPECT_EQ(image.log_end_offset, 1);
    EXPECT_EQ(image.producers[0], (Log::ProducerState{9, 3, 40, 500, -1, 0}));
    append(log, marker(1, 9, 3, 1000, 8));
    ASSERT_EQ(log.truncateSuffix(0), 0);
    image = log.inspectProducerState();
    EXPECT_EQ(image.log_end_offset, 0);
    EXPECT_TRUE(image.producers.empty());
}

TEST_F(LogProducerInspectionTest, RestartReconstructsEveryFieldWithoutBrokerCacheOrSnapshots) {
    config_.segment_size = 1;
    std::vector<Log::ProducerState> expected;
    {
        Log log("data", 0, dir_.string(), config_);
        append(log, data(0, 9, 3, 40, 500, true));
        append(log, marker(1, 9, 3, 800, 7));
        append(log, data(2, 9, 3, 41, 900, true));
        expected = log.inspectProducerState().producers;
    }
    Log reopened("data", 0, dir_.string(), config_);
    const auto image = reopened.inspectProducerState();
    EXPECT_EQ(image.log_end_offset, 3);
    EXPECT_EQ(image.producers, expected);
    ASSERT_EQ(image.producers.size(), 1u);
    EXPECT_EQ(image.producers[0], (Log::ProducerState{9, 3, 41, 900, 7, 2}));
}

TEST_F(LogProducerInspectionTest, DeletedAndRecreatedTopicCannotReuseOldProducerState) {
    LogManager manager(dir_.string(), config_);
    auto* original = manager.getOrCreateLog("data", 0);
    append(*original, data(0, 9, 3, 40, 500, true));
    ASSERT_EQ(original->inspectProducerState().producers.size(), 1u);
    manager.deleteLog("data", 0);
    ASSERT_EQ(manager.getLog("data", 0), nullptr);
    auto* recreated = manager.getOrCreateLog("data", 0);
    EXPECT_TRUE(recreated->inspectProducerState().producers.empty());
    append(*recreated, data(0, 2, 1, 0, 900));
    const auto image = recreated->inspectProducerState();
    ASSERT_EQ(image.producers.size(), 1u);
    EXPECT_EQ(image.producers[0], (Log::ProducerState{2, 1, 0, 900, -1, -1}));
}

TEST_F(LogProducerInspectionTest, PrefixDeletionAndCompactionGapsRefuseIncompleteHistory) {
    config_.segment_size = 1;
    config_.cleanup_policy_compact = true;
    config_.cleanup_policy_delete = false;
    Log log("data", 0, dir_.string(), config_);
    append(log, data(0, 9, 3, 0, 500));
    append(log, data(1, 9, 3, 1, 600));
    append(log, data(2, 2, 1, 0, 900));
    log.setHighWatermark(3);
    log.cleanup();  // Repeated key erases earlier whole batches.
    expectError(log, ErrorCode::OFFSET_NOT_AVAILABLE);
    log.truncatePrefix(2);
    expectError(log, ErrorCode::OFFSET_NOT_AVAILABLE);
}

TEST_F(LogProducerInspectionTest, AuthorizedFollowerGapStillCannotInventMissingProducerHistory) {
    config_.cleanup_policy_compact = true;
    Log log("data", 0, dir_.string(), config_);
    append(log, data(0, 9, 3, 0, 500));
    ASSERT_EQ(log.appendReplicatedBatch(data(3, 9, 3, 3, 900), 4),
              Log::ReplicaAppendResult::kAppended);
    expectError(log, ErrorCode::OFFSET_NOT_AVAILABLE);
}

TEST_F(LogProducerInspectionTest, MalformedAndUnsupportedControlMarkersRefuseWholeImage) {
    for (int kind = 0; kind < 4; ++kind) {
        Log log("data", 0, (dir_ / std::to_string(kind)).string(), config_);
        append(log, data(0, 9, 3, 0, 500, true));
        auto bad = marker(1, 9, 3, 800, 7);
        Record record = bad.records()[0];
        if (kind == 0)
            record.value = std::vector<uint8_t>(4, 0);  // obsolete unversioned value
        if (kind == 1)
            record.key = std::vector<uint8_t>{0, 0, 0, 5};
        if (kind == 2)
            record.value = std::vector<uint8_t>{0xff, 0xff, 0, 0, 0, 7};
        if (kind == 3)
            record.key.reset();
        RecordBatch invalid;
        invalid.setBaseOffset(1);
        invalid.setProducerId(9);
        invalid.setProducerEpoch(3);
        invalid.setBaseSequence(-1);
        invalid.setAttributes(0x30);
        invalid.addRecord(record);
        append(log, invalid);
        expectError(log, ErrorCode::CORRUPT_MESSAGE);
    }
}

TEST_F(LogProducerInspectionTest, ClosedLogRefusesEvenIfAnEarlierInspectionSucceeded) {
    Log log("data", 0, dir_.string(), config_);
    append(log, data(0, 9, 3, 0, 500));
    ASSERT_EQ(log.inspectProducerState().producers.size(), 1u);
    log.close();
    expectError(log, ErrorCode::KAFKA_STORAGE_ERROR);
}

TEST_F(LogProducerInspectionTest, ConcurrentAppendCannotMixLogEndAndProducerSequence) {
    Log log("data", 0, dir_.string(), config_);
    append(log, data(0, 9, 3, 0, 500));
    auto writer = std::async(std::launch::async, [&] {
        for (int index = 1; index < 100; ++index)
            if (log.appendReplicatedBatch(data(index, 9, 3, index, 500 + index)) !=
                Log::ReplicaAppendResult::kAppended)
                return false;
        return true;
    });
    for (int iteration = 0; iteration < 100; ++iteration) {
        const auto image = log.inspectProducerState();
        ASSERT_EQ(image.producers.size(), 1u);
        EXPECT_EQ(image.producers[0].last_sequence, image.log_end_offset - 1);
        EXPECT_EQ(image.producers[0].last_timestamp, 499 + image.log_end_offset);
    }
    EXPECT_TRUE(writer.get());
    EXPECT_EQ(log.inspectProducerState().log_end_offset, 100);
}
}  // namespace
}  // namespace kawasan::storage
