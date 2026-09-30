// Locks in the durability guarantees added in Phase A1:
//  - the default flush mode is kSync (an acked produce fsyncs before returning),
//  - records and the high-watermark survive a close/reopen cycle, and
//  - the checkpoint is written atomically (a leftover *.tmp from a crash mid-write
//    never corrupts recovery).
// A true power-loss test (records lost when sync=false) can't be expressed as a
// unit test because a process-level close()/reopen always flushes RocksDB; these
// tests instead pin the safe default and the crash-safe checkpoint mechanics.
#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include "kawasan/common/logger.h"
#include "kawasan/storage/log.h"
#include "kawasan/storage/log_manager.h"
#include "kawasan/storage/record_batch.h"

namespace kawasan::storage {
namespace {

std::string makeTestDir() {
    const auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
    auto tmp = std::filesystem::temp_directory_path() /
               ("kawasan-log-durability-" + std::to_string(ts));
    std::filesystem::create_directories(tmp);
    return tmp.string();
}

RecordBatch makeBatch(const std::string& value) {
    RecordBatch batch;
    Record record;
    record.timestamp = std::chrono::system_clock::now().time_since_epoch().count();
    record.value = std::vector<uint8_t>(value.begin(), value.end());
    batch.addRecord(record);
    return batch;
}

void ensureLogger() {
    static bool initialized = false;
    if (!initialized) {
        Logger::init("warn");
        initialized = true;
    }
}

class LogDurabilityTest : public ::testing::Test {
protected:
    void SetUp() override {
        ensureLogger();
        dir_ = makeTestDir();
    }
    void TearDown() override {
        if (!dir_.empty() && std::filesystem::exists(dir_)) {
            std::filesystem::remove_all(dir_);
        }
    }
    std::string dir_;
};

int64_t checkpointHw(const std::filesystem::path& file) {
    std::ifstream in(file);
    std::string line;
    while (std::getline(in, line)) {
        if (line.rfind("high_watermark=", 0) == 0) {
            return std::stoll(line.substr(15));
        }
    }
    return -1;
}

void writeStaleCheckpoint(const std::filesystem::path& file, int64_t end) {
    std::ofstream(file, std::ios::trunc)
        << "log_start_offset=0\nlog_end_offset=" << end << "\nhigh_watermark=0\n";
}

size_t countRecords(Log& log) {
    size_t n = 0;
    for (const auto& batch : log.read(0, 16 * 1024 * 1024)) {
        n += batch.records().size();
    }
    return n;
}

}  // namespace

// The at-least-once durability guarantee depends on this default. If a future
// change flips it to async, this test fails loudly rather than silently
// re-opening the data-loss window.
TEST_F(LogDurabilityTest, DefaultFlushModeIsSync) {
    EXPECT_EQ(LogConfig{}.flush_mode, FlushMode::kSync);
}

TEST_F(LogDurabilityTest, RecordsAndHighWatermarkSurviveReopen) {
    {
        Log log("durable-topic", 0, dir_);  // default config => kSync
        log.appendBatch(makeBatch("one"));
        log.appendBatch(makeBatch("two"));
        log.appendBatch(makeBatch("three"));
        EXPECT_EQ(3, log.logEndOffset());
        EXPECT_EQ(3, log.highWatermark());
        log.close();
    }

    Log reopened("durable-topic", 0, dir_);
    EXPECT_EQ(3, reopened.logEndOffset());
    EXPECT_EQ(3, reopened.highWatermark());
    EXPECT_EQ(3u, countRecords(reopened));

    const auto checkpoint = std::filesystem::path(dir_) / "checkpoint.meta";
    EXPECT_TRUE(std::filesystem::exists(checkpoint));
}

// A crash between the temp write and the rename can leave a checkpoint.meta.tmp
// behind. The atomic temp+fsync+rename write means recovery reads the committed
// checkpoint and ignores the stale temp.
TEST_F(LogDurabilityTest, CheckpointSurvivesStaleTempFile) {
    {
        Log log("durable-topic", 0, dir_);
        log.appendBatch(makeBatch("a"));
        log.appendBatch(makeBatch("b"));
        log.close();
    }

    // Simulate a torn write left behind by a crash mid-checkpoint.
    const auto tmp = std::filesystem::path(dir_) / "checkpoint.meta.tmp";
    std::ofstream(tmp) << "log_end_offset=9999\nhigh_watermark=9999\n";

    Log reopened("durable-topic", 0, dir_);
    EXPECT_EQ(2, reopened.logEndOffset());
    EXPECT_EQ(2, reopened.highWatermark());
    EXPECT_EQ(2u, countRecords(reopened));
}

// The HW checkpoint is written lazily (periodic, like Kafka's
// replica.high.watermark.checkpoint.interval.ms) instead of fsync'd on every
// append; flushCheckpoint() persists only what changed.
TEST_F(LogDurabilityTest, CheckpointIsWrittenLazilyAndFlushedOnDemand) {
    Log log("durable-topic", 0, dir_);
    const auto checkpoint = std::filesystem::path(dir_) / "checkpoint.meta";
    const int64_t initial = checkpointHw(checkpoint);
    log.appendBatch(makeBatch("a"));
    log.appendBatch(makeBatch("b"));
    EXPECT_EQ(initial, checkpointHw(checkpoint));  // not rewritten per append

    log.flushCheckpoint();
    EXPECT_EQ(2, checkpointHw(checkpoint));
}

// Durability-critical internal writes (force_sync) keep a synchronous checkpoint.
TEST_F(LogDurabilityTest, ForceSyncAppendPersistsCheckpointImmediately) {
    Log log("durable-topic", 0, dir_);
    Record record;
    record.timestamp = 0;
    record.value = std::vector<uint8_t>{'x'};
    log.append({record}, /*force_sync=*/true);
    EXPECT_EQ(1, checkpointHw(std::filesystem::path(dir_) / "checkpoint.meta"));
}

// After a crash the checkpoint may lag the log. Single-node brokers recover HW
// to the log end (every record was committed by the sole replica); otherwise
// the stored HW is kept (clamped) and replication re-advances it.
TEST_F(LogDurabilityTest, StaleCheckpointHighWatermarkRecoveryModes) {
    {
        LogManager manager(dir_);
        auto* log = manager.getOrCreateLog("t", 0);
        log->appendBatch(makeBatch("a"));
        log->appendBatch(makeBatch("b"));
        log->appendBatch(makeBatch("c"));
    }
    const auto checkpoint = std::filesystem::path(dir_) / "t-0" / "checkpoint.meta";

    writeStaleCheckpoint(checkpoint, 3);
    {
        LogManager replicated(dir_);
        EXPECT_EQ(0, replicated.getOrCreateLog("t", 0)->highWatermark());
    }

    writeStaleCheckpoint(checkpoint, 3);
    {
        LogManager single_node(dir_);
        single_node.setRecoverHighWatermarkToLogEnd(true);
        auto* log = single_node.getOrCreateLog("t", 0);
        EXPECT_EQ(3, log->logEndOffset());
        EXPECT_EQ(3, log->highWatermark());
    }
}

// LogManager::stop() flushes dirty checkpoints (a clean shutdown never leaves a
// stale HW behind even if the periodic flusher hasn't run yet).
TEST_F(LogDurabilityTest, LogManagerStopFlushesDirtyCheckpoints) {
    LogManager manager(dir_);
    manager.setCheckpointIntervalMs(3600 * 1000);
    manager.start();
    auto* log = manager.getOrCreateLog("t", 0);
    log->appendBatch(makeBatch("a"));
    log->appendBatch(makeBatch("b"));
    manager.stop();
    EXPECT_EQ(2, checkpointHw(std::filesystem::path(dir_) / "t-0" / "checkpoint.meta"));
}

}  // namespace kawasan::storage
