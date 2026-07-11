// M7: exhaustive tests for Log::truncateSuffix — the corruption-risk primitive a
// follower uses to reconcile a divergent tail against a new leader. It must:
// remove exactly the records at/after the target (dropping whole trailing
// segments and truncating the segment that contains the target), leave the
// prefix intact and readable, clamp the high watermark down, survive a reopen,
// and allow appends to continue from the new log-end.
#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <string>
#include <vector>

#include "kawasan/common/logger.h"
#include "kawasan/storage/log.h"
#include "kawasan/storage/record_batch.h"

namespace fs = std::filesystem;
using kawasan::storage::Log;
using kawasan::storage::LogConfig;
using kawasan::storage::RecordBatch;

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
    auto p = fs::temp_directory_path() / ("kawasan-trunc-suffix-" + std::to_string(ts));
    fs::create_directories(p);
    return p.string();
}

RecordBatch batchOf(int n) {
    RecordBatch b;
    b.setMagic(2);
    b.setFirstTimestamp(0);
    for (int i = 0; i < n; ++i) {
        kawasan::Record r;
        r.timestamp = 0;
        r.value = std::vector<uint8_t>{static_cast<uint8_t>('a' + i)};
        b.addRecord(r);
    }
    return b;
}

// Total record count read back from offset 0.
int64_t recordCount(Log& log) {
    int64_t n = 0;
    for (const auto& b : log.read(0, 64 * 1024 * 1024)) {
        n += static_cast<int64_t>(b.records().size());
    }
    return n;
}

std::vector<kawasan::Offset> baseOffsets(Log& log) {
    std::vector<kawasan::Offset> out;
    for (const auto& b : log.read(0, 64 * 1024 * 1024)) {
        out.push_back(b.baseOffset());
    }
    return out;
}

class TruncateSuffixTest : public ::testing::Test {
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

// Truncating across segment boundaries: with a tiny segment size each batch gets
// its own segment, so truncation drops whole trailing segments.
TEST_F(TruncateSuffixTest, DropsWholeTrailingSegments) {
    LogConfig cfg;
    cfg.segment_size = 1;  // force one batch per segment
    Log log("t", 0, dir_, cfg);
    for (int i = 0; i < 6; ++i) {
        log.appendBatch(batchOf(1));  // single-record batches at offsets 0..5
    }
    ASSERT_EQ(log.logEndOffset(), 6);
    ASSERT_EQ(recordCount(log), 6);

    EXPECT_EQ(log.truncateSuffix(3), 3);
    EXPECT_EQ(log.logEndOffset(), 3);
    EXPECT_EQ(recordCount(log), 3);
    EXPECT_EQ(baseOffsets(log), (std::vector<kawasan::Offset>{0, 1, 2}));

    // Appends continue from the new log-end.
    log.appendBatch(batchOf(1));  // offset 3
    EXPECT_EQ(log.logEndOffset(), 4);
    EXPECT_EQ(baseOffsets(log), (std::vector<kawasan::Offset>{0, 1, 2, 3}));
}

// Truncating inside a single segment removes the batches at/after the target and
// keeps the prefix. A batch that straddles the target is removed whole.
TEST_F(TruncateSuffixTest, TruncatesWithinSegmentAndRemovesStraddlingBatch) {
    Log log("t", 0, dir_);        // default (large) segment: everything in one segment
    log.appendBatch(batchOf(3));  // offsets 0,1,2  (base 0)
    log.appendBatch(batchOf(3));  // offsets 3,4,5  (base 3)
    ASSERT_EQ(log.logEndOffset(), 6);

    // Target 4 falls inside the second batch (base 3 covers 3,4,5). A batch is
    // atomic, so the whole straddling batch is removed -> log-end becomes 3.
    EXPECT_EQ(log.truncateSuffix(4), 3);
    EXPECT_EQ(log.logEndOffset(), 3);
    EXPECT_EQ(recordCount(log), 3);
    EXPECT_EQ(baseOffsets(log), (std::vector<kawasan::Offset>{0}));
}

// Truncating exactly on a batch boundary removes that batch and keeps the prefix.
TEST_F(TruncateSuffixTest, TruncatesOnBatchBoundary) {
    Log log("t", 0, dir_);
    log.appendBatch(batchOf(3));  // base 0 -> 0,1,2
    log.appendBatch(batchOf(2));  // base 3 -> 3,4
    ASSERT_EQ(log.logEndOffset(), 5);

    EXPECT_EQ(log.truncateSuffix(3), 3);  // remove the second batch exactly
    EXPECT_EQ(log.logEndOffset(), 3);
    EXPECT_EQ(recordCount(log), 3);
}

// The high watermark is clamped down to the new log-end.
TEST_F(TruncateSuffixTest, ClampsHighWatermark) {
    Log log("t", 0, dir_);
    log.appendBatch(batchOf(3));
    log.appendBatch(batchOf(3));
    log.setHighWatermark(6);
    ASSERT_EQ(log.highWatermark(), 6);

    log.truncateSuffix(3);
    EXPECT_EQ(log.highWatermark(), 3) << "HW must never exceed the log-end after truncation";
}

// Truncating at/after the log-end is a no-op; below the log-start is clamped.
TEST_F(TruncateSuffixTest, NoOpAtOrAboveLogEnd) {
    Log log("t", 0, dir_);
    log.appendBatch(batchOf(3));
    ASSERT_EQ(log.logEndOffset(), 3);

    EXPECT_EQ(log.truncateSuffix(3), 3);   // == log end
    EXPECT_EQ(log.truncateSuffix(10), 3);  // past log end
    EXPECT_EQ(log.logEndOffset(), 3);
    EXPECT_EQ(recordCount(log), 3);
}

// Truncating to 0 empties the log; it stays usable and appends resume at 0.
TEST_F(TruncateSuffixTest, TruncateToZeroEmptiesLog) {
    LogConfig cfg;
    cfg.segment_size = 1;
    Log log("t", 0, dir_, cfg);
    for (int i = 0; i < 4; ++i)
        log.appendBatch(batchOf(1));
    ASSERT_EQ(log.logEndOffset(), 4);

    EXPECT_EQ(log.truncateSuffix(0), 0);
    EXPECT_EQ(log.logEndOffset(), 0);
    EXPECT_EQ(recordCount(log), 0);

    log.appendBatch(batchOf(1));  // resumes at offset 0
    EXPECT_EQ(log.logEndOffset(), 1);
    EXPECT_EQ(baseOffsets(log), (std::vector<kawasan::Offset>{0}));
}

// Truncation survives a reopen: the removed suffix stays gone and the log-end is
// the truncated value (verifies the on-disk state, not just in-memory).
TEST_F(TruncateSuffixTest, TruncationSurvivesReopen) {
    {
        LogConfig cfg;
        cfg.segment_size = 1;
        Log log("t", 0, dir_, cfg);
        for (int i = 0; i < 6; ++i)
            log.appendBatch(batchOf(1));
        log.truncateSuffix(3);
        ASSERT_EQ(log.logEndOffset(), 3);
        log.close();
    }
    Log reopened("t", 0, dir_);
    EXPECT_EQ(reopened.logEndOffset(), 3);
    EXPECT_EQ(recordCount(reopened), 3);
    EXPECT_EQ(baseOffsets(reopened), (std::vector<kawasan::Offset>{0, 1, 2}));
    // And it keeps working.
    reopened.appendBatch(batchOf(2));  // base 3 -> 3,4
    EXPECT_EQ(reopened.logEndOffset(), 5);
}
