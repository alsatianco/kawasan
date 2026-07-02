#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <string>
#include <vector>

#include "kawasan/common/logger.h"
#include "kawasan/storage/log.h"
#include "kawasan/storage/record_batch.h"

namespace kawasan::storage {

namespace {

std::string makeTestDir(const std::string& tag) {
    const auto timestamp = std::chrono::steady_clock::now().time_since_epoch().count();
    auto tmp = std::filesystem::temp_directory_path() /
               ("kawasan-max-ts-" + tag + "-" + std::to_string(timestamp));
    std::filesystem::create_directories(tmp);
    return tmp.string();
}

RecordBatch makeDataBatch(int64_t timestamp, const std::string& value) {
    RecordBatch batch;
    Record record;
    record.timestamp = timestamp;
    record.value = std::vector<uint8_t>(value.begin(), value.end());
    batch.addRecord(record);
    batch.setFirstTimestamp(timestamp);
    batch.setMaxTimestamp(timestamp);
    return batch;
}

void ensureLoggerInitialized() {
    static bool initialized = false;
    if (!initialized) {
        Logger::init("warn");
        initialized = true;
    }
}

LogConfig asyncConfig() {
    LogConfig config;
    config.flush_mode = FlushMode::kAsync;
    return config;
}

}  // namespace

class LogMaxTimestampTest : public ::testing::Test {
protected:
    void SetUp() override { ensureLoggerInitialized(); }

    void TearDown() override {
        if (!test_dir_.empty() && std::filesystem::exists(test_dir_)) {
            std::filesystem::remove_all(test_dir_);
        }
    }

    std::string test_dir_;
};

TEST_F(LogMaxTimestampTest, EmptyLogHasNoMaxTimestamp) {
    test_dir_ = makeTestDir("empty");
    Log log("max-ts-empty", 0, test_dir_, asyncConfig());
    EXPECT_FALSE(log.maxTimestampOffset().has_value());
}

TEST_F(LogMaxTimestampTest, PicksBatchWithHighestTimestamp) {
    test_dir_ = makeTestDir("highest");
    Log log("max-ts-highest", 0, test_dir_, asyncConfig());
    log.appendBatch(makeDataBatch(100, "a"));  // offset 0
    log.appendBatch(makeDataBatch(300, "b"));  // offset 1
    log.appendBatch(makeDataBatch(200, "c"));  // offset 2

    auto result = log.maxTimestampOffset();
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->first, 1);
    EXPECT_EQ(result->second, 300);
}

// Transaction COMMIT/ABORT markers carry wall-clock timestamps that are
// typically newer than any data record; ListOffsets MAX_TIMESTAMP must
// answer with a data record's offset, never a control marker's (KIP-734 /
// Kafka ListOffsetsRequest v7 semantics).
TEST_F(LogMaxTimestampTest, IgnoresControlBatches) {
    test_dir_ = makeTestDir("control");
    Log log("max-ts-control", 0, test_dir_, asyncConfig());
    log.appendBatch(makeDataBatch(1000, "data"));  // offset 0
    log.appendBatch(RecordBatch::makeControlBatch(/*producer_id=*/1,
                                                  /*producer_epoch=*/0,
                                                  /*base_offset=*/1,
                                                  /*committed=*/true,
                                                  /*timestamp_ms=*/9999));  // offset 1

    auto result = log.maxTimestampOffset();
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->first, 0) << "a control marker must not win the MAX_TIMESTAMP scan";
    EXPECT_EQ(result->second, 1000);
}

TEST_F(LogMaxTimestampTest, OnlyControlBatchesMeansNoMaxTimestamp) {
    test_dir_ = makeTestDir("only-control");
    Log log("max-ts-only-control", 0, test_dir_, asyncConfig());
    log.appendBatch(RecordBatch::makeControlBatch(1, 0, 0, true, 5000));

    EXPECT_FALSE(log.maxTimestampOffset().has_value());
}

}  // namespace kawasan::storage
