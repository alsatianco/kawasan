// LogManager::getOrCreateLog must never destroy partition data when a log
// fails to open. Environmental failures (e.g. the RocksDB LOCK already held)
// surface as errors with the data untouched; genuine corruption quarantines the
// directory (renamed, not deleted) before starting an empty log.
#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include "kawasan/common/logger.h"
#include "kawasan/storage/log_manager.h"

namespace fs = std::filesystem;
using kawasan::storage::LogConfig;
using kawasan::storage::LogManager;

namespace {

std::string makeDir() {
    const auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
    auto p = fs::temp_directory_path() / ("kawasan-log-open-" + std::to_string(ts));
    fs::create_directories(p);
    return p.string();
}

std::vector<kawasan::Record> records(int n) {
    std::vector<kawasan::Record> out;
    for (int i = 0; i < n; ++i) {
        kawasan::Record r;
        r.timestamp = 0;
        r.value = std::vector<uint8_t>{static_cast<uint8_t>('a' + i)};
        out.push_back(r);
    }
    return out;
}

std::vector<fs::path> quarantined(const std::string& base) {
    std::vector<fs::path> out;
    for (const auto& e : fs::directory_iterator(base)) {
        if (e.path().filename().string().find(".corrupt-") != std::string::npos) {
            out.push_back(e.path());
        }
    }
    return out;
}

class LogOpenFailureTest : public ::testing::Test {
protected:
    void SetUp() override {
        static bool logger_ready = false;
        if (!logger_ready) {
            kawasan::Logger::init("warn");
            logger_ready = true;
        }
        dir_ = makeDir();
    }
    void TearDown() override { fs::remove_all(dir_); }
    std::string dir_;
};

}  // namespace

TEST_F(LogOpenFailureTest, LockedLogIsNotWipedAndErrorSurfaces) {
    {
        LogManager owner(dir_, LogConfig{});
        auto* log = owner.getOrCreateLog("t", 0);
        ASSERT_NE(log, nullptr);
        log->append(records(3), true);
        ASSERT_EQ(log->logEndOffset(), 3);

        // Same directory opened while the owner still holds the RocksDB lock.
        LogManager intruder(dir_, LogConfig{});
        EXPECT_THROW(intruder.getOrCreateLog("t", 0), std::exception);
        EXPECT_TRUE(quarantined(dir_).empty());

        // The owner's open log is still fully usable.
        log->append(records(1), true);
        EXPECT_EQ(log->logEndOffset(), 4);
    }
    LogManager reopened(dir_, LogConfig{});
    auto* log = reopened.getOrCreateLog("t", 0);
    ASSERT_NE(log, nullptr);
    EXPECT_EQ(log->logEndOffset(), 4);
}

TEST_F(LogOpenFailureTest, CorruptLogIsQuarantinedNotDeleted) {
    const fs::path segment = fs::path(dir_) / "t-0" / "0";
    fs::create_directories(segment);
    {
        std::ofstream current(segment / "CURRENT", std::ios::binary);
        current << "garbage-without-newline";
    }

    LogManager manager(dir_, LogConfig{});
    auto* log = manager.getOrCreateLog("t", 0);
    ASSERT_NE(log, nullptr);
    EXPECT_EQ(log->logEndOffset(), 0);

    auto q = quarantined(dir_);
    ASSERT_EQ(q.size(), 1u);
    EXPECT_TRUE(fs::exists(q[0] / "0" / "CURRENT"));
}
