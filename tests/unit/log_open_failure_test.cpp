// LogManager::getOrCreateLog must never destroy partition data when a log
// fails to open. Environmental failures (e.g. the RocksDB LOCK already held)
// surface as errors with the data untouched; genuine corruption quarantines the
// directory (renamed, not deleted) before starting an empty log by default.
// Authoritative topics instead propagate every failure and require existing
// segment databases and a valid committed-prefix checkpoint.
#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include <rocksdb/options.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

#include "../sparse_record_batch.h"
#include "kawasan/common/logger.h"
#include "kawasan/common/rocksdb_compat.h"
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
    void TearDown() override {
        if (HasFailure()) {
            std::cerr << "Retained failed fixture: " << dir_ << '\n';
        } else {
            fs::remove_all(dir_);
        }
    }
    void authoritative(LogManager& manager) {
        manager.setAuthoritativeTopic("__transaction_state");
    }
    fs::path partition() const { return fs::path(dir_) / "__transaction_state-0"; }
    void seed(int count = 3) {
        LogManager manager(dir_);
        manager.getOrCreateLog("__transaction_state", 0)->append(records(count), true);
    }
    void expectUnregistered(LogManager& manager) {
        EXPECT_EQ(manager.getLog("__transaction_state", 0), nullptr);
        EXPECT_EQ(manager.openLogCount(), 0u);
        EXPECT_TRUE(quarantined(dir_).empty());
    }
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

TEST_F(LogOpenFailureTest, AuthoritativeMissingPartitionIsNotCreated) {
    LogManager manager(dir_);
    authoritative(manager);
    EXPECT_THROW(manager.getOrCreateLog("__transaction_state", 0), std::exception);
    EXPECT_FALSE(fs::exists(partition()));
    expectUnregistered(manager);
}

TEST_F(LogOpenFailureTest, AuthoritativeEmptyPartitionIsNotBootstrapped) {
    fs::create_directories(partition());
    LogManager manager(dir_);
    authoritative(manager);
    EXPECT_THROW(manager.getOrCreateLog("__transaction_state", 0), std::exception);
    EXPECT_TRUE(fs::is_empty(partition()));
    expectUnregistered(manager);
}

TEST_F(LogOpenFailureTest, AuthoritativeMissingSegmentDatabaseIsNotCreated) {
    seed();
    fs::remove_all(partition() / "0");
    fs::create_directory(partition() / "0");
    LogManager manager(dir_);
    authoritative(manager);
    EXPECT_THROW(manager.getOrCreateLog("__transaction_state", 0), std::exception);
    EXPECT_TRUE(fs::is_empty(partition() / "0"));
    expectUnregistered(manager);
}

TEST_F(LogOpenFailureTest, AuthoritativeCorruptLogIsNotQuarantinedOrReplaced) {
    seed();
    const auto current = partition() / "0" / "CURRENT";
    const std::string corrupt = "garbage-without-newline";
    {
        std::ofstream out(current, std::ios::binary);
        out << corrupt;
    }
    LogManager manager(dir_);
    authoritative(manager);
    EXPECT_THROW(manager.getOrCreateLog("__transaction_state", 0), std::exception);
    EXPECT_THROW(manager.getOrCreateLog("__transaction_state", 0), std::exception);
    std::ifstream in(current, std::ios::binary);
    const std::string remaining((std::istreambuf_iterator<char>(in)), {});
    EXPECT_EQ(remaining, corrupt);
    expectUnregistered(manager);
}

TEST_F(LogOpenFailureTest, AuthoritativeLockedLogPreservesDataAndError) {
    {
        LogManager owner(dir_);
        auto* log = owner.getOrCreateLog("__transaction_state", 0);
        log->append(records(3), true);
        LogManager intruder(dir_);
        authoritative(intruder);
        try {
            intruder.getOrCreateLog("__transaction_state", 0);
            FAIL() << "LOCK failure must propagate";
        } catch (const std::exception& ex) {
            EXPECT_NE(std::string(ex.what()).find("LOCK"), std::string::npos);
        }
        expectUnregistered(intruder);
        EXPECT_EQ(log->logEndOffset(), 3);
    }
    LogManager reopened(dir_);
    authoritative(reopened);
    EXPECT_EQ(reopened.getOrCreateLog("__transaction_state", 0)->logEndOffset(), 3);
}

TEST_F(LogOpenFailureTest, AuthoritativeReopenPreservesCommittedPrefixAndSupportsAppends) {
    {
        LogManager owner(dir_);
        auto* log = owner.getOrCreateLog("__transaction_state", 0);
        log->append(records(3), true);
        log->setHighWatermark(2);
    }
    LogManager manager(dir_);
    authoritative(manager);
    auto* log = manager.getOrCreateLog("__transaction_state", 0);
    ASSERT_NE(log, nullptr);
    EXPECT_EQ(log->logEndOffset(), 3);
    EXPECT_EQ(log->highWatermark(), 2);
    ASSERT_EQ(log->read(0, 10000).size(), 1u);
    EXPECT_EQ(log->read(0, 10000).front().records().size(), 3u);
    EXPECT_EQ(manager.getOrCreateLog("__transaction_state", 0), log);
    log->append(records(1), true);
    EXPECT_EQ(log->logEndOffset(), 4);
}

TEST_F(LogOpenFailureTest, AuthoritativeExistingEmptyDatabaseIsValid) {
    seed(0);
    LogManager manager(dir_);
    authoritative(manager);
    EXPECT_EQ(manager.getOrCreateLog("__transaction_state", 0)->logEndOffset(), 0);
    EXPECT_TRUE(quarantined(dir_).empty());
}

TEST_F(LogOpenFailureTest, AuthoritativeMissingCheckpointIsNotCommittedToLeo) {
    seed();
    fs::remove(partition() / "checkpoint.meta");
    LogManager manager(dir_);
    authoritative(manager);
    EXPECT_THROW(manager.getOrCreateLog("__transaction_state", 0), std::exception);
    EXPECT_FALSE(fs::exists(partition() / "checkpoint.meta"));
    expectUnregistered(manager);
}

TEST_F(LogOpenFailureTest, AuthoritativeMissingCurrentIsNotReinitialized) {
    seed();
    fs::remove(partition() / "0" / "CURRENT");
    LogManager manager(dir_);
    authoritative(manager);
    EXPECT_THROW(manager.getOrCreateLog("__transaction_state", 0), std::exception);
    EXPECT_FALSE(fs::exists(partition() / "0" / "CURRENT"));
    expectUnregistered(manager);
}

TEST_F(LogOpenFailureTest, AuthoritativeInvalidCheckpointIsNotRewritten) {
    seed();
    const std::vector<std::string> invalid = {
        "garbage",
        "log_start_offset=0\nlog_end_offset=3\n",
        "log_start_offset=0\nlog_end_offset=3\nhigh_watermark=4\n",
        "log_start_offset=0\nlog_end_offset=3\nhigh_watermark=-1\n",
        "log_start_offset=-1\nlog_end_offset=3\nhigh_watermark=2\n",
        "log_start_offset=0\nlog_end_offset=4\nhigh_watermark=2\n",
        "log_start_offset=0\nlog_end_offset=3\nhigh_watermark=2garbage\n",
        "log_start_offset=0\nlog_end_offset=3\nhigh_watermark=2\nhigh_watermark=3\n",
    };
    for (const auto& contents : invalid) {
        SCOPED_TRACE(contents);
        {
            std::ofstream out(partition() / "checkpoint.meta");
            out << contents;
        }
        LogManager manager(dir_);
        authoritative(manager);
        EXPECT_THROW(manager.getOrCreateLog("__transaction_state", 0), std::exception);
        std::ifstream in(partition() / "checkpoint.meta");
        EXPECT_EQ(std::string((std::istreambuf_iterator<char>(in)), {}), contents);
        expectUnregistered(manager);
    }
}

TEST_F(LogOpenFailureTest, AuthoritativePolicyCannotTrustPreviouslyOpenedLog) {
    LogManager manager(dir_);
    manager.getOrCreateLog("__transaction_state", 1);
    EXPECT_THROW(manager.setAuthoritativeTopic("__transaction_state"), std::exception);
    EXPECT_EQ(manager.openLogCount(), 1u);
}

TEST_F(LogOpenFailureTest, AuthoritativePolicyCoversAllPartitionsAndSurvivesClose) {
    seed();
    LogManager manager(dir_);
    authoritative(manager);
    auto* log = manager.getOrCreateLog("__transaction_state", 0);
    EXPECT_NE(log, nullptr);
    EXPECT_NO_THROW(authoritative(manager));
    EXPECT_THROW(manager.getOrCreateLog("__transaction_state", 1), std::exception);
    EXPECT_FALSE(fs::exists(fs::path(dir_) / "__transaction_state-1"));
    manager.closeAll();
    fs::remove(partition() / "0" / "CURRENT");
    EXPECT_THROW(manager.getOrCreateLog("__transaction_state", 0), std::exception);
    expectUnregistered(manager);
    EXPECT_NE(manager.getOrCreateLog("user", 0), nullptr);
}

TEST_F(LogOpenFailureTest, AuthoritativePolicyDoesNotRecoverHighWatermarkToLeo) {
    {
        LogManager owner(dir_);
        auto* log = owner.getOrCreateLog("__transaction_state", 0);
        log->append(records(3), true);
        log->setHighWatermark(1);
    }
    LogManager manager(dir_);
    manager.setRecoverHighWatermarkToLogEnd(true);
    authoritative(manager);
    EXPECT_EQ(manager.getOrCreateLog("__transaction_state", 0)->highWatermark(), 1);
}

TEST_F(LogOpenFailureTest, AuthoritativeStaleCheckpointKeepsUncommittedTail) {
    {
        LogManager owner(dir_);
        auto* log = owner.getOrCreateLog("__transaction_state", 0);
        log->append(records(3), true);
    }
    {
        std::ofstream out(partition() / "checkpoint.meta");
        out << "log_start_offset=0\nlog_end_offset=2\nhigh_watermark=1\n";
    }
    LogManager manager(dir_);
    authoritative(manager);
    auto* log = manager.getOrCreateLog("__transaction_state", 0);
    EXPECT_EQ(log->highWatermark(), 1);
    EXPECT_EQ(log->logEndOffset(), 3);
}

TEST_F(LogOpenFailureTest, AuthoritativeMalformedStoredBatchIsNotReplaced) {
    seed();
    const auto bytes = kawasan::test_support::sparseBatch(0, -2).serialize();
    const std::string key(8, '\0');
    const auto path = (partition() / "0").string();
    {
        rocksdb::Options options;
        std::unique_ptr<rocksdb::DB> db;
        ASSERT_TRUE(kawasan::openRocksDb(options, path, db).ok());
        rocksdb::WriteOptions write;
        write.sync = true;
        ASSERT_TRUE(
            db->Put(write, key,
                    rocksdb::Slice(reinterpret_cast<const char*>(bytes.data()), bytes.size()))
                .ok());
    }
    LogManager manager(dir_);
    authoritative(manager);
    EXPECT_THROW(manager.getOrCreateLog("__transaction_state", 0), std::exception);
    expectUnregistered(manager);
    rocksdb::Options options;
    std::unique_ptr<rocksdb::DB> db;
    ASSERT_TRUE(kawasan::openRocksDb(options, path, db).ok());
    std::string remaining;
    ASSERT_TRUE(db->Get(rocksdb::ReadOptions{}, key, &remaining).ok());
    EXPECT_EQ(remaining, std::string(bytes.begin(), bytes.end()));
}

TEST_F(LogOpenFailureTest, DisposableOffsetCacheCannotBootstrapMissingAuthoritativeSource) {
    // A local cache has no bearing on whether an authoritative replica exists.
    fs::create_directories(fs::path(dir_) / "offset-cache");
    {
        std::ofstream cache(fs::path(dir_) / "offset-cache" / "checkpoint");
        cache << "group-offset=42";
    }
    LogManager manager(dir_);
    manager.setAuthoritativeTopic("__consumer_offsets");
    EXPECT_THROW(manager.getOrCreateLog("__consumer_offsets", 0), std::exception);
    EXPECT_FALSE(fs::exists(fs::path(dir_) / "__consumer_offsets-0"));
    EXPECT_EQ(manager.openLogCount(), 0u);
    EXPECT_TRUE(quarantined(dir_).empty());
    EXPECT_TRUE(fs::exists(fs::path(dir_) / "offset-cache" / "checkpoint"));
}
