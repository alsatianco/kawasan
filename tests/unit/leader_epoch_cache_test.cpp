// M8-F1: KIP-101 leader-epoch cache — the per-partition history of which leader
// epoch started at which offset. Followers use it (via OffsetForLeaderEpoch) to
// find exactly where their log diverged from a new leader's after failovers.
#include "kawasan/storage/leader_epoch_cache.h"

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <fstream>

#include "kawasan/common/logger.h"
#include "kawasan/storage/log.h"

namespace fs = std::filesystem;
using kawasan::storage::EpochEntry;
using kawasan::storage::LeaderEpochCache;

namespace {

class LeaderEpochCacheTest : public ::testing::Test {
protected:
    void SetUp() override {
        static const bool logger = [] {
            kawasan::Logger::init("warn");
            return true;
        }();
        (void)logger;
        const auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
        dir_ = (fs::temp_directory_path() / ("kawasan-epoch-cache-" + std::to_string(ts))).string();
        fs::create_directories(dir_);
        path_ = dir_ + "/leader-epoch-checkpoint";
    }
    void TearDown() override { fs::remove_all(dir_); }

    std::vector<EpochEntry> entries(const LeaderEpochCache& c) { return c.entries(); }

    std::string dir_;
    std::string path_;
};

}  // namespace

TEST_F(LeaderEpochCacheTest, StartsEmpty) {
    LeaderEpochCache cache(path_);
    EXPECT_FALSE(cache.latestEpoch().has_value());
    EXPECT_EQ(cache.endOffsetFor(0, 100), (std::pair<int32_t, kawasan::Offset>{-1, -1}));
}

TEST_F(LeaderEpochCacheTest, AssignIsAppendOnlyAndIgnoresStaleOrDuplicateEpochs) {
    LeaderEpochCache cache(path_);
    cache.assign(1, 0);
    cache.assign(1, 5);   // duplicate epoch: keeps the first start
    cache.assign(3, 10);
    cache.assign(2, 12);  // stale epoch: ignored
    cache.assign(-1, 20);  // undefined: ignored
    EXPECT_EQ(entries(cache), (std::vector<EpochEntry>{{1, 0}, {3, 10}}));
    EXPECT_EQ(cache.latestEpoch(), std::optional<int32_t>(3));
}

TEST_F(LeaderEpochCacheTest, NewEpochAtSameOffsetReplacesEmptyEpochs) {
    LeaderEpochCache cache(path_);
    cache.assign(1, 0);
    cache.assign(2, 10);
    cache.assign(3, 10);  // epoch 2 never wrote anything
    EXPECT_EQ(entries(cache), (std::vector<EpochEntry>{{1, 0}, {3, 10}}));
}

// KIP-101 lookup: largest cached epoch <= requested; its end is the next cached
// epoch's start, or the log end for the latest epoch.
TEST_F(LeaderEpochCacheTest, EndOffsetFor) {
    LeaderEpochCache cache(path_);
    cache.assign(1, 0);
    cache.assign(3, 10);
    cache.assign(5, 25);
    const kawasan::Offset leo = 40;
    EXPECT_EQ(cache.endOffsetFor(1, leo), (std::pair<int32_t, kawasan::Offset>{1, 10}));
    EXPECT_EQ(cache.endOffsetFor(2, leo), (std::pair<int32_t, kawasan::Offset>{1, 10}));
    EXPECT_EQ(cache.endOffsetFor(3, leo), (std::pair<int32_t, kawasan::Offset>{3, 25}));
    EXPECT_EQ(cache.endOffsetFor(4, leo), (std::pair<int32_t, kawasan::Offset>{3, 25}));
    EXPECT_EQ(cache.endOffsetFor(5, leo), (std::pair<int32_t, kawasan::Offset>{5, 40}));
    // Newer than anything we know: undefined.
    EXPECT_EQ(cache.endOffsetFor(6, leo), (std::pair<int32_t, kawasan::Offset>{-1, -1}));
    // Older than the earliest cached epoch: that epoch ends where the earliest starts.
    LeaderEpochCache later(dir_ + "/other");
    later.assign(4, 7);
    EXPECT_EQ(later.endOffsetFor(2, leo), (std::pair<int32_t, kawasan::Offset>{2, 7}));
    EXPECT_EQ(cache.endOffsetFor(-1, leo), (std::pair<int32_t, kawasan::Offset>{-1, -1}));
}

TEST_F(LeaderEpochCacheTest, TruncateFromEndDropsEpochsStartingAtOrAfterTheEnd) {
    LeaderEpochCache cache(path_);
    cache.assign(1, 0);
    cache.assign(3, 10);
    cache.assign(5, 25);
    cache.truncateFromEnd(25);
    EXPECT_EQ(entries(cache), (std::vector<EpochEntry>{{1, 0}, {3, 10}}));
    cache.truncateFromEnd(11);
    EXPECT_EQ(entries(cache), (std::vector<EpochEntry>{{1, 0}, {3, 10}}));
    cache.truncateFromEnd(0);
    EXPECT_TRUE(entries(cache).empty());
}

TEST_F(LeaderEpochCacheTest, TruncateFromStartClampsTheOldestLiveEpoch) {
    LeaderEpochCache cache(path_);
    cache.assign(1, 0);
    cache.assign(3, 10);
    cache.assign(5, 25);
    cache.truncateFromStart(12);  // epoch 1 is gone; epoch 3 now starts at 12
    EXPECT_EQ(entries(cache), (std::vector<EpochEntry>{{3, 12}, {5, 25}}));
    cache.truncateFromStart(5);  // below the earliest: no change
    EXPECT_EQ(entries(cache), (std::vector<EpochEntry>{{3, 12}, {5, 25}}));
}

TEST_F(LeaderEpochCacheTest, PersistsAndReloadsAtomically) {
    {
        LeaderEpochCache cache(path_);
        cache.assign(2, 0);
        cache.assign(4, 9);
    }
    LeaderEpochCache reloaded(path_);
    EXPECT_EQ(entries(reloaded), (std::vector<EpochEntry>{{2, 0}, {4, 9}}));
    for (const auto& e : fs::directory_iterator(dir_)) {
        EXPECT_NE(e.path().extension(), ".tmp");
    }
}

TEST_F(LeaderEpochCacheTest, CorruptCheckpointStartsEmpty) {
    std::ofstream(path_) << "0\n3\n1 0\ngarbage\n";
    LeaderEpochCache cache(path_);
    EXPECT_TRUE(entries(cache).empty());
    cache.assign(1, 0);  // and it is usable
    EXPECT_EQ(cache.latestEpoch(), std::optional<int32_t>(1));
}

// Log integration: the cache lives in the partition directory and follows the
// log's truncations.
TEST_F(LeaderEpochCacheTest, LogOwnsCacheAndTruncationsMaintainIt) {
    auto batch = [](int n) {
        kawasan::storage::RecordBatch b;
        b.setMagic(2);
        for (int i = 0; i < n; ++i) {
            kawasan::Record r;
            r.value = std::vector<uint8_t>{'x'};
            b.addRecord(r);
        }
        return b;
    };
    {
        kawasan::storage::Log log("t", 0, dir_);
        log.assignLeaderEpochStart(1, 0);
        log.appendBatch(batch(5));
        log.assignLeaderEpochStart(2, log.logEndOffset());
        log.appendBatch(batch(5));
        EXPECT_EQ(log.latestLeaderEpoch(), std::optional<int32_t>(2));
        EXPECT_EQ(log.epochEndOffset(1), (std::pair<int32_t, kawasan::Offset>{1, 5}));
        EXPECT_EQ(log.epochEndOffset(2), (std::pair<int32_t, kawasan::Offset>{2, 10}));
        log.truncateSuffix(5);
        EXPECT_EQ(log.latestLeaderEpoch(), std::optional<int32_t>(1));
        EXPECT_EQ(log.epochEndOffset(1), (std::pair<int32_t, kawasan::Offset>{1, 5}));
        log.close();
    }
    kawasan::storage::Log reopened("t", 0, dir_);
    EXPECT_EQ(reopened.latestLeaderEpoch(), std::optional<int32_t>(1));
    EXPECT_TRUE(fs::exists(dir_ + "/leader-epoch-checkpoint"));
}
