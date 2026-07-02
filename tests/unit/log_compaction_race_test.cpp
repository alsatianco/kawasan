#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <map>
#include <string>
#include <thread>
#include <vector>

#include "kawasan/common/logger.h"
#include "kawasan/storage/log.h"
#include "kawasan/storage/log_segment.h"
#include "kawasan/storage/record_batch.h"

namespace kawasan::storage {

namespace {

std::string makeTestDir(const std::string& tag) {
    const auto timestamp = std::chrono::steady_clock::now().time_since_epoch().count();
    auto tmp = std::filesystem::temp_directory_path() /
               ("kawasan-compaction-race-" + tag + "-" + std::to_string(timestamp));
    std::filesystem::create_directories(tmp);
    return tmp.string();
}

Record makeKeyedRecord(const std::string& key, const std::string& value) {
    Record record;
    record.timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
                           std::chrono::system_clock::now().time_since_epoch())
                           .count();
    record.key = std::vector<uint8_t>(key.begin(), key.end());
    record.value = std::vector<uint8_t>(value.begin(), value.end());
    return record;
}

RecordBatch makeKeyedBatch(Offset base_offset, const std::string& key, const std::string& value) {
    RecordBatch batch;
    batch.setBaseOffset(base_offset);
    batch.addRecord(makeKeyedRecord(key, value));
    return batch;
}

void ensureLoggerInitialized() {
    static bool initialized = false;
    if (!initialized) {
        Logger::init("warn");
        initialized = true;
    }
}

std::string valueToString(const Record& record) {
    if (!record.value)
        return {};
    return std::string(record.value->begin(), record.value->end());
}

}  // namespace

class LogCompactionRaceTest : public ::testing::Test {
protected:
    void SetUp() override { ensureLoggerInitialized(); }

    void TearDown() override {
        if (!test_dir_.empty() && std::filesystem::exists(test_dir_)) {
            std::filesystem::remove_all(test_dir_);
        }
    }

    std::string test_dir_;
};

// The deletion primitive itself must refuse to touch the active segment.
// Log::cleanup()'s pass-2 loop bound skips the active segment today, but that
// is an invariant enforced by one caller's loop shape; any other caller of
// deleteBatchAt (future log-start-offset trimming, refactors) would corrupt
// in-flight produce data silently.
TEST_F(LogCompactionRaceTest, ActiveSegmentRefusesDeleteBatchAt) {
    test_dir_ = makeTestDir("guard");
    LogSegment segment(0, test_dir_ + "/0");
    segment.append(makeKeyedBatch(0, "k", "v1"), /*sync=*/false);

    segment.setActive(true);
    EXPECT_TRUE(segment.isActive());
    EXPECT_FALSE(segment.deleteBatchAt(0))
        << "deleteBatchAt must refuse to delete from the active segment";
    ASSERT_TRUE(segment.read(0).has_value()) << "the batch must survive a refused delete";

    segment.setActive(false);
    EXPECT_FALSE(segment.isActive());
    EXPECT_TRUE(segment.deleteBatchAt(0)) << "a rolled (inactive) segment must remain compactable";
    EXPECT_FALSE(segment.read(0).has_value());
}

// Every batch in the active segment survives compaction, superseded or not —
// Kafka's immutable-once-rolled model: only rolled segments are cleanable.
TEST_F(LogCompactionRaceTest, CompactionPreservesSupersededBatchesInActiveSegment) {
    test_dir_ = makeTestDir("active");
    LogConfig config;
    config.cleanup_policy_compact = true;
    config.cleanup_policy_delete = false;
    config.flush_mode = FlushMode::kAsync;  // default 1GB segment: no rolls

    Log log("compaction-active", 0, test_dir_, config);
    log.append({makeKeyedRecord("k", "v1")});  // offset 0, active segment
    log.append({makeKeyedRecord("k", "v2")});  // offset 1, same (active) segment

    log.cleanup();

    std::map<Offset, std::string> surviving;
    for (const auto& batch : log.read(0, std::numeric_limits<size_t>::max())) {
        for (size_t i = 0; i < batch.records().size(); ++i) {
            surviving[batch.baseOffset() + static_cast<Offset>(i)] =
                valueToString(batch.records()[i]);
        }
    }

    ASSERT_EQ(surviving.size(), 2u) << "no batch in the active segment may be compacted away";
    EXPECT_EQ(surviving[0], "v1");
    EXPECT_EQ(surviving[1], "v2");
}

// The guard must not over-block: superseded batches in rolled (inactive)
// segments are still reclaimed.
TEST_F(LogCompactionRaceTest, CompactionDropsSupersededBatchesInRolledSegments) {
    test_dir_ = makeTestDir("rolled");
    LogConfig config;
    config.cleanup_policy_compact = true;
    config.cleanup_policy_delete = false;
    config.segment_size = 1;  // roll before every append after the first
    config.flush_mode = FlushMode::kAsync;

    Log log("compaction-rolled", 0, test_dir_, config);
    log.append({makeKeyedRecord("k", "v1")});  // offset 0, segment 0
    log.append({makeKeyedRecord("k", "v2")});  // offset 1, rolls: segment 1
    log.append({makeKeyedRecord("k", "v3")});  // offset 2, rolls: segment 2 (active)

    log.cleanup();

    std::map<Offset, std::string> surviving;
    for (const auto& batch : log.read(0, std::numeric_limits<size_t>::max())) {
        for (size_t i = 0; i < batch.records().size(); ++i) {
            surviving[batch.baseOffset() + static_cast<Offset>(i)] =
                valueToString(batch.records()[i]);
        }
    }

    EXPECT_EQ(surviving.count(0), 0u) << "superseded batch in rolled segment must be compacted";
    EXPECT_EQ(surviving.count(1), 0u) << "superseded batch in rolled segment must be compacted";
    ASSERT_EQ(surviving.count(2), 1u) << "latest value must always survive";
    EXPECT_EQ(surviving[2], "v3");
    EXPECT_EQ(log.logEndOffset(), 3);
}

// Regression net: compaction loops concurrently with a producer forcing
// segment rolls. Serialization is Log::mutex_'s job; the invariants are that
// the latest value per key is always readable afterwards and the log end
// offset accounts for every append.
TEST_F(LogCompactionRaceTest, ConcurrentCompactionProduceStress) {
    test_dir_ = makeTestDir("stress");
    LogConfig config;
    config.cleanup_policy_compact = true;
    config.cleanup_policy_delete = false;
    config.segment_size = 4096;  // roll every ~40 single-record batches
    config.flush_mode = FlushMode::kAsync;

    Log log("compaction-stress", 0, test_dir_, config);

    constexpr int kAppends = 400;
    constexpr int kKeys = 5;
    std::atomic<bool> done{false};

    std::thread cleaner([&] {
        while (!done.load()) {
            log.cleanup();
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    });

    for (int i = 0; i < kAppends; ++i) {
        const std::string key = "key-" + std::to_string(i % kKeys);
        log.append({makeKeyedRecord(key, "value-" + std::to_string(i))});
    }
    done.store(true);
    cleaner.join();
    log.cleanup();

    ASSERT_EQ(log.logEndOffset(), kAppends);

    std::map<std::string, std::pair<Offset, std::string>> latest;
    for (const auto& batch : log.read(0, std::numeric_limits<size_t>::max())) {
        for (size_t i = 0; i < batch.records().size(); ++i) {
            const auto& record = batch.records()[i];
            ASSERT_TRUE(record.key.has_value());
            std::string key(record.key->begin(), record.key->end());
            const Offset offset = batch.baseOffset() + static_cast<Offset>(i);
            auto it = latest.find(key);
            if (it == latest.end() || offset > it->second.first) {
                latest[key] = {offset, valueToString(record)};
            }
        }
    }

    ASSERT_EQ(latest.size(), static_cast<size_t>(kKeys));
    for (int k = 0; k < kKeys; ++k) {
        const std::string key = "key-" + std::to_string(k);
        // The last append for key k is the highest i < kAppends with i % kKeys == k.
        const int last_i = kAppends - kKeys + k;
        ASSERT_TRUE(latest.count(key)) << key;
        EXPECT_EQ(latest[key].second, "value-" + std::to_string(last_i)) << key;
    }
}

}  // namespace kawasan::storage
