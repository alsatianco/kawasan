#include "kawasan/streams/streams_builder.h"
#include "kawasan/streams/windows.h"
#include "kawasan/streams/timestamp_extractor.h"
#include "kawasan/streams/processor_context.h"
#include "kawasan/streams/window_store.h"
#include "kawasan/streams/stores.h"
#include <gtest/gtest.h>
#include <chrono>
#include <filesystem>
#include <string>

using namespace kawasan::streams;
using namespace std::chrono;

// ============================================================================
// TimeWindows Tests
// ============================================================================

class TimeWindowsTest : public ::testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(TimeWindowsTest, TumblingWindowCreation) {
    auto windows = TimeWindows::of(seconds(5));

    EXPECT_EQ(windows.sizeMs(), 5000);
    EXPECT_EQ(windows.advanceMs(), 5000);  // Tumbling: advance == size
    EXPECT_EQ(windows.graceMs(), 0);
}

TEST_F(TimeWindowsTest, HoppingWindowCreation) {
    auto windows = TimeWindows::of(seconds(10)).advanceBy(seconds(5));

    EXPECT_EQ(windows.sizeMs(), 10000);
    EXPECT_EQ(windows.advanceMs(), 5000);  // Hopping: advance < size
    EXPECT_EQ(windows.graceMs(), 0);
}

TEST_F(TimeWindowsTest, WindowWithGrace) {
    auto windows = TimeWindows::of(seconds(5)).grace(seconds(2));

    EXPECT_EQ(windows.sizeMs(), 5000);
    EXPECT_EQ(windows.graceMs(), 2000);
}

TEST_F(TimeWindowsTest, TumblingWindowsForTimestamp) {
    auto windows = TimeWindows::of(seconds(5));  // 5-second tumbling windows

    // Timestamp 7000ms should be in window [5000, 10000)
    auto result = windows.windowsForTimestamp(7000);
    ASSERT_EQ(result.size(), 1);
    EXPECT_EQ(result[0].startTime(), 5000);
    EXPECT_EQ(result[0].endTime(), 10000);
}

TEST_F(TimeWindowsTest, HoppingWindowsForTimestamp) {
    // 10-second windows advancing every 5 seconds
    auto windows = TimeWindows::of(seconds(10)).advanceBy(seconds(5));

    // Timestamp 7000ms should be in windows:
    // [0, 10000) and [5000, 15000)
    auto result = windows.windowsForTimestamp(7000);
    ASSERT_EQ(result.size(), 2);

    // Check both windows contain the timestamp
    bool foundFirst = false, foundSecond = false;
    for (const auto& w : result) {
        if (w.startTime() == 0 && w.endTime() == 10000) foundFirst = true;
        if (w.startTime() == 5000 && w.endTime() == 15000) foundSecond = true;
    }
    EXPECT_TRUE(foundFirst);
    EXPECT_TRUE(foundSecond);
}

// ============================================================================
// SessionWindows Tests
// ============================================================================

class SessionWindowsTest : public ::testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(SessionWindowsTest, SessionWindowCreation) {
    auto windows = SessionWindows::with(seconds(30));

    EXPECT_EQ(windows.inactivityGapMs(), 30000);
    EXPECT_EQ(windows.graceMs(), 0);
}

TEST_F(SessionWindowsTest, SessionWindowWithGrace) {
    auto windows = SessionWindows::with(seconds(30)).grace(seconds(5));

    EXPECT_EQ(windows.inactivityGapMs(), 30000);
    EXPECT_EQ(windows.graceMs(), 5000);
}

TEST_F(SessionWindowsTest, ShouldMergeAdjacentSessions) {
    auto windows = SessionWindows::with(seconds(10));

    // Two sessions within 10 seconds of each other should merge
    Window session1(1000, 5000);
    Window session2(10000, 15000);  // Gap is 5 seconds (< 10 second gap)

    EXPECT_TRUE(windows.shouldMerge(session1, session2));
}

TEST_F(SessionWindowsTest, ShouldNotMergeSeparateSessions) {
    auto windows = SessionWindows::with(seconds(10));

    // Two sessions more than 10 seconds apart should not merge
    Window session1(1000, 5000);
    Window session2(20000, 25000);  // Gap is 15 seconds (> 10 second gap)

    EXPECT_FALSE(windows.shouldMerge(session1, session2));
}

TEST_F(SessionWindowsTest, MergeSessions) {
    auto windows = SessionWindows::with(seconds(10));

    Window session1(1000, 5000);
    Window session2(8000, 12000);

    auto merged = windows.merge(session1, session2);

    EXPECT_EQ(merged.startTime(), 1000);
    EXPECT_EQ(merged.endTime(), 12000);
}

// ============================================================================
// Window class Tests
// ============================================================================

class WindowTest : public ::testing::Test {};

TEST_F(WindowTest, WindowContainsTimestamp) {
    Window w(1000, 5000);  // [1000, 5000)

    EXPECT_FALSE(w.contains(999));   // Before start
    EXPECT_TRUE(w.contains(1000));   // At start (inclusive)
    EXPECT_TRUE(w.contains(3000));   // In middle
    EXPECT_FALSE(w.contains(5000));  // At end (exclusive)
    EXPECT_FALSE(w.contains(6000));  // After end
}

TEST_F(WindowTest, WindowOverlaps) {
    Window w1(0, 1000);
    Window w2(500, 1500);
    Window w3(1000, 2000);

    EXPECT_TRUE(w1.overlaps(w2));   // Overlapping
    EXPECT_FALSE(w1.overlaps(w3)); // Adjacent, not overlapping
}

// ============================================================================
// JoinWindows Tests
// ============================================================================

class JoinWindowsTest : public ::testing::Test {};

TEST_F(JoinWindowsTest, SymmetricJoinWindow) {
    auto windows = JoinWindows::of(seconds(5));

    EXPECT_EQ(windows.beforeMs(), -5000);
    EXPECT_EQ(windows.afterMs(), 5000);
}

TEST_F(JoinWindowsTest, IsWithinWindow) {
    auto windows = JoinWindows::of(seconds(5));

    // Timestamp 10000, other within +/- 5 seconds
    EXPECT_TRUE(windows.isWithinWindow(10000, 10000));   // Same time
    EXPECT_TRUE(windows.isWithinWindow(10000, 5001));    // 4999ms before
    EXPECT_TRUE(windows.isWithinWindow(10000, 14999));   // 4999ms after
    EXPECT_FALSE(windows.isWithinWindow(10000, 4000));   // 6000ms before
    EXPECT_FALSE(windows.isWithinWindow(10000, 16000));  // 6000ms after
}

// ============================================================================
// TimestampExtractor Tests
// ============================================================================

class TimestampExtractorTest : public ::testing::Test {};

TEST_F(TimestampExtractorTest, FailOnInvalidTimestamp) {
    FailOnInvalidTimestamp extractor;

    ConsumerRecord record("topic", 0, 0, 12345, "key", "value");
    EXPECT_EQ(extractor.extract(record, -1), 12345);

    // Invalid timestamp should throw
    ConsumerRecord invalidRecord("topic", 0, 0, -1, "key", "value");
    EXPECT_THROW(extractor.extract(invalidRecord, -1), std::runtime_error);
}

TEST_F(TimestampExtractorTest, LogAndSkipOnInvalidTimestamp) {
    LogAndSkipOnInvalidTimestamp extractor;

    // Valid timestamp
    ConsumerRecord record("topic", 0, 0, 12345, "key", "value");
    EXPECT_EQ(extractor.extract(record, -1), 12345);

    // Invalid timestamp - should return partition time
    ConsumerRecord invalidRecord("topic", 0, 0, -1, "key", "value");
    EXPECT_EQ(extractor.extract(invalidRecord, 10000), 10000);

    // Invalid timestamp and no partition time - should return 0
    EXPECT_EQ(extractor.extract(invalidRecord, -1), 0);
}

TEST_F(TimestampExtractorTest, WallclockTimestampExtractor) {
    WallclockTimestampExtractor extractor;

    ConsumerRecord record("topic", 0, 0, 12345, "key", "value");

    auto before = duration_cast<milliseconds>(
        system_clock::now().time_since_epoch()).count();

    auto ts = extractor.extract(record, -1);

    auto after = duration_cast<milliseconds>(
        system_clock::now().time_since_epoch()).count();

    // Should be current wall clock time (within the test window)
    EXPECT_GE(ts, before);
    EXPECT_LE(ts, after);
}

// ============================================================================
// ProcessorContext Tests
// ============================================================================

class ProcessorContextTest : public ::testing::Test {
protected:
    ProcessorContext context;
};

TEST_F(ProcessorContextTest, SetAndGetRecordMetadata) {
    context.setRecordMetadata("test-topic", 2, 100, 12345);

    EXPECT_EQ(context.topic(), "test-topic");
    EXPECT_EQ(context.partition(), 2);
    EXPECT_EQ(context.offset(), 100);
    EXPECT_EQ(context.timestamp(), 12345);
}

TEST_F(ProcessorContextTest, StreamTimeAdvances) {
    context.setRecordMetadata("topic", 0, 0, 1000);
    EXPECT_EQ(context.currentStreamTimeMs(), 1000);

    context.setRecordMetadata("topic", 0, 1, 2000);
    EXPECT_EQ(context.currentStreamTimeMs(), 2000);

    // Older timestamp shouldn't decrease stream time
    context.setRecordMetadata("topic", 0, 2, 1500);
    EXPECT_EQ(context.currentStreamTimeMs(), 2000);
}

TEST_F(ProcessorContextTest, SchedulePunctuation) {
    int callCount = 0;
    int64_t lastTimestamp = 0;

    auto cancellable = context.schedule(
        milliseconds(100),
        PunctuationType::STREAM_TIME,
        [&](int64_t ts) {
            callCount++;
            lastTimestamp = ts;
        });

    EXPECT_NE(cancellable, nullptr);

    // Advance stream time and fire punctuations
    context.maybeFirePunctuations(150, 0);
    EXPECT_EQ(callCount, 1);
    EXPECT_EQ(lastTimestamp, 100);

    // Further advance
    context.maybeFirePunctuations(250, 0);
    EXPECT_EQ(callCount, 2);
    EXPECT_EQ(lastTimestamp, 200);
}

TEST_F(ProcessorContextTest, CancelPunctuation) {
    int callCount = 0;

    auto cancellable = context.schedule(
        milliseconds(100),
        PunctuationType::STREAM_TIME,
        [&](int64_t) { callCount++; });

    // Cancel before it fires
    cancellable->cancel();

    context.maybeFirePunctuations(150, 0);
    EXPECT_EQ(callCount, 0);  // Should not have been called
}

// ============================================================================
// Window Store Tests
// ============================================================================

class WindowStoreTest : public ::testing::Test {
protected:
    std::string test_dir;
    std::shared_ptr<RocksDBWindowStore<std::string, int64_t>> store;

    void SetUp() override {
        test_dir = "/tmp/kawasan-window-store-test-" +
                   std::to_string(std::chrono::system_clock::now()
                                      .time_since_epoch().count());
        std::filesystem::create_directories(test_dir);

        store = std::make_shared<RocksDBWindowStore<std::string, int64_t>>(
            "test-window-store",
            test_dir,
            60000,  // 60 second retention
            std::make_shared<StringSerde>(),
            std::make_shared<LongSerde>(),
            false);

        store->init();
    }

    void TearDown() override {
        if (store && store->isOpen()) {
            store->close();
        }
        std::filesystem::remove_all(test_dir);
    }
};

TEST_F(WindowStoreTest, PutAndFetch) {
    store->put("key1", 100, 1000);
    store->put("key1", 200, 2000);
    store->put("key2", 300, 1500);

    auto val = store->fetch("key1", 1000);
    ASSERT_TRUE(val.has_value());
    EXPECT_EQ(*val, 100);

    val = store->fetch("key1", 2000);
    ASSERT_TRUE(val.has_value());
    EXPECT_EQ(*val, 200);

    val = store->fetch("key2", 1500);
    ASSERT_TRUE(val.has_value());
    EXPECT_EQ(*val, 300);

    // Non-existent
    val = store->fetch("key1", 3000);
    EXPECT_FALSE(val.has_value());
}

TEST_F(WindowStoreTest, FetchRange) {
    store->put("key1", 100, 1000);
    store->put("key1", 200, 2000);
    store->put("key1", 300, 3000);
    store->put("key1", 400, 4000);

    // Fetch range [1500, 3500]
    auto iter = store->fetch("key1", 1500, 3500);

    std::vector<std::pair<int64_t, int64_t>> results;
    while (iter->hasNext()) {
        results.push_back(iter->next());
    }
    iter->close();

    ASSERT_EQ(results.size(), 2);
    // Results should be for timestamps 2000 and 3000
    EXPECT_EQ(results[0].first, 2000);
    EXPECT_EQ(results[0].second, 200);
    EXPECT_EQ(results[1].first, 3000);
    EXPECT_EQ(results[1].second, 300);
}

TEST_F(WindowStoreTest, ObservedStreamTime) {
    EXPECT_EQ(store->observedStreamTime(), 0);

    store->put("key1", 100, 5000);
    EXPECT_EQ(store->observedStreamTime(), 5000);

    store->put("key1", 200, 3000);  // Earlier timestamp
    EXPECT_EQ(store->observedStreamTime(), 5000);  // Should not decrease

    store->put("key1", 300, 7000);
    EXPECT_EQ(store->observedStreamTime(), 7000);
}

// ============================================================================
// Windowed Aggregation Topology Tests
// ============================================================================

class WindowedAggregationTest : public ::testing::Test {
protected:
    StreamsBuilder builder;
};

TEST_F(WindowedAggregationTest, TimeWindowedCount) {
    auto stream = builder.stream<std::string, std::string>("input-topic");

    // Create a windowed count
    stream.groupByKey()
          .windowedBy(TimeWindows::of(seconds(5)))
          .count();

    auto topology = builder.build();
    auto& nodes = topology.nodes();

    // Should have: SOURCE -> GROUP_BY -> TIME_WINDOW_COUNT
    EXPECT_GE(nodes.size(), 2);

    // Verify window count node exists
    bool hasWindowCount = false;
    for (const auto& [name, node] : nodes) {
        if (name.find("TIME_WINDOW_COUNT") != std::string::npos) {
            hasWindowCount = true;
            break;
        }
    }
    EXPECT_TRUE(hasWindowCount);
}

TEST_F(WindowedAggregationTest, SessionWindowedCount) {
    auto stream = builder.stream<std::string, std::string>("input-topic");

    // Create a session-windowed count
    stream.groupByKey()
          .windowedBy(SessionWindows::with(seconds(30)))
          .count();

    auto topology = builder.build();
    auto& nodes = topology.nodes();

    // Should have: SOURCE -> GROUP_BY -> SESSION_WINDOW_COUNT
    EXPECT_GE(nodes.size(), 2);

    // Verify session window count node exists
    bool hasSessionCount = false;
    for (const auto& [name, node] : nodes) {
        if (name.find("SESSION_WINDOW_COUNT") != std::string::npos) {
            hasSessionCount = true;
            break;
        }
    }
    EXPECT_TRUE(hasSessionCount);
}

TEST_F(WindowedAggregationTest, HoppingWindowAggregate) {
    auto stream = builder.stream<std::string, int64_t>("input-topic");

    // Create hopping window aggregate (10s windows, 5s advance)
    stream.groupByKey()
          .windowedBy(TimeWindows::of(seconds(10)).advanceBy(seconds(5)))
          .aggregate<int64_t>(
              []() { return 0L; },
              [](const std::string&, int64_t value, int64_t agg) {
                  return agg + value;
              });

    auto topology = builder.build();

    // Verify aggregate node exists
    bool hasAggregate = false;
    for (const auto& [name, node] : topology.nodes()) {
        if (name.find("TIME_WINDOW_AGGREGATE") != std::string::npos) {
            hasAggregate = true;
            break;
        }
    }
    EXPECT_TRUE(hasAggregate);
}

TEST_F(WindowedAggregationTest, TimeWindowedReduce) {
    auto stream = builder.stream<std::string, int64_t>("input-topic");

    // Create windowed reduce (max value per window)
    stream.groupByKey()
          .windowedBy(TimeWindows::of(seconds(5)))
          .reduce([](int64_t a, int64_t b) { return std::max(a, b); });

    auto topology = builder.build();

    // Verify reduce node exists
    bool hasReduce = false;
    for (const auto& [name, node] : topology.nodes()) {
        if (name.find("TIME_WINDOW_REDUCE") != std::string::npos) {
            hasReduce = true;
            break;
        }
    }
    EXPECT_TRUE(hasReduce);
}

// ============================================================================
// WindowStoreBuilder Tests
// ============================================================================

class WindowStoreBuilderTest : public ::testing::Test {
protected:
    std::string test_dir;

    void SetUp() override {
        test_dir = "/tmp/kawasan-window-builder-test-" +
                   std::to_string(std::chrono::system_clock::now()
                                      .time_since_epoch().count());
    }

    void TearDown() override {
        std::filesystem::remove_all(test_dir);
    }
};

TEST_F(WindowStoreBuilderTest, CreateWindowStore) {
    auto builder = Stores::windowStoreBuilder<std::string, int64_t>(
        "test-store",
        milliseconds(60000),
        std::make_shared<StringSerde>(),
        std::make_shared<LongSerde>());

    builder.withLoggingEnabled(false);

    EXPECT_EQ(builder.name(), "test-store");
    EXPECT_EQ(builder.retention(), milliseconds(60000));
    EXPECT_FALSE(builder.loggingEnabled());
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
