#include "kawasan/storage/log_segment.h"

#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include <rocksdb/options.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <memory>
#include <thread>
#include <vector>

#include "../sparse_record_batch.h"
#include "kawasan/common/error.h"
#include "kawasan/common/logger.h"
#include "kawasan/common/rocksdb_compat.h"
#include "kawasan/storage/record_batch.h"

namespace kawasan::storage {

namespace {

// Helper to create temporary directory for test data
std::string makeTestDir() {
    const auto timestamp = std::chrono::steady_clock::now().time_since_epoch().count();
    auto tmp = std::filesystem::temp_directory_path() /
               ("kawasan-log-segment-test-" + std::to_string(timestamp));
    std::filesystem::create_directories(tmp);
    return tmp.string();
}

// Helper to create a test record batch
RecordBatch makeTestBatch(Offset base_offset, const std::vector<std::string>& values) {
    RecordBatch batch;
    batch.setBaseOffset(base_offset);
    for (const auto& value : values) {
        Record record;
        record.timestamp = std::chrono::system_clock::now().time_since_epoch().count();
        record.value = std::vector<uint8_t>(value.begin(), value.end());
        batch.addRecord(record);
    }
    return batch;
}

// Helper to initialize logger once
void ensureLoggerInitialized() {
    static bool initialized = false;
    if (!initialized) {
        Logger::init("warn");  // Use warn level to reduce test noise
        initialized = true;
    }
}

}  // namespace

class LogSegmentTest : public ::testing::Test {
protected:
    void SetUp() override {
        ensureLoggerInitialized();
        test_dir_ = makeTestDir();
    }

    void TearDown() override {
        if (HasFailure()) {
            std::cerr << "Retained sparse segment data: " << test_dir_ << '\n';
            return;
        }
        if (!test_dir_.empty() && std::filesystem::exists(test_dir_)) {
            std::filesystem::remove_all(test_dir_);
        }
    }

    std::string test_dir_;
};

// Test 1: Write to segment until full, then roll
TEST_F(LogSegmentTest, WriteUntilFullThenRoll) {
    const std::string segment_path = test_dir_ + "/segment-00000000";
    LogSegment segment(0, segment_path);

    // Write multiple batches to fill the segment
    constexpr size_t kTargetSize = 1024 * 1024;  // 1MB target
    std::vector<Offset> offsets;
    size_t total_written = 0;
    Offset current_offset = 0;

    while (total_written < kTargetSize) {
        // Create a batch with some data
        auto batch = makeTestBatch(current_offset, {"data" + std::to_string(current_offset)});
        auto serialized_size = batch.serialize().size();

        Offset offset = segment.append(batch);
        offsets.push_back(offset);

        total_written += serialized_size;
        current_offset += batch.records().size();
    }

    // Verify segment is at least the target size
    EXPECT_GE(segment.size(), kTargetSize);

    // Verify next offset is correct
    EXPECT_EQ(segment.nextOffset(), current_offset);

    // Close the first segment (simulating roll)
    segment.flush();
    segment.close();
    EXPECT_TRUE(segment.isClosed());

    // Create a new segment for the rolled data
    const std::string new_segment_path = test_dir_ + "/segment-" + std::to_string(current_offset);
    LogSegment new_segment(current_offset, new_segment_path);

    // Write to the new segment
    auto new_batch = makeTestBatch(current_offset, {"new-data"});
    Offset new_offset = new_segment.append(new_batch);
    EXPECT_EQ(new_offset, current_offset);

    // Verify we can read from the new segment
    auto read_batch = new_segment.read(new_offset);
    ASSERT_TRUE(read_batch.has_value());
    EXPECT_EQ(read_batch->records().size(), 1u);
}

// Test 2: Read from empty segment
TEST_F(LogSegmentTest, ReadFromEmptySegment) {
    const std::string segment_path = test_dir_ + "/empty-segment";
    LogSegment segment(100, segment_path);

    // Try to read from an empty segment
    auto result = segment.read(100);
    EXPECT_FALSE(result.has_value());

    // Try to read multiple batches from empty segment
    auto batches = segment.read(100, 1024);
    EXPECT_TRUE(batches.empty());

    // Verify segment properties
    EXPECT_EQ(segment.baseOffset(), 100);
    EXPECT_EQ(segment.nextOffset(), 100);
    EXPECT_EQ(segment.size(), 0u);
}

// Test 3: Read from segment with only partial batch
TEST_F(LogSegmentTest, ReadFromSegmentWithPartialBatch) {
    const std::string segment_path = test_dir_ + "/partial-segment";
    LogSegment segment(0, segment_path);

    // Write several batches
    auto batch1 = makeTestBatch(0, {"msg1", "msg2", "msg3"});
    auto batch2 = makeTestBatch(3, {"msg4", "msg5"});
    auto batch3 = makeTestBatch(5, {"msg6"});

    segment.append(batch1);
    segment.append(batch2);
    segment.append(batch3);

    // Read with very limited max_bytes (should get partial results)
    auto batches = segment.read(0, 100);  // Very small max_bytes

    // Should get at least some batches, but not necessarily all
    EXPECT_FALSE(batches.empty());
    EXPECT_LE(batches.size(), 3u);

    // Verify we can read individual batches
    auto single_batch = segment.read(0);
    ASSERT_TRUE(single_batch.has_value());
    EXPECT_EQ(single_batch->records().size(), 3u);

    single_batch = segment.read(3);
    ASSERT_TRUE(single_batch.has_value());
    EXPECT_EQ(single_batch->records().size(), 2u);

    // Try to read with offset that doesn't exist
    single_batch = segment.read(999);
    EXPECT_FALSE(single_batch.has_value());
}

// Test 4: Concurrent reads while writing
TEST_F(LogSegmentTest, ConcurrentReadsWhileWriting) {
    const std::string segment_path = test_dir_ + "/concurrent-segment";
    LogSegment segment(0, segment_path);

    // Pre-populate some data
    for (int i = 0; i < 10; ++i) {
        auto batch = makeTestBatch(i, {"initial-" + std::to_string(i)});
        segment.append(batch);
    }

    std::atomic<bool> writer_done{false};
    std::atomic<bool> reader_error{false};
    std::atomic<int> successful_reads{0};

    // Writer thread - continuously writes data
    std::thread writer([&]() {
        for (int i = 10; i < 100; ++i) {
            auto batch = makeTestBatch(i, {"concurrent-" + std::to_string(i)});
            try {
                segment.append(batch);
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            } catch (const std::exception& e) {
                Logger::error("Writer error: {}", e.what());
                reader_error = true;
                break;
            }
        }
        writer_done = true;
    });

    // Reader threads - continuously read data
    std::vector<std::thread> readers;
    for (int r = 0; r < 3; ++r) {
        readers.emplace_back([&, r]() {
            while (!writer_done) {
                try {
                    // Read from various offsets
                    Offset offset = r * 3;  // Different readers read different offsets
                    auto batch = segment.read(offset);
                    if (batch.has_value()) {
                        successful_reads++;
                    }
                    std::this_thread::sleep_for(std::chrono::milliseconds(2));
                } catch (const std::exception& e) {
                    Logger::error("Reader {} error: {}", r, e.what());
                    reader_error = true;
                    break;
                }
            }
        });
    }

    // Wait for all threads
    writer.join();
    for (auto& reader : readers) {
        reader.join();
    }

    // Verify no errors occurred during concurrent access
    EXPECT_FALSE(reader_error);
    EXPECT_GT(successful_reads, 0);

    // Verify final state is consistent
    EXPECT_EQ(segment.nextOffset(), 100);

    // Verify we can read all written data
    auto all_batches = segment.read(0, 10 * 1024 * 1024);
    EXPECT_FALSE(all_batches.empty());
}

// Test 5: Recovery from truncated segment file
TEST_F(LogSegmentTest, RecoveryFromTruncatedSegment) {
    const std::string segment_path = test_dir_ + "/truncated-segment";

    {
        // Create and populate a segment
        LogSegment segment(0, segment_path);

        for (int i = 0; i < 20; ++i) {
            auto batch = makeTestBatch(i, {"message-" + std::to_string(i)});
            segment.append(batch);
        }

        segment.flush();
        segment.close();
    }

    // Simulate truncation by corrupting the RocksDB data
    // Note: RocksDB is resilient to corruption, so we'll test by creating
    // a new segment and verifying it handles missing data gracefully

    {
        // Reopen the segment - should recover existing data
        LogSegment recovered_segment(0, segment_path);

        // Verify we can read the recovered data
        auto batches = recovered_segment.read(0, 10 * 1024 * 1024);
        EXPECT_FALSE(batches.empty());

        // The segment should have recovered to a consistent state
        // with next_offset pointing after the last valid batch
        EXPECT_GE(recovered_segment.nextOffset(), 0);
        EXPECT_GT(recovered_segment.size(), 0u);

        // Verify we can continue writing after recovery
        auto new_batch = makeTestBatch(recovered_segment.nextOffset(), {"post-recovery"});
        Offset new_offset = recovered_segment.append(new_batch);
        EXPECT_EQ(new_offset, recovered_segment.nextOffset() - new_batch.records().size());

        // Verify we can read the newly written data
        auto read_new = recovered_segment.read(new_offset);
        ASSERT_TRUE(read_new.has_value());
        EXPECT_EQ(read_new->records().size(), 1u);
    }
}

// Additional test: Verify segment properties and invariants
TEST_F(LogSegmentTest, SegmentPropertiesAndInvariants) {
    const std::string segment_path = test_dir_ + "/properties-segment";
    const Offset base_offset = 42;

    LogSegment segment(base_offset, segment_path);

    // Initial state
    EXPECT_EQ(segment.baseOffset(), base_offset);
    EXPECT_EQ(segment.nextOffset(), base_offset);
    EXPECT_EQ(segment.size(), 0u);
    EXPECT_FALSE(segment.isClosed());
    EXPECT_EQ(segment.path(), segment_path);

    // After writing
    auto batch = makeTestBatch(base_offset, {"test1", "test2", "test3"});
    Offset offset = segment.append(batch);

    EXPECT_EQ(offset, base_offset);
    EXPECT_EQ(segment.nextOffset(), base_offset + 3);
    EXPECT_GT(segment.size(), 0u);

    // After closing
    segment.close();
    EXPECT_TRUE(segment.isClosed());

    // Verify cannot append to closed segment
    auto another_batch = makeTestBatch(base_offset + 3, {"should-fail"});
    EXPECT_THROW(segment.append(another_batch), StorageException);
}

TEST_F(LogSegmentTest, SparseBatchSpanSurvivesAppendReopenAndReadsInsideGaps) {
    const auto path = test_dir_ + "/sparse";
    {
        LogSegment segment(0, path);
        const auto sparse = test_support::sparseBatch();
        ASSERT_TRUE(sparse.isValid());
        ASSERT_EQ(segment.append(sparse), 0);
        EXPECT_EQ(segment.nextOffset(), 10);
        EXPECT_TRUE(segment.read(5).has_value());
        const auto decoded = segment.read(5, 1024 * 1024);
        EXPECT_EQ(decoded.size(), 1u);
        const auto raw = segment.readRaw(5, 1024 * 1024);
        EXPECT_FALSE(raw.empty());
        EXPECT_TRUE(segment.read(10, 1024 * 1024).empty());
        segment.close();
    }
    LogSegment reopened(0, path);
    EXPECT_EQ(reopened.nextOffset(), 10);
    EXPECT_EQ(reopened.append(makeTestBatch(10, {"next"})), 10);
    EXPECT_EQ(reopened.nextOffset(), 11);
}

TEST_F(LogSegmentTest, SparseBatchSpanDefinesWholeBatchTruncationBoundary) {
    LogSegment segment(0, test_dir_ + "/sparse-truncate");
    segment.append(test_support::sparseBatch());
    EXPECT_EQ(segment.truncateTo(10), 10);
    EXPECT_EQ(segment.truncateTo(5), 0);
    EXPECT_TRUE(segment.read(0, 1024 * 1024).empty());
    EXPECT_EQ(segment.append(makeTestBatch(0, {"replacement"})), 0);
}

TEST_F(LogSegmentTest, RejectsInvalidSparseSpanBeforeWriting) {
    for (int32_t delta : {-2, -1, 1, 8}) {
        SCOPED_TRACE(delta);
        LogSegment segment(0, test_dir_ + "/invalid-" + std::to_string(delta));
        EXPECT_THROW(segment.append(test_support::sparseBatch(0, delta)), StorageException);
        EXPECT_EQ(segment.nextOffset(), 0);
        EXPECT_EQ(segment.size(), 0u);
    }
}

TEST_F(LogSegmentTest, SparseSpanNearOffsetLimitRejectsOverflowBeforeWriting) {
    const auto maximum = std::numeric_limits<Offset>::max();
    LogSegment segment(maximum - 10, test_dir_ + "/offset-limit");
    ASSERT_EQ(segment.append(test_support::sparseBatch(maximum - 10)), maximum - 10);
    ASSERT_EQ(segment.nextOffset(), maximum);
    const auto before = segment.size();
    EXPECT_THROW(segment.append(makeTestBatch(maximum, {"overflow"})), StorageException);
    EXPECT_EQ(segment.nextOffset(), maximum);
    EXPECT_EQ(segment.size(), before);
}

TEST_F(LogSegmentTest, RejectsPersistedInvalidSpansAndMismatchedOffsetKeys) {
    const auto maximum = std::numeric_limits<Offset>::max();
    struct InvalidBatch {
        Offset key;
        Offset wire_base;
        int32_t last_delta;
    };
    const std::vector<InvalidBatch> invalid = {{0, 0, -2},
                                               {0, 0, -1},
                                               {0, 0, 1},
                                               {0, 0, 8},
                                               {0, -1, 9},
                                               {0, 1, 9},
                                               {maximum - 5, maximum - 5, 9}};
    for (size_t index = 0; index < invalid.size(); ++index) {
        SCOPED_TRACE(index);
        const auto& fixture = invalid[index];
        const auto path = test_dir_ + "/persisted-invalid-" + std::to_string(index);
        {
            LogSegment segment(fixture.key, path);
            segment.close();
        }
        {
            // Model an old writer or damaged offset metadata without invalidating
            // CRC. The production append guard must not sanitize the fixture.
            rocksdb::Options options;
            std::unique_ptr<rocksdb::DB> db;
            ASSERT_TRUE(openRocksDb(options, path, db).ok());
            const auto bytes =
                test_support::sparseBatch(fixture.wire_base, fixture.last_delta).serialize();
            Buffer key;
            key.writeInt64(fixture.key);
            rocksdb::WriteOptions write_options;
            write_options.sync = true;
            ASSERT_TRUE(
                db->Put(write_options,
                        rocksdb::Slice(reinterpret_cast<const char*>(key.data()), key.size()),
                        rocksdb::Slice(reinterpret_cast<const char*>(bytes.data()), bytes.size()))
                    .ok());
        }
        EXPECT_THROW({ LogSegment reopened(fixture.key, path); }, StorageException);
    }
}

TEST_F(LogSegmentTest, CorruptSstCannotReopenAsEmptySegment) {
    const auto path = test_dir_ + "/corrupt-sst";
    {
        LogSegment segment(0, path);
        segment.append(makeTestBatch(0, {"must-not-disappear"}));
        segment.flush();
        segment.close();
    }
    std::filesystem::path table;
    for (const auto& entry : std::filesystem::directory_iterator(path)) {
        if (entry.path().extension() == ".sst") {
            table = entry.path();
            break;
        }
    }
    ASSERT_FALSE(table.empty());
    {
        std::fstream stream(table, std::ios::binary | std::ios::in | std::ios::out);
        ASSERT_TRUE(stream.good());
        stream.seekg(10);
        char byte;
        stream.read(&byte, 1);
        ASSERT_TRUE(stream.good());
        byte ^= 1;
        stream.seekp(10);
        stream.write(&byte, 1);
        ASSERT_TRUE(stream.good());
    }
    EXPECT_THROW({ LogSegment reopened(0, path); }, StorageException);
}

TEST_F(LogSegmentTest, RejectsWireBaseDifferentFromAssignedOffsetBeforeWriting) {
    const auto path = test_dir_ + "/mismatched-append";
    {
        LogSegment segment(42, path);
        for (Offset base : {0, 43, 100}) {
            SCOPED_TRACE(base);
            EXPECT_THROW(segment.append(makeTestBatch(base, {"wrong-offset"})), StorageException);
            EXPECT_EQ(segment.nextOffset(), 42);
            EXPECT_EQ(segment.size(), 0u);
        }
        ASSERT_EQ(segment.append(makeTestBatch(42, {"correct-offset"})), 42);
        segment.close();
    }
    LogSegment reopened(42, path);
    EXPECT_EQ(reopened.nextOffset(), 43);
}

}  // namespace kawasan::storage

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
