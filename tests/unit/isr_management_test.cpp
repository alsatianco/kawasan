#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <memory>
#include <thread>

#include "kawasan/broker/replica_manager.h"
#include "kawasan/storage/log.h"

namespace fs = std::filesystem;

namespace kawasan::broker {

class ISRManagementTest : public ::testing::Test {
protected:
    void SetUp() override {
        test_dir_ = fs::temp_directory_path() / "isr_management_test";
        fs::create_directories(test_dir_);
        
        replica_manager_ = std::make_unique<ReplicaManager>();
    }

    void TearDown() override {
        replica_manager_.reset();
        if (fs::exists(test_dir_)) {
            fs::remove_all(test_dir_);
        }
    }

    std::shared_ptr<storage::Log> createTestLog(const std::string& topic,
                                                PartitionId partition) {
        auto log_dir = test_dir_ / topic / std::to_string(partition);
        fs::create_directories(log_dir);
        
        storage::LogConfig config;
        config.segment_size = 1024 * 1024;  // 1MB
        config.retention_ms = 7 * 24 * 3600 * 1000;  // 7 days
        
        return std::make_shared<storage::Log>(topic, partition, log_dir.string(), config);
    }

    fs::path test_dir_;
    std::unique_ptr<ReplicaManager> replica_manager_;
};

// Test 1: Basic replica tracking
TEST_F(ISRManagementTest, BasicReplicaTracking) {
    TopicPartition tp{"test-topic", 0};
    auto log = createTestLog(tp.topic, tp.partition);
    
    // Add replica
    replica_manager_->addReplica(tp, log);
    
    // Verify leader status
    EXPECT_TRUE(replica_manager_->isLeader(tp));
    
    // Verify ISR contains only leader in single-node mode
    auto isr = replica_manager_->getISR(tp);
    EXPECT_EQ(isr.size(), 1);
    EXPECT_EQ(isr[0], 0);  // Broker ID 0
}

// Test 2: Follower offset tracking
TEST_F(ISRManagementTest, FollowerOffsetTracking) {
    TopicPartition tp{"test-topic", 0};
    auto log = createTestLog(tp.topic, tp.partition);
    
    replica_manager_->addReplica(tp, log);
    
    // Simulate follower fetch offset updates
    BrokerId follower1 = 1;
    BrokerId follower2 = 2;
    
    replica_manager_->updateFollowerFetchOffset(tp, follower1, 100);
    replica_manager_->updateFollowerFetchOffset(tp, follower2, 95);
    
    // Verify follower lags (leader is at offset 0 initially)
    auto lag1 = replica_manager_->getFollowerLag(tp, follower1);
    auto lag2 = replica_manager_->getFollowerLag(tp, follower2);
    
    ASSERT_TRUE(lag1.has_value());
    ASSERT_TRUE(lag2.has_value());
    
    // Lag = log_end_offset - follower_offset
    // Since log is empty (LEO = 0), lag would be negative,
    // but in practice followers can't be ahead
    EXPECT_EQ(*lag1, -100);
    EXPECT_EQ(*lag2, -95);
}

// Test 3: ISR shrink when follower lags
TEST_F(ISRManagementTest, ISRShrinkOnLag) {
    TopicPartition tp{"test-topic", 0};
    auto log = createTestLog(tp.topic, tp.partition);
    
    replica_manager_->addReplica(tp, log);
    
    // Set max lag to 100 messages
    replica_manager_->setMaxReplicaLag(100);
    
    // Append some records to the log
    std::vector<Record> records;
    for (int i = 0; i < 150; i++) {
        Record rec;
        rec.key = std::vector<uint8_t>{'k', static_cast<uint8_t>(i)};
        rec.value = std::vector<uint8_t>{'v', static_cast<uint8_t>(i)};
        rec.timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        records.push_back(rec);
    }
    log->append(records);
    
    // Manually update ISR to include followers
    std::vector<BrokerId> isr = {0, 1, 2};
    replica_manager_->updateISR(tp, isr);
    
    // Update follower offsets
    // Follower 1 is caught up (offset 150)
    replica_manager_->updateFollowerFetchOffset(tp, 1, 150);
    // Follower 2 is lagging (offset 40, lag = 150 - 40 = 110 > 100)
    replica_manager_->updateFollowerFetchOffset(tp, 2, 40);
    
    // Check and update ISR
    bool isr_changed = replica_manager_->checkAndUpdateISR(tp);
    
    EXPECT_TRUE(isr_changed);
    
    // Verify ISR now contains only leader and follower 1
    auto new_isr = replica_manager_->getISR(tp);
    EXPECT_EQ(new_isr.size(), 2);
    EXPECT_NE(std::find(new_isr.begin(), new_isr.end(), 0), new_isr.end());
    EXPECT_NE(std::find(new_isr.begin(), new_isr.end(), 1), new_isr.end());
    EXPECT_EQ(std::find(new_isr.begin(), new_isr.end(), 2), new_isr.end());
}

// Test 4: ISR grow when follower catches up
TEST_F(ISRManagementTest, ISRGrowOnCatchUp) {
    TopicPartition tp{"test-topic", 0};
    auto log = createTestLog(tp.topic, tp.partition);
    
    replica_manager_->addReplica(tp, log);
    replica_manager_->setMaxReplicaLag(100);
    
    // Append records
    std::vector<Record> records;
    for (int i = 0; i < 150; i++) {
        Record rec;
        rec.key = std::vector<uint8_t>{'k', static_cast<uint8_t>(i)};
        rec.value = std::vector<uint8_t>{'v', static_cast<uint8_t>(i)};
        rec.timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        records.push_back(rec);
    }
    log->append(records);
    
    // Start with ISR containing only leader
    std::vector<BrokerId> isr = {0};
    replica_manager_->updateISR(tp, isr);
    
    // Follower 1 is lagging initially
    replica_manager_->updateFollowerFetchOffset(tp, 1, 40);
    
    // Check ISR - should not change
    bool isr_changed = replica_manager_->checkAndUpdateISR(tp);
    EXPECT_FALSE(isr_changed);
    
    // Follower 1 catches up
    replica_manager_->updateFollowerFetchOffset(tp, 1, 145);  // lag = 5 < 100
    
    // Check ISR - should add follower 1
    isr_changed = replica_manager_->checkAndUpdateISR(tp);
    EXPECT_TRUE(isr_changed);
    
    auto new_isr = replica_manager_->getISR(tp);
    EXPECT_EQ(new_isr.size(), 2);
    EXPECT_NE(std::find(new_isr.begin(), new_isr.end(), 0), new_isr.end());
    EXPECT_NE(std::find(new_isr.begin(), new_isr.end(), 1), new_isr.end());
}

// Test 5: Multiple ISR updates
TEST_F(ISRManagementTest, MultipleISRUpdates) {
    TopicPartition tp{"test-topic", 0};
    auto log = createTestLog(tp.topic, tp.partition);
    
    replica_manager_->addReplica(tp, log);
    replica_manager_->setMaxReplicaLag(50);
    
    // Append records
    std::vector<Record> records;
    for (int i = 0; i < 200; i++) {
        Record rec;
        rec.key = std::vector<uint8_t>{'k', static_cast<uint8_t>(i)};
        rec.value = std::vector<uint8_t>{'v', static_cast<uint8_t>(i)};
        rec.timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        records.push_back(rec);
    }
    log->append(records);
    
    // Start with 3 followers in ISR
    std::vector<BrokerId> isr = {0, 1, 2, 3};
    replica_manager_->updateISR(tp, isr);
    
    // All followers caught up
    replica_manager_->updateFollowerFetchOffset(tp, 1, 200);
    replica_manager_->updateFollowerFetchOffset(tp, 2, 200);
    replica_manager_->updateFollowerFetchOffset(tp, 3, 200);
    
    // No change expected
    EXPECT_FALSE(replica_manager_->checkAndUpdateISR(tp));
    
    // Follower 2 falls behind
    replica_manager_->updateFollowerFetchOffset(tp, 2, 140);  // lag = 60 > 50
    EXPECT_TRUE(replica_manager_->checkAndUpdateISR(tp));
    
    auto isr1 = replica_manager_->getISR(tp);
    EXPECT_EQ(isr1.size(), 3);  // Leader + followers 1 and 3
    
    // Follower 3 falls behind
    replica_manager_->updateFollowerFetchOffset(tp, 3, 145);  // lag = 55 > 50
    EXPECT_TRUE(replica_manager_->checkAndUpdateISR(tp));
    
    auto isr2 = replica_manager_->getISR(tp);
    EXPECT_EQ(isr2.size(), 2);  // Leader + follower 1
    
    // Both followers catch up
    replica_manager_->updateFollowerFetchOffset(tp, 2, 195);  // lag = 5 < 50
    replica_manager_->updateFollowerFetchOffset(tp, 3, 198);  // lag = 2 < 50
    EXPECT_TRUE(replica_manager_->checkAndUpdateISR(tp));
    
    auto isr3 = replica_manager_->getISR(tp);
    EXPECT_EQ(isr3.size(), 4);  // All back in ISR
}

// Test 6: High watermark management with ISR
TEST_F(ISRManagementTest, HighWatermarkManagement) {
    TopicPartition tp{"test-topic", 0};
    auto log = createTestLog(tp.topic, tp.partition);
    
    replica_manager_->addReplica(tp, log);
    
    // Append records
    std::vector<Record> records;
    for (int i = 0; i < 100; i++) {
        Record rec;
        rec.key = std::vector<uint8_t>{'k', static_cast<uint8_t>(i)};
        rec.value = std::vector<uint8_t>{'v', static_cast<uint8_t>(i)};
        rec.timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        records.push_back(rec);
    }
    log->append(records);
    
    // Set high watermark
    replica_manager_->updateHighWatermark(tp, 50);
    
    auto hw = replica_manager_->getHighWatermark(tp);
    ASSERT_TRUE(hw.has_value());
    EXPECT_EQ(*hw, 50);
    
    // Update high watermark
    replica_manager_->updateHighWatermark(tp, 75);
    hw = replica_manager_->getHighWatermark(tp);
    ASSERT_TRUE(hw.has_value());
    EXPECT_EQ(*hw, 75);
}

// Test 7: Max replica lag configuration
TEST_F(ISRManagementTest, MaxReplicaLagConfiguration) {
    EXPECT_EQ(replica_manager_->getMaxReplicaLag(), 10000);  // Default value
    
    replica_manager_->setMaxReplicaLag(5000);
    EXPECT_EQ(replica_manager_->getMaxReplicaLag(), 5000);
    
    replica_manager_->setMaxReplicaLag(100);
    EXPECT_EQ(replica_manager_->getMaxReplicaLag(), 100);
}

// Test 8: ISR check with no follower state
TEST_F(ISRManagementTest, ISRCheckWithNoFollowerState) {
    TopicPartition tp{"test-topic", 0};
    auto log = createTestLog(tp.topic, tp.partition);
    
    replica_manager_->addReplica(tp, log);
    
    // Add a follower to ISR without any follower state
    std::vector<BrokerId> isr = {0, 1};
    replica_manager_->updateISR(tp, isr);
    
    // Check ISR - follower 1 should be removed due to no state
    bool isr_changed = replica_manager_->checkAndUpdateISR(tp);
    EXPECT_TRUE(isr_changed);
    
    auto new_isr = replica_manager_->getISR(tp);
    EXPECT_EQ(new_isr.size(), 1);  // Only leader remains
    EXPECT_EQ(new_isr[0], 0);
}

// --- Phase B: leader epoch tracking ---

TEST_F(ISRManagementTest, LeaderEpochStartsAtZeroAndBumps) {
    TopicPartition tp{"test-topic", 0};
    auto log = createTestLog(tp.topic, tp.partition);
    replica_manager_->addReplica(tp, log);

    auto epoch = replica_manager_->getLeaderEpoch(tp);
    ASSERT_TRUE(epoch.has_value());
    EXPECT_EQ(*epoch, 0);

    EXPECT_EQ(replica_manager_->bumpLeaderEpoch(tp).value_or(-1), 1);
    EXPECT_EQ(replica_manager_->bumpLeaderEpoch(tp).value_or(-1), 2);
    EXPECT_EQ(replica_manager_->getLeaderEpoch(tp).value_or(-1), 2);

    // Unknown partition -> nullopt.
    EXPECT_FALSE(replica_manager_->getLeaderEpoch(TopicPartition{"missing", 0}).has_value());
    EXPECT_FALSE(replica_manager_->bumpLeaderEpoch(TopicPartition{"missing", 0}).has_value());
}

// --- Phase B: ISR-committed offset (basis for acks=all) ---

namespace {
void appendN(std::shared_ptr<kawasan::storage::Log>& log, int n) {
    std::vector<kawasan::Record> records;
    for (int i = 0; i < n; ++i) {
        kawasan::Record rec;
        rec.value = std::vector<uint8_t>{'v', static_cast<uint8_t>(i)};
        rec.timestamp = 0;
        records.push_back(rec);
    }
    log->append(records);
}
}  // namespace

TEST_F(ISRManagementTest, IsrCommittedOffsetSingleReplicaIsLeo) {
    TopicPartition tp{"test-topic", 0};
    auto log = createTestLog(tp.topic, tp.partition);
    replica_manager_->addReplica(tp, log);
    appendN(log, 100);

    // ISR is just the leader -> committed offset is the leader's LEO.
    auto committed = replica_manager_->isrCommittedOffset(tp);
    ASSERT_TRUE(committed.has_value());
    EXPECT_EQ(*committed, 100);
}

TEST_F(ISRManagementTest, IsrCommittedOffsetHeldByLaggingFollower) {
    TopicPartition tp{"test-topic", 0};
    auto log = createTestLog(tp.topic, tp.partition);
    replica_manager_->addReplica(tp, log);
    appendN(log, 100);

    replica_manager_->updateISR(tp, {0, 1});
    replica_manager_->updateFollowerFetchOffset(tp, 1, 60);
    // Committed is held back to the slowest in-sync follower.
    EXPECT_EQ(replica_manager_->isrCommittedOffset(tp).value_or(-1), 60);

    // Follower catches up -> committed advances to the LEO.
    replica_manager_->updateFollowerFetchOffset(tp, 1, 100);
    EXPECT_EQ(replica_manager_->isrCommittedOffset(tp).value_or(-1), 100);
}

TEST_F(ISRManagementTest, IsrCommittedOffsetFollowerWithNoStateHoldsAtLogStart) {
    TopicPartition tp{"test-topic", 0};
    auto log = createTestLog(tp.topic, tp.partition);
    replica_manager_->addReplica(tp, log);
    appendN(log, 100);

    // A follower in the ISR that has never reported progress holds the committed
    // offset at the log start (it is not safe to consider its data replicated).
    replica_manager_->updateISR(tp, {0, 1});
    EXPECT_EQ(replica_manager_->isrCommittedOffset(tp).value_or(-1), 0);
}

}  // namespace kawasan::broker
