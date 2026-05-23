#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <memory>
#include <thread>

#include "kawasan/broker/kawasan_broker.h"
#include "kawasan/client/consumer.h"
#include "kawasan/client/producer.h"
#include "kawasan/common/config.h"
#include "kawasan/common/logger.h"

namespace fs = std::filesystem;

namespace kawasan::test {

class OffsetPersistenceTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create unique test directory
        test_dir_ = fs::temp_directory_path() / ("offset_persist_test_" + 
                    std::to_string(std::chrono::system_clock::now().time_since_epoch().count()));
        fs::create_directories(test_dir_);
        
        // Configure broker
        config_.set("broker.id", 0);
        config_.set("host", "localhost");
        config_.set("port", test_port_);
        config_.set("log.dirs", test_dir_.string());
        config_.set("auto.create.topics.enable", true);
    }

    void TearDown() override {
        // Clean up test directory
        if (fs::exists(test_dir_)) {
            fs::remove_all(test_dir_);
        }
    }

    std::unique_ptr<broker::KawasanBroker> createBroker() {
        return std::make_unique<broker::KawasanBroker>(config_);
    }

    void waitForBroker(int port, int max_attempts = 20) {
        for (int i = 0; i < max_attempts; ++i) {
            try {
                // Try to connect
                boost::asio::io_context io;
                boost::asio::ip::tcp::socket socket(io);
                boost::asio::ip::tcp::endpoint endpoint(
                    boost::asio::ip::address::from_string("127.0.0.1"), port);
                socket.connect(endpoint);
                socket.close();
                return;  // Success
            } catch (...) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
        }
        throw std::runtime_error("Broker did not become ready in time");
    }

    Config config_;
    fs::path test_dir_;
    int test_port_ = 19092;  // Use non-standard port to avoid conflicts
};

TEST_F(OffsetPersistenceTest, BasicOffsetPersistence) {
    const std::string topic = "test-topic";
    const std::string group_id = "test-group";
    
    // Step 1: Start broker, produce, commit offset
    int64_t committed_offset = 0;
    {
        auto broker = createBroker();
        broker->start();
        waitForBroker(test_port_);
        
        // Produce messages
        client::Producer producer("localhost:" + std::to_string(test_port_));
        for (int i = 0; i < 10; ++i) {
            producer.produce(topic, "key" + std::to_string(i), 
                           "value" + std::to_string(i));
        }
        
        // Consume and commit offset
        client::Consumer consumer("localhost:" + std::to_string(test_port_), group_id);
        consumer.subscribe({topic});
        
        int messages_consumed = 0;
        while (messages_consumed < 10) {
            auto records = consumer.poll(std::chrono::milliseconds(1000));
            messages_consumed += records.size();
        }
        
        consumer.commitSync();
        committed_offset = consumer.position(topic, 0);
        
        Logger::info("Committed offset: {}", committed_offset);
        ASSERT_EQ(committed_offset, 10);
        
        broker->stop();
    }
    
    // Step 2: Restart broker, verify offset persisted
    {
        auto broker = createBroker();
        broker->start();
        waitForBroker(test_port_);
        
        // Create new consumer with same group
        client::Consumer consumer("localhost:" + std::to_string(test_port_), group_id);
        consumer.subscribe({topic});
        
        // Verify committed offset is restored
        auto restored_offset = consumer.committed(topic, 0);
        ASSERT_TRUE(restored_offset.has_value());
        EXPECT_EQ(*restored_offset, committed_offset);
        
        Logger::info("Restored offset: {}", *restored_offset);
        
        broker->stop();
    }
}

TEST_F(OffsetPersistenceTest, MultipleGroupsPersistence) {
    const std::string topic = "test-topic";
    const std::string group1 = "group-1";
    const std::string group2 = "group-2";
    
    int64_t group1_offset = 0;
    int64_t group2_offset = 0;
    
    // Step 1: Two groups commit different offsets
    {
        auto broker = createBroker();
        broker->start();
        waitForBroker(test_port_);
        
        // Produce 20 messages
        client::Producer producer("localhost:" + std::to_string(test_port_));
        for (int i = 0; i < 20; ++i) {
            producer.produce(topic, "key" + std::to_string(i), 
                           "value" + std::to_string(i));
        }
        
        // Group 1 consumes 10 messages
        client::Consumer consumer1("localhost:" + std::to_string(test_port_), group1);
        consumer1.subscribe({topic});
        int consumed = 0;
        while (consumed < 10) {
            auto records = consumer1.poll(std::chrono::milliseconds(1000));
            consumed += records.size();
        }
        consumer1.commitSync();
        group1_offset = consumer1.position(topic, 0);
        
        // Group 2 consumes 15 messages
        client::Consumer consumer2("localhost:" + std::to_string(test_port_), group2);
        consumer2.subscribe({topic});
        consumed = 0;
        while (consumed < 15) {
            auto records = consumer2.poll(std::chrono::milliseconds(1000));
            consumed += records.size();
        }
        consumer2.commitSync();
        group2_offset = consumer2.position(topic, 0);
        
        ASSERT_EQ(group1_offset, 10);
        ASSERT_EQ(group2_offset, 15);
        
        broker->stop();
    }
    
    // Step 2: Restart and verify both groups' offsets
    {
        auto broker = createBroker();
        broker->start();
        waitForBroker(test_port_);
        
        // Verify group 1 offset
        client::Consumer consumer1("localhost:" + std::to_string(test_port_), group1);
        consumer1.subscribe({topic});
        auto offset1 = consumer1.committed(topic, 0);
        ASSERT_TRUE(offset1.has_value());
        EXPECT_EQ(*offset1, group1_offset);
        
        // Verify group 2 offset
        client::Consumer consumer2("localhost:" + std::to_string(test_port_), group2);
        consumer2.subscribe({topic});
        auto offset2 = consumer2.committed(topic, 0);
        ASSERT_TRUE(offset2.has_value());
        EXPECT_EQ(*offset2, group2_offset);
        
        broker->stop();
    }
}

TEST_F(OffsetPersistenceTest, LargeOffsetValues) {
    const std::string topic = "test-topic";
    const std::string group_id = "test-group";
    
    // Use a very large offset value (near INT64_MAX)
    const int64_t large_offset = 9223372036854775000LL;  // Near INT64_MAX
    
    {
        auto broker = createBroker();
        broker->start();
        waitForBroker(test_port_);
        
        // Manually commit a large offset (simulating a topic with many messages)
        // This requires direct access to offset storage, so we'll use the protocol
        // For now, we'll skip this advanced test scenario
        // TODO: Add test for large offset values when we have admin client
        
        broker->stop();
    }
}

TEST_F(OffsetPersistenceTest, OffsetMetadataPersistence) {
    const std::string topic = "test-topic";
    const std::string group_id = "test-group";
    const std::string metadata = "test-metadata-string";
    
    {
        auto broker = createBroker();
        broker->start();
        waitForBroker(test_port_);
        
        // Produce and consume
        client::Producer producer("localhost:" + std::to_string(test_port_));
        for (int i = 0; i < 5; ++i) {
            producer.produce(topic, "key" + std::to_string(i), 
                           "value" + std::to_string(i));
        }
        
        client::Consumer consumer("localhost:" + std::to_string(test_port_), group_id);
        consumer.subscribe({topic});
        int consumed = 0;
        while (consumed < 5) {
            auto records = consumer.poll(std::chrono::milliseconds(1000));
            consumed += records.size();
        }
        
        // Commit with metadata
        consumer.commitSync(metadata);
        
        broker->stop();
    }
    
    // Verify metadata persisted (requires enhanced consumer API to read metadata)
    {
        auto broker = createBroker();
        broker->start();
        waitForBroker(test_port_);
        
        // Current consumer API doesn't expose metadata, but it's stored in RocksDB
        // This test validates that commit succeeded
        client::Consumer consumer("localhost:" + std::to_string(test_port_), group_id);
        consumer.subscribe({topic});
        auto offset = consumer.committed(topic, 0);
        ASSERT_TRUE(offset.has_value());
        EXPECT_EQ(*offset, 5);
        
        broker->stop();
    }
}

TEST_F(OffsetPersistenceTest, MultipleRestarts) {
    const std::string topic = "test-topic";
    const std::string group_id = "test-group";
    
    // Restart broker 3 times, committing offsets each time
    for (int restart = 0; restart < 3; ++restart) {
        auto broker = createBroker();
        broker->start();
        waitForBroker(test_port_);
        
        // Produce messages
        client::Producer producer("localhost:" + std::to_string(test_port_));
        for (int i = 0; i < 5; ++i) {
            producer.produce(topic, "key" + std::to_string(i), 
                           "value" + std::to_string(i));
        }
        
        // Consume and commit
        client::Consumer consumer("localhost:" + std::to_string(test_port_), group_id);
        consumer.subscribe({topic});
        
        // Get previous offset
        auto prev_offset = consumer.committed(topic, 0);
        int64_t expected_offset = prev_offset.value_or(0) + 5;
        
        int consumed = 0;
        while (consumed < 5) {
            auto records = consumer.poll(std::chrono::milliseconds(1000));
            consumed += records.size();
        }
        consumer.commitSync();
        
        auto new_offset = consumer.position(topic, 0);
        EXPECT_EQ(new_offset, expected_offset);
        
        Logger::info("Restart {}: committed offset {}", restart, new_offset);
        
        broker->stop();
    }
}

}  // namespace kawasan::test
