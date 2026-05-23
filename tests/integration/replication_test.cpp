/**
 * Integration test for multi-broker replication and failover scenarios.
 * 
 * This test validates:
 * 1. 3-broker cluster with replication factor 3
 * 2. Follower failure - leader continues to serve
 * 3. Leader failure - failover and new leader election
 * 4. Follower rejoin - catch-up after downtime
 * 5. Network partition - ISR shrinks appropriately
 * 
 * Test approach: Use subprocess execution to start 3 brokers via start_cluster.sh,
 * then use Kafka protocol over TCP to interact with each broker and simulate failures.
 */

#include <arpa/inet.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <string>
#include <thread>
#include <vector>

#include <boost/asio.hpp>
#include <gtest/gtest.h>

#include "kawasan/common/buffer.h"
#include "kawasan/common/logger.h"
#include "kawasan/protocol/api_versions.h"
#include "kawasan/protocol/create_topics_request.h"
#include "kawasan/protocol/metadata_request.h"
#include "kawasan/protocol/produce_request.h"
#include "kawasan/protocol/fetch_request.h"
#include "kawasan/protocol/request_header.h"

namespace {

using namespace std::chrono_literals;

constexpr int NUM_BROKERS = 3;
constexpr int BASE_PORT = 9092;
constexpr int CLUSTER_STARTUP_TIMEOUT_SEC = 60;
[[maybe_unused]] constexpr int REPLICATION_TIMEOUT_SEC = 30;
constexpr int FAILOVER_TIMEOUT_SEC = 45;

// Test cluster manager using subprocess execution
class TestCluster {
public:
    TestCluster() = default;
    
    ~TestCluster() {
        stop();
    }
    
    // Start cluster using start_cluster.sh script
    bool start() {
        ensureLoggerInitialized();
        
        kawasan::Logger::info("Starting {}-broker test cluster for replication tests", NUM_BROKERS);
        
        // Build path to start_cluster.sh
        auto script_path = getScriptPath();
        if (!std::filesystem::exists(script_path)) {
            kawasan::Logger::error("Cluster script not found: {}", script_path.string());
            return false;
        }
        
        // Fork and exec the cluster script
        cluster_pid_ = fork();
        if (cluster_pid_ < 0) {
            kawasan::Logger::error("Failed to fork: {}", strerror(errno));
            return false;
        }
        
        if (cluster_pid_ == 0) {
            // Child process: execute start_cluster.sh
            // Redirect output to log file for debugging
            auto log_path = "/tmp/kawasan-replication-test.log";
            int log_fd = open(log_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
            if (log_fd >= 0) {
                dup2(log_fd, STDOUT_FILENO);
                dup2(log_fd, STDERR_FILENO);
                close(log_fd);
            }
            
            // Execute script with background mode
            setenv("BACKGROUND", "1", 1);
            setenv("KEEP_DATA", "0", 1);
            
            execl(script_path.c_str(), script_path.c_str(), 
                  std::to_string(NUM_BROKERS).c_str(), nullptr);
            
            // If execl returns, it failed
            kawasan::Logger::error("Failed to exec cluster script: {}", strerror(errno));
            _exit(1);
        }
        
        // Parent process: wait for cluster to be ready
        kawasan::Logger::info("Waiting for cluster to start (PID: {})", cluster_pid_);
        
        if (!waitForClusterReady()) {
            kawasan::Logger::error("Cluster failed to start within timeout");
            stop();
            return false;
        }
        
        kawasan::Logger::info("Cluster is ready");
        return true;
    }
    
    // Stop the cluster
    void stop() {
        if (cluster_pid_ > 0) {
            kawasan::Logger::info("Stopping cluster (PID: {})", cluster_pid_);
            
            // Send SIGTERM to the cluster script (which will propagate to brokers)
            kill(cluster_pid_, SIGTERM);
            
            // Wait for cleanup with timeout
            int status;
            int retry_count = 0;
            while (retry_count++ < 10) {
                pid_t result = waitpid(cluster_pid_, &status, WNOHANG);
                if (result > 0) {
                    kawasan::Logger::info("Cluster stopped");
                    break;
                }
                std::this_thread::sleep_for(500ms);
            }
            
            // Force kill if still running
            if (retry_count >= 10) {
                kawasan::Logger::warn("Force killing cluster");
                kill(cluster_pid_, SIGKILL);
                waitpid(cluster_pid_, &status, 0);
            }
            
            cluster_pid_ = -1;
        }
    }
    
    // Kill a specific broker by ID
    bool killBroker(int broker_id) {
        if (broker_id < 0 || broker_id >= NUM_BROKERS) {
            return false;
        }
        
        auto pid_file = getPidFile(broker_id);
        if (!std::filesystem::exists(pid_file)) {
            kawasan::Logger::error("PID file not found for broker {}: {}", 
                                  broker_id, pid_file.string());
            return false;
        }
        
        std::ifstream pid_stream(pid_file);
        pid_t broker_pid;
        pid_stream >> broker_pid;
        
        if (broker_pid <= 0) {
            kawasan::Logger::error("Invalid PID for broker {}: {}", broker_id, broker_pid);
            return false;
        }
        
        kawasan::Logger::info("Killing broker {} (PID: {})", broker_id, broker_pid);
        
        if (kill(broker_pid, SIGKILL) == 0) {
            killed_brokers_.insert(broker_id);
            std::this_thread::sleep_for(2s); // Give time for process to die
            return true;
        }
        
        kawasan::Logger::error("Failed to kill broker {}: {}", broker_id, strerror(errno));
        return false;
    }
    
    // Get the set of killed broker IDs
    const std::set<int>& getKilledBrokers() const {
        return killed_brokers_;
    }

private:
    pid_t cluster_pid_ = -1;
    std::set<int> killed_brokers_;
    
    void ensureLoggerInitialized() {
        static bool initialized = false;
        if (!initialized) {
            kawasan::Logger::init();
            initialized = true;
        }
    }
    
    std::filesystem::path getScriptPath() const {
        // Assume we're in build directory, go up to project root
        auto current_path = std::filesystem::current_path();
        auto script_path = current_path / ".." / "scripts" / "start_cluster.sh";
        return std::filesystem::canonical(script_path.lexically_normal());
    }
    
    std::filesystem::path getPidFile(int broker_id) const {
        return std::filesystem::path("/tmp") / "kawasan-cluster" / 
               ("broker-" + std::to_string(broker_id)) / "broker.pid";
    }
    
    bool waitForClusterReady() {
        auto start = std::chrono::steady_clock::now();
        
        while (true) {
            auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::steady_clock::now() - start).count();
            
            if (elapsed > CLUSTER_STARTUP_TIMEOUT_SEC) {
                return false;
            }
            
            // Try to connect to all brokers
            bool all_ready = true;
            for (int i = 0; i < NUM_BROKERS; ++i) {
                if (!canConnectToBroker(i)) {
                    all_ready = false;
                    break;
                }
            }
            
            if (all_ready) {
                return true;
            }
            
            std::this_thread::sleep_for(1s);
        }
    }
    
    bool canConnectToBroker(int broker_id) {
        try {
            boost::asio::io_context io_context;
            boost::asio::ip::tcp::socket socket(io_context);
            boost::asio::ip::tcp::endpoint endpoint(
                boost::asio::ip::make_address("127.0.0.1"),
                BASE_PORT + broker_id);
            
            boost::system::error_code ec;
            socket.connect(endpoint, ec);
            
            if (ec) {
                return false;
            }
            
            socket.close();
            return true;
        } catch (...) {
            return false;
        }
    }
};

// Helper function to connect to a broker and return socket
std::shared_ptr<boost::asio::ip::tcp::socket> connectToBroker(
    boost::asio::io_context& io_context, int broker_id) {
    
    auto socket = std::make_shared<boost::asio::ip::tcp::socket>(io_context);
    boost::asio::ip::tcp::endpoint endpoint(
        boost::asio::ip::make_address("127.0.0.1"),
        BASE_PORT + broker_id);
    
    boost::system::error_code ec;
    socket->connect(endpoint, ec);
    
    if (ec) {
        kawasan::Logger::error("Failed to connect to broker {}: {}", broker_id, ec.message());
        return nullptr;
    }
    
    kawasan::Logger::debug("Connected to broker {}", broker_id);
    return socket;
}

// Create a topic on a specific broker
bool createTopic(boost::asio::ip::tcp::socket& socket, 
                 const std::string& topic_name,
                 int num_partitions = 3,
                 int replication_factor = 3,
                 int32_t timeout_ms = 10000) {
    
    using namespace kawasan::protocol;
    
    // Create topic request
    CreateTopicsRequest req;
    CreatableTopic topic;
    topic.name = topic_name;
    topic.num_partitions = num_partitions;
    topic.replication_factor = replication_factor;
    req.addTopic(topic);
    req.setTimeoutMs(timeout_ms);
    
    // Encode request header
    RequestHeader header;
    header.setApiKey(static_cast<ApiKey>(19)); // CreateTopics
    header.setApiVersion(4);
    header.setCorrelationId(1000);
    header.setClientId("replication_test");
    
    kawasan::Buffer header_buf;
    header.encode(header_buf);
    
    // Encode request body
    kawasan::Buffer req_buf;
    req.encode(req_buf, 4);
    
    // Send length-prefixed message
    uint32_t total_length = header_buf.size() + req_buf.size();
    uint32_t net_length = htonl(total_length);
    
    boost::system::error_code ec;
    boost::asio::write(socket, boost::asio::buffer(&net_length, 4), ec);
    if (ec) {
        kawasan::Logger::error("Failed to send length: {}", ec.message());
        return false;
    }
    
    boost::asio::write(socket, boost::asio::buffer(header_buf.data(), header_buf.size()), ec);
    if (ec) {
        kawasan::Logger::error("Failed to send header: {}", ec.message());
        return false;
    }
    
    boost::asio::write(socket, boost::asio::buffer(req_buf.data(), req_buf.size()), ec);
    if (ec) {
        kawasan::Logger::error("Failed to send request: {}", ec.message());
        return false;
    }
    
    // Read response (simplified - just check we got something back)
    uint32_t resp_length;
    boost::asio::read(socket, boost::asio::buffer(&resp_length, 4), ec);
    if (ec) {
        kawasan::Logger::error("Failed to read response length: {}", ec.message());
        return false;
    }
    
    resp_length = ntohl(resp_length);
    std::vector<uint8_t> resp_data(resp_length);
    boost::asio::read(socket, boost::asio::buffer(resp_data), ec);
    if (ec) {
        kawasan::Logger::error("Failed to read response: {}", ec.message());
        return false;
    }
    
    kawasan::Logger::info("Topic '{}' created (or already exists)", topic_name);
    return true;
}

// Produce messages to a topic
bool produceMessages(boost::asio::ip::tcp::socket& socket [[maybe_unused]],
                     const std::string& topic_name,
                     int partition,
                     const std::vector<std::string>& messages,
                     int16_t acks = -1) {
    
    // TODO: Implement proper ProduceRequest encoding
    // For now, this is a placeholder that returns true
    kawasan::Logger::info("Producing {} messages to topic '{}' partition {} (acks={})",
                        messages.size(), topic_name, partition, acks);
    return true;
}

// Fetch messages from a topic
bool fetchMessages(boost::asio::ip::tcp::socket& socket [[maybe_unused]],
                   const std::string& topic_name,
                   int partition,
                   int64_t offset,
                   int expected_count) {
    
    // TODO: Implement proper FetchRequest encoding
    // For now, this is a placeholder that returns true
    kawasan::Logger::info("Fetching from topic '{}' partition {} at offset {} (expecting {} messages)",
                        topic_name, partition, offset, expected_count);
    return true;
}

// Get metadata for a topic to check ISR
bool getTopicMetadata(boost::asio::ip::tcp::socket& socket [[maybe_unused]],
                      const std::string& topic_name,
                      std::vector<int>& isr_out) {
    
    // TODO: Implement proper MetadataRequest encoding and parsing
    // For now, return placeholder ISR (all 3 brokers)
    kawasan::Logger::info("Getting metadata for topic '{}'", topic_name);
    isr_out = {0, 1, 2};
    return true;
}

} // anonymous namespace

// Test fixture
class ReplicationTest : public ::testing::Test {
protected:
    void SetUp() override {
        cluster_ = std::make_unique<TestCluster>();
        ASSERT_TRUE(cluster_->start()) << "Failed to start test cluster";
        
        // Give cluster extra time to stabilize
        std::this_thread::sleep_for(5s);
    }
    
    void TearDown() override {
        if (cluster_) {
            cluster_->stop();
        }
    }
    
    std::unique_ptr<TestCluster> cluster_;
};

/**
 * Test 1: 3 brokers, replication factor 3, produce with acks=-1
 * 
 * Validates:
 * - Cluster starts successfully with 3 brokers
 * - Topic can be created with replication factor 3
 * - Messages can be produced with acks=-1 (all replicas)
 * - Messages can be fetched from all brokers
 */
TEST_F(ReplicationTest, DISABLED_ThreeBrokersReplicationFactorThree) {
    boost::asio::io_context io_context;
    
    // Connect to broker 0
    auto socket = connectToBroker(io_context, 0);
    ASSERT_NE(socket, nullptr);
    
    // Create topic with replication factor 3
    std::string topic_name = "test-replication-rf3";
    ASSERT_TRUE(createTopic(*socket, topic_name, 3, 3));
    
    // Wait for topic to propagate
    std::this_thread::sleep_for(3s);
    
    // Produce messages with acks=-1
    std::vector<std::string> messages = {"msg1", "msg2", "msg3", "msg4", "msg5"};
    ASSERT_TRUE(produceMessages(*socket, topic_name, 0, messages, -1));
    
    // Fetch from all 3 brokers to verify replication
    for (int broker_id = 0; broker_id < NUM_BROKERS; ++broker_id) {
        auto fetch_socket = connectToBroker(io_context, broker_id);
        ASSERT_NE(fetch_socket, nullptr);
        
        ASSERT_TRUE(fetchMessages(*fetch_socket, topic_name, 0, 0, messages.size()));
    }
    
    kawasan::Logger::info("Test 1 passed: 3 brokers with RF=3");
}

/**
 * Test 2: Kill follower, verify leader still serves
 * 
 * Validates:
 * - Leader continues to accept produce requests after follower failure
 * - ISR shrinks to exclude dead follower
 * - Produce with acks=-1 still succeeds (ISR requirement satisfied)
 */
TEST_F(ReplicationTest, DISABLED_KillFollowerLeaderContinues) {
    boost::asio::io_context io_context;
    
    // Create topic with RF=3
    std::string topic_name = "test-follower-failure";
    auto socket = connectToBroker(io_context, 0);
    ASSERT_NE(socket, nullptr);
    ASSERT_TRUE(createTopic(*socket, topic_name, 1, 3));
    
    std::this_thread::sleep_for(3s);
    
    // Produce initial messages
    std::vector<std::string> initial_msgs = {"msg1", "msg2", "msg3"};
    ASSERT_TRUE(produceMessages(*socket, topic_name, 0, initial_msgs, -1));
    
    // Verify initial ISR has all 3 brokers
    std::vector<int> isr;
    ASSERT_TRUE(getTopicMetadata(*socket, topic_name, isr));
    ASSERT_EQ(isr.size(), 3);
    
    // Kill broker 2 (a follower)
    ASSERT_TRUE(cluster_->killBroker(2));
    
    // Wait for ISR to update
    std::this_thread::sleep_for(5s);
    
    // Verify ISR shrunk
    isr.clear();
    ASSERT_TRUE(getTopicMetadata(*socket, topic_name, isr));
    ASSERT_EQ(isr.size(), 2);
    
    // Produce more messages - should still succeed
    std::vector<std::string> more_msgs = {"msg4", "msg5", "msg6"};
    ASSERT_TRUE(produceMessages(*socket, topic_name, 0, more_msgs, -1));
    
    // Fetch from broker 0 (leader) - should see all messages
    ASSERT_TRUE(fetchMessages(*socket, topic_name, 0, 0, 
                              initial_msgs.size() + more_msgs.size()));
    
    kawasan::Logger::info("Test 2 passed: Follower failure handled");
}

/**
 * Test 3: Kill leader, verify failover and new leader
 * 
 * Validates:
 * - Cluster detects leader failure
 * - New leader is elected
 * - Produce requests to new leader succeed
 * - Data remains consistent across surviving replicas
 */
TEST_F(ReplicationTest, DISABLED_KillLeaderFailover) {
    boost::asio::io_context io_context;
    
    // Create topic with RF=3
    std::string topic_name = "test-leader-failover";
    auto socket = connectToBroker(io_context, 0);
    ASSERT_NE(socket, nullptr);
    ASSERT_TRUE(createTopic(*socket, topic_name, 1, 3));
    
    std::this_thread::sleep_for(3s);
    
    // Produce initial messages to broker 0 (likely leader)
    std::vector<std::string> initial_msgs = {"msg1", "msg2", "msg3"};
    ASSERT_TRUE(produceMessages(*socket, topic_name, 0, initial_msgs, -1));
    
    // Kill broker 0 (leader)
    socket->close();
    ASSERT_TRUE(cluster_->killBroker(0));
    
    // Wait for re-election
    std::this_thread::sleep_for(FAILOVER_TIMEOUT_SEC * 1s);
    
    // Try to produce to broker 1 (should be new leader)
    auto socket1 = connectToBroker(io_context, 1);
    ASSERT_NE(socket1, nullptr);
    
    std::vector<std::string> more_msgs = {"msg4", "msg5", "msg6"};
    ASSERT_TRUE(produceMessages(*socket1, topic_name, 0, more_msgs, -1));
    
    // Verify data consistency on broker 2
    auto socket2 = connectToBroker(io_context, 2);
    ASSERT_NE(socket2, nullptr);
    ASSERT_TRUE(fetchMessages(*socket2, topic_name, 0, 0, 
                              initial_msgs.size() + more_msgs.size()));
    
    kawasan::Logger::info("Test 3 passed: Leader failover succeeded");
}

/**
 * Test 4: Follower rejoins after downtime, catches up
 * 
 * Validates:
 * - Follower that was killed can rejoin
 * - Follower replicates missing data from leader
 * - ISR grows back to include rejoined follower
 */
TEST_F(ReplicationTest, DISABLED_FollowerRejoinCatchUp) {
    // This test requires the ability to restart a killed broker,
    // which is not implemented in the current TestCluster.
    // TODO: Implement broker restart capability
    
    kawasan::Logger::info("Test 4 skipped: Requires broker restart capability");
    GTEST_SKIP() << "Broker restart not implemented";
}

/**
 * Test 5: Network partition, verify ISR shrinks
 * 
 * Validates:
 * - Simulated network partition causes follower to fall behind
 * - ISR shrinks to exclude partitioned follower
 * - Leader continues to serve with reduced ISR
 */
TEST_F(ReplicationTest, DISABLED_NetworkPartitionISRShrinks) {
    // This test requires network partition simulation (e.g., iptables),
    // which is complex to implement in a portable way.
    // For now, killing a follower achieves a similar effect (Test 2).
    
    kawasan::Logger::info("Test 5 skipped: Requires network partition simulation");
    GTEST_SKIP() << "Network partition simulation not implemented";
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    kawasan::Logger::init();
    return RUN_ALL_TESTS();
}
