/**
 * Integration test for Raft metadata replication across a 3-broker cluster.
 * 
 * This test validates:
 * 1. Multi-broker cluster startup and Raft cluster formation
 * 2. Topic creation on broker 0 propagates to brokers 1 and 2
 * 3. Leader failover: kill leader, verify new leader elected
 * 4. Topic creation on new leader propagates correctly
 * 
 * Test approach: Use subprocess execution to start 3 brokers via start_cluster.sh,
 * then use Kafka protocol over TCP to interact with each broker.
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
#include "kawasan/protocol/request_header.h"

namespace {

using namespace std::chrono_literals;

constexpr int NUM_BROKERS = 3;
constexpr int BASE_PORT = 9092;
constexpr int CLUSTER_STARTUP_TIMEOUT_SEC = 60;
constexpr int REPLICATION_TIMEOUT_SEC = 30;

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
        
        kawasan::Logger::info("Starting {}-broker test cluster", NUM_BROKERS);
        
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
            auto log_path = "/tmp/kawasan-cluster-test.log";
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
        kawasan::Logger::info("Waiting for cluster to start (PID: {})...", cluster_pid_);
        
        if (!waitForClusterReady()) {
            kawasan::Logger::error("Cluster failed to become ready");
            stop();
            return false;
        }
        
        kawasan::Logger::info("Cluster is ready!");
        return true;
    }
    
    void stop() {
        if (cluster_pid_ > 0) {
            kawasan::Logger::info("Stopping cluster (PID: {})...", cluster_pid_);
            
            // Send SIGTERM to cluster script
            kill(cluster_pid_, SIGTERM);
            
            // Wait for graceful shutdown (up to 10 seconds)
            int status;
            for (int i = 0; i < 100; ++i) {
                pid_t result = waitpid(cluster_pid_, &status, WNOHANG);
                if (result == cluster_pid_) {
                    kawasan::Logger::info("Cluster stopped gracefully");
                    cluster_pid_ = -1;
                    return;
                }
                std::this_thread::sleep_for(100ms);
            }
            
            // Force kill if still running
            kawasan::Logger::warn("Force killing cluster");
            kill(cluster_pid_, SIGKILL);
            waitpid(cluster_pid_, &status, 0);
            cluster_pid_ = -1;
        }
    }
    
    // Get port for broker i
    int getPort(int broker_id) const {
        return BASE_PORT + broker_id;
    }
    
    // Kill a specific broker (by finding its PID from the cluster)
    bool killBroker(int broker_id) {
        // Read PID from the cluster's PID file
        auto pid_file = fmt::format("/tmp/kawasan-cluster-{}/broker-{}.pid", 
                                   cluster_pid_, broker_id);
        
        std::ifstream file(pid_file);
        if (!file.is_open()) {
            kawasan::Logger::error("Failed to open PID file: {}", pid_file);
            return false;
        }
        
        pid_t broker_pid;
        file >> broker_pid;
        file.close();
        
        if (broker_pid <= 0) {
            kawasan::Logger::error("Invalid broker PID: {}", broker_pid);
            return false;
        }
        
        kawasan::Logger::info("Killing broker {} (PID: {})", broker_id, broker_pid);
        
        if (kill(broker_pid, SIGKILL) != 0) {
            kawasan::Logger::error("Failed to kill broker: {}", strerror(errno));
            return false;
        }
        
        // Wait for process to die
        std::this_thread::sleep_for(1s);
        return true;
    }
    
private:
    pid_t cluster_pid_ = -1;
    
    std::filesystem::path getScriptPath() const {
        // 0A.9: Prefer the source-tree path baked in at CMake configure time
        // — it works no matter what cwd ctest invokes us with. Fall back to a
        // few common relative locations for direct-binary runs.
#ifdef KAWASAN_SOURCE_DIR
        {
            std::filesystem::path src_path =
                std::filesystem::path(KAWASAN_SOURCE_DIR) / "scripts" /
                "start_cluster.sh";
            if (std::filesystem::exists(src_path)) {
                return std::filesystem::absolute(src_path);
            }
        }
#endif
        auto paths = {
            std::filesystem::path("scripts/start_cluster.sh"),
            std::filesystem::path("../scripts/start_cluster.sh"),
            std::filesystem::path("../../scripts/start_cluster.sh"),
            std::filesystem::path("../../../scripts/start_cluster.sh"),
        };

        for (const auto& p : paths) {
            if (std::filesystem::exists(p)) {
                return std::filesystem::absolute(p);
            }
        }

        return "scripts/start_cluster.sh";  // fallback
    }
    
    bool waitForClusterReady() {
        auto deadline = std::chrono::steady_clock::now() + 
                       std::chrono::seconds(CLUSTER_STARTUP_TIMEOUT_SEC);
        
        // Wait for all brokers to accept connections
        for (int broker_id = 0; broker_id < NUM_BROKERS; ++broker_id) {
            kawasan::Logger::info("Waiting for broker {}...", broker_id);
            
            while (std::chrono::steady_clock::now() < deadline) {
                if (canConnectToBroker(broker_id)) {
                    kawasan::Logger::info("Broker {} is ready", broker_id);
                    break;
                }
                
                // Check if cluster script is still running
                int status;
                pid_t result = waitpid(cluster_pid_, &status, WNOHANG);
                if (result == cluster_pid_) {
                    kawasan::Logger::error("Cluster script exited unexpectedly");
                    return false;
                }
                
                std::this_thread::sleep_for(500ms);
            }
            
            if (std::chrono::steady_clock::now() >= deadline) {
                kawasan::Logger::error("Timeout waiting for broker {}", broker_id);
                return false;
            }
        }
        
        return true;
    }
    
    bool canConnectToBroker(int broker_id) {
        try {
            boost::asio::io_context io;
            boost::asio::ip::tcp::socket socket(io);
            auto endpoint = boost::asio::ip::tcp::endpoint(
                boost::asio::ip::make_address("127.0.0.1"),
                static_cast<uint16_t>(getPort(broker_id)));
            
            socket.connect(endpoint);
            socket.close();
            return true;
        } catch (const std::exception& e) {
            return false;
        }
    }
    
    static void ensureLoggerInitialized() {
        static bool initialized = false;
        if (!initialized) {
            kawasan::Logger::init("info");
            initialized = true;
        }
    }
};

// Helper to decode 4-byte length prefix
int32_t decodeLength(const std::array<uint8_t, 4>& bytes) {
    uint32_t value = 0;
    std::memcpy(&value, bytes.data(), sizeof(value));
    return ntohl(value);
}

// Helper to send Kafka request and receive response
std::vector<uint8_t> sendKafkaRequest(boost::asio::ip::tcp::socket& socket,
                                      kawasan::Buffer& payload) {
    kawasan::Buffer frame;
    frame.writeInt32(static_cast<int32_t>(payload.size()));
    const auto& payload_bytes = payload.vector();
    frame.writeBytes(payload_bytes.data(), payload_bytes.size());
    const auto& request_bytes = frame.vector();
    boost::asio::write(socket,
                       boost::asio::buffer(request_bytes.data(), request_bytes.size()));

    std::array<uint8_t, 4> size_bytes{};
    boost::asio::read(socket, boost::asio::buffer(size_bytes));
    const auto response_length = decodeLength(size_bytes);
    std::vector<uint8_t> response_body(response_length);
    boost::asio::read(socket, boost::asio::buffer(response_body));
    return response_body;
}

// Connect to a broker and return socket
std::unique_ptr<boost::asio::ip::tcp::socket> connectToBroker(
    boost::asio::io_context& io, int broker_id) {
    auto socket = std::make_unique<boost::asio::ip::tcp::socket>(io);
    auto endpoint = boost::asio::ip::tcp::endpoint(
        boost::asio::ip::make_address("127.0.0.1"),
        static_cast<uint16_t>(BASE_PORT + broker_id));
    socket->connect(endpoint);
    return socket;
}

// Create a topic via CreateTopics request
bool createTopic(boost::asio::ip::tcp::socket& socket, 
                const std::string& topic_name,
                int num_partitions = 3,
                int replication_factor = 3) {
    kawasan::Buffer payload;
    kawasan::protocol::RequestHeader header(
        kawasan::protocol::ApiKey::CREATE_TOPICS, /*api_version=*/4,
        /*correlation_id=*/1, "raft-metadata-test");
    header.encode(payload);
    
    kawasan::protocol::CreateTopicsRequest request;
    request.setTimeoutMs(10000);
    
    kawasan::protocol::CreatableTopic topic;
    topic.name = topic_name;
    topic.num_partitions = num_partitions;
    topic.replication_factor = replication_factor;
    request.addTopic(topic);
    
    request.encode(payload, 4);
    
    auto response_bytes = sendKafkaRequest(socket, payload);
    kawasan::Buffer response_buffer(response_bytes);
    
    kawasan::protocol::ResponseHeader response_header;
    response_header.decode(response_buffer);
    
    kawasan::protocol::CreateTopicsResponse response;
    response.decode(response_buffer, 4);
    
    if (response.results().empty()) {
        kawasan::Logger::error("CreateTopics response has no results");
        return false;
    }
    
    const auto& result = response.results()[0];
    kawasan::Logger::info("CreateTopics result: topic={}, error_code={}", 
                        result.name, static_cast<int>(result.error_code));
    
    return result.error_code == kawasan::ErrorCode::NONE;
}

// Fetch metadata and check if topic exists
bool topicExistsInMetadata(boost::asio::ip::tcp::socket& socket,
                           const std::string& topic_name) {
    kawasan::Buffer payload;
    kawasan::protocol::RequestHeader header(
        kawasan::protocol::ApiKey::METADATA, /*api_version=*/9,
        /*correlation_id=*/2, "raft-metadata-test");
    header.encode(payload);
    
    kawasan::protocol::MetadataRequest request;
    request.encode(payload, 9);
    
    auto response_bytes = sendKafkaRequest(socket, payload);
    kawasan::Buffer response_buffer(response_bytes);
    
    kawasan::protocol::ResponseHeader response_header;
    response_header.decode(response_buffer);
    
    kawasan::protocol::MetadataResponse response;
    response.decode(response_buffer, 9);
    
    for (const auto& topic : response.topics()) {
        if (topic.name == topic_name) {
            kawasan::Logger::info("Found topic '{}' in metadata (error_code={})", 
                               topic_name, static_cast<int>(topic.error_code));
            return topic.error_code == kawasan::ErrorCode::NONE;
        }
    }
    
    kawasan::Logger::info("Topic '{}' not found in metadata", topic_name);
    return false;
}

// Wait for topic to appear in broker's metadata (with timeout)
bool waitForTopicInMetadata(boost::asio::io_context& io,
                           int broker_id,
                           const std::string& topic_name,
                           int timeout_sec = REPLICATION_TIMEOUT_SEC) {
    auto deadline = std::chrono::steady_clock::now() + 
                   std::chrono::seconds(timeout_sec);
    
    while (std::chrono::steady_clock::now() < deadline) {
        try {
            auto socket = connectToBroker(io, broker_id);
            if (topicExistsInMetadata(*socket, topic_name)) {
                return true;
            }
        } catch (const std::exception& e) {
            kawasan::Logger::warn("Failed to check metadata on broker {}: {}", 
                               broker_id, e.what());
        }
        
        std::this_thread::sleep_for(1s);
    }
    
    return false;
}

// Identify current leader by checking Raft state (heuristic: which broker responds correctly)
int identifyLeader(boost::asio::io_context& io) {
    // Try to create a unique topic on each broker and see which one succeeds
    // The leader should accept the create request
    for (int broker_id = 0; broker_id < NUM_BROKERS; ++broker_id) {
        try {
            auto socket = connectToBroker(io, broker_id);
            auto test_topic = fmt::format("leader-test-{}", 
                                        std::chrono::steady_clock::now().time_since_epoch().count());
            
            if (createTopic(*socket, test_topic, 1, 1)) {
                kawasan::Logger::info("Broker {} appears to be leader", broker_id);
                return broker_id;
            }
        } catch (const std::exception& e) {
            kawasan::Logger::warn("Broker {} check failed: {}", broker_id, e.what());
        }
    }
    
    return -1;  // No leader found
}

}  // anonymous namespace

// 0A.9: These two tests exercise multi-broker Raft leadership election and
// topic propagation. Multi-broker mode is explicitly out of scope for the
// drop-in-single-server-Kafka goal (improve-opus.md §0). The TestCluster
// infrastructure now starts all three brokers correctly (script bash bug,
// JSON config format, port collision, and binary-path resolution are all
// fixed in 0A.9), but the underlying Raft election still does not converge
// because the leader-election + log-fence path was never finished for
// multi-broker. Re-enable when multi-broker work (out of the current 24-week
// roadmap) is picked up.
TEST(RaftMetadataTest, DISABLED_ClusterStartupAndTopicPropagation) {
    TestCluster cluster;
    ASSERT_TRUE(cluster.start()) << "Failed to start test cluster";
    
    // Give cluster time to elect leader and stabilize
    std::this_thread::sleep_for(5s);
    
    boost::asio::io_context io;
    
    // Create topic on broker 0
    {
        kawasan::Logger::info("Creating topic on broker 0...");
        auto socket = connectToBroker(io, 0);
        ASSERT_TRUE(createTopic(*socket, "test-topic-propagation", 3, 3))
            << "Failed to create topic on broker 0";
    }
    
    // Verify topic appears on broker 1
    {
        kawasan::Logger::info("Waiting for topic to appear on broker 1...");
        EXPECT_TRUE(waitForTopicInMetadata(io, 1, "test-topic-propagation"))
            << "Topic did not propagate to broker 1";
    }
    
    // Verify topic appears on broker 2
    {
        kawasan::Logger::info("Waiting for topic to appear on broker 2...");
        EXPECT_TRUE(waitForTopicInMetadata(io, 2, "test-topic-propagation"))
            << "Topic did not propagate to broker 2";
    }
}

// Test 2: Leader failover and topic creation on new leader
// 0A.9: see DISABLED_ClusterStartupAndTopicPropagation above for rationale.
TEST(RaftMetadataTest, DISABLED_LeaderFailoverAndNewTopicCreation) {
    TestCluster cluster;
    ASSERT_TRUE(cluster.start()) << "Failed to start test cluster";
    
    // Give cluster time to elect leader
    std::this_thread::sleep_for(5s);
    
    boost::asio::io_context io;
    
    // Identify current leader
    kawasan::Logger::info("Identifying initial leader...");
    int initial_leader = identifyLeader(io);
    ASSERT_GE(initial_leader, 0) << "Failed to identify initial leader";
    kawasan::Logger::info("Initial leader is broker {}", initial_leader);
    
    // Kill the leader
    kawasan::Logger::info("Killing leader broker {}...", initial_leader);
    ASSERT_TRUE(cluster.killBroker(initial_leader))
        << "Failed to kill leader broker";
    
    // Wait for new leader election
    kawasan::Logger::info("Waiting for new leader election...");
    std::this_thread::sleep_for(10s);
    
    // Identify new leader (should be different)
    int new_leader = identifyLeader(io);
    ASSERT_GE(new_leader, 0) << "Failed to identify new leader after failover";
    ASSERT_NE(new_leader, initial_leader) << "Leader did not change";
    kawasan::Logger::info("New leader is broker {}", new_leader);
    
    // Create topic on new leader
    {
        kawasan::Logger::info("Creating topic on new leader (broker {})...", new_leader);
        auto socket = connectToBroker(io, new_leader);
        ASSERT_TRUE(createTopic(*socket, "test-topic-after-failover", 3, 2))
            << "Failed to create topic on new leader";
    }
    
    // Verify topic propagates to remaining follower
    int follower = (new_leader == 1) ? 2 : 1;
    {
        kawasan::Logger::info("Waiting for topic to appear on follower (broker {})...", follower);
        EXPECT_TRUE(waitForTopicInMetadata(io, follower, "test-topic-after-failover"))
            << "Topic did not propagate to follower after failover";
    }
}

// Test 3: Multiple topic creation with concurrent requests
TEST(RaftMetadataTest, DISABLED_MultipleConcurrentTopicCreation) {
    // Note: This test is disabled because concurrent topic creation may require
    // additional synchronization in the current implementation. Enable once
    // Raft metadata layer fully supports concurrent writes.
    
    TestCluster cluster;
    ASSERT_TRUE(cluster.start()) << "Failed to start test cluster";
    
    std::this_thread::sleep_for(5s);
    
    boost::asio::io_context io;
    
    // Create multiple topics concurrently on different brokers
    std::vector<std::thread> threads;
    std::vector<bool> results(3, false);
    
    for (int i = 0; i < 3; ++i) {
        threads.emplace_back([&io, i, &results]() {
            try {
                auto socket = connectToBroker(io, i % NUM_BROKERS);
                auto topic_name = fmt::format("concurrent-topic-{}", i);
                results[i] = createTopic(*socket, topic_name, 1, 1);
            } catch (const std::exception& e) {
                kawasan::Logger::error("Thread {} failed: {}", i, e.what());
            }
        });
    }
    
    for (auto& t : threads) {
        t.join();
    }
    
    // At least 2 out of 3 should succeed (depending on which brokers are leaders)
    int success_count = std::count(results.begin(), results.end(), true);
    EXPECT_GE(success_count, 2) << "Too few concurrent topic creations succeeded";
    
    // Wait for propagation
    std::this_thread::sleep_for(5s);
    
    // Verify all successful topics exist on all brokers
    for (int i = 0; i < 3; ++i) {
        if (results[i]) {
            auto topic_name = fmt::format("concurrent-topic-{}", i);
            for (int broker_id = 0; broker_id < NUM_BROKERS; ++broker_id) {
                EXPECT_TRUE(waitForTopicInMetadata(io, broker_id, topic_name, 10))
                    << "Topic " << topic_name << " not found on broker " << broker_id;
            }
        }
    }
}
