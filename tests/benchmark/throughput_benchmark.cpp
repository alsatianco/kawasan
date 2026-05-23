/**
 * Throughput Performance Benchmark
 * 
 * This benchmark measures the raw throughput of a single-broker Kawasan instance
 * to establish a performance baseline and identify hotspots.
 * 
 * Benchmark scenario:
 * - Single broker
 * - Single topic with 10 partitions  
 * - Produce 10M messages (100 bytes each)
 * - Measure throughput (msg/sec) and latency (p50, p99, p999)
 * 
 * Target: Establish baseline for optimization (aim for 100k+ msg/sec after optimization)
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
#include <iomanip>
#include <iostream>
#include <numeric>
#include <string>
#include <thread>
#include <vector>

#include "kawasan/client/producer.h"
#include "kawasan/common/logger.h"

namespace {

using namespace std::chrono_literals;

// Benchmark configuration
constexpr int BROKER_PORT = 9092;
constexpr int BROKER_STARTUP_TIMEOUT_SEC = 30;
constexpr int64_t TOTAL_MESSAGES = 1000000; // 1M messages (reduced from 10M for faster benchmarking)
constexpr int BATCH_SIZE = 100;
constexpr int MESSAGE_SIZE = 100; // bytes
constexpr int NUM_PARTITIONS = 10;
constexpr int REPLICATION_FACTOR = 1; // single broker

// Benchmark results structure
struct BenchmarkResults {
    int64_t total_messages = 0;
    int64_t total_bytes = 0;
    double duration_sec = 0.0;
    double throughput_msg_sec = 0.0;
    double throughput_mb_sec = 0.0;
    
    std::vector<double> latencies_ms; // per-message latencies
    double p50_latency_ms = 0.0;
    double p95_latency_ms = 0.0;
    double p99_latency_ms = 0.0;
    double p999_latency_ms = 0.0;
    double avg_latency_ms = 0.0;
};

// Single broker manager
class BrokerInstance {
public:
    BrokerInstance() = default;
    
    ~BrokerInstance() {
        stop();
    }
    
    bool start() {
        std::cout << "Starting single broker on port " << BROKER_PORT << "..." << std::endl;
        
        // Clean up old data
        std::filesystem::remove_all("/tmp/kawasan-benchmark");
        
        auto broker_path = getBrokerPath();
        if (!std::filesystem::exists(broker_path)) {
            std::cerr << "Broker binary not found: " << broker_path << std::endl;
            return false;
        }
        
        auto config_path = getConfigPath();
        if (!std::filesystem::exists(config_path)) {
            std::cerr << "Config file not found: " << config_path << std::endl;
            return false;
        }
        
        broker_pid_ = fork();
        if (broker_pid_ < 0) {
            std::cerr << "Failed to fork: " << strerror(errno) << std::endl;
            return false;
        }
        
        if (broker_pid_ == 0) {
            // Child: execute broker
            auto log_path = "/tmp/kawasan-benchmark-broker.log";
            int log_fd = open(log_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
            if (log_fd >= 0) {
                dup2(log_fd, STDOUT_FILENO);
                dup2(log_fd, STDERR_FILENO);
                close(log_fd);
            }
            
            // Override data directories for benchmark
            setenv("LOG_DIR", "/tmp/kawasan-benchmark/data", 1);
            setenv("METADATA_DIR", "/tmp/kawasan-benchmark/meta", 1);
            
            execl(broker_path.c_str(), broker_path.c_str(), 
                  "--config", config_path.c_str(), nullptr);
            
            std::cerr << "Failed to exec broker" << std::endl;
            _exit(1);
        }
        
        // Parent: wait for broker ready
        if (!waitForBrokerReady()) {
            std::cerr << "Broker failed to start" << std::endl;
            stop();
            return false;
        }
        
        std::cout << "Broker is ready" << std::endl;
        return true;
    }
    
    void stop() {
        if (broker_pid_ > 0) {
            std::cout << "Stopping broker..." << std::endl;
            kill(broker_pid_, SIGTERM);
            
            int status;
            int retry = 0;
            while (retry++ < 10) {
                if (waitpid(broker_pid_, &status, WNOHANG) > 0) {
                    break;
                }
                std::this_thread::sleep_for(500ms);
            }
            
            if (retry >= 10) {
                kill(broker_pid_, SIGKILL);
                waitpid(broker_pid_, &status, 0);
            }
            
            broker_pid_ = -1;
        }
    }

private:
    pid_t broker_pid_ = -1;
    
    std::filesystem::path getBrokerPath() const {
        auto current_path = std::filesystem::current_path();
        auto broker_path = current_path / ".." / ".." / "tools" / "kawasan-broker";
        if (std::filesystem::exists(broker_path)) {
            return std::filesystem::canonical(broker_path);
        }
        // Try alternative path for build directory execution
        broker_path = current_path / "tools" / "kawasan-broker";
        if (std::filesystem::exists(broker_path)) {
            return broker_path;
        }
        return "/usr/local/bin/kawasan-broker";
    }
    
    std::filesystem::path getConfigPath() const {
        auto current_path = std::filesystem::current_path();
        auto config_path = current_path / ".." / ".." / "config" / "broker.macos.properties";
        if (std::filesystem::exists(config_path)) {
            return std::filesystem::canonical(config_path);
        }
        config_path = current_path / "config" / "broker.macos.properties";
        if (std::filesystem::exists(config_path)) {
            return config_path;
        }
        return "/etc/kawasan/server.properties";
    }
    
    bool waitForBrokerReady() {
        auto start = std::chrono::steady_clock::now();
        
        while (true) {
            auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::steady_clock::now() - start).count();
            
            if (elapsed > BROKER_STARTUP_TIMEOUT_SEC) {
                return false;
            }
            
            if (canConnectToBroker()) {
                // Give broker a bit more time to fully initialize
                std::this_thread::sleep_for(2s);
                return true;
            }
            
            std::this_thread::sleep_for(1s);
        }
    }
    
    bool canConnectToBroker() {
        // Use nc command to check if port is open
        std::string cmd = "nc -z -w 1 127.0.0.1 " + std::to_string(BROKER_PORT) + " 2>/dev/null";
        return system(cmd.c_str()) == 0;
    }
};

// Create topic using kawasan-topics CLI
bool createTopic(const std::string& topic_name) {
    std::filesystem::path topics_tool;
    auto current_path = std::filesystem::current_path();
    
    auto candidate1 = current_path / ".." / ".." / "tools" / "kawasan-topics";
    if (std::filesystem::exists(candidate1)) {
        topics_tool = std::filesystem::canonical(candidate1);
    } else {
        auto candidate2 = current_path / "tools" / "kawasan-topics";
        if (std::filesystem::exists(candidate2)) {
            topics_tool = candidate2;
        } else {
            std::cerr << "kawasan-topics not found" << std::endl;
            return false;
        }
    }
    
    std::string cmd = topics_tool.string() + 
                     " --bootstrap-server localhost:" + std::to_string(BROKER_PORT) +
                     " --create --topic " + topic_name +
                     " --partitions " + std::to_string(NUM_PARTITIONS) +
                     " --replication-factor " + std::to_string(REPLICATION_FACTOR) +
                     " 2>&1";
    
    int result = system(cmd.c_str());
    return result == 0;
}

// Run the throughput benchmark
BenchmarkResults runBenchmark() {
    BenchmarkResults results;
    
    std::cout << "\n========================================" << std::endl;
    std::cout << "Starting Throughput Benchmark" << std::endl;
    std::cout << "========================================" << std::endl;
    std::cout << "Total messages: " << TOTAL_MESSAGES << std::endl;
    std::cout << "Batch size: " << BATCH_SIZE << std::endl;
    std::cout << "Message size: " << MESSAGE_SIZE << " bytes" << std::endl;
    std::cout << "Partitions: " << NUM_PARTITIONS << std::endl;
    std::cout << "========================================\n" << std::endl;
    
    // Create topic
    std::cout << "Creating topic 'benchmark-throughput' with " << NUM_PARTITIONS 
              << " partitions..." << std::endl;
    
    if (!createTopic("benchmark-throughput")) {
        std::cerr << "Failed to create topic" << std::endl;
        return results;
    }
    
    std::this_thread::sleep_for(2s);
    std::cout << "Topic created successfully\n" << std::endl;
    
    // Initialize producer
    kawasan::client::ProducerConfig config;
    config.bootstrap_servers = "localhost:" + std::to_string(BROKER_PORT);
    kawasan::client::Producer producer(config);
    
    // Produce messages
    std::cout << "Producing " << TOTAL_MESSAGES << " messages..." << std::endl;
    
    std::string message_value(MESSAGE_SIZE, 'x');
    auto start_time = std::chrono::high_resolution_clock::now();
    int64_t messages_sent = 0;
    
    while (messages_sent < TOTAL_MESSAGES) {
        auto msg_start = std::chrono::high_resolution_clock::now();
        
        try {
            auto future = producer.send("benchmark-throughput", "", message_value);
            auto metadata = future.get();  // Wait for acknowledgment
            
            auto msg_end = std::chrono::high_resolution_clock::now();
            auto msg_duration = std::chrono::duration<double, std::milli>(
                msg_end - msg_start).count();
            
            results.latencies_ms.push_back(msg_duration);
            messages_sent++;
            
            // Progress indicator
            if (messages_sent % 100000 == 0) {
                double progress = (100.0 * messages_sent) / TOTAL_MESSAGES;
                auto elapsed = std::chrono::duration<double>(
                    std::chrono::high_resolution_clock::now() - start_time).count();
                double current_rate = messages_sent / elapsed;
                
                std::cout << "Progress: " << std::fixed << std::setprecision(1) 
                         << progress << "% (" << messages_sent << " messages, "
                         << static_cast<int>(current_rate) << " msg/sec)" 
                         << std::endl;
            }
        } catch (const std::exception& e) {
            std::cerr << "Error sending message: " << e.what() << std::endl;
        }
    }
    
    auto end_time = std::chrono::high_resolution_clock::now();
    
    // Close producer
    producer.close();
    
    // Calculate results
    results.total_messages = messages_sent;
    results.total_bytes = messages_sent * MESSAGE_SIZE;
    results.duration_sec = std::chrono::duration<double>(
        end_time - start_time).count();
    
    results.throughput_msg_sec = results.total_messages / results.duration_sec;
    results.throughput_mb_sec = (results.total_bytes / (1024.0 * 1024.0)) / 
                                results.duration_sec;
    
    // Calculate latency percentiles
    if (!results.latencies_ms.empty()) {
        std::sort(results.latencies_ms.begin(), results.latencies_ms.end());
        
        size_t p50_idx = results.latencies_ms.size() * 50 / 100;
        size_t p95_idx = results.latencies_ms.size() * 95 / 100;
        size_t p99_idx = results.latencies_ms.size() * 99 / 100;
        size_t p999_idx = results.latencies_ms.size() * 999 / 1000;
        
        results.p50_latency_ms = results.latencies_ms[p50_idx];
        results.p95_latency_ms = results.latencies_ms[p95_idx];
        results.p99_latency_ms = results.latencies_ms[p99_idx];
        results.p999_latency_ms = results.latencies_ms[p999_idx];
        
        results.avg_latency_ms = std::accumulate(
            results.latencies_ms.begin(), results.latencies_ms.end(), 0.0) /
            results.latencies_ms.size();
    }
    
    return results;
}

// Display results
void displayResults(const BenchmarkResults& results) {
    std::cout << "\n========================================" << std::endl;
    std::cout << "Benchmark Results" << std::endl;
    std::cout << "========================================" << std::endl;
    std::cout << std::fixed << std::setprecision(2);
    std::cout << "Total Messages:     " << results.total_messages << std::endl;
    std::cout << "Total Data:         " << (results.total_bytes / (1024.0 * 1024.0)) 
              << " MB" << std::endl;
    std::cout << "Duration:           " << results.duration_sec << " seconds" << std::endl;
    std::cout << "\nThroughput:" << std::endl;
    std::cout << "  Messages/sec:     " << results.throughput_msg_sec << std::endl;
    std::cout << "  MB/sec:           " << results.throughput_mb_sec << std::endl;
    std::cout << "\nLatency (per message):" << std::endl;
    std::cout << "  Average:          " << results.avg_latency_ms << " ms" << std::endl;
    std::cout << "  p50:              " << results.p50_latency_ms << " ms" << std::endl;
    std::cout << "  p95:              " << results.p95_latency_ms << " ms" << std::endl;
    std::cout << "  p99:              " << results.p99_latency_ms << " ms" << std::endl;
    std::cout << "  p999:             " << results.p999_latency_ms << " ms" << std::endl;
    std::cout << "========================================\n" << std::endl;
}

// Append results to PERFORMANCE.md
void saveResults(const BenchmarkResults& results) {
    auto now = std::chrono::system_clock::now();
    auto time_t_now = std::chrono::system_clock::to_time_t(now);
    std::tm tm_now = *std::localtime(&time_t_now);
    
    std::ostringstream timestamp;
    timestamp << std::put_time(&tm_now, "%Y-%m-%d %H:%M:%S");
    
    // Find PERFORMANCE.md
    std::filesystem::path perf_file;
    auto current_path = std::filesystem::current_path();
    
    auto candidate1 = current_path / ".." / ".." / "docs" / "PERFORMANCE.md";
    if (std::filesystem::exists(candidate1)) {
        perf_file = std::filesystem::canonical(candidate1);
    } else {
        auto candidate2 = current_path / "docs" / "PERFORMANCE.md";
        if (std::filesystem::exists(candidate2)) {
            perf_file = candidate2;
        } else {
            perf_file = "/tmp/PERFORMANCE.md";
        }
    }
    
    std::ofstream file(perf_file, std::ios::app);
    if (!file) {
        std::cerr << "Failed to open PERFORMANCE.md for writing" << std::endl;
        return;
    }
    
    file << "\n## Throughput Benchmark - " << timestamp.str() << "\n\n";
    file << "**Configuration:**\n";
    file << "- Single broker\n";
    file << "- Topic: 'benchmark-throughput' with " << NUM_PARTITIONS << " partitions\n";
    file << "- Total messages: " << TOTAL_MESSAGES << "\n";
    file << "- Message size: " << MESSAGE_SIZE << " bytes\n";
    file << "- Replication factor: " << REPLICATION_FACTOR << "\n\n";
    
    file << "**Results:**\n";
    file << "- Total data: " << std::fixed << std::setprecision(2) 
         << (results.total_bytes / (1024.0 * 1024.0)) << " MB\n";
    file << "- Duration: " << results.duration_sec << " seconds\n";
    file << "- **Throughput: " << results.throughput_msg_sec << " msg/sec** ("
         << results.throughput_mb_sec << " MB/sec)\n\n";
    
    file << "**Latency (per message):**\n";
    file << "- Average: " << results.avg_latency_ms << " ms\n";
    file << "- p50: " << results.p50_latency_ms << " ms\n";
    file << "- p95: " << results.p95_latency_ms << " ms\n";
    file << "- p99: " << results.p99_latency_ms << " ms\n";
    file << "- p999: " << results.p999_latency_ms << " ms\n\n";
    
    file << "**Status:** ";
    if (results.throughput_msg_sec >= 100000) {
        file << "✅ Target achieved (100k+ msg/sec)\n\n";
    } else {
        file << "⏳ Below target (baseline: " << results.throughput_msg_sec 
             << " msg/sec, target: 100k+ msg/sec)\n\n";
    }
    
    file << "---\n";
    file.close();
    
    std::cout << "Results appended to " << perf_file << std::endl;
}

} // anonymous namespace

int main() {
    // Start broker
    BrokerInstance broker;
    if (!broker.start()) {
        std::cerr << "Failed to start broker" << std::endl;
        return 1;
    }
    
    // Run benchmark
    auto results = runBenchmark();
    
    // Display results
    displayResults(results);
    
    // Save results
    saveResults(results);
    
    // Stop broker
    broker.stop();
    
    // Cleanup
    std::filesystem::remove_all("/tmp/kawasan-benchmark");
    
    std::cout << "Benchmark completed successfully!" << std::endl;
    return 0;
}
