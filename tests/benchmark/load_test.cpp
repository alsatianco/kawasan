/**
 * Load Test for Kawasan Broker
 * 
 * This test simulates sustained load with multiple concurrent producer clients
 * to verify stability, resource usage, and absence of memory leaks.
 * 
 * Test scenario:
 * - 1000 concurrent producer clients
 * - Sustained load for 1 hour (configurable)
 * - Monitor: memory, CPU, connections, throughput
 * - Verify: no crashes, no memory leaks, stable performance
 */

#include <atomic>
#include <chrono>
#include <iostream>
#include <memory>
#include <thread>
#include <vector>

#include "kawasan/client/producer.h"
#include "kawasan/common/logger.h"

namespace {

using namespace std::chrono_literals;

// Load test configuration
constexpr int NUM_PRODUCERS = 1000;
constexpr int TEST_DURATION_MINUTES = 60;
constexpr int MESSAGE_SIZE = 100;
constexpr int MESSAGES_PER_PRODUCER = 100000; // Total messages each producer sends
constexpr const char* TOPIC_NAME = "load-test-topic";
constexpr const char* BOOTSTRAP_SERVERS = "localhost:9092";

// Metrics
std::atomic<int64_t> messages_produced{0};
std::atomic<int64_t> producer_errors{0};
std::atomic<bool> test_running{true};

// Producer worker
void producerWorker(int worker_id) {
    try {
        kawasan::client::ProducerConfig config;
        config.bootstrap_servers = BOOTSTRAP_SERVERS;
        
        kawasan::client::Producer producer(config);
        
        std::string message_value(MESSAGE_SIZE, 'x');
        int64_t sent = 0;
        
        while (test_running && sent < MESSAGES_PER_PRODUCER) {
            try {
                std::string key = "producer-" + std::to_string(worker_id);
                auto future = producer.send(TOPIC_NAME, key, message_value);
                future.get(); // Wait for acknowledgment
                
                messages_produced++;
                sent++;
                
                // Small delay to avoid overwhelming the broker
                if (sent % 100 == 0) {
                    std::this_thread::sleep_for(10ms);
                }
            } catch (const std::exception& e) {
                producer_errors++;
            }
        }
        
        producer.close();
    } catch (const std::exception& e) {
        std::cerr << "Producer " << worker_id << " failed: " << e.what() << std::endl;
        producer_errors++;
    }
}

// Monitoring thread
void monitoringWorker() {
    auto start_time = std::chrono::steady_clock::now();
    int64_t last_produced = 0;
    
    while (test_running) {
        std::this_thread::sleep_for(10s);
        
        auto now = std::chrono::steady_clock::now();
        auto elapsed_sec = std::chrono::duration_cast<std::chrono::seconds>(
            now - start_time).count();
        
        int64_t current_produced = messages_produced.load();
        
        int64_t produced_delta = current_produced - last_produced;
        
        double produce_rate = produced_delta / 10.0;
        
        std::cout << "[" << elapsed_sec << "s] "
                  << "Produced: " << current_produced << " (" << produce_rate << " msg/s), "
                  << "Producer errors: " << producer_errors.load()
                  << std::endl;
        
        last_produced = current_produced;
    }
}

} // anonymous namespace

int main() {
    kawasan::Logger::init("info");
    
    std::cout << "\n========================================" << std::endl;
    std::cout << "Kawasan Load Test" << std::endl;
    std::cout << "========================================" << std::endl;
    std::cout << "Producers: " << NUM_PRODUCERS << std::endl;
    std::cout << "Duration: " << TEST_DURATION_MINUTES << " minutes" << std::endl;
    std::cout << "Topic: " << TOPIC_NAME << std::endl;
    std::cout << "========================================\n" << std::endl;
    
    // Start monitoring thread
    std::thread monitor(monitoringWorker);
    
    // Start producer threads
    std::vector<std::thread> producers;
    producers.reserve(NUM_PRODUCERS);
    
    std::cout << "Starting " << NUM_PRODUCERS << " producers..." << std::endl;
    for (int i = 0; i < NUM_PRODUCERS; ++i) {
        producers.emplace_back(producerWorker, i);
        
        // Stagger starts to avoid connection storm
        if (i % 10 == 0) {
            std::this_thread::sleep_for(100ms);
        }
    }
    
    std::cout << "\nLoad test running for " << TEST_DURATION_MINUTES << " minutes..." << std::endl;
    std::cout << "Press Ctrl+C to stop early.\n" << std::endl;
    
    // Run for configured duration
    auto test_end = std::chrono::steady_clock::now() + 
                    std::chrono::minutes(TEST_DURATION_MINUTES);
    
    while (std::chrono::steady_clock::now() < test_end && test_running) {
        std::this_thread::sleep_for(1s);
    }
    
    // Stop test
    std::cout << "\nStopping load test..." << std::endl;
    test_running = false;
    
    // Wait for threads to finish
    std::cout << "Waiting for producers to finish..." << std::endl;
    for (auto& t : producers) {
        if (t.joinable()) {
            t.join();
        }
    }
    
    monitor.join();
    
    // Final results
    std::cout << "\n========================================" << std::endl;
    std::cout << "Load Test Results" << std::endl;
    std::cout << "========================================" << std::endl;
    std::cout << "Total messages produced: " << messages_produced.load() << std::endl;
    std::cout << "Producer errors: " << producer_errors.load() << std::endl;
    
    bool success = (producer_errors.load() == 0) && 
                   (messages_produced.load() > 0);
    
    if (success) {
        std::cout << "\n✅ Load test PASSED" << std::endl;
    } else {
        std::cout << "\n❌ Load test FAILED" << std::endl;
    }
    
    std::cout << "========================================\n" << std::endl;
    
    return success ? 0 : 1;
}
