#pragma once

#include "topology.h"
#include "stream_task.h"
#include <string>
#include <map>
#include <vector>
#include <memory>
#include <atomic>

namespace kawasan {
namespace streams {

/**
 * State of the KawasanStreams application
 */
enum class State {
    CREATED,        // Initial state
    REBALANCING,    // Consumer group rebalancing in progress
    RUNNING,        // Processing records
    PENDING_SHUTDOWN, // Shutdown requested
    ERROR           // Unrecoverable error occurred
};

/**
 * Properties for configuring the Streams application
 */
using Properties = std::map<std::string, std::string>;

/**
 * KawasanStreams is the main entry point for running a stream processing application.
 * 
 * Usage:
 * 
 *   StreamsBuilder builder;
 *   auto stream = builder.stream<std::string, std::string>("input");
 *   stream.filter(...).mapValues(...).to("output");
 *   Topology topology = builder.build();
 *   
 *   Properties config;
 *   config["application.id"] = "my-app";
 *   config["bootstrap.servers"] = "localhost:9092";
 *   config["num.stream.threads"] = "2";
 *   
 *   KawasanStreams streams(topology, config);
 *   streams.start();
 *   
 *   // ... let it run ...
 *   
 *   streams.close();
 */
class KawasanStreams {
public:
    /**
     * Create a KawasanStreams instance
     * 
     * @param topology Stream processing topology
     * @param config Configuration properties
     */
    KawasanStreams(Topology topology, Properties config);
    
    /**
     * Create a KawasanStreams instance (move topology)
     * 
     * @param topology Stream processing topology (moved)
     * @param config Configuration properties
     */
    KawasanStreams(Topology&& topology, Properties config);
    
    ~KawasanStreams();
    
    /**
     * Start the stream processing application
     * 
     * This method:
     * - Validates the topology
     * - Creates stream threads
     * - Assigns tasks to threads
     * - Starts processing
     * 
     * Returns immediately. Call close() to stop.
     */
    void start();
    
    /**
     * Stop the stream processing application
     * 
     * @param timeoutMs Maximum time to wait for graceful shutdown
     */
    void close(int timeoutMs = 30000);
    
    /**
     * Get the current state of the application
     */
    State state() const { return state_; }
    
    /**
     * Get the application ID
     */
    const std::string& applicationId() const { return applicationId_; }
    
    /**
     * Get metrics for the application
     * 
     * Returns a map of metric name to value.
     * Examples:
     * - "records-processed-total"
     * - "records-processed-rate"
     * - "commit-latency-avg"
     * - "commit-latency-max"
     */
    std::map<std::string, double> metrics() const;
    
private:
    Topology topology_;
    Properties config_;
    std::string applicationId_;
    std::atomic<State> state_{State::CREATED};
    
    // Stream threads
    std::vector<std::unique_ptr<StreamThread>> threads_;
    
    // Tasks (shared across threads)
    std::vector<std::shared_ptr<StreamTask>> tasks_;
    
    // Configuration
    int numStreamThreads_ = 1;
    std::string bootstrapServers_;
    std::string stateDir_;
    int commitIntervalMs_ = 30000;  // 30 seconds
    
    // Helper methods
    void parseConfig();
    void validateConfig();
    void createTasks();
    void assignTasksToThreads();
    void startThreads();
    void stopThreads(int timeoutMs);
};

} // namespace streams
} // namespace kawasan
