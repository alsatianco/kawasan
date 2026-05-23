#pragma once

#include "topology.h"
#include <string>
#include <vector>
#include <memory>
#include <map>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>

namespace kawasan {
namespace streams {

// Forward declarations - these will be actual types when client library is implemented
// For now, using void* to avoid incomplete type errors
// class Consumer;
// class Producer;
// class StateStore;

/**
 * Topic-partition pair
 */
struct TopicPartition {
    std::string topic;
    int32_t partition;
    
    bool operator<(const TopicPartition& other) const {
        if (topic != other.topic) {
            return topic < other.topic;
        }
        return partition < other.partition;
    }
    
    bool operator==(const TopicPartition& other) const {
        return topic == other.topic && partition == other.partition;
    }
};

/**
 * StreamTask processes a subset of input partitions.
 * 
 * Each task:
 * - Consumes from one or more input partitions
 * - Maintains its own state stores
 * - Processes records through the topology
 * - Produces to output topics
 * - Commits offsets periodically
 */
class StreamTask {
public:
    /**
     * Create a stream task
     * 
     * @param taskId Unique task identifier (e.g., "0_0" for subtopology 0, task 0)
     * @param partitions Input partitions assigned to this task
     * @param topology Topology to process (does not take ownership)
     * @param stateDir Directory for state stores
     * @param config Task configuration
     */
    StreamTask(std::string taskId,
              std::vector<TopicPartition> partitions,
              const Topology* topology,
              std::string stateDir,
              std::map<std::string, std::string> config);
    
    ~StreamTask();
    
    /**
     * Initialize the task
     * 
     * - Create consumer and producer
     * - Initialize state stores
     * - Restore state from changelogs
     */
    void initialize();
    
    /**
     * Process one iteration
     * 
     * - Poll consumer for records
     * - Process each record through the topology
     * - Produce output records
     * 
     * @param timeoutMs Poll timeout in milliseconds
     * @return Number of records processed
     */
    int process(int timeoutMs);
    
    /**
     * Commit offsets and state
     * 
     * - Flush state stores to disk
     * - Produce pending changelog records
     * - Commit consumer offsets
     */
    void commit();
    
    /**
     * Close the task
     * 
     * - Commit offsets and state
     * - Close state stores
     * - Close consumer and producer
     */
    void close();
    
    /**
     * Get task ID
     */
    const std::string& taskId() const { return taskId_; }
    
    /**
     * Get assigned partitions
     */
    const std::vector<TopicPartition>& partitions() const { return partitions_; }
    
    /**
     * Check if task is running
     */
    bool isRunning() const { return running_; }
    
private:
    std::string taskId_;
    std::vector<TopicPartition> partitions_;
    const Topology* topology_;
    std::string stateDir_;
    std::map<std::string, std::string> config_;
    
    // Runtime state
    bool running_ = false;
    // std::unique_ptr<Consumer> consumer_;  // TODO: Uncomment when Consumer exists
    // std::unique_ptr<Producer> producer_;  // TODO: Uncomment when Producer exists
    // std::map<std::string, std::shared_ptr<StateStore>> stateStores_;  // TODO: Uncomment
    
    // Helper methods
    void initializeConsumer();
    void initializeProducer();
    void initializeStateStores();
    void restoreStateStores();
    void processRecord(const std::string& key, const std::string& value);
};

/**
 * StreamThread runs a task processing loop.
 * 
 * Each thread:
 * - Manages one or more StreamTasks
 * - Runs a continuous poll-process-commit loop
 * - Handles rebalancing and task reassignment
 */
class StreamThread {
public:
    /**
     * Create a stream thread
     * 
     * @param threadId Thread identifier
     * @param tasks Initial tasks to run
     * @param config Thread configuration
     */
    StreamThread(std::string threadId,
                std::vector<std::shared_ptr<StreamTask>> tasks,
                std::map<std::string, std::string> config);
    
    ~StreamThread();
    
    /**
     * Start the thread
     */
    void start();
    
    /**
     * Stop the thread
     * 
     * @param timeoutMs Maximum time to wait for graceful shutdown
     */
    void stop(int timeoutMs = 30000);
    
    /**
     * Check if thread is running
     */
    bool isRunning() const { return running_; }
    
    /**
     * Get thread ID
     */
    const std::string& threadId() const { return threadId_; }
    
    /**
     * Add a task to this thread
     */
    void addTask(std::shared_ptr<StreamTask> task);
    
    /**
     * Remove a task from this thread
     */
    void removeTask(const std::string& taskId);
    
private:
    std::string threadId_;
    std::vector<std::shared_ptr<StreamTask>> tasks_;
    std::map<std::string, std::string> config_;
    
    // Runtime state
    bool running_ = false;
    std::unique_ptr<std::thread> thread_;
    
    // Thread entry point
    void run();
    
    // Helper methods
    void commitAll();
};

} // namespace streams
} // namespace kawasan
