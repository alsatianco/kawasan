#include "kawasan/streams/kawasan_streams.h"
#include <spdlog/spdlog.h>
#include <stdexcept>
#include <algorithm>

namespace kawasan {
namespace streams {

KawasanStreams::KawasanStreams(Topology topology, Properties config)
    : topology_(std::move(topology)), config_(std::move(config)) {
    
    parseConfig();
    validateConfig();
    
    spdlog::info("Created KawasanStreams application: {}", applicationId_);
}

KawasanStreams::KawasanStreams(Topology&& topology, Properties config)
    : topology_(std::move(topology)), config_(std::move(config)) {
    
    parseConfig();
    validateConfig();
    
    spdlog::info("Created KawasanStreams application: {}", applicationId_);
}

KawasanStreams::~KawasanStreams() {
    if (state_ == State::RUNNING) {
        try {
            close();
        } catch (...) {
            // Ignore errors in destructor
        }
    }
}

void KawasanStreams::parseConfig() {
    // Required: application.id
    auto it = config_.find("application.id");
    if (it == config_.end()) {
        throw std::runtime_error("Missing required config: application.id");
    }
    applicationId_ = it->second;
    
    // Required: bootstrap.servers
    it = config_.find("bootstrap.servers");
    if (it == config_.end()) {
        throw std::runtime_error("Missing required config: bootstrap.servers");
    }
    bootstrapServers_ = it->second;
    
    // Optional: num.stream.threads (default: 1)
    it = config_.find("num.stream.threads");
    if (it != config_.end()) {
        numStreamThreads_ = std::stoi(it->second);
    }
    
    // Optional: state.dir (default: /tmp/kafka-streams/<application.id>)
    it = config_.find("state.dir");
    if (it != config_.end()) {
        stateDir_ = it->second;
    } else {
        stateDir_ = "/tmp/kafka-streams/" + applicationId_;
    }
    
    // Optional: commit.interval.ms (default: 30000)
    it = config_.find("commit.interval.ms");
    if (it != config_.end()) {
        commitIntervalMs_ = std::stoi(it->second);
    }
}

void KawasanStreams::validateConfig() {
    if (applicationId_.empty()) {
        throw std::runtime_error("application.id cannot be empty");
    }
    
    if (bootstrapServers_.empty()) {
        throw std::runtime_error("bootstrap.servers cannot be empty");
    }
    
    if (numStreamThreads_ < 1) {
        throw std::runtime_error("num.stream.threads must be >= 1");
    }
    
    if (commitIntervalMs_ < 0) {
        throw std::runtime_error("commit.interval.ms must be >= 0");
    }
}

void KawasanStreams::start() {
    if (state_ == State::RUNNING) {
        throw std::runtime_error("KawasanStreams is already running");
    }
    
    spdlog::info("[{}] Starting KawasanStreams application", applicationId_);
    
    try {
        // Validate topology
        if (!topology_.validate()) {
            throw std::runtime_error("Invalid topology");
        }
        
        spdlog::info("[{}] Topology description:\n{}", 
                     applicationId_, topology_.describe());
        
        // Create tasks from topology
        createTasks();
        
        // Assign tasks to threads
        assignTasksToThreads();
        
        // Start all threads
        startThreads();
        
        state_ = State::RUNNING;
        spdlog::info("[{}] KawasanStreams application started successfully", 
                     applicationId_);
        
    } catch (const std::exception& e) {
        state_ = State::ERROR;
        spdlog::error("[{}] Failed to start KawasanStreams: {}", 
                     applicationId_, e.what());
        throw;
    }
}

void KawasanStreams::createTasks() {
    // TODO: Implement task creation from topology
    // 
    // For each source node:
    //   1. Get list of topics
    //   2. For each topic, get partition count
    //   3. Create task for each partition (or group of co-partitioned partitions)
    //   4. Assign state stores to tasks
    
    spdlog::info("[{}] Creating tasks from topology", applicationId_);
    
    // Placeholder: Create one task per thread for now
    for (int i = 0; i < numStreamThreads_; ++i) {
        std::string taskId = "0_" + std::to_string(i);
        std::vector<TopicPartition> partitions;  // Empty for now
        
        auto task = std::make_shared<StreamTask>(
            taskId,
            partitions,
            &topology_,  // Pass pointer to topology
            stateDir_,
            config_
        );
        
        tasks_.push_back(task);
    }
    
    spdlog::info("[{}] Created {} tasks", applicationId_, tasks_.size());
}

void KawasanStreams::assignTasksToThreads() {
    spdlog::info("[{}] Assigning {} tasks to {} threads", 
                 applicationId_, tasks_.size(), numStreamThreads_);
    
    // Simple round-robin assignment
    int threadIdx = 0;
    for (auto& _ : tasks_) {
        (void)_; // Mark as intentionally unused
        if (threadIdx >= static_cast<int>(threads_.size())) {
            // Create new thread
            std::string threadId = applicationId_ + "-StreamThread-" + 
                                  std::to_string(threadIdx);
            
            auto thread = std::make_unique<StreamThread>(
                threadId,
                std::vector<std::shared_ptr<StreamTask>>{},
                config_
            );
            
            threads_.push_back(std::move(thread));
        }
        
        // threads_[threadIdx]->addTask(task);  // TODO: Uncomment when StreamThread is ready
        threadIdx = (threadIdx + 1) % numStreamThreads_;
    }
    
    spdlog::info("[{}] Task assignment completed", applicationId_);
}

void KawasanStreams::startThreads() {
    spdlog::info("[{}] Starting {} stream threads", applicationId_, threads_.size());
    
    for (auto& _ : threads_) {
        (void)_; // Mark as intentionally unused
        // thread->start();  // TODO: Uncomment when StreamThread is ready
    }
    
    spdlog::info("[{}] All stream threads started", applicationId_);
}

void KawasanStreams::stopThreads(int /* timeoutMs */) {
    spdlog::info("[{}] Stopping {} stream threads", applicationId_, threads_.size());
    
    for (auto& _ : threads_) {
        (void)_; // Mark as intentionally unused
        // thread->stop(timeoutMs);  // TODO: Uncomment when StreamThread is ready
    }
    
    threads_.clear();
    
    spdlog::info("[{}] All stream threads stopped", applicationId_);
}

void KawasanStreams::close(int timeoutMs) {
    if (state_ != State::RUNNING && state_ != State::ERROR) {
        return;
    }
    
    spdlog::info("[{}] Closing KawasanStreams application", applicationId_);
    
    state_ = State::PENDING_SHUTDOWN;
    
    try {
        stopThreads(timeoutMs);
        tasks_.clear();
        
        state_ = State::CREATED;
        spdlog::info("[{}] KawasanStreams application closed", applicationId_);
        
    } catch (const std::exception& e) {
        state_ = State::ERROR;
        spdlog::error("[{}] Error closing KawasanStreams: {}", 
                     applicationId_, e.what());
        throw;
    }
}

std::map<std::string, double> KawasanStreams::metrics() const {
    // TODO: Collect metrics from all threads and tasks
    std::map<std::string, double> metrics;
    
    metrics["num-threads"] = static_cast<double>(threads_.size());
    metrics["num-tasks"] = static_cast<double>(tasks_.size());
    
    return metrics;
}

} // namespace streams
} // namespace kawasan
