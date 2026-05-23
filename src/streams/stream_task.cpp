#include "kawasan/streams/stream_task.h"
#include <spdlog/spdlog.h>
#include <stdexcept>

namespace kawasan {
namespace streams {

// Forward declaration for Consumer and Producer that don't exist yet
// These will be implemented when we have the actual client library
// class Consumer;
// class Producer;
// class StateStore;

StreamTask::StreamTask(std::string taskId,
                      std::vector<TopicPartition> partitions,
                      const Topology* topology,
                      std::string stateDir,
                      std::map<std::string, std::string> config)
    : taskId_(std::move(taskId)),
      partitions_(std::move(partitions)),
      topology_(topology),
      stateDir_(std::move(stateDir)),
      config_(std::move(config)) {
}

StreamTask::~StreamTask() {
    if (running_) {
        try {
            close();
        } catch (...) {
            // Ignore errors in destructor
        }
    }
}

void StreamTask::initialize() {
    spdlog::info("[Task {}] Initializing task with {} partitions", 
                 taskId_, partitions_.size());
    
    try {
        initializeConsumer();
        initializeProducer();
        initializeStateStores();
        restoreStateStores();
        
        running_ = true;
        spdlog::info("[Task {}] Task initialized successfully", taskId_);
        
    } catch (const std::exception& e) {
        spdlog::error("[Task {}] Failed to initialize task: {}", taskId_, e.what());
        throw;
    }
}

void StreamTask::initializeConsumer() {
    // TODO: Create and configure consumer
    // consumer_ = std::make_unique<Consumer>(config_);
    // consumer_->assign(partitions_);
    spdlog::debug("[Task {}] Consumer initialized (placeholder)", taskId_);
}

void StreamTask::initializeProducer() {
    // TODO: Create and configure producer
    // producer_ = std::make_unique<Producer>(config_);
    spdlog::debug("[Task {}] Producer initialized (placeholder)", taskId_);
}

void StreamTask::initializeStateStores() {
    // TODO: Create state stores from topology
    const auto& storeBuilders = topology_->storeBuilders();
    
    for (const auto& [storeName, builder] : storeBuilders) {
        spdlog::debug("[Task {}] Creating state store: {}", taskId_, storeName);
        
        std::string storePath = stateDir_ + "/" + taskId_ + "/" + storeName;
        auto store = builder->build(storePath);
        
        // stateStores_[storeName] = store;
    }
    
    spdlog::debug("[Task {}] Initialized {} state stores", 
                 taskId_, storeBuilders.size());
}

void StreamTask::restoreStateStores() {
    // TODO: Restore state from changelog topics
    // For each state store:
    //   1. Seek to beginning of changelog topic
    //   2. Read all records
    //   3. Apply to state store
    //   4. Track last offset
    
    spdlog::debug("[Task {}] State stores restored (placeholder)", taskId_);
}

int StreamTask::process(int /* timeoutMs */) {
    if (!running_) {
        throw std::runtime_error("Task is not running");
    }
    
    // TODO: Poll consumer and process records
    // auto records = consumer_->poll(timeoutMs);
    // 
    // for (auto& record : records) {
    //     processRecord(record.key, record.value);
    // }
    // 
    // return records.size();
    
    return 0;  // Placeholder
}

void StreamTask::processRecord(const std::string& /* key */, const std::string& /* value */) {
    // TODO: Process record through the topology
    // 
    // 1. Start at source nodes
    // 2. For each node:
    //    - Execute processor logic
    //    - Forward to child nodes
    // 3. Write to sink topics
    // 4. Update state stores
    // 5. Write to changelog topics
}

void StreamTask::commit() {
    if (!running_) {
        return;
    }
    
    spdlog::debug("[Task {}] Committing offsets and state", taskId_);
    
    try {
        // TODO: Flush state stores to disk
        // for (auto& [name, store] : stateStores_) {
        //     store->flush();
        // }
        
        // TODO: Flush producer (changelog records)
        // producer_->flush();
        
        // TODO: Commit consumer offsets
        // consumer_->commitSync();
        
        spdlog::debug("[Task {}] Commit completed", taskId_);
        
    } catch (const std::exception& e) {
        spdlog::error("[Task {}] Failed to commit: {}", taskId_, e.what());
        throw;
    }
}

void StreamTask::close() {
    if (!running_) {
        return;
    }
    
    spdlog::info("[Task {}] Closing task", taskId_);
    
    try {
        // Commit before closing
        commit();
        
        // Close state stores
        // for (auto& [name, store] : stateStores_) {
        //     store->close();
        // }
        
        // Close producer and consumer
        // producer_.reset();
        // consumer_.reset();
        
        running_ = false;
        spdlog::info("[Task {}] Task closed", taskId_);
        
    } catch (const std::exception& e) {
        spdlog::error("[Task {}] Error closing task: {}", taskId_, e.what());
        throw;
    }
}

// StreamThread implementation

StreamThread::StreamThread(std::string threadId,
                          std::vector<std::shared_ptr<StreamTask>> tasks,
                          std::map<std::string, std::string> config)
    : threadId_(std::move(threadId)),
      tasks_(std::move(tasks)),
      config_(std::move(config)) {
}

StreamThread::~StreamThread() {
    if (running_) {
        stop();
    }
}

void StreamThread::start() {
    if (running_) {
        throw std::runtime_error("Thread is already running");
    }
    
    spdlog::info("[Thread {}] Starting stream thread with {} tasks", 
                 threadId_, tasks_.size());
    
    // Initialize all tasks
    for (auto& task : tasks_) {
        task->initialize();
    }
    
    running_ = true;
    thread_ = std::make_unique<std::thread>(&StreamThread::run, this);
    
    spdlog::info("[Thread {}] Stream thread started", threadId_);
}

void StreamThread::stop(int /* timeoutMs */) {
    if (!running_) {
        return;
    }
    
    spdlog::info("[Thread {}] Stopping stream thread", threadId_);
    
    running_ = false;
    
    if (thread_ && thread_->joinable()) {
        thread_->join();
    }
    
    // Close all tasks
    for (auto& task : tasks_) {
        task->close();
    }
    
    spdlog::info("[Thread {}] Stream thread stopped", threadId_);
}

void StreamThread::run() {
    spdlog::info("[Thread {}] Thread loop started", threadId_);
    
    try {
        while (running_) {
            // Process records from all tasks
            int totalRecords = 0;
            for (auto& task : tasks_) {
                totalRecords += task->process(100);  // 100ms timeout
            }
            
            // Periodic commit (every 30 seconds or configurable)
            // TODO: Add commit interval logic
            // if (shouldCommit()) {
            //     commitAll();
            // }
            
            // Small sleep if no records processed
            if (totalRecords == 0) {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
        }
        
    } catch (const std::exception& e) {
        spdlog::error("[Thread {}] Thread loop error: {}", threadId_, e.what());
        running_ = false;
    }
    
    spdlog::info("[Thread {}] Thread loop finished", threadId_);
}

void StreamThread::commitAll() {
    spdlog::debug("[Thread {}] Committing all tasks", threadId_);
    
    for (auto& task : tasks_) {
        try {
            task->commit();
        } catch (const std::exception& e) {
            spdlog::error("[Thread {}] Failed to commit task {}: {}", 
                         threadId_, task->taskId(), e.what());
        }
    }
}

void StreamThread::addTask(std::shared_ptr<StreamTask> task) {
    spdlog::info("[Thread {}] Adding task {}", threadId_, task->taskId());
    tasks_.push_back(std::move(task));
}

void StreamThread::removeTask(const std::string& taskId) {
    spdlog::info("[Thread {}] Removing task {}", threadId_, taskId);
    
    auto it = std::remove_if(tasks_.begin(), tasks_.end(),
                            [&taskId](const auto& task) {
                                return task->taskId() == taskId;
                            });
    
    if (it != tasks_.end()) {
        (*it)->close();
        tasks_.erase(it, tasks_.end());
    }
}

} // namespace streams
} // namespace kawasan
