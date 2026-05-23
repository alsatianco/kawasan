#include "kawasan/connect/worker.h"
#include <spdlog/spdlog.h>
#include <chrono>
#include <iostream>

namespace kawasan {
namespace connect {

// ============================================================================
// ConnectWorker Implementation
// ============================================================================

ConnectWorker::ConnectWorker(const WorkerConfig& config)
    : config_(config) {

    // Create offset storage
    std::map<std::string, std::string> storageConfig;
    storageConfig["file.path"] = config_.offsetStorageFile;

    offsetStorage_ = OffsetStorageFactory::create(config_.offsetStorageType, storageConfig);
}

ConnectWorker::~ConnectWorker() {
    stop();
}

void ConnectWorker::start() {
    if (running_.exchange(true)) {
        return;  // Already running
    }

    spdlog::info("Starting ConnectWorker with {} threads", config_.workerThreads);

    // Initialize offset storage
    offsetStorage_->init();

    // Start offset flush thread
    offsetFlushThread_ = std::thread([this]() {
        offsetFlushLoop();
    });

    // Start task restart thread
    taskRestartThread_ = std::thread([this]() {
        taskRestartLoop();
    });

    spdlog::info("ConnectWorker started");
}

void ConnectWorker::stop() {
    if (!running_.exchange(false)) {
        return;  // Already stopped
    }

    spdlog::info("Stopping ConnectWorker...");

    // Wake up waiting threads
    cv_.notify_all();

    // Stop all connectors
    {
        std::lock_guard<std::mutex> lock(mutex_);
        for (auto& [name, state] : connectors_) {
            stopTasks(*state);
            if (state->connector) {
                state->connector->stop();
            }
            state->status = ConnectorStatus::STOPPED;
        }
    }

    // Wait for threads to finish
    if (offsetFlushThread_.joinable()) {
        offsetFlushThread_.join();
    }
    if (taskRestartThread_.joinable()) {
        taskRestartThread_.join();
    }

    // Flush and close offset storage
    offsetStorage_->flush();
    offsetStorage_->close();

    spdlog::info("ConnectWorker stopped");
}

std::string ConnectWorker::startConnector(const std::string& name, const Properties& config) {
    std::lock_guard<std::mutex> lock(mutex_);

    // Check if connector already exists
    if (connectors_.find(name) != connectors_.end()) {
        return "Connector already exists: " + name;
    }

    // Get connector class
    std::string connectorClass = config.get("connector.class");
    if (connectorClass.empty()) {
        return "Missing required property: connector.class";
    }

    // Create connector
    auto connector = ConnectorFactory::create(connectorClass);
    if (!connector) {
        return "Unknown connector class: " + connectorClass;
    }

    // Validate configuration
    std::string validationError = connector->validate(config);
    if (!validationError.empty()) {
        return validationError;
    }

    // Create connector state
    auto state = std::make_unique<ConnectorState>();
    state->name = name;
    state->connector = std::move(connector);
    state->config = config;

    try {
        // Start connector
        state->connector->start(config);
        state->status = ConnectorStatus::RUNNING;

        // Start tasks
        startTasks(*state);

        spdlog::info("Started connector '{}' with {} tasks",
            name, state->tasks.size());

    } catch (const std::exception& e) {
        state->status = ConnectorStatus::FAILED;
        state->errorMessage = e.what();
        return std::string("Failed to start connector: ") + e.what();
    }

    connectors_[name] = std::move(state);
    return "";
}

std::string ConnectWorker::stopConnector(const std::string& name) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = connectors_.find(name);
    if (it == connectors_.end()) {
        return "Connector not found: " + name;
    }

    auto& state = it->second;

    // Stop tasks
    stopTasks(*state);

    // Stop connector
    if (state->connector) {
        state->connector->stop();
    }
    state->status = ConnectorStatus::STOPPED;

    spdlog::info("Stopped connector '{}'", name);

    // Remove connector
    connectors_.erase(it);
    return "";
}

std::string ConnectWorker::pauseConnector(const std::string& name) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = connectors_.find(name);
    if (it == connectors_.end()) {
        return "Connector not found: " + name;
    }

    auto& state = it->second;
    if (state->status != ConnectorStatus::RUNNING) {
        return "Connector is not running";
    }

    // Pause tasks
    for (auto& task : state->tasks) {
        task->running.store(false);
        task->status = TaskStatus::PAUSED;
    }

    state->status = ConnectorStatus::PAUSED;
    spdlog::info("Paused connector '{}'", name);
    return "";
}

std::string ConnectWorker::resumeConnector(const std::string& name) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = connectors_.find(name);
    if (it == connectors_.end()) {
        return "Connector not found: " + name;
    }

    auto& state = it->second;
    if (state->status != ConnectorStatus::PAUSED) {
        return "Connector is not paused";
    }

    // Resume tasks
    for (auto& task : state->tasks) {
        task->running.store(true);
        task->status = TaskStatus::RUNNING;
    }

    state->status = ConnectorStatus::RUNNING;
    spdlog::info("Resumed connector '{}'", name);
    return "";
}

std::string ConnectWorker::restartConnector(const std::string& name) {
    Properties config;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = connectors_.find(name);
        if (it == connectors_.end()) {
            return "Connector not found: " + name;
        }
        config = it->second->config;
    }

    // Stop and start
    std::string error = stopConnector(name);
    if (!error.empty()) {
        return error;
    }

    return startConnector(name, config);
}

std::string ConnectWorker::restartTask(const std::string& connectorName, int taskId) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = connectors_.find(connectorName);
    if (it == connectors_.end()) {
        return "Connector not found: " + connectorName;
    }

    auto& state = it->second;
    for (auto& task : state->tasks) {
        if (task->taskId == taskId) {
            // Stop task
            task->running.store(false);
            if (task->thread.joinable()) {
                task->thread.join();
            }

            // Restart task
            task->running.store(true);
            task->status = TaskStatus::RUNNING;
            task->errorMessage.clear();

            std::string connType = state->connector->type();
            if (connType == "source") {
                task->thread = std::thread([this, &runner = *task]() {
                    runSourceTask(runner);
                });
            } else {
                task->thread = std::thread([this, &runner = *task]() {
                    runSinkTask(runner);
                });
            }

            spdlog::info("Restarted task {} for connector '{}'", taskId, connectorName);
            return "";
        }
    }

    return "Task not found: " + std::to_string(taskId);
}

ConnectorStatusInfo ConnectWorker::getConnectorStatus(const std::string& name) const {
    std::lock_guard<std::mutex> lock(mutex_);

    ConnectorStatusInfo info;
    info.name = name;

    auto it = connectors_.find(name);
    if (it == connectors_.end()) {
        info.status = ConnectorStatus::UNASSIGNED;
        return info;
    }

    const auto& state = it->second;
    info.status = state->status;
    info.type = state->connector ? state->connector->type() : "";
    info.errorMessage = state->errorMessage;

    for (const auto& task : state->tasks) {
        TaskStatusInfo taskInfo;
        taskInfo.taskId = task->taskId;
        taskInfo.status = task->status;
        taskInfo.errorMessage = task->errorMessage;
        info.tasks.push_back(taskInfo);
    }

    return info;
}

std::vector<std::string> ConnectWorker::getConnectorNames() const {
    std::lock_guard<std::mutex> lock(mutex_);

    std::vector<std::string> names;
    for (const auto& [name, _] : connectors_) {
        names.push_back(name);
    }
    return names;
}

void ConnectWorker::startTasks(ConnectorState& state) {
    int maxTasks = state.config.getInt("tasks.max", 1);
    auto taskConfigs = state.connector->taskConfigs(maxTasks);

    std::string connType = state.connector->type();

    for (size_t i = 0; i < taskConfigs.size(); ++i) {
        auto runner = std::make_unique<TaskRunner>();
        runner->taskId = static_cast<int>(i);
        runner->connectorName = state.name;
        runner->config = taskConfigs[i];
        runner->running.store(true);
        runner->status = TaskStatus::RUNNING;

        // Create task
        std::string connectorClass = state.config.get("connector.class");
        if (connType == "source") {
            runner->task = TaskFactory::createSourceTask(connectorClass);
            if (runner->task) {
                runner->task->start(runner->config);
                runner->thread = std::thread([this, &r = *runner]() {
                    runSourceTask(r);
                });
            }
        } else {
            runner->task = TaskFactory::createSinkTask(connectorClass);
            if (runner->task) {
                runner->task->start(runner->config);
                runner->thread = std::thread([this, &r = *runner]() {
                    runSinkTask(r);
                });
            }
        }

        state.tasks.push_back(std::move(runner));
    }
}

void ConnectWorker::stopTasks(ConnectorState& state) {
    for (auto& task : state.tasks) {
        task->running.store(false);
    }

    for (auto& task : state.tasks) {
        if (task->thread.joinable()) {
            task->thread.join();
        }
        if (task->task) {
            task->task->stop();
        }
        task->status = TaskStatus::STOPPED;
    }
}

void ConnectWorker::runSourceTask(TaskRunner& runner) {
    spdlog::info("Source task {} started for connector '{}'",
        runner.taskId, runner.connectorName);

    auto* sourceTask = dynamic_cast<SourceTask*>(runner.task.get());
    if (!sourceTask) {
        runner.status = TaskStatus::FAILED;
        runner.errorMessage = "Invalid source task";
        return;
    }

    while (runner.running.load() && running_.load()) {
        try {
            // Poll for records
            auto records = sourceTask->poll();

            if (!records.empty()) {
                // Produce records to Kawasan
                if (produceRecords(records)) {
                    // Store offsets
                    for (const auto& record : records) {
                        std::lock_guard<std::mutex> lock(offsetMutex_);
                        // Create a unique key for this partition
                        nlohmann::json partKey = record.sourcePartition;
                        pendingOffsets_[runner.connectorName][partKey.dump()] =
                            nlohmann::json(record.sourceOffset).dump();
                    }

                    // Notify task of successful commit
                    sourceTask->commit();
                }
            }

            // Sleep between polls
            std::this_thread::sleep_for(
                std::chrono::milliseconds(config_.pollIntervalMs));

        } catch (const std::exception& e) {
            spdlog::error("Source task {} error: {}", runner.taskId, e.what());
            runner.errorMessage = e.what();
            runner.status = TaskStatus::FAILED;
            runner.running.store(false);
            break;
        }
    }

    spdlog::info("Source task {} stopped for connector '{}'",
        runner.taskId, runner.connectorName);
}

void ConnectWorker::runSinkTask(TaskRunner& runner) {
    spdlog::info("Sink task {} started for connector '{}'",
        runner.taskId, runner.connectorName);

    auto* sinkTask = dynamic_cast<SinkTask*>(runner.task.get());
    if (!sinkTask) {
        runner.status = TaskStatus::FAILED;
        runner.errorMessage = "Invalid sink task";
        return;
    }

    // Get topic from config
    std::string topic = runner.config.get("topics", "");

    while (runner.running.load() && running_.load()) {
        try {
            // Fetch records from Kawasan
            auto records = fetchRecords(topic, runner.taskId);

            if (!records.empty()) {
                // Put records to sink
                sinkTask->put(records);
            }

            // Sleep between fetches
            std::this_thread::sleep_for(
                std::chrono::milliseconds(config_.fetchMaxWaitMs));

        } catch (const std::exception& e) {
            spdlog::error("Sink task {} error: {}", runner.taskId, e.what());
            runner.errorMessage = e.what();
            runner.status = TaskStatus::FAILED;
            runner.running.store(false);
            break;
        }
    }

    spdlog::info("Sink task {} stopped for connector '{}'",
        runner.taskId, runner.connectorName);
}

void ConnectWorker::offsetFlushLoop() {
    while (running_.load()) {
        // Wait for flush interval
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait_for(lock, std::chrono::milliseconds(config_.offsetFlushIntervalMs),
            [this]() { return !running_.load(); });

        if (!running_.load()) break;
        lock.unlock();

        // Flush pending offsets
        {
            std::lock_guard<std::mutex> offsetLock(offsetMutex_);
            for (const auto& [connector, partitions] : pendingOffsets_) {
                for (const auto& [partKey, offsetStr] : partitions) {
                    try {
                        auto partition = nlohmann::json::parse(partKey)
                            .get<std::map<std::string, std::string>>();
                        auto offset = nlohmann::json::parse(offsetStr)
                            .get<std::map<std::string, std::string>>();
                        offsetStorage_->store(connector, partition, offset);
                    } catch (...) {
                        // Ignore parse errors
                    }
                }
            }
            pendingOffsets_.clear();
        }

        offsetStorage_->flush();
        spdlog::debug("Flushed offsets");
    }
}

void ConnectWorker::taskRestartLoop() {
    while (running_.load()) {
        // Check every 5 seconds
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait_for(lock, std::chrono::seconds(5),
            [this]() { return !running_.load(); });

        if (!running_.load()) break;

        auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();

        // Check for failed tasks that need restart
        for (auto& [name, state] : connectors_) {
            if (state->status != ConnectorStatus::RUNNING) continue;

            for (auto& task : state->tasks) {
                if (task->status == TaskStatus::FAILED) {
                    // Check if enough time has passed
                    int64_t backoff = config_.taskRestartBackoffMs *
                        (1 << std::min(task->restartAttempts, 6));  // Max 64x backoff

                    if (now - task->lastRestartTime > backoff &&
                        task->restartAttempts < config_.taskRestartMaxAttempts) {

                        spdlog::info("Restarting failed task {} for connector '{}' (attempt {})",
                            task->taskId, name, task->restartAttempts + 1);

                        task->restartAttempts++;
                        task->lastRestartTime = now;

                        // Restart in background
                        lock.unlock();
                        restartTask(name, task->taskId);
                        lock.lock();
                    }
                }
            }
        }
    }
}

bool ConnectWorker::produceRecords(const std::vector<SourceRecord>& records) {
    // TODO: Implement actual Kawasan producer
    // For now, just log the records
    for (const auto& record : records) {
        spdlog::debug("Producing to {}: key={}, value={}",
            record.topic,
            record.key.value_or("null"),
            record.value.substr(0, 100));
    }
    return true;
}

std::vector<SinkRecord> ConnectWorker::fetchRecords(
    const std::string& topic, int32_t partition) {
    // TODO: Implement actual Kawasan consumer
    (void)topic;
    (void)partition;
    return {};
}

// ============================================================================
// ConsoleSinkTask Implementation
// ============================================================================

void ConsoleSinkTask::start(const Properties& config) {
    prefix_ = config.get("prefix", "");
    printKey_ = config.getBool("print.key", true);
    printTimestamp_ = config.getBool("print.timestamp", true);
    spdlog::info("ConsoleSinkTask started");
}

void ConsoleSinkTask::stop() {
    spdlog::info("ConsoleSinkTask stopped");
}

void ConsoleSinkTask::put(const std::vector<SinkRecord>& records) {
    for (const auto& record : records) {
        std::cout << prefix_;

        if (printTimestamp_) {
            std::cout << "[" << record.timestamp << "] ";
        }

        std::cout << record.topic << ":" << record.partition << ":" << record.offset;

        if (printKey_ && record.key.has_value()) {
            std::cout << " key=" << *record.key;
        }

        std::cout << " value=" << record.value << std::endl;
    }
}

// ============================================================================
// ConsoleSinkConnector Implementation
// ============================================================================

void ConsoleSinkConnector::start(const Properties& config) {
    config_ = config;
    spdlog::info("ConsoleSinkConnector started");
}

std::vector<Properties> ConsoleSinkConnector::taskConfigs(int maxTasks) {
    std::vector<Properties> configs;
    for (int i = 0; i < maxTasks; ++i) {
        Properties taskConfig;
        taskConfig.setAll(config_.getAll());
        taskConfig.set("task.id", std::to_string(i));
        configs.push_back(taskConfig);
    }
    return configs;
}

void ConsoleSinkConnector::stop() {
    spdlog::info("ConsoleSinkConnector stopped");
}

std::string ConsoleSinkConnector::validate(const Properties& config) {
    std::string baseError = SinkConnector::validate(config);
    if (!baseError.empty()) return baseError;

    if (!config.contains("topics")) {
        return "Missing required property: topics";
    }
    return "";
}

// ============================================================================
// GeneratorSourceTask Implementation
// ============================================================================

void GeneratorSourceTask::start(const Properties& config) {
    topic_ = config.get("topic", "generated-topic");
    maxRecords_ = config.getLong("max.records", -1);
    intervalMs_ = config.getLong("interval.ms", 1000);
    keyPrefix_ = config.get("key.prefix", "key-");
    valuePrefix_ = config.get("value.prefix", "value-");

    // Load offset if available
    counter_ = config.getLong("_offset.counter", 0);

    running_ = true;
    spdlog::info("GeneratorSourceTask started, topic={}", topic_);
}

void GeneratorSourceTask::stop() {
    running_ = false;
    spdlog::info("GeneratorSourceTask stopped");
}

std::vector<SourceRecord> GeneratorSourceTask::poll() {
    std::vector<SourceRecord> records;

    if (!running_) return records;

    if (maxRecords_ >= 0 && counter_ >= maxRecords_) {
        return records;
    }

    // Generate one record
    std::map<std::string, std::string> partition = {{"topic", topic_}};
    std::map<std::string, std::string> offset = {{"counter", std::to_string(counter_ + 1)}};

    SourceRecord record(
        partition,
        offset,
        topic_,
        keyPrefix_ + std::to_string(counter_),
        valuePrefix_ + std::to_string(counter_)
    );

    records.push_back(record);
    counter_++;

    // Sleep to control rate
    std::this_thread::sleep_for(std::chrono::milliseconds(intervalMs_));

    return records;
}

// ============================================================================
// GeneratorSourceConnector Implementation
// ============================================================================

void GeneratorSourceConnector::start(const Properties& config) {
    config_ = config;
    spdlog::info("GeneratorSourceConnector started");
}

std::vector<Properties> GeneratorSourceConnector::taskConfigs(int maxTasks) {
    std::vector<Properties> configs;
    int64_t totalRecords = config_.getLong("max.records", -1);
    int64_t recordsPerTask = totalRecords > 0 ? totalRecords / maxTasks : -1;

    for (int i = 0; i < maxTasks; ++i) {
        Properties taskConfig;
        taskConfig.setAll(config_.getAll());
        taskConfig.set("task.id", std::to_string(i));

        if (recordsPerTask > 0) {
            taskConfig.set("max.records", std::to_string(recordsPerTask));
        }

        configs.push_back(taskConfig);
    }
    return configs;
}

void GeneratorSourceConnector::stop() {
    spdlog::info("GeneratorSourceConnector stopped");
}

std::string GeneratorSourceConnector::validate(const Properties& config) {
    std::string baseError = SourceConnector::validate(config);
    if (!baseError.empty()) return baseError;

    if (!config.contains("topic")) {
        return "Missing required property: topic";
    }
    return "";
}

// ============================================================================
// Static initialization - register built-in connectors
// ============================================================================

namespace {

struct ConnectorRegistrar {
    ConnectorRegistrar() {
        // Register built-in connectors
        ConnectorFactory::registerConnector<ConsoleSinkConnector>("ConsoleSink");
        ConnectorFactory::registerConnector<GeneratorSourceConnector>("GeneratorSource");

        // Register task factories
        TaskFactory::registerSinkTask("ConsoleSink", []() {
            return std::make_unique<ConsoleSinkTask>();
        });
        TaskFactory::registerSourceTask("GeneratorSource", []() {
            return std::make_unique<GeneratorSourceTask>();
        });
    }
} connectorRegistrar;

}  // namespace

} // namespace connect
} // namespace kawasan
