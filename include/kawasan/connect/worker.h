#pragma once

#include "connector.h"
#include "offset_storage.h"
#include <atomic>
#include <condition_variable>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <vector>

namespace kawasan {
namespace connect {

/**
 * Worker configuration.
 */
struct WorkerConfig {
    // Kawasan connection
    std::string bootstrapServers = "localhost:9092";
    std::string groupId = "connect-cluster";

    // Offset storage
    std::string offsetStorageType = "file";  // "file" or "kafka"
    std::string offsetStorageFile = "/tmp/kawasan-connect-offsets.json";
    std::string offsetStorageTopic = "connect-offsets";
    int64_t offsetFlushIntervalMs = 60000;  // 60 seconds

    // Task execution
    int workerThreads = 4;
    int taskRestartMaxAttempts = 3;
    int64_t taskRestartBackoffMs = 10000;  // 10 seconds

    // Source task settings
    int64_t pollIntervalMs = 100;
    int pollBatchSize = 500;

    // Sink task settings
    int64_t fetchMaxWaitMs = 500;
    int fetchMinBytes = 1;
    int fetchMaxBytes = 1048576;  // 1MB
};

/**
 * Internal task runner state.
 */
struct TaskRunner {
    int taskId = 0;
    std::string connectorName;
    Properties config;
    std::unique_ptr<Task> task;
    TaskStatus status = TaskStatus::UNASSIGNED;
    std::thread thread;
    std::atomic<bool> running{false};
    std::string errorMessage;
    int restartAttempts = 0;
    int64_t lastRestartTime = 0;
};

/**
 * Internal connector state.
 */
struct ConnectorState {
    std::string name;
    std::unique_ptr<Connector> connector;
    Properties config;
    ConnectorStatus status = ConnectorStatus::UNASSIGNED;
    std::vector<std::unique_ptr<TaskRunner>> tasks;
    std::string errorMessage;
};

/**
 * Standalone Connect Worker.
 *
 * Manages connectors and their tasks in a single process.
 * Tasks run in a thread pool and offsets are stored locally.
 */
class ConnectWorker {
public:
    /**
     * Create a worker with the given configuration.
     *
     * @param config Worker configuration
     */
    explicit ConnectWorker(const WorkerConfig& config = WorkerConfig());

    ~ConnectWorker();

    // Disable copy
    ConnectWorker(const ConnectWorker&) = delete;
    ConnectWorker& operator=(const ConnectWorker&) = delete;

    /**
     * Start the worker.
     *
     * Initializes the thread pool and offset storage.
     */
    void start();

    /**
     * Stop the worker.
     *
     * Gracefully stops all connectors and tasks.
     */
    void stop();

    /**
     * Check if the worker is running.
     */
    bool isRunning() const { return running_.load(); }

    /**
     * Start a connector with the given configuration.
     *
     * @param name Unique connector name
     * @param config Connector configuration
     * @return Empty string on success, error message on failure
     */
    std::string startConnector(const std::string& name, const Properties& config);

    /**
     * Stop a connector.
     *
     * @param name Connector name
     * @return Empty string on success, error message on failure
     */
    std::string stopConnector(const std::string& name);

    /**
     * Pause a connector.
     *
     * @param name Connector name
     * @return Empty string on success, error message on failure
     */
    std::string pauseConnector(const std::string& name);

    /**
     * Resume a paused connector.
     *
     * @param name Connector name
     * @return Empty string on success, error message on failure
     */
    std::string resumeConnector(const std::string& name);

    /**
     * Restart a connector.
     *
     * @param name Connector name
     * @return Empty string on success, error message on failure
     */
    std::string restartConnector(const std::string& name);

    /**
     * Restart a specific task.
     *
     * @param connectorName Connector name
     * @param taskId Task ID
     * @return Empty string on success, error message on failure
     */
    std::string restartTask(const std::string& connectorName, int taskId);

    /**
     * Get connector status.
     *
     * @param name Connector name
     * @return Connector status info
     */
    ConnectorStatusInfo getConnectorStatus(const std::string& name) const;

    /**
     * Get all connector names.
     *
     * @return Vector of connector names
     */
    std::vector<std::string> getConnectorNames() const;

    /**
     * Get worker configuration.
     */
    const WorkerConfig& config() const { return config_; }

private:
    // Start tasks for a connector
    void startTasks(ConnectorState& state);

    // Stop tasks for a connector
    void stopTasks(ConnectorState& state);

    // Run a source task
    void runSourceTask(TaskRunner& runner);

    // Run a sink task
    void runSinkTask(TaskRunner& runner);

    // Offset flush thread
    void offsetFlushLoop();

    // Task restart thread
    void taskRestartLoop();

    // Produce records to Kawasan (for source tasks)
    bool produceRecords(const std::vector<SourceRecord>& records);

    // Fetch records from Kawasan (for sink tasks)
    std::vector<SinkRecord> fetchRecords(const std::string& topic, int32_t partition);

    WorkerConfig config_;
    std::unique_ptr<OffsetStorage> offsetStorage_;

    mutable std::mutex mutex_;
    std::map<std::string, std::unique_ptr<ConnectorState>> connectors_;

    std::atomic<bool> running_{false};
    std::thread offsetFlushThread_;
    std::thread taskRestartThread_;
    std::condition_variable cv_;

    // Pending offsets to commit
    std::mutex offsetMutex_;
    std::map<std::string, std::map<std::string, std::string>> pendingOffsets_;
};

/**
 * Simple console sink task for testing.
 *
 * Writes records to stdout.
 */
class ConsoleSinkTask : public SinkTask {
public:
    void start(const Properties& config) override;
    void stop() override;
    std::string version() const override { return "1.0.0"; }
    void put(const std::vector<SinkRecord>& records) override;

private:
    std::string prefix_;
    bool printKey_ = true;
    bool printTimestamp_ = true;
};

/**
 * Console sink connector.
 */
class ConsoleSinkConnector : public SinkConnector {
public:
    void start(const Properties& config) override;
    std::vector<Properties> taskConfigs(int maxTasks) override;
    void stop() override;
    std::string version() const override { return "1.0.0"; }
    std::string validate(const Properties& config) override;
};

/**
 * Simple generator source task for testing.
 *
 * Generates sequential numbered records.
 */
class GeneratorSourceTask : public SourceTask {
public:
    void start(const Properties& config) override;
    void stop() override;
    std::string version() const override { return "1.0.0"; }
    std::vector<SourceRecord> poll() override;

private:
    std::string topic_;
    int64_t counter_ = 0;
    int64_t maxRecords_ = -1;  // -1 = unlimited
    int64_t intervalMs_ = 1000;
    std::string keyPrefix_ = "key-";
    std::string valuePrefix_ = "value-";
    bool running_ = false;
};

/**
 * Generator source connector.
 */
class GeneratorSourceConnector : public SourceConnector {
public:
    void start(const Properties& config) override;
    std::vector<Properties> taskConfigs(int maxTasks) override;
    void stop() override;
    std::string version() const override { return "1.0.0"; }
    std::string validate(const Properties& config) override;
};

} // namespace connect
} // namespace kawasan
