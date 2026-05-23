#pragma once

#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace kawasan {
namespace connect {

/**
 * Properties class for connector configuration.
 * Simple key-value store with type conversion helpers.
 */
class Properties {
public:
    Properties() = default;

    void set(const std::string& key, const std::string& value) {
        props_[key] = value;
    }

    std::string get(const std::string& key) const {
        auto it = props_.find(key);
        return it != props_.end() ? it->second : "";
    }

    std::string get(const std::string& key, const std::string& defaultValue) const {
        auto it = props_.find(key);
        return it != props_.end() ? it->second : defaultValue;
    }

    int getInt(const std::string& key, int defaultValue = 0) const {
        auto it = props_.find(key);
        if (it != props_.end()) {
            try {
                return std::stoi(it->second);
            } catch (...) {
                return defaultValue;
            }
        }
        return defaultValue;
    }

    int64_t getLong(const std::string& key, int64_t defaultValue = 0) const {
        auto it = props_.find(key);
        if (it != props_.end()) {
            try {
                return std::stoll(it->second);
            } catch (...) {
                return defaultValue;
            }
        }
        return defaultValue;
    }

    bool getBool(const std::string& key, bool defaultValue = false) const {
        auto it = props_.find(key);
        if (it != props_.end()) {
            return it->second == "true" || it->second == "1" || it->second == "yes";
        }
        return defaultValue;
    }

    bool contains(const std::string& key) const {
        return props_.find(key) != props_.end();
    }

    const std::map<std::string, std::string>& getAll() const {
        return props_;
    }

    void setAll(const std::map<std::string, std::string>& other) {
        for (const auto& [k, v] : other) {
            props_[k] = v;
        }
    }

private:
    std::map<std::string, std::string> props_;
};

/**
 * Connector status enumeration.
 */
enum class ConnectorStatus {
    UNASSIGNED,  // Not yet started
    RUNNING,     // Active and processing
    PAUSED,      // Temporarily stopped
    FAILED,      // Error state
    STOPPED      // Gracefully stopped
};

/**
 * Task status enumeration.
 */
enum class TaskStatus {
    UNASSIGNED,
    RUNNING,
    PAUSED,
    FAILED,
    STOPPED
};

/**
 * Status information for a connector task.
 */
struct TaskStatusInfo {
    int taskId = 0;
    TaskStatus status = TaskStatus::UNASSIGNED;
    std::string workerId;
    std::string errorMessage;
    int64_t lastUpdateTime = 0;
};

/**
 * Status information for a connector.
 */
struct ConnectorStatusInfo {
    std::string name;
    ConnectorStatus status = ConnectorStatus::UNASSIGNED;
    std::string type;  // "source" or "sink"
    std::string workerId;
    std::string errorMessage;
    std::vector<TaskStatusInfo> tasks;
    int64_t lastUpdateTime = 0;
};

/**
 * Base Connector interface.
 *
 * Connectors define how to create tasks and manage their lifecycle.
 * Subclasses implement SourceConnector or SinkConnector.
 */
class Connector {
public:
    virtual ~Connector() = default;

    /**
     * Start the connector with the given configuration.
     *
     * @param config Connector configuration properties
     */
    virtual void start(const Properties& config) = 0;

    /**
     * Generate task configurations for parallel execution.
     *
     * @param maxTasks Maximum number of tasks to create
     * @return Vector of Properties, one for each task
     */
    virtual std::vector<Properties> taskConfigs(int maxTasks) = 0;

    /**
     * Stop the connector and release resources.
     */
    virtual void stop() = 0;

    /**
     * Get the connector version string.
     *
     * @return Version string (e.g., "1.0.0")
     */
    virtual std::string version() const = 0;

    /**
     * Get the connector type.
     *
     * @return "source" or "sink"
     */
    virtual std::string type() const = 0;

    /**
     * Validate the configuration before starting.
     *
     * @param config Configuration to validate
     * @return Empty string if valid, error message if invalid
     */
    virtual std::string validate(const Properties& config) {
        // Default implementation: check for required properties
        if (!config.contains("name")) {
            return "Missing required property: name";
        }
        return "";
    }

protected:
    Properties config_;
};

/**
 * Source Connector base class.
 *
 * Source connectors read data from external systems and write to Kawasan topics.
 */
class SourceConnector : public Connector {
public:
    std::string type() const override { return "source"; }
};

/**
 * Sink Connector base class.
 *
 * Sink connectors read data from Kawasan topics and write to external systems.
 */
class SinkConnector : public Connector {
public:
    std::string type() const override { return "sink"; }
};

/**
 * Source record produced by source tasks.
 */
struct SourceRecord {
    // Source tracking for offset management
    std::map<std::string, std::string> sourcePartition;
    std::map<std::string, std::string> sourceOffset;

    // Destination topic
    std::string topic;
    std::optional<int32_t> partition;

    // Record data
    std::optional<std::string> key;
    std::string value;

    // Metadata
    int64_t timestamp = 0;
    std::map<std::string, std::string> headers;

    SourceRecord() = default;

    SourceRecord(
        std::map<std::string, std::string> srcPartition,
        std::map<std::string, std::string> srcOffset,
        std::string destTopic,
        std::string val)
        : sourcePartition(std::move(srcPartition))
        , sourceOffset(std::move(srcOffset))
        , topic(std::move(destTopic))
        , value(std::move(val))
        , timestamp(std::chrono::duration_cast<std::chrono::milliseconds>(
              std::chrono::system_clock::now().time_since_epoch()).count()) {}

    SourceRecord(
        std::map<std::string, std::string> srcPartition,
        std::map<std::string, std::string> srcOffset,
        std::string destTopic,
        std::string k,
        std::string val)
        : sourcePartition(std::move(srcPartition))
        , sourceOffset(std::move(srcOffset))
        , topic(std::move(destTopic))
        , key(std::move(k))
        , value(std::move(val))
        , timestamp(std::chrono::duration_cast<std::chrono::milliseconds>(
              std::chrono::system_clock::now().time_since_epoch()).count()) {}
};

/**
 * Sink record consumed by sink tasks.
 */
struct SinkRecord {
    std::string topic;
    int32_t partition = 0;
    int64_t offset = 0;

    std::optional<std::string> key;
    std::string value;

    int64_t timestamp = 0;
    std::map<std::string, std::string> headers;

    SinkRecord() = default;

    SinkRecord(
        std::string t,
        int32_t p,
        int64_t o,
        std::string val)
        : topic(std::move(t))
        , partition(p)
        , offset(o)
        , value(std::move(val)) {}

    SinkRecord(
        std::string t,
        int32_t p,
        int64_t o,
        std::string k,
        std::string val)
        : topic(std::move(t))
        , partition(p)
        , offset(o)
        , key(std::move(k))
        , value(std::move(val)) {}
};

/**
 * Base Task interface.
 *
 * Tasks are the unit of parallelism in Connect.
 */
class Task {
public:
    virtual ~Task() = default;

    /**
     * Start the task with the given configuration.
     *
     * @param config Task configuration properties
     */
    virtual void start(const Properties& config) = 0;

    /**
     * Stop the task and release resources.
     */
    virtual void stop() = 0;

    /**
     * Get the task version string.
     *
     * @return Version string
     */
    virtual std::string version() const = 0;
};

/**
 * Source Task interface.
 *
 * Source tasks poll external systems for new data.
 */
class SourceTask : public Task {
public:
    /**
     * Poll for new records from the source system.
     *
     * @return Vector of source records, empty if no data available
     */
    virtual std::vector<SourceRecord> poll() = 0;

    /**
     * Called when offsets have been committed.
     *
     * Tasks can implement this to perform cleanup after commit.
     */
    virtual void commit() {}

    /**
     * Called when specific offsets have been committed.
     *
     * @param offsets Map of partition to committed offset
     */
    virtual void commitRecord(const SourceRecord& record) {
        (void)record;
    }
};

/**
 * Sink Task interface.
 *
 * Sink tasks write records to external systems.
 */
class SinkTask : public Task {
public:
    /**
     * Put records to the sink system.
     *
     * @param records Records to write
     */
    virtual void put(const std::vector<SinkRecord>& records) = 0;

    /**
     * Flush any buffered data.
     *
     * @param currentOffsets Current offsets being processed
     */
    virtual void flush(const std::map<std::string, int64_t>& currentOffsets) {
        (void)currentOffsets;
    }

    /**
     * Called before records are delivered.
     * Can be used to create resources like database connections.
     *
     * @param partitions Assigned partitions
     */
    virtual void open(const std::vector<std::pair<std::string, int32_t>>& partitions) {
        (void)partitions;
    }

    /**
     * Called when partitions are revoked.
     * Should flush any pending writes.
     *
     * @param partitions Revoked partitions
     */
    virtual void close(const std::vector<std::pair<std::string, int32_t>>& partitions) {
        (void)partitions;
    }
};

/**
 * Connector factory for creating connectors by name.
 */
class ConnectorFactory {
public:
    using CreatorFunc = std::function<std::unique_ptr<Connector>()>;

    /**
     * Register a connector type.
     *
     * @param name Connector class name
     * @param creator Factory function
     */
    static void registerConnector(const std::string& name, CreatorFunc creator) {
        creators()[name] = std::move(creator);
    }

    /**
     * Register a connector type using template.
     *
     * @tparam T Connector class
     * @param name Connector class name
     */
    template<typename T>
    static void registerConnector(const std::string& name) {
        registerConnector(name, []() { return std::make_unique<T>(); });
    }

    /**
     * Create a connector by name.
     *
     * @param name Connector class name
     * @return Unique pointer to connector, or nullptr if not found
     */
    static std::unique_ptr<Connector> create(const std::string& name) {
        auto it = creators().find(name);
        if (it != creators().end()) {
            return it->second();
        }
        return nullptr;
    }

    /**
     * Check if a connector type is registered.
     *
     * @param name Connector class name
     * @return true if registered
     */
    static bool isRegistered(const std::string& name) {
        return creators().find(name) != creators().end();
    }

    /**
     * Get all registered connector names.
     *
     * @return Vector of connector names
     */
    static std::vector<std::string> registeredConnectors() {
        std::vector<std::string> names;
        for (const auto& [name, _] : creators()) {
            names.push_back(name);
        }
        return names;
    }

private:
    static std::map<std::string, CreatorFunc>& creators() {
        static std::map<std::string, CreatorFunc> instance;
        return instance;
    }
};

/**
 * Task factory for creating tasks from connectors.
 */
class TaskFactory {
public:
    using SourceTaskCreator = std::function<std::unique_ptr<SourceTask>()>;
    using SinkTaskCreator = std::function<std::unique_ptr<SinkTask>()>;

    /**
     * Register a source task type.
     */
    static void registerSourceTask(const std::string& connectorClass, SourceTaskCreator creator) {
        sourceCreators()[connectorClass] = std::move(creator);
    }

    /**
     * Register a sink task type.
     */
    static void registerSinkTask(const std::string& connectorClass, SinkTaskCreator creator) {
        sinkCreators()[connectorClass] = std::move(creator);
    }

    /**
     * Create a source task.
     */
    static std::unique_ptr<SourceTask> createSourceTask(const std::string& connectorClass) {
        auto it = sourceCreators().find(connectorClass);
        if (it != sourceCreators().end()) {
            return it->second();
        }
        return nullptr;
    }

    /**
     * Create a sink task.
     */
    static std::unique_ptr<SinkTask> createSinkTask(const std::string& connectorClass) {
        auto it = sinkCreators().find(connectorClass);
        if (it != sinkCreators().end()) {
            return it->second();
        }
        return nullptr;
    }

private:
    static std::map<std::string, SourceTaskCreator>& sourceCreators() {
        static std::map<std::string, SourceTaskCreator> instance;
        return instance;
    }

    static std::map<std::string, SinkTaskCreator>& sinkCreators() {
        static std::map<std::string, SinkTaskCreator> instance;
        return instance;
    }
};

} // namespace connect
} // namespace kawasan
