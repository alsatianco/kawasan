#pragma once

#include <fstream>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <nlohmann/json.hpp>

namespace kawasan {
namespace connect {

/**
 * Offset key combining connector name and partition info.
 */
struct OffsetKey {
    std::string connectorName;
    std::map<std::string, std::string> partition;

    bool operator<(const OffsetKey& other) const {
        if (connectorName != other.connectorName) {
            return connectorName < other.connectorName;
        }
        return partition < other.partition;
    }

    bool operator==(const OffsetKey& other) const {
        return connectorName == other.connectorName && partition == other.partition;
    }
};

/**
 * Abstract offset storage interface.
 *
 * Manages source connector offsets for exactly-once semantics.
 */
class OffsetStorage {
public:
    virtual ~OffsetStorage() = default;

    /**
     * Initialize the offset storage.
     */
    virtual void init() = 0;

    /**
     * Load offset for a given partition.
     *
     * @param connectorName Name of the connector
     * @param partition Partition identifier (key-value map)
     * @return Offset map, empty if not found
     */
    virtual std::map<std::string, std::string> load(
        const std::string& connectorName,
        const std::map<std::string, std::string>& partition) = 0;

    /**
     * Load all offsets for a connector.
     *
     * @param connectorName Name of the connector
     * @return Map of partition to offset
     */
    virtual std::map<OffsetKey, std::map<std::string, std::string>> loadAll(
        const std::string& connectorName) = 0;

    /**
     * Store offset for a partition.
     *
     * @param connectorName Name of the connector
     * @param partition Partition identifier
     * @param offset Offset to store
     */
    virtual void store(
        const std::string& connectorName,
        const std::map<std::string, std::string>& partition,
        const std::map<std::string, std::string>& offset) = 0;

    /**
     * Flush pending offsets to persistent storage.
     */
    virtual void flush() = 0;

    /**
     * Remove offsets for a connector.
     *
     * @param connectorName Name of the connector
     */
    virtual void remove(const std::string& connectorName) = 0;

    /**
     * Close the offset storage.
     */
    virtual void close() = 0;
};

/**
 * File-based offset storage.
 *
 * Stores offsets in a local JSON file.
 * Suitable for standalone mode and development.
 */
class FileOffsetStorage : public OffsetStorage {
public:
    /**
     * Create file offset storage.
     *
     * @param filePath Path to the offset file
     */
    explicit FileOffsetStorage(const std::string& filePath);

    ~FileOffsetStorage() override;

    void init() override;

    std::map<std::string, std::string> load(
        const std::string& connectorName,
        const std::map<std::string, std::string>& partition) override;

    std::map<OffsetKey, std::map<std::string, std::string>> loadAll(
        const std::string& connectorName) override;

    void store(
        const std::string& connectorName,
        const std::map<std::string, std::string>& partition,
        const std::map<std::string, std::string>& offset) override;

    void flush() override;

    void remove(const std::string& connectorName) override;

    void close() override;

private:
    // Convert partition map to string key for internal storage
    std::string partitionToKey(const std::map<std::string, std::string>& partition) const;

    // Parse string key back to partition map
    std::map<std::string, std::string> keyToPartition(const std::string& key) const;

    // Load from file
    void loadFromFile();

    // Save to file
    void saveToFile();

    std::string filePath_;
    mutable std::mutex mutex_;

    // Internal storage: connector -> partition_key -> offset
    std::map<std::string, std::map<std::string, std::map<std::string, std::string>>> offsets_;

    bool dirty_ = false;
};

/**
 * In-memory offset storage for testing.
 *
 * Does not persist offsets - they are lost on restart.
 */
class InMemoryOffsetStorage : public OffsetStorage {
public:
    InMemoryOffsetStorage() = default;
    ~InMemoryOffsetStorage() override = default;

    void init() override {}

    std::map<std::string, std::string> load(
        const std::string& connectorName,
        const std::map<std::string, std::string>& partition) override;

    std::map<OffsetKey, std::map<std::string, std::string>> loadAll(
        const std::string& connectorName) override;

    void store(
        const std::string& connectorName,
        const std::map<std::string, std::string>& partition,
        const std::map<std::string, std::string>& offset) override;

    void flush() override {}

    void remove(const std::string& connectorName) override;

    void close() override {}

    // Test helper: get all stored offsets
    const auto& getAllOffsets() const { return offsets_; }

private:
    std::string partitionToKey(const std::map<std::string, std::string>& partition) const;
    std::map<std::string, std::string> keyToPartition(const std::string& key) const;

    mutable std::mutex mutex_;
    std::map<std::string, std::map<std::string, std::map<std::string, std::string>>> offsets_;
};

/**
 * Factory for creating offset storage instances.
 */
class OffsetStorageFactory {
public:
    /**
     * Create offset storage based on type.
     *
     * @param type Storage type ("file", "memory", "kafka")
     * @param config Configuration parameters
     * @return Unique pointer to offset storage
     */
    static std::unique_ptr<OffsetStorage> create(
        const std::string& type,
        const std::map<std::string, std::string>& config = {});
};

} // namespace connect
} // namespace kawasan
