#include "kawasan/connect/offset_storage.h"
#include <spdlog/spdlog.h>
#include <filesystem>
#include <sstream>

namespace kawasan {
namespace connect {

// ============================================================================
// FileOffsetStorage Implementation
// ============================================================================

FileOffsetStorage::FileOffsetStorage(const std::string& filePath)
    : filePath_(filePath) {}

FileOffsetStorage::~FileOffsetStorage() {
    close();
}

void FileOffsetStorage::init() {
    std::lock_guard<std::mutex> lock(mutex_);
    loadFromFile();
    spdlog::info("FileOffsetStorage initialized from {}", filePath_);
}

std::map<std::string, std::string> FileOffsetStorage::load(
    const std::string& connectorName,
    const std::map<std::string, std::string>& partition) {

    std::lock_guard<std::mutex> lock(mutex_);

    auto connIt = offsets_.find(connectorName);
    if (connIt == offsets_.end()) {
        return {};
    }

    std::string key = partitionToKey(partition);
    auto partIt = connIt->second.find(key);
    if (partIt == connIt->second.end()) {
        return {};
    }

    return partIt->second;
}

std::map<OffsetKey, std::map<std::string, std::string>> FileOffsetStorage::loadAll(
    const std::string& connectorName) {

    std::lock_guard<std::mutex> lock(mutex_);
    std::map<OffsetKey, std::map<std::string, std::string>> result;

    auto connIt = offsets_.find(connectorName);
    if (connIt == offsets_.end()) {
        return result;
    }

    for (const auto& [partKey, offset] : connIt->second) {
        OffsetKey key;
        key.connectorName = connectorName;
        key.partition = keyToPartition(partKey);
        result[key] = offset;
    }

    return result;
}

void FileOffsetStorage::store(
    const std::string& connectorName,
    const std::map<std::string, std::string>& partition,
    const std::map<std::string, std::string>& offset) {

    std::lock_guard<std::mutex> lock(mutex_);

    std::string key = partitionToKey(partition);
    offsets_[connectorName][key] = offset;
    dirty_ = true;
}

void FileOffsetStorage::flush() {
    std::lock_guard<std::mutex> lock(mutex_);

    if (dirty_) {
        saveToFile();
        dirty_ = false;
        spdlog::debug("FileOffsetStorage flushed to {}", filePath_);
    }
}

void FileOffsetStorage::remove(const std::string& connectorName) {
    std::lock_guard<std::mutex> lock(mutex_);

    offsets_.erase(connectorName);
    dirty_ = true;
}

void FileOffsetStorage::close() {
    flush();
}

std::string FileOffsetStorage::partitionToKey(
    const std::map<std::string, std::string>& partition) const {

    // Convert partition map to JSON string for use as key
    nlohmann::json j = partition;
    return j.dump();
}

std::map<std::string, std::string> FileOffsetStorage::keyToPartition(
    const std::string& key) const {

    try {
        nlohmann::json j = nlohmann::json::parse(key);
        return j.get<std::map<std::string, std::string>>();
    } catch (...) {
        return {};
    }
}

void FileOffsetStorage::loadFromFile() {
    offsets_.clear();

    if (!std::filesystem::exists(filePath_)) {
        spdlog::debug("Offset file does not exist: {}", filePath_);
        return;
    }

    try {
        std::ifstream file(filePath_);
        if (!file.is_open()) {
            spdlog::warn("Cannot open offset file: {}", filePath_);
            return;
        }

        nlohmann::json j;
        file >> j;

        if (j.contains("offsets") && j["offsets"].is_array()) {
            for (const auto& entry : j["offsets"]) {
                std::string connector = entry.value("connector", "");
                std::string partKey = entry.value("partition", "");
                auto offset = entry.value("offset", std::map<std::string, std::string>{});

                if (!connector.empty() && !partKey.empty()) {
                    offsets_[connector][partKey] = offset;
                }
            }
        }

        spdlog::info("Loaded {} connector offsets from {}", offsets_.size(), filePath_);

    } catch (const std::exception& e) {
        spdlog::error("Failed to load offset file {}: {}", filePath_, e.what());
    }
}

void FileOffsetStorage::saveToFile() {
    try {
        // Ensure directory exists
        auto parentDir = std::filesystem::path(filePath_).parent_path();
        if (!parentDir.empty() && !std::filesystem::exists(parentDir)) {
            std::filesystem::create_directories(parentDir);
        }

        nlohmann::json j;
        j["offsets"] = nlohmann::json::array();

        for (const auto& [connector, partitions] : offsets_) {
            for (const auto& [partKey, offset] : partitions) {
                nlohmann::json entry;
                entry["connector"] = connector;
                entry["partition"] = partKey;
                entry["offset"] = offset;
                j["offsets"].push_back(entry);
            }
        }

        std::ofstream file(filePath_);
        if (!file.is_open()) {
            spdlog::error("Cannot write to offset file: {}", filePath_);
            return;
        }

        file << j.dump(2);
        spdlog::debug("Saved offsets to {}", filePath_);

    } catch (const std::exception& e) {
        spdlog::error("Failed to save offset file {}: {}", filePath_, e.what());
    }
}

// ============================================================================
// InMemoryOffsetStorage Implementation
// ============================================================================

std::map<std::string, std::string> InMemoryOffsetStorage::load(
    const std::string& connectorName,
    const std::map<std::string, std::string>& partition) {

    std::lock_guard<std::mutex> lock(mutex_);

    auto connIt = offsets_.find(connectorName);
    if (connIt == offsets_.end()) {
        return {};
    }

    std::string key = partitionToKey(partition);
    auto partIt = connIt->second.find(key);
    if (partIt == connIt->second.end()) {
        return {};
    }

    return partIt->second;
}

std::map<OffsetKey, std::map<std::string, std::string>> InMemoryOffsetStorage::loadAll(
    const std::string& connectorName) {

    std::lock_guard<std::mutex> lock(mutex_);
    std::map<OffsetKey, std::map<std::string, std::string>> result;

    auto connIt = offsets_.find(connectorName);
    if (connIt == offsets_.end()) {
        return result;
    }

    for (const auto& [partKey, offset] : connIt->second) {
        OffsetKey key;
        key.connectorName = connectorName;
        key.partition = keyToPartition(partKey);
        result[key] = offset;
    }

    return result;
}

void InMemoryOffsetStorage::store(
    const std::string& connectorName,
    const std::map<std::string, std::string>& partition,
    const std::map<std::string, std::string>& offset) {

    std::lock_guard<std::mutex> lock(mutex_);
    std::string key = partitionToKey(partition);
    offsets_[connectorName][key] = offset;
}

void InMemoryOffsetStorage::remove(const std::string& connectorName) {
    std::lock_guard<std::mutex> lock(mutex_);
    offsets_.erase(connectorName);
}

std::string InMemoryOffsetStorage::partitionToKey(
    const std::map<std::string, std::string>& partition) const {

    nlohmann::json j = partition;
    return j.dump();
}

std::map<std::string, std::string> InMemoryOffsetStorage::keyToPartition(
    const std::string& key) const {

    try {
        nlohmann::json j = nlohmann::json::parse(key);
        return j.get<std::map<std::string, std::string>>();
    } catch (...) {
        return {};
    }
}

// ============================================================================
// OffsetStorageFactory Implementation
// ============================================================================

std::unique_ptr<OffsetStorage> OffsetStorageFactory::create(
    const std::string& type,
    const std::map<std::string, std::string>& config) {

    if (type == "file") {
        std::string path = "/tmp/kawasan-connect-offsets.json";
        auto it = config.find("file.path");
        if (it != config.end()) {
            path = it->second;
        }
        return std::make_unique<FileOffsetStorage>(path);
    }

    if (type == "memory") {
        return std::make_unique<InMemoryOffsetStorage>();
    }

    // Default to in-memory
    spdlog::warn("Unknown offset storage type '{}', using in-memory", type);
    return std::make_unique<InMemoryOffsetStorage>();
}

} // namespace connect
} // namespace kawasan
