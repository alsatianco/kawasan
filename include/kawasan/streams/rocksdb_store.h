#pragma once

#include "kawasan/common/rocksdb_compat.h"
#include "state_store.h"
#include <rocksdb/db.h>
#include <memory>
#include <string>
#include <mutex>

namespace kawasan {
namespace streams {

/**
 * RocksDB-backed KeyValueIterator implementation.
 */
template<typename K, typename V>
class RocksDBKeyValueIterator : public KeyValueIterator<K, V> {
public:
    RocksDBKeyValueIterator(
        std::unique_ptr<rocksdb::Iterator> it,
        std::shared_ptr<Serde<K>> key_serde,
        std::shared_ptr<Serde<V>> value_serde);
    
    bool hasNext() const override;
    std::pair<K, V> next() override;
    void close() override;
    
private:
    std::unique_ptr<rocksdb::Iterator> it_;
    std::shared_ptr<Serde<K>> key_serde_;
    std::shared_ptr<Serde<V>> value_serde_;
    bool closed_ = false;
};

/**
 * RocksDB-backed KeyValueStore implementation.
 * 
 * This store provides persistent key-value storage with optional
 * changelog topic integration for fault tolerance.
 */
template<typename K, typename V>
class RocksDBKeyValueStore : public KeyValueStore<K, V> {
public:
    /**
     * Create a new RocksDB key-value store.
     * 
     * @param name Store name
     * @param db_path Path to RocksDB database directory
     * @param key_serde Serializer/deserializer for keys
     * @param value_serde Serializer/deserializer for values
     * @param logging_enabled Enable changelog topic integration
     */
    RocksDBKeyValueStore(
        const std::string& name,
        const std::string& db_path,
        std::shared_ptr<Serde<K>> key_serde,
        std::shared_ptr<Serde<V>> value_serde,
        bool logging_enabled = true);
    
    ~RocksDBKeyValueStore() override;
    
    // StateStore interface
    std::string name() const override { return name_; }
    void init() override;
    void flush() override;
    void close() override;
    bool isOpen() const override { return db_ != nullptr; }
    bool persistent() const override { return true; }
    
    // KeyValueStore interface
    void put(const K& key, const V& value) override;
    std::optional<V> putIfAbsent(const K& key, const V& value) override;
    void putAll(const std::vector<std::pair<K, V>>& entries) override;
    std::optional<V> get(const K& key) override;
    std::optional<V> remove(const K& key) override;
    std::unique_ptr<KeyValueIterator<K, V>> all() override;
    std::unique_ptr<KeyValueIterator<K, V>> range(const K& from, const K& to) override;
    int64_t approximateNumEntries() override;
    
    /**
     * Set the changelog topic name for this store.
     * If logging is enabled, all mutations will be sent to this topic.
     */
    void setChangelogTopic(const std::string& topic) {
        changelog_topic_ = topic;
    }
    
    /**
     * Get the changelog topic name.
     */
    std::string changelogTopic() const { return changelog_topic_; }
    
private:
    std::string name_;
    std::string db_path_;
    std::shared_ptr<Serde<K>> key_serde_;
    std::shared_ptr<Serde<V>> value_serde_;
    bool logging_enabled_;
    std::string changelog_topic_;
    
    std::unique_ptr<rocksdb::DB> db_;
    mutable std::mutex mutex_; // Protect concurrent access
    
    // Helper methods
    std::vector<uint8_t> serializeKey(const K& key);
    K deserializeKey(const std::vector<uint8_t>& bytes);
    std::vector<uint8_t> serializeValue(const V& value);
    V deserializeValue(const std::vector<uint8_t>& bytes);
    void sendToChangelog(const K& key, const std::optional<V>& value);
};

// Implementation needs to be in header for template

template<typename K, typename V>
RocksDBKeyValueIterator<K, V>::RocksDBKeyValueIterator(
    std::unique_ptr<rocksdb::Iterator> it,
    std::shared_ptr<Serde<K>> key_serde,
    std::shared_ptr<Serde<V>> value_serde)
    : it_(std::move(it))
    , key_serde_(key_serde)
    , value_serde_(value_serde) {
    if (it_) {
        it_->SeekToFirst();
    }
}

template<typename K, typename V>
bool RocksDBKeyValueIterator<K, V>::hasNext() const {
    return it_ && it_->Valid();
}

template<typename K, typename V>
std::pair<K, V> RocksDBKeyValueIterator<K, V>::next() {
    if (!hasNext()) {
        throw std::runtime_error("No more elements in iterator");
    }
    
    auto key_slice = it_->key();
    auto value_slice = it_->value();
    
    std::vector<uint8_t> key_bytes(key_slice.data(), key_slice.data() + key_slice.size());
    std::vector<uint8_t> value_bytes(value_slice.data(), value_slice.data() + value_slice.size());
    
    K key = key_serde_->deserialize(key_bytes);
    V value = value_serde_->deserialize(value_bytes);
    
    it_->Next();
    
    return {key, value};
}

template<typename K, typename V>
void RocksDBKeyValueIterator<K, V>::close() {
    closed_ = true;
    it_.reset();
}

template<typename K, typename V>
RocksDBKeyValueStore<K, V>::RocksDBKeyValueStore(
    const std::string& name,
    const std::string& db_path,
    std::shared_ptr<Serde<K>> key_serde,
    std::shared_ptr<Serde<V>> value_serde,
    bool logging_enabled)
    : name_(name)
    , db_path_(db_path)
    , key_serde_(key_serde)
    , value_serde_(value_serde)
    , logging_enabled_(logging_enabled)
    , changelog_topic_(name + "-changelog") {}

template<typename K, typename V>
RocksDBKeyValueStore<K, V>::~RocksDBKeyValueStore() {
    if (isOpen()) {
        close();
    }
}

template<typename K, typename V>
void RocksDBKeyValueStore<K, V>::init() {
    rocksdb::Options options;
    options.create_if_missing = true;
    options.error_if_exists = false;
    
    std::unique_ptr<rocksdb::DB> db_ptr;
    rocksdb::Status status = openRocksDb(options, db_path_, db_ptr);
    
    if (!status.ok()) {
        throw std::runtime_error("Failed to open RocksDB: " + status.ToString());
    }
    
    db_ = std::move(db_ptr);
}

template<typename K, typename V>
void RocksDBKeyValueStore<K, V>::flush() {
    if (!isOpen()) {
        return;
    }
    
    rocksdb::FlushOptions options;
    options.wait = true;
    rocksdb::Status status = db_->Flush(options);
    
    if (!status.ok()) {
        throw std::runtime_error("Failed to flush RocksDB: " + status.ToString());
    }
}

template<typename K, typename V>
void RocksDBKeyValueStore<K, V>::close() {
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (db_) {
        flush();
        db_.reset();
    }
}

template<typename K, typename V>
void RocksDBKeyValueStore<K, V>::put(const K& key, const V& value) {
    if (!isOpen()) {
        throw std::runtime_error("Store is not open");
    }
    
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto key_bytes = serializeKey(key);
    auto value_bytes = serializeValue(value);
    
    rocksdb::Slice key_slice(reinterpret_cast<const char*>(key_bytes.data()), key_bytes.size());
    rocksdb::Slice value_slice(reinterpret_cast<const char*>(value_bytes.data()), value_bytes.size());
    
    rocksdb::Status status = db_->Put(rocksdb::WriteOptions(), key_slice, value_slice);
    
    if (!status.ok()) {
        throw std::runtime_error("Failed to put value: " + status.ToString());
    }
    
    // Send to changelog if enabled
    if (logging_enabled_) {
        sendToChangelog(key, value);
    }
}

template<typename K, typename V>
std::optional<V> RocksDBKeyValueStore<K, V>::putIfAbsent(const K& key, const V& value) {
    if (!isOpen()) {
        throw std::runtime_error("Store is not open");
    }
    
    std::lock_guard<std::mutex> lock(mutex_);
    
    // Check if key exists
    auto existing = get(key);
    if (existing) {
        return existing;
    }
    
    // Key doesn't exist, put the value
    put(key, value);
    return std::nullopt;
}

template<typename K, typename V>
void RocksDBKeyValueStore<K, V>::putAll(const std::vector<std::pair<K, V>>& entries) {
    if (!isOpen()) {
        throw std::runtime_error("Store is not open");
    }
    
    std::lock_guard<std::mutex> lock(mutex_);
    
    rocksdb::WriteBatch batch;
    
    for (const auto& [key, value] : entries) {
        auto key_bytes = serializeKey(key);
        auto value_bytes = serializeValue(value);
        
        rocksdb::Slice key_slice(reinterpret_cast<const char*>(key_bytes.data()), key_bytes.size());
        rocksdb::Slice value_slice(reinterpret_cast<const char*>(value_bytes.data()), value_bytes.size());
        
        batch.Put(key_slice, value_slice);
        
        // Send to changelog if enabled
        if (logging_enabled_) {
            sendToChangelog(key, value);
        }
    }
    
    rocksdb::Status status = db_->Write(rocksdb::WriteOptions(), &batch);
    
    if (!status.ok()) {
        throw std::runtime_error("Failed to write batch: " + status.ToString());
    }
}

template<typename K, typename V>
std::optional<V> RocksDBKeyValueStore<K, V>::get(const K& key) {
    if (!isOpen()) {
        throw std::runtime_error("Store is not open");
    }
    
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto key_bytes = serializeKey(key);
    rocksdb::Slice key_slice(reinterpret_cast<const char*>(key_bytes.data()), key_bytes.size());
    
    std::string value_str;
    rocksdb::Status status = db_->Get(rocksdb::ReadOptions(), key_slice, &value_str);
    
    if (status.IsNotFound()) {
        return std::nullopt;
    }
    
    if (!status.ok()) {
        throw std::runtime_error("Failed to get value: " + status.ToString());
    }
    
    std::vector<uint8_t> value_bytes(value_str.begin(), value_str.end());
    return deserializeValue(value_bytes);
}

template<typename K, typename V>
std::optional<V> RocksDBKeyValueStore<K, V>::remove(const K& key) {
    if (!isOpen()) {
        throw std::runtime_error("Store is not open");
    }
    
    std::lock_guard<std::mutex> lock(mutex_);
    
    // Get existing value first
    auto existing = get(key);
    
    auto key_bytes = serializeKey(key);
    rocksdb::Slice key_slice(reinterpret_cast<const char*>(key_bytes.data()), key_bytes.size());
    
    rocksdb::Status status = db_->Delete(rocksdb::WriteOptions(), key_slice);
    
    if (!status.ok() && !status.IsNotFound()) {
        throw std::runtime_error("Failed to delete value: " + status.ToString());
    }
    
    // Send tombstone to changelog if enabled
    if (logging_enabled_ && existing) {
        sendToChangelog(key, std::nullopt);
    }
    
    return existing;
}

template<typename K, typename V>
std::unique_ptr<KeyValueIterator<K, V>> RocksDBKeyValueStore<K, V>::all() {
    if (!isOpen()) {
        throw std::runtime_error("Store is not open");
    }
    
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto it = std::unique_ptr<rocksdb::Iterator>(
        db_->NewIterator(rocksdb::ReadOptions()));
    
    return std::make_unique<RocksDBKeyValueIterator<K, V>>(
        std::move(it), key_serde_, value_serde_);
}

template<typename K, typename V>
std::unique_ptr<KeyValueIterator<K, V>> RocksDBKeyValueStore<K, V>::range(
    const K& /*from*/, const K& /*to*/) {
    if (!isOpen()) {
        throw std::runtime_error("Store is not open");
    }
    
    std::lock_guard<std::mutex> lock(mutex_);
    
    // TODO: Implement range query with upper bound
    // For now, return all and let caller filter
    return all();
}

template<typename K, typename V>
int64_t RocksDBKeyValueStore<K, V>::approximateNumEntries() {
    if (!isOpen()) {
        return 0;
    }
    
    std::lock_guard<std::mutex> lock(mutex_);
    
    std::string value;
    db_->GetProperty("rocksdb.estimate-num-keys", &value);
    
    try {
        return std::stoll(value);
    } catch (...) {
        return 0;
    }
}

template<typename K, typename V>
std::vector<uint8_t> RocksDBKeyValueStore<K, V>::serializeKey(const K& key) {
    return key_serde_->serialize(key);
}

template<typename K, typename V>
K RocksDBKeyValueStore<K, V>::deserializeKey(const std::vector<uint8_t>& bytes) {
    return key_serde_->deserialize(bytes);
}

template<typename K, typename V>
std::vector<uint8_t> RocksDBKeyValueStore<K, V>::serializeValue(const V& value) {
    return value_serde_->serialize(value);
}

template<typename K, typename V>
V RocksDBKeyValueStore<K, V>::deserializeValue(const std::vector<uint8_t>& bytes) {
    return value_serde_->deserialize(bytes);
}

template<typename K, typename V>
void RocksDBKeyValueStore<K, V>::sendToChangelog(
    const K& /*key*/, const std::optional<V>& /*value*/) {
    // TODO: Implement changelog topic integration
    // For now, this is a placeholder that will be implemented when
    // we wire in the producer client for changelog topics
    // 
    // Implementation would:
    // 1. Serialize key and value (or null for tombstone)
    // 2. Send to changelog topic using producer
    // 3. Handle errors and retries
}

} // namespace streams
} // namespace kawasan
