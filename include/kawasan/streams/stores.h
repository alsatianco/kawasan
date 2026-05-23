#pragma once

#include "state_store.h"
#include "rocksdb_store.h"
#include "window_store.h"
#include <memory>
#include <string>
#include <chrono>
#include <filesystem>

namespace kawasan {
namespace streams {

/**
 * Materialized configuration for state stores.
 * Specifies how a state store should be materialized.
 */
template<typename K, typename V>
class Materialized {
public:
    // Default constructor with auto-generated store name
    Materialized() : store_name_("store-" + std::to_string(reinterpret_cast<uintptr_t>(this))) {}
    
    Materialized(const std::string& name) : store_name_(name) {}
    
    std::string storeName() const { return store_name_; }
    
    Materialized& withKeySerde(std::shared_ptr<Serde<K>> serde) {
        key_serde_ = serde;
        return *this;
    }
    
    Materialized& withValueSerde(std::shared_ptr<Serde<V>> serde) {
        value_serde_ = serde;
        return *this;
    }
    
    Materialized& withCachingEnabled() {
        caching_enabled_ = true;
        return *this;
    }
    
    Materialized& withLoggingEnabled() {
        logging_enabled_ = true;
        return *this;
    }
    
    std::shared_ptr<Serde<K>> keySerde() const { return key_serde_; }
    std::shared_ptr<Serde<V>> valueSerde() const { return value_serde_; }
    bool cachingEnabled() const { return caching_enabled_; }
    bool loggingEnabled() const { return logging_enabled_; }
    
private:
    std::string store_name_;
    std::shared_ptr<Serde<K>> key_serde_;
    std::shared_ptr<Serde<V>> value_serde_;
    bool caching_enabled_ = false;
    bool logging_enabled_ = true; // Enabled by default for fault tolerance
};

/**
 * Abstract builder for state stores.
 */
template<typename K, typename V, typename Store>
class StateStoreBuilder {
public:
    virtual ~StateStoreBuilder() = default;
    
    /**
     * Get the name of the store to be built.
     */
    virtual std::string name() const = 0;
    
    /**
     * Build the state store.
     */
    virtual std::shared_ptr<Store> build() = 0;
    
    /**
     * Enable/disable logging (changelog topic).
     */
    virtual StateStoreBuilder& withLoggingEnabled(bool enabled) = 0;
    
    /**
     * Enable/disable caching.
     */
    virtual StateStoreBuilder& withCachingEnabled(bool enabled) = 0;
};

/**
 * KeyValueStore builder implementation.
 */
template<typename K, typename V>
class KeyValueStoreBuilder : public StateStoreBuilder<K, V, KeyValueStore<K, V>> {
public:
    KeyValueStoreBuilder(
        const std::string& name,
        std::shared_ptr<Serde<K>> key_serde,
        std::shared_ptr<Serde<V>> value_serde,
        const std::string& state_dir = "/tmp/kawasan-streams-state")
        : name_(name)
        , key_serde_(key_serde)
        , value_serde_(value_serde)
        , state_dir_(state_dir)
        , logging_enabled_(true)
        , caching_enabled_(false) {}
    
    std::string name() const override { return name_; }
    
    std::shared_ptr<KeyValueStore<K, V>> build() override {
        // Create directory if it doesn't exist
        std::filesystem::path db_path = std::filesystem::path(state_dir_) / name_;
        std::filesystem::create_directories(db_path);
        
        auto store = std::make_shared<RocksDBKeyValueStore<K, V>>(
            name_,
            db_path.string(),
            key_serde_,
            value_serde_,
            logging_enabled_
        );
        
        store->init();
        return store;
    }
    
    KeyValueStoreBuilder& withLoggingEnabled(bool enabled) override {
        logging_enabled_ = enabled;
        return *this;
    }
    
    KeyValueStoreBuilder& withCachingEnabled(bool enabled) override {
        caching_enabled_ = enabled;
        return *this;
    }
    
    std::shared_ptr<Serde<K>> keySerde() const { return key_serde_; }
    std::shared_ptr<Serde<V>> valueSerde() const { return value_serde_; }
    std::string stateDir() const { return state_dir_; }
    bool loggingEnabled() const { return logging_enabled_; }
    bool cachingEnabled() const { return caching_enabled_; }
    
private:
    std::string name_;
    std::shared_ptr<Serde<K>> key_serde_;
    std::shared_ptr<Serde<V>> value_serde_;
    std::string state_dir_;
    bool logging_enabled_;
    bool caching_enabled_;
};

/**
 * WindowStore builder implementation.
 */
template<typename K, typename V>
class WindowStoreBuilder : public StateStoreBuilder<K, V, WindowStore<K, V>> {
public:
    WindowStoreBuilder(
        const std::string& name,
        std::chrono::milliseconds retention,
        std::shared_ptr<Serde<K>> key_serde,
        std::shared_ptr<Serde<V>> value_serde,
        const std::string& state_dir = "/tmp/kawasan-streams-state")
        : name_(name)
        , retention_(retention)
        , key_serde_(key_serde)
        , value_serde_(value_serde)
        , state_dir_(state_dir)
        , logging_enabled_(true)
        , caching_enabled_(false) {}
    
    std::string name() const override { return name_; }
    
    std::shared_ptr<WindowStore<K, V>> build() override {
        // Create directory if it doesn't exist
        std::filesystem::path db_path = std::filesystem::path(state_dir_) / name_;
        std::filesystem::create_directories(db_path);

        auto store = std::make_shared<RocksDBWindowStore<K, V>>(
            name_,
            db_path.string(),
            retention_.count(),
            key_serde_,
            value_serde_,
            logging_enabled_
        );

        store->init();
        return store;
    }
    
    WindowStoreBuilder& withLoggingEnabled(bool enabled) override {
        logging_enabled_ = enabled;
        return *this;
    }
    
    WindowStoreBuilder& withCachingEnabled(bool enabled) override {
        caching_enabled_ = enabled;
        return *this;
    }
    
    std::chrono::milliseconds retention() const { return retention_; }
    std::shared_ptr<Serde<K>> keySerde() const { return key_serde_; }
    std::shared_ptr<Serde<V>> valueSerde() const { return value_serde_; }
    std::string stateDir() const { return state_dir_; }
    bool loggingEnabled() const { return logging_enabled_; }
    bool cachingEnabled() const { return caching_enabled_; }
    
private:
    std::string name_;
    std::chrono::milliseconds retention_;
    std::shared_ptr<Serde<K>> key_serde_;
    std::shared_ptr<Serde<V>> value_serde_;
    std::string state_dir_;
    bool logging_enabled_;
    bool caching_enabled_;
};

/**
 * Simple in-memory key-value store for testing.
 */
template<typename K, typename V>
class InMemoryKeyValueStore : public KeyValueStore<K, V> {
public:
    explicit InMemoryKeyValueStore(const std::string& name = "in-memory-store")
        : name_(name), is_open_(true) {}

    std::string name() const override { return name_; }
    void init() override { is_open_ = true; }
    void flush() override {}
    void close() override { is_open_ = false; }
    bool isOpen() const override { return is_open_; }
    bool persistent() const override { return false; }

    void put(const K& key, const V& value) override {
        store_[key] = value;
    }

    std::optional<V> putIfAbsent(const K& key, const V& value) override {
        auto it = store_.find(key);
        if (it != store_.end()) {
            return it->second;
        }
        store_[key] = value;
        return std::nullopt;
    }

    void putAll(const std::vector<std::pair<K, V>>& entries) override {
        for (const auto& [k, v] : entries) {
            store_[k] = v;
        }
    }

    std::optional<V> get(const K& key) override {
        auto it = store_.find(key);
        if (it != store_.end()) {
            return it->second;
        }
        return std::nullopt;
    }

    std::optional<V> remove(const K& key) override {
        auto it = store_.find(key);
        if (it != store_.end()) {
            V value = it->second;
            store_.erase(it);
            return value;
        }
        return std::nullopt;
    }

    std::unique_ptr<KeyValueIterator<K, V>> all() override {
        return nullptr;  // Simplified for testing
    }

    std::unique_ptr<KeyValueIterator<K, V>> range(const K&, const K&) override {
        return nullptr;  // Simplified for testing
    }

    int64_t approximateNumEntries() override {
        return static_cast<int64_t>(store_.size());
    }

private:
    std::string name_;
    std::map<K, V> store_;
    bool is_open_;
};

/**
 * Factory class for creating state store builders.
 */
class Stores {
public:
    /**
     * Create a key-value store builder.
     * 
     * @param name Store name (must be unique per topology)
     * @param key_serde Serializer/deserializer for keys
     * @param value_serde Serializer/deserializer for values
     * @return KeyValueStoreBuilder instance
     */
    template<typename K, typename V>
    static KeyValueStoreBuilder<K, V> keyValueStoreBuilder(
        const std::string& name,
        std::shared_ptr<Serde<K>> key_serde,
        std::shared_ptr<Serde<V>> value_serde) {
        return KeyValueStoreBuilder<K, V>(name, key_serde, value_serde);
    }
    
    /**
     * Create a window store builder.
     * 
     * @param name Store name (must be unique per topology)
     * @param retention Window retention duration
     * @param key_serde Serializer/deserializer for keys
     * @param value_serde Serializer/deserializer for values
     * @return WindowStoreBuilder instance
     */
    template<typename K, typename V>
    static WindowStoreBuilder<K, V> windowStoreBuilder(
        const std::string& name,
        std::chrono::milliseconds retention,
        std::shared_ptr<Serde<K>> key_serde,
        std::shared_ptr<Serde<V>> value_serde) {
        return WindowStoreBuilder<K, V>(name, retention, key_serde, value_serde);
    }
    
    /**
     * Convenience method to create a persistent key-value store builder.
     */
    template<typename K, typename V>
    static KeyValueStoreBuilder<K, V> persistentKeyValueStore(
        const std::string& name) {
        return keyValueStoreBuilder<K, V>(
            name,
            std::make_shared<StringSerde>(),
            std::make_shared<StringSerde>()
        );
    }
    
    /**
     * Convenience method to create a persistent window store builder.
     */
    template<typename K, typename V>
    static WindowStoreBuilder<K, V> persistentWindowStore(
        const std::string& name,
        std::chrono::milliseconds retention) {
        return windowStoreBuilder<K, V>(
            name,
            retention,
            std::make_shared<StringSerde>(),
            std::make_shared<StringSerde>()
        );
    }
};

} // namespace streams
} // namespace kawasan
