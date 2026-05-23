#pragma once

#include "serde.h"
#include <optional>
#include <string>
#include <vector>
#include <memory>
#include <functional>
#include <cstdint>

namespace kawasan {
namespace streams {

/**
 * Base interface for all state stores.
 */
class StateStore {
public:
    virtual ~StateStore() = default;
    
    /**
     * Get the name of this state store.
     */
    virtual std::string name() const = 0;
    
    /**
     * Initialize the state store.
     */
    virtual void init() = 0;
    
    /**
     * Flush any cached data to persistent storage.
     */
    virtual void flush() = 0;
    
    /**
     * Close the state store and release resources.
     */
    virtual void close() = 0;
    
    /**
     * Check if the state store is open.
     */
    virtual bool isOpen() const = 0;
    
    /**
     * Check if the state store is persistent.
     */
    virtual bool persistent() const = 0;
};

/**
 * Iterator interface for traversing key-value pairs.
 */
template<typename K, typename V>
class KeyValueIterator {
public:
    virtual ~KeyValueIterator() = default;
    
    /**
     * Check if there are more entries.
     */
    virtual bool hasNext() const = 0;
    
    /**
     * Get the next entry.
     * Returns pair of (key, value).
     */
    virtual std::pair<K, V> next() = 0;
    
    /**
     * Close the iterator and release resources.
     */
    virtual void close() = 0;
};

/**
 * Key-value store interface.
 * 
 * This is the primary interface for stateful stream processing operations
 * like aggregations and joins.
 */
template<typename K, typename V>
class KeyValueStore : public StateStore {
public:
    /**
     * Put a key-value pair into the store.
     * If the key already exists, its value is updated.
     * 
     * @param key The key
     * @param value The value
     */
    virtual void put(const K& key, const V& value) = 0;
    
    /**
     * Put a key-value pair only if the key doesn't already exist.
     * 
     * @param key The key
     * @param value The value
     * @return The previous value if the key existed, nullopt otherwise
     */
    virtual std::optional<V> putIfAbsent(const K& key, const V& value) = 0;
    
    /**
     * Put all key-value pairs from the given vector.
     * 
     * @param entries Vector of (key, value) pairs
     */
    virtual void putAll(const std::vector<std::pair<K, V>>& entries) = 0;
    
    /**
     * Get the value associated with the given key.
     * 
     * @param key The key
     * @return The value if found, nullopt otherwise
     */
    virtual std::optional<V> get(const K& key) = 0;
    
    /**
     * Remove the key-value pair for the given key.
     * 
     * @param key The key
     * @return The previous value if the key existed, nullopt otherwise
     */
    virtual std::optional<V> remove(const K& key) = 0;
    
    /**
     * Get an iterator over all entries in the store.
     * 
     * @return Iterator over all (key, value) pairs
     */
    virtual std::unique_ptr<KeyValueIterator<K, V>> all() = 0;
    
    /**
     * Get an iterator over entries with keys in the given range.
     * The range is inclusive on both ends.
     * 
     * @param from Start key (inclusive)
     * @param to End key (inclusive)
     * @return Iterator over matching (key, value) pairs
     */
    virtual std::unique_ptr<KeyValueIterator<K, V>> range(const K& from, const K& to) = 0;
    
    /**
     * Get an approximate count of entries in the store.
     * 
     * @return Approximate entry count
     */
    virtual int64_t approximateNumEntries() = 0;
};

/**
 * Window store interface for time-windowed data.
 */
template<typename K, typename V>
class WindowStore : public StateStore {
public:
    /**
     * Put a value for the given key and timestamp.
     * 
     * @param key The key
     * @param value The value
     * @param timestamp The timestamp in milliseconds
     */
    virtual void put(const K& key, const V& value, int64_t timestamp) = 0;
    
    /**
     * Get the value for the given key at the specified timestamp.
     * 
     * @param key The key
     * @param timestamp The timestamp in milliseconds
     * @return The value if found, nullopt otherwise
     */
    virtual std::optional<V> fetch(const K& key, int64_t timestamp) = 0;
    
    /**
     * Get all values for the given key within the time range.
     * 
     * @param key The key
     * @param timeFrom Start time (inclusive)
     * @param timeTo End time (inclusive)
     * @return Iterator over matching (timestamp, value) pairs
     */
    virtual std::unique_ptr<KeyValueIterator<int64_t, V>> fetch(
        const K& key, int64_t timeFrom, int64_t timeTo) = 0;
    
    /**
     * Get all values for all keys within the time range.
     * 
     * @param timeFrom Start time (inclusive)
     * @param timeTo End time (inclusive)
     * @return Iterator over matching (key, value, timestamp) tuples
     */
    virtual std::unique_ptr<KeyValueIterator<std::pair<K, int64_t>, V>> fetchAll(
        int64_t timeFrom, int64_t timeTo) = 0;
};

// Serde classes are now defined in serde.h

} // namespace streams
} // namespace kawasan
