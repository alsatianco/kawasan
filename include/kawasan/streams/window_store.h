#pragma once

#include "state_store.h"
#include "windows.h"
#include <rocksdb/db.h>
#include <memory>
#include <string>
#include <mutex>
#include <cstring>
#include <algorithm>

namespace kawasan {
namespace streams {

/**
 * Composite key for window store entries.
 *
 * The key combines the record key with a timestamp to enable efficient
 * time-range queries using RocksDB's sorted key order.
 *
 * Key format: [key_bytes][timestamp_big_endian]
 * Using big-endian for timestamp ensures proper lexicographic ordering.
 */
template<typename K>
struct WindowedKey {
    K key;
    int64_t timestamp_ms;

    WindowedKey() = default;
    WindowedKey(const K& k, int64_t ts) : key(k), timestamp_ms(ts) {}

    bool operator<(const WindowedKey& other) const {
        if (key != other.key) {
            return key < other.key;
        }
        return timestamp_ms < other.timestamp_ms;
    }

    bool operator==(const WindowedKey& other) const {
        return key == other.key && timestamp_ms == other.timestamp_ms;
    }
};

/**
 * Serde for windowed keys.
 *
 * Serializes keys as: [key_length][key_bytes][timestamp_big_endian]
 * This format ensures proper ordering by key first, then by timestamp.
 */
template<typename K>
class WindowedKeySerde : public Serde<WindowedKey<K>> {
public:
    explicit WindowedKeySerde(std::shared_ptr<Serde<K>> key_serde)
        : key_serde_(key_serde) {}

    std::vector<uint8_t> serialize(const WindowedKey<K>& data) override {
        auto key_bytes = key_serde_->serialize(data.key);

        std::vector<uint8_t> result;
        result.reserve(4 + key_bytes.size() + 8);

        // Key length (4 bytes, big-endian)
        uint32_t key_len = static_cast<uint32_t>(key_bytes.size());
        result.push_back((key_len >> 24) & 0xFF);
        result.push_back((key_len >> 16) & 0xFF);
        result.push_back((key_len >> 8) & 0xFF);
        result.push_back(key_len & 0xFF);

        // Key bytes
        result.insert(result.end(), key_bytes.begin(), key_bytes.end());

        // Timestamp (8 bytes, big-endian for proper ordering)
        int64_t ts = data.timestamp_ms;
        result.push_back((ts >> 56) & 0xFF);
        result.push_back((ts >> 48) & 0xFF);
        result.push_back((ts >> 40) & 0xFF);
        result.push_back((ts >> 32) & 0xFF);
        result.push_back((ts >> 24) & 0xFF);
        result.push_back((ts >> 16) & 0xFF);
        result.push_back((ts >> 8) & 0xFF);
        result.push_back(ts & 0xFF);

        return result;
    }

    WindowedKey<K> deserialize(const std::vector<uint8_t>& bytes) override {
        if (bytes.size() < 12) {  // minimum: 4 (length) + 0 (key) + 8 (timestamp)
            throw std::runtime_error("Invalid windowed key bytes");
        }

        // Read key length
        uint32_t key_len = (static_cast<uint32_t>(bytes[0]) << 24) |
                           (static_cast<uint32_t>(bytes[1]) << 16) |
                           (static_cast<uint32_t>(bytes[2]) << 8) |
                           static_cast<uint32_t>(bytes[3]);

        if (bytes.size() != 4 + key_len + 8) {
            throw std::runtime_error("Invalid windowed key bytes size");
        }

        // Read key bytes
        std::vector<uint8_t> key_bytes(bytes.begin() + 4, bytes.begin() + 4 + key_len);
        K key = key_serde_->deserialize(key_bytes);

        // Read timestamp (big-endian)
        size_t ts_offset = 4 + key_len;
        int64_t ts = (static_cast<int64_t>(bytes[ts_offset]) << 56) |
                     (static_cast<int64_t>(bytes[ts_offset + 1]) << 48) |
                     (static_cast<int64_t>(bytes[ts_offset + 2]) << 40) |
                     (static_cast<int64_t>(bytes[ts_offset + 3]) << 32) |
                     (static_cast<int64_t>(bytes[ts_offset + 4]) << 24) |
                     (static_cast<int64_t>(bytes[ts_offset + 5]) << 16) |
                     (static_cast<int64_t>(bytes[ts_offset + 6]) << 8) |
                     static_cast<int64_t>(bytes[ts_offset + 7]);

        return WindowedKey<K>(key, ts);
    }

private:
    std::shared_ptr<Serde<K>> key_serde_;
};

/**
 * Iterator for window store time range queries.
 */
template<typename V>
class WindowStoreTimeRangeIterator : public KeyValueIterator<int64_t, V> {
public:
    WindowStoreTimeRangeIterator(
        std::unique_ptr<rocksdb::Iterator> it,
        std::shared_ptr<Serde<V>> value_serde,
        int64_t time_from,
        int64_t time_to,
        const std::vector<uint8_t>& key_prefix)
        : it_(std::move(it))
        , value_serde_(value_serde)
        , time_from_(time_from)
        , time_to_(time_to)
        , key_prefix_(key_prefix) {
        seekToFirst();
    }

    bool hasNext() const override {
        return it_ && it_->Valid() && isWithinBounds();
    }

    std::pair<int64_t, V> next() override {
        if (!hasNext()) {
            throw std::runtime_error("No more elements in iterator");
        }

        auto key_slice = it_->key();
        auto value_slice = it_->value();

        // Extract timestamp from the end of the key
        int64_t ts = extractTimestamp(key_slice);

        std::vector<uint8_t> value_bytes(value_slice.data(),
                                         value_slice.data() + value_slice.size());
        V value = value_serde_->deserialize(value_bytes);

        it_->Next();

        return {ts, value};
    }

    void close() override {
        it_.reset();
    }

private:
    void seekToFirst() {
        if (!it_) return;

        // Seek to the start of the key prefix
        if (!key_prefix_.empty()) {
            rocksdb::Slice prefix(reinterpret_cast<const char*>(key_prefix_.data()),
                                  key_prefix_.size());
            it_->Seek(prefix);
        } else {
            it_->SeekToFirst();
        }

        // Skip entries before time_from
        while (it_->Valid() && hasMatchingKeyPrefix()) {
            int64_t ts = extractTimestamp(it_->key());
            if (ts >= time_from_) {
                break;
            }
            it_->Next();
        }
    }

    bool isWithinBounds() const {
        if (!hasMatchingKeyPrefix()) {
            return false;
        }
        int64_t ts = extractTimestamp(it_->key());
        return ts >= time_from_ && ts <= time_to_;
    }

    bool hasMatchingKeyPrefix() const {
        if (key_prefix_.empty()) {
            return true;
        }

        auto key_slice = it_->key();
        if (key_slice.size() < key_prefix_.size()) {
            return false;
        }

        return std::memcmp(key_slice.data(), key_prefix_.data(), key_prefix_.size()) == 0;
    }

    int64_t extractTimestamp(const rocksdb::Slice& key) const {
        // Timestamp is the last 8 bytes of the key
        if (key.size() < 8) {
            return 0;
        }

        const uint8_t* data = reinterpret_cast<const uint8_t*>(key.data() + key.size() - 8);
        return (static_cast<int64_t>(data[0]) << 56) |
               (static_cast<int64_t>(data[1]) << 48) |
               (static_cast<int64_t>(data[2]) << 40) |
               (static_cast<int64_t>(data[3]) << 32) |
               (static_cast<int64_t>(data[4]) << 24) |
               (static_cast<int64_t>(data[5]) << 16) |
               (static_cast<int64_t>(data[6]) << 8) |
               static_cast<int64_t>(data[7]);
    }

    std::unique_ptr<rocksdb::Iterator> it_;
    std::shared_ptr<Serde<V>> value_serde_;
    int64_t time_from_;
    int64_t time_to_;
    std::vector<uint8_t> key_prefix_;
};

/**
 * Iterator for fetching all entries in a time range.
 */
template<typename K, typename V>
class WindowStoreAllIterator : public KeyValueIterator<std::pair<K, int64_t>, V> {
public:
    WindowStoreAllIterator(
        std::unique_ptr<rocksdb::Iterator> it,
        std::shared_ptr<Serde<K>> key_serde,
        std::shared_ptr<Serde<V>> value_serde,
        int64_t time_from,
        int64_t time_to)
        : it_(std::move(it))
        , windowed_key_serde_(std::make_shared<WindowedKeySerde<K>>(key_serde))
        , value_serde_(value_serde)
        , time_from_(time_from)
        , time_to_(time_to) {
        if (it_) {
            it_->SeekToFirst();
            skipToValidEntry();
        }
    }

    bool hasNext() const override {
        return it_ && it_->Valid() && isWithinTimeRange();
    }

    std::pair<std::pair<K, int64_t>, V> next() override {
        if (!hasNext()) {
            throw std::runtime_error("No more elements in iterator");
        }

        auto key_slice = it_->key();
        auto value_slice = it_->value();

        std::vector<uint8_t> key_bytes(key_slice.data(), key_slice.data() + key_slice.size());
        WindowedKey<K> wk = windowed_key_serde_->deserialize(key_bytes);

        std::vector<uint8_t> value_bytes(value_slice.data(),
                                         value_slice.data() + value_slice.size());
        V value = value_serde_->deserialize(value_bytes);

        it_->Next();
        skipToValidEntry();

        return {{wk.key, wk.timestamp_ms}, value};
    }

    void close() override {
        it_.reset();
    }

private:
    void skipToValidEntry() {
        while (it_->Valid() && !isWithinTimeRange()) {
            it_->Next();
        }
    }

    bool isWithinTimeRange() const {
        if (!it_->Valid()) return false;

        auto key_slice = it_->key();
        // Extract timestamp from key
        if (key_slice.size() < 8) return false;

        const uint8_t* data = reinterpret_cast<const uint8_t*>(
            key_slice.data() + key_slice.size() - 8);
        int64_t ts = (static_cast<int64_t>(data[0]) << 56) |
                     (static_cast<int64_t>(data[1]) << 48) |
                     (static_cast<int64_t>(data[2]) << 40) |
                     (static_cast<int64_t>(data[3]) << 32) |
                     (static_cast<int64_t>(data[4]) << 24) |
                     (static_cast<int64_t>(data[5]) << 16) |
                     (static_cast<int64_t>(data[6]) << 8) |
                     static_cast<int64_t>(data[7]);

        return ts >= time_from_ && ts <= time_to_;
    }

    std::unique_ptr<rocksdb::Iterator> it_;
    std::shared_ptr<WindowedKeySerde<K>> windowed_key_serde_;
    std::shared_ptr<Serde<V>> value_serde_;
    int64_t time_from_;
    int64_t time_to_;
};

/**
 * RocksDB-backed window store implementation.
 *
 * This store provides windowed key-value storage with:
 * - Time-based key organization for efficient range scans
 * - Automatic expiration of old windows
 * - Support for tumbling, hopping, and session windows
 */
template<typename K, typename V>
class RocksDBWindowStore : public WindowStore<K, V> {
public:
    /**
     * Create a new RocksDB window store.
     *
     * @param name Store name
     * @param db_path Path to RocksDB database directory
     * @param retention_ms How long to keep window data (in milliseconds)
     * @param key_serde Serializer/deserializer for keys
     * @param value_serde Serializer/deserializer for values
     * @param logging_enabled Enable changelog topic integration
     */
    RocksDBWindowStore(
        const std::string& name,
        const std::string& db_path,
        int64_t retention_ms,
        std::shared_ptr<Serde<K>> key_serde,
        std::shared_ptr<Serde<V>> value_serde,
        bool logging_enabled = true)
        : name_(name)
        , db_path_(db_path)
        , retention_ms_(retention_ms)
        , key_serde_(key_serde)
        , value_serde_(value_serde)
        , windowed_key_serde_(std::make_shared<WindowedKeySerde<K>>(key_serde))
        , logging_enabled_(logging_enabled)
        , changelog_topic_(name + "-changelog")
        , observed_stream_time_(0) {}

    ~RocksDBWindowStore() override {
        if (isOpen()) {
            close();
        }
    }

    // StateStore interface
    std::string name() const override { return name_; }

    void init() override {
        rocksdb::Options options;
        options.create_if_missing = true;
        options.error_if_exists = false;

        rocksdb::DB* db_ptr = nullptr;
        rocksdb::Status status = rocksdb::DB::Open(options, db_path_, &db_ptr);

        if (!status.ok()) {
            throw std::runtime_error("Failed to open RocksDB: " + status.ToString());
        }

        db_.reset(db_ptr);
    }

    void flush() override {
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

    void close() override {
        std::lock_guard<std::mutex> lock(mutex_);

        if (db_) {
            flush();
            db_.reset();
        }
    }

    bool isOpen() const override { return db_ != nullptr; }

    bool persistent() const override { return true; }

    // WindowStore interface
    void put(const K& key, const V& value, int64_t timestamp) override {
        if (!isOpen()) {
            throw std::runtime_error("Store is not open");
        }

        std::lock_guard<std::mutex> lock(mutex_);

        // Update observed stream time
        if (timestamp > observed_stream_time_) {
            observed_stream_time_ = timestamp;
        }

        WindowedKey<K> wk(key, timestamp);
        auto key_bytes = windowed_key_serde_->serialize(wk);
        auto value_bytes = value_serde_->serialize(value);

        rocksdb::Slice key_slice(reinterpret_cast<const char*>(key_bytes.data()),
                                 key_bytes.size());
        rocksdb::Slice value_slice(reinterpret_cast<const char*>(value_bytes.data()),
                                   value_bytes.size());

        rocksdb::Status status = db_->Put(rocksdb::WriteOptions(), key_slice, value_slice);

        if (!status.ok()) {
            throw std::runtime_error("Failed to put value: " + status.ToString());
        }

        // Periodically clean up expired entries
        maybeExpireOldEntries();
    }

    std::optional<V> fetch(const K& key, int64_t timestamp) override {
        if (!isOpen()) {
            throw std::runtime_error("Store is not open");
        }

        std::lock_guard<std::mutex> lock(mutex_);

        WindowedKey<K> wk(key, timestamp);
        auto key_bytes = windowed_key_serde_->serialize(wk);

        rocksdb::Slice key_slice(reinterpret_cast<const char*>(key_bytes.data()),
                                 key_bytes.size());

        std::string value_str;
        rocksdb::Status status = db_->Get(rocksdb::ReadOptions(), key_slice, &value_str);

        if (status.IsNotFound()) {
            return std::nullopt;
        }

        if (!status.ok()) {
            throw std::runtime_error("Failed to get value: " + status.ToString());
        }

        std::vector<uint8_t> value_bytes(value_str.begin(), value_str.end());
        return value_serde_->deserialize(value_bytes);
    }

    std::unique_ptr<KeyValueIterator<int64_t, V>> fetch(
        const K& key, int64_t time_from, int64_t time_to) override {
        if (!isOpen()) {
            throw std::runtime_error("Store is not open");
        }

        std::lock_guard<std::mutex> lock(mutex_);

        // Build key prefix for the given key
        auto key_bytes = key_serde_->serialize(key);
        std::vector<uint8_t> prefix;
        prefix.reserve(4 + key_bytes.size());

        // Key length (4 bytes, big-endian)
        uint32_t key_len = static_cast<uint32_t>(key_bytes.size());
        prefix.push_back((key_len >> 24) & 0xFF);
        prefix.push_back((key_len >> 16) & 0xFF);
        prefix.push_back((key_len >> 8) & 0xFF);
        prefix.push_back(key_len & 0xFF);
        prefix.insert(prefix.end(), key_bytes.begin(), key_bytes.end());

        auto it = std::unique_ptr<rocksdb::Iterator>(
            db_->NewIterator(rocksdb::ReadOptions()));

        return std::make_unique<WindowStoreTimeRangeIterator<V>>(
            std::move(it), value_serde_, time_from, time_to, prefix);
    }

    std::unique_ptr<KeyValueIterator<std::pair<K, int64_t>, V>> fetchAll(
        int64_t time_from, int64_t time_to) override {
        if (!isOpen()) {
            throw std::runtime_error("Store is not open");
        }

        std::lock_guard<std::mutex> lock(mutex_);

        auto it = std::unique_ptr<rocksdb::Iterator>(
            db_->NewIterator(rocksdb::ReadOptions()));

        return std::make_unique<WindowStoreAllIterator<K, V>>(
            std::move(it), key_serde_, value_serde_, time_from, time_to);
    }

    /**
     * Set the changelog topic name for this store.
     */
    void setChangelogTopic(const std::string& topic) {
        changelog_topic_ = topic;
    }

    /**
     * Get the changelog topic name.
     */
    std::string changelogTopic() const { return changelog_topic_; }

    /**
     * Get the retention period in milliseconds.
     */
    int64_t retentionMs() const { return retention_ms_; }

    /**
     * Get the current observed stream time.
     */
    int64_t observedStreamTime() const { return observed_stream_time_; }

private:
    void maybeExpireOldEntries() {
        // Only expire if we have enough stream time progress
        int64_t expire_before = observed_stream_time_ - retention_ms_;
        if (expire_before <= last_expiration_time_) {
            return;
        }

        // Delete entries older than the retention period
        // For efficiency, we do this in batches
        auto it = std::unique_ptr<rocksdb::Iterator>(
            db_->NewIterator(rocksdb::ReadOptions()));

        rocksdb::WriteBatch batch;
        int deleted = 0;
        const int max_deletes_per_batch = 1000;

        for (it->SeekToFirst(); it->Valid() && deleted < max_deletes_per_batch; it->Next()) {
            auto key_slice = it->key();
            if (key_slice.size() < 8) continue;

            // Extract timestamp from the end of the key
            const uint8_t* data = reinterpret_cast<const uint8_t*>(
                key_slice.data() + key_slice.size() - 8);
            int64_t ts = (static_cast<int64_t>(data[0]) << 56) |
                         (static_cast<int64_t>(data[1]) << 48) |
                         (static_cast<int64_t>(data[2]) << 40) |
                         (static_cast<int64_t>(data[3]) << 32) |
                         (static_cast<int64_t>(data[4]) << 24) |
                         (static_cast<int64_t>(data[5]) << 16) |
                         (static_cast<int64_t>(data[6]) << 8) |
                         static_cast<int64_t>(data[7]);

            if (ts < expire_before) {
                batch.Delete(key_slice);
                deleted++;
            }
        }

        if (deleted > 0) {
            rocksdb::Status status = db_->Write(rocksdb::WriteOptions(), &batch);
            if (!status.ok()) {
                // Log error but don't throw - expiration is best-effort
            }
        }

        last_expiration_time_ = expire_before;
    }

    std::string name_;
    std::string db_path_;
    int64_t retention_ms_;
    std::shared_ptr<Serde<K>> key_serde_;
    std::shared_ptr<Serde<V>> value_serde_;
    std::shared_ptr<WindowedKeySerde<K>> windowed_key_serde_;
    bool logging_enabled_;
    std::string changelog_topic_;

    std::unique_ptr<rocksdb::DB> db_;
    mutable std::mutex mutex_;

    int64_t observed_stream_time_ = 0;
    int64_t last_expiration_time_ = 0;
};

} // namespace streams
} // namespace kawasan
