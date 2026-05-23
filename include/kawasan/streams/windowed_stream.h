#pragma once

#include "windows.h"
#include "stores.h"
#include "state_store.h"
#include <functional>
#include <memory>
#include <string>

namespace kawasan {
namespace streams {

// Forward declarations
template<typename K, typename V> class KTable;
class StreamsBuilder;

/**
 * Windowed aggregate result containing the window and the aggregated value.
 */
template<typename K, typename V>
struct Windowed {
    K key;
    Window window;
    V value;

    Windowed() = default;
    Windowed(const K& k, const Window& w, const V& v) : key(k), window(w), value(v) {}
};

/**
 * Materialized configuration for windowed state stores.
 *
 * Extends the base Materialized with window-specific configuration.
 */
template<typename K, typename V>
class WindowedMaterialized {
public:
    WindowedMaterialized() : store_name_("windowed-store-" +
        std::to_string(reinterpret_cast<uintptr_t>(this))) {}

    explicit WindowedMaterialized(const std::string& name) : store_name_(name) {}

    std::string storeName() const { return store_name_; }

    WindowedMaterialized& withKeySerde(std::shared_ptr<Serde<K>> serde) {
        key_serde_ = serde;
        return *this;
    }

    WindowedMaterialized& withValueSerde(std::shared_ptr<Serde<V>> serde) {
        value_serde_ = serde;
        return *this;
    }

    WindowedMaterialized& withRetention(Duration retention) {
        retention_ms_ = retention.count();
        return *this;
    }

    WindowedMaterialized& withCachingEnabled() {
        caching_enabled_ = true;
        return *this;
    }

    WindowedMaterialized& withLoggingEnabled() {
        logging_enabled_ = true;
        return *this;
    }

    std::shared_ptr<Serde<K>> keySerde() const { return key_serde_; }
    std::shared_ptr<Serde<V>> valueSerde() const { return value_serde_; }
    int64_t retentionMs() const { return retention_ms_; }
    bool cachingEnabled() const { return caching_enabled_; }
    bool loggingEnabled() const { return logging_enabled_; }

private:
    std::string store_name_;
    std::shared_ptr<Serde<K>> key_serde_;
    std::shared_ptr<Serde<V>> value_serde_;
    int64_t retention_ms_ = 24 * 60 * 60 * 1000; // Default 24 hours
    bool caching_enabled_ = false;
    bool logging_enabled_ = true;
};

/**
 * TimeWindowedKStream represents a grouped stream with time-based windowing.
 *
 * Records are grouped by key AND by time window before aggregation.
 * This enables time-based aggregations like "count per 5-minute window".
 *
 * @tparam K Key type
 * @tparam V Value type
 */
template<typename K, typename V>
class TimeWindowedKStream {
public:
    /**
     * Count the number of records in each window.
     *
     * @return KTable with windowed counts
     */
    KTable<Windowed<K, int64_t>, int64_t> count();

    /**
     * Count the number of records in each window.
     *
     * @param materialized Configuration for the state store
     * @return KTable with windowed counts
     */
    KTable<Windowed<K, int64_t>, int64_t> count(
        const WindowedMaterialized<K, int64_t>& materialized);

    /**
     * Aggregate records in each window.
     *
     * @tparam VR Aggregated value type
     * @param initializer Function that provides the initial value for each window
     * @param aggregator Function that combines a new value with the aggregate
     * @return KTable with windowed aggregates
     */
    template<typename VR>
    KTable<Windowed<K, VR>, VR> aggregate(
        std::function<VR()> initializer,
        std::function<VR(const K&, const V&, VR)> aggregator);

    /**
     * Aggregate records in each window with custom materialization.
     *
     * @tparam VR Aggregated value type
     * @param initializer Function that provides the initial value for each window
     * @param aggregator Function that combines a new value with the aggregate
     * @param materialized Configuration for the state store
     * @return KTable with windowed aggregates
     */
    template<typename VR>
    KTable<Windowed<K, VR>, VR> aggregate(
        std::function<VR()> initializer,
        std::function<VR(const K&, const V&, VR)> aggregator,
        const WindowedMaterialized<K, VR>& materialized);

    /**
     * Reduce records in each window using a reducer function.
     *
     * @param reducer Function that combines two values
     * @return KTable with windowed reduced values
     */
    KTable<Windowed<K, V>, V> reduce(
        std::function<V(const V&, const V&)> reducer);

    /**
     * Reduce records in each window with custom materialization.
     *
     * @param reducer Function that combines two values
     * @param materialized Configuration for the state store
     * @return KTable with windowed reduced values
     */
    KTable<Windowed<K, V>, V> reduce(
        std::function<V(const V&, const V&)> reducer,
        const WindowedMaterialized<K, V>& materialized);

    // Internal constructor
    TimeWindowedKStream(StreamsBuilder* builder, const std::string& sourceNode,
                        const TimeWindows& windows)
        : builder_(builder), sourceNode_(sourceNode), windows_(windows) {}

    /**
     * Get the time windows configuration.
     */
    const TimeWindows& windows() const { return windows_; }

private:
    StreamsBuilder* builder_;
    std::string sourceNode_;
    TimeWindows windows_;

    template<typename K2, typename V2> friend class KGroupedStream;
};

/**
 * SessionWindowedKStream represents a grouped stream with session-based windowing.
 *
 * Records are grouped by key AND by session (periods of activity separated
 * by inactivity gaps).
 *
 * @tparam K Key type
 * @tparam V Value type
 */
template<typename K, typename V>
class SessionWindowedKStream {
public:
    /**
     * Count the number of records in each session.
     *
     * @return KTable with session counts
     */
    KTable<Windowed<K, int64_t>, int64_t> count();

    /**
     * Count the number of records in each session.
     *
     * @param materialized Configuration for the state store
     * @return KTable with session counts
     */
    KTable<Windowed<K, int64_t>, int64_t> count(
        const WindowedMaterialized<K, int64_t>& materialized);

    /**
     * Aggregate records in each session.
     *
     * @tparam VR Aggregated value type
     * @param initializer Function that provides the initial value for each session
     * @param aggregator Function that adds a value to the aggregate
     * @param sessionMerger Function that merges two aggregates when sessions merge
     * @return KTable with session aggregates
     */
    template<typename VR>
    KTable<Windowed<K, VR>, VR> aggregate(
        std::function<VR()> initializer,
        std::function<VR(const K&, const V&, VR)> aggregator,
        std::function<VR(const K&, VR, VR)> sessionMerger);

    /**
     * Aggregate records in each session with custom materialization.
     *
     * @tparam VR Aggregated value type
     * @param initializer Function that provides the initial value
     * @param aggregator Function that adds a value to the aggregate
     * @param sessionMerger Function that merges two aggregates when sessions merge
     * @param materialized Configuration for the state store
     * @return KTable with session aggregates
     */
    template<typename VR>
    KTable<Windowed<K, VR>, VR> aggregate(
        std::function<VR()> initializer,
        std::function<VR(const K&, const V&, VR)> aggregator,
        std::function<VR(const K&, VR, VR)> sessionMerger,
        const WindowedMaterialized<K, VR>& materialized);

    /**
     * Reduce records in each session.
     *
     * @param reducer Function that combines two values
     * @return KTable with session reduced values
     */
    KTable<Windowed<K, V>, V> reduce(
        std::function<V(const V&, const V&)> reducer);

    /**
     * Reduce records in each session with custom materialization.
     *
     * @param reducer Function that combines two values
     * @param materialized Configuration for the state store
     * @return KTable with session reduced values
     */
    KTable<Windowed<K, V>, V> reduce(
        std::function<V(const V&, const V&)> reducer,
        const WindowedMaterialized<K, V>& materialized);

    // Internal constructor
    SessionWindowedKStream(StreamsBuilder* builder, const std::string& sourceNode,
                           const SessionWindows& windows)
        : builder_(builder), sourceNode_(sourceNode), windows_(windows) {}

    /**
     * Get the session windows configuration.
     */
    const SessionWindows& windows() const { return windows_; }

private:
    StreamsBuilder* builder_;
    std::string sourceNode_;
    SessionWindows windows_;

    template<typename K2, typename V2> friend class KGroupedStream;
};

} // namespace streams
} // namespace kawasan
