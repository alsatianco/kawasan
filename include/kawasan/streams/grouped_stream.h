#pragma once

#include "topology.h"
#include "state_store.h"
#include "stores.h"
#include "windows.h"
#include <functional>
#include <memory>
#include <string>

namespace kawasan {
namespace streams {

// Forward declarations
template<typename K, typename V> class KStream;
template<typename K, typename V> class KTable;
template<typename K, typename V> class TimeWindowedKStream;
template<typename K, typename V> class SessionWindowedKStream;
class StreamsBuilder;

/**
 * KGroupedStream represents a grouped stream of records.
 * 
 * This is an intermediate abstraction used for aggregations.
 * Records are grouped by key before applying aggregation functions.
 * 
 * @tparam K Key type
 * @tparam V Value type
 */
template<typename K, typename V>
class KGroupedStream {
public:
    /**
     * Count the number of records for each key.
     * 
     * @param materialized Materialization configuration for the state store
     * @return KTable with counts
     */
    KTable<K, int64_t> count(const Materialized<K, int64_t>& materialized);
    
    /**
     * Count the number of records for each key (with default materialization).
     * 
     * @return KTable with counts
     */
    KTable<K, int64_t> count();
    
    /**
     * Aggregate records by key.
     * 
     * @tparam VR Aggregated value type
     * @param initializer Function that provides the initial value
     * @param aggregator Function that combines a new value with the aggregate
     * @param materialized Materialization configuration for the state store
     * @return KTable with aggregated values
     */
    template<typename VR>
    KTable<K, VR> aggregate(
        std::function<VR()> initializer,
        std::function<VR(const K&, const V&, VR)> aggregator,
        const Materialized<K, VR>& materialized);
    
    /**
     * Reduce records by key using a binary operator.
     * 
     * @param reducer Function that combines two values
     * @param materialized Materialization configuration for the state store
     * @return KTable with reduced values
     */
    KTable<K, V> reduce(
        std::function<V(const V&, const V&)> reducer,
        const Materialized<K, V>& materialized);
    
    /**
     * Reduce records by key (with default materialization).
     *
     * @param reducer Function that combines two values
     * @return KTable with reduced values
     */
    KTable<K, V> reduce(
        std::function<V(const V&, const V&)> reducer);

    /**
     * Create a time-windowed stream for windowed aggregations.
     *
     * Use this to aggregate records into time-based windows (tumbling or hopping).
     *
     * Example - 5-minute tumbling windows:
     *   stream.groupByKey()
     *         .windowedBy(TimeWindows::of(std::chrono::minutes(5)))
     *         .count();
     *
     * Example - 10-minute hopping windows with 1-minute advance:
     *   stream.groupByKey()
     *         .windowedBy(TimeWindows::of(std::chrono::minutes(10))
     *                                .advanceBy(std::chrono::minutes(1)))
     *         .count();
     *
     * @param windows Time window specification
     * @return TimeWindowedKStream for windowed aggregations
     */
    TimeWindowedKStream<K, V> windowedBy(const TimeWindows& windows);

    /**
     * Create a session-windowed stream for session-based aggregations.
     *
     * Use this to aggregate records into sessions (periods of activity).
     * Sessions are defined by inactivity gaps - if no new records arrive
     * for a key within the gap period, the session closes.
     *
     * Example - 30-second inactivity gap:
     *   stream.groupByKey()
     *         .windowedBy(SessionWindows::with(std::chrono::seconds(30)))
     *         .count();
     *
     * @param windows Session window specification
     * @return SessionWindowedKStream for session-based aggregations
     */
    SessionWindowedKStream<K, V> windowedBy(const SessionWindows& windows);

    // Internal constructor
    KGroupedStream(StreamsBuilder* builder, const std::string& sourceNode)
        : builder_(builder), sourceNode_(sourceNode) {}
    
private:
    StreamsBuilder* builder_;
    std::string sourceNode_;
    
    template<typename K2, typename V2> friend class KStream;
};

/**
 * KGroupedTable represents a grouped table of records.
 * 
 * Similar to KGroupedStream but for KTable aggregations.
 * 
 * @tparam K Key type
 * @tparam V Value type
 */
template<typename K, typename V>
class KGroupedTable {
public:
    /**
     * Count the number of records for each key.
     * 
     * @param materialized Materialization configuration for the state store
     * @return KTable with counts
     */
    KTable<K, int64_t> count(const Materialized<K, int64_t>& materialized);
    
    /**
     * Aggregate records by key.
     * 
     * @tparam VR Aggregated value type
     * @param initializer Function that provides the initial value
     * @param adder Function that adds a value to the aggregate
     * @param subtractor Function that subtracts a value from the aggregate
     * @param materialized Materialization configuration for the state store
     * @return KTable with aggregated values
     */
    template<typename VR>
    KTable<K, VR> aggregate(
        std::function<VR()> initializer,
        std::function<VR(const K&, const V&, VR)> adder,
        std::function<VR(const K&, const V&, VR)> subtractor,
        const Materialized<K, VR>& materialized);
    
    /**
     * Reduce records by key.
     * 
     * @param adder Function that adds two values
     * @param subtractor Function that subtracts one value from another
     * @param materialized Materialization configuration for the state store
     * @return KTable with reduced values
     */
    KTable<K, V> reduce(
        std::function<V(const V&, const V&)> adder,
        std::function<V(const V&, const V&)> subtractor,
        const Materialized<K, V>& materialized);
    
    // Internal constructor
    KGroupedTable(StreamsBuilder* builder, const std::string& sourceNode)
        : builder_(builder), sourceNode_(sourceNode) {}
    
private:
    StreamsBuilder* builder_;
    std::string sourceNode_;
    
    template<typename K2, typename V2> friend class KTable;
};

} // namespace streams
} // namespace kawasan
