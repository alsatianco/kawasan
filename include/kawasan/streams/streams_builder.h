#pragma once

#include "topology.h"
#include "grouped_stream.h"
#include "windows.h"
#include <memory>
#include <string>
#include <functional>
#include <optional>

namespace kawasan {
namespace streams {

// Forward declarations
template<typename K, typename V> class KStream;
template<typename K, typename V> class KTable;

/**
 * StreamsBuilder provides a high-level DSL for building stream processing topologies.
 * 
 * Example usage:
 * 
 *   StreamsBuilder builder;
 *   auto stream = builder.stream<std::string, std::string>("input-topic");
 *   stream.filter([](auto k, auto v) { return v.length() > 5; })
 *         .mapValues([](auto v) { return std::toupper(v[0]) + v.substr(1); })
 *         .to("output-topic");
 *   
 *   Topology topology = builder.build();
 */
class StreamsBuilder {
public:
    StreamsBuilder();
    ~StreamsBuilder();
    
    /**
     * Create a KStream from a Kafka topic
     * 
     * @tparam K Key type
     * @tparam V Value type
     * @param topic Topic name to read from
     * @return KStream that reads from the topic
     */
    template<typename K, typename V>
    KStream<K, V> stream(const std::string& topic);
    
    /**
     * Create a KStream from multiple Kafka topics
     * 
     * @tparam K Key type
     * @tparam V Value type
     * @param topics List of topic names to read from
     * @return KStream that reads from the topics
     */
    template<typename K, typename V>
    KStream<K, V> stream(const std::vector<std::string>& topics);
    
    /**
     * Create a KTable from a Kafka topic
     * 
     * The topic should be a compacted topic where each key represents
     * the latest state for that key.
     * 
     * @tparam K Key type
     * @tparam V Value type
     * @param topic Topic name to read from
     * @return KTable that represents the materialized view
     */
    template<typename K, typename V>
    KTable<K, V> table(const std::string& topic);
    
    /**
     * Build the topology
     * 
     * After calling this method, the StreamsBuilder should not be used
     * to add more operations.
     * 
     * @return The constructed topology
     */
    Topology build();
    
    // Internal: Get the underlying topology (for KStream/KTable to modify)
    Topology& topology() { return topology_; }
    
    // Internal: Generate a unique node name
    std::string generateNodeName(const std::string& prefix);
    
private:
    Topology topology_;
    int nodeCounter_ = 0;
};

/**
 * KStream represents an unbounded sequence of records.
 * 
 * Each record is an independent event with a key and value.
 * Operations on KStream produce new KStream instances (immutable).
 * 
 * @tparam K Key type
 * @tparam V Value type
 */
template<typename K, typename V>
class KStream {
public:
    /**
     * Filter records based on a predicate
     * 
     * @param predicate Function that returns true to keep the record
     * @return New KStream with filtered records
     */
    KStream<K, V> filter(std::function<bool(const K&, const V&)> predicate);
    
    /**
     * Filter records based on a predicate (inverse)
     * 
     * @param predicate Function that returns true to discard the record
     * @return New KStream with filtered records
     */
    KStream<K, V> filterNot(std::function<bool(const K&, const V&)> predicate);
    
    /**
     * Transform values (keys unchanged)
     * 
     * @tparam VR Result value type
     * @param mapper Function that transforms the value
     * @return New KStream with transformed values
     */
    template<typename VR>
    KStream<K, VR> mapValues(std::function<VR(const V&)> mapper);
    
    /**
     * Transform keys and values
     * 
     * @tparam KR Result key type
     * @tparam VR Result value type
     * @param mapper Function that transforms key and value
     * @return New KStream with transformed records
     */
    template<typename KR, typename VR>
    KStream<KR, VR> map(std::function<std::pair<KR, VR>(const K&, const V&)> mapper);
    
    /**
     * Transform each record into zero or more records
     * 
     * @tparam KR Result key type
     * @tparam VR Result value type
     * @param mapper Function that returns a list of key-value pairs
     * @return New KStream with flattened results
     */
    template<typename KR, typename VR>
    KStream<KR, VR> flatMap(
        std::function<std::vector<std::pair<KR, VR>>(const K&, const V&)> mapper);
    
    /**
     * Perform a stateless operation on each record (side effect)
     * 
     * This is useful for logging, metrics, or other side effects.
     * The records pass through unchanged.
     * 
     * @param action Function to execute for each record
     * @return This KStream (for chaining)
     */
    KStream<K, V> peek(std::function<void(const K&, const V&)> action);
    
    /**
     * Write records to a Kafka topic (terminal operation)
     * 
     * @param topic Topic name to write to
     */
    void to(const std::string& topic);
    
    /**
     * Apply an action to each record (terminal operation)
     * 
     * This consumes the stream without writing to a topic.
     * 
     * @param action Function to execute for each record
     */
    void foreach(std::function<void(const K&, const V&)> action);
    
    /**
     * Branch the stream into multiple streams based on predicates
     * 
     * Each record is tested against predicates in order.
     * The record goes to the first matching predicate's stream.
     * Records that don't match any predicate are discarded.
     * 
     * @param predicates List of predicate functions
     * @return List of KStreams, one per predicate
     */
    std::vector<KStream<K, V>> branch(
        std::vector<std::function<bool(const K&, const V&)>> predicates);
    
    /**
     * Merge multiple streams into one
     * 
     * @param streams List of streams to merge
     * @return New KStream with merged records
     */
    static KStream<K, V> merge(std::vector<KStream<K, V>> streams);
    
    /**
     * Group records by their existing key.
     * 
     * This is the first step for stateful aggregations.
     * Records must have the same key to be grouped together.
     * 
     * @return KGroupedStream for aggregation operations
     */
    KGroupedStream<K, V> groupByKey();
    
    /**
     * Group records by a new key extracted from each record.
     *
     * @tparam KR Result key type
     * @param keySelector Function that extracts the new key
     * @return KGroupedStream with rekeyed records
     */
    template<typename KR>
    KGroupedStream<KR, V> groupBy(std::function<KR(const K&, const V&)> keySelector);

    // =========================================================================
    // Stream-Stream Joins
    // =========================================================================

    /**
     * Join this stream with another stream (inner join).
     *
     * Records from both streams are joined within the specified time window.
     * Only pairs where both sides have a matching key are emitted.
     *
     * @tparam V2 Value type of the other stream
     * @tparam VR Result value type
     * @param other The other stream to join with
     * @param joiner Function that combines values from both streams
     * @param windows Join window configuration
     * @return New KStream with joined records
     */
    template<typename V2, typename VR>
    KStream<K, VR> join(
        KStream<K, V2> other,
        std::function<VR(const V&, const V2&)> joiner,
        const JoinWindows& windows);

    /**
     * Left join this stream with another stream.
     *
     * All records from this stream are included. If a matching record
     * from the other stream is found within the window, it's joined.
     * Otherwise, the other value is std::nullopt.
     *
     * @tparam V2 Value type of the other stream
     * @tparam VR Result value type
     * @param other The other stream to join with
     * @param joiner Function that combines values (other may be nullopt)
     * @param windows Join window configuration
     * @return New KStream with joined records
     */
    template<typename V2, typename VR>
    KStream<K, VR> leftJoin(
        KStream<K, V2> other,
        std::function<VR(const V&, const std::optional<V2>&)> joiner,
        const JoinWindows& windows);

    /**
     * Outer join this stream with another stream.
     *
     * All records from both streams are included. Either value may be
     * std::nullopt if no matching record is found within the window.
     *
     * @tparam V2 Value type of the other stream
     * @tparam VR Result value type
     * @param other The other stream to join with
     * @param joiner Function that combines values (either may be nullopt)
     * @param windows Join window configuration
     * @return New KStream with joined records
     */
    template<typename V2, typename VR>
    KStream<K, VR> outerJoin(
        KStream<K, V2> other,
        std::function<VR(const std::optional<V>&, const std::optional<V2>&)> joiner,
        const JoinWindows& windows);

    // =========================================================================
    // Stream-Table Joins
    // =========================================================================

    /**
     * Join this stream with a table (inner join).
     *
     * Each stream record looks up the table for a matching key.
     * Only stream records with a matching table entry are emitted.
     *
     * @tparam V2 Value type of the table
     * @tparam VR Result value type
     * @param table The table to join with
     * @param joiner Function that combines stream value with table value
     * @return New KStream with joined records
     */
    template<typename V2, typename VR>
    KStream<K, VR> join(
        KTable<K, V2> table,
        std::function<VR(const V&, const V2&)> joiner);

    /**
     * Left join this stream with a table.
     *
     * Each stream record looks up the table for a matching key.
     * All stream records are emitted; table value is std::nullopt if not found.
     *
     * @tparam V2 Value type of the table
     * @tparam VR Result value type
     * @param table The table to join with
     * @param joiner Function that combines stream value with table value (may be nullopt)
     * @return New KStream with joined records
     */
    template<typename V2, typename VR>
    KStream<K, VR> leftJoin(
        KTable<K, V2> table,
        std::function<VR(const V&, const std::optional<V2>&)> joiner);

    // Internal constructor
    KStream(StreamsBuilder* builder, const std::string& sourceNode)
        : builder_(builder), sourceNode_(sourceNode) {}

    // Internal: get source node name (for joins)
    std::string sourceNode() const { return sourceNode_; }

private:
    StreamsBuilder* builder_;
    std::string sourceNode_;  // The node that produces records for this stream

    template<typename K2, typename V2> friend class KStream;
    template<typename K2, typename V2> friend class KTable;
};

/**
 * KTable represents a changelog stream where each key has at most one value.
 *
 * Updates to the same key overwrite previous values.
 * Null values are tombstones (deletions).
 *
 * @tparam K Key type
 * @tparam V Value type
 */
template<typename K, typename V>
class KTable {
public:
    /**
     * Transform values (keys unchanged)
     *
     * @tparam VR Result value type
     * @param mapper Function that transforms the value
     * @return New KTable with transformed values
     */
    template<typename VR>
    KTable<K, VR> mapValues(std::function<VR(const V&)> mapper);

    /**
     * Filter records based on a predicate
     *
     * @param predicate Function that returns true to keep the record
     * @return New KTable with filtered records
     */
    KTable<K, V> filter(std::function<bool(const K&, const V&)> predicate);

    /**
     * Convert this KTable to a KStream
     *
     * @return KStream that emits all updates (including tombstones)
     */
    KStream<K, V> toStream();

    // =========================================================================
    // Table-Table Joins
    // =========================================================================

    /**
     * Join this table with another table (inner join).
     *
     * Records are joined by key. When either table is updated,
     * the join is recomputed for that key.
     *
     * @tparam V2 Value type of the other table
     * @tparam VR Result value type
     * @param other The other table to join with
     * @param joiner Function that combines values from both tables
     * @return New KTable with joined records
     */
    template<typename V2, typename VR>
    KTable<K, VR> join(
        KTable<K, V2> other,
        std::function<VR(const V&, const V2&)> joiner);

    /**
     * Left join this table with another table.
     *
     * All keys from this table are included. When a matching key
     * exists in the other table, values are joined.
     * Otherwise, the other value is std::nullopt.
     *
     * @tparam V2 Value type of the other table
     * @tparam VR Result value type
     * @param other The other table to join with
     * @param joiner Function that combines values (other may be nullopt)
     * @return New KTable with joined records
     */
    template<typename V2, typename VR>
    KTable<K, VR> leftJoin(
        KTable<K, V2> other,
        std::function<VR(const V&, const std::optional<V2>&)> joiner);

    /**
     * Outer join this table with another table.
     *
     * All keys from both tables are included. Either value may be
     * std::nullopt if no matching key exists in that table.
     *
     * @tparam V2 Value type of the other table
     * @tparam VR Result value type
     * @param other The other table to join with
     * @param joiner Function that combines values (either may be nullopt)
     * @return New KTable with joined records
     */
    template<typename V2, typename VR>
    KTable<K, VR> outerJoin(
        KTable<K, V2> other,
        std::function<VR(const std::optional<V>&, const std::optional<V2>&)> joiner);

    /**
     * Group records by key (for re-aggregation).
     *
     * @return KGroupedTable for aggregation operations
     */
    KGroupedTable<K, V> groupByKey();

    // Internal constructor
    KTable(StreamsBuilder* builder, const std::string& sourceNode)
        : builder_(builder), sourceNode_(sourceNode) {}

    // Internal: get source node name (for joins)
    std::string sourceNode() const { return sourceNode_; }

private:
    StreamsBuilder* builder_;
    std::string sourceNode_;

    template<typename K2, typename V2> friend class KStream;
    template<typename K2, typename V2> friend class KTable;
};

} // namespace streams
} // namespace kawasan

// Include template implementations that depend on StreamsBuilder being fully defined
#include "grouped_stream_impl.h"
