#pragma once

#include "grouped_stream.h"
#include "windowed_stream.h"

// This file contains the template method implementations for KGroupedStream and KGroupedTable.
// It must be included AFTER StreamsBuilder is fully defined to avoid incomplete type errors.

namespace kawasan {
namespace streams {

// Helper class to create placeholder processor suppliers
class PlaceholderProcessorSupplier : public ProcessorSupplier {
public:
    std::shared_ptr<void> get() override {
        return nullptr;  // Placeholder - will be properly implemented in later tasks
    }
};

template<typename K, typename V>
KTable<K, int64_t> KGroupedStream<K, V>::count(const Materialized<K, int64_t>& materialized) {
    // Create an aggregate node that counts records
    auto aggregator = [](const K& /*key*/, const V& /*value*/, int64_t count) -> int64_t {
        return count + 1;
    };
    
    auto initializer = []() -> int64_t {
        return 0;
    };
    
    return aggregate<int64_t>(initializer, aggregator, materialized);
}

template<typename K, typename V>
KTable<K, int64_t> KGroupedStream<K, V>::count() {
    std::string storeName = builder_->generateNodeName("COUNT");
    Materialized<K, int64_t> materialized(storeName);
    return count(materialized);
}

template<typename K, typename V>
template<typename VR>
KTable<K, VR> KGroupedStream<K, V>::aggregate(
    std::function<VR()> initializer,
    std::function<VR(const K&, const V&, VR)> aggregator,
    const Materialized<K, VR>& /*materialized*/) {
    
    // Create aggregate processor node
    std::string nodeName = builder_->generateNodeName("AGGREGATE");
    
    // TODO: Create AggregateProcessor that:
    // 1. Reads current aggregate from state store
    // 2. Applies aggregator function
    // 3. Writes new aggregate to state store
    // 4. Forwards update to downstream
    
    // For now, add a placeholder processor node
    auto processorSupplier = std::make_shared<PlaceholderProcessorSupplier>();
    builder_->topology().addProcessor(nodeName, processorSupplier, std::vector<std::string>{sourceNode_});
    
    // Add state store
    // TODO: Wire up the actual state store from materialized config
    // Capture initializer and aggregator for future use
    (void)initializer;
    (void)aggregator;
    
    return KTable<K, VR>(builder_, nodeName);
}

template<typename K, typename V>
KTable<K, V> KGroupedStream<K, V>::reduce(
    std::function<V(const V&, const V&)> reducer,
    const Materialized<K, V>& materialized) {
    
    // Reduce is aggregate with the reducer as both initializer and aggregator
    auto aggregator = [reducer](const K& /*key*/, const V& value, V aggregate) -> V {
        return reducer(aggregate, value);
    };
    
    // For reduce, we need at least one value, so initializer returns the first value
    // This is handled in the processor implementation
    auto initializer = []() -> V {
        return V(); // Placeholder - actual implementation will use first value
    };
    
    return aggregate<V>(initializer, aggregator, materialized);
}

template<typename K, typename V>
KTable<K, V> KGroupedStream<K, V>::reduce(
    std::function<V(const V&, const V&)> reducer) {
    
    std::string storeName = builder_->generateNodeName("REDUCE");
    Materialized<K, V> materialized(storeName);
    return reduce(reducer, materialized);
}

template<typename K, typename V>
KTable<K, int64_t> KGroupedTable<K, V>::count(const Materialized<K, int64_t>& /*materialized*/) {
    // Similar to KGroupedStream::count but handles updates (add/subtract)
    // TODO: Implement table aggregation
    
    std::string nodeName = builder_->generateNodeName("TABLE_COUNT");
    auto processorSupplier = std::make_shared<PlaceholderProcessorSupplier>();
    builder_->topology().addProcessor(nodeName, processorSupplier, std::vector<std::string>{sourceNode_});
    
    return KTable<K, int64_t>(builder_, nodeName);
}

template<typename K, typename V>
template<typename VR>
KTable<K, VR> KGroupedTable<K, V>::aggregate(
    std::function<VR()> initializer,
    std::function<VR(const K&, const V&, VR)> adder,
    std::function<VR(const K&, const V&, VR)> subtractor,
    const Materialized<K, VR>& /*materialized*/) {
    
    // Create aggregate processor node for table
    std::string nodeName = builder_->generateNodeName("TABLE_AGGREGATE");
    
    // TODO: Create TableAggregateProcessor that handles add and subtract
    auto processorSupplier = std::make_shared<PlaceholderProcessorSupplier>();
    builder_->topology().addProcessor(nodeName, processorSupplier, std::vector<std::string>{sourceNode_});
    
    // Capture parameters for future use
    (void)initializer;
    (void)adder;
    (void)subtractor;
    
    return KTable<K, VR>(builder_, nodeName);
}

template<typename K, typename V>
KTable<K, V> KGroupedTable<K, V>::reduce(
    std::function<V(const V&, const V&)> adder,
    std::function<V(const V&, const V&)> subtractor,
    const Materialized<K, V>& /*materialized*/) {

    // Create reduce processor node for table
    std::string nodeName = builder_->generateNodeName("TABLE_REDUCE");

    // TODO: Create TableReduceProcessor that handles add and subtract
    auto processorSupplier = std::make_shared<PlaceholderProcessorSupplier>();
    builder_->topology().addProcessor(nodeName, processorSupplier, std::vector<std::string>{sourceNode_});

    // Capture parameters for future use
    (void)adder;
    (void)subtractor;

    return KTable<K, V>(builder_, nodeName);
}

// ============================================================================
// KGroupedStream windowed aggregation implementations
// ============================================================================

template<typename K, typename V>
TimeWindowedKStream<K, V> KGroupedStream<K, V>::windowedBy(const TimeWindows& windows) {
    return TimeWindowedKStream<K, V>(builder_, sourceNode_, windows);
}

template<typename K, typename V>
SessionWindowedKStream<K, V> KGroupedStream<K, V>::windowedBy(const SessionWindows& windows) {
    return SessionWindowedKStream<K, V>(builder_, sourceNode_, windows);
}

// ============================================================================
// TimeWindowedKStream implementations
// ============================================================================

template<typename K, typename V>
KTable<Windowed<K, int64_t>, int64_t> TimeWindowedKStream<K, V>::count() {
    std::string storeName = builder_->generateNodeName("TIME_WINDOW_COUNT");
    WindowedMaterialized<K, int64_t> materialized(storeName);
    return count(materialized);
}

template<typename K, typename V>
KTable<Windowed<K, int64_t>, int64_t> TimeWindowedKStream<K, V>::count(
    const WindowedMaterialized<K, int64_t>& /*materialized*/) {

    std::string nodeName = builder_->generateNodeName("TIME_WINDOW_COUNT");
    auto processorSupplier = std::make_shared<PlaceholderProcessorSupplier>();
    builder_->topology().addProcessor(nodeName, processorSupplier, std::vector<std::string>{sourceNode_});

    return KTable<Windowed<K, int64_t>, int64_t>(builder_, nodeName);
}

template<typename K, typename V>
template<typename VR>
KTable<Windowed<K, VR>, VR> TimeWindowedKStream<K, V>::aggregate(
    std::function<VR()> initializer,
    std::function<VR(const K&, const V&, VR)> aggregator) {

    std::string storeName = builder_->generateNodeName("TIME_WINDOW_AGGREGATE");
    WindowedMaterialized<K, VR> materialized(storeName);
    return aggregate(initializer, aggregator, materialized);
}

template<typename K, typename V>
template<typename VR>
KTable<Windowed<K, VR>, VR> TimeWindowedKStream<K, V>::aggregate(
    std::function<VR()> initializer,
    std::function<VR(const K&, const V&, VR)> aggregator,
    const WindowedMaterialized<K, VR>& /*materialized*/) {

    std::string nodeName = builder_->generateNodeName("TIME_WINDOW_AGGREGATE");
    auto processorSupplier = std::make_shared<PlaceholderProcessorSupplier>();
    builder_->topology().addProcessor(nodeName, processorSupplier, std::vector<std::string>{sourceNode_});

    (void)initializer;
    (void)aggregator;

    return KTable<Windowed<K, VR>, VR>(builder_, nodeName);
}

template<typename K, typename V>
KTable<Windowed<K, V>, V> TimeWindowedKStream<K, V>::reduce(
    std::function<V(const V&, const V&)> reducer) {

    std::string storeName = builder_->generateNodeName("TIME_WINDOW_REDUCE");
    WindowedMaterialized<K, V> materialized(storeName);
    return reduce(reducer, materialized);
}

template<typename K, typename V>
KTable<Windowed<K, V>, V> TimeWindowedKStream<K, V>::reduce(
    std::function<V(const V&, const V&)> reducer,
    const WindowedMaterialized<K, V>& /*materialized*/) {

    std::string nodeName = builder_->generateNodeName("TIME_WINDOW_REDUCE");
    auto processorSupplier = std::make_shared<PlaceholderProcessorSupplier>();
    builder_->topology().addProcessor(nodeName, processorSupplier, std::vector<std::string>{sourceNode_});

    (void)reducer;

    return KTable<Windowed<K, V>, V>(builder_, nodeName);
}

// ============================================================================
// SessionWindowedKStream implementations
// ============================================================================

template<typename K, typename V>
KTable<Windowed<K, int64_t>, int64_t> SessionWindowedKStream<K, V>::count() {
    std::string storeName = builder_->generateNodeName("SESSION_WINDOW_COUNT");
    WindowedMaterialized<K, int64_t> materialized(storeName);
    return count(materialized);
}

template<typename K, typename V>
KTable<Windowed<K, int64_t>, int64_t> SessionWindowedKStream<K, V>::count(
    const WindowedMaterialized<K, int64_t>& /*materialized*/) {

    std::string nodeName = builder_->generateNodeName("SESSION_WINDOW_COUNT");
    auto processorSupplier = std::make_shared<PlaceholderProcessorSupplier>();
    builder_->topology().addProcessor(nodeName, processorSupplier, std::vector<std::string>{sourceNode_});

    return KTable<Windowed<K, int64_t>, int64_t>(builder_, nodeName);
}

template<typename K, typename V>
template<typename VR>
KTable<Windowed<K, VR>, VR> SessionWindowedKStream<K, V>::aggregate(
    std::function<VR()> initializer,
    std::function<VR(const K&, const V&, VR)> aggregator,
    std::function<VR(const K&, VR, VR)> sessionMerger) {

    std::string storeName = builder_->generateNodeName("SESSION_WINDOW_AGGREGATE");
    WindowedMaterialized<K, VR> materialized(storeName);
    return aggregate(initializer, aggregator, sessionMerger, materialized);
}

template<typename K, typename V>
template<typename VR>
KTable<Windowed<K, VR>, VR> SessionWindowedKStream<K, V>::aggregate(
    std::function<VR()> initializer,
    std::function<VR(const K&, const V&, VR)> aggregator,
    std::function<VR(const K&, VR, VR)> sessionMerger,
    const WindowedMaterialized<K, VR>& /*materialized*/) {

    std::string nodeName = builder_->generateNodeName("SESSION_WINDOW_AGGREGATE");
    auto processorSupplier = std::make_shared<PlaceholderProcessorSupplier>();
    builder_->topology().addProcessor(nodeName, processorSupplier, std::vector<std::string>{sourceNode_});

    (void)initializer;
    (void)aggregator;
    (void)sessionMerger;

    return KTable<Windowed<K, VR>, VR>(builder_, nodeName);
}

template<typename K, typename V>
KTable<Windowed<K, V>, V> SessionWindowedKStream<K, V>::reduce(
    std::function<V(const V&, const V&)> reducer) {

    std::string storeName = builder_->generateNodeName("SESSION_WINDOW_REDUCE");
    WindowedMaterialized<K, V> materialized(storeName);
    return reduce(reducer, materialized);
}

template<typename K, typename V>
KTable<Windowed<K, V>, V> SessionWindowedKStream<K, V>::reduce(
    std::function<V(const V&, const V&)> reducer,
    const WindowedMaterialized<K, V>& /*materialized*/) {

    std::string nodeName = builder_->generateNodeName("SESSION_WINDOW_REDUCE");
    auto processorSupplier = std::make_shared<PlaceholderProcessorSupplier>();
    builder_->topology().addProcessor(nodeName, processorSupplier, std::vector<std::string>{sourceNode_});

    (void)reducer;

    return KTable<Windowed<K, V>, V>(builder_, nodeName);
}

} // namespace streams
} // kawasan
