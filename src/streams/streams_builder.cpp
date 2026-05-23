#include "kawasan/streams/streams_builder.h"
#include "kawasan/streams/processors.h"
#include <sstream>
#include <iomanip>

namespace kawasan {
namespace streams {

StreamsBuilder::StreamsBuilder() = default;
StreamsBuilder::~StreamsBuilder() = default;

template<typename K, typename V>
KStream<K, V> StreamsBuilder::stream(const std::string& topic) {
    return stream<K, V>(std::vector<std::string>{topic});
}

template<typename K, typename V>
KStream<K, V> StreamsBuilder::stream(const std::vector<std::string>& topics) {
    std::string sourceName = generateNodeName("KSTREAM-SOURCE");
    topology_.addSource(sourceName, topics);
    return KStream<K, V>(this, sourceName);
}

template<typename K, typename V>
KTable<K, V> StreamsBuilder::table(const std::string& topic) {
    std::string sourceName = generateNodeName("KTABLE-SOURCE");
    topology_.addSource(sourceName, std::vector<std::string>{topic});
    return KTable<K, V>(this, sourceName);
}

Topology StreamsBuilder::build() {
    // Validate the topology before returning
    if (!topology_.validate()) {
        throw std::runtime_error("Invalid topology: contains cycles or orphaned nodes");
    }
    
    return std::move(topology_);
}

std::string StreamsBuilder::generateNodeName(const std::string& prefix) {
    std::ostringstream oss;
    oss << prefix << "-" << std::setfill('0') << std::setw(10) << nodeCounter_++;
    return oss.str();
}

// KStream implementation

template<typename K, typename V>
KStream<K, V> KStream<K, V>::filter(std::function<bool(const K&, const V&)> predicate) {
    std::string processorName = builder_->generateNodeName("KSTREAM-FILTER");
    
    // Create a processor supplier that wraps the predicate
    auto supplier = std::make_shared<FilterProcessorSupplier<K, V>>(std::move(predicate));
    
    builder_->topology().addProcessor(processorName, supplier, {sourceNode_});
    
    return KStream<K, V>(builder_, processorName);
}

template<typename K, typename V>
KStream<K, V> KStream<K, V>::filterNot(std::function<bool(const K&, const V&)> predicate) {
    return filter([predicate](const K& k, const V& v) {
        return !predicate(k, v);
    });
}

template<typename K, typename V>
template<typename VR>
KStream<K, VR> KStream<K, V>::mapValues(std::function<VR(const V&)> mapper) {
    std::string processorName = builder_->generateNodeName("KSTREAM-MAPVALUES");
    
    // Create a processor supplier that wraps the mapper
    auto supplier = std::make_shared<MapValuesProcessorSupplier<K, V, VR>>(std::move(mapper));
    
    builder_->topology().addProcessor(processorName, supplier, {sourceNode_});
    
    return KStream<K, VR>(builder_, processorName);
}

template<typename K, typename V>
template<typename KR, typename VR>
KStream<KR, VR> KStream<K, V>::map(
    std::function<std::pair<KR, VR>(const K&, const V&)> mapper) {
    std::string processorName = builder_->generateNodeName("KSTREAM-MAP");
    
    auto supplier = std::make_shared<MapProcessorSupplier<K, V, KR, VR>>(std::move(mapper));
    
    builder_->topology().addProcessor(processorName, supplier, {sourceNode_});
    
    return KStream<KR, VR>(builder_, processorName);
}

template<typename K, typename V>
template<typename KR, typename VR>
KStream<KR, VR> KStream<K, V>::flatMap(
    std::function<std::vector<std::pair<KR, VR>>(const K&, const V&)> mapper) {
    std::string processorName = builder_->generateNodeName("KSTREAM-FLATMAP");
    
    auto supplier = std::make_shared<FlatMapProcessorSupplier<K, V, KR, VR>>(std::move(mapper));
    
    builder_->topology().addProcessor(processorName, supplier, {sourceNode_});
    
    return KStream<KR, VR>(builder_, processorName);
}

template<typename K, typename V>
KStream<K, V> KStream<K, V>::peek(std::function<void(const K&, const V&)> action) {
    std::string processorName = builder_->generateNodeName("KSTREAM-PEEK");
    
    auto supplier = std::make_shared<PeekProcessorSupplier<K, V>>(std::move(action));
    
    builder_->topology().addProcessor(processorName, supplier, {sourceNode_});
    
    return KStream<K, V>(builder_, processorName);
}

template<typename K, typename V>
void KStream<K, V>::to(const std::string& topic) {
    std::string sinkName = builder_->generateNodeName("KSTREAM-SINK");
    builder_->topology().addSink(sinkName, topic, {sourceNode_});
}

template<typename K, typename V>
void KStream<K, V>::foreach(std::function<void(const K&, const V&)> action) {
    std::string processorName = builder_->generateNodeName("KSTREAM-FOREACH");
    
    auto supplier = std::make_shared<ForeachProcessorSupplier<K, V>>(std::move(action));
    
    builder_->topology().addProcessor(processorName, supplier, {sourceNode_});
}

template<typename K, typename V>
std::vector<KStream<K, V>> KStream<K, V>::branch(
    std::vector<std::function<bool(const K&, const V&)>> predicates) {
    
    std::vector<KStream<K, V>> branches;
    
    for (size_t i = 0; i < predicates.size(); ++i) {
        std::string processorName = builder_->generateNodeName("KSTREAM-BRANCH-" + std::to_string(i));
        
        auto supplier = std::make_shared<BranchProcessorSupplier<K, V>>(predicates, i);
        
        builder_->topology().addProcessor(processorName, supplier, {sourceNode_});
        
        branches.push_back(KStream<K, V>(builder_, processorName));
    }
    
    return branches;
}

template<typename K, typename V>
KStream<K, V> KStream<K, V>::merge(std::vector<KStream<K, V>> streams) {
    if (streams.empty()) {
        throw std::runtime_error("Cannot merge empty list of streams");
    }
    
    auto* builder = streams[0].builder_;
    std::string processorName = builder->generateNodeName("KSTREAM-MERGE");
    
    std::vector<std::string> parents;
    for (const auto& stream : streams) {
        parents.push_back(stream.sourceNode_);
    }
    
    auto supplier = std::make_shared<MergeProcessorSupplier<K, V>>();
    
    builder->topology().addProcessor(processorName, supplier, parents);
    
    return KStream<K, V>(builder, processorName);
}

// KTable implementation

template<typename K, typename V>
template<typename VR>
KTable<K, VR> KTable<K, V>::mapValues(std::function<VR(const V&)> mapper) {
    std::string processorName = builder_->generateNodeName("KTABLE-MAPVALUES");

    auto supplier = std::make_shared<MapValuesProcessorSupplier<K, V, VR>>(std::move(mapper));

    builder_->topology().addProcessor(processorName, supplier, {sourceNode_});

    return KTable<K, VR>(builder_, processorName);
}

template<typename K, typename V>
KTable<K, V> KTable<K, V>::filter(std::function<bool(const K&, const V&)> predicate) {
    std::string processorName = builder_->generateNodeName("KTABLE-FILTER");

    auto supplier = std::make_shared<FilterProcessorSupplier<K, V>>(std::move(predicate));

    builder_->topology().addProcessor(processorName, supplier, {sourceNode_});

    return KTable<K, V>(builder_, processorName);
}

template<typename K, typename V>
KStream<K, V> KTable<K, V>::toStream() {
    std::string processorName = builder_->generateNodeName("KTABLE-TOSTREAM");

    // Just a passthrough processor
    auto supplier = std::make_shared<MergeProcessorSupplier<K, V>>();

    builder_->topology().addProcessor(processorName, supplier, {sourceNode_});

    return KStream<K, V>(builder_, processorName);
}

// ============================================================================
// KTable Join Implementations
// ============================================================================

template<typename K, typename V>
template<typename V2, typename VR>
KTable<K, VR> KTable<K, V>::join(
    KTable<K, V2> other,
    std::function<VR(const V&, const V2&)> joiner) {

    std::string processorName = builder_->generateNodeName("KTABLE-JOIN");

    // Create table-table join processor
    auto supplier = std::make_shared<TableTableJoinProcessorSupplier<K, V, V2, VR>>(
        std::move(joiner), false);

    // The join processor takes both tables as parents
    builder_->topology().addProcessor(processorName, supplier, {sourceNode_, other.sourceNode()});

    return KTable<K, VR>(builder_, processorName);
}

template<typename K, typename V>
template<typename V2, typename VR>
KTable<K, VR> KTable<K, V>::leftJoin(
    KTable<K, V2> other,
    std::function<VR(const V&, const std::optional<V2>&)> joiner) {

    std::string processorName = builder_->generateNodeName("KTABLE-LEFTJOIN");

    // Wrap the joiner to handle optional
    auto innerJoiner = [joiner](const V& v1, const V2& v2) -> VR {
        return joiner(v1, std::optional<V2>(v2));
    };

    auto supplier = std::make_shared<TableTableJoinProcessorSupplier<K, V, V2, VR>>(
        std::move(innerJoiner), true);

    builder_->topology().addProcessor(processorName, supplier, {sourceNode_, other.sourceNode()});

    return KTable<K, VR>(builder_, processorName);
}

template<typename K, typename V>
template<typename V2, typename VR>
KTable<K, VR> KTable<K, V>::outerJoin(
    KTable<K, V2> other,
    std::function<VR(const std::optional<V>&, const std::optional<V2>&)> joiner) {

    std::string processorName = builder_->generateNodeName("KTABLE-OUTERJOIN");

    // Wrap the joiner to handle non-optional case
    auto innerJoiner = [joiner](const V& v1, const V2& v2) -> VR {
        return joiner(std::optional<V>(v1), std::optional<V2>(v2));
    };

    // For outer join, both sides can trigger output
    auto supplier = std::make_shared<TableTableJoinProcessorSupplier<K, V, V2, VR>>(
        std::move(innerJoiner), false);

    builder_->topology().addProcessor(processorName, supplier, {sourceNode_, other.sourceNode()});

    return KTable<K, VR>(builder_, processorName);
}

template<typename K, typename V>
KGroupedTable<K, V> KTable<K, V>::groupByKey() {
    std::string processorName = builder_->generateNodeName("KTABLE-GROUPBYKEY");

    auto supplier = std::make_shared<PlaceholderProcessorSupplier>();
    builder_->topology().addProcessor(processorName, supplier, {sourceNode_});

    return KGroupedTable<K, V>(builder_, processorName);
}

template<typename K, typename V>
KGroupedStream<K, V> KStream<K, V>::groupByKey() {
    // Create a groupBy processor node
    // In actual implementation, this would repartition data if needed
    std::string processorName = builder_->generateNodeName("GROUPBYKEY");

    // For now, just mark this as a group-by node in the topology
    // The actual grouping happens in the aggregate/reduce/count operations
    auto supplier = std::make_shared<PlaceholderProcessorSupplier>();
    builder_->topology().addProcessor(processorName, supplier, {sourceNode_});

    return KGroupedStream<K, V>(builder_, processorName);
}

template<typename K, typename V>
template<typename KR>
KGroupedStream<KR, V> KStream<K, V>::groupBy(std::function<KR(const K&, const V&)> /*keySelector*/) {
    // Create a rekeying processor followed by grouping
    std::string rekeyProcessorName = builder_->generateNodeName("REKEY");

    // TODO: Create RekeyProcessor that applies keySelector
    auto supplier = std::make_shared<PlaceholderProcessorSupplier>();
    builder_->topology().addProcessor(rekeyProcessorName, supplier, {sourceNode_});

    // Now group by the new key
    std::string groupProcessorName = builder_->generateNodeName("GROUPBY");
    auto groupSupplier = std::make_shared<PlaceholderProcessorSupplier>();
    builder_->topology().addProcessor(groupProcessorName, groupSupplier, {rekeyProcessorName});

    return KGroupedStream<KR, V>(builder_, groupProcessorName);
}

// ============================================================================
// KStream-KStream Join Implementations
// ============================================================================

template<typename K, typename V>
template<typename V2, typename VR>
KStream<K, VR> KStream<K, V>::join(
    KStream<K, V2> other,
    std::function<VR(const V&, const V2&)> joiner,
    const JoinWindows& windows) {

    std::string processorName = builder_->generateNodeName("KSTREAM-JOIN");

    // Create stream-stream join processor
    using JoinType = typename StreamStreamJoinProcessor<K, V, V2, VR>::JoinType;
    auto supplier = std::make_shared<StreamStreamJoinProcessorSupplier<K, V, V2, VR>>(
        std::move(joiner),
        windows.beforeMs(),
        windows.afterMs(),
        JoinType::INNER);

    // The join processor takes both streams as parents
    builder_->topology().addProcessor(processorName, supplier, {sourceNode_, other.sourceNode()});

    return KStream<K, VR>(builder_, processorName);
}

template<typename K, typename V>
template<typename V2, typename VR>
KStream<K, VR> KStream<K, V>::leftJoin(
    KStream<K, V2> other,
    std::function<VR(const V&, const std::optional<V2>&)> joiner,
    const JoinWindows& windows) {

    std::string processorName = builder_->generateNodeName("KSTREAM-LEFTJOIN");

    // Wrap the joiner to handle the inner join case
    auto innerJoiner = [joiner](const V& v1, const V2& v2) -> VR {
        return joiner(v1, std::optional<V2>(v2));
    };

    using JoinType = typename StreamStreamJoinProcessor<K, V, V2, VR>::JoinType;
    auto supplier = std::make_shared<StreamStreamJoinProcessorSupplier<K, V, V2, VR>>(
        std::move(innerJoiner),
        windows.beforeMs(),
        windows.afterMs(),
        JoinType::LEFT);

    builder_->topology().addProcessor(processorName, supplier, {sourceNode_, other.sourceNode()});

    return KStream<K, VR>(builder_, processorName);
}

template<typename K, typename V>
template<typename V2, typename VR>
KStream<K, VR> KStream<K, V>::outerJoin(
    KStream<K, V2> other,
    std::function<VR(const std::optional<V>&, const std::optional<V2>&)> joiner,
    const JoinWindows& windows) {

    std::string processorName = builder_->generateNodeName("KSTREAM-OUTERJOIN");

    // Wrap the joiner to handle the inner join case
    auto innerJoiner = [joiner](const V& v1, const V2& v2) -> VR {
        return joiner(std::optional<V>(v1), std::optional<V2>(v2));
    };

    using JoinType = typename StreamStreamJoinProcessor<K, V, V2, VR>::JoinType;
    auto supplier = std::make_shared<StreamStreamJoinProcessorSupplier<K, V, V2, VR>>(
        std::move(innerJoiner),
        windows.beforeMs(),
        windows.afterMs(),
        JoinType::OUTER);

    builder_->topology().addProcessor(processorName, supplier, {sourceNode_, other.sourceNode()});

    return KStream<K, VR>(builder_, processorName);
}

// ============================================================================
// KStream-KTable Join Implementations
// ============================================================================

template<typename K, typename V>
template<typename V2, typename VR>
KStream<K, VR> KStream<K, V>::join(
    KTable<K, V2> table,
    std::function<VR(const V&, const V2&)> joiner) {

    std::string processorName = builder_->generateNodeName("KSTREAM-KTABLE-JOIN");

    // Create stream-table join processor
    auto supplier = std::make_shared<StreamTableJoinProcessorSupplier<K, V, V2, VR>>(
        std::move(joiner), false);

    // The join processor takes stream and table as parents
    builder_->topology().addProcessor(processorName, supplier, {sourceNode_, table.sourceNode()});

    return KStream<K, VR>(builder_, processorName);
}

template<typename K, typename V>
template<typename V2, typename VR>
KStream<K, VR> KStream<K, V>::leftJoin(
    KTable<K, V2> table,
    std::function<VR(const V&, const std::optional<V2>&)> joiner) {

    std::string processorName = builder_->generateNodeName("KSTREAM-KTABLE-LEFTJOIN");

    // Wrap the joiner to handle optional
    auto innerJoiner = [joiner](const V& v1, const V2& v2) -> VR {
        return joiner(v1, std::optional<V2>(v2));
    };

    auto supplier = std::make_shared<StreamTableJoinProcessorSupplier<K, V, V2, VR>>(
        std::move(innerJoiner), true);

    builder_->topology().addProcessor(processorName, supplier, {sourceNode_, table.sourceNode()});

    return KStream<K, VR>(builder_, processorName);
}

// Explicit template instantiations for common types
template KStream<std::string, std::string> StreamsBuilder::stream<std::string, std::string>(const std::string&);
template KStream<std::string, std::string> StreamsBuilder::stream<std::string, std::string>(const std::vector<std::string>&);
template KStream<std::string, int> StreamsBuilder::stream<std::string, int>(const std::string&);
template KStream<std::string, int64_t> StreamsBuilder::stream<std::string, int64_t>(const std::string&);
template KTable<std::string, std::string> StreamsBuilder::table<std::string, std::string>(const std::string&);
template KTable<std::string, int64_t> StreamsBuilder::table<std::string, int64_t>(const std::string&);

// KStream explicit instantiations (this instantiates all methods including groupByKey)
template class KStream<std::string, std::string>;
template class KStream<std::string, int>;
template class KStream<std::string, int64_t>;
template class KStream<int, std::string>;

// Explicit instantiation of groupBy with custom key types
template KGroupedStream<int, std::string> KStream<std::string, std::string>::groupBy<int>(std::function<int(const std::string&, const std::string&)>);

// KGroupedStream explicit instantiations
template class KGroupedStream<std::string, std::string>;
template class KGroupedStream<std::string, int64_t>;
template class KGroupedStream<int, std::string>;

// KGroupedTable explicit instantiations
template class KGroupedTable<std::string, std::string>;
template class KGroupedTable<std::string, int64_t>;

// KTable explicit instantiations
template class KTable<std::string, std::string>;
template class KTable<std::string, int64_t>;

// KTable method instantiations
template KTable<std::string, std::string> KTable<std::string, std::string>::mapValues<std::string>(
    std::function<std::string(const std::string&)>);

// ============================================================================
// Join method explicit instantiations
// ============================================================================

// KStream-KStream join instantiations
template KStream<std::string, std::string> KStream<std::string, std::string>::join<std::string, std::string>(
    KStream<std::string, std::string>,
    std::function<std::string(const std::string&, const std::string&)>,
    const JoinWindows&);

template KStream<std::string, std::string> KStream<std::string, std::string>::leftJoin<std::string, std::string>(
    KStream<std::string, std::string>,
    std::function<std::string(const std::string&, const std::optional<std::string>&)>,
    const JoinWindows&);

template KStream<std::string, std::string> KStream<std::string, std::string>::outerJoin<std::string, std::string>(
    KStream<std::string, std::string>,
    std::function<std::string(const std::optional<std::string>&, const std::optional<std::string>&)>,
    const JoinWindows&);

// KStream-KTable join instantiations
template KStream<std::string, std::string> KStream<std::string, std::string>::join<std::string, std::string>(
    KTable<std::string, std::string>,
    std::function<std::string(const std::string&, const std::string&)>);

template KStream<std::string, std::string> KStream<std::string, std::string>::leftJoin<std::string, std::string>(
    KTable<std::string, std::string>,
    std::function<std::string(const std::string&, const std::optional<std::string>&)>);

// KTable-KTable join instantiations
template KTable<std::string, std::string> KTable<std::string, std::string>::join<std::string, std::string>(
    KTable<std::string, std::string>,
    std::function<std::string(const std::string&, const std::string&)>);

template KTable<std::string, std::string> KTable<std::string, std::string>::leftJoin<std::string, std::string>(
    KTable<std::string, std::string>,
    std::function<std::string(const std::string&, const std::optional<std::string>&)>);

template KTable<std::string, std::string> KTable<std::string, std::string>::outerJoin<std::string, std::string>(
    KTable<std::string, std::string>,
    std::function<std::string(const std::optional<std::string>&, const std::optional<std::string>&)>);

// ============================================================================
// Additional explicit instantiations for test coverage
// ============================================================================

// KStream mapValues instantiations
template KStream<std::string, std::string> KStream<std::string, std::string>::mapValues<std::string>(
    std::function<std::string(const std::string&)>);

template KStream<std::string, std::string> KStream<std::string, int64_t>::mapValues<std::string>(
    std::function<std::string(const int64_t&)>);

template KStream<std::string, int64_t> KStream<std::string, int64_t>::mapValues<int64_t>(
    std::function<int64_t(const int64_t&)>);

// KStream flatMap instantiations
template KStream<std::string, std::string> KStream<std::string, std::string>::flatMap<std::string, std::string>(
    std::function<std::vector<std::pair<std::string, std::string>>(const std::string&, const std::string&)>);

} // namespace streams
} // namespace kawasan
