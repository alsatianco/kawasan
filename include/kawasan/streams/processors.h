#pragma once

#include "topology.h"
#include <functional>
#include <memory>
#include <vector>
#include <utility>

namespace kawasan {
namespace streams {

/**
 * Base processor interface
 * 
 * Processors are the building blocks of stream processing topologies.
 * Each processor receives records from parent nodes, processes them,
 * and forwards results to child nodes.
 * 
 * @tparam K Key type
 * @tparam V Value type
 */
template<typename K, typename V>
class Processor {
public:
    virtual ~Processor() = default;
    
    /**
     * Process a single record
     * 
     * @param key Record key
     * @param value Record value
     * @param forward Callback to forward results to child nodes
     */
    virtual void process(const K& key, const V& value,
                        std::function<void(const K&, const V&)> forward) = 0;
};

/**
 * Filter processor
 * 
 * Passes through records that match the predicate, discards others.
 */
template<typename K, typename V>
class FilterProcessor : public Processor<K, V> {
public:
    explicit FilterProcessor(std::function<bool(const K&, const V&)> predicate)
        : predicate_(std::move(predicate)) {}
    
    void process(const K& key, const V& value,
                std::function<void(const K&, const V&)> forward) override {
        if (predicate_(key, value)) {
            forward(key, value);
        }
    }
    
private:
    std::function<bool(const K&, const V&)> predicate_;
};

/**
 * MapValues processor
 *
 * Transforms values while preserving keys.
 * Note: Does not inherit from Processor<K,V> because output type differs.
 */
template<typename K, typename V, typename VR>
class MapValuesProcessor {
public:
    explicit MapValuesProcessor(std::function<VR(const V&)> mapper)
        : mapper_(std::move(mapper)) {}

    void process(const K& key, const V& value,
                std::function<void(const K&, const VR&)> forward) {
        VR newValue = mapper_(value);
        forward(key, newValue);
    }

private:
    std::function<VR(const V&)> mapper_;
};

/**
 * Map processor
 *
 * Transforms both keys and values.
 * Note: Does not inherit from Processor<K,V> because output types differ.
 */
template<typename K, typename V, typename KR, typename VR>
class MapProcessor {
public:
    explicit MapProcessor(std::function<std::pair<KR, VR>(const K&, const V&)> mapper)
        : mapper_(std::move(mapper)) {}

    void process(const K& key, const V& value,
                std::function<void(const KR&, const VR&)> forward) {
        auto result = mapper_(key, value);
        forward(result.first, result.second);
    }

private:
    std::function<std::pair<KR, VR>(const K&, const V&)> mapper_;
};

/**
 * FlatMap processor
 *
 * Transforms each record into zero or more output records.
 * Note: Does not inherit from Processor<K,V> because output types differ.
 */
template<typename K, typename V, typename KR, typename VR>
class FlatMapProcessor {
public:
    explicit FlatMapProcessor(
        std::function<std::vector<std::pair<KR, VR>>(const K&, const V&)> mapper)
        : mapper_(std::move(mapper)) {}

    void process(const K& key, const V& value,
                std::function<void(const KR&, const VR&)> forward) {
        auto results = mapper_(key, value);
        for (const auto& result : results) {
            forward(result.first, result.second);
        }
    }

private:
    std::function<std::vector<std::pair<KR, VR>>(const K&, const V&)> mapper_;
};

/**
 * Peek processor
 * 
 * Performs a side effect on each record, then forwards it unchanged.
 */
template<typename K, typename V>
class PeekProcessor : public Processor<K, V> {
public:
    explicit PeekProcessor(std::function<void(const K&, const V&)> action)
        : action_(std::move(action)) {}
    
    void process(const K& key, const V& value,
                std::function<void(const K&, const V&)> forward) override {
        action_(key, value);
        forward(key, value);
    }
    
private:
    std::function<void(const K&, const V&)> action_;
};

/**
 * Foreach processor (terminal)
 * 
 * Performs a side effect on each record without forwarding.
 */
template<typename K, typename V>
class ForeachProcessor : public Processor<K, V> {
public:
    explicit ForeachProcessor(std::function<void(const K&, const V&)> action)
        : action_(std::move(action)) {}
    
    void process(const K& key, const V& value,
                std::function<void(const K&, const V&)> /* forward */) override {
        action_(key, value);
        // Terminal operation - don't forward
    }
    
private:
    std::function<void(const K&, const V&)> action_;
};

/**
 * Branch processor
 * 
 * Routes records to different outputs based on predicates.
 * Each record is tested against predicates in order and sent to
 * the first matching output.
 */
template<typename K, typename V>
class BranchProcessor : public Processor<K, V> {
public:
    explicit BranchProcessor(std::vector<std::function<bool(const K&, const V&)>> predicates,
                           size_t branchIndex)
        : predicates_(std::move(predicates)), branchIndex_(branchIndex) {}
    
    void process(const K& key, const V& value,
                std::function<void(const K&, const V&)> forward) override {
        if (branchIndex_ < predicates_.size() && predicates_[branchIndex_](key, value)) {
            forward(key, value);
        }
    }
    
private:
    std::vector<std::function<bool(const K&, const V&)>> predicates_;
    size_t branchIndex_;
};

/**
 * Merge processor
 * 
 * Simply forwards all incoming records.
 * The topology structure handles the actual merging.
 */
template<typename K, typename V>
class MergeProcessor : public Processor<K, V> {
public:
    MergeProcessor() = default;
    
    void process(const K& key, const V& value,
                std::function<void(const K&, const V&)> forward) override {
        forward(key, value);
    }
};

// ProcessorSupplier implementations

template<typename K, typename V>
class FilterProcessorSupplier : public ProcessorSupplier {
public:
    explicit FilterProcessorSupplier(std::function<bool(const K&, const V&)> predicate)
        : predicate_(std::move(predicate)) {}
    
    std::shared_ptr<void> get() override {
        return std::make_shared<FilterProcessor<K, V>>(predicate_);
    }
    
private:
    std::function<bool(const K&, const V&)> predicate_;
};

template<typename K, typename V, typename VR>
class MapValuesProcessorSupplier : public ProcessorSupplier {
public:
    explicit MapValuesProcessorSupplier(std::function<VR(const V&)> mapper)
        : mapper_(std::move(mapper)) {}
    
    std::shared_ptr<void> get() override {
        return std::make_shared<MapValuesProcessor<K, V, VR>>(mapper_);
    }
    
private:
    std::function<VR(const V&)> mapper_;
};

template<typename K, typename V, typename KR, typename VR>
class MapProcessorSupplier : public ProcessorSupplier {
public:
    explicit MapProcessorSupplier(std::function<std::pair<KR, VR>(const K&, const V&)> mapper)
        : mapper_(std::move(mapper)) {}
    
    std::shared_ptr<void> get() override {
        return std::make_shared<MapProcessor<K, V, KR, VR>>(mapper_);
    }
    
private:
    std::function<std::pair<KR, VR>(const K&, const V&)> mapper_;
};

template<typename K, typename V, typename KR, typename VR>
class FlatMapProcessorSupplier : public ProcessorSupplier {
public:
    explicit FlatMapProcessorSupplier(
        std::function<std::vector<std::pair<KR, VR>>(const K&, const V&)> mapper)
        : mapper_(std::move(mapper)) {}
    
    std::shared_ptr<void> get() override {
        return std::make_shared<FlatMapProcessor<K, V, KR, VR>>(mapper_);
    }
    
private:
    std::function<std::vector<std::pair<KR, VR>>(const K&, const V&)> mapper_;
};

template<typename K, typename V>
class PeekProcessorSupplier : public ProcessorSupplier {
public:
    explicit PeekProcessorSupplier(std::function<void(const K&, const V&)> action)
        : action_(std::move(action)) {}
    
    std::shared_ptr<void> get() override {
        return std::make_shared<PeekProcessor<K, V>>(action_);
    }
    
private:
    std::function<void(const K&, const V&)> action_;
};

template<typename K, typename V>
class ForeachProcessorSupplier : public ProcessorSupplier {
public:
    explicit ForeachProcessorSupplier(std::function<void(const K&, const V&)> action)
        : action_(std::move(action)) {}
    
    std::shared_ptr<void> get() override {
        return std::make_shared<ForeachProcessor<K, V>>(action_);
    }
    
private:
    std::function<void(const K&, const V&)> action_;
};

template<typename K, typename V>
class BranchProcessorSupplier : public ProcessorSupplier {
public:
    explicit BranchProcessorSupplier(std::vector<std::function<bool(const K&, const V&)>> predicates,
                                    size_t branchIndex)
        : predicates_(std::move(predicates)), branchIndex_(branchIndex) {}
    
    std::shared_ptr<void> get() override {
        return std::make_shared<BranchProcessor<K, V>>(predicates_, branchIndex_);
    }
    
private:
    std::vector<std::function<bool(const K&, const V&)>> predicates_;
    size_t branchIndex_;
};

template<typename K, typename V>
class MergeProcessorSupplier : public ProcessorSupplier {
public:
    MergeProcessorSupplier() = default;

    std::shared_ptr<void> get() override {
        return std::make_shared<MergeProcessor<K, V>>();
    }
};

// ============================================================================
// Join Processors
// ============================================================================

/**
 * Base class for join processors that maintains a state store.
 */
template<typename K, typename V>
class StatefulProcessor : public Processor<K, V> {
public:
    virtual ~StatefulProcessor() = default;
};

/**
 * TableTableJoinProcessor
 *
 * Joins two KTables by key. When a record arrives from either side,
 * looks up the other table and emits a joined record if found.
 */
/**
 * TableTableJoinProcessor
 *
 * Note: Does not inherit from Processor because output type differs.
 */
template<typename K, typename V1, typename V2, typename VR>
class TableTableJoinProcessor {
public:
    using JoinFunction = std::function<VR(const V1&, const V2&)>;

    explicit TableTableJoinProcessor(
        JoinFunction joiner,
        bool left_join = false)
        : joiner_(std::move(joiner))
        , left_join_(left_join) {}

    void process(const K& key, const V1& value,
                std::function<void(const K&, const VR&)> forward) {
        // In a real implementation, we would look up key in the other table's store
        // For now, we emit a placeholder result
        // The actual store lookup will be wired in the runtime
        (void)key;
        (void)value;
        (void)forward;
        (void)left_join_;
        // Placeholder - actual implementation requires ProcessorContext
    }

    // Store the other table's state (will be set by runtime)
    void setOtherStore(std::shared_ptr<void> store) {
        other_store_ = store;
    }

private:
    JoinFunction joiner_;
    bool left_join_;
    std::shared_ptr<void> other_store_;
};

/**
 * StreamStreamJoinProcessor
 *
 * Joins two streams within a time window. Records from each side
 * are stored in a window store and matched against records from
 * the other side that fall within the join window.
 */
/**
 * StreamStreamJoinProcessor
 *
 * Note: Does not inherit from Processor because output type differs.
 */
template<typename K, typename V1, typename V2, typename VR>
class StreamStreamJoinProcessor {
public:
    using JoinFunction = std::function<VR(const V1&, const V2&)>;

    enum class JoinType { INNER, LEFT, OUTER };

    explicit StreamStreamJoinProcessor(
        JoinFunction joiner,
        int64_t before_ms,
        int64_t after_ms,
        JoinType join_type = JoinType::INNER)
        : joiner_(std::move(joiner))
        , before_ms_(before_ms)
        , after_ms_(after_ms)
        , join_type_(join_type) {}

    void process(const K& key, const V1& value,
                std::function<void(const K&, const VR&)> forward) {
        // Store the record in our window store
        // Look up matching records from the other side's window store
        // For each match within the time window, emit a joined record
        (void)key;
        (void)value;
        (void)forward;
        // Placeholder - actual implementation requires WindowStore and ProcessorContext
    }

    void setOtherStore(std::shared_ptr<void> store) {
        other_store_ = store;
    }

    void setThisStore(std::shared_ptr<void> store) {
        this_store_ = store;
    }

private:
    JoinFunction joiner_;
    int64_t before_ms_;
    int64_t after_ms_;
    JoinType join_type_;
    std::shared_ptr<void> other_store_;
    std::shared_ptr<void> this_store_;
};

/**
 * StreamTableJoinProcessor
 *
 * Joins a stream with a table. Each stream record looks up the
 * table for a matching key and emits a joined record.
 */
/**
 * StreamTableJoinProcessor
 *
 * Note: Does not inherit from Processor because output type differs.
 */
template<typename K, typename V1, typename V2, typename VR>
class StreamTableJoinProcessor {
public:
    using JoinFunction = std::function<VR(const V1&, const V2&)>;

    explicit StreamTableJoinProcessor(
        JoinFunction joiner,
        bool left_join = false)
        : joiner_(std::move(joiner))
        , left_join_(left_join) {}

    void process(const K& key, const V1& value,
                std::function<void(const K&, const VR&)> forward) {
        // Look up key in the table's state store
        // If found, emit joined record
        // If not found and left join, emit record with null/default value
        (void)key;
        (void)value;
        (void)forward;
        // Placeholder - actual implementation requires ProcessorContext
    }

    void setTableStore(std::shared_ptr<void> store) {
        table_store_ = store;
    }

private:
    JoinFunction joiner_;
    bool left_join_;
    std::shared_ptr<void> table_store_;
};

// ============================================================================
// Join Processor Suppliers
// ============================================================================

template<typename K, typename V1, typename V2, typename VR>
class TableTableJoinProcessorSupplier : public ProcessorSupplier {
public:
    using JoinFunction = std::function<VR(const V1&, const V2&)>;

    explicit TableTableJoinProcessorSupplier(JoinFunction joiner, bool left_join = false)
        : joiner_(std::move(joiner)), left_join_(left_join) {}

    std::shared_ptr<void> get() override {
        return std::make_shared<TableTableJoinProcessor<K, V1, V2, VR>>(joiner_, left_join_);
    }

private:
    JoinFunction joiner_;
    bool left_join_;
};

template<typename K, typename V1, typename V2, typename VR>
class StreamStreamJoinProcessorSupplier : public ProcessorSupplier {
public:
    using JoinFunction = std::function<VR(const V1&, const V2&)>;
    using JoinType = typename StreamStreamJoinProcessor<K, V1, V2, VR>::JoinType;

    explicit StreamStreamJoinProcessorSupplier(
        JoinFunction joiner,
        int64_t before_ms,
        int64_t after_ms,
        JoinType join_type = JoinType::INNER)
        : joiner_(std::move(joiner))
        , before_ms_(before_ms)
        , after_ms_(after_ms)
        , join_type_(join_type) {}

    std::shared_ptr<void> get() override {
        return std::make_shared<StreamStreamJoinProcessor<K, V1, V2, VR>>(
            joiner_, before_ms_, after_ms_, join_type_);
    }

private:
    JoinFunction joiner_;
    int64_t before_ms_;
    int64_t after_ms_;
    JoinType join_type_;
};

template<typename K, typename V1, typename V2, typename VR>
class StreamTableJoinProcessorSupplier : public ProcessorSupplier {
public:
    using JoinFunction = std::function<VR(const V1&, const V2&)>;

    explicit StreamTableJoinProcessorSupplier(JoinFunction joiner, bool left_join = false)
        : joiner_(std::move(joiner)), left_join_(left_join) {}

    std::shared_ptr<void> get() override {
        return std::make_shared<StreamTableJoinProcessor<K, V1, V2, VR>>(joiner_, left_join_);
    }

private:
    JoinFunction joiner_;
    bool left_join_;
};

} // namespace streams
} // namespace kawasan
