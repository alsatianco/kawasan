#pragma once

#include "processor_context.h"
#include "state_store.h"
#include <functional>
#include <memory>
#include <string>

namespace kawasan {
namespace streams {

/**
 * Low-level Processor API
 *
 * The Processor API provides fine-grained control over stream processing.
 * Unlike the DSL (filter, map, etc.), the Processor API allows:
 * - Direct access to ProcessorContext for state stores and forwarding
 * - Scheduling of punctuations for time-based operations
 * - Custom processing logic with full control over output
 *
 * This API is typically used for advanced use cases where the DSL is
 * insufficient.
 *
 * @tparam K Input key type
 * @tparam V Input value type
 */
template<typename K, typename V>
class Processor {
public:
    virtual ~Processor() = default;

    /**
     * Initialize the processor with the given context.
     *
     * Called once when the processor is created. Use this method to:
     * - Store a reference to the context for later use
     * - Get references to state stores
     * - Schedule periodic punctuations
     *
     * @param context The processor context
     */
    virtual void init(ProcessorContext& context) {
        context_ = &context;
    }

    /**
     * Process a single record.
     *
     * This method is called for each record received by this processor.
     * The processor should:
     * - Process the record according to its logic
     * - Forward results to child processors if needed
     * - Update state stores as needed
     *
     * @param key Record key
     * @param value Record value
     */
    virtual void process(const K& key, const V& value) = 0;

    /**
     * Close the processor and release any resources.
     *
     * Called when the stream task is shutting down. Use this method to:
     * - Flush any buffered data
     * - Close any open resources
     * - Perform cleanup
     */
    virtual void close() {}

protected:
    /**
     * Get the processor context.
     *
     * @return Pointer to the context, or nullptr if not initialized
     */
    ProcessorContext* context() const { return context_; }

    /**
     * Forward a record to all child processors.
     *
     * @tparam KOut Output key type
     * @tparam VOut Output value type
     * @param key Output key
     * @param value Output value
     */
    template<typename KOut, typename VOut>
    void forward(const KOut& key, const VOut& value) {
        if (context_) {
            context_->forward(key, value);
        }
    }

    /**
     * Forward a record to a specific child processor.
     *
     * @tparam KOut Output key type
     * @tparam VOut Output value type
     * @param key Output key
     * @param value Output value
     * @param childName Name of the child processor
     */
    template<typename KOut, typename VOut>
    void forward(const KOut& key, const VOut& value, const std::string& childName) {
        if (context_) {
            context_->forward(key, value, childName);
        }
    }

    /**
     * Get a typed key-value state store.
     *
     * @tparam SK Store key type
     * @tparam SV Store value type
     * @param name Store name
     * @return The state store, or nullptr if not found
     */
    template<typename SK, typename SV>
    std::shared_ptr<KeyValueStore<SK, SV>> getStateStore(const std::string& name) {
        if (context_) {
            return context_->getKeyValueStore<SK, SV>(name);
        }
        return nullptr;
    }

    /**
     * Schedule a punctuation callback.
     *
     * @param interval Time interval between punctuations
     * @param type STREAM_TIME or WALL_CLOCK_TIME
     * @param punctuator Callback to execute
     * @return Cancellable to cancel the punctuation
     */
    std::shared_ptr<Cancellable> schedule(
        Duration interval,
        PunctuationType type,
        std::function<void(int64_t)> punctuator) {
        if (context_) {
            return context_->schedule(interval, type, std::move(punctuator));
        }
        return nullptr;
    }

    /**
     * Request a commit of the current processing progress.
     */
    void commit() {
        if (context_) {
            context_->commit();
        }
    }

private:
    ProcessorContext* context_ = nullptr;
};

/**
 * ProcessorSupplier for the low-level Processor API.
 *
 * This supplier creates Processor instances and supports custom processor
 * implementations in the topology.
 *
 * @tparam K Key type
 * @tparam V Value type
 * @tparam P Processor type (must derive from Processor<K, V>)
 */
template<typename K, typename V, typename P>
class TypedProcessorSupplier : public ProcessorSupplier {
    static_assert(std::is_base_of<Processor<K, V>, P>::value,
                  "P must derive from Processor<K, V>");

public:
    TypedProcessorSupplier() = default;

    /**
     * Create with processor factory function.
     *
     * @param factory Function that creates new processor instances
     */
    explicit TypedProcessorSupplier(std::function<std::shared_ptr<P>()> factory)
        : factory_(std::move(factory)) {}

    std::shared_ptr<void> get() override {
        if (factory_) {
            return factory_();
        }
        return std::make_shared<P>();
    }

private:
    std::function<std::shared_ptr<P>()> factory_;
};

/**
 * Lambda-based processor for simple use cases.
 *
 * Allows defining processors using lambda functions instead of
 * creating a full class.
 *
 * @tparam K Key type
 * @tparam V Value type
 */
template<typename K, typename V>
class LambdaProcessor : public Processor<K, V> {
public:
    using ProcessFunction = std::function<void(const K&, const V&, ProcessorContext&)>;
    using InitFunction = std::function<void(ProcessorContext&)>;
    using CloseFunction = std::function<void()>;

    explicit LambdaProcessor(ProcessFunction process_fn)
        : process_fn_(std::move(process_fn)) {}

    LambdaProcessor(ProcessFunction process_fn, InitFunction init_fn)
        : process_fn_(std::move(process_fn))
        , init_fn_(std::move(init_fn)) {}

    LambdaProcessor(ProcessFunction process_fn, InitFunction init_fn, CloseFunction close_fn)
        : process_fn_(std::move(process_fn))
        , init_fn_(std::move(init_fn))
        , close_fn_(std::move(close_fn)) {}

    void init(ProcessorContext& context) override {
        Processor<K, V>::init(context);
        if (init_fn_) {
            init_fn_(context);
        }
    }

    void process(const K& key, const V& value) override {
        if (process_fn_ && this->context()) {
            process_fn_(key, value, *this->context());
        }
    }

    void close() override {
        if (close_fn_) {
            close_fn_();
        }
    }

private:
    ProcessFunction process_fn_;
    InitFunction init_fn_;
    CloseFunction close_fn_;
};

/**
 * Supplier for lambda-based processors.
 *
 * @tparam K Key type
 * @tparam V Value type
 */
template<typename K, typename V>
class LambdaProcessorSupplier : public ProcessorSupplier {
public:
    using ProcessFunction = typename LambdaProcessor<K, V>::ProcessFunction;
    using InitFunction = typename LambdaProcessor<K, V>::InitFunction;
    using CloseFunction = typename LambdaProcessor<K, V>::CloseFunction;

    explicit LambdaProcessorSupplier(ProcessFunction process_fn)
        : process_fn_(std::move(process_fn)) {}

    LambdaProcessorSupplier(ProcessFunction process_fn, InitFunction init_fn)
        : process_fn_(std::move(process_fn))
        , init_fn_(std::move(init_fn)) {}

    LambdaProcessorSupplier(ProcessFunction process_fn, InitFunction init_fn, CloseFunction close_fn)
        : process_fn_(std::move(process_fn))
        , init_fn_(std::move(init_fn))
        , close_fn_(std::move(close_fn)) {}

    std::shared_ptr<void> get() override {
        return std::make_shared<LambdaProcessor<K, V>>(process_fn_, init_fn_, close_fn_);
    }

private:
    ProcessFunction process_fn_;
    InitFunction init_fn_;
    CloseFunction close_fn_;
};

/**
 * Transformer interface for transforming records.
 *
 * A Transformer is like a Processor but with explicit output types.
 * It can transform one input record into zero, one, or multiple output records.
 *
 * @tparam K Input key type
 * @tparam V Input value type
 * @tparam KR Output key type
 * @tparam VR Output value type
 */
template<typename K, typename V, typename KR, typename VR>
class Transformer {
public:
    virtual ~Transformer() = default;

    /**
     * Initialize the transformer.
     *
     * @param context The processor context
     */
    virtual void init(ProcessorContext& context) {
        context_ = &context;
    }

    /**
     * Transform a single record.
     *
     * @param key Input key
     * @param value Input value
     * @return Output key-value pair, or nullopt to filter out the record
     */
    virtual std::optional<std::pair<KR, VR>> transform(const K& key, const V& value) = 0;

    /**
     * Close the transformer.
     */
    virtual void close() {}

protected:
    ProcessorContext* context() const { return context_; }

private:
    ProcessorContext* context_ = nullptr;
};

/**
 * ValueTransformer for transforming only values.
 *
 * @tparam V Input value type
 * @tparam VR Output value type
 */
template<typename V, typename VR>
class ValueTransformer {
public:
    virtual ~ValueTransformer() = default;

    virtual void init(ProcessorContext& context) {
        context_ = &context;
    }

    virtual std::optional<VR> transform(const V& value) = 0;

    virtual void close() {}

protected:
    ProcessorContext* context() const { return context_; }

private:
    ProcessorContext* context_ = nullptr;
};

} // namespace streams
} // namespace kawasan
