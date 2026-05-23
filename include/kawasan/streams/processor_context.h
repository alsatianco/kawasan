#pragma once

#include "state_store.h"
#include "timestamp_extractor.h"
#include <chrono>
#include <functional>
#include <memory>
#include <string>
#include <vector>
#include <map>
#include <mutex>

namespace kawasan {
namespace streams {

using Duration = std::chrono::milliseconds;

/**
 * Scheduled punctuation entry for tracking pending punctuations.
 */
struct ScheduledPunctuation {
    int64_t scheduled_time_ms;
    Duration interval;
    PunctuationType type;
    std::function<void(int64_t)> punctuator;
    bool cancelled = false;

    ScheduledPunctuation() = default;
    ScheduledPunctuation(int64_t time, Duration intv, PunctuationType t,
                         std::function<void(int64_t)> p)
        : scheduled_time_ms(time), interval(intv), type(t), punctuator(std::move(p)) {}
};

/**
 * Implementation of Cancellable for scheduled punctuations.
 */
class CancellableImpl : public Cancellable {
public:
    explicit CancellableImpl(std::shared_ptr<ScheduledPunctuation> punctuation)
        : punctuation_(punctuation) {}

    void cancel() override {
        if (punctuation_) {
            punctuation_->cancelled = true;
        }
    }

private:
    std::shared_ptr<ScheduledPunctuation> punctuation_;
};

/**
 * ProcessorContext provides access to stream processor context and state.
 *
 * This context is passed to processors during initialization and provides:
 * - Access to registered state stores
 * - Methods to forward records to child processors
 * - Scheduling of punctuations
 * - Access to record metadata
 *
 * This is part of the low-level Processor API for advanced stream processing.
 */
class ProcessorContext {
public:
    ProcessorContext() = default;
    virtual ~ProcessorContext() = default;

    /**
     * Get the application id for this streams application.
     */
    virtual std::string applicationId() const { return application_id_; }

    /**
     * Get the task id for this processor.
     */
    virtual std::string taskId() const { return task_id_; }

    /**
     * Get a state store by name.
     *
     * The state store must have been registered with the topology.
     *
     * @param name Name of the state store
     * @return The state store, or nullptr if not found
     */
    virtual std::shared_ptr<StateStore> getStateStore(const std::string& name) {
        auto it = state_stores_.find(name);
        if (it != state_stores_.end()) {
            return it->second;
        }
        return nullptr;
    }

    /**
     * Get a typed key-value state store.
     *
     * @tparam K Key type
     * @tparam V Value type
     * @param name Name of the state store
     * @return The typed state store, or nullptr if not found or wrong type
     */
    template<typename K, typename V>
    std::shared_ptr<KeyValueStore<K, V>> getKeyValueStore(const std::string& name) {
        auto store = getStateStore(name);
        return std::dynamic_pointer_cast<KeyValueStore<K, V>>(store);
    }

    /**
     * Get a typed window store.
     *
     * @tparam K Key type
     * @tparam V Value type
     * @param name Name of the state store
     * @return The typed window store, or nullptr if not found or wrong type
     */
    template<typename K, typename V>
    std::shared_ptr<WindowStore<K, V>> getWindowStore(const std::string& name) {
        auto store = getStateStore(name);
        return std::dynamic_pointer_cast<WindowStore<K, V>>(store);
    }

    /**
     * Forward a record to all child processors.
     *
     * @tparam K Key type
     * @tparam V Value type
     * @param key Record key
     * @param value Record value
     */
    template<typename K, typename V>
    void forward(const K& key, const V& value) {
        // TODO: Implement forwarding to child processors
        (void)key;
        (void)value;
    }

    /**
     * Forward a record to a specific child processor.
     *
     * @tparam K Key type
     * @tparam V Value type
     * @param key Record key
     * @param value Record value
     * @param childName Name of the child processor to forward to
     */
    template<typename K, typename V>
    void forward(const K& key, const V& value, const std::string& childName) {
        // TODO: Implement forwarding to specific child processor
        (void)key;
        (void)value;
        (void)childName;
    }

    /**
     * Request a commit of the current processing progress.
     *
     * The commit is asynchronous and will happen after the current record
     * has been fully processed.
     */
    virtual void commit() {
        commit_requested_ = true;
    }

    /**
     * Schedule a punctuation to be called periodically.
     *
     * @param interval The interval between punctuations
     * @param type The type of time semantics (STREAM_TIME or WALL_CLOCK_TIME)
     * @param punctuator The callback function to execute
     * @return A Cancellable to cancel the scheduled punctuation
     */
    virtual std::shared_ptr<Cancellable> schedule(
        Duration interval,
        PunctuationType type,
        std::function<void(int64_t)> punctuator) {

        std::lock_guard<std::mutex> lock(mutex_);

        int64_t first_time;
        if (type == PunctuationType::STREAM_TIME) {
            first_time = current_stream_time_ + interval.count();
        } else {
            first_time = currentSystemTimeMs() + interval.count();
        }

        auto scheduled = std::make_shared<ScheduledPunctuation>(
            first_time, interval, type, std::move(punctuator));

        scheduled_punctuations_.push_back(scheduled);

        return std::make_shared<CancellableImpl>(scheduled);
    }

    /**
     * Get the topic of the current record being processed.
     */
    virtual std::string topic() const { return current_topic_; }

    /**
     * Get the partition of the current record being processed.
     */
    virtual int32_t partition() const { return current_partition_; }

    /**
     * Get the offset of the current record being processed.
     */
    virtual int64_t offset() const { return current_offset_; }

    /**
     * Get the timestamp of the current record being processed.
     */
    virtual int64_t timestamp() const { return current_timestamp_; }

    /**
     * Get the current stream time.
     *
     * Stream time is the maximum timestamp observed so far across all
     * partitions being processed by this task.
     */
    virtual int64_t currentStreamTimeMs() const { return current_stream_time_; }

    /**
     * Get the current system/wall clock time.
     */
    virtual int64_t currentSystemTimeMs() const {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()
        ).count();
    }

    // ========================================================================
    // Internal methods for runtime to use
    // ========================================================================

    /**
     * Register a state store with this context.
     */
    void registerStateStore(const std::string& name, std::shared_ptr<StateStore> store) {
        state_stores_[name] = store;
    }

    /**
     * Set the current record metadata.
     */
    void setRecordMetadata(const std::string& topic, int32_t partition,
                           int64_t offset, int64_t timestamp) {
        current_topic_ = topic;
        current_partition_ = partition;
        current_offset_ = offset;
        current_timestamp_ = timestamp;

        // Update stream time if this timestamp is higher
        if (timestamp > current_stream_time_) {
            current_stream_time_ = timestamp;
        }
    }

    /**
     * Clear the current record metadata.
     */
    void clearRecordMetadata() {
        current_topic_.clear();
        current_partition_ = -1;
        current_offset_ = -1;
        current_timestamp_ = -1;
    }

    /**
     * Check and execute any pending punctuations.
     *
     * @param stream_time Current stream time
     * @param wall_clock_time Current wall clock time
     */
    void maybeFirePunctuations(int64_t stream_time, int64_t wall_clock_time) {
        std::lock_guard<std::mutex> lock(mutex_);

        for (auto& scheduled : scheduled_punctuations_) {
            if (scheduled->cancelled) {
                continue;
            }

            int64_t current_time = (scheduled->type == PunctuationType::STREAM_TIME)
                                       ? stream_time
                                       : wall_clock_time;

            while (current_time >= scheduled->scheduled_time_ms) {
                if (scheduled->cancelled) break;

                // Execute the punctuator
                scheduled->punctuator(scheduled->scheduled_time_ms);

                // Schedule next punctuation
                scheduled->scheduled_time_ms += scheduled->interval.count();
            }
        }

        // Remove cancelled punctuations
        scheduled_punctuations_.erase(
            std::remove_if(scheduled_punctuations_.begin(),
                           scheduled_punctuations_.end(),
                           [](const auto& p) { return p->cancelled; }),
            scheduled_punctuations_.end());
    }

    /**
     * Check if a commit has been requested.
     */
    bool isCommitRequested() const { return commit_requested_; }

    /**
     * Clear the commit request flag.
     */
    void clearCommitRequest() { commit_requested_ = false; }

    /**
     * Set the application and task IDs.
     */
    void setIds(const std::string& applicationId, const std::string& taskId) {
        application_id_ = applicationId;
        task_id_ = taskId;
    }

protected:
    std::string application_id_;
    std::string task_id_;

    // Current record metadata
    std::string current_topic_;
    int32_t current_partition_ = -1;
    int64_t current_offset_ = -1;
    int64_t current_timestamp_ = -1;

    // Stream time tracking
    int64_t current_stream_time_ = 0;

    // State stores
    std::map<std::string, std::shared_ptr<StateStore>> state_stores_;

    // Punctuation scheduling
    std::vector<std::shared_ptr<ScheduledPunctuation>> scheduled_punctuations_;
    std::mutex mutex_;

    // Commit tracking
    bool commit_requested_ = false;
};

/**
 * Extended Processor interface with context access.
 *
 * This is the low-level API for stream processing. Processors receive
 * records, can access state stores, and forward records to children.
 *
 * @tparam KIn Input key type
 * @tparam VIn Input value type
 * @tparam KOut Output key type (defaults to KIn)
 * @tparam VOut Output value type (defaults to VIn)
 */
template<typename KIn, typename VIn, typename KOut = KIn, typename VOut = VIn>
class StatefulProcessor {
public:
    virtual ~StatefulProcessor() = default;

    /**
     * Initialize the processor with the context.
     *
     * Called once when the processor is created.
     *
     * @param context The processor context
     */
    virtual void init(ProcessorContext& context) {
        context_ = &context;
    }

    /**
     * Process a single record.
     *
     * @param key Record key
     * @param value Record value
     */
    virtual void process(const KIn& key, const VIn& value) = 0;

    /**
     * Close the processor and release resources.
     *
     * Called when the stream task is shutting down.
     */
    virtual void close() {}

protected:
    /**
     * Get the processor context.
     */
    ProcessorContext* context() { return context_; }

    /**
     * Forward a record to all child processors.
     */
    void forward(const KOut& key, const VOut& value) {
        if (context_) {
            context_->forward(key, value);
        }
    }

    /**
     * Forward a record to a specific child processor.
     */
    void forward(const KOut& key, const VOut& value, const std::string& childName) {
        if (context_) {
            context_->forward(key, value, childName);
        }
    }

    /**
     * Schedule a punctuation.
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

private:
    ProcessorContext* context_ = nullptr;
};

} // namespace streams
} // namespace kawasan
