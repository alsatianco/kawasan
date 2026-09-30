#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

#include "kawasan/common/types.h"

namespace kawasan::broker {

/// @brief Parks requests that cannot be answered yet (e.g. a long-poll Fetch
/// with nothing to return) OFF the network IO threads, and completes them when
/// a watched topic-partition changes or their deadline passes. Kafka's
/// DelayedOperationPurgatory analog.
///
/// Each operation supplies `try_complete(expired)`: it re-evaluates the request
/// and, if it can answer (or `expired` is true, in which case it MUST answer),
/// sends the response and returns true. It runs on the purgatory's worker
/// threads and is never run concurrently with itself.
class DelayedOperationPurgatory {
public:
    using Clock = std::chrono::steady_clock;
    using TryComplete = std::function<bool(bool expired)>;

    explicit DelayedOperationPurgatory(size_t num_workers = 2);
    ~DelayedOperationPurgatory();

    DelayedOperationPurgatory(const DelayedOperationPurgatory&) = delete;
    DelayedOperationPurgatory& operator=(const DelayedOperationPurgatory&) = delete;

    /// @brief Parks an operation watching `keys` until `deadline`. One retry is
    /// scheduled immediately to close the race with a change that landed
    /// between the caller's own first attempt and registration. Returns false
    /// (and does not take ownership) if the purgatory is stopped.
    bool watch(const std::vector<TopicPartition>& keys, Clock::time_point deadline,
               TryComplete try_complete);

    /// @brief Schedules a retry of every operation watching `key`. Cheap when
    /// nothing is parked (one atomic load).
    void notify(const std::string& topic, PartitionId partition);

    /// @brief Stops the workers and drops all parked operations without
    /// running them (their captured state, e.g. connections, is released).
    void stop();

    size_t pendingCount() const { return pending_.load(std::memory_order_relaxed); }

private:
    struct Operation {
        std::mutex mutex;
        TryComplete try_complete;
        std::vector<TopicPartition> keys;
        bool done = false;
        std::atomic<bool> queued{false};
    };
    using OperationPtr = std::shared_ptr<Operation>;

    struct Task {
        OperationPtr op;
        bool expired;
    };
    struct TimerEntry {
        Clock::time_point deadline;
        OperationPtr op;
        bool operator>(const TimerEntry& other) const { return deadline > other.deadline; }
    };

    void workerLoop();
    void run(const OperationPtr& op, bool expired);
    void enqueueLocked(const OperationPtr& op, bool expired);
    void unwatchLocked(const OperationPtr& op);

    mutable std::mutex mutex_;
    std::condition_variable cv_;
    bool stopped_ = false;
    std::atomic<size_t> pending_{0};
    std::map<TopicPartition, std::vector<OperationPtr>> watchers_;
    std::deque<Task> ready_;
    std::priority_queue<TimerEntry, std::vector<TimerEntry>, std::greater<>> timers_;
    std::vector<std::thread> workers_;
};

}  // namespace kawasan::broker
