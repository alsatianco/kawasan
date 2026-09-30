#include "kawasan/broker/delayed_operation_purgatory.h"

#include <algorithm>

#include "kawasan/common/logger.h"

namespace kawasan::broker {

DelayedOperationPurgatory::DelayedOperationPurgatory(size_t num_workers) {
    const size_t n = std::max<size_t>(1, num_workers);
    workers_.reserve(n);
    for (size_t i = 0; i < n; ++i) {
        workers_.emplace_back([this] { workerLoop(); });
    }
}

DelayedOperationPurgatory::~DelayedOperationPurgatory() {
    stop();
}

bool DelayedOperationPurgatory::watch(const std::vector<TopicPartition>& keys,
                                      Clock::time_point deadline, TryComplete try_complete) {
    auto op = std::make_shared<Operation>();
    op->try_complete = std::move(try_complete);
    op->keys = keys;

    std::lock_guard<std::mutex> lock(mutex_);
    if (stopped_) {
        return false;
    }
    for (const auto& key : op->keys) {
        watchers_[key].push_back(op);
    }
    timers_.push(TimerEntry{deadline, op});
    pending_.fetch_add(1, std::memory_order_relaxed);
    enqueueLocked(op, /*expired=*/false);
    cv_.notify_all();  // the new deadline may be earlier than what workers wait on
    return true;
}

void DelayedOperationPurgatory::notify(const std::string& topic, PartitionId partition) {
    if (pending_.load(std::memory_order_relaxed) == 0) {
        return;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    if (stopped_) {
        return;
    }
    auto it = watchers_.find(TopicPartition{topic, partition});
    if (it == watchers_.end()) {
        return;
    }
    for (const auto& op : it->second) {
        enqueueLocked(op, /*expired=*/false);
    }
    cv_.notify_all();
}

void DelayedOperationPurgatory::stop() {
    std::map<TopicPartition, std::vector<OperationPtr>> watchers;
    std::deque<Task> ready;
    std::priority_queue<TimerEntry, std::vector<TimerEntry>, std::greater<>> timers;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (stopped_) {
            return;
        }
        stopped_ = true;
        watchers.swap(watchers_);
        ready.swap(ready_);
        timers.swap(timers_);
        pending_.store(0, std::memory_order_relaxed);
    }
    cv_.notify_all();
    for (auto& worker : workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }
    // Parked operations are released here, after every worker has exited.
}

void DelayedOperationPurgatory::enqueueLocked(const OperationPtr& op, bool expired) {
    if (expired) {
        ready_.push_back(Task{op, true});
        return;
    }
    // Coalesce: a retry already queued will observe this change too.
    if (!op->queued.exchange(true)) {
        ready_.push_back(Task{op, false});
    }
}

void DelayedOperationPurgatory::unwatchLocked(const OperationPtr& op) {
    for (const auto& key : op->keys) {
        auto it = watchers_.find(key);
        if (it == watchers_.end()) {
            continue;
        }
        auto& list = it->second;
        list.erase(std::remove(list.begin(), list.end(), op), list.end());
        if (list.empty()) {
            watchers_.erase(it);
        }
    }
    if (!stopped_) {
        pending_.fetch_sub(1, std::memory_order_relaxed);
    }
}

void DelayedOperationPurgatory::workerLoop() {
    std::unique_lock<std::mutex> lock(mutex_);
    while (!stopped_) {
        const auto now = Clock::now();
        while (!timers_.empty() && timers_.top().deadline <= now) {
            ready_.push_back(Task{timers_.top().op, true});
            timers_.pop();
        }
        if (!ready_.empty()) {
            Task task = std::move(ready_.front());
            ready_.pop_front();
            lock.unlock();
            run(task.op, task.expired);
            lock.lock();
            continue;
        }
        if (timers_.empty()) {
            cv_.wait(lock);
        } else {
            cv_.wait_until(lock, timers_.top().deadline);
        }
    }
}

void DelayedOperationPurgatory::run(const OperationPtr& op, bool expired) {
    std::lock_guard<std::mutex> op_lock(op->mutex);
    if (op->done) {
        return;
    }
    if (!expired) {
        // Cleared before evaluating, so a change that lands mid-evaluation
        // re-queues another retry instead of being lost.
        op->queued.store(false);
    }
    bool completed = false;
    try {
        completed = op->try_complete(expired);
    } catch (const std::exception& ex) {
        Logger::error("Delayed operation failed: {}", ex.what());
    } catch (...) {
        Logger::error("Delayed operation failed with unknown exception");
    }
    if (!completed && !expired) {
        return;
    }
    op->done = true;
    op->try_complete = nullptr;  // release captured request/connection state now
    std::lock_guard<std::mutex> lock(mutex_);
    unwatchLocked(op);
}

}  // namespace kawasan::broker
