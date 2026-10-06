#include "kawasan/broker/coordinator_executor.h"

#include <algorithm>

#include "kawasan/common/logger.h"

namespace kawasan::broker {
CoordinatorExecutor::CoordinatorExecutor(size_t workers, size_t capacity)
    : capacity_(std::max<size_t>(1, capacity)) {
    try {
        for (size_t i = 0; i < std::max<size_t>(1, workers); ++i)
            workers_.emplace_back([this] { run(); });
    } catch (...) {
        seal();
        join();
        throw;
    }
}
CoordinatorExecutor::~CoordinatorExecutor() {
    seal();
    join();
    // Broker explicitly drains responses while transport/storage still exist.
    // Tickets own only State, so an abandoned external sink cannot dangle here.
}
CoordinatorExecutor::Lease::Lease(std::shared_ptr<State> state) : state(std::move(state)) {
    std::lock_guard<std::mutex> lock(this->state->mutex);
    ++this->state->outstanding;
}
CoordinatorExecutor::Lease::~Lease() {
    std::lock_guard<std::mutex> lock(state->mutex);
    --state->outstanding;
    state->changed.notify_all();
}
bool CoordinatorExecutor::submit(Task task) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (stopping_ || !task || queue_.size() >= capacity_)
        return false;
    auto ticket = std::make_shared<Lease>(state_);
    queue_.push_back({std::move(task), std::move(ticket)});
    changed_.notify_one();
    return true;
}
void CoordinatorExecutor::seal() {
    std::lock_guard<std::mutex> lock(mutex_);
    stopping_ = true;
    changed_.notify_all();
}
void CoordinatorExecutor::join() {
    for (auto& worker : workers_)
        if (worker.joinable())
            worker.join();
    workers_.clear();
}
void CoordinatorExecutor::drainResponses() {
    std::unique_lock<std::mutex> lock(state_->mutex);
    state_->changed.wait(lock, [&] { return state_->outstanding == 0; });
}
bool CoordinatorExecutor::stopping() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return stopping_;
}
void CoordinatorExecutor::run() {
    for (;;) {
        Entry entry;
        bool admitted;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            changed_.wait(lock, [&] { return stopping_ || !queue_.empty(); });
            if (queue_.empty())
                return;
            entry = std::move(queue_.front());
            queue_.pop_front();
            admitted = !stopping_;
        }
        try {
            entry.task(admitted, std::move(entry.ticket));
        } catch (const std::exception& ex) {
            Logger::error("Coordinator worker failed: {}", ex.what());
        } catch (...) {
            Logger::error("Coordinator worker failed with an unknown exception");
        }
    }
}
}  // namespace kawasan::broker
