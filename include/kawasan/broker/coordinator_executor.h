#pragma once

#include <condition_variable>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

namespace kawasan::broker {

// Bounded coordinator work queue. A ticket follows a request from queue admission
// through the transport's final write/cancellation, independently of worker exit.
class CoordinatorExecutor {
public:
    using Ticket = std::shared_ptr<void>;
    using Task = std::function<void(bool admitted, Ticket)>;
    explicit CoordinatorExecutor(size_t workers = 2, size_t capacity = 1024);
    ~CoordinatorExecutor();
    CoordinatorExecutor(const CoordinatorExecutor&) = delete;
    CoordinatorExecutor& operator=(const CoordinatorExecutor&) = delete;
    bool submit(Task task);
    void seal();
    void join();
    void drainResponses();
    bool stopping() const;

private:
    struct State {
        std::mutex mutex;
        std::condition_variable changed;
        size_t outstanding = 0;
    };
    struct Lease {
        explicit Lease(std::shared_ptr<State> state);
        ~Lease();
        std::shared_ptr<State> state;
    };
    struct Entry {
        Task task;
        Ticket ticket;
    };
    void run();
    size_t capacity_;
    mutable std::mutex mutex_;
    std::condition_variable changed_;
    bool stopping_ = false;
    std::deque<Entry> queue_;
    std::vector<std::thread> workers_;
    std::shared_ptr<State> state_ = std::make_shared<State>();
};
}  // namespace kawasan::broker
