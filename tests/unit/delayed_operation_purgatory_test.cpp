#include "kawasan/broker/delayed_operation_purgatory.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>

using kawasan::TopicPartition;
using kawasan::broker::DelayedOperationPurgatory;
using namespace std::chrono_literals;

namespace {

template <typename Pred>
bool waitUntil(Pred pred, std::chrono::milliseconds timeout = 2000ms) {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) {
            return true;
        }
        std::this_thread::sleep_for(1ms);
    }
    return pred();
}

}  // namespace

TEST(DelayedOperationPurgatoryTest, NotifyCompletesWatchingOperation) {
    DelayedOperationPurgatory purgatory(2);
    std::atomic<bool> ready{false};
    std::atomic<int> completions{0};
    std::atomic<bool> completed_expired{true};

    ASSERT_TRUE(purgatory.watch({TopicPartition{"t", 0}},
                                DelayedOperationPurgatory::Clock::now() + 30s,
                                [&](bool expired) {
                                    if (!ready.load() && !expired) {
                                        return false;
                                    }
                                    completed_expired = expired;
                                    ++completions;
                                    return true;
                                }));
    // The initial race-closing retry must not complete it.
    std::this_thread::sleep_for(20ms);
    EXPECT_EQ(completions.load(), 0);
    EXPECT_EQ(purgatory.pendingCount(), 1u);

    // A different partition does not wake it.
    ready = true;
    purgatory.notify("t", 1);
    purgatory.notify("other", 0);
    std::this_thread::sleep_for(20ms);
    EXPECT_EQ(completions.load(), 0);

    purgatory.notify("t", 0);
    ASSERT_TRUE(waitUntil([&] { return completions.load() == 1; }));
    EXPECT_FALSE(completed_expired.load());
    EXPECT_TRUE(waitUntil([&] { return purgatory.pendingCount() == 0; }));

    // Further notifies never re-run a completed operation.
    purgatory.notify("t", 0);
    std::this_thread::sleep_for(20ms);
    EXPECT_EQ(completions.load(), 1);
}

TEST(DelayedOperationPurgatoryTest, DeadlineForcesExpiredCompletion) {
    DelayedOperationPurgatory purgatory(1);
    std::atomic<int> expired_calls{0};
    auto start = std::chrono::steady_clock::now();
    std::atomic<int64_t> elapsed_ms{0};

    ASSERT_TRUE(purgatory.watch({TopicPartition{"t", 0}},
                                DelayedOperationPurgatory::Clock::now() + 100ms,
                                [&](bool expired) {
                                    if (!expired) {
                                        return false;
                                    }
                                    elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                                     std::chrono::steady_clock::now() - start)
                                                     .count();
                                    ++expired_calls;
                                    return false;  // Ignored: expiry always finishes the op.
                                }));
    ASSERT_TRUE(waitUntil([&] { return expired_calls.load() == 1; }));
    EXPECT_GE(elapsed_ms.load(), 90);
    EXPECT_TRUE(waitUntil([&] { return purgatory.pendingCount() == 0; }));
    purgatory.notify("t", 0);
    std::this_thread::sleep_for(20ms);
    EXPECT_EQ(expired_calls.load(), 1);
}

TEST(DelayedOperationPurgatoryTest, OperationNeverRunsConcurrentlyWithItself) {
    DelayedOperationPurgatory purgatory(4);
    std::atomic<int> in_flight{0};
    std::atomic<int> max_in_flight{0};
    std::atomic<int> attempts{0};

    ASSERT_TRUE(purgatory.watch({TopicPartition{"t", 0}},
                                DelayedOperationPurgatory::Clock::now() + 300ms,
                                [&](bool expired) {
                                    int now = ++in_flight;
                                    int prev = max_in_flight.load();
                                    while (now > prev && !max_in_flight.compare_exchange_weak(prev, now)) {
                                    }
                                    ++attempts;
                                    std::this_thread::sleep_for(2ms);
                                    --in_flight;
                                    return expired;
                                }));
    for (int i = 0; i < 200; ++i) {
        purgatory.notify("t", 0);
    }
    ASSERT_TRUE(waitUntil([&] { return purgatory.pendingCount() == 0; }));
    EXPECT_EQ(max_in_flight.load(), 1);
    // Retries are coalesced: far fewer attempts than notifications.
    EXPECT_LT(attempts.load(), 200);
}

TEST(DelayedOperationPurgatoryTest, StopDropsParkedOperationsAndRejectsNewOnes) {
    DelayedOperationPurgatory purgatory(1);
    auto token = std::make_shared<int>(0);
    std::weak_ptr<int> weak = token;
    std::atomic<int> calls{0};

    ASSERT_TRUE(purgatory.watch({TopicPartition{"t", 0}},
                                DelayedOperationPurgatory::Clock::now() + 30s,
                                [&calls, token](bool) {
                                    ++calls;
                                    return false;
                                }));
    token.reset();
    std::this_thread::sleep_for(20ms);
    int calls_before_stop = calls.load();

    purgatory.stop();
    EXPECT_TRUE(weak.expired());  // Captured state released.
    EXPECT_EQ(purgatory.pendingCount(), 0u);
    purgatory.notify("t", 0);
    EXPECT_EQ(calls.load(), calls_before_stop);

    EXPECT_FALSE(purgatory.watch({TopicPartition{"t", 0}},
                                 DelayedOperationPurgatory::Clock::now() + 1s,
                                 [](bool) { return true; }));
}
