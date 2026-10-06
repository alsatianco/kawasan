#include "kawasan/broker/coordinator_executor.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>

using namespace kawasan::broker;
using namespace std::chrono_literals;

TEST(CoordinatorExecutorTest, SealRejectsQueuedWorkAndRetainedResponsesKeepDrainOpen) {
    CoordinatorExecutor executor(1, 1);
    std::promise<void> entered, release;
    auto released = release.get_future().share();
    CoordinatorExecutor::Ticket retained;
    ASSERT_TRUE(executor.submit([&](bool admitted, auto ticket) {
        EXPECT_TRUE(admitted);
        retained = std::move(ticket);
        entered.set_value();
        released.wait();
    }));
    ASSERT_EQ(entered.get_future().wait_for(2s), std::future_status::ready);
    std::promise<bool> queued;
    EXPECT_TRUE(executor.submit([&](bool admitted, auto) { queued.set_value(admitted); }));
    EXPECT_FALSE(executor.submit([](bool, auto) { ADD_FAILURE() << "queue overflow ran"; }));
    executor.seal();
    EXPECT_FALSE(executor.submit([](bool, auto) { ADD_FAILURE() << "sealed queue ran"; }));
    release.set_value();
    executor.join();
    EXPECT_FALSE(queued.get_future().get());
    auto drain = std::async(std::launch::async, [&] { executor.drainResponses(); });
    EXPECT_EQ(drain.wait_for(50ms), std::future_status::timeout);
    retained.reset();
    EXPECT_EQ(drain.wait_for(2s), std::future_status::ready);
    drain.get();
}

TEST(CoordinatorExecutorTest, ThrowingTaskReleasesTicketAndWorkerContinues) {
    CoordinatorExecutor executor(1);
    std::promise<void> entered;
    ASSERT_TRUE(executor.submit([](bool, auto) { throw std::runtime_error("task failed"); }));
    ASSERT_TRUE(executor.submit([&](bool admitted, auto) {
        EXPECT_TRUE(admitted);
        entered.set_value();
    }));
    EXPECT_EQ(entered.get_future().wait_for(2s), std::future_status::ready);
    executor.seal();
    executor.join();
    executor.drainResponses();
}
