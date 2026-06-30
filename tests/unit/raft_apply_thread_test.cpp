// Phase B prerequisite #3: commit-apply off the Raft lock. These lock in the
// dedicated apply-thread contract: committed entries are applied to the state
// machine (via commit_callback_) on a single dedicated thread, off log_mutex_,
// in strict index order, exactly once — and the null-callback startup window
// and clean shutdown drain are handled. Uses a single-node RaftNode (becomes
// leader immediately, no peers), so appendCommand commits at once and the apply
// thread does the rest.
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <mutex>
#include <thread>
#include <vector>

#include <boost/asio/io_context.hpp>

#include "kawasan/raft/raft_node.h"

using namespace kawasan::raft;

namespace {

// Waits up to ~3s for `pred` to hold, polling every 5ms.
template <typename Pred>
bool waitFor(Pred pred) {
    for (int i = 0; i < 600; ++i) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    return pred();
}

}  // namespace

TEST(RaftApplyThreadTest, AppliesInOrderExactlyOnce) {
    boost::asio::io_context io;
    RaftNode node(/*id=*/1, std::vector<PeerInfo>{}, io, /*port=*/0, /*data_dir=*/"");

    std::mutex m;
    std::vector<int64_t> applied;
    node.setCommitCallback([&](const LogEntry& e) {
        std::lock_guard<std::mutex> lk(m);
        applied.push_back(e.index);
    });

    node.start();  // single-node -> leader immediately; apply thread running

    constexpr int N = 100;
    for (int i = 0; i < N; ++i) {
        node.appendCommand({static_cast<uint8_t>(i)}, "test").get();
    }

    ASSERT_TRUE(waitFor([&] {
        std::lock_guard<std::mutex> lk(m);
        return applied.size() >= static_cast<size_t>(N);
    }));

    node.stop();

    std::lock_guard<std::mutex> lk(m);
    ASSERT_EQ(applied.size(), static_cast<size_t>(N));
    for (int64_t i = 0; i < N; ++i) {
        EXPECT_EQ(applied[static_cast<size_t>(i)], i + 1) << "index " << i;  // 1..N in order
    }
}

// A slow callback must NOT block the Raft hot path: appendCommand (which takes
// log_mutex_) returns promptly even while a 50ms apply is in flight, proving the
// apply runs off log_mutex_.
TEST(RaftApplyThreadTest, ApplyDoesNotBlockTheRaftLock) {
    boost::asio::io_context io;
    RaftNode node(1, std::vector<PeerInfo>{}, io, 0, "");

    std::atomic<int> applied{0};
    node.setCommitCallback([&](const LogEntry&) {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));  // heavy work
        applied.fetch_add(1);
    });
    node.start();

    node.appendCommand({0}, "test").get();  // triggers a 50ms apply

    // While that apply is sleeping, a second appendCommand must not wait ~50ms.
    const auto t0 = std::chrono::steady_clock::now();
    node.appendCommand({1}, "test").get();
    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - t0).count();
    EXPECT_LT(elapsed, 40) << "appendCommand blocked on the apply callback (not off-lock)";

    ASSERT_TRUE(waitFor([&] { return applied.load() >= 2; }));
    node.stop();
}

// Entries committed before the callback is installed must be applied once it is
// set — never skipped (the startup null-callback window).
TEST(RaftApplyThreadTest, NullCallbackWindowDoesNotDropEntries) {
    boost::asio::io_context io;
    RaftNode node(1, std::vector<PeerInfo>{}, io, 0, "");

    node.start();  // no callback installed yet
    node.appendCommand({0}, "test").get();
    node.appendCommand({1}, "test").get();
    std::this_thread::sleep_for(std::chrono::milliseconds(50));  // worker spins on null cb

    std::mutex m;
    std::vector<int64_t> applied;
    node.setCommitCallback([&](const LogEntry& e) {
        std::lock_guard<std::mutex> lk(m);
        applied.push_back(e.index);
    });

    ASSERT_TRUE(waitFor([&] {
        std::lock_guard<std::mutex> lk(m);
        return applied.size() >= 2;
    }));
    node.stop();

    std::lock_guard<std::mutex> lk(m);
    ASSERT_EQ(applied.size(), 2u);
    EXPECT_EQ(applied[0], 1);
    EXPECT_EQ(applied[1], 2);
}

// stop() drains all committed entries before joining (graceful), even with a
// slow callback.
TEST(RaftApplyThreadTest, CleanStopDrainsCommittedEntries) {
    boost::asio::io_context io;
    RaftNode node(1, std::vector<PeerInfo>{}, io, 0, "");

    std::atomic<int> applied{0};
    node.setCommitCallback([&](const LogEntry&) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        applied.fetch_add(1);
    });
    node.start();
    for (int i = 0; i < 20; ++i) {
        node.appendCommand({static_cast<uint8_t>(i)}, "test").get();
    }
    node.stop();  // must drain all 20 before returning
    EXPECT_EQ(applied.load(), 20);
}
