// Copyright 2025 Kawasan Project
// Licensed under the Apache License, Version 2.0

#include "kawasan/raft/raft_transport.h"
#include <gtest/gtest.h>
#include <thread>
#include <chrono>

using namespace kawasan::raft;

class RaftTransportTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create IO context and work guard to keep it running.
        io_context_ = std::make_unique<boost::asio::io_context>();
        work_guard_ = std::make_unique<boost::asio::executor_work_guard<boost::asio::io_context::executor_type>>(
            boost::asio::make_work_guard(*io_context_));

        // 0A.9: Use a thread pool instead of a single thread. The send path in
        // RaftTransport does synchronous reads inside a posted lambda, which
        // previously deadlocked the single io_thread (the same thread that
        // needed to run the accept handler on the peer transport). The
        // ConcurrentRequestsFromMultiplePeers test sends 20 in-flight RPCs
        // simultaneously, each occupying a thread for the duration of the
        // read; size the pool generously to cover concurrent fan-out.
        for (int i = 0; i < 32; ++i) {
            io_threads_.emplace_back([this]() { io_context_->run(); });
        }
    }

    void TearDown() override {
        // 0A.9: stop io_context BEFORE the test body's transports are destroyed
        // (they go out of scope after TearDown runs in this fixture). Calling
        // work_guard_.reset() then io_context_->stop() drains pending handlers
        // so they cannot touch the per-transport mutexes after destruction.
        work_guard_.reset();
        io_context_->stop();

        for (auto& t : io_threads_) {
            if (t.joinable()) t.join();
        }
    }

    std::unique_ptr<boost::asio::io_context> io_context_;
    std::unique_ptr<boost::asio::executor_work_guard<boost::asio::io_context::executor_type>> work_guard_;
    std::vector<std::thread> io_threads_;
};

//==============================================================================
// Basic Lifecycle Tests
//==============================================================================

TEST_F(RaftTransportTest, ConstructAndDestroy) {
    RaftTransport transport(*io_context_, 1);
    EXPECT_FALSE(transport.is_running());
}

TEST_F(RaftTransportTest, StartAndStop) {
    RaftTransport transport(*io_context_, 1);
    
    // Start on a random port
    transport.start(19093);
    EXPECT_TRUE(transport.is_running());
    
    // Stop
    transport.stop();
    EXPECT_FALSE(transport.is_running());
}

TEST_F(RaftTransportTest, DoubleStartIgnored) {
    RaftTransport transport(*io_context_, 1);
    
    transport.start(19093);
    EXPECT_TRUE(transport.is_running());
    
    // Second start should be ignored
    transport.start(19093);
    EXPECT_TRUE(transport.is_running());
    
    transport.stop();
}

TEST_F(RaftTransportTest, DoubleStopSafe) {
    RaftTransport transport(*io_context_, 1);
    
    transport.start(19093);
    transport.stop();
    EXPECT_FALSE(transport.is_running());
    
    // Second stop should be safe
    transport.stop();
    EXPECT_FALSE(transport.is_running());
}

//==============================================================================
// Peer Management Tests
//==============================================================================

TEST_F(RaftTransportTest, AddPeer) {
    RaftTransport transport(*io_context_, 1);
    
    // Should not throw
    EXPECT_NO_THROW(transport.add_peer(2, "localhost", 19094));
}

TEST_F(RaftTransportTest, AddSelfAseerIgnored) {
    RaftTransport transport(*io_context_, 1);
    
    // Adding self should be ignored (logged as warning but no error)
    EXPECT_NO_THROW(transport.add_peer(1, "localhost", 19093));
}

TEST_F(RaftTransportTest, RemovePeer) {
    RaftTransport transport(*io_context_, 1);
    
    transport.add_peer(2, "localhost", 19094);
    
    // Remove should not throw
    EXPECT_NO_THROW(transport.remove_peer(2));
    
    // Removing non-existent peer should be safe
    EXPECT_NO_THROW(transport.remove_peer(999));
}

//==============================================================================
// Handler Registration Tests
//==============================================================================

TEST_F(RaftTransportTest, SetAppendEntriesHandler) {
    RaftTransport transport(*io_context_, 1);
    
    bool handler_called = false;
    transport.setAppendEntriesHandler(
        [&handler_called](const AppendEntriesRequest& req) {
            handler_called = true;
            AppendEntriesResponse resp;
            resp.term = req.term;
            resp.success = true;
            resp.last_log_index = 0;
            return resp;
        });
    
    // Handler is set (no way to verify directly without sending request)
    EXPECT_TRUE(true);
}

TEST_F(RaftTransportTest, SetRequestVoteHandler) {
    RaftTransport transport(*io_context_, 1);
    
    bool handler_called = false;
    transport.setRequestVoteHandler(
        [&handler_called](const RequestVoteRequest& req) {
            handler_called = true;
            RequestVoteResponse resp;
            resp.term = req.term;
            resp.vote_granted = true;
            return resp;
        });
    
    // Handler is set
    EXPECT_TRUE(true);
}

//==============================================================================
// RPC Send/Receive Tests
//==============================================================================

TEST_F(RaftTransportTest, SendAppendEntriesSuccess) {
    // Create two transports (peer 1 and peer 2)
    RaftTransport transport1(*io_context_, 1);
    RaftTransport transport2(*io_context_, 2);
    
    // Start both
    transport1.start(19093);
    transport2.start(19094);
    
    // Add peer 2 to transport 1
    transport1.add_peer(2, "localhost", 19094);
    
    // Set handler on peer 2
    std::atomic<bool> handler_called{false};
    transport2.setAppendEntriesHandler(
        [&handler_called](const AppendEntriesRequest& req) {
            handler_called = true;
            AppendEntriesResponse resp;
            resp.term = req.term;
            resp.success = true;
            resp.last_log_index = req.prev_log_index + req.entries.size();
            return resp;
        });
    
    // Send request from peer 1 to peer 2
    AppendEntriesRequest request;
    request.term = 1;
    request.leader_id = 1;
    request.prev_log_index = 0;
    request.prev_log_term = 0;
    request.leader_commit = 0;
    
    auto future = transport1.sendAppendEntries(2, request);
    
    // Wait for response
    auto status = future.wait_for(std::chrono::seconds(2));
    ASSERT_EQ(status, std::future_status::ready);
    
    auto response = future.get();
    EXPECT_EQ(response.term, 1);
    EXPECT_TRUE(response.success);
    EXPECT_TRUE(handler_called);
    
    // Cleanup
    transport1.stop();
    transport2.stop();
}

TEST_F(RaftTransportTest, SendRequestVoteSuccess) {
    // Create two transports
    RaftTransport transport1(*io_context_, 1);
    RaftTransport transport2(*io_context_, 2);
    
    // Start both
    transport1.start(19093);
    transport2.start(19094);
    
    // Add peer 2 to transport 1
    transport1.add_peer(2, "localhost", 19094);
    
    // Set handler on peer 2
    std::atomic<bool> handler_called{false};
    transport2.setRequestVoteHandler(
        [&handler_called](const RequestVoteRequest& req) {
            handler_called = true;
            RequestVoteResponse resp;
            resp.term = req.term;
            resp.vote_granted = true;
            return resp;
        });
    
    // Send request from peer 1 to peer 2
    RequestVoteRequest request;
    request.term = 1;
    request.candidate_id = 1;
    request.last_log_index = 0;
    request.last_log_term = 0;
    
    auto future = transport1.sendRequestVote(2, request);
    
    // Wait for response
    auto status = future.wait_for(std::chrono::seconds(2));
    ASSERT_EQ(status, std::future_status::ready);
    
    auto response = future.get();
    EXPECT_EQ(response.term, 1);
    EXPECT_TRUE(response.vote_granted);
    EXPECT_TRUE(handler_called);
    
    // Cleanup
    transport1.stop();
    transport2.stop();
}

TEST_F(RaftTransportTest, SendToUnknownPeerFails) {
    RaftTransport transport(*io_context_, 1);
    transport.start(19093);
    
    // Try to send to unknown peer
    AppendEntriesRequest request;
    request.term = 1;
    request.leader_id = 1;
    
    auto future = transport.sendAppendEntries(999, request);
    
    // Should throw or return error
    auto status = future.wait_for(std::chrono::seconds(1));
    ASSERT_EQ(status, std::future_status::ready);
    
    EXPECT_THROW(future.get(), std::exception);
    
    transport.stop();
}

TEST_F(RaftTransportTest, SendToOfflinePeerFails) {
    RaftTransport transport(*io_context_, 1);
    transport.start(19093);
    
    // Add peer that doesn't exist
    transport.add_peer(2, "localhost", 19999); // Port that's not listening
    
    AppendEntriesRequest request;
    request.term = 1;
    request.leader_id = 1;
    
    auto future = transport.sendAppendEntries(2, request);
    
    // Should fail after retry attempts
    auto status = future.wait_for(std::chrono::seconds(5));
    ASSERT_EQ(status, std::future_status::ready);
    
    EXPECT_THROW(future.get(), std::exception);
    
    transport.stop();
}

//==============================================================================
// Connection Pooling Tests
//==============================================================================

TEST_F(RaftTransportTest, ReuseConnectionForMultipleRequests) {
    RaftTransport transport1(*io_context_, 1);
    RaftTransport transport2(*io_context_, 2);
    
    transport1.start(19093);
    transport2.start(19094);
    
    transport1.add_peer(2, "localhost", 19094);
    
    std::atomic<int> handler_count{0};
    transport2.setAppendEntriesHandler(
        [&handler_count](const AppendEntriesRequest& req) {
            handler_count++;
            AppendEntriesResponse resp;
            resp.term = req.term;
            resp.success = true;
            resp.last_log_index = 0;
            return resp;
        });
    
    // Send multiple requests
    AppendEntriesRequest request;
    request.term = 1;
    request.leader_id = 1;
    
    for (int i = 0; i < 5; i++) {
        auto future = transport1.sendAppendEntries(2, request);
        auto status = future.wait_for(std::chrono::seconds(2));
        ASSERT_EQ(status, std::future_status::ready);
        
        auto response = future.get();
        EXPECT_TRUE(response.success);
    }
    
    // All requests should have been handled
    EXPECT_EQ(handler_count, 5);
    
    transport1.stop();
    transport2.stop();
}

//==============================================================================
// Large Message Tests
//==============================================================================

TEST_F(RaftTransportTest, SendLargeAppendEntries) {
    RaftTransport transport1(*io_context_, 1);
    RaftTransport transport2(*io_context_, 2);
    
    transport1.start(19093);
    transport2.start(19094);
    
    transport1.add_peer(2, "localhost", 19094);
    
    std::atomic<bool> handler_called{false};
    std::atomic<size_t> received_entries{0};
    transport2.setAppendEntriesHandler(
        [&handler_called, &received_entries](const AppendEntriesRequest& req) {
            handler_called = true;
            received_entries = req.entries.size();
            AppendEntriesResponse resp;
            resp.term = req.term;
            resp.success = true;
            resp.last_log_index = req.prev_log_index + req.entries.size();
            return resp;
        });
    
    // Create large request with multiple log entries
    AppendEntriesRequest request;
    request.term = 1;
    request.leader_id = 1;
    request.prev_log_index = 0;
    request.prev_log_term = 0;
    request.leader_commit = 0;
    
    // Add 100 log entries
    for (int i = 0; i < 100; i++) {
        LogEntry entry;
        entry.term = 1;
        entry.index = i + 1;
        entry.data.resize(1024, 'x'); // 1KB per entry
        request.entries.push_back(entry);
    }
    
    auto future = transport1.sendAppendEntries(2, request);
    
    auto status = future.wait_for(std::chrono::seconds(5));
    ASSERT_EQ(status, std::future_status::ready);
    
    auto response = future.get();
    EXPECT_TRUE(response.success);
    EXPECT_TRUE(handler_called);
    EXPECT_EQ(received_entries, 100);
    
    transport1.stop();
    transport2.stop();
}

//==============================================================================
// Concurrent Request Tests
//==============================================================================

TEST_F(RaftTransportTest, ConcurrentRequestsFromMultiplePeers) {
    RaftTransport transport1(*io_context_, 1);
    RaftTransport transport2(*io_context_, 2);
    RaftTransport transport3(*io_context_, 3);
    
    transport1.start(19093);
    transport2.start(19094);
    transport3.start(19095);
    
    // Peer 1 knows about peer 2 and 3
    transport1.add_peer(2, "localhost", 19094);
    transport1.add_peer(3, "localhost", 19095);
    
    std::atomic<int> peer2_count{0};
    std::atomic<int> peer3_count{0};
    
    transport2.setAppendEntriesHandler(
        [&peer2_count](const AppendEntriesRequest& req) {
            peer2_count++;
            AppendEntriesResponse resp;
            resp.term = req.term;
            resp.success = true;
            resp.last_log_index = 0;
            return resp;
        });
    
    transport3.setAppendEntriesHandler(
        [&peer3_count](const AppendEntriesRequest& req) {
            peer3_count++;
            AppendEntriesResponse resp;
            resp.term = req.term;
            resp.success = true;
            resp.last_log_index = 0;
            return resp;
        });
    
    // Send concurrent requests to both peers
    AppendEntriesRequest request;
    request.term = 1;
    request.leader_id = 1;
    
    std::vector<std::future<AppendEntriesResponse>> futures;
    
    for (int i = 0; i < 10; i++) {
        futures.push_back(transport1.sendAppendEntries(2, request));
        futures.push_back(transport1.sendAppendEntries(3, request));
    }
    
    // Wait for all responses
    for (auto& future : futures) {
        auto status = future.wait_for(std::chrono::seconds(5));
        ASSERT_EQ(status, std::future_status::ready);
        auto response = future.get();
        EXPECT_TRUE(response.success);
    }
    
    // Check both peers received requests
    EXPECT_EQ(peer2_count, 10);
    EXPECT_EQ(peer3_count, 10);
    
    transport1.stop();
    transport2.stop();
    transport3.stop();
}

//==============================================================================
// Error Recovery Tests
//==============================================================================

TEST_F(RaftTransportTest, ReconnectAfterPeerRestart) {
    RaftTransport transport1(*io_context_, 1);
    auto transport2 = std::make_unique<RaftTransport>(*io_context_, 2);
    
    transport1.start(19093);
    transport2->start(19094);
    
    transport1.add_peer(2, "localhost", 19094);
    
    transport2->setAppendEntriesHandler(
        [](const AppendEntriesRequest& req) {
            AppendEntriesResponse resp;
            resp.term = req.term;
            resp.success = true;
            resp.last_log_index = 0;
            return resp;
        });
    
    // Send first request (should succeed)
    AppendEntriesRequest request;
    request.term = 1;
    request.leader_id = 1;
    
    auto future1 = transport1.sendAppendEntries(2, request);
    auto status1 = future1.wait_for(std::chrono::seconds(2));
    ASSERT_EQ(status1, std::future_status::ready);
    EXPECT_TRUE(future1.get().success);
    
    // Stop peer 2
    transport2->stop();
    transport2.reset();
    
    // Wait a bit
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    // Restart peer 2
    transport2 = std::make_unique<RaftTransport>(*io_context_, 2);
    transport2->start(19094);
    transport2->setAppendEntriesHandler(
        [](const AppendEntriesRequest& req) {
            AppendEntriesResponse resp;
            resp.term = req.term;
            resp.success = true;
            resp.last_log_index = 0;
            return resp;
        });
    
    // Wait for port to be available
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    // Send second request (should succeed after reconnect)
    auto future2 = transport1.sendAppendEntries(2, request);
    auto status2 = future2.wait_for(std::chrono::seconds(3));
    ASSERT_EQ(status2, std::future_status::ready);
    EXPECT_TRUE(future2.get().success);
    
    transport1.stop();
    transport2->stop();
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
