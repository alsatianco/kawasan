// M8-B: Raft-derived broker liveness. The Raft leader (== controller) records
// when each peer last answered an AppendEntries; a stopped peer's age grows
// while live peers stay fresh, and a newly elected leader grants every peer a
// full grace window (ages restart near 0) instead of treating them as dead.
// Runs three real RaftNodes in-process over loopback.
#include <gtest/gtest.h>

#include <atomic>
#include <boost/asio.hpp>
#include <chrono>
#include <memory>
#include <thread>
#include <vector>

#include "kawasan/common/logger.h"
#include "kawasan/raft/raft_node.h"
#include "kawasan/raft/raft_transport.h"

using namespace kawasan::raft;
using namespace std::chrono_literals;

namespace {

int freePort() {
    boost::asio::io_context io;
    boost::asio::ip::tcp::acceptor acceptor(
        io, boost::asio::ip::tcp::endpoint(boost::asio::ip::make_address("127.0.0.1"), 0));
    return acceptor.local_endpoint().port();
}

// One io_context per node, as in a real broker: RaftTransport's client RPCs do
// blocking reads on io_context threads, so nodes must not share threads.
struct NodeIo {
    boost::asio::io_context io;
    boost::asio::executor_work_guard<boost::asio::io_context::executor_type> guard{
        io.get_executor()};
    std::vector<std::thread> threads;
    NodeIo() {
        for (int i = 0; i < 2; ++i) {
            threads.emplace_back([this] { io.run(); });
        }
    }
    ~NodeIo() {
        guard.reset();
        io.stop();
        for (auto& t : threads) {
            t.join();
        }
    }
};

struct Cluster {
    std::vector<std::unique_ptr<NodeIo>> ios;
    std::vector<std::unique_ptr<RaftNode>> nodes;  // index == broker id

    Cluster() {
        static bool logger = [] {
            kawasan::Logger::init("warn");
            return true;
        }();
        (void)logger;
        std::vector<int> ports{freePort(), freePort(), freePort()};
        for (int id = 0; id < 3; ++id) {
            std::vector<PeerInfo> peers;
            for (int other = 0; other < 3; ++other) {
                if (other != id) {
                    peers.push_back(PeerInfo{other, "127.0.0.1", ports[other]});
                }
            }
            ios.push_back(std::make_unique<NodeIo>());
            nodes.push_back(std::make_unique<RaftNode>(id, peers, ios.back()->io, ports[id], ""));
            // A real broker always has its metadata state machine attached;
            // without one nothing is applied and no follower is ever current.
            nodes.back()->setCommitCallback([](const LogEntry&) {});
        }
        for (auto& node : nodes) {
            node->start();
        }
    }
    ~Cluster() {
        for (auto& node : nodes) {
            node->stop();
        }
        nodes.clear();
        ios.clear();
    }

    // Waits for a leader among the running nodes (excluding `skip`).
    int waitForLeader(int skip = -1, std::chrono::milliseconds timeout = 10s) {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            for (int id = 0; id < 3; ++id) {
                if (id != skip && nodes[id]->isLeader()) {
                    return id;
                }
            }
            std::this_thread::sleep_for(5ms);
        }
        return -1;
    }
};

}  // namespace

TEST(RaftLivenessTest, NonLeaderReportsNoAges) {
    boost::asio::io_context io;
    RaftNode node(1, {PeerInfo{2, "127.0.0.1", 1}}, io, 0, "");
    EXPECT_TRUE(node.peerAckAgesMs().empty());    // not started, not leader
    EXPECT_FALSE(node.hasCurrentMetadata(1000));  // never heard from a leader
}

TEST(RaftLivenessTest, LeaderTracksPeerAcksAndDeadPeerAges) {
    Cluster cluster;
    const int leader = cluster.waitForLeader();
    ASSERT_GE(leader, 0);

    // Heartbeats every 50ms: both peers answer and stay fresh.
    std::this_thread::sleep_for(500ms);
    auto ages = cluster.nodes[leader]->peerAckAgesMs();
    ASSERT_EQ(ages.size(), 2u);
    for (const auto& [id, age] : ages) {
        EXPECT_LT(age, 400) << "peer " << id;
    }

    const int victim = (leader + 1) % 3;
    const int survivor = (leader + 2) % 3;
    cluster.nodes[victim]->stop();
    std::this_thread::sleep_for(1200ms);

    ages = cluster.nodes[leader]->peerAckAgesMs();
    EXPECT_GT(ages.at(victim), 900);
    EXPECT_LT(ages.at(survivor), 400);
}

TEST(RaftLivenessTest, NewLeaderGrantsEveryPeerAGraceWindow) {
    Cluster cluster;
    const int old_leader = cluster.waitForLeader();
    ASSERT_GE(old_leader, 0);

    cluster.nodes[old_leader]->stop();
    const int new_leader = cluster.waitForLeader(old_leader);
    ASSERT_GE(new_leader, 0);

    // The dead old leader never acked the new leader, but its age restarts at
    // leadership acquisition — not "infinitely old".
    auto ages = cluster.nodes[new_leader]->peerAckAgesMs();
    ASSERT_EQ(ages.size(), 2u);
    EXPECT_LT(ages.at(old_leader), 1000);
    EXPECT_GE(ages.at(old_leader), 0);

    std::this_thread::sleep_for(1200ms);
    ages = cluster.nodes[new_leader]->peerAckAgesMs();
    EXPECT_GT(ages.at(old_leader), 900);  // and then grows, since it is dead
}

// M8-E1: a node may serve its metadata view only while it is provably current:
// a leader needs a quorum of recent acks, a follower needs to have caught up
// with a leader since it started and to have heard from it recently.
TEST(RaftLivenessTest, MetadataCurrencyNeedsQuorumOrLeaderContact) {
    Cluster cluster;
    const int leader = cluster.waitForLeader();
    ASSERT_GE(leader, 0);
    std::this_thread::sleep_for(500ms);
    for (int id = 0; id < 3; ++id) {
        EXPECT_TRUE(cluster.nodes[id]->hasCurrentMetadata(1000)) << "node " << id;
    }

    // One follower down: the leader still has a quorum, the other follower
    // still hears from the leader.
    const int f1 = (leader + 1) % 3;
    const int f2 = (leader + 2) % 3;
    cluster.nodes[f1]->stop();
    std::this_thread::sleep_for(1500ms);
    EXPECT_TRUE(cluster.nodes[leader]->hasCurrentMetadata(1000));
    EXPECT_TRUE(cluster.nodes[f2]->hasCurrentMetadata(1000));

    // Both followers down: the leader has lost its quorum.
    cluster.nodes[f2]->stop();
    std::this_thread::sleep_for(1500ms);
    EXPECT_FALSE(cluster.nodes[leader]->hasCurrentMetadata(1000));
}

TEST(RaftLivenessTest, FollowerLosesCurrencyWithoutALeaderAndRegainsIt) {
    Cluster cluster;
    const int leader = cluster.waitForLeader();
    ASSERT_GE(leader, 0);
    std::this_thread::sleep_for(300ms);
    cluster.nodes[leader]->stop();
    // Immediately after, the followers are still within the lease...
    // ...and a new leader is elected quickly; its follower is current again.
    const int new_leader = cluster.waitForLeader(leader);
    ASSERT_GE(new_leader, 0);
    const int follower = 3 - leader - new_leader;
    const auto deadline = std::chrono::steady_clock::now() + 5s;
    while (!cluster.nodes[follower]->hasCurrentMetadata(1000) &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(20ms);
    }
    EXPECT_TRUE(cluster.nodes[follower]->hasCurrentMetadata(1000));
    EXPECT_TRUE(cluster.nodes[new_leader]->hasCurrentMetadata(1000));
}

// M8-E1: a new leader commits a no-op of its own term, so entries committed by
// the previous leader reach every state machine without waiting for a command.
TEST(RaftLivenessTest, NewLeaderCommitsANoop) {
    Cluster cluster;
    std::vector<std::shared_ptr<std::atomic<int>>> noops;
    for (int id = 0; id < 3; ++id) {
        auto counter = std::make_shared<std::atomic<int>>(0);
        noops.push_back(counter);
        cluster.nodes[id]->setCommitCallback([counter](const LogEntry& entry) {
            if (entry.command_type == "noop") {
                counter->fetch_add(1);
            }
        });
    }
    ASSERT_GE(cluster.waitForLeader(), 0);
    const auto deadline = std::chrono::steady_clock::now() + 5s;
    auto all_applied = [&] {
        for (const auto& n : noops) {
            if (n->load() == 0) {
                return false;
            }
        }
        return true;
    };
    while (!all_applied() && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(20ms);
    }
    EXPECT_TRUE(all_applied());
}

// Regression (found by the M8 SIGSTOP nemesis): RaftTransport ran outgoing RPCs
// as blocking reads with no timeout on the same single io thread that serves
// incoming RPCs. One unresponsive peer then wedged that thread forever, and two
// nodes calling each other at once deadlocked. Outgoing RPCs must now be
// bounded, and must never stop the node from answering others.
TEST(RaftLivenessTest, UnresponsivePeerNeitherHangsRpcsNorBlocksServing) {
    // A "frozen" peer: accepts connections, never answers.
    boost::asio::io_context hole_io;
    boost::asio::ip::tcp::acceptor hole(
        hole_io, boost::asio::ip::tcp::endpoint(boost::asio::ip::make_address("127.0.0.1"), 0));
    const int hole_port = hole.local_endpoint().port();
    boost::asio::ip::tcp::socket held(hole_io);
    std::thread hole_thread([&] {
        boost::system::error_code ec;
        hole.accept(held, ec);
    });

    // Node A: one io thread, exactly like a broker.
    boost::asio::io_context a_io;
    auto a_guard = boost::asio::make_work_guard(a_io);
    std::thread a_thread([&] { a_io.run(); });
    const int a_port = freePort();
    RaftTransport a(a_io, 1);
    a.setRequestVoteHandler([](const RequestVoteRequest& req) {
        RequestVoteResponse resp;
        resp.term = req.term;
        resp.vote_granted = true;
        return resp;
    });
    a.start(a_port);
    a.add_peer(9, "127.0.0.1", hole_port);

    RequestVoteRequest req;
    req.term = 7;
    req.candidate_id = 1;
    auto stuck = a.sendRequestVote(9, req);

    // While A's RPC to the frozen peer is outstanding, B can still get an answer
    // from A.
    boost::asio::io_context b_io;
    auto b_guard = boost::asio::make_work_guard(b_io);
    std::thread b_thread([&] { b_io.run(); });
    RaftTransport b(b_io, 2);
    b.start(freePort());
    b.add_peer(1, "127.0.0.1", a_port);
    std::this_thread::sleep_for(100ms);
    auto answered = b.sendRequestVote(1, req);
    ASSERT_EQ(answered.wait_for(2s), std::future_status::ready);
    EXPECT_TRUE(answered.get().vote_granted);

    // And the RPC to the frozen peer fails within its deadline instead of
    // hanging forever.
    ASSERT_EQ(stuck.wait_for(5s), std::future_status::ready);
    EXPECT_THROW(stuck.get(), std::exception);

    a.stop();
    b.stop();
    a_guard.reset();
    b_guard.reset();
    a_io.stop();
    b_io.stop();
    a_thread.join();
    b_thread.join();
    hole.close();
    hole_io.stop();
    hole_thread.join();
}

// Regression (M8 divergence nemesis): RPCs queued to a frozen peer must not
// starve RPCs to live peers. With a shared client pool, a heartbeat backlog to
// the frozen peer occupied every thread and elections never converged.
TEST(RaftLivenessTest, FrozenPeerBacklogDoesNotDelayOtherPeers) {
    boost::asio::io_context hole_io;
    boost::asio::ip::tcp::acceptor hole(
        hole_io, boost::asio::ip::tcp::endpoint(boost::asio::ip::make_address("127.0.0.1"), 0));
    boost::asio::ip::tcp::socket held(hole_io);
    std::thread hole_thread([&] {
        boost::system::error_code ec;
        hole.accept(held, ec);
    });

    NodeIo live_io;
    const int live_port = freePort();
    RaftTransport live(live_io.io, 2);
    live.setRequestVoteHandler([](const RequestVoteRequest& req) {
        RequestVoteResponse resp;
        resp.term = req.term;
        resp.vote_granted = true;
        return resp;
    });
    live.start(live_port);

    NodeIo a_io;
    RaftTransport a(a_io.io, 1);
    a.start(freePort());
    a.add_peer(9, "127.0.0.1", hole.local_endpoint().port());
    a.add_peer(2, "127.0.0.1", live_port);

    RequestVoteRequest req;
    req.term = 3;
    req.candidate_id = 1;
    std::vector<std::future<RequestVoteResponse>> backlog;
    for (int i = 0; i < 20; ++i) {
        backlog.push_back(a.sendRequestVote(9, req));
    }
    const auto start = std::chrono::steady_clock::now();
    auto vote = a.sendRequestVote(2, req);
    ASSERT_EQ(vote.wait_for(500ms), std::future_status::ready);
    EXPECT_TRUE(vote.get().vote_granted);
    EXPECT_LT(std::chrono::steady_clock::now() - start, 500ms);

    a.stop();
    live.stop();
    hole.close();
    hole_io.stop();
    hole_thread.join();
}

// Regression (M8 divergence nemesis): a follower that was cut off (frozen,
// partitioned) must re-catch-up before it counts as current again. Merely
// hearing from the leader is not enough: its applied metadata may still name
// it the leader of partitions that have since failed over.
TEST(RaftLivenessTest, FollowerMustCatchUpAgainAfterAContactGap) {
    boost::asio::io_context io;
    RaftNode node(1, {PeerInfo{2, "127.0.0.1", 1}}, io, 0, "");
    std::atomic<bool> release_second{false};
    std::atomic<int64_t> applied{0};
    node.setCommitCallback([&](const LogEntry& entry) {
        while (entry.index == 2 && !release_second.load()) {
            std::this_thread::sleep_for(5ms);
        }
        applied.store(entry.index);
    });
    node.start();

    // High terms: the started node campaigns on its own (its only peer is
    // unreachable) and keeps raising its term.
    auto entry = [](int64_t index, int64_t term) {
        LogEntry e;
        e.term = term;
        e.index = index;
        e.command_type = "metadata";
        return e;
    };
    AppendEntriesRequest first;
    first.term = 1000;
    first.leader_id = 2;
    first.prev_log_index = 0;
    first.prev_log_term = 0;
    first.entries = {entry(1, 1000)};
    first.leader_commit = 1;
    ASSERT_TRUE(node.handleAppendEntries(first).success);
    const auto deadline = std::chrono::steady_clock::now() + 2s;
    while (applied.load() < 1 && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(5ms);
    }
    EXPECT_TRUE(node.hasCurrentMetadata(5000));

    std::this_thread::sleep_for(1200ms);  // cut off from the leader
    AppendEntriesRequest second;
    second.term = 2000;
    second.leader_id = 2;
    second.prev_log_index = 1;
    second.prev_log_term = 1000;
    second.entries = {entry(2, 2000)};
    second.leader_commit = 2;
    ASSERT_TRUE(node.handleAppendEntries(second).success);
    // Contact is fresh (well within the lease), but entry 2 is not applied yet.
    EXPECT_FALSE(node.hasCurrentMetadata(5000));
    release_second.store(true);
    while (applied.load() < 2 && std::chrono::steady_clock::now() < deadline + 2s) {
        std::this_thread::sleep_for(5ms);
    }
    AppendEntriesRequest heartbeat = second;  // keep it a follower for the check
    heartbeat.prev_log_index = 2;
    heartbeat.prev_log_term = 2000;
    heartbeat.entries.clear();
    ASSERT_TRUE(node.handleAppendEntries(heartbeat).success);
    EXPECT_TRUE(node.hasCurrentMetadata(5000));
    node.stop();
}

// Regression (M8 divergence nemesis): with a frozen peer FIRST in the peer
// list, its pending vote/heartbeat future consumed the whole response window
// and the loop gave up before looking at the live peer's (already granted)
// vote — two live nodes out of three never elected a leader.
TEST(RaftLivenessTest, FrozenFirstPeerDoesNotBlockElection) {
    boost::asio::io_context hole_io;
    boost::asio::ip::tcp::acceptor hole(
        hole_io, boost::asio::ip::tcp::endpoint(boost::asio::ip::make_address("127.0.0.1"), 0));
    std::vector<std::unique_ptr<boost::asio::ip::tcp::socket>> held;
    std::atomic<bool> accepting{true};
    std::thread hole_thread([&] {
        while (accepting.load()) {
            auto sock = std::make_unique<boost::asio::ip::tcp::socket>(hole_io);
            boost::system::error_code ec;
            hole.accept(*sock, ec);
            if (ec) {
                break;
            }
            held.push_back(std::move(sock));  // never read, never answer
        }
    });
    const int hole_port = hole.local_endpoint().port();

    std::vector<int> ports{freePort(), freePort()};  // brokers 1 and 2
    std::vector<std::unique_ptr<NodeIo>> ios;
    std::vector<std::unique_ptr<RaftNode>> nodes;
    for (int i = 0; i < 2; ++i) {
        const int id = i + 1;
        std::vector<PeerInfo> peers{PeerInfo{0, "127.0.0.1", hole_port}};  // frozen, first
        peers.push_back(PeerInfo{3 - id, "127.0.0.1", ports[(3 - id) - 1]});
        ios.push_back(std::make_unique<NodeIo>());
        nodes.push_back(std::make_unique<RaftNode>(id, peers, ios.back()->io, ports[i], ""));
        nodes.back()->setCommitCallback([](const LogEntry&) {});
        nodes.back()->start();
    }
    const auto deadline = std::chrono::steady_clock::now() + 10s;
    bool elected = false;
    while (!elected && std::chrono::steady_clock::now() < deadline) {
        elected = nodes[0]->isLeader() || nodes[1]->isLeader();
        std::this_thread::sleep_for(20ms);
    }
    EXPECT_TRUE(elected);
    // A frozen first peer must not stretch the heartbeat cadence until the
    // remaining follower repeatedly times out and deposes the live leader.
    const int leader = nodes[0]->isLeader() ? 0 : 1;
    const auto term = nodes[leader]->currentTerm();
    const auto stable_until = std::chrono::steady_clock::now() + 4s;
    while (std::chrono::steady_clock::now() < stable_until) {
        EXPECT_EQ(nodes[leader]->currentTerm(), term);
        EXPECT_TRUE(nodes[leader]->isLeader());
        std::this_thread::sleep_for(100ms);
    }
    for (auto& n : nodes) {
        n->stop();
    }
    nodes.clear();
    ios.clear();
    accepting.store(false);
    hole.close();
    hole_io.stop();
    hole_thread.join();
}

// A vote reply can be overtaken by a newer leader's AppendEntries. The
// election thread must compare against the CURRENT term, under the state lock.
TEST(RaftLivenessTest, DelayedVoteReplyCannotRollBackANewerTerm) {
    NodeIo node_io;
    NodeIo peer_io;
    RaftTransport peer(peer_io.io, 1);
    const int peer_port = freePort();
    peer.start(peer_port);
    std::atomic<int64_t> requested_term{0};
    std::promise<void> release;
    auto released = release.get_future().share();
    peer.setRequestVoteHandler([&](const RequestVoteRequest& request) {
        requested_term.store(request.term);
        released.wait_for(2s);
        RequestVoteResponse response;
        response.term = request.term + 1;
        response.vote_granted = false;
        return response;
    });
    RaftNode node(0, {PeerInfo{1, "127.0.0.1", peer_port}}, node_io.io, freePort(), "");
    node.setCommitCallback([](const LogEntry&) {});
    node.start();
    const auto deadline = std::chrono::steady_clock::now() + 2s;
    while (requested_term.load() == 0 && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(1ms);
    }
    EXPECT_GT(requested_term.load(), 0);
    const auto newer_term = requested_term.load() + 10;
    AppendEntriesRequest heartbeat;
    heartbeat.term = newer_term;
    heartbeat.leader_id = 1;
    node.handleAppendEntries(heartbeat);
    release.set_value();
    const auto observe_until = std::chrono::steady_clock::now() + 100ms;
    while (std::chrono::steady_clock::now() < observe_until) {
        EXPECT_GE(node.currentTerm(), newer_term);
        std::this_thread::sleep_for(1ms);
    }
    node.stop();
    peer.stop();
}
