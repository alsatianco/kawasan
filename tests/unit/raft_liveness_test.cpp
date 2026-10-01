// M8-B: Raft-derived broker liveness. The Raft leader (== controller) records
// when each peer last answered an AppendEntries; a stopped peer's age grows
// while live peers stay fresh, and a newly elected leader grants every peer a
// full grace window (ages restart near 0) instead of treating them as dead.
// Runs three real RaftNodes in-process over loopback.
#include <gtest/gtest.h>

#include <boost/asio.hpp>
#include <chrono>
#include <memory>
#include <thread>
#include <vector>

#include "kawasan/common/logger.h"
#include "kawasan/raft/raft_node.h"

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
    EXPECT_TRUE(node.peerAckAgesMs().empty());  // not started, not leader
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
