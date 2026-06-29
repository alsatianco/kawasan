// Regression tests for the Phase B Raft election fixes.
//
// Two bugs prevented a multi-broker cluster from ever electing a leader:
//   1. startElection held log_mutex_ across the RPC send + response wait, so a
//      peer's handleRequestVote/handleAppendEntries (which also take log_mutex_)
//      blocked for the whole election window — no node could grant a vote.
//   2. handleRequestVote/handleAppendEntries captured response.term BEFORE
//      advancing to the candidate's higher term, so a granted vote echoed the
//      peer's stale (lower) term; the candidate's `response.term == election_term`
//      check then rejected the vote and the cluster spun electing forever.
//
// These tests lock in fix #2 (directly observable on a single node). Fix #1 is
// covered end-to-end by scripts/tests/cluster_harness.sh, which now elects a
// leader in term 1.
#include <gtest/gtest.h>

#include <boost/asio/io_context.hpp>

#include "kawasan/raft/raft_node.h"

using namespace kawasan::raft;

namespace {
// Construct an unstarted single node (no peers, ephemeral Raft port, in-memory
// state). handleRequestVote/handleAppendEntries are synchronous and need no
// running io_context or started election thread.
std::unique_ptr<RaftNode> makeNode(boost::asio::io_context& io) {
    return std::make_unique<RaftNode>(/*id=*/1, std::vector<PeerInfo>{}, io,
                                      /*raft_port=*/0, /*data_dir=*/"");
}
}  // namespace

TEST(RaftElectionTest, GrantedVoteReportsAdvancedTerm) {
    boost::asio::io_context io;
    auto node = makeNode(io);

    RequestVoteRequest req;
    req.term = 5;  // higher than the node's initial term (0)
    req.candidate_id = 2;
    req.last_log_index = 0;
    req.last_log_term = 0;

    auto resp = node->handleRequestVote(req);
    EXPECT_TRUE(resp.vote_granted);
    // The load-bearing assertion: the response term is the advanced term (5),
    // not the stale pre-advance term (0). With the bug, this returned 0 and the
    // candidate discarded the vote.
    EXPECT_EQ(resp.term, 5);
}

TEST(RaftElectionTest, AlreadyVotedRejectsSecondCandidateSameTerm) {
    boost::asio::io_context io;
    auto node = makeNode(io);

    RequestVoteRequest first;
    first.term = 3;
    first.candidate_id = 2;
    auto r1 = node->handleRequestVote(first);
    EXPECT_TRUE(r1.vote_granted);
    EXPECT_EQ(r1.term, 3);

    // Same term, different candidate -> already voted -> denied, but the term is
    // still reported correctly.
    RequestVoteRequest second;
    second.term = 3;
    second.candidate_id = 7;
    auto r2 = node->handleRequestVote(second);
    EXPECT_FALSE(r2.vote_granted);
    EXPECT_EQ(r2.term, 3);
}

TEST(RaftElectionTest, StaleTermRequestIsRejectedWithCurrentTerm) {
    boost::asio::io_context io;
    auto node = makeNode(io);

    // Advance the node to term 5 via a higher-term vote request.
    RequestVoteRequest advance;
    advance.term = 5;
    advance.candidate_id = 2;
    (void)node->handleRequestVote(advance);

    // A stale (lower-term) request is rejected and the response reports the
    // node's current term so the stale candidate steps down.
    RequestVoteRequest stale;
    stale.term = 2;
    stale.candidate_id = 9;
    auto resp = node->handleRequestVote(stale);
    EXPECT_FALSE(resp.vote_granted);
    EXPECT_EQ(resp.term, 5);
}

TEST(RaftElectionTest, AppendEntriesReportsAdvancedTerm) {
    boost::asio::io_context io;
    auto node = makeNode(io);

    AppendEntriesRequest req;
    req.term = 4;  // a new leader at term 4
    req.leader_id = 2;
    req.prev_log_index = 0;
    req.prev_log_term = 0;
    req.leader_commit = 0;

    auto resp = node->handleAppendEntries(req);
    // The follower accepts and reports the advanced term (4), not the stale 0.
    EXPECT_EQ(resp.term, 4);
    EXPECT_TRUE(resp.success);
}
