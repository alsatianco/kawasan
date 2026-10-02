// EX-12: GroupCoordinator protocol-echo + rebalance-timeout tests.
//
// Covers the two fixes that unblock distributed Kafka Connect:
//   1. JoinGroup/SyncGroup responses must echo protocol_type/protocol_name
//      (v7+/v5+). Connect's leader-side performAssignment throws on a null
//      protocol name, so the echo is what lets Connect advance to SyncGroup.
//   2. A rebalance that overruns its deadline (stalled leader) must be
//      force-recovered so a multi-worker group can't hang forever.
#include "kawasan/broker/group_coordinator.h"

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>

#include "kawasan/broker/offset_manager.h"
#include "kawasan/protocol/describe_groups_request.h"
#include "kawasan/protocol/heartbeat_request.h"
#include "kawasan/protocol/join_group_request.h"
#include "kawasan/protocol/sync_group_request.h"

using kawasan::ErrorCode;
using kawasan::broker::GroupCoordinator;
using kawasan::broker::OffsetManager;
namespace protocol = kawasan::protocol;
namespace fs = std::filesystem;

namespace {

class GroupCoordinatorTest : public ::testing::Test {
protected:
    void SetUp() override {
        const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
        db_path_ =
            (fs::temp_directory_path() / ("kawasan-gc-test-" + std::to_string(stamp))).string();
        fs::create_directories(db_path_);
        offset_mgr_ = std::make_shared<OffsetManager>(db_path_);
        coordinator_ = std::make_unique<GroupCoordinator>(offset_mgr_);
    }

    void TearDown() override {
        coordinator_.reset();
        offset_mgr_.reset();
        std::error_code ec;
        fs::remove_all(db_path_, ec);
    }

    // Build a JoinGroup request that admits immediately (non-empty
    // protocol_type + protocols => legacy admit-on-first-join path).
    protocol::JoinGroupRequest makeJoin(const std::string& group, const std::string& member_id,
                                        const std::string& proto_type,
                                        const std::string& proto_name,
                                        int32_t rebalance_ms = 60000) {
        protocol::JoinGroupRequest req;
        req.setGroupId(group);
        req.setMemberId(member_id);
        req.setProtocolType(proto_type);
        req.setSessionTimeoutMs(30000);
        req.setRebalanceTimeoutMs(rebalance_ms);
        protocol::JoinGroupRequest::GroupProtocol p;
        p.name = proto_name;
        p.metadata = {0x01, 0x02, 0x03};
        req.setGroupProtocols({p});
        return req;
    }

    protocol::DescribeGroupsResponse::Group describe(const std::string& group) {
        auto groups = coordinator_->describeGroups({group});
        EXPECT_EQ(groups.size(), 1u);
        return groups.empty() ? protocol::DescribeGroupsResponse::Group{} : groups.front();
    }

    std::string db_path_;
    std::shared_ptr<OffsetManager> offset_mgr_;
    std::unique_ptr<GroupCoordinator> coordinator_;
};

// Fix 1a: JoinGroup result carries protocol_type + protocol_name so the
// broker can echo them in the v7+ response.
TEST_F(GroupCoordinatorTest, JoinGroupEchoesProtocolType) {
    auto result =
        coordinator_->handleJoinGroup(makeJoin("connect-cluster", "", "connect", "sessioned"));

    EXPECT_EQ(result.error, ErrorCode::NONE);
    EXPECT_EQ(result.protocol_type, "connect");
    EXPECT_EQ(result.protocol_name, "sessioned");
    EXPECT_FALSE(result.member_id.empty());
    EXPECT_EQ(result.leader_id, result.member_id);  // sole member is leader
}

// Fix 1b: SyncGroup result carries protocol_type + protocol_name (v5+).
TEST_F(GroupCoordinatorTest, SyncGroupEchoesProtocolFields) {
    auto join =
        coordinator_->handleJoinGroup(makeJoin("connect-cluster", "", "connect", "sessioned"));
    ASSERT_EQ(join.error, ErrorCode::NONE);

    protocol::SyncGroupRequest sync;
    sync.setGroupId("connect-cluster");
    sync.setGenerationId(join.generation_id);
    sync.setMemberId(join.member_id);
    protocol::SyncGroupRequest::Assignment a;
    a.member_id = join.member_id;
    a.assignment = {0xAA, 0xBB};
    sync.setAssignments({a});

    auto result = coordinator_->handleSyncGroup(sync);
    EXPECT_EQ(result.error, ErrorCode::NONE);
    EXPECT_EQ(result.protocol_type, "connect");
    EXPECT_EQ(result.protocol_name, "sessioned");
    EXPECT_EQ(result.assignment, (std::vector<uint8_t>{0xAA, 0xBB}));
}

// Fix 2: a rebalance that overruns its deadline evicts the stalled leader
// and forces a fresh rebalance, so the surviving member can take over.
TEST_F(GroupCoordinatorTest, RebalanceTimeoutEvictsStalledLeader) {
    // Two members join with a 1s rebalance timeout; neither SyncGroups.
    auto j1 = coordinator_->handleJoinGroup(
        makeJoin("g-stall", "", "connect", "sessioned", /*rebalance_ms=*/1000));
    ASSERT_EQ(j1.error, ErrorCode::NONE);
    const std::string leader = j1.member_id;

    auto j2 = coordinator_->handleJoinGroup(
        makeJoin("g-stall", "", "connect", "sessioned", /*rebalance_ms=*/1000));
    ASSERT_EQ(j2.error, ErrorCode::NONE);
    const std::string follower = j2.member_id;
    ASSERT_NE(leader, follower);
    ASSERT_EQ(describe("g-stall").members.size(), 2u);

    // Within the deadline: no recovery.
    coordinator_->checkRebalanceTimeouts();
    EXPECT_EQ(describe("g-stall").members.size(), 2u);

    // Exceed the 1s rebalance timeout, then run the check.
    std::this_thread::sleep_for(std::chrono::milliseconds(1200));
    coordinator_->checkRebalanceTimeouts();

    auto desc = describe("g-stall");
    EXPECT_EQ(desc.group_state, "PreparingRebalance");
    ASSERT_EQ(desc.members.size(), 1u) << "stalled leader should be evicted";
    EXPECT_EQ(desc.members.front().member_id, follower)
        << "the surviving member is the non-leader follower";

    // The surviving follower re-joins and is elected the new leader at a
    // bumped generation — proving recovery completed, not just eviction.
    auto rejoin =
        coordinator_->handleJoinGroup(makeJoin("g-stall", follower, "connect", "sessioned", 1000));
    EXPECT_EQ(rejoin.error, ErrorCode::NONE);
    EXPECT_EQ(rejoin.leader_id, follower);
    EXPECT_GT(rejoin.generation_id, j1.generation_id);
}

// Guard: a healthy rebalance within its deadline is untouched.
TEST_F(GroupCoordinatorTest, RebalanceWithinDeadlineNotDisturbed) {
    auto j1 = coordinator_->handleJoinGroup(
        makeJoin("g-ok", "", "connect", "sessioned", /*rebalance_ms=*/60000));
    ASSERT_EQ(j1.error, ErrorCode::NONE);

    coordinator_->checkRebalanceTimeouts();  // 60s window — nothing should change

    auto desc = describe("g-ok");
    EXPECT_EQ(desc.members.size(), 1u);
    EXPECT_NE(desc.group_state, "Empty");
    EXPECT_NE(desc.group_state, "PreparingRebalance");
}

}  // namespace

TEST_F(GroupCoordinatorTest, JoiningDuringSyncRejectsAnIncompleteAssignment) {
    const auto first =
        coordinator_->handleJoinGroup(makeJoin("join-race", "", "consumer", "range"));
    const auto second =
        coordinator_->handleJoinGroup(makeJoin("join-race", "", "consumer", "range"));
    protocol::SyncGroupRequest sync;
    sync.setGroupId("join-race");
    sync.setMemberId(first.member_id);
    sync.setGenerationId(first.generation_id);
    sync.setAssignments({{first.member_id, {0xAA}}});
    EXPECT_EQ(coordinator_->handleSyncGroup(sync).error, ErrorCode::ILLEGAL_GENERATION);

    const auto rejoin =
        coordinator_->handleJoinGroup(makeJoin("join-race", first.member_id, "consumer", "range"));
    EXPECT_GT(rejoin.generation_id, first.generation_id);
    ASSERT_EQ(rejoin.members.size(), 2);
    sync.setGenerationId(rejoin.generation_id);
    sync.setAssignments({{first.member_id, {0xAA}}, {second.member_id, {0xBB}}});
    EXPECT_EQ(coordinator_->handleSyncGroup(sync).error, ErrorCode::NONE);

    // A same-subscription rejoin must preserve the stable generation.
    const auto existing =
        coordinator_->handleJoinGroup(makeJoin("join-race", second.member_id, "consumer", "range"));
    EXPECT_EQ(existing.generation_id, rejoin.generation_id);
    sync.setMemberId(second.member_id);
    sync.setAssignments({});
    const auto follower = coordinator_->handleSyncGroup(sync);
    EXPECT_EQ(follower.error, ErrorCode::NONE);
    EXPECT_EQ(follower.assignment, (std::vector<uint8_t>{0xBB}));
}

// M9 I4 finding: transactional/manual consumers can commit offsets without
// JoinGroup. Durable offsets must remain fetchable without an in-memory group.
TEST_F(GroupCoordinatorTest, FetchesDurableOffsetsWithoutGroupMembershipAfterRestart) {
    offset_mgr_->commitOffset("manual-txn-group", "input", 0, 7, "txn-checkpoint");
    coordinator_.reset();
    offset_mgr_.reset();
    offset_mgr_ = std::make_shared<OffsetManager>(db_path_);
    coordinator_ = std::make_unique<GroupCoordinator>(offset_mgr_);

    protocol::OffsetFetchRequest request;
    request.setGroupId("manual-txn-group");
    request.setTopics({{"input", {{0}, {1}}}});
    ErrorCode error = ErrorCode::NONE;
    auto result = coordinator_->handleOffsetFetch(request, error);
    ASSERT_EQ(error, ErrorCode::NONE);
    ASSERT_EQ(result.size(), 1u);
    ASSERT_EQ(result[0].partitions.size(), 2u);
    EXPECT_EQ(result[0].partitions[0].offset, 7);
    EXPECT_EQ(result[0].partitions[0].metadata, "txn-checkpoint");
    EXPECT_EQ(result[0].partitions[1].offset, -1);
}
