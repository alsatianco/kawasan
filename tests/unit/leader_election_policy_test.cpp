// M8-C: the controller's failover policy as a pure function of (metadata, dead
// brokers, unclean flag). The sweep plumbing just applies these decisions.
#include "kawasan/broker/leader_election_policy.h"

#include <gtest/gtest.h>

using kawasan::BrokerId;
using kawasan::PartitionMetadata;
using kawasan::TopicMetadata;
using kawasan::broker::computeLeadershipChanges;
using kawasan::broker::PartitionLeadershipChange;

namespace {

PartitionMetadata pm(int32_t partition, BrokerId leader, std::vector<BrokerId> replicas,
                     std::vector<BrokerId> isr) {
    PartitionMetadata p;
    p.partition = partition;
    p.leader = leader;
    p.replicas = std::move(replicas);
    p.isr = std::move(isr);
    return p;
}

std::vector<TopicMetadata> topic(std::vector<PartitionMetadata> partitions) {
    TopicMetadata t;
    t.name = "t";
    t.partitions = std::move(partitions);
    return {t};
}

}  // namespace

TEST(LeaderElectionPolicyTest, NoDeadBrokersNoChanges) {
    auto changes = computeLeadershipChanges(topic({pm(0, 0, {0, 1, 2}, {0, 1, 2})}), {}, false);
    EXPECT_TRUE(changes.empty());
}

TEST(LeaderElectionPolicyTest, DeadLeaderFailsOverToFirstLiveIsrMemberInAssignmentOrder) {
    // Assignment order {0, 2, 1}: 2 is preferred over 1 when 0 dies.
    auto changes = computeLeadershipChanges(topic({pm(0, 0, {0, 2, 1}, {0, 1, 2})}), {0}, false);
    ASSERT_EQ(changes.size(), 1u);
    EXPECT_EQ(changes[0].topic, "t");
    EXPECT_EQ(changes[0].partition, 0);
    EXPECT_EQ(changes[0].new_leader, std::optional<BrokerId>(2));
    EXPECT_EQ(changes[0].new_isr, (std::optional<std::vector<BrokerId>>{{2, 1}}));
    EXPECT_FALSE(changes[0].unclean);
}

TEST(LeaderElectionPolicyTest, NeverElectsOutsideIsrWhenUncleanDisabled) {
    // Replica 2 is alive but out of sync; the only ISR member (0) is dead.
    auto changes = computeLeadershipChanges(topic({pm(0, 0, {0, 1, 2}, {0})}), {0, 1}, false);
    ASSERT_EQ(changes.size(), 1u);
    EXPECT_EQ(changes[0].new_leader, std::optional<BrokerId>(-1));  // offline
    EXPECT_FALSE(changes[0].new_isr.has_value());                  // ISR kept
    EXPECT_FALSE(changes[0].unclean);
}

TEST(LeaderElectionPolicyTest, UncleanElectsFirstLiveReplicaWhenEnabled) {
    auto changes = computeLeadershipChanges(topic({pm(0, 0, {0, 1, 2}, {0})}), {0, 1}, true);
    ASSERT_EQ(changes.size(), 1u);
    EXPECT_EQ(changes[0].new_leader, std::optional<BrokerId>(2));
    EXPECT_EQ(changes[0].new_isr, (std::optional<std::vector<BrokerId>>{{2}}));
    EXPECT_TRUE(changes[0].unclean);
}

TEST(LeaderElectionPolicyTest, Rf1WithDeadReplicaGoesOfflineOnce) {
    auto changes = computeLeadershipChanges(topic({pm(0, 1, {1}, {1})}), {1}, true);
    ASSERT_EQ(changes.size(), 1u);
    EXPECT_EQ(changes[0].new_leader, std::optional<BrokerId>(-1));
    // Already offline and still no candidate: nothing more to do.
    EXPECT_TRUE(computeLeadershipChanges(topic({pm(0, -1, {1}, {1})}), {1}, true).empty());
}

TEST(LeaderElectionPolicyTest, OfflinePartitionRecoversWhenAnIsrMemberReturns) {
    auto changes = computeLeadershipChanges(topic({pm(0, -1, {0, 1, 2}, {0, 1})}), {0}, false);
    ASSERT_EQ(changes.size(), 1u);
    EXPECT_EQ(changes[0].new_leader, std::optional<BrokerId>(1));
    EXPECT_EQ(changes[0].new_isr, (std::optional<std::vector<BrokerId>>{{1}}));
}

TEST(LeaderElectionPolicyTest, DeadFollowerIsShrunkFromIsrWithoutElection) {
    auto changes = computeLeadershipChanges(topic({pm(0, 0, {0, 1, 2}, {0, 1, 2})}), {2}, false);
    ASSERT_EQ(changes.size(), 1u);
    EXPECT_FALSE(changes[0].new_leader.has_value());
    EXPECT_EQ(changes[0].new_isr, (std::optional<std::vector<BrokerId>>{{0, 1}}));
}

TEST(LeaderElectionPolicyTest, IsrKeepsItsOwnOrderWhenOnlyShrinking) {
    auto changes = computeLeadershipChanges(topic({pm(0, 1, {0, 1, 2}, {1, 2, 0})}), {0}, false);
    ASSERT_EQ(changes.size(), 1u);
    EXPECT_EQ(changes[0].new_isr, (std::optional<std::vector<BrokerId>>{{1, 2}}));
}

TEST(LeaderElectionPolicyTest, ScansEveryTopicAndPartition) {
    TopicMetadata a;
    a.name = "a";
    a.partitions = {pm(0, 0, {0, 1}, {0, 1}), pm(1, 1, {1, 0}, {1, 0})};
    TopicMetadata b;
    b.name = "b";
    b.partitions = {pm(0, 2, {2}, {2})};
    auto changes = computeLeadershipChanges({a, b}, {0}, false);
    ASSERT_EQ(changes.size(), 2u);
    EXPECT_EQ(changes[0].topic, "a");
    EXPECT_EQ(changes[0].partition, 0);
    EXPECT_EQ(changes[0].new_leader, std::optional<BrokerId>(1));
    EXPECT_EQ(changes[1].topic, "a");
    EXPECT_EQ(changes[1].partition, 1);
    EXPECT_FALSE(changes[1].new_leader.has_value());
    EXPECT_EQ(changes[1].new_isr, (std::optional<std::vector<BrokerId>>{{1}}));
}
