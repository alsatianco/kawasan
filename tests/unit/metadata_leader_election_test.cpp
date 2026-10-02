// M7: MetadataStore::applyUpdateLeader — the leader-election command behind
// ElectLeaders. Electing a new leader must set the partition leader (only to an
// assigned replica) and bump the leader epoch (KIP-101), and reject invalid
// requests. Deterministic, no cluster required.
#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <optional>
#include <string>

#include "kawasan/broker/metadata_store.h"
#include "kawasan/broker/metadata_types.h"
#include "kawasan/common/logger.h"
#include "kawasan/common/types.h"

namespace fs = std::filesystem;
using kawasan::BrokerMetadata;
using kawasan::ErrorCode;
using kawasan::broker::MetadataStore;
using kawasan::broker::TopicSpecification;

namespace {

void ensureLogger() {
    static bool init = false;
    if (!init) {
        kawasan::Logger::init("warn");
        init = true;
    }
}

std::string makeDir() {
    const auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
    auto p = fs::temp_directory_path() / ("kawasan-elect-" + std::to_string(ts));
    fs::create_directories(p);
    return p.string();
}

class MetadataLeaderElectionTest : public ::testing::Test {
protected:
    void SetUp() override {
        ensureLogger();
        dir_ = makeDir();
        store_ = std::make_unique<MetadataStore>(
            dir_, "cid", BrokerMetadata{0, "127.0.0.1", 9092, std::nullopt}, nullptr);
        store_->load();
        store_->registerBroker(BrokerMetadata{1, "127.0.0.1", 9192, std::nullopt});
        store_->registerBroker(BrokerMetadata{2, "127.0.0.1", 9292, std::nullopt});
        TopicSpecification spec;
        spec.name = "t";
        spec.num_partitions = 1;
        spec.replication_factor = 3;
        ASSERT_EQ(store_->applyCreate(spec).error_code, ErrorCode::NONE);
    }
    void TearDown() override {
        store_.reset();
        if (fs::exists(dir_))
            fs::remove_all(dir_);
    }

    kawasan::PartitionMetadata partition0() {
        auto md = store_->describeTopics({"t"});
        EXPECT_EQ(md.size(), 1u);
        return md.front().partitions.front();
    }

    std::string dir_;
    std::unique_ptr<MetadataStore> store_;
};

}  // namespace

TEST_F(MetadataLeaderElectionTest, ElectsNewLeaderAndBumpsEpoch) {
    const auto before = partition0();
    ASSERT_EQ(before.leader_epoch, 0);
    ASSERT_EQ(before.replicas.size(), 3u);

    // Pick an assigned replica that is not the current leader.
    kawasan::BrokerId new_leader = before.leader;
    for (auto r : before.replicas) {
        if (r != before.leader) {
            new_leader = r;
            break;
        }
    }
    ASSERT_NE(new_leader, before.leader);

    EXPECT_EQ(store_->applyUpdateLeader("t", 0, new_leader).error_code, ErrorCode::NONE);

    const auto after = partition0();
    EXPECT_EQ(after.leader, new_leader) << "leader must change to the elected replica";
    EXPECT_EQ(after.leader_epoch, before.leader_epoch + 1) << "leader epoch must bump (KIP-101)";
    // Replicas set is unchanged by an election.
    EXPECT_EQ(after.replicas, before.replicas);
}

TEST_F(MetadataLeaderElectionTest, RejectsNonReplicaLeader) {
    EXPECT_EQ(store_->applyUpdateLeader("t", 0, /*broker=*/999).error_code,
              ErrorCode::INVALID_REPLICA_ASSIGNMENT);
    // Unchanged.
    EXPECT_EQ(partition0().leader_epoch, 0);
}

TEST_F(MetadataLeaderElectionTest, RejectsUnknownTopicAndPartition) {
    EXPECT_EQ(store_->applyUpdateLeader("no-such-topic", 0, 0).error_code,
              ErrorCode::UNKNOWN_TOPIC_OR_PARTITION);
    EXPECT_EQ(store_->applyUpdateLeader("t", 99, 0).error_code,
              ErrorCode::UNKNOWN_TOPIC_OR_PARTITION);
}

TEST_F(MetadataLeaderElectionTest, ElectionSurvivesReload) {
    const auto before = partition0();
    kawasan::BrokerId new_leader = before.replicas.back();
    if (new_leader == before.leader)
        new_leader = before.replicas.front();
    ASSERT_EQ(store_->applyUpdateLeader("t", 0, new_leader).error_code, ErrorCode::NONE);
    const int32_t epoch_after = partition0().leader_epoch;

    // Reload from disk: the elected leader + bumped epoch must persist.
    store_ = std::make_unique<MetadataStore>(
        dir_, "cid", BrokerMetadata{0, "127.0.0.1", 9092, std::nullopt}, nullptr);
    store_->load();
    const auto reloaded = partition0();
    EXPECT_EQ(reloaded.leader, new_leader);
    EXPECT_EQ(reloaded.leader_epoch, epoch_after);
}

// M8-C: leader = -1 marks a partition offline (no eligible leader). It bumps the
// epoch like any election, persists, and a later election brings it back.
TEST_F(MetadataLeaderElectionTest, OfflineMarkerAndRecovery) {
    const auto before = partition0();
    ASSERT_EQ(store_->applyUpdateLeader("t", 0, /*offline=*/-1).error_code, ErrorCode::NONE);
    auto offline = partition0();
    EXPECT_EQ(offline.leader, -1);
    EXPECT_EQ(offline.leader_epoch, before.leader_epoch + 1);
    EXPECT_EQ(offline.isr, before.isr);  // ISR kept: its members stay eligible

    store_ = std::make_unique<MetadataStore>(
        dir_, "cid", BrokerMetadata{0, "127.0.0.1", 9092, std::nullopt}, nullptr);
    store_->load();
    EXPECT_EQ(partition0().leader, -1);

    ASSERT_EQ(store_->applyUpdateLeader("t", 0, before.replicas.back()).error_code,
              ErrorCode::NONE);
    EXPECT_EQ(partition0().leader, before.replicas.back());
    EXPECT_EQ(partition0().leader_epoch, before.leader_epoch + 2);
}

TEST_F(MetadataLeaderElectionTest, RejectsOtherNegativeLeaders) {
    EXPECT_EQ(store_->applyUpdateLeader("t", 0, -2).error_code,
              ErrorCode::INVALID_REPLICA_ASSIGNMENT);
}

// A controller sweep can be delayed behind other Raft commands. An old ISR
// decision must not resurrect a lagging replica, or elect that replica later.
TEST_F(MetadataLeaderElectionTest, RejectsStaleIsrAndElectionDecisionsAtApply) {
    const auto before = partition0();
    const auto lagging = before.replicas.back();
    std::vector<kawasan::BrokerId> caught_up = before.isr;
    caught_up.erase(std::remove(caught_up.begin(), caught_up.end(), lagging), caught_up.end());
    ASSERT_EQ(store_->applyUpdateISR("t", 0, caught_up, before.partition_epoch).error_code,
              ErrorCode::NONE);
    const auto shrunk = partition0();
    EXPECT_EQ(store_->applyUpdateISR("t", 0, before.isr, before.partition_epoch).error_code,
              ErrorCode::INVALID_UPDATE_VERSION);
    EXPECT_EQ(store_->applyUpdateLeader("t", 0, lagging, before.partition_epoch).error_code,
              ErrorCode::INVALID_UPDATE_VERSION);
    EXPECT_EQ(partition0().isr, caught_up);
    EXPECT_EQ(partition0().leader, before.leader);
    EXPECT_EQ(shrunk.partition_epoch, before.partition_epoch + 1);
}

TEST_F(MetadataLeaderElectionTest, ElectionFencesOldIsrAndPersistsPartitionVersion) {
    const auto before = partition0();
    ASSERT_EQ(store_->applyUpdateLeader("t", 0, before.replicas.back(), before.partition_epoch)
                  .error_code,
              ErrorCode::NONE);
    EXPECT_EQ(store_->applyUpdateISR("t", 0, {before.leader}, before.partition_epoch).error_code,
              ErrorCode::INVALID_UPDATE_VERSION);
    store_ = std::make_unique<MetadataStore>(
        dir_, "cid", BrokerMetadata{0, "127.0.0.1", 9092, std::nullopt}, nullptr);
    store_->load();
    EXPECT_EQ(partition0().partition_epoch, before.partition_epoch + 1);
    EXPECT_EQ(partition0().isr, before.isr);
}
