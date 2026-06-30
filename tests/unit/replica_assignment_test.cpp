// Phase B prerequisite #2: cross-broker replica assignment. Verifies that once
// the metadata store knows multiple brokers (seeded from raft.peers), creating a
// topic with replication factor > 1 spreads each partition's replicas across
// distinct brokers (leader = first replica, ISR = replicas). Single-broker /
// RF=1 keeps everything local. Drives the real MetadataStore::applyCreate path
// (no live Raft cluster needed), so it's deterministic.
#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <set>

#include "kawasan/broker/metadata_store.h"

namespace fs = std::filesystem;
using namespace kawasan::broker;
using kawasan::BrokerMetadata;

namespace {

std::string makeDir() {
    const auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
    auto p = fs::temp_directory_path() / ("kawasan-replasgn-" + std::to_string(ts));
    fs::create_directories(p);
    return p.string();
}

BrokerMetadata broker(kawasan::BrokerId id) {
    BrokerMetadata b;
    b.id = id;
    b.host = "127.0.0.1";
    b.port = 9092 + id;
    b.rack = std::nullopt;
    return b;
}

// A store that knows `n` brokers (ids 0..n-1), local = 0.
std::unique_ptr<MetadataStore> storeWithBrokers(const std::string& dir, int n) {
    auto store = std::make_unique<MetadataStore>(dir, "test-cluster", broker(0),
                                                 /*log_manager=*/nullptr);
    store->load();
    for (int i = 1; i < n; ++i) {
        store->registerBroker(broker(i));
    }
    return store;
}

TopicSpecification topicSpec(const std::string& name, int32_t partitions, int16_t rf) {
    TopicSpecification spec;
    spec.name = name;
    spec.num_partitions = partitions;
    spec.replication_factor = rf;
    return spec;
}

}  // namespace

TEST(ReplicaAssignmentTest, Rf3SpreadsAcrossThreeBrokers) {
    const auto dir = makeDir();
    auto store = storeWithBrokers(dir, 3);
    ASSERT_EQ(store->brokerCount(), 3u);

    auto result = store->applyCreate(topicSpec("t3", /*partitions=*/3, /*rf=*/3));
    ASSERT_EQ(result.error_code, kawasan::ErrorCode::NONE);

    auto md = store->describeTopics({"t3"});
    ASSERT_EQ(md.size(), 1u);
    ASSERT_EQ(md.front().partitions.size(), 3u);

    std::set<kawasan::BrokerId> all_leaders;
    for (const auto& p : md.front().partitions) {
        // Each partition has 3 distinct replicas spanning {0,1,2}.
        std::set<kawasan::BrokerId> replicas(p.replicas.begin(), p.replicas.end());
        EXPECT_EQ(replicas.size(), 3u) << "partition " << p.partition;
        EXPECT_EQ(replicas, (std::set<kawasan::BrokerId>{0, 1, 2}));
        // Leader is the first replica; ISR == replicas at creation.
        EXPECT_EQ(p.leader, p.replicas.front());
        EXPECT_EQ(p.isr, p.replicas);
        all_leaders.insert(p.leader);
    }
    // Round-robin should not pin every partition to the same leader.
    EXPECT_GT(all_leaders.size(), 1u);

    fs::remove_all(dir);
}

TEST(ReplicaAssignmentTest, Rf2UsesTwoDistinctBrokers) {
    const auto dir = makeDir();
    auto store = storeWithBrokers(dir, 3);

    auto result = store->applyCreate(topicSpec("t2", /*partitions=*/3, /*rf=*/2));
    ASSERT_EQ(result.error_code, kawasan::ErrorCode::NONE);

    auto md = store->describeTopics({"t2"});
    for (const auto& p : md.front().partitions) {
        std::set<kawasan::BrokerId> replicas(p.replicas.begin(), p.replicas.end());
        EXPECT_EQ(replicas.size(), 2u) << "partition " << p.partition;
        EXPECT_EQ(p.leader, p.replicas.front());
    }
    fs::remove_all(dir);
}

TEST(ReplicaAssignmentTest, Rf1KeepsEverythingLocal) {
    const auto dir = makeDir();
    auto store = storeWithBrokers(dir, 3);

    auto result = store->applyCreate(topicSpec("t1", /*partitions=*/3, /*rf=*/1));
    ASSERT_EQ(result.error_code, kawasan::ErrorCode::NONE);

    auto md = store->describeTopics({"t1"});
    for (const auto& p : md.front().partitions) {
        ASSERT_EQ(p.replicas.size(), 1u);
        EXPECT_EQ(p.replicas.front(), 0);  // local broker
        EXPECT_EQ(p.leader, 0);
    }
    fs::remove_all(dir);
}

TEST(ReplicaAssignmentTest, RfExceedingBrokerCountIsRejected) {
    const auto dir = makeDir();
    auto store = storeWithBrokers(dir, 2);  // only 2 brokers

    auto result = store->applyCreate(topicSpec("toobig", /*partitions=*/1, /*rf=*/3));
    EXPECT_NE(result.error_code, kawasan::ErrorCode::NONE);
    fs::remove_all(dir);
}

TEST(ReplicaAssignmentTest, SingleBrokerStaysLocalEvenWithHigherRfRequest) {
    const auto dir = makeDir();
    auto store = storeWithBrokers(dir, 1);  // single-node

    // RF=1 is the only valid request on a single-node store.
    auto result = store->applyCreate(topicSpec("solo", /*partitions=*/2, /*rf=*/1));
    ASSERT_EQ(result.error_code, kawasan::ErrorCode::NONE);
    auto md = store->describeTopics({"solo"});
    for (const auto& p : md.front().partitions) {
        ASSERT_EQ(p.replicas.size(), 1u);
        EXPECT_EQ(p.replicas.front(), 0);
    }
    fs::remove_all(dir);
}
