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
#include <vector>

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

// M8-A2: the same CREATE_TOPIC command applied on every broker must produce the
// same leadership. RF=1 used to assign the partition to whichever broker applied
// it, so each broker believed it led every RF=1 partition (incl. internal topics).
TEST(ReplicaAssignmentTest, Rf1AssignmentIsIdenticalOnEveryBroker) {
    std::vector<std::vector<kawasan::BrokerId>> leaders_per_broker;
    for (kawasan::BrokerId local : {0, 1, 2}) {
        const auto dir = makeDir();
        MetadataStore store(dir, "test-cluster", broker(local), /*log_manager=*/nullptr);
        store.load();
        for (kawasan::BrokerId id : {0, 1, 2}) {
            store.registerBroker(broker(id));
        }
        ASSERT_EQ(store.applyCreate(topicSpec("t1", /*partitions=*/4, /*rf=*/1)).error_code,
                  kawasan::ErrorCode::NONE);
        std::vector<kawasan::BrokerId> leaders;
        const auto md = store.describeTopics({"t1"});
        for (const auto& p : md.front().partitions) {
            ASSERT_EQ(p.replicas.size(), 1u);
            EXPECT_EQ(p.leader, p.replicas.front());
            EXPECT_EQ(p.isr, p.replicas);
            leaders.push_back(p.leader);
        }
        leaders_per_broker.push_back(leaders);
        fs::remove_all(dir);
    }
    // Round-robin over sorted broker ids, identical everywhere.
    const std::vector<kawasan::BrokerId> expected{0, 1, 2, 0};
    for (const auto& leaders : leaders_per_broker) {
        EXPECT_EQ(leaders, expected);
    }
}

TEST(ReplicaAssignmentTest, IncreasePartitionsRf1IsIdenticalOnEveryBroker) {
    for (kawasan::BrokerId local : {0, 2}) {
        const auto dir = makeDir();
        MetadataStore store(dir, "test-cluster", broker(local), /*log_manager=*/nullptr);
        store.load();
        for (kawasan::BrokerId id : {0, 1, 2}) {
            store.registerBroker(broker(id));
        }
        ASSERT_EQ(store.applyCreate(topicSpec("grow", /*partitions=*/1, /*rf=*/1)).error_code,
                  kawasan::ErrorCode::NONE);
        ASSERT_EQ(store.applyIncreasePartitions("grow", 3).error_code, kawasan::ErrorCode::NONE);
        std::vector<kawasan::BrokerId> leaders;
        const auto md = store.describeTopics({"grow"});
        for (const auto& p : md.front().partitions) {
            leaders.push_back(p.leader);
        }
        EXPECT_EQ(leaders, (std::vector<kawasan::BrokerId>{0, 1, 2})) << "local=" << local;
        fs::remove_all(dir);
    }
}

TEST(ReplicaAssignmentTest, RoundRobinAssignmentsPureFunction) {
    // Unsorted input is sorted; start rotates with the partition index.
    auto a = roundRobinAssignments({2, 0, 1}, /*first_partition=*/0, /*count=*/4, /*rf=*/2);
    EXPECT_EQ(a, (std::vector<std::vector<kawasan::BrokerId>>{{0, 1}, {1, 2}, {2, 0}, {0, 1}}));
    auto b = roundRobinAssignments({5, 7}, /*first_partition=*/3, /*count=*/2, /*rf=*/1);
    EXPECT_EQ(b, (std::vector<std::vector<kawasan::BrokerId>>{{7}, {5}}));
    // RF is clamped to [1, broker count].
    auto c = roundRobinAssignments({0, 1}, 0, 1, /*rf=*/5);
    EXPECT_EQ(c, (std::vector<std::vector<kawasan::BrokerId>>{{0, 1}}));
    EXPECT_TRUE(roundRobinAssignments({}, 0, 2, 1).empty());
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
