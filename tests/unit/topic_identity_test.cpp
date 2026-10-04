#include <gtest/gtest.h>
#include <rocksdb/db.h>

#include <boost/asio.hpp>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <memory>
#include <nlohmann/json.hpp>

#include "kawasan/broker/metadata_controller.h"
#include "kawasan/common/rocksdb_compat.h"

using namespace kawasan;
using namespace kawasan::broker;
namespace fs = std::filesystem;
namespace {
struct Workspace {
    fs::path path = fs::temp_directory_path() /
                    ("kawasan-topic-id-" +
                     std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    Workspace() { fs::create_directories(path); }
    ~Workspace() { fs::remove_all(path); }
};
TopicSpecification topic(const std::string& name) {
    TopicSpecification spec;
    spec.name = name;
    spec.num_partitions = 1;
    spec.replication_factor = 1;
    return spec;
}
BrokerMetadata testBroker(BrokerId id) {
    return {id, "localhost", 9092 + id, std::nullopt};
}
struct Node {
    boost::asio::io_context io;
    std::unique_ptr<raft::RaftNode> raft_node;
    std::unique_ptr<MetadataController> controller;
    Node(const fs::path& path, BrokerId id) {
        raft_node = std::make_unique<raft::RaftNode>(id, std::vector<raft::PeerInfo>{}, io, 0,
                                                     (path / "raft").string());
        raft_node->start();
        controller = std::make_unique<MetadataController>((path / "meta").string(), "cluster",
                                                          testBroker(id), nullptr, raft_node.get());
        controller->start();
        controller->registerBroker(testBroker(0));
        controller->registerBroker(testBroker(1));
    }
    ~Node() {
        controller->stop();
        raft_node->stop();
    }
};
std::array<uint8_t, 16> id(const MetadataController& c, const std::string& name) {
    return c.describeTopics({name}).at(0).topic_id;
}
}  // namespace

TEST(TopicIdentityTest, ControllerIdentitySurvivesRestartAndChangesOnRecreation) {
    Workspace w;
    std::array<uint8_t, 16> before{};
    {
        Node node(w.path, 0);
        ASSERT_EQ(node.controller->createTopic(topic("t")).error_code, ErrorCode::NONE);
        before = id(*node.controller, "t");
        ASSERT_NE(before, (std::array<uint8_t, 16>{}));
    }
    {
        Node node(w.path, 0);
        EXPECT_EQ(id(*node.controller, "t"), before);
        ASSERT_EQ(node.controller->deleteTopic("t").error_code, ErrorCode::NONE);
        ASSERT_EQ(node.controller->createTopic(topic("t")).error_code, ErrorCode::NONE);
        EXPECT_NE(id(*node.controller, "t"), before);
    }
}

TEST(TopicIdentityTest, ReplicatedCreateHasSameIdentityOnEveryBroker) {
    Workspace leader, follower;
    std::array<uint8_t, 16> before{};
    {
        Node node(leader.path, 0);
        ASSERT_EQ(node.controller->createTopic(topic("t")).error_code, ErrorCode::NONE);
        before = id(*node.controller, "t");
    }
    // Read the actual controller command that Raft persisted, then commit that
    // identical command through another broker's real state machine.
    std::vector<uint8_t> command;
    {
        std::unique_ptr<rocksdb::DB> db;
        ASSERT_TRUE(openRocksDb(rocksdb::Options{}, (leader.path / "raft").string(), db).ok());
        std::unique_ptr<rocksdb::Iterator> it(db->NewIterator(rocksdb::ReadOptions{}));
        it->Seek("log:");
        ASSERT_TRUE(it->Valid());
        const auto raw = it->value().ToString();
        raft::ByteBuffer bytes(std::vector<uint8_t>(raw.begin(), raw.end()));
        command = raft::LogEntryCodec::decodeFrom(bytes).data;
    }
    {
        Node node(follower.path, 1);
        node.raft_node->appendCommand(command, "metadata").get();
        const auto end = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (node.controller->describeTopics({"t"}).front().error_code != ErrorCode::NONE &&
               std::chrono::steady_clock::now() < end) {
            std::this_thread::yield();
        }
        ASSERT_EQ(node.controller->describeTopics({"t"}).front().error_code, ErrorCode::NONE);
        EXPECT_EQ(id(*node.controller, "t"), before);
    }
}

TEST(TopicIdentityTest, LegacyMetadataGetsStableNonzeroIdentityAcrossBrokers) {
    Workspace w;
    MetadataStore initial(w.path.string(), "cluster", testBroker(0), nullptr);
    initial.load();
    ASSERT_EQ(initial.applyCreate(topic("legacy")).error_code, ErrorCode::NONE);
    const auto file = w.path / "topics.json";
    nlohmann::json json;
    {
        std::ifstream in(file);
        in >> json;
    }
    json["topics"][0].erase("topic_id");
    {
        std::ofstream out(file);
        out << json;
    }
    MetadataStore a(w.path.string(), "cluster", testBroker(0), nullptr);
    a.load();
    MetadataStore b(w.path.string(), "cluster", testBroker(1), nullptr);
    b.load();
    const auto first = a.describeTopics({"legacy"}).front().topic_id;
    EXPECT_NE(first, (std::array<uint8_t, 16>{}));
    EXPECT_EQ(first, b.describeTopics({"legacy"}).front().topic_id);
    {
        std::ifstream in(file);
        in >> json;
    }
    EXPECT_TRUE(json["topics"][0].contains("topic_id"));
}

TEST(TopicIdentityTest, IdentityIndexTracksReloadDeletionAndRecreation) {
    Workspace w;
    MetadataStore store(w.path.string(), "cluster", testBroker(0), nullptr);
    store.load();
    auto first = topic("t");
    first.topic_id = newTopicId();
    ASSERT_EQ(store.applyCreate(first).error_code, ErrorCode::NONE);
    ASSERT_TRUE(store.topicById(first.topic_id));
    EXPECT_EQ(store.topicById(first.topic_id)->name, "t");
    store.load();
    store.load();
    ASSERT_TRUE(store.topicById(first.topic_id));
    ASSERT_EQ(store.applyDelete("t").error_code, ErrorCode::NONE);
    EXPECT_FALSE(store.topicById(first.topic_id));
    auto next = topic("t");
    next.topic_id = newTopicId();
    ASSERT_EQ(store.applyCreate(next).error_code, ErrorCode::NONE);
    EXPECT_FALSE(store.topicById(first.topic_id));
    ASSERT_TRUE(store.topicById(next.topic_id));
    auto duplicate = topic("other");
    duplicate.topic_id = next.topic_id;
    EXPECT_EQ(store.applyCreate(duplicate).error_code, ErrorCode::INVALID_REQUEST);
}

TEST(TopicIdentityTest, RejectsMalformedAndDuplicatePersistedIdentities) {
    Workspace w;
    MetadataStore original(w.path.string(), "cluster", testBroker(0), nullptr);
    original.load();
    ASSERT_EQ(original.applyCreate(topic("one")).error_code, ErrorCode::NONE);
    ASSERT_EQ(original.applyCreate(topic("two")).error_code, ErrorCode::NONE);
    nlohmann::json valid;
    const auto file = w.path / "topics.json";
    {
        std::ifstream in(file);
        in >> valid;
    }
    auto check_bad = [&](const nlohmann::json& corrupt) {
        {
            std::ofstream out(file);
            out << corrupt;
        }
        MetadataStore reader(w.path.string(), "cluster", testBroker(0), nullptr);
        EXPECT_THROW(reader.load(), std::exception);
    };
    for (const auto& invalid : {nlohmann::json::array({1}), nlohmann::json::array(),
                                nlohmann::json(std::array<uint8_t, 16>{})}) {
        auto corrupt = valid;
        corrupt["topics"][0]["topic_id"] = invalid;
        check_bad(corrupt);
    }
    for (const auto& invalid : {nlohmann::json(-1), nlohmann::json(256), nlohmann::json(1.5)}) {
        auto corrupt = valid;
        corrupt["topics"][0]["topic_id"][0] = invalid;
        check_bad(corrupt);
    }
    auto corrupt = valid;
    corrupt["topics"][1]["topic_id"] = corrupt["topics"][0]["topic_id"];
    check_bad(corrupt);
}
