#include "kawasan/broker/coordinator_format.h"

#include <gtest/gtest.h>

#include <boost/asio.hpp>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <nlohmann/json.hpp>
#include <thread>

#include "kawasan/broker/kawasan_broker.h"
#include "kawasan/broker/metadata_controller.h"
#include "kawasan/common/logger.h"
#include "kawasan/storage/log_manager.h"

namespace fs = std::filesystem;
using namespace kawasan;
using namespace kawasan::broker;

class CoordinatorFormatTest : public ::testing::Test {
protected:
    void SetUp() override {
        static const bool logging = [] {
            Logger::init("warn");
            return true;
        }();
        (void)logging;
        dir = fs::temp_directory_path() /
              ("kawasan-format-" +
               std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        fs::create_directories(dir / "meta");
    }
    void TearDown() override {
        if (!HasFailure())
            fs::remove_all(dir);
        else
            std::cerr << "Retained fixture: " << dir << '\n';
    }
    CoordinatorFormatStorage storage() { return {format, dir.string(), (dir / "meta").string()}; }
    fs::path manifest() { return dir / "meta/coordinator-format.json"; }
    CoordinatorFormat format{1, "test-cluster", "java-byte-hash-unsigned-mod-v1", 2, 2};
    fs::path dir;
};

TEST_F(CoordinatorFormatTest, EmptyJoinWaitsForCommittedDeclarationWithoutWriting) {
    auto local = storage();
    local.admit(std::nullopt);
    EXPECT_FALSE(fs::exists(manifest()));
    storage::LogManager logs(dir.string());
    EXPECT_THROW(local.openReplica(logs, "__consumer_offsets", 0), std::runtime_error);
    local.admit(format);
    EXPECT_EQ(CoordinatorFormat::deserialize(format.serialize()), format);
    EXPECT_TRUE(fs::exists(manifest()));
}

TEST_F(CoordinatorFormatTest, FreshReplicaRequiresAdmissionAndMissingSourceCannotReinitialize) {
    auto local = storage();
    local.admit(format);
    storage::LogManager logs(dir.string());
    auto* log = local.openReplica(logs, "__consumer_offsets", 0);
    ASSERT_NE(log, nullptr);
    EXPECT_EQ(log->highWatermark(), 0);
    Record r;
    r.value = std::vector<uint8_t>{1};
    log->append({r}, true);
    logs.closeAll();
    auto reopened = storage();
    reopened.admit(format);
    EXPECT_EQ(reopened.openReplica(logs, "__consumer_offsets", 0)->logEndOffset(), 1);
    logs.closeAll();
    fs::remove_all(dir / "__consumer_offsets-0");
    EXPECT_THROW(reopened.openReplica(logs, "__consumer_offsets", 0), std::exception);
    EXPECT_FALSE(fs::exists(dir / "__consumer_offsets-0"));
}

TEST_F(CoordinatorFormatTest, CacheLossIsAllowedButFormatAndPartialSourcesFailClosed) {
    auto local = storage();
    local.admit(format);
    fs::create_directories(dir / "consumer_offsets");
    fs::remove_all(dir / "consumer_offsets");
    EXPECT_NO_THROW(storage().admit(format));
    EXPECT_THROW(storage().admit(std::nullopt), std::exception);
    auto changed = format;
    changed.offsets_partitions++;
    EXPECT_THROW(storage().admit(changed), std::exception);
    storage::LogManager logs(dir.string());
    fs::create_directories(dir / "__transaction_state-0");
    EXPECT_THROW(local.openReplica(logs, "__transaction_state", 0), std::exception);
    EXPECT_TRUE(fs::exists(dir / "__transaction_state-0"));
    EXPECT_THROW(local.openReplica(logs, "__transaction_state", 2), std::exception);
    EXPECT_THROW(local.openReplica(logs, "user-topic", 0), std::exception);
}

TEST_F(CoordinatorFormatTest, LegacyOffsetsGroupsTransactionsAndQuarantinesAreNeverModified) {
    for (const auto* name : {"consumer_offsets", "__consumer_offsets-0", "__transaction_state-0",
                             "__transaction_state-0.corrupt-123"}) {
        fs::create_directories(dir / name);
        std::ofstream(dir / name / "sentinel") << "retain";
        EXPECT_THROW(storage().admit(format), std::exception) << name;
        EXPECT_THROW(storage().admit(std::nullopt), std::exception) << name;
        EXPECT_TRUE(fs::exists(dir / name / "sentinel"));
        EXPECT_FALSE(fs::exists(manifest()));
        fs::remove_all(dir / name);
    }
}

TEST_F(CoordinatorFormatTest, UnsupportedPartialAndChangedContractsAreRejected) {
    auto changed = format;
    changed.version = 99;
    EXPECT_THROW(changed.validate(), std::exception);
    changed = format;
    changed.routing = "positive-java-hash";
    EXPECT_THROW(changed.validate(), std::exception);
    changed = format;
    changed.cluster_id.clear();
    EXPECT_THROW(changed.validate(), std::exception);
    changed = format;
    changed.offsets_partitions = 0;
    EXPECT_THROW(changed.validate(), std::exception);
    EXPECT_THROW(CoordinatorFormat::deserialize("{}"), std::exception);
    std::ofstream(manifest()) << "{}";
    EXPECT_THROW(storage().admit(format), std::exception);
    fs::remove(manifest());
    std::ofstream(manifest().string() + ".tmp") << "partial";
    EXPECT_THROW(storage().admit(format), std::exception);
    EXPECT_FALSE(fs::exists(manifest()));
}

TEST_F(CoordinatorFormatTest, ManifestFailureAndReservedButMissingSourceDoNotBootstrap) {
    // A directory where the manifest file should be is an environmental error.
    fs::create_directories(manifest());
    EXPECT_THROW(storage().admit(format), std::exception);
    fs::remove_all(manifest());
    auto local = storage();
    local.admit(format);
    auto json = nlohmann::json::parse(std::ifstream(manifest()));
    json["initialized"].push_back({{"topic", "__transaction_state"}, {"partition", 0}});
    std::ofstream(manifest()) << json;
    auto reserved = storage();
    reserved.admit(format);
    storage::LogManager logs(dir.string());
    EXPECT_THROW(reserved.openReplica(logs, "__transaction_state", 0), std::exception);
    EXPECT_FALSE(fs::exists(dir / "__transaction_state-0"));
}

TEST_F(CoordinatorFormatTest, MissingCacheDoesNotRelaxStrictSourceHighWatermark) {
    auto local = storage();
    local.admit(format);
    storage::LogManager logs(dir.string());
    logs.setRecoverHighWatermarkToLogEnd(true);
    auto* log = local.openReplica(logs, "__transaction_state", 0);
    Record r;
    r.value = std::vector<uint8_t>{1};
    log->append({r}, true);
    log->setHighWatermark(0);
    logs.closeAll();
    auto reopened = storage();
    reopened.admit(format);
    EXPECT_EQ(reopened.openReplica(logs, "__transaction_state", 0)->highWatermark(), 0);
}

TEST_F(CoordinatorFormatTest, MetadataRejectsLegacyAndCountChangesBeforeMutation) {
    const auto meta = (dir / "meta").string();
    const BrokerMetadata broker{0, "127.0.0.1", 9092, std::nullopt};
    {
        MetadataStore old(meta, format.cluster_id, broker, nullptr);
        old.load();
        TopicSpecification spec;
        spec.name = "__consumer_offsets";
        spec.num_partitions = 2;
        spec.replication_factor = 1;
        ASSERT_EQ(old.applyCreate(spec).error_code, ErrorCode::NONE);
    }
    std::ifstream in(dir / "meta/topics.json");
    const std::string before((std::istreambuf_iterator<char>(in)), {});
    MetadataStore fresh(meta, format.cluster_id, broker, nullptr);
    fresh.configureCoordinatorFormat(format, dir.string());
    EXPECT_THROW(fresh.load(), std::exception);
    std::ifstream after_file(dir / "meta/topics.json");
    EXPECT_EQ(std::string((std::istreambuf_iterator<char>(after_file)), {}), before);
    EXPECT_FALSE(fs::exists(manifest()));
}

TEST_F(CoordinatorFormatTest, LegacyRuntimeRejectsManifestBeforeCreatingOffsetDatabase) {
    auto local = storage();
    local.admit(format);
    Config config;
    config.set("broker.id", 0);
    config.set("log.dirs", dir.string());
    config.set("metadata.dir", (dir / "meta").string());
    EXPECT_THROW(KawasanBroker broker(config), std::exception);
    EXPECT_FALSE(fs::exists(dir / "consumer_offsets"));
    EXPECT_FALSE(fs::exists(dir / "meta/topics.json"));
}

TEST_F(CoordinatorFormatTest, DeclarationCommitsThroughRaftAndSurvivesRestart) {
    const BrokerMetadata broker{0, "127.0.0.1", 9092, std::nullopt};
    for (int pass = 0; pass < 2; ++pass) {
        boost::asio::io_context io;
        raft::RaftNode node(0, {}, io, 0, (dir / "raft").string());
        storage::LogManager logs(dir.string());
        MetadataController controller((dir / "meta").string(), format.cluster_id, broker, &logs,
                                      &node);
        controller.configureCoordinatorFormat(format, dir.string());
        node.start();
        controller.start();
        ASSERT_EQ(controller.declareCoordinatorFormat(format).error_code, ErrorCode::NONE);
        EXPECT_EQ(controller.coordinatorFormat(), format);
        TopicSpecification spec;
        spec.name = "__consumer_offsets";
        spec.num_partitions = 3;
        spec.replication_factor = 1;
        spec.configs = {{"cleanup.policy", "compact"}};
        EXPECT_EQ(controller.createTopic(spec).error_code, ErrorCode::INVALID_REQUEST);
        spec.num_partitions = 2;
        if (pass == 0)
            ASSERT_EQ(controller.createTopic(spec).error_code, ErrorCode::NONE);
        EXPECT_EQ(controller.increasePartitions(spec.name, 3).error_code,
                  ErrorCode::INVALID_REQUEST);
        EXPECT_EQ(controller.deleteTopic(spec.name).error_code, ErrorCode::INVALID_REQUEST);
        EXPECT_EQ(
            controller.alterTopicConfigs(spec.name, {{"cleanup.policy", "delete", 0}}, false, false)
                .error_code,
            ErrorCode::INVALID_CONFIG);
        // Ensure repeat declaration and unrelated commands cannot lose the contract.
        spec.name = "user" + std::to_string(pass);
        spec.num_partitions = 1;
        EXPECT_EQ(controller.createTopic(spec).error_code, ErrorCode::NONE);
        node.stop();
        controller.stop();
    }
    EXPECT_TRUE(fs::exists(manifest()));
    MetadataStore mixed((dir / "meta").string(), format.cluster_id, broker, nullptr);
    EXPECT_THROW(mixed.load(), std::exception);
    auto changed = format;
    changed.transaction_partitions++;
    MetadataStore wrong((dir / "meta").string(), format.cluster_id, broker, nullptr);
    wrong.configureCoordinatorFormat(changed, dir.string());
    EXPECT_THROW(wrong.load(), std::exception);
}

TEST_F(CoordinatorFormatTest, FormatAdmissionDoesNotInitializeUnassignedPartitions) {
    storage::LogManager logs(dir.string());
    MetadataStore store((dir / "meta").string(), format.cluster_id,
                        BrokerMetadata{0, "127.0.0.1", 9092, std::nullopt}, &logs);
    store.configureCoordinatorFormat(format, dir.string());
    store.load();
    store.registerBroker(BrokerMetadata{1, "127.0.0.1", 9093, std::nullopt});
    store.applyCoordinatorFormat(format);
    TopicSpecification spec;
    spec.name = "__transaction_state";
    spec.num_partitions = 2;
    spec.replication_factor = 1;
    spec.assignments = {{1}, {0}};
    spec.configs = {{"cleanup.policy", "compact"}};
    ASSERT_EQ(store.applyCreate(spec).error_code, ErrorCode::NONE);
    EXPECT_FALSE(fs::exists(dir / "__transaction_state-0"));
    EXPECT_TRUE(fs::exists(dir / "__transaction_state-1"));
    EXPECT_THROW(logs.getOrCreateLog(spec.name, 0), std::exception);
}

TEST_F(CoordinatorFormatTest, EmptyLateJoiningBrokerObtainsCommittedDeclaration) {
    using Tcp = boost::asio::ip::tcp;
    struct Node {
        boost::asio::io_context io;
        boost::asio::executor_work_guard<boost::asio::io_context::executor_type> guard{
            io.get_executor()};
        std::thread thread{[this] { io.run(); }};
        std::unique_ptr<storage::LogManager> logs;
        std::unique_ptr<raft::RaftNode> raft;
        std::unique_ptr<MetadataController> controller;
        ~Node() {
            if (raft)
                raft->stop();
            if (controller)
                controller->stop();
            controller.reset();
            raft.reset();
            logs.reset();
            guard.reset();
            io.stop();
            thread.join();
        }
    };
    boost::asio::io_context reserve_io;
    std::vector<std::unique_ptr<Tcp::acceptor>> reservations;
    std::vector<int> ports;
    for (int i = 0; i < 3; ++i) {
        auto socket = std::make_unique<Tcp::acceptor>(reserve_io, Tcp::endpoint(Tcp::v4(), 0));
        ports.push_back(socket->local_endpoint().port());
        reservations.push_back(std::move(socket));
    }
    std::vector<std::unique_ptr<Node>> nodes;
    for (int id = 0; id < 3; ++id) {
        auto n = std::make_unique<Node>();
        const auto base = (dir / std::to_string(id)).string();
        std::vector<raft::PeerInfo> peers;
        for (int other = 0; other < 3; ++other)
            if (other != id)
                peers.push_back({other, "127.0.0.1", ports[other]});
        n->logs = std::make_unique<storage::LogManager>(base);
        n->raft = std::make_unique<raft::RaftNode>(id, peers, n->io, ports[id], base + "/raft");
        n->controller = std::make_unique<MetadataController>(
            base + "/meta", format.cluster_id,
            BrokerMetadata{id, "127.0.0.1", ports[id] - 1, std::nullopt}, n->logs.get(),
            n->raft.get());
        n->controller->configureCoordinatorFormat(format, base);
        n->controller->start();
        nodes.push_back(std::move(n));
    }
    reservations.clear();
    nodes[0]->raft->start();
    nodes[1]->raft->start();
    auto waitUntil = [](auto predicate) {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (std::chrono::steady_clock::now() < deadline) {
            if (predicate())
                return true;
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        return false;
    };
    ASSERT_TRUE(
        waitUntil([&] { return nodes[0]->raft->isLeader() || nodes[1]->raft->isLeader(); }));
    const int leader = nodes[0]->raft->isLeader() ? 0 : 1;
    ASSERT_EQ(nodes[leader]->controller->declareCoordinatorFormat(format).error_code,
              ErrorCode::NONE);
    EXPECT_FALSE(fs::exists(dir / "2/meta/coordinator-format.json"));
    EXPECT_FALSE(nodes[2]->controller->coordinatorFormat());
    nodes[2]->raft->start();
    ASSERT_TRUE(waitUntil([&] { return nodes[2]->controller->coordinatorFormat().has_value(); }));
    for (const auto& n : nodes)
        EXPECT_EQ(n->controller->coordinatorFormat(), format);
    EXPECT_TRUE(fs::exists(dir / "2/meta/coordinator-format.json"));
    EXPECT_EQ(nodes[2]->controller->declareCoordinatorFormat(format).error_code,
              ErrorCode::NOT_CONTROLLER);
}

TEST_F(CoordinatorFormatTest, FailedDeclarationLatchesAndDoesNotAdvanceMetadataIndex) {
    boost::asio::io_context io;
    raft::RaftNode node(0, {}, io, 0, (dir / "raft").string());
    MetadataController controller((dir / "meta").string(), format.cluster_id,
                                  BrokerMetadata{0, "127.0.0.1", 9092, std::nullopt}, nullptr,
                                  &node);
    controller.configureCoordinatorFormat(format, dir.string());
    controller.start();
    node.start();
    auto changed = format;
    changed.transaction_partitions++;
    EXPECT_EQ(controller.declareCoordinatorFormat(changed).error_code,
              ErrorCode::KAFKA_STORAGE_ERROR);
    EXPECT_FALSE(controller.coordinatorFormat());
    EXPECT_FALSE(fs::exists(manifest()));
    TopicSpecification spec;
    spec.name = "after-failure";
    spec.num_partitions = 1;
    spec.replication_factor = 1;
    EXPECT_EQ(controller.createTopic(spec).error_code, ErrorCode::KAFKA_STORAGE_ERROR);
    EXPECT_EQ(nlohmann::json::parse(std::ifstream(dir / "meta/topics.json")).at("applied_index"),
              0);
    node.stop();
    controller.stop();
}
