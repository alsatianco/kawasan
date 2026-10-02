// M8-E1 prerequisite: Raft's commit/applied indexes are not persisted, so after
// a restart every Raft log entry is re-committed and re-delivered to the
// metadata state machine — on top of a metadata.json that already contains its
// effects. Non-idempotent commands (UPDATE_LEADER bumps leader_epoch; a replayed
// DELETE_TOPIC would delete a re-created topic) must not apply twice: the store
// persists the last applied Raft index atomically with its state and the
// controller skips entries at or below it.
#include <gtest/gtest.h>

#include <boost/asio.hpp>
#include <chrono>
#include <filesystem>
#include <memory>
#include <thread>

#include "kawasan/broker/metadata_controller.h"
#include "kawasan/common/logger.h"
#include "kawasan/raft/raft_node.h"
#include "kawasan/storage/log_manager.h"

namespace fs = std::filesystem;
using namespace kawasan;
using namespace kawasan::broker;

namespace {

std::string makeDir() {
    const auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
    auto p = fs::temp_directory_path() / ("kawasan-mdreplay-" + std::to_string(ts));
    fs::create_directories(p);
    return p.string();
}

// A single-node Raft + MetadataController over persistent dirs, restartable.
struct Node {
    boost::asio::io_context io;
    boost::asio::executor_work_guard<boost::asio::io_context::executor_type> guard{
        io.get_executor()};
    std::thread io_thread{[this] { io.run(); }};
    std::unique_ptr<storage::LogManager> logs;
    std::unique_ptr<raft::RaftNode> raft;
    std::unique_ptr<MetadataController> controller;

    explicit Node(const std::string& dir) {
        logs = std::make_unique<storage::LogManager>(dir + "/data");
        logs->start();
        raft = std::make_unique<raft::RaftNode>(0, std::vector<raft::PeerInfo>{}, io, 0,
                                                dir + "/raft");
        raft->start();
        controller = std::make_unique<MetadataController>(
            dir + "/meta", "cid", BrokerMetadata{0, "127.0.0.1", 9092, std::nullopt}, logs.get(),
            raft.get());
        controller->start();
        for (BrokerId id : {1, 2}) {
            controller->registerBroker(BrokerMetadata{id, "127.0.0.1", 9092 + id, std::nullopt});
        }
    }
    ~Node() {
        controller->stop();
        raft->stop();
        controller.reset();
        raft.reset();
        logs->stop();
        logs.reset();
        guard.reset();
        io.stop();
        io_thread.join();
    }

    PartitionMetadata partition(const std::string& topic) {
        auto md = controller->describeTopics({topic});
        EXPECT_EQ(md.size(), 1u);
        EXPECT_EQ(md.front().error_code, ErrorCode::NONE);
        return md.front().partitions.at(0);
    }
};

TopicSpecification spec(const std::string& name, int16_t rf) {
    TopicSpecification s;
    s.name = name;
    s.num_partitions = 1;
    s.replication_factor = rf;
    return s;
}

}  // namespace

TEST(MetadataReplayTest, RestartDoesNotReapplyCommittedCommands) {
    Logger::init("warn");
    const auto dir = makeDir();
    {
        Node node(dir);
        ASSERT_EQ(node.controller->createTopic(spec("t", 3)).error_code, ErrorCode::NONE);
        const auto before = node.partition("t");
        BrokerId other = before.replicas.back();
        ASSERT_EQ(node.controller->updatePartitionLeader("t", 0, other).error_code,
                  ErrorCode::NONE);
        ASSERT_EQ(node.partition("t").leader_epoch, 1);
        // History that must not replay: delete "gone", then re-create it.
        ASSERT_EQ(node.controller->createTopic(spec("gone", 1)).error_code, ErrorCode::NONE);
        ASSERT_EQ(node.controller->deleteTopic("gone").error_code, ErrorCode::NONE);
        ASSERT_EQ(node.controller->createTopic(spec("gone", 3)).error_code, ErrorCode::NONE);
        kawasan::Record record;
        record.value = std::vector<uint8_t>{'x'};
        node.logs->getOrCreateLog("gone", 0)->append({record});
        ASSERT_EQ(node.logs->getLog("gone", 0)->logEndOffset(), 1);
    }
    {
        Node node(dir);
        ASSERT_EQ(node.logs->getOrCreateLog("gone", 0)->logEndOffset(),
                  1);  // open, as a broker does
        // A new command makes the restarted single-node leader commit (and
        // deliver) every earlier entry again.
        ASSERT_EQ(node.controller->createTopic(spec("after-restart", 1)).error_code,
                  ErrorCode::NONE);
        EXPECT_EQ(node.partition("t").leader_epoch, 1) << "UPDATE_LEADER re-applied on restart";
        auto* gone = node.logs->getLog("gone", 0);
        ASSERT_NE(gone, nullptr);
        EXPECT_EQ(gone->logEndOffset(), 1)
            << "historical DELETE_TOPIC replayed: the re-created topic's data was wiped";
    }
    fs::remove_all(dir);
}

TEST(MetadataReplayTest, AppliedIndexSurvivesReload) {
    const auto dir = makeDir();
    {
        MetadataStore store(dir, "cid", BrokerMetadata{0, "127.0.0.1", 9092, std::nullopt},
                            nullptr);
        store.load();
        EXPECT_EQ(store.appliedIndex(), 0);
        store.setAppliedIndex(42);
        ASSERT_EQ(store.applyCreate(spec("x", 1)).error_code, ErrorCode::NONE);
    }
    {
        MetadataStore store(dir, "cid", BrokerMetadata{0, "127.0.0.1", 9092, std::nullopt},
                            nullptr);
        store.load();
        EXPECT_EQ(store.appliedIndex(), 42);
        for (const auto& entry : fs::directory_iterator(dir)) {
            EXPECT_NE(entry.path().extension(), ".tmp") << entry.path();
        }
    }
    fs::remove_all(dir);
}

// If the Raft log is lost (its directory wiped) while the metadata file
// survives, new entries restart at index 1 and must still apply.
TEST(MetadataReplayTest, LostRaftLogResetsAppliedIndex) {
    const auto dir = makeDir();
    {
        Node node(dir);
        ASSERT_EQ(node.controller->createTopic(spec("a", 1)).error_code, ErrorCode::NONE);
        ASSERT_EQ(node.controller->createTopic(spec("b", 1)).error_code, ErrorCode::NONE);
    }
    fs::remove_all(dir + "/raft");
    {
        Node node(dir);
        ASSERT_EQ(node.controller->createTopic(spec("c", 1)).error_code, ErrorCode::NONE);
        EXPECT_EQ(node.controller->describeTopics({"c"}).front().error_code, ErrorCode::NONE);
    }
    fs::remove_all(dir);
}

TEST(MetadataReplayTest, VersionFenceSurvivesRaftSerializationAndRestart) {
    // Logger is initialized by the suite's first test.
    const auto dir = makeDir();
    {
        Node node(dir);
        ASSERT_EQ(node.controller->createTopic(spec("versioned", 3)).error_code, ErrorCode::NONE);
        ASSERT_EQ(node.controller->updatePartitionISR("versioned", 0, {0, 1}, 0).error_code,
                  ErrorCode::NONE);
        EXPECT_EQ(node.controller->updatePartitionISR("versioned", 0, {0, 1, 2}, 0).error_code,
                  ErrorCode::INVALID_UPDATE_VERSION);
        EXPECT_EQ(node.controller->updatePartitionLeader("versioned", 0, 2, 0).error_code,
                  ErrorCode::INVALID_UPDATE_VERSION);
        EXPECT_EQ(node.partition("versioned").leader, 0);
        EXPECT_EQ(node.partition("versioned").partition_epoch, 1);
    }
    {
        Node node(dir);
        EXPECT_EQ(node.partition("versioned").isr, (std::vector<BrokerId>{0, 1}));
        ASSERT_EQ(node.controller->updatePartitionLeader("versioned", 0, 1, 1).error_code,
                  ErrorCode::NONE);
        EXPECT_EQ(node.partition("versioned").partition_epoch, 2);
        EXPECT_EQ(node.controller->updatePartitionISR("versioned", 0, {0}, 1).error_code,
                  ErrorCode::INVALID_UPDATE_VERSION);
    }
    fs::remove_all(dir);
}
