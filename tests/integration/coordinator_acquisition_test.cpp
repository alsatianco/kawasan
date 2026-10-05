#include <fcntl.h>
#include <gtest/gtest.h>
#include <poll.h>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include <boost/asio.hpp>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <thread>

#include "../sparse_broker_test_client.h"
#include "kawasan/broker/coordinator_routing.h"
#include "kawasan/broker/group_state_manager.h"
#include "kawasan/broker/kawasan_broker.h"
#include "kawasan/broker/transaction_state_manager.h"
#include "kawasan/common/logger.h"
#include "kawasan/protocol/offset_fetch_request.h"

using namespace kawasan;
using namespace kawasan::broker;
using namespace std::chrono_literals;
namespace fs = std::filesystem;
namespace asio = boost::asio;

namespace kawasan::broker {
struct CoordinatorAcquisitionProbe {
    static OffsetManager* offsets(KawasanBroker& b) { return b.offset_manager_.get(); }
    static TransactionCoordinator* transactions(KawasanBroker& b) {
        return b.transaction_coordinator_.get();
    }
    static GroupCoordinator* groups(KawasanBroker& b) { return b.group_coordinator_.get(); }
    static void expectGroupClocks(GroupCoordinator& coordinator, const std::string& id,
                                  int64_t stored_timestamp) {
        std::lock_guard<std::mutex> lock(coordinator.mutex_);
        const auto& group = coordinator.groups_.at(id);
        EXPECT_EQ(std::chrono::duration_cast<std::chrono::milliseconds>(
                      group.last_activity.time_since_epoch())
                      .count(),
                  stored_timestamp);
        const auto now = std::chrono::steady_clock::now();
        EXPECT_LT(now - group.rebalance_started_at, 5s);
        for (const auto& [member_id, member] : group.members) {
            (void)member_id;
            EXPECT_LT(now - member.last_heartbeat, 5s);
        }
    }
};
}  // namespace kawasan::broker

namespace {
std::string crash_executable;
void crashChild(const std::string& config_path, int pass, int ready_fd) {
    Config child_config;
    child_config.load(config_path);
    const auto child_log =
        fs::path(child_config.get<std::string>("log.dirs")).parent_path() / "child.log";
    const int fd = open(child_log.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0600);
    if (fd >= 0) {
        dup2(fd, STDOUT_FILENO);
        dup2(fd, STDERR_FILENO);
        close(fd);
    }
    try {
        Config c;
        c.load(config_path);
        Logger::init("warn");
        CoordinatorFormat contract{1, c.get<std::string>("cluster.id"),
                                   "java-byte-hash-unsigned-mod-v1", 1, 1};
        KawasanBroker b(c, KawasanBroker::CoordinatorAcquisitionOnly{contract});
        b.start();
        b.reconcileReplicas();
        if (pass == 0) {
            GroupRecord group;
            group.key.group_id = "crash-group";
            group.group = {5,          3,
                           "consumer", "range",
                           "member",   60000,
                           123,        {{"member", "client", "host", "instance", {255}, {128}}}};
            auto offset = group;
            offset.key.kind = GroupRecordKey::Kind::Offset;
            offset.key.topic = "data";
            offset.key.partition = 0;
            offset.offset = {567, "crash-checkpoint", 1, 999, 9};
            auto pending = offset;
            pending.key.kind = GroupRecordKey::Kind::PendingOffset;
            pending.key.transactional_id = "crash-txn";
            pending.key.producer_id = 75;
            pending.key.producer_epoch = 4;
            pending.offset.offset = 568;
            b.logManager()
                ->getLog("__consumer_offsets", 0)
                ->append({GroupStateManager::encode(group), GroupStateManager::encode(offset),
                          GroupStateManager::encode(pending)},
                         true);
            TransactionCoordinator::TxnSnapshot txn;
            txn.transactional_id = "crash-txn";
            txn.producer_id = 75;
            txn.producer_epoch = 4;
            txn.state = TransactionCoordinator::State::PrepareCommit;
            Record r;
            r.key = std::vector<uint8_t>(txn.transactional_id.begin(), txn.transactional_id.end());
            r.value = TransactionStateManager::serialize(txn);
            b.logManager()->getLog("__transaction_state", 0)->append({r}, true);
            b.logManager()->flushAll();
        } else {
            const auto offset = CoordinatorAcquisitionProbe::offsets(b)->fetchOffsetWithMetadata(
                "crash-group", "data", 0);
            const auto txn = CoordinatorAcquisitionProbe::transactions(b)->describe("crash-txn");
            const auto pending =
                CoordinatorAcquisitionProbe::groups(b)->pendingCoordinatorOffsets();
            if (!offset || offset->offset != 567 || offset->committed_leader_epoch != 9 || !txn ||
                txn->producer_epoch != 4 ||
                txn->state != TransactionCoordinator::State::PrepareCommit || pending.size() != 1 ||
                pending[0].offset.offset != 568 ||
                CoordinatorAcquisitionProbe::groups(b)->validateTxnOffsetCommit(
                    "crash-group", 5, "member", "instance") != ErrorCode::NONE)
                throw std::runtime_error("SIGKILL source acquisition lost committed state");
        }
        const char ok = 1;
        if (write(ready_fd, &ok, 1) != 1)
            _exit(3);
        close(ready_fd);
        for (;;)
            pause();
    } catch (const std::exception& ex) {
        std::cerr << "Child failed: " << ex.what() << '\n';
        _exit(2);
    }
}

template <typename F>
bool waitUntil(F f) {
    auto until = std::chrono::steady_clock::now() + 20s;
    do {
        if (f())
            return true;
        std::this_thread::sleep_for(20ms);
    } while (std::chrono::steady_clock::now() < until);
    return f();
}
class CoordinatorAcquisitionTest : public ::testing::Test {
protected:
    fs::path dir;
    CoordinatorFormat format{1, "acquisition-test", "java-byte-hash-unsigned-mod-v1", 3, 3};
    std::vector<std::unique_ptr<KawasanBroker>> brokers;
    std::vector<Config> configs;
    asio::io_context io;
    std::vector<std::unique_ptr<asio::ip::tcp::acceptor>> reservations;
    void SetUp() override {
        static const bool logging = [] {
            Logger::init("warn");
            return true;
        }();
        (void)logging;
        dir = fs::temp_directory_path() /
              ("kawasan-acquisition-" +
               std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    }
    void TearDown() override {
        for (auto& b : brokers)
            if (b)
                b->stop();
        brokers.clear();
        if (!HasFailure())
            fs::remove_all(dir);
        else
            std::cerr << "Retained fixture: " << dir << '\n';
    }
    Config config(int id = 0) {
        Config c;
        c.setInt("broker.id", id);
        c.setString("cluster.id", format.cluster_id);
        c.setString("log.dirs", (dir / std::to_string(id)).string());
        c.setString("host", "127.0.0.1");
        c.setBool("monitoring.enabled", false);
        c.setInt("monitoring.port", 0);
        c.setInt("port", 0);
        c.setInt("raft.port", 0);
        c.setInt("offsets.topic.num.partitions", 3);
        c.setInt("transaction.state.topic.num.partitions", 3);
        return c;
    }
    void startCluster() {
        std::string peers;
        std::vector<int> ports;
        for (int id = 0; id < 3; ++id) {
            for (;;) {
                auto kafka = std::make_unique<asio::ip::tcp::acceptor>(
                    io, asio::ip::tcp::endpoint(asio::ip::make_address("127.0.0.1"), 0));
                const int port = kafka->local_endpoint().port();
                if (port == 65535)
                    continue;
                auto raft = std::make_unique<asio::ip::tcp::acceptor>(io);
                raft->open(asio::ip::tcp::v4());
                boost::system::error_code ec;
                raft->bind({asio::ip::make_address("127.0.0.1"), static_cast<uint16_t>(port + 1)},
                           ec);
                if (ec)
                    continue;
                ports.push_back(port);
                reservations.push_back(std::move(kafka));
                reservations.push_back(std::move(raft));
                break;
            }
            if (id)
                peers += ',';
            peers += std::to_string(id) + ":127.0.0.1:" + std::to_string(ports.back() + 1);
        }
        for (int id = 0; id < 3; ++id) {
            auto c = config(id);
            c.setInt("port", ports[id]);
            c.setInt("raft.port", ports[id] + 1);
            c.setString("raft.peers", peers);
            c.setInt("broker.liveness.timeout.ms", 2000);
            configs.push_back(c);
            reservations[id * 2]->close();
            reservations[id * 2 + 1]->close();
            brokers.push_back(std::make_unique<KawasanBroker>(
                c, KawasanBroker::CoordinatorAcquisitionOnly{format}));
            brokers.back()->start();
        }
    }
    std::string key(int p) {
        for (int n = 0;; ++n) {
            auto k = "key:" + std::to_string(n);
            if (coordinatorPartitionFor(k, 3) == p)
                return k;
        }
    }
};

TEST_F(CoordinatorAcquisitionTest, FreshConstructionDefersCacheAndManifestUntilRaftDeclaration) {
    KawasanBroker broker(config(), KawasanBroker::CoordinatorAcquisitionOnly{format});
    EXPECT_FALSE(fs::exists(dir / "0/consumer_offsets"));
    EXPECT_FALSE(fs::exists(dir / "0/meta/coordinator-format.json"));
    EXPECT_EQ(broker.coordinatorLoadStatus(key(0), protocol::CoordinatorType::GROUP),
              ErrorCode::COORDINATOR_LOAD_IN_PROGRESS);
}

TEST_F(CoordinatorAcquisitionTest, LegacySourceIsRejectedBeforeAnyCacheOrMetadataMutation) {
    fs::create_directories(dir / "0/__transaction_state-0");
    std::ofstream(dir / "0/__transaction_state-0/sentinel") << "preserve";
    EXPECT_THROW(KawasanBroker broker(config(), KawasanBroker::CoordinatorAcquisitionOnly{format}),
                 std::exception);
    EXPECT_FALSE(fs::exists(dir / "0/consumer_offsets"));
    EXPECT_FALSE(fs::exists(dir / "0/meta"));
    EXPECT_TRUE(fs::exists(dir / "0/__transaction_state-0/sentinel"));
}

TEST_F(CoordinatorAcquisitionTest, ConfiguredFormatMismatchIsRejectedBeforeMutation) {
    auto c = config();
    c.setInt("transaction.state.topic.num.partitions", 4);
    EXPECT_THROW(KawasanBroker broker(c, KawasanBroker::CoordinatorAcquisitionOnly{format}),
                 std::exception);
    EXPECT_FALSE(fs::exists(dir / "0"));
}

TEST_F(CoordinatorAcquisitionTest, PartitionReplacementPreservesUnrelatedCachesAndPendingEpochs) {
    fs::create_directories(dir);
    auto offsets = std::make_shared<OffsetManager>((dir / "cache").string());
    GroupCoordinator groups(offsets);
    offsets->commitOffset(key(0), "old", 1, 100);
    offsets->commitOffset(key(1), "other", 1, 999);
    GroupRecord group;
    group.key.group_id = key(0);
    group.group = {7,          3,
                   "consumer", "range",
                   "member",   60000,
                   12,         {{"member", "client", "host", "instance", {255, 0}, {128, 0}}}};
    auto checkpoint = group;
    checkpoint.key.kind = GroupRecordKey::Kind::Offset;
    checkpoint.key.topic = "data";
    checkpoint.key.partition = 2;
    checkpoint.offset = {42, std::string("\xff\0", 2), 123, 999, 8};
    auto pending = checkpoint;
    pending.key.kind = GroupRecordKey::Kind::PendingOffset;
    pending.key.transactional_id = key(2);
    pending.key.producer_id = 50;
    pending.key.producer_epoch = 4;
    pending.offset.offset = 43;
    groups.replaceCoordinatorPartition(0, 3, {group, checkpoint, pending}, offsets);
    CoordinatorAcquisitionProbe::expectGroupClocks(groups, key(0), 12);
    EXPECT_FALSE(offsets->fetchOffset(key(0), "old", 1));
    EXPECT_EQ(offsets->fetchOffset(key(1), "other", 1), 999);
    const auto restored = offsets->fetchOffsetWithMetadata(key(0), "data", 2);
    ASSERT_TRUE(restored);
    EXPECT_EQ(restored->offset, 42);
    EXPECT_EQ(restored->metadata, checkpoint.offset.metadata);
    EXPECT_EQ(restored->commit_timestamp, 123);
    EXPECT_EQ(restored->expiry_timestamp, 999);
    EXPECT_EQ(restored->committed_leader_epoch, 8);
    EXPECT_EQ(groups.validateTxnOffsetCommit(key(0), 7, "member", "instance"), ErrorCode::NONE);
    EXPECT_EQ(groups.validateTxnOffsetCommit(key(0), 7, "other", "instance"),
              ErrorCode::FENCED_INSTANCE_ID);
    const auto descriptions = groups.describeGroups({key(0)});
    ASSERT_EQ(descriptions.size(), 1);
    ASSERT_EQ(descriptions[0].members.size(), 1);
    EXPECT_EQ(descriptions[0].members[0].member_assignment, group.group.members[0].assignment);
    const auto staged = groups.pendingCoordinatorOffsets();
    ASSERT_EQ(staged.size(), 1);
    EXPECT_EQ(staged[0].key.producer_epoch, 4);
    EXPECT_EQ(staged[0].offset.offset, 43);
    auto wrong = group;
    wrong.key.group_id = key(1);
    EXPECT_THROW(groups.replaceCoordinatorPartition(0, 3, {wrong}, offsets), std::exception);
    EXPECT_EQ(offsets->fetchOffset(key(0), "data", 2), 42);
    groups.replaceCoordinatorPartition(0, 3, {}, offsets);
    EXPECT_FALSE(offsets->fetchOffset(key(0), "data", 2));
    EXPECT_EQ(offsets->fetchOffset(key(1), "other", 1), 999);
    EXPECT_TRUE(groups.pendingCoordinatorOffsets().empty());

    TransactionCoordinator txns;
    TransactionCoordinator::TxnSnapshot old;
    old.transactional_id = key(0);
    old.producer_id = 1;
    txns.restore(old);
    auto unrelated = old;
    unrelated.transactional_id = key(1);
    txns.restore(unrelated);
    auto newer = old;
    newer.producer_id = 50;
    newer.producer_epoch = 4;
    newer.state = TransactionCoordinator::State::PrepareAbort;
    txns.replaceCoordinatorPartition(0, 3, {newer});
    EXPECT_EQ(txns.describe(key(0))->producer_id, 50);
    EXPECT_EQ(txns.describe(key(0))->state, TransactionCoordinator::State::PrepareAbort);
    EXPECT_EQ(txns.describe(key(1))->producer_id, 1);
    txns.replaceCoordinatorPartition(0, 3, {});
    EXPECT_FALSE(txns.describe(key(0)));
    EXPECT_TRUE(txns.describe(key(1)));
}

TEST_F(CoordinatorAcquisitionTest, SigkillRestoresSourceWithoutCacheOrLegacyPrepareRedrive) {
    // The parent has no live broker threads. SIGKILL runs neither destructors
    // nor a graceful checkpoint; child source writes explicitly fsync/checkpoint.
    fs::create_directories(dir);
    for (int pass = 0; pass < 2; ++pass) {
        int ready[2];
        ASSERT_EQ(pipe(ready), 0);
        auto c = config();
        c.setInt("offsets.topic.num.partitions", 1);
        c.setInt("transaction.state.topic.num.partitions", 1);
        const auto config_path = (dir / "child.properties").string();
        c.save(config_path);
        const auto pass_arg = std::to_string(pass);
        const auto fd_arg = std::to_string(ready[1]);
        const pid_t pid = fork();
        ASSERT_GE(pid, 0);
        if (pid == 0) {
            close(ready[0]);
            execl(crash_executable.c_str(), crash_executable.c_str(), "--crash-child",
                  config_path.c_str(), pass_arg.c_str(), fd_arg.c_str(), nullptr);
            _exit(127);
        }
        close(ready[1]);
        pollfd descriptor{ready[0], POLLIN, 0};
        char ok = 0;
        const bool success =
            poll(&descriptor, 1, 15000) > 0 && read(ready[0], &ok, 1) == 1 && ok == 1;
        close(ready[0]);
        EXPECT_EQ(kill(pid, SIGKILL), 0);
        int status = 0;
        while (waitpid(pid, &status, 0) < 0 && errno == EINTR) {
        }
        ASSERT_TRUE(success) << "See " << dir / "child.log";
        ASSERT_TRUE(WIFSIGNALED(status));
        EXPECT_EQ(WTERMSIG(status), SIGKILL);
        if (pass == 0)
            fs::remove_all(dir / "0/consumer_offsets");
    }
}

TEST_F(CoordinatorAcquisitionTest, FreshFollowersAcquireOnlyOwnedPartitionsAndRestartFromSource) {
    startCluster();
    ASSERT_TRUE(waitUntil([&] {
        for (int id = 0; id < 3; ++id) {
            if (brokers[id]->coordinatorLoadStatus(key(id), protocol::CoordinatorType::GROUP) !=
                ErrorCode::NONE)
                return false;
            if (brokers[id]->coordinatorLoadStatus(
                    key(id), protocol::CoordinatorType::TRANSACTION) != ErrorCode::NONE)
                return false;
        }
        return true;
    }));
    for (int id = 0; id < 3; ++id) {
        EXPECT_EQ(brokers[id]->openLogCount(), 2);
        EXPECT_NE(CoordinatorAcquisitionProbe::offsets(*brokers[id]), nullptr);
        for (int p = 0; p < 3; ++p)
            if (p != id)
                EXPECT_EQ(
                    brokers[id]->coordinatorLoadStatus(key(p), protocol::CoordinatorType::GROUP),
                    ErrorCode::NOT_COORDINATOR);
    }
    // Seed authoritative committed records, not legacy broker persistence.
    const int owner = 1;
    auto& b = *brokers[owner];
    GroupRecord group;
    group.key.group_id = key(owner);
    group.group = {
        9,        3,     "consumer", "range",
        "member", 60000, 123,        {{"member", "client", "host", "instance", {255}, {128}}}};
    auto offset = group;
    offset.key.kind = GroupRecordKey::Kind::Offset;
    offset.key.topic = "data";
    offset.key.partition = 0;
    offset.offset = {456, "checkpoint", 111, 999, 8};
    auto* log = b.logManager()->getLog("__consumer_offsets", owner);
    ASSERT_NE(log, nullptr);
    b.replicaManager()->stop();
    log->append({GroupStateManager::encode(group), GroupStateManager::encode(offset)}, true);
    KawasanBroker* current_controller = nullptr;
    ASSERT_TRUE(waitUntil([&] {
        for (auto& candidate : brokers)
            if (candidate->raftNode()->isLeader()) {
                current_controller = candidate.get();
                return true;
            }
        return false;
    }));
    ASSERT_EQ(current_controller->metadataController()
                  ->updatePartitionLeader("__consumer_offsets", owner, owner)
                  .error_code,
              ErrorCode::NONE);
    ASSERT_TRUE(waitUntil([&] {
        return b.metadataController()
                   ->describeTopics({"__consumer_offsets"})
                   .front()
                   .partitions[owner]
                   .leader_epoch > 0;
    }));
    const auto epoch = b.metadataController()
                           ->describeTopics({"__consumer_offsets"})
                           .front()
                           .partitions[owner]
                           .leader_epoch;
    const TopicPartition tp{"__consumer_offsets", owner};
    // Drive the existing ReplicaManager readiness barrier deterministically.
    // HW alone is insufficient when a new leadership has an unconfirmed tail.
    b.replicaManager()->reconcileReplica(tp, std::shared_ptr<storage::Log>(log, [](auto*) {}),
                                         owner, {0, owner}, epoch);
    b.acquireCoordinatorPartitions();
    EXPECT_EQ(b.coordinatorLoadStatus(key(owner), protocol::CoordinatorType::GROUP),
              ErrorCode::COORDINATOR_LOAD_IN_PROGRESS);
    EXPECT_FALSE(CoordinatorAcquisitionProbe::offsets(b)->fetchOffset(key(owner), "data", 0));
    b.replicaManager()->updateFollowerFetchOffset(tp, 0, 2);
    b.replicaManager()->maybeAdvanceHighWatermark(tp);
    // Append an uncommitted malformed tail after the inherited prefix is safe.
    storage::RecordBatch bad_tail;
    Record bad;
    bad.key = std::vector<uint8_t>{0};
    bad.value = std::vector<uint8_t>{0};
    bad_tail.addRecord(bad);
    log->appendBatch(bad_tail, false);
    ASSERT_EQ(log->highWatermark(), 2);
    b.acquireCoordinatorPartitions();
    EXPECT_EQ(b.coordinatorLoadStatus(key(owner), protocol::CoordinatorType::GROUP),
              ErrorCode::NONE);
    EXPECT_EQ(CoordinatorAcquisitionProbe::offsets(b)->fetchOffset(key(owner), "data", 0), 456);
    // A committed malformed prefix fails closed and clears stale cache state.
    log->setHighWatermark(3);
    ASSERT_EQ(current_controller->metadataController()
                  ->updatePartitionLeader("__consumer_offsets", owner, owner)
                  .error_code,
              ErrorCode::NONE);
    ASSERT_TRUE(waitUntil([&] {
        return b.metadataController()
                   ->describeTopics({"__consumer_offsets"})
                   .front()
                   .partitions[owner]
                   .leader_epoch > epoch;
    }));
    const auto next_epoch = b.metadataController()
                                ->describeTopics({"__consumer_offsets"})
                                .front()
                                .partitions[owner]
                                .leader_epoch;
    b.replicaManager()->reconcileReplica(tp, std::shared_ptr<storage::Log>(log, [](auto*) {}),
                                         owner, {owner}, next_epoch);
    b.acquireCoordinatorPartitions();
    EXPECT_EQ(b.coordinatorLoadStatus(key(owner), protocol::CoordinatorType::GROUP),
              ErrorCode::COORDINATOR_LOAD_IN_PROGRESS);
    EXPECT_FALSE(CoordinatorAcquisitionProbe::offsets(b)->fetchOffset(key(owner), "data", 0));
    // Explicit fixture repair, then acquisition retries from the source.
    log->truncateSuffix(2);
    b.acquireCoordinatorPartitions();
    EXPECT_EQ(b.coordinatorLoadStatus(key(owner), protocol::CoordinatorType::GROUP),
              ErrorCode::NONE);
    EXPECT_EQ(CoordinatorAcquisitionProbe::offsets(b)->fetchOffset(key(owner), "data", 0), 456);
    TransactionCoordinator::TxnSnapshot txn;
    txn.transactional_id = key(owner);
    txn.producer_id = 50;
    txn.producer_epoch = 3;
    txn.state = TransactionCoordinator::State::PrepareCommit;
    Record r;
    r.key = std::vector<uint8_t>(txn.transactional_id.begin(), txn.transactional_id.end());
    r.value = TransactionStateManager::serialize(txn);
    b.logManager()->getLog("__transaction_state", owner)->append({r}, true);
    b.stop();
    brokers[owner].reset();
    // Cache loss must rebuild from the manifest-bound logs.
    fs::remove_all(dir / "1/consumer_offsets");
    brokers[owner] = std::make_unique<KawasanBroker>(
        configs[owner], KawasanBroker::CoordinatorAcquisitionOnly{format});
    brokers[owner]->start();
    ASSERT_TRUE(waitUntil([&] {
        return brokers[owner]->coordinatorLoadStatus(
                   key(owner), protocol::CoordinatorType::GROUP) == ErrorCode::NONE &&
               brokers[owner]->coordinatorLoadStatus(
                   key(owner), protocol::CoordinatorType::TRANSACTION) == ErrorCode::NONE;
    }));
    auto& restarted = *brokers[owner];
    auto restored = CoordinatorAcquisitionProbe::offsets(restarted)->fetchOffsetWithMetadata(
        key(owner), "data", 0);
    ASSERT_TRUE(restored);
    EXPECT_EQ(restored->offset, 456);
    EXPECT_EQ(restored->committed_leader_epoch, 8);
    auto snapshot = CoordinatorAcquisitionProbe::transactions(restarted)->describe(key(owner));
    ASSERT_TRUE(snapshot);
    EXPECT_EQ(snapshot->state, TransactionCoordinator::State::PrepareCommit);
    EXPECT_EQ(snapshot->producer_epoch, 3);
    EXPECT_EQ(restarted.openLogCount(), 2);
    Buffer payload;
    protocol::RequestHeader(protocol::ApiKey::OFFSET_FETCH, 8, 1, "acquisition-test")
        .encode(payload);
    protocol::OffsetFetchRequest request;
    protocol::OffsetFetchRequest::Group requested;
    requested.group_id = key(owner);
    requested.fetch_all_topics = true;
    request.addGroup(requested);
    request.encode(payload, 8);
    auto wire = test_support::brokerRequest(restarted.port(), payload);
    EXPECT_EQ(wire.readInt32(), 1);
    wire.skipTaggedFields();
    protocol::OffsetFetchResponse response;
    response.decode(wire, 8);
    ASSERT_EQ(response.groups().size(), 1);
    EXPECT_EQ(response.groups()[0].group_id, key(owner));
    EXPECT_EQ(response.groups()[0].error_code, ErrorCode::COORDINATOR_LOAD_IN_PROGRESS);
    EXPECT_EQ(wire.remaining(), 0);
    // External Produce cannot mutate a format-bound coordinator source.
    auto illegal = test_support::producerBatch(99, 0);
    const auto attempted =
        test_support::brokerProduce(brokers[0]->port(), "__consumer_offsets", illegal);
    EXPECT_EQ(attempted.error_code, ErrorCode::TOPIC_AUTHORIZATION_FAILED);
    EXPECT_EQ(brokers[0]->logManager()->getLog("__consumer_offsets", 0)->logEndOffset(), 0);
    EXPECT_EQ(CoordinatorAcquisitionProbe::groups(restarted)->validateTxnOffsetCommit(
                  key(owner), 9, "member", "instance"),
              ErrorCode::NONE);
    KawasanBroker* controller = nullptr;
    ASSERT_TRUE(waitUntil([&] {
        for (auto& candidate : brokers)
            if (candidate->raftNode()->isLeader()) {
                controller = candidate.get();
                return true;
            }
        return false;
    }));
    auto fence = restarted.lockPartitionWrites({"__consumer_offsets", owner});
    ASSERT_EQ(controller->metadataController()
                  ->updatePartitionLeader("__consumer_offsets", owner, -1)
                  .error_code,
              ErrorCode::NONE);
    ASSERT_TRUE(waitUntil([&] {
        const auto md = restarted.metadataController()->describeTopics({"__consumer_offsets"});
        return md.front().partitions[owner].leader == -1;
    }));
    // Reconciliation is blocked on this partition lock; request admission
    // must still observe the committed ownership loss immediately.
    EXPECT_EQ(restarted.coordinatorLoadStatus(key(owner), protocol::CoordinatorType::GROUP),
              ErrorCode::NOT_COORDINATOR);
    ASSERT_EQ(controller->metadataController()
                  ->updatePartitionLeader("__consumer_offsets", owner, owner)
                  .error_code,
              ErrorCode::NONE);
    ASSERT_TRUE(waitUntil([&] {
        const auto md = restarted.metadataController()->describeTopics({"__consumer_offsets"});
        return md.front().partitions[owner].leader == owner;
    }));
    EXPECT_EQ(restarted.coordinatorLoadStatus(key(owner), protocol::CoordinatorType::GROUP),
              ErrorCode::COORDINATOR_LOAD_IN_PROGRESS);
    fence.unlock();
    ASSERT_TRUE(waitUntil([&] {
        return restarted.coordinatorLoadStatus(key(owner), protocol::CoordinatorType::GROUP) ==
               ErrorCode::NONE;
    }));
    EXPECT_EQ(CoordinatorAcquisitionProbe::offsets(restarted)->fetchOffset(key(owner), "data", 0),
              456);
}
}  // namespace

int main(int argc, char** argv) {
    if (argc == 5 && std::string(argv[1]) == "--crash-child") {
        crashChild(argv[2], std::stoi(argv[3]), std::stoi(argv[4]));
        return 2;
    }
    crash_executable = fs::absolute(argv[0]).string();
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
