#include <gtest/gtest.h>

#include <boost/asio.hpp>
#include <chrono>
#include <filesystem>
#include <future>
#include <iostream>
#include <thread>

#include "../sparse_broker_test_client.h"
#include "kawasan/broker/coordinator_routing.h"
#include "kawasan/broker/group_state_manager.h"
#include "kawasan/broker/kawasan_broker.h"
#include "kawasan/broker/transaction_state_manager.h"
#include "kawasan/common/logger.h"

using namespace kawasan;
using namespace kawasan::broker;
using namespace std::chrono_literals;
namespace fs = std::filesystem;
namespace asio = boost::asio;

namespace kawasan::broker {
struct CoordinatorPersistenceProbe {
    static OffsetManager& offsets(KawasanBroker& b) { return *b.offset_manager_; }
    static std::shared_ptr<OffsetManager> retainOffsets(KawasanBroker& b) {
        return b.offset_manager_;
    }
    static TransactionCoordinator& transactions(KawasanBroker& b) {
        return *b.transaction_coordinator_;
    }
    static GroupCoordinator& groups(KawasanBroker& b) { return *b.group_coordinator_; }
    static void reacquireGroupCache(KawasanBroker& b, int32_t partition) {
        const auto image = GroupStateManager(b.log_manager_.get(), b.offsets_topic_num_partitions_)
                               .loadCommittedPartition(partition);
        std::vector<GroupRecord> records;
        for (const auto& [key, record] : image) {
            (void)key;
            records.push_back(record);
        }
        b.group_coordinator_->replaceCoordinatorPartition(
            partition, b.offsets_topic_num_partitions_, records, b.offset_manager_);
    }
};
struct CoordinatorAcquisitionProbe {
    struct Runtime {
        std::chrono::steady_clock::time_point heartbeat;
        std::chrono::steady_clock::time_point rebalance;
        std::chrono::system_clock::time_point activity;
        int64_t rebalances;
        bool operator==(const Runtime&) const = default;
    };
    static Runtime runtime(GroupCoordinator& coordinator, const std::string& id) {
        std::lock_guard<std::mutex> lock(coordinator.mutex_);
        const auto& group = coordinator.groups_.at(id);
        return {group.members.at("member").last_heartbeat, group.rebalance_started_at,
                group.last_activity, group.rebalances_total.load()};
    }
    static void ageRuntime(GroupCoordinator& coordinator, const std::string& id) {
        std::lock_guard<std::mutex> lock(coordinator.mutex_);
        auto& group = coordinator.groups_.at(id);
        group.members.at("member").last_heartbeat -= 10s;
        group.rebalance_started_at -= 20s;
        group.last_activity = std::chrono::system_clock::now();
        group.rebalances_total.store(11);
    }
};
}  // namespace kawasan::broker

namespace {
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
class CoordinatorPersistenceTest : public ::testing::Test {
protected:
    fs::path dir;
    CoordinatorFormat format{1, "persistence-test", "java-byte-hash-unsigned-mod-v1", 3, 3};
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
              ("kawasan-persistence-" +
               std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    }
    void TearDown() override {
        // Keep the Raft quorum alive while stopping data-plane sweeps. Stopping
        // brokers serially first can leave the last controller waiting on
        // uncommittable UPDATE_ISR commands during fixture teardown.
        for (auto& b : brokers)
            if (b)
                b->replicaManager()->stop();
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
        c.setString("log.durability", "async");
        c.setInt("replica.lag.time.max.ms", 60000);
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
                c, KawasanBroker::CoordinatorPersistenceOnly{format}));
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

GroupRecord groupRecord(const std::string& key) {
    GroupRecord r;
    r.key.group_id = key;
    r.group = {
        7,        3,     "consumer", "range",
        "member", 60000, 123,        {{"member", "client", "host", "instance", {255}, {128}}}};
    return r;
}
GroupRecord offsetRecord(const std::string& key, int64_t offset = 42) {
    auto r = groupRecord(key);
    r.key.kind = GroupRecordKey::Kind::Offset;
    r.key.topic = "data";
    r.key.partition = 0;
    r.offset = {offset, "checkpoint", 123, 999, 4};
    return r;
}
ErrorCode put(KawasanBroker& b, const std::string& id, std::vector<GroupRecord> records,
              std::chrono::milliseconds timeout = 5s) {
    return b.mutateCoordinatorGroup(
        id,
        [&](const auto&, auto& changes) {
            changes = records;
            return ErrorCode::NONE;
        },
        timeout);
}
TransactionCoordinator::TxnSnapshot transaction(const std::string& key) {
    TransactionCoordinator::TxnSnapshot s;
    s.transactional_id = key;
    s.producer_id = 75;
    s.producer_epoch = 2;
    s.state = TransactionCoordinator::State::Ongoing;
    s.transaction_timeout_ms = 1000;
    s.state_start_time_ms = 123;
    s.partitions = {{"data", 0, 0}};
    return s;
}
ErrorCode init(KawasanBroker& b, const TransactionCoordinator::TxnSnapshot& s) {
    return b.mutateCoordinatorTransaction(s.transactional_id, std::nullopt, [&](auto& proposal) {
        proposal = s;
        return ErrorCode::NONE;
    });
}

TEST_F(CoordinatorPersistenceTest, PublicationPreservesLiveGroupsSharingThePartition) {
    KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
    b.start();
    b.reconcileReplicas();
    const auto id = key(0);
    std::string other;
    for (int n = 0;; ++n) {
        other = "other:" + std::to_string(n);
        if (coordinatorPartitionFor(other, 3) == 0)
            break;
    }
    ASSERT_EQ(coordinatorPartitionFor(other, 3), 0);
    ASSERT_EQ(put(b, id, {groupRecord(id)}), ErrorCode::NONE);
    ASSERT_EQ(put(b, other, {groupRecord(other)}), ErrorCode::NONE);
    auto& groups = CoordinatorPersistenceProbe::groups(b);
    CoordinatorAcquisitionProbe::ageRuntime(groups, id);
    CoordinatorAcquisitionProbe::ageRuntime(groups, other);
    const auto before = CoordinatorAcquisitionProbe::runtime(groups, id);
    const auto other_before = CoordinatorAcquisitionProbe::runtime(groups, other);
    ASSERT_EQ(put(b, other, {offsetRecord(other)}), ErrorCode::NONE);
    EXPECT_EQ(CoordinatorAcquisitionProbe::runtime(groups, id), before);
    EXPECT_EQ(CoordinatorAcquisitionProbe::runtime(groups, other), other_before);
    ASSERT_EQ(put(b, id, {offsetRecord(id)}), ErrorCode::NONE);
    EXPECT_EQ(CoordinatorAcquisitionProbe::runtime(groups, id), before);
    EXPECT_EQ(CoordinatorAcquisitionProbe::runtime(groups, other), other_before);
    ASSERT_EQ(b.deleteCoordinatorGroup(other), ErrorCode::NONE);
    EXPECT_EQ(CoordinatorAcquisitionProbe::runtime(groups, id), before);
    // A changed generation starts a fresh rebalance, while surviving member
    // identities retain their actual last heartbeat and cumulative counter.
    auto changed = groupRecord(id);
    ++changed.group.generation;
    ASSERT_EQ(put(b, id, {changed}), ErrorCode::NONE);
    const auto newer = CoordinatorAcquisitionProbe::runtime(groups, id);
    EXPECT_EQ(newer.heartbeat, before.heartbeat);
    EXPECT_EQ(newer.activity, before.activity);
    EXPECT_GT(newer.rebalance, before.rebalance);
    EXPECT_EQ(newer.rebalances, before.rebalances + 1);
    changed.group.members.front().group_instance_id = "replacement-instance";
    ASSERT_EQ(put(b, id, {changed}), ErrorCode::NONE);
    const auto replaced = CoordinatorAcquisitionProbe::runtime(groups, id);
    EXPECT_GT(replaced.heartbeat, newer.heartbeat);
    EXPECT_EQ(replaced.rebalance, newer.rebalance);
    EXPECT_EQ(replaced.rebalances, newer.rebalances);
    // Acquisition has different semantics: rebuild steady-clock deadlines
    // from now and keep the source's persisted activity timestamp verbatim.
    CoordinatorPersistenceProbe::reacquireGroupCache(b, 0);
    const auto acquired = CoordinatorAcquisitionProbe::runtime(groups, id);
    EXPECT_GT(acquired.heartbeat, replaced.heartbeat);
    EXPECT_GT(acquired.rebalance, replaced.rebalance);
    EXPECT_EQ(acquired.rebalances, 0);
    EXPECT_EQ(acquired.activity.time_since_epoch(),
              std::chrono::milliseconds(changed.group.last_update_timestamp));
}

TEST_F(CoordinatorPersistenceTest, ShutdownDrainsAdmittedGroupAndTransactionProposals) {
    for (bool group : {true, false}) {
        const auto cfg = config(group ? 0 : 1);
        KawasanBroker b(cfg, KawasanBroker::CoordinatorPersistenceOnly{format});
        b.start();
        b.reconcileReplicas();
        std::promise<void> entered;
        std::promise<void> release;
        auto released = release.get_future().share();
        const auto id = key(0);
        auto pending = std::async(std::launch::async, [&] {
            if (group)
                return b.mutateCoordinatorGroup(id, [&](const auto&, auto& changes) {
                    entered.set_value();
                    released.wait();
                    changes = {offsetRecord(id)};
                    return ErrorCode::NONE;
                });
            return b.mutateCoordinatorTransaction(id, std::nullopt, [&](auto& proposal) {
                entered.set_value();
                released.wait();
                proposal = transaction(id);
                return ErrorCode::NONE;
            });
        });
        const auto admitted = entered.get_future().wait_for(5s);
        if (admitted != std::future_status::ready) {
            release.set_value();
            EXPECT_EQ(pending.get(), ErrorCode::NONE);
            FAIL() << "Mutation proposal was not admitted";
        }
        auto stopped = std::async(std::launch::async, [&] { b.stop(); });
        // An admitted proposal may still inspect its source/cache. Shutdown
        // must wait until it exits, even before it has reached an ISR wait.
        EXPECT_EQ(stopped.wait_for(2s), std::future_status::timeout);
        EXPECT_NE(b.logManager()->getLog(group ? "__consumer_offsets" : "__transaction_state", 0),
                  nullptr);
        release.set_value();
        EXPECT_EQ(pending.get(), ErrorCode::NOT_COORDINATOR);
        EXPECT_EQ(stopped.wait_for(5s), std::future_status::ready);
        stopped.get();
        bool invoked = false;
        EXPECT_EQ(b.mutateCoordinatorGroup(id,
                                           [&](const auto&, auto&) {
                                               invoked = true;
                                               return ErrorCode::NONE;
                                           }),
                  ErrorCode::NOT_COORDINATOR);
        EXPECT_FALSE(invoked);
        EXPECT_EQ(b.mutateCoordinatorTransaction(id, std::nullopt,
                                                 [&](auto&) {
                                                     invoked = true;
                                                     return ErrorCode::NONE;
                                                 }),
                  ErrorCode::NOT_COORDINATOR);
        EXPECT_FALSE(invoked);
    }
}

TEST_F(CoordinatorPersistenceTest, CommitsCompleteGroupOffsetsAndExactTombstonesBeforeCache) {
    auto b = std::make_unique<KawasanBroker>(config(),
                                             KawasanBroker::CoordinatorPersistenceOnly{format});
    b->start();
    b->reconcileReplicas();
    const auto id = key(0);
    ASSERT_TRUE(waitUntil([&] {
        return b->coordinatorLoadStatus(id, protocol::CoordinatorType::GROUP) == ErrorCode::NONE;
    }));
    auto pending = offsetRecord(id, 43);
    pending.key.kind = GroupRecordKey::Kind::PendingOffset;
    pending.key.transactional_id = "txn";
    pending.key.producer_id = 75;
    pending.key.producer_epoch = 2;
    ASSERT_EQ(put(*b, id, {groupRecord(id), offsetRecord(id), pending}), ErrorCode::NONE);
    auto& offsets = CoordinatorPersistenceProbe::offsets(*b);
    ASSERT_TRUE(offsets.fetchOffsetWithMetadata(id, "data", 0));
    EXPECT_EQ(offsets.fetchOffsetWithMetadata(id, "data", 0)->committed_leader_epoch, 4);
    EXPECT_EQ(CoordinatorPersistenceProbe::groups(*b).validateTxnOffsetCommit(id, 7, "member",
                                                                              "instance"),
              ErrorCode::NONE);
    ASSERT_EQ(CoordinatorPersistenceProbe::groups(*b).pendingCoordinatorOffsets().size(), 1);
    // A successful staged mutation does not authorize legacy wire handlers.
    Buffer payload;
    protocol::RequestHeader(protocol::ApiKey::OFFSET_FETCH, 8, 1, "persistence-test")
        .encode(payload);
    protocol::OffsetFetchRequest request;
    protocol::OffsetFetchRequest::Group requested;
    requested.group_id = id;
    requested.fetch_all_topics = true;
    request.addGroup(requested);
    request.encode(payload, 8);
    auto wire = test_support::brokerRequest(b->port(), payload);
    EXPECT_EQ(wire.readInt32(), 1);
    wire.skipTaggedFields();
    protocol::OffsetFetchResponse response;
    response.decode(wire, 8);
    ASSERT_EQ(response.groups().size(), 1);
    EXPECT_EQ(response.groups()[0].error_code, ErrorCode::COORDINATOR_LOAD_IN_PROGRESS);
    EXPECT_EQ(wire.remaining(), 0);
    const auto illegal = test_support::brokerProduce(b->port(), "__consumer_offsets",
                                                     test_support::producerBatch(99, 0));
    EXPECT_NE(illegal.error_code, ErrorCode::NONE);
    ASSERT_EQ(put(*b, key(1), {offsetRecord(key(1), 999)}), ErrorCode::NONE);
    auto tombstone = offsetRecord(id);
    tombstone.tombstone = true;
    ASSERT_EQ(put(*b, id, {tombstone}), ErrorCode::NONE);
    EXPECT_FALSE(offsets.fetchOffset(id, "data", 0));
    EXPECT_EQ(CoordinatorPersistenceProbe::groups(*b).pendingCoordinatorOffsets().size(), 1);
    ASSERT_EQ(b->deleteCoordinatorGroup(id), ErrorCode::NONE);
    ASSERT_EQ(b->deleteCoordinatorGroup(id), ErrorCode::NONE);
    EXPECT_TRUE(GroupStateManager(b->logManager(), 3).loadCommittedPartition(0).empty());
    EXPECT_TRUE(CoordinatorPersistenceProbe::groups(*b).pendingCoordinatorOffsets().empty());
    EXPECT_EQ(offsets.fetchOffset(key(1), "data", 0), 999);
    b->stop();
    b.reset();
    fs::remove_all(dir / "0/consumer_offsets");
    b = std::make_unique<KawasanBroker>(config(),
                                        KawasanBroker::CoordinatorPersistenceOnly{format});
    b->start();
    b->reconcileReplicas();
    ASSERT_TRUE(waitUntil([&] {
        return b->coordinatorLoadStatus(key(1), protocol::CoordinatorType::GROUP) ==
               ErrorCode::NONE;
    }));
    EXPECT_FALSE(CoordinatorPersistenceProbe::offsets(*b).fetchOffset(id, "data", 0));
    EXPECT_EQ(CoordinatorPersistenceProbe::offsets(*b).fetchOffset(key(1), "data", 0), 999);
    b->stop();
}

TEST_F(CoordinatorPersistenceTest, CheckpointFailureRefusesAcknowledgementAndInvalidatesCache) {
    KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
    b.start();
    b.reconcileReplicas();
    const auto id = key(0);
    const TopicPartition tp{"__consumer_offsets", 0};
    ASSERT_TRUE(waitUntil([&] {
        return b.coordinatorLoadStatus(id, protocol::CoordinatorType::GROUP) == ErrorCode::NONE;
    }));
    const auto blocked = dir / "0/__consumer_offsets-0/checkpoint.meta.tmp";
    fs::create_directory(blocked);
    EXPECT_EQ(put(b, id, {offsetRecord(id)}), ErrorCode::KAFKA_STORAGE_ERROR);
    EXPECT_FALSE(CoordinatorPersistenceProbe::offsets(b).fetchOffset(id, "data", 0));
    fs::remove(blocked);
    b.acquireCoordinatorPartitions();
    ASSERT_EQ(b.coordinatorLoadStatus(id, protocol::CoordinatorType::GROUP), ErrorCode::NONE);
    // Failed acknowledgements are ambiguous: committed source can be recovered.
    EXPECT_EQ(CoordinatorPersistenceProbe::offsets(b).fetchOffset(id, "data", 0), 42);
    b.stop();
}

TEST_F(CoordinatorPersistenceTest,
       TransactionAdmissionFencesStaleSweeperAndSameEpochNewTransaction) {
    KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
    b.start();
    b.reconcileReplicas();
    const auto id = key(0);
    ASSERT_TRUE(waitUntil([&] {
        return b.coordinatorLoadStatus(id, protocol::CoordinatorType::TRANSACTION) ==
               ErrorCode::NONE;
    }));
    const auto old = transaction(id);
    ASSERT_EQ(init(b, old), ErrorCode::NONE);
    ASSERT_EQ(b.mutateCoordinatorTransaction(id, old,
                                             [](auto& s) {
                                                 ++s->producer_epoch;
                                                 s->state_start_time_ms = 456;
                                                 return ErrorCode::NONE;
                                             }),
              ErrorCode::NONE);
    bool invoked = false;
    const auto stale = [&](auto& s) {
        invoked = true;
        s->state = TransactionCoordinator::State::PrepareAbort;
        return ErrorCode::NONE;
    };
    EXPECT_EQ(b.mutateCoordinatorTransaction(id, old, stale), ErrorCode::INVALID_PRODUCER_EPOCH);
    EXPECT_FALSE(invoked);
    auto newer = CoordinatorPersistenceProbe::transactions(b).describe(id);
    ASSERT_EQ(b.mutateCoordinatorTransaction(id, newer,
                                             [](auto& s) {
                                                 ++s->state_start_time_ms;
                                                 return ErrorCode::NONE;
                                             }),
              ErrorCode::NONE);
    EXPECT_EQ(b.mutateCoordinatorTransaction(id, newer, stale), ErrorCode::INVALID_TXN_STATE);
    EXPECT_FALSE(invoked);
    auto final = CoordinatorPersistenceProbe::transactions(b).describe(id);
    ASSERT_EQ(final->state, TransactionCoordinator::State::Ongoing);
    EXPECT_EQ(final->producer_epoch, 3);
    EXPECT_EQ(TransactionStateManager(b.logManager(), 3).loadCommittedPartition(0).front(), *final);
    ASSERT_EQ(b.mutateCoordinatorTransaction(id, final,
                                             [](auto& s) {
                                                 s.reset();
                                                 return ErrorCode::NONE;
                                             }),
              ErrorCode::NONE);
    EXPECT_FALSE(CoordinatorPersistenceProbe::transactions(b).describe(id));
    EXPECT_TRUE(TransactionStateManager(b.logManager(), 3).loadCommittedPartition(0).empty());
    b.stop();
}

TEST_F(CoordinatorPersistenceTest, ConcurrentTransactionAdmissionsCannotBothPublish) {
    KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
    b.start();
    b.reconcileReplicas();
    const auto id = key(0);
    ASSERT_TRUE(waitUntil([&] {
        return b.coordinatorLoadStatus(id, protocol::CoordinatorType::TRANSACTION) ==
               ErrorCode::NONE;
    }));
    auto s = transaction(id);
    ASSERT_EQ(init(b, s), ErrorCode::NONE);
    std::promise<void> start;
    auto gate = start.get_future().share();
    auto mutate = [&] {
        gate.wait();
        return b.mutateCoordinatorTransaction(id, s, [](auto& proposal) {
            proposal->state = TransactionCoordinator::State::PrepareAbort;
            return ErrorCode::NONE;
        });
    };
    auto first = std::async(std::launch::async, mutate);
    auto second = std::async(std::launch::async, mutate);
    start.set_value();
    const auto a = first.get(), c = second.get();
    EXPECT_TRUE((a == ErrorCode::NONE && c == ErrorCode::INVALID_TXN_STATE) ||
                (c == ErrorCode::NONE && a == ErrorCode::INVALID_TXN_STATE));
    EXPECT_EQ(b.logManager()->getLog("__transaction_state", 0)->logEndOffset(), 2);
    b.stop();
}

TEST_F(CoordinatorPersistenceTest, AppendFailureLatchesPartitionUntilSourceReopen) {
    KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
    b.start();
    b.reconcileReplicas();
    const auto id = key(0);
    ASSERT_EQ(b.coordinatorLoadStatus(id, protocol::CoordinatorType::GROUP), ErrorCode::NONE);
    auto* log = b.logManager()->getLog("__consumer_offsets", 0);
    log->close();
    EXPECT_EQ(put(b, id, {offsetRecord(id)}), ErrorCode::KAFKA_STORAGE_ERROR);
    EXPECT_FALSE(CoordinatorPersistenceProbe::offsets(b).fetchOffset(id, "data", 0));
    b.acquireCoordinatorPartitions();
    EXPECT_EQ(b.coordinatorLoadStatus(id, protocol::CoordinatorType::GROUP),
              ErrorCode::COORDINATOR_LOAD_IN_PROGRESS);
    EXPECT_EQ(put(b, id, {offsetRecord(id)}), ErrorCode::COORDINATOR_LOAD_IN_PROGRESS);
    b.stop();
}

TEST_F(CoordinatorPersistenceTest, InvalidAndRejectedProposalsDoNotAppendOrPublish) {
    KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
    b.start();
    b.reconcileReplicas();
    const auto id = key(0);
    auto* log = b.logManager()->getLog("__consumer_offsets", 0);
    EXPECT_EQ(put(b, id, {offsetRecord(key(1))}), ErrorCode::INVALID_REQUEST);
    EXPECT_EQ(put(b, id, {offsetRecord(id), offsetRecord(id)}), ErrorCode::INVALID_REQUEST);
    EXPECT_EQ(b.mutateCoordinatorGroup(
                  id, [](const auto&, auto&) { return ErrorCode::ILLEGAL_GENERATION; }),
              ErrorCode::ILLEGAL_GENERATION);
    EXPECT_EQ(log->logEndOffset(), 0);
    EXPECT_FALSE(CoordinatorPersistenceProbe::offsets(b).fetchOffset(id, "data", 0));
    auto wrong = transaction(id);
    wrong.producer_epoch = -1;
    EXPECT_EQ(init(b, wrong), ErrorCode::INVALID_REQUEST);
    EXPECT_EQ(b.logManager()->getLog("__transaction_state", 0)->logEndOffset(), 0);
    EXPECT_FALSE(CoordinatorPersistenceProbe::transactions(b).describe(id));
    b.stop();
}

TEST_F(CoordinatorPersistenceTest, ThreeBrokerCommitSurvivesOwnerLossWithCompleteState) {
    startCluster();
    const int owner = 1;
    const auto id = key(owner);
    ASSERT_TRUE(waitUntil([&] {
        return brokers[owner]->coordinatorLoadStatus(id, protocol::CoordinatorType::GROUP) ==
                   ErrorCode::NONE &&
               brokers[owner]->coordinatorLoadStatus(id, protocol::CoordinatorType::TRANSACTION) ==
                   ErrorCode::NONE;
    }));
    auto& b = *brokers[owner];
    ASSERT_EQ(b.metadataController()
                  ->describeTopics({"__consumer_offsets"})
                  .front()
                  .partitions[owner]
                  .replicas.size(),
              3);
    auto pending = offsetRecord(id, 43);
    pending.key.kind = GroupRecordKey::Kind::PendingOffset;
    pending.key.transactional_id = id;
    pending.key.producer_id = 75;
    pending.key.producer_epoch = 2;
    ASSERT_EQ(put(b, id, {groupRecord(id), offsetRecord(id), pending}), ErrorCode::NONE);
    ASSERT_EQ(init(b, transaction(id)), ErrorCode::NONE);
    for (int follower : {0, 2}) {
        EXPECT_GE(
            brokers[follower]->logManager()->getLog("__consumer_offsets", owner)->logEndOffset(),
            3);
        EXPECT_GE(
            brokers[follower]->logManager()->getLog("__transaction_state", owner)->logEndOffset(),
            1);
    }
    b.stop();
    brokers[owner].reset();
    int next_owner = -1;
    ASSERT_TRUE(waitUntil([&] {
        for (int id : {0, 2}) {
            const auto md =
                brokers[id]->metadataController()->describeTopics({"__consumer_offsets"});
            if (!md.empty() && md.front().partitions[owner].leader >= 0 &&
                md.front().partitions[owner].leader != owner &&
                brokers[id]->coordinatorLoadStatus(key(owner), protocol::CoordinatorType::GROUP) ==
                    ErrorCode::NONE) {
                next_owner = id;
                return true;
            }
        }
        return false;
    }));
    auto& new_broker = *brokers[next_owner];
    const auto fetched =
        CoordinatorPersistenceProbe::offsets(new_broker).fetchOffsetWithMetadata(id, "data", 0);
    ASSERT_TRUE(fetched);
    EXPECT_EQ(fetched->offset, 42);
    EXPECT_EQ(fetched->metadata, "checkpoint");
    EXPECT_EQ(fetched->committed_leader_epoch, 4);
    const auto groups = CoordinatorPersistenceProbe::groups(new_broker).describeGroups({id});
    ASSERT_EQ(groups.size(), 1);
    ASSERT_EQ(groups[0].members.size(), 1);
    EXPECT_EQ(groups[0].members[0].member_assignment, std::vector<uint8_t>{128});
    EXPECT_EQ(CoordinatorPersistenceProbe::groups(new_broker)
                  .validateTxnOffsetCommit(id, 7, "member", "instance"),
              ErrorCode::NONE);
    ASSERT_EQ(CoordinatorPersistenceProbe::groups(new_broker).pendingCoordinatorOffsets().size(),
              1);
    ASSERT_TRUE(waitUntil([&] {
        for (int id : {0, 2})
            if (brokers[id]->coordinatorLoadStatus(
                    key(owner), protocol::CoordinatorType::TRANSACTION) == ErrorCode::NONE)
                return CoordinatorPersistenceProbe::transactions(*brokers[id])
                           .describe(key(owner)) == transaction(key(owner));
        return false;
    }));
    ASSERT_EQ(put(new_broker, id, {offsetRecord(id, 44)}), ErrorCode::NONE);
    EXPECT_EQ(CoordinatorPersistenceProbe::offsets(new_broker).fetchOffset(id, "data", 0), 44);
}

TEST_F(CoordinatorPersistenceTest, IsrTimeoutDoesNotPublishOrAdmitNewMutationsUntilTailResolves) {
    startCluster();
    const auto id = key(1);
    auto& b = *brokers[1];
    ASSERT_TRUE(waitUntil([&] {
        return b.coordinatorLoadStatus(id, protocol::CoordinatorType::GROUP) == ErrorCode::NONE;
    }));
    ASSERT_EQ(put(b, id, {offsetRecord(id, 10)}), ErrorCode::NONE);
    brokers[0]->replicaManager()->stop();
    brokers[2]->replicaManager()->stop();
    EXPECT_EQ(put(b, id, {offsetRecord(id, 11)}, 100ms), ErrorCode::REQUEST_TIMED_OUT);
    EXPECT_EQ(CoordinatorPersistenceProbe::offsets(b).fetchOffset(id, "data", 0), 10);
    b.acquireCoordinatorPartitions();
    EXPECT_EQ(b.coordinatorLoadStatus(id, protocol::CoordinatorType::GROUP),
              ErrorCode::COORDINATOR_LOAD_IN_PROGRESS);
    bool invoked = false;
    EXPECT_EQ(b.mutateCoordinatorGroup(id,
                                       [&](const auto&, auto&) {
                                           invoked = true;
                                           return ErrorCode::NONE;
                                       }),
              ErrorCode::COORDINATOR_LOAD_IN_PROGRESS);
    EXPECT_FALSE(invoked);
    brokers[0]->replicaManager()->start();
    brokers[2]->replicaManager()->start();
    ASSERT_TRUE(waitUntil([&] {
        return b.coordinatorLoadStatus(id, protocol::CoordinatorType::GROUP) == ErrorCode::NONE;
    }));
    EXPECT_EQ(CoordinatorPersistenceProbe::offsets(b).fetchOffset(id, "data", 0), 11);
    EXPECT_EQ(put(b, id, {offsetRecord(id, 12)}), ErrorCode::NONE);
}

TEST_F(CoordinatorPersistenceTest, InsufficientIsrBeforeAndAfterAppendCannotAcknowledge) {
    startCluster();
    const auto id = key(1);
    auto& b = *brokers[1];
    ASSERT_TRUE(waitUntil([&] {
        return b.coordinatorLoadStatus(id, protocol::CoordinatorType::GROUP) == ErrorCode::NONE;
    }));
    ASSERT_EQ(put(b, id, {offsetRecord(id, 10)}), ErrorCode::NONE);
    KawasanBroker* controller = nullptr;
    ASSERT_TRUE(waitUntil([&] {
        for (auto& broker : brokers)
            if (broker->raftNode()->isLeader()) {
                controller = broker.get();
                return true;
            }
        return false;
    }));
    // Raft stays live; stop only data replication/ISR maintenance for a
    // deterministic durability boundary, without a liveness election.
    for (auto& broker : brokers)
        broker->replicaManager()->stop();
    ASSERT_EQ(controller->metadataController()
                  ->updatePartitionISR("__consumer_offsets", 1, {1})
                  .error_code,
              ErrorCode::NONE);
    ASSERT_TRUE(waitUntil([&] {
        return b.metadataController()
                   ->describeTopics({"__consumer_offsets"})
                   .front()
                   .partitions[1]
                   .isr.size() == 1;
    }));
    EXPECT_EQ(put(b, id, {offsetRecord(id, 11)}), ErrorCode::NOT_ENOUGH_REPLICAS);
    EXPECT_EQ(b.logManager()->getLog("__consumer_offsets", 1)->logEndOffset(), 1);
    EXPECT_EQ(CoordinatorPersistenceProbe::offsets(b).fetchOffset(id, "data", 0), 10);
    ASSERT_EQ(controller->metadataController()
                  ->updatePartitionISR("__consumer_offsets", 1, {0, 1, 2})
                  .error_code,
              ErrorCode::NONE);
    ASSERT_TRUE(waitUntil([&] {
        return b.metadataController()
                   ->describeTopics({"__consumer_offsets"})
                   .front()
                   .partitions[1]
                   .isr.size() == 3;
    }));
    auto pending =
        std::async(std::launch::async, [&] { return put(b, id, {offsetRecord(id, 11)}); });
    ASSERT_TRUE(waitUntil(
        [&] { return b.logManager()->getLog("__consumer_offsets", 1)->logEndOffset() == 2; }));
    ASSERT_EQ(controller->metadataController()
                  ->updatePartitionISR("__consumer_offsets", 1, {1})
                  .error_code,
              ErrorCode::NONE);
    EXPECT_EQ(pending.get(), ErrorCode::NOT_ENOUGH_REPLICAS_AFTER_APPEND);
    EXPECT_NE(CoordinatorPersistenceProbe::offsets(b).fetchOffset(id, "data", 0), 11);
}

TEST_F(CoordinatorPersistenceTest, OwnershipLossDuringIsrWaitPreventsCachePublication) {
    startCluster();
    const auto id = key(1);
    auto& b = *brokers[1];
    ASSERT_TRUE(waitUntil([&] {
        return b.coordinatorLoadStatus(id, protocol::CoordinatorType::GROUP) == ErrorCode::NONE;
    }));
    ASSERT_EQ(put(b, id, {offsetRecord(id, 10)}), ErrorCode::NONE);
    brokers[0]->replicaManager()->stop();
    brokers[2]->replicaManager()->stop();
    auto pending =
        std::async(std::launch::async, [&] { return put(b, id, {offsetRecord(id, 11)}); });
    ASSERT_TRUE(waitUntil(
        [&] { return b.logManager()->getLog("__consumer_offsets", 1)->logEndOffset() == 2; }));
    KawasanBroker* controller = nullptr;
    ASSERT_TRUE(waitUntil([&] {
        for (auto& broker : brokers)
            if (broker->raftNode()->isLeader()) {
                controller = broker.get();
                return true;
            }
        return false;
    }));
    ASSERT_EQ(controller->metadataController()
                  ->updatePartitionLeader("__consumer_offsets", 1, 0)
                  .error_code,
              ErrorCode::NONE);
    EXPECT_EQ(pending.get(), ErrorCode::NOT_COORDINATOR);
    // Old owner cannot acknowledge or publish the candidate, even before poll.
    EXPECT_NE(CoordinatorPersistenceProbe::offsets(b).fetchOffset(id, "data", 0), 11);
    bool invoked = false;
    EXPECT_EQ(b.mutateCoordinatorGroup(id,
                                       [&](const auto&, auto&) {
                                           invoked = true;
                                           return ErrorCode::NONE;
                                       }),
              ErrorCode::NOT_COORDINATOR);
    EXPECT_FALSE(invoked);
}

TEST_F(CoordinatorPersistenceTest, ShutdownCancelsIsrWaitBeforeClosingStorage) {
    startCluster();
    const auto id = key(1);
    auto& b = *brokers[1];
    ASSERT_TRUE(waitUntil([&] {
        return b.coordinatorLoadStatus(id, protocol::CoordinatorType::GROUP) == ErrorCode::NONE;
    }));
    ASSERT_EQ(put(b, id, {offsetRecord(id, 10)}), ErrorCode::NONE);
    const auto prior = transaction(id);
    ASSERT_EQ(init(b, prior), ErrorCode::NONE);
    const auto retained_offsets = CoordinatorPersistenceProbe::retainOffsets(b);
    brokers[0]->replicaManager()->stop();
    brokers[2]->replicaManager()->stop();
    auto pending =
        std::async(std::launch::async, [&] { return put(b, id, {offsetRecord(id, 11)}, 30s); });
    ASSERT_TRUE(waitUntil(
        [&] { return b.logManager()->getLog("__consumer_offsets", 1)->logEndOffset() == 2; }));
    auto pending_txn = std::async(std::launch::async, [&] {
        return b.mutateCoordinatorTransaction(
            id, prior,
            [&](auto& proposal) {
                proposal->state = TransactionCoordinator::State::PrepareAbort;
                return ErrorCode::NONE;
            },
            30s);
    });
    ASSERT_TRUE(waitUntil(
        [&] { return b.logManager()->getLog("__transaction_state", 1)->logEndOffset() == 2; }));
    std::atomic<bool> invoked{false};
    auto queued = std::async(std::launch::async, [&] {
        return b.mutateCoordinatorGroup(id, [&](const auto&, auto& changes) {
            invoked.store(true);
            changes = {offsetRecord(id, 12)};
            return ErrorCode::NONE;
        });
    });
    auto stopped = std::async(std::launch::async, [&] { b.stop(); });
    EXPECT_EQ(pending.wait_for(2s), std::future_status::ready);
    EXPECT_EQ(pending.get(), ErrorCode::NOT_COORDINATOR);
    EXPECT_EQ(pending_txn.wait_for(2s), std::future_status::ready);
    EXPECT_EQ(pending_txn.get(), ErrorCode::NOT_COORDINATOR);
    EXPECT_EQ(queued.get(), ErrorCode::NOT_COORDINATOR);
    EXPECT_FALSE(invoked.load());
    EXPECT_EQ(stopped.wait_for(5s), std::future_status::ready);
    stopped.get();
    EXPECT_EQ(retained_offsets->fetchOffset(id, "data", 0), 10);
    EXPECT_EQ(CoordinatorPersistenceProbe::transactions(b).describe(id), prior);
}
}  // namespace
