#include <fcntl.h>
#include <gtest/gtest.h>
#include <sys/resource.h>
#include <unistd.h>

#include <boost/asio.hpp>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <future>
#include <iostream>
#include <limits>
#include <thread>

#include "../sparse_broker_test_client.h"
#include "kawasan/broker/coordinator_routing.h"
#include "kawasan/broker/group_state_manager.h"
#include "kawasan/broker/kawasan_broker.h"
#include "kawasan/broker/transaction_state_manager.h"
#include "kawasan/common/logger.h"
#include "kawasan/protocol/admin_misc_requests.h"
#include "kawasan/protocol/admin_stubs.h"
#include "kawasan/protocol/api_versions.h"
#include "kawasan/protocol/describe_groups_request.h"
#include "kawasan/protocol/leave_group_request.h"
#include "kawasan/protocol/txn_request.h"

using namespace kawasan;
using namespace kawasan::broker;
using namespace std::chrono_literals;
namespace fs = std::filesystem;
namespace asio = boost::asio;

namespace kawasan::broker {
struct CoordinatorExecutorProbe {
    static Buffer error(const RequestDispatcher::RequestContext& context, ErrorCode code, int16_t) {
        Buffer payload;
        payload.writeInt32(context.header.correlationId());
        payload.writeInt16(static_cast<int16_t>(code));
        return payload;
    }
    static RequestDispatcher::HandlerFunc handler(
        KawasanBroker& b, const std::string& id, RequestDispatcher::HandlerFunc work,
        bool read_only = false, protocol::CoordinatorType type = protocol::CoordinatorType::GROUP) {
        return [&b, id, work, read_only, type](auto& context) {
            return b.deferCoordinatorRequest(context, id, type, work, error, read_only);
        };
    }
    static void wire(KawasanBroker& b, const std::string& id, RequestDispatcher::HandlerFunc work) {
        // Test-only override exercises the staged seam over Heartbeat v0 bytes.
        // Production registration still refuses all staged coordinator requests.
        b.request_dispatcher_->handlers_.at(static_cast<int16_t>(protocol::ApiKey::HEARTBEAT))
            .front()
            .handler = handler(b, id, std::move(work));
    }
    static RequestDispatcher::DispatchResult dispatch(
        KawasanBroker& b, const std::string& id, RequestDispatcher::HandlerFunc work,
        std::function<void(RequestDispatcher::DispatchResult)> sink, bool read_only = false,
        protocol::CoordinatorType type = protocol::CoordinatorType::GROUP) {
        RequestDispatcher dispatcher(nullptr);
        dispatcher.registerHandler(protocol::ApiKey::HEARTBEAT, 0, 0,
                                   handler(b, id, std::move(work), read_only, type), error);
        RequestDispatcher::RequestContext context;
        context.header = {protocol::ApiKey::HEARTBEAT, 0, 71, "executor-test"};
        context.deferred_sink = std::move(sink);
        return dispatcher.dispatch(std::move(context));
    }
    static std::unique_lock<std::mutex> holdAcquisition(KawasanBroker& b) {
        return std::unique_lock<std::mutex>(b.coordinator_acquisition_mutex_);
    }
    static ErrorCode fencedStatus(KawasanBroker& b, const TopicPartition& tp, int32_t epoch) {
        auto status = ErrorCode::NONE;
        b.withCoordinatorRequestFence(tp, epoch, [&](auto error) { status = error; });
        return status;
    }
    static int64_t allocate(KawasanBroker& b) { return b.allocateNextProducerId(); }
    static std::unique_lock<std::mutex> holdProducerIds(KawasanBroker& b) {
        return std::unique_lock<std::mutex>(b.producer_id_mutex_);
    }
    static bool current(KawasanBroker& b) { return b.dataPlaneCurrent(); }

    static bool submit(KawasanBroker& b, CoordinatorExecutor::Task task) {
        return b.coordinator_executor_->submit(std::move(task));
    }
    static void scheduleTimeouts(KawasanBroker& b) { b.scheduleCoordinatorTimeouts(); }
    static void scheduleTransactionTimeouts(KawasanBroker& b) {
        b.scheduleCoordinatorTransactionTimeouts();
    }
    static bool transactionTimeoutPending(KawasanBroker& b, const std::string& id) {
        std::lock_guard<std::mutex> lock(b.coordinator_timeout_mutex_);
        return b.coordinator_transaction_timeout_pending_.contains(id);
    }
    static ErrorCode prepareTransactionTimeout(KawasanBroker& b,
                                               const TransactionCoordinator::TxnSnapshot& expected,
                                               std::chrono::milliseconds timeout = 5s) {
        return b.prepareCoordinatorTransactionTimeout(expected, timeout);
    }
    static void scheduleRetention(KawasanBroker& b) { b.scheduleCoordinatorRetention(); }
    static bool retentionPending(KawasanBroker& b, const std::string& id) {
        std::lock_guard<std::mutex> lock(b.coordinator_timeout_mutex_);
        return b.coordinator_retention_pending_.contains(id);
    }
    static RequestDispatcher::DispatchResult dispatchProtocol(
        KawasanBroker& b, protocol::ApiKey api, int16_t version, Buffer payload,
        std::function<void(RequestDispatcher::DispatchResult)> sink) {
        RequestDispatcher::RequestContext context;
        context.header = {api, version, 91, "offset-test"};
        context.payload = std::move(payload);
        context.deferred_sink = std::move(sink);
        return b.request_dispatcher_->dispatch(std::move(context));
    }

    static bool timeoutPending(KawasanBroker& b, const std::string& id) {
        std::lock_guard<std::mutex> lock(b.coordinator_timeout_mutex_);
        return b.coordinator_timeout_pending_.contains(id);
    }
    static bool stopping(KawasanBroker& b) { return b.coordinator_executor_->stopping(); }
};
struct CoordinatorPersistenceProbe {
    static OffsetManager& offsets(KawasanBroker& b) { return *b.offset_manager_; }
    static std::shared_ptr<OffsetManager> retainOffsets(KawasanBroker& b) {
        return b.offset_manager_;
    }
    static void transactionClockBeforeStart(KawasanBroker& b, TransactionCoordinator::Clock clock) {
        b.transaction_coordinator_ = std::make_unique<TransactionCoordinator>(std::move(clock));
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
    static void ageMember(GroupCoordinator& coordinator, const std::string& id,
                          const std::string& member, std::chrono::milliseconds age) {
        std::lock_guard<std::mutex> lock(coordinator.mutex_);
        coordinator.groups_.at(id).members.at(member).last_heartbeat -= age;
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
bool waitUntil(F f, std::chrono::milliseconds timeout = 20s) {
    auto until = std::chrono::steady_clock::now() + timeout;
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
        c.setInt("network.io_threads", 1);
        c.setInt("raft.port", 0);
        c.setString("log.durability", "async");
        c.setInt("replica.lag.time.max.ms", 60000);
        c.setInt("offsets.topic.num.partitions", 3);
        c.setInt("transaction.state.topic.num.partitions", 3);
        // Historical snapshot fixtures intentionally use old wall clocks. Keep
        // the automatic scan outside those tests; timeout cases invoke the real
        // scheduler explicitly, and the timer integration case uses 1000 ms.
        c.setInt("transaction.abort.timed.out.transaction.cleanup.interval.ms", 600000);
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
        // Group ownership can become readable while the transaction topic is
        // still being created and replayed on other brokers. Wait for both
        // coordinator types on every owner before mutations or replication
        // pauses: slow startup I/O can otherwise invalidate metadata freshness
        // between a test's group-only readiness check and its first append.
        ASSERT_TRUE(waitUntil([&] {
            for (int owner = 0; owner < 3; ++owner) {
                const auto id = key(owner);
                for (const auto type :
                     {protocol::CoordinatorType::GROUP, protocol::CoordinatorType::TRANSACTION}) {
                    if (brokers[owner]->coordinatorLoadStatus(id, type) != ErrorCode::NONE)
                        return false;
                }
            }
            return true;
        }));
    }
    std::string key(int p) {
        for (int n = 0;; ++n) {
            auto k = "key:" + std::to_string(n);
            if (coordinatorPartitionFor(k, 3) == p)
                return k;
        }
    }
};

// Separate cluster startup/fsync workloads into their own bounded CTest gate.
class CoordinatorClusterPersistenceTest : public CoordinatorPersistenceTest {};

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

protocol::JoinGroupRequest joinRequest(const std::string& group, const std::string& member = {}) {
    protocol::JoinGroupRequest request;
    request.setGroupId(group);
    request.setMemberId(member);
    request.setProtocolType("consumer");
    request.setGroupProtocols({{"range", {255}}});
    request.setRebalanceTimeoutMs(60000);
    return request;
}
protocol::HeartbeatRequest heartbeatRequest(const std::string& group, int generation = 7) {
    protocol::HeartbeatRequest request;
    request.setGroupId(group);
    request.setGenerationId(generation);
    request.setMemberId("member");
    return request;
}
GroupSnapshot committedGroup(KawasanBroker& b, const std::string& id) {
    auto image =
        GroupStateManager(b.logManager(), 3).loadCommittedPartition(coordinatorPartitionFor(id, 3));
    GroupRecordKey key;
    key.group_id = id;
    return image.at(key).group;
}

template <typename Response, typename Request>
Response membershipWire(KawasanBroker& b, protocol::ApiKey api, const Request& request,
                        int16_t version) {
    Buffer payload;
    protocol::RequestHeader(api, version, 91, "membership-test").encode(payload);
    request.encode(payload, version);
    auto body = test_support::brokerRequest(b.port(), payload);
    protocol::ResponseHeader header;
    header.setFlexible(
        protocol::RequestHeader(api, version, 91, "membership-test").isFlexibleResponseHeader());
    header.decode(body);
    Response response;
    response.decode(body, version);
    return response;
}

protocol::OffsetCommitRequest offsetCommit(const std::string& id, int64_t offset = 123) {
    protocol::OffsetCommitRequest request;
    request.setGroupId(id);
    request.setGenerationId(7);
    request.setMemberId("member");
    request.setGroupInstanceId("instance");
    request.setTopics({{"data", {{0, offset, 9, -1, "wire-checkpoint"}}}});
    return request;
}

Buffer deleteWire(KawasanBroker& b, protocol::ApiKey api, const std::string& id,
                  const std::vector<std::string>& groups = {}) {
    Buffer payload;
    protocol::RequestHeader(api, 0, 92, "offset-test").encode(payload);
    if (api == protocol::ApiKey::DELETE_GROUPS) {
        payload.writeInt32(static_cast<int32_t>(groups.size()));
        for (const auto& group : groups)
            payload.writeString(group);
    } else {
        payload.writeString(id);
        payload.writeInt32(1);
        payload.writeString("data");
        payload.writeInt32(1);
        payload.writeInt32(0);
    }
    auto response = test_support::brokerRequest(b.port(), payload);
    EXPECT_EQ(response.readInt32(), 92);
    return response;
}

struct ProducerIdentity {
    ErrorCode error;
    int64_t id;
    int16_t epoch;
};
Buffer producerInitPayload(const std::optional<std::string>& id, int16_t version = 4,
                           int64_t expected_id = -1, int16_t expected_epoch = -1,
                           int32_t timeout = 60000) {
    Buffer payload;
    if (version >= 2)
        payload.writeCompactNullableString(id);
    else
        payload.writeNullableString(id);
    payload.writeInt32(timeout);
    if (version >= 3) {
        payload.writeInt64(expected_id);
        payload.writeInt16(expected_epoch);
    }
    if (version >= 2)
        payload.writeEmptyTaggedFields();
    return payload;
}
ProducerIdentity producerIdentity(Buffer response, int16_t version = 4) {
    EXPECT_EQ(response.readInt32(), 91);
    if (version >= 2)
        response.skipTaggedFields();
    EXPECT_EQ(response.readInt32(), 0);
    const auto error = static_cast<ErrorCode>(response.readInt16());
    const auto id = response.readInt64();
    const auto epoch = response.readInt16();
    if (version >= 2)
        response.skipTaggedFields();
    EXPECT_EQ(response.remaining(), 0);
    return {error, id, epoch};
}
ProducerIdentity producerInitWire(KawasanBroker& b, const std::optional<std::string>& id,
                                  int16_t version = 4, int64_t expected_id = -1,
                                  int16_t expected_epoch = -1, int32_t timeout = 60000) {
    Buffer request;
    protocol::RequestHeader(protocol::ApiKey::INIT_PRODUCER_ID, version, 91, "identity-test")
        .encode(request);
    const auto payload = producerInitPayload(id, version, expected_id, expected_epoch, timeout);
    request.writeBytes(payload.data(), payload.size());
    return producerIdentity(test_support::brokerRequest(b.port(), request), version);
}

protocol::AddPartitionsToTxnRequest addPartitions(const std::string& id, ProducerIdentity producer,
                                                  std::vector<int32_t> partitions = {0}) {
    protocol::AddPartitionsToTxnRequest request;
    request.setTransactionalId(id);
    request.setProducerId(producer.id);
    request.setProducerEpoch(producer.epoch);
    request.addTopic({"data", std::move(partitions)});
    return request;
}
protocol::AddOffsetsToTxnRequest addOffsets(const std::string& id, ProducerIdentity producer,
                                            const std::string& group) {
    protocol::AddOffsetsToTxnRequest request;
    request.setTransactionalId(id);
    request.setProducerId(producer.id);
    request.setProducerEpoch(producer.epoch);
    request.setGroupId(group);
    return request;
}
void createData(KawasanBroker& b) {
    TopicSpecification spec;
    spec.name = "data";
    spec.num_partitions = 2;
    spec.replication_factor = 1;
    ASSERT_EQ(b.metadataController()->createTopic(spec).error_code, ErrorCode::NONE);
    b.reconcileReplicas();
}
TEST_F(CoordinatorPersistenceTest, TransactionRegistrationWireCommitsAndPreservesFirstOffsets) {
    KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
    b.start();
    b.reconcileReplicas();
    createData(b);
    const auto id = key(0);
    auto producer = producerInitWire(b, id);
    ASSERT_EQ(producer.error, ErrorCode::NONE);
    producer = producerInitWire(b, id, 4, producer.id, producer.epoch);
    ASSERT_EQ(producer.error, ErrorCode::NONE);
    const auto before = *CoordinatorPersistenceProbe::transactions(b).describe(id);
    auto response = membershipWire<protocol::AddPartitionsToTxnResponse>(
        b, protocol::ApiKey::ADD_PARTITIONS_TO_TXN, addPartitions(id, producer, {0, 0}), 3);
    ASSERT_EQ(response.topics().size(), 1u);
    EXPECT_EQ(response.topics()[0].partitions[0].error_code, ErrorCode::NONE);
    auto committed = *CoordinatorPersistenceProbe::transactions(b).describe(id);
    EXPECT_EQ(committed.state, TransactionCoordinator::State::Ongoing);
    EXPECT_EQ(committed.partitions,
              (std::vector<TransactionCoordinator::TxnPartition>{{"data", 0, 0}}));
    EXPECT_EQ(committed.previous_producer_id, before.previous_producer_id);
    EXPECT_EQ(committed.previous_producer_epoch, before.previous_producer_epoch);
    auto* data = b.logManager()->getLog("data", 0);
    data->append({Record("advance", "value")});
    auto* source = b.logManager()->getLog("__transaction_state", 0);
    const auto end = source->logEndOffset();
    for (int16_t version = 0; version <= 3; ++version) {
        response = membershipWire<protocol::AddPartitionsToTxnResponse>(
            b, protocol::ApiKey::ADD_PARTITIONS_TO_TXN, addPartitions(id, producer), version);
        EXPECT_EQ(response.topics()[0].partitions[0].error_code, ErrorCode::NONE);
        EXPECT_EQ(*CoordinatorPersistenceProbe::transactions(b).describe(id), committed);
        EXPECT_EQ(source->logEndOffset(), end);
    }
    const auto group = key(2);  // group source is independent of transaction source
    for (int16_t version = 0; version <= 3; ++version) {
        auto offsets = membershipWire<protocol::AddOffsetsToTxnResponse>(
            b, protocol::ApiKey::ADD_OFFSETS_TO_TXN, addOffsets(id, producer, group), version);
        EXPECT_EQ(offsets.errorCode(), ErrorCode::NONE);
    }
    const auto registered = *CoordinatorPersistenceProbe::transactions(b).describe(id);
    EXPECT_EQ(registered.state_start_time_ms, committed.state_start_time_ms);
    EXPECT_EQ(registered.partitions, (std::vector<TransactionCoordinator::TxnPartition>{
                                         {"data", 0, 0}, {"__consumer_offsets", 2, -1}}));
    EXPECT_EQ(source->logEndOffset(), end + 1);
    EXPECT_EQ(TransactionStateManager(b.logManager(), 3).loadCommittedPartition(0)[0], registered);
}
TEST_F(CoordinatorPersistenceTest,
       TransactionRegistrationRejectsUnknownPartitionsWithoutGhostLogs) {
    KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
    b.start();
    b.reconcileReplicas();
    createData(b);
    const auto id = key(0);
    const auto producer = producerInitWire(b, id);
    ASSERT_EQ(producer.error, ErrorCode::NONE);
    const auto before = CoordinatorPersistenceProbe::transactions(b).describe(id);
    auto request = addPartitions(id, producer, {0, -1, 2});
    request.addTopic({"missing", {0}});
    request.addTopic({"__transaction_state", {0}});
    const auto result = membershipWire<protocol::AddPartitionsToTxnResponse>(
        b, protocol::ApiKey::ADD_PARTITIONS_TO_TXN, request, 3);
    ASSERT_EQ(result.topics().size(), 3u);
    ASSERT_EQ(result.topics()[0].partitions.size(), 3u);
    EXPECT_EQ(result.topics()[0].partitions[0].error_code, ErrorCode::OPERATION_NOT_ATTEMPTED);
    EXPECT_EQ(result.topics()[0].partitions[1].error_code, ErrorCode::UNKNOWN_TOPIC_OR_PARTITION);
    EXPECT_EQ(result.topics()[0].partitions[2].error_code, ErrorCode::UNKNOWN_TOPIC_OR_PARTITION);
    EXPECT_EQ(result.topics()[1].partitions[0].error_code, ErrorCode::UNKNOWN_TOPIC_OR_PARTITION);
    EXPECT_EQ(result.topics()[2].partitions[0].error_code, ErrorCode::TOPIC_AUTHORIZATION_FAILED);
    EXPECT_EQ(CoordinatorPersistenceProbe::transactions(b).describe(id), before);
    EXPECT_EQ(b.logManager()->getLog("__transaction_state", 0)->logEndOffset(), 1);
    EXPECT_FALSE(b.logManager()->getLog("missing", 0));
    EXPECT_FALSE(b.logManager()->getLog("data", -1));
    EXPECT_FALSE(b.logManager()->getLog("data", 2));
    EXPECT_FALSE(fs::exists(dir / "0/missing-0"));
}

TEST_F(CoordinatorPersistenceTest, TransactionRegistrationWireFencesProducerAndPrepareStates) {
    KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
    b.start();
    b.reconcileReplicas();
    createData(b);
    const auto id = key(0);
    auto producer = producerInitWire(b, id);
    ASSERT_EQ(producer.error, ErrorCode::NONE);
    auto* source = b.logManager()->getLog("__transaction_state", 0);
    auto check = [&](const std::string& txn, ProducerIdentity identity, int16_t version,
                     ErrorCode expected) {
        const auto before = CoordinatorPersistenceProbe::transactions(b).describe(id);
        const auto end = source->logEndOffset();
        const auto parts = membershipWire<protocol::AddPartitionsToTxnResponse>(
            b, protocol::ApiKey::ADD_PARTITIONS_TO_TXN, addPartitions(txn, identity), version);
        ASSERT_EQ(parts.topics().size(), 1u);
        EXPECT_EQ(parts.topics()[0].partitions[0].error_code, expected);
        const auto offsets = membershipWire<protocol::AddOffsetsToTxnResponse>(
            b, protocol::ApiKey::ADD_OFFSETS_TO_TXN, addOffsets(txn, identity, key(1)), version);
        EXPECT_EQ(offsets.errorCode(), expected);
        EXPECT_EQ(CoordinatorPersistenceProbe::transactions(b).describe(id), before);
        EXPECT_EQ(source->logEndOffset(), end);
    };
    for (int16_t version = 0; version <= 3; ++version) {
        check("", producer, version, ErrorCode::INVALID_REQUEST);
        check("unknown", producer, version, ErrorCode::INVALID_PRODUCER_ID_MAPPING);
        check(id, {ErrorCode::NONE, -1, 0}, version, ErrorCode::INVALID_REQUEST);
        check(id, {ErrorCode::NONE, producer.id, -1}, version, ErrorCode::INVALID_REQUEST);
        check(id, {ErrorCode::NONE, producer.id + 1, 0}, version,
              ErrorCode::INVALID_PRODUCER_ID_MAPPING);
        check(id, {ErrorCode::NONE, producer.id, 10}, version,
              version < 2 ? ErrorCode::INVALID_PRODUCER_EPOCH : ErrorCode::PRODUCER_FENCED);
    }
    producer = producerInitWire(b, id, 4, producer.id, producer.epoch);
    ASSERT_EQ(producer.error, ErrorCode::NONE);
    check(id, {ErrorCode::NONE, producer.id, 0}, 3, ErrorCode::PRODUCER_FENCED);
    using State = TransactionCoordinator::State;
    for (auto state :
         {State::PrepareCommit, State::PrepareAbort, State::PrepareEpochFence, State::Dead}) {
        const auto before = CoordinatorPersistenceProbe::transactions(b).describe(id);
        ASSERT_EQ(b.mutateCoordinatorTransaction(id, before,
                                                 [&](auto& draft) {
                                                     draft->state = state;
                                                     draft->partitions = {{"data", 0, 17}};
                                                     draft->pending_offsets = {
                                                         {key(1), "data", 0, 41, "pending", 8}};
                                                     return ErrorCode::NONE;
                                                 }),
                  ErrorCode::NONE);
        check(id, producer, 3,
              state == State::Dead ? ErrorCode::INVALID_TXN_STATE
                                   : ErrorCode::CONCURRENT_TRANSACTIONS);
    }
    for (auto state : {State::Empty, State::CompleteCommit, State::CompleteAbort}) {
        const auto before = CoordinatorPersistenceProbe::transactions(b).describe(id);
        ASSERT_EQ(b.mutateCoordinatorTransaction(id, before,
                                                 [&](auto& draft) {
                                                     draft->state = state;
                                                     draft->partitions.clear();
                                                     draft->pending_offsets.clear();
                                                     draft->state_start_time_ms = 123;
                                                     return ErrorCode::NONE;
                                                 }),
                  ErrorCode::NONE);
        const auto offsets = membershipWire<protocol::AddOffsetsToTxnResponse>(
            b, protocol::ApiKey::ADD_OFFSETS_TO_TXN, addOffsets(id, producer, key(1)), 3);
        EXPECT_EQ(offsets.errorCode(), ErrorCode::NONE);
        auto ongoing = *CoordinatorPersistenceProbe::transactions(b).describe(id);
        EXPECT_EQ(ongoing.state, State::Ongoing);
        EXPECT_GT(ongoing.state_start_time_ms, 123);
        ASSERT_EQ(b.mutateCoordinatorTransaction(id, ongoing,
                                                 [&](auto& draft) {
                                                     draft->pending_offsets = {
                                                         {key(1), "data", 0, 41, "pending", 8}};
                                                     return ErrorCode::NONE;
                                                 }),
                  ErrorCode::NONE);
        ongoing = *CoordinatorPersistenceProbe::transactions(b).describe(id);
        EXPECT_EQ(membershipWire<protocol::AddPartitionsToTxnResponse>(
                      b, protocol::ApiKey::ADD_PARTITIONS_TO_TXN, addPartitions(id, producer), 3)
                      .topics()[0]
                      .partitions[0]
                      .error_code,
                  ErrorCode::NONE);
        const auto registered = *CoordinatorPersistenceProbe::transactions(b).describe(id);
        EXPECT_EQ(registered.pending_offsets, ongoing.pending_offsets);
        EXPECT_EQ(registered.state_start_time_ms, ongoing.state_start_time_ms);
        EXPECT_EQ(registered.previous_producer_epoch, ongoing.previous_producer_epoch);
    }
}

TEST_F(CoordinatorPersistenceTest,
       TransactionRegistrationAcceptsUserTopicWithDoubleUnderscorePrefix) {
    KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
    b.start();
    b.reconcileReplicas();
    TopicSpecification spec;
    spec.name = "__user_data";
    spec.num_partitions = 1;
    spec.replication_factor = 1;
    ASSERT_EQ(b.metadataController()->createTopic(spec).error_code, ErrorCode::NONE);
    b.reconcileReplicas();
    const auto id = key(0);
    const auto producer = producerInitWire(b, id);
    ASSERT_EQ(producer.error, ErrorCode::NONE);
    auto request = addPartitions(id, producer, {});
    request.addTopic({spec.name, {0}});
    const auto response = membershipWire<protocol::AddPartitionsToTxnResponse>(
        b, protocol::ApiKey::ADD_PARTITIONS_TO_TXN, request, 3);
    EXPECT_EQ(response.topics()[1].partitions[0].error_code, ErrorCode::NONE);
    const auto snapshot = *CoordinatorPersistenceProbe::transactions(b).describe(id);
    EXPECT_EQ(snapshot.partitions,
              (std::vector<TransactionCoordinator::TxnPartition>{{spec.name, 0, 0}}));
}

TEST_F(CoordinatorPersistenceTest, TransactionRegistrationCheckpointFailurePreservesSnapshot) {
    KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
    b.start();
    b.reconcileReplicas();
    createData(b);
    const auto id = key(0);
    const auto producer = producerInitWire(b, id);
    ASSERT_EQ(producer.error, ErrorCode::NONE);
    const auto blocked = dir / "0/__transaction_state-0/checkpoint.meta.tmp";
    for (auto api :
         {protocol::ApiKey::ADD_PARTITIONS_TO_TXN, protocol::ApiKey::ADD_OFFSETS_TO_TXN}) {
        const auto before = CoordinatorPersistenceProbe::transactions(b).describe(id);
        fs::create_directory(blocked);
        if (api == protocol::ApiKey::ADD_PARTITIONS_TO_TXN) {
            const auto result = membershipWire<protocol::AddPartitionsToTxnResponse>(
                b, api, addPartitions(id, producer), 3);
            EXPECT_EQ(result.topics()[0].partitions[0].error_code, ErrorCode::KAFKA_STORAGE_ERROR);
        } else {
            const auto result = membershipWire<protocol::AddOffsetsToTxnResponse>(
                b, api, addOffsets(id, producer, key(1)), 3);
            EXPECT_EQ(result.errorCode(), ErrorCode::KAFKA_STORAGE_ERROR);
        }
        EXPECT_EQ(CoordinatorPersistenceProbe::transactions(b).describe(id), before);
        fs::remove(blocked);
        b.reconcileReplicas();
        ASSERT_TRUE(waitUntil([&] {
            return b.coordinatorLoadStatus(id, protocol::CoordinatorType::TRANSACTION) ==
                   ErrorCode::NONE;
        }));
        // The attempted append may have committed despite a failed checkpoint;
        // acquisition recovers it before a retry can acknowledge that snapshot.
        EXPECT_EQ(CoordinatorPersistenceProbe::transactions(b).describe(id)->state,
                  TransactionCoordinator::State::Ongoing);
    }
}

TEST_F(CoordinatorPersistenceTest, TransactionRegistrationSourceReadFailureLatchesSource) {
    KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
    b.start();
    b.reconcileReplicas();
    createData(b);
    for (int partition = 0; partition < 2; ++partition) {
        const auto id = key(partition);
        const auto producer = producerInitWire(b, id);
        ASSERT_EQ(producer.error, ErrorCode::NONE);
        const auto before = CoordinatorPersistenceProbe::transactions(b).describe(id);
        b.logManager()->getLog("__transaction_state", partition)->close();
        if (partition == 0) {
            const auto result = membershipWire<protocol::AddPartitionsToTxnResponse>(
                b, protocol::ApiKey::ADD_PARTITIONS_TO_TXN, addPartitions(id, producer), 0);
            EXPECT_EQ(result.topics()[0].partitions[0].error_code, ErrorCode::KAFKA_STORAGE_ERROR);
        } else {
            const auto result = membershipWire<protocol::AddOffsetsToTxnResponse>(
                b, protocol::ApiKey::ADD_OFFSETS_TO_TXN, addOffsets(id, producer, key(2)), 0);
            EXPECT_EQ(result.errorCode(), ErrorCode::KAFKA_STORAGE_ERROR);
        }
        EXPECT_EQ(CoordinatorPersistenceProbe::transactions(b).describe(id), before);
        b.acquireCoordinatorPartitions();
        EXPECT_EQ(b.coordinatorLoadStatus(id, protocol::CoordinatorType::TRANSACTION),
                  ErrorCode::COORDINATOR_LOAD_IN_PROGRESS);
    }
}

// Fault the actual RocksDB WAL descriptor, without a production test hook.
// A read-only replacement fails write; a pipe accepts write but refuses fsync.
int sourceWalDescriptor(const fs::path& source) {
    struct rlimit limit {};
    if (::getrlimit(RLIMIT_NOFILE, &limit) != 0)
        return -1;
    for (int fd = 0; fd < static_cast<int>(std::min<rlim_t>(limit.rlim_cur, 65536)); ++fd) {
        const auto flags = ::fcntl(fd, F_GETFL);
        if (flags == -1 || (flags & O_ACCMODE) == O_RDONLY)
            continue;
        fs::path path;
#ifdef __APPLE__
        std::array<char, 4096> name{};
        if (::fcntl(fd, F_GETPATH, name.data()) == -1)
            continue;
        path = name.data();
#else
        std::error_code error;
        path = fs::read_symlink("/proc/self/fd/" + std::to_string(fd), error);
        if (error)
            continue;
#endif
        if (path.extension() == ".log" && path.string().starts_with(source.string() + "/"))
            return fd;
    }
    return -1;
}
TEST_F(CoordinatorPersistenceTest,
       TransactionRegistrationWalWriteAndSyncFailuresRefuseAcknowledgement) {
    for (bool fail_sync : {false, true}) {
        auto cfg = config();
        const auto root = fs::canonical(fs::temp_directory_path()) / dir.filename() /
                          (fail_sync ? "sync-failure" : "write-failure");
        cfg.setString("log.dirs", root.string());
        KawasanBroker b(cfg, KawasanBroker::CoordinatorPersistenceOnly{format});
        b.start();
        b.reconcileReplicas();
        createData(b);
        for (int partition = 0; partition < 2; ++partition) {
            const auto id = key(partition);
            const auto producer = producerInitWire(b, id);
            ASSERT_EQ(producer.error, ErrorCode::NONE);
            const auto before = CoordinatorPersistenceProbe::transactions(b).describe(id);
            const auto source = root / ("__transaction_state-" + std::to_string(partition));
            const int wal = sourceWalDescriptor(source);
            ASSERT_GE(wal, 0);
            struct RestoreDescriptor {
                int wal;
                int original;
                std::array<int, 2> pipe{-1, -1};
                int replacement = -1;
                ~RestoreDescriptor() {
                    if (original >= 0) {
                        ::dup2(original, wal);
                        ::close(original);
                    }
                    if (replacement >= 0)
                        ::close(replacement);
                    for (const int fd : pipe)
                        if (fd >= 0)
                            ::close(fd);
                }
            } restore{wal, ::dup(wal)};
            ASSERT_GE(restore.original, 0);
            if (fail_sync) {
                ASSERT_EQ(::pipe(restore.pipe.data()), 0);
                ASSERT_GE(::dup2(restore.pipe[1], wal), 0);
            } else {
                restore.replacement = ::open("/dev/null", O_RDONLY);
                ASSERT_GE(restore.replacement, 0);
                ASSERT_GE(::dup2(restore.replacement, wal), 0);
            }
            if (partition == 0) {
                const auto result = membershipWire<protocol::AddPartitionsToTxnResponse>(
                    b, protocol::ApiKey::ADD_PARTITIONS_TO_TXN, addPartitions(id, producer), 3);
                EXPECT_EQ(result.topics()[0].partitions[0].error_code,
                          ErrorCode::KAFKA_STORAGE_ERROR);
            } else {
                const auto result = membershipWire<protocol::AddOffsetsToTxnResponse>(
                    b, protocol::ApiKey::ADD_OFFSETS_TO_TXN, addOffsets(id, producer, key(2)), 3);
                EXPECT_EQ(result.errorCode(), ErrorCode::KAFKA_STORAGE_ERROR);
            }
            EXPECT_EQ(CoordinatorPersistenceProbe::transactions(b).describe(id), before);
            EXPECT_EQ(b.logManager()->getLog("__transaction_state", partition)->logEndOffset(), 1);
            b.acquireCoordinatorPartitions();
            EXPECT_EQ(b.coordinatorLoadStatus(id, protocol::CoordinatorType::TRANSACTION),
                      ErrorCode::COORDINATOR_LOAD_IN_PROGRESS);
        }
    }
}

TEST_F(CoordinatorPersistenceTest, TransactionRegistrationWireRestartRebuildsParticipation) {
    const auto id = key(0);
    ProducerIdentity producer{};
    std::optional<TransactionCoordinator::TxnSnapshot> snapshot;
    {
        KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
        b.start();
        b.reconcileReplicas();
        createData(b);
        producer = producerInitWire(b, id);
        ASSERT_EQ(producer.error, ErrorCode::NONE);
        EXPECT_EQ(membershipWire<protocol::AddPartitionsToTxnResponse>(
                      b, protocol::ApiKey::ADD_PARTITIONS_TO_TXN, addPartitions(id, producer), 3)
                      .topics()[0]
                      .partitions[0]
                      .error_code,
                  ErrorCode::NONE);
        EXPECT_EQ(membershipWire<protocol::AddOffsetsToTxnResponse>(
                      b, protocol::ApiKey::ADD_OFFSETS_TO_TXN, addOffsets(id, producer, key(2)), 3)
                      .errorCode(),
                  ErrorCode::NONE);
        snapshot = CoordinatorPersistenceProbe::transactions(b).describe(id);
        b.stop();
    }
    fs::remove_all(dir / "0/consumer_offsets");
    KawasanBroker recovered(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
    recovered.start();
    recovered.reconcileReplicas();
    EXPECT_EQ(CoordinatorPersistenceProbe::transactions(recovered).describe(id), snapshot);
    const auto end = recovered.logManager()->getLog("__transaction_state", 0)->logEndOffset();
    EXPECT_EQ(
        membershipWire<protocol::AddPartitionsToTxnResponse>(
            recovered, protocol::ApiKey::ADD_PARTITIONS_TO_TXN, addPartitions(id, producer), 0)
            .topics()[0]
            .partitions[0]
            .error_code,
        ErrorCode::NONE);
    EXPECT_EQ(
        membershipWire<protocol::AddOffsetsToTxnResponse>(
            recovered, protocol::ApiKey::ADD_OFFSETS_TO_TXN, addOffsets(id, producer, key(2)), 0)
            .errorCode(),
        ErrorCode::NONE);
    EXPECT_EQ(recovered.logManager()->getLog("__transaction_state", 0)->logEndOffset(), end);
    // Completion remains gated: registration does not silently run markers.
    Buffer end_payload;
    end_payload.writeCompactString(id);
    end_payload.writeInt64(producer.id);
    end_payload.writeInt16(producer.epoch);
    end_payload.writeInt8(1);
    end_payload.writeEmptyTaggedFields();
    std::promise<RequestDispatcher::DispatchResult> delivered;
    const auto result = CoordinatorExecutorProbe::dispatchProtocol(
        recovered, protocol::ApiKey::END_TXN, 3, std::move(end_payload),
        [&](auto response) { delivered.set_value(std::move(response)); });
    EXPECT_FALSE(result.deferred);
    EXPECT_EQ(CoordinatorPersistenceProbe::transactions(recovered).describe(id), snapshot);
}

TEST_F(CoordinatorClusterPersistenceTest,
       ThreeBrokerTransactionRegistrationWireWaitsForIsrAndKeepsIoLive) {
    startCluster();
    KawasanBroker* controller = nullptr;
    ASSERT_TRUE(waitUntil([&] {
        for (auto& broker : brokers)
            if (broker->raftNode()->isLeader())
                controller = broker.get();
        return controller != nullptr;
    }));
    createData(*controller);
    for (auto& broker : brokers)
        broker->reconcileReplicas();
    const auto id = key(1);
    auto& b = *brokers[1];
    // Isolate exact wire success from the periodic acquisition try-lock.
    b.replicaManager()->stop();
    const auto producer = producerInitWire(b, id);
    ASSERT_EQ(producer.error, ErrorCode::NONE);
    for (int wrong : {0, 2}) {
        const auto parts = membershipWire<protocol::AddPartitionsToTxnResponse>(
            *brokers[wrong], protocol::ApiKey::ADD_PARTITIONS_TO_TXN,
            addPartitions(id, producer, {0, 1}), 3);
        EXPECT_EQ(parts.topics()[0].partitions[0].error_code, ErrorCode::NOT_COORDINATOR);
        EXPECT_EQ(membershipWire<protocol::AddOffsetsToTxnResponse>(
                      *brokers[wrong], protocol::ApiKey::ADD_OFFSETS_TO_TXN,
                      addOffsets(id, producer, key(2)), 3)
                      .errorCode(),
                  ErrorCode::NOT_COORDINATOR);
    }
    for (auto api :
         {protocol::ApiKey::ADD_PARTITIONS_TO_TXN, protocol::ApiKey::ADD_OFFSETS_TO_TXN}) {
        const auto before = CoordinatorPersistenceProbe::transactions(b).describe(id);
        const auto end = b.logManager()->getLog("__transaction_state", 1)->logEndOffset();
        brokers[0]->replicaManager()->stop();
        brokers[2]->replicaManager()->stop();
        auto pending = std::async(std::launch::async, [&] {
            if (api == protocol::ApiKey::ADD_PARTITIONS_TO_TXN)
                return membershipWire<protocol::AddPartitionsToTxnResponse>(
                           b, api, addPartitions(id, producer, {0, 1}), 3)
                    .topics()[0]
                    .partitions[0]
                    .error_code;
            return membershipWire<protocol::AddOffsetsToTxnResponse>(
                       b, api, addOffsets(id, producer, key(2)), 3)
                .errorCode();
        });
        EXPECT_TRUE(waitUntil([&] {
            return b.logManager()->getLog("__transaction_state", 1)->logEndOffset() == end + 1;
        }));
        EXPECT_EQ(pending.wait_for(50ms), std::future_status::timeout);
        EXPECT_EQ(CoordinatorPersistenceProbe::transactions(b).describe(id), before);
        Buffer versions;
        protocol::RequestHeader(protocol::ApiKey::API_VERSIONS, 1, 95, "io-progress")
            .encode(versions);
        protocol::ApiVersionsRequest{}.encode(versions, 1);
        auto progress = test_support::brokerRequest(b.port(), versions);
        EXPECT_EQ(progress.readInt32(), 95);
        brokers[0]->replicaManager()->start();
        brokers[2]->replicaManager()->start();
        ASSERT_EQ(pending.wait_for(10s), std::future_status::ready);
        EXPECT_EQ(pending.get(), ErrorCode::NONE);
        ASSERT_TRUE(waitUntil([&] {
            for (auto& broker : brokers)
                if (broker->logManager()->getLog("__transaction_state", 1)->highWatermark() <
                    end + 1)
                    return false;
            return true;
        }));
        for (auto& broker : brokers)
            EXPECT_EQ(TransactionStateManager(broker->logManager(), 3).loadCommittedPartition(1)[0],
                      *CoordinatorPersistenceProbe::transactions(b).describe(id));
    }
    const auto snapshot = *CoordinatorPersistenceProbe::transactions(b).describe(id);
    EXPECT_EQ(snapshot.partitions,
              (std::vector<TransactionCoordinator::TxnPartition>{
                  {"data", 0, -1}, {"data", 1, 0}, {"__consumer_offsets", 2, -1}}));
    // Resume replication before moving this source to another owner.
    b.replicaManager()->start();
    // Move ownership through an actual Raft command and acquire the committed image.
    ASSERT_EQ(controller->metadataController()
                  ->updatePartitionLeader("__transaction_state", 1, 2)
                  .error_code,
              ErrorCode::NONE);
    ASSERT_TRUE(waitUntil([&] {
        return brokers[2]->coordinatorLoadStatus(id, protocol::CoordinatorType::TRANSACTION) ==
               ErrorCode::NONE;
    }));
    brokers[2]->replicaManager()->stop();
    EXPECT_EQ(CoordinatorPersistenceProbe::transactions(*brokers[2]).describe(id), snapshot);
    EXPECT_EQ(membershipWire<protocol::AddPartitionsToTxnResponse>(
                  *brokers[2], protocol::ApiKey::ADD_PARTITIONS_TO_TXN,
                  addPartitions(id, producer, {0, 1}), 3)
                  .topics()[0]
                  .partitions[0]
                  .error_code,
              ErrorCode::NONE);
}

TEST_F(CoordinatorClusterPersistenceTest,
       ThreeBrokerRegistrationPropagatesMinIsrAndTimeoutFailures) {
    startCluster();
    KawasanBroker* controller = nullptr;
    ASSERT_TRUE(waitUntil([&] {
        for (auto& broker : brokers)
            if (broker->raftNode()->isLeader())
                controller = broker.get();
        return controller != nullptr;
    }));
    createData(*controller);
    const auto id = key(1);
    auto& b = *brokers[1];
    const auto producer = producerInitWire(b, id);
    ASSERT_EQ(producer.error, ErrorCode::NONE);
    const auto before = CoordinatorPersistenceProbe::transactions(b).describe(id);
    // Manual ISR transitions must not race the owner's automatic ISR expansion.
    for (auto& broker : brokers)
        broker->replicaManager()->stop();
    ASSERT_EQ(controller->metadataController()
                  ->updatePartitionISR("__transaction_state", 1, {1})
                  .error_code,
              ErrorCode::NONE);
    ASSERT_TRUE(waitUntil([&] {
        b.reconcileReplicas();
        return b.replicaManager()->getISR({"__transaction_state", 1}).size() == 1;
    }));
    EXPECT_EQ(membershipWire<protocol::AddPartitionsToTxnResponse>(
                  b, protocol::ApiKey::ADD_PARTITIONS_TO_TXN, addPartitions(id, producer), 3)
                  .topics()[0]
                  .partitions[0]
                  .error_code,
              ErrorCode::NOT_ENOUGH_REPLICAS);
    EXPECT_EQ(membershipWire<protocol::AddOffsetsToTxnResponse>(
                  b, protocol::ApiKey::ADD_OFFSETS_TO_TXN, addOffsets(id, producer, key(0)), 3)
                  .errorCode(),
              ErrorCode::NOT_ENOUGH_REPLICAS);
    EXPECT_EQ(CoordinatorPersistenceProbe::transactions(b).describe(id), before);
    ASSERT_EQ(controller->metadataController()
                  ->updatePartitionISR("__transaction_state", 1, {0, 1, 2})
                  .error_code,
              ErrorCode::NONE);
    ASSERT_TRUE(waitUntil([&] {
        b.reconcileReplicas();
        return b.replicaManager()->getISR({"__transaction_state", 1}).size() == 3;
    }));
    EXPECT_EQ(membershipWire<protocol::AddPartitionsToTxnResponse>(
                  b, protocol::ApiKey::ADD_PARTITIONS_TO_TXN, addPartitions(id, producer), 3)
                  .topics()[0]
                  .partitions[0]
                  .error_code,
              ErrorCode::REQUEST_TIMED_OUT);
    EXPECT_EQ(CoordinatorPersistenceProbe::transactions(b).describe(id), before);
    for (auto& broker : brokers)
        broker->replicaManager()->start();
    ASSERT_TRUE(waitUntil([&] {
        return b.coordinatorLoadStatus(id, protocol::CoordinatorType::TRANSACTION) ==
               ErrorCode::NONE;
    }));
    const auto ongoing = CoordinatorPersistenceProbe::transactions(b).describe(id);
    ASSERT_EQ(ongoing->state, TransactionCoordinator::State::Ongoing);
    // Manual ISR transitions must not race the owner's automatic ISR expansion.
    for (auto& broker : brokers)
        broker->replicaManager()->stop();
    const auto end = b.logManager()->getLog("__transaction_state", 1)->logEndOffset();
    auto pending = std::async(std::launch::async, [&] {
        return membershipWire<protocol::AddOffsetsToTxnResponse>(
                   b, protocol::ApiKey::ADD_OFFSETS_TO_TXN, addOffsets(id, producer, key(0)), 3)
            .errorCode();
    });
    EXPECT_TRUE(waitUntil([&] {
        return b.logManager()->getLog("__transaction_state", 1)->logEndOffset() == end + 1;
    }));
    EXPECT_EQ(controller->metadataController()
                  ->updatePartitionISR("__transaction_state", 1, {1})
                  .error_code,
              ErrorCode::NONE);
    EXPECT_EQ(pending.get(), ErrorCode::NOT_ENOUGH_REPLICAS_AFTER_APPEND);
    // Preserve a reference while acquisition invalidates/rebuilds the live image.
    EXPECT_TRUE(CoordinatorPersistenceProbe::transactions(b).describe(id) == ongoing ||
                b.coordinatorLoadStatus(id, protocol::CoordinatorType::TRANSACTION) !=
                    ErrorCode::NONE);
    for (auto& broker : brokers)
        broker->replicaManager()->start();
}

TEST_F(CoordinatorClusterPersistenceTest,
       ThreeBrokerRegistrationOwnerEpochLossDuringIsrRefusesWireSuccess) {
    startCluster();
    KawasanBroker* controller = nullptr;
    ASSERT_TRUE(waitUntil([&] {
        for (auto& broker : brokers)
            if (broker->raftNode()->isLeader())
                controller = broker.get();
        return controller != nullptr;
    }));
    createData(*controller);
    const auto id = key(1);
    auto& b = *brokers[1];
    const auto producer = producerInitWire(b, id);
    ASSERT_EQ(producer.error, ErrorCode::NONE);
    for (auto api :
         {protocol::ApiKey::ADD_PARTITIONS_TO_TXN, protocol::ApiKey::ADD_OFFSETS_TO_TXN}) {
        const auto end = b.logManager()->getLog("__transaction_state", 1)->logEndOffset();
        brokers[0]->replicaManager()->stop();
        brokers[2]->replicaManager()->stop();
        auto pending = std::async(std::launch::async, [&] {
            if (api == protocol::ApiKey::ADD_PARTITIONS_TO_TXN)
                return membershipWire<protocol::AddPartitionsToTxnResponse>(
                           b, api, addPartitions(id, producer), 3)
                    .topics()[0]
                    .partitions[0]
                    .error_code;
            return membershipWire<protocol::AddOffsetsToTxnResponse>(
                       b, api, addOffsets(id, producer, key(2)), 3)
                .errorCode();
        });
        EXPECT_TRUE(waitUntil([&] {
            return b.logManager()->getLog("__transaction_state", 1)->logEndOffset() == end + 1;
        }));
        EXPECT_EQ(pending.wait_for(50ms), std::future_status::timeout);
        // A new owner epoch on this broker must still fence the old request.
        EXPECT_EQ(controller->metadataController()
                      ->updatePartitionLeader("__transaction_state", 1, 1)
                      .error_code,
                  ErrorCode::NONE);
        EXPECT_EQ(pending.get(), ErrorCode::NOT_COORDINATOR);
        brokers[0]->replicaManager()->start();
        brokers[2]->replicaManager()->start();
        ASSERT_TRUE(waitUntil([&] {
            return b.coordinatorLoadStatus(id, protocol::CoordinatorType::TRANSACTION) ==
                   ErrorCode::NONE;
        }));
    }
}

Buffer registrationPayload(protocol::ApiKey api, const std::string& id, ProducerIdentity producer) {
    Buffer payload;
    if (api == protocol::ApiKey::ADD_PARTITIONS_TO_TXN)
        addPartitions(id, producer).encode(payload, 3);
    else
        addOffsets(id, producer, "group").encode(payload, 3);
    return payload;
}
ErrorCode publishRegistration(RequestDispatcher::DispatchResult& result, protocol::ApiKey api) {
    auto frame = result.frame;
    result.publication_guard([&](auto status) {
        if (status != ErrorCode::NONE)
            frame = result.publication_error(status);
    });
    Buffer response(frame);
    (void)response.readInt32();
    (void)response.readInt32();
    response.skipTaggedFields();
    if (api == protocol::ApiKey::ADD_OFFSETS_TO_TXN) {
        protocol::AddOffsetsToTxnResponse decoded;
        decoded.decode(response, 3);
        EXPECT_EQ(response.remaining(), 0);
        return decoded.errorCode();
    }
    protocol::AddPartitionsToTxnResponse decoded;
    decoded.decode(response, 3);
    EXPECT_EQ(response.remaining(), 0);
    return decoded.topics().at(0).partitions.at(0).error_code;
}
TEST_F(CoordinatorPersistenceTest, TransactionRegistrationResponsesFenceEpochAndDrainShutdown) {
    KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
    b.start();
    b.reconcileReplicas();
    createData(b);
    const auto id = key(0);
    const auto producer = producerInitWire(b, id);
    ASSERT_EQ(producer.error, ErrorCode::NONE);
    std::vector<RequestDispatcher::DispatchResult> retained;
    for (auto api :
         {protocol::ApiKey::ADD_PARTITIONS_TO_TXN, protocol::ApiKey::ADD_OFFSETS_TO_TXN}) {
        std::promise<RequestDispatcher::DispatchResult> delivered;
        EXPECT_TRUE(CoordinatorExecutorProbe::dispatchProtocol(
                        b, api, 3, registrationPayload(api, id, producer),
                        [&](auto result) { delivered.set_value(std::move(result)); })
                        .deferred);
        auto ready = delivered.get_future();
        ASSERT_EQ(ready.wait_for(5s), std::future_status::ready);
        retained.push_back(ready.get());
        EXPECT_EQ(publishRegistration(retained.back(), api), ErrorCode::NONE);
    }
    ASSERT_EQ(b.metadataController()->updatePartitionLeader("__transaction_state", 0, 0).error_code,
              ErrorCode::NONE);
    b.reconcileReplicas();
    EXPECT_EQ(publishRegistration(retained[0], protocol::ApiKey::ADD_PARTITIONS_TO_TXN),
              ErrorCode::NOT_COORDINATOR);
    EXPECT_EQ(publishRegistration(retained[1], protocol::ApiKey::ADD_OFFSETS_TO_TXN),
              ErrorCode::NOT_COORDINATOR);
    auto stopped = std::async(std::launch::async, [&] { b.stop(); });
    EXPECT_TRUE(waitUntil([&] { return CoordinatorExecutorProbe::stopping(b); }));
    EXPECT_EQ(stopped.wait_for(50ms), std::future_status::timeout);
    retained.clear();
    EXPECT_EQ(stopped.wait_for(5s), std::future_status::ready);
    stopped.get();
}

TEST_F(CoordinatorPersistenceTest, QueuedRegistrationsCannotMutateNewIdentityOrReacquiredEpoch) {
    KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
    b.start();
    b.reconcileReplicas();
    createData(b);
    const auto id = key(0);
    const auto producer = producerInitWire(b, id);
    ASSERT_EQ(producer.error, ErrorCode::NONE);
    for (bool change_epoch : {false, true}) {
        std::array<std::promise<void>, 2> release;
        struct ReleaseWorkers {
            std::array<std::promise<void>, 2>& release;
            bool first_done = false;
            bool done = false;
            void finish() {
                if (!done) {
                    done = true;
                    if (!first_done)
                        release[0].set_value();
                    release[1].set_value();
                }
            }
            ~ReleaseWorkers() { finish(); }
        };
        std::array<std::shared_future<void>, 2> released{release[0].get_future().share(),
                                                         release[1].get_future().share()};
        auto entered = std::make_shared<std::atomic<int>>(0);
        ReleaseWorkers guard{release};
        for (int worker = 0; worker < 2; ++worker)
            ASSERT_TRUE(
                CoordinatorExecutorProbe::submit(b, [worker, released, entered](bool, auto) {
                    ++*entered;
                    released[worker].wait();
                }));
        ASSERT_TRUE(waitUntil([&] { return entered->load() == 2; }));
        std::vector<std::future<RequestDispatcher::DispatchResult>> results;
        for (auto api :
             {protocol::ApiKey::ADD_PARTITIONS_TO_TXN, protocol::ApiKey::ADD_OFFSETS_TO_TXN}) {
            auto delivered = std::make_shared<std::promise<RequestDispatcher::DispatchResult>>();
            results.push_back(delivered->get_future());
            EXPECT_TRUE(CoordinatorExecutorProbe::dispatchProtocol(
                            b, api, 3, registrationPayload(api, id, producer),
                            [delivered](auto result) { delivered->set_value(std::move(result)); })
                            .deferred);
        }
        if (change_epoch) {
            EXPECT_EQ(b.metadataController()
                          ->updatePartitionLeader("__transaction_state", 0, 0)
                          .error_code,
                      ErrorCode::NONE);
            b.reconcileReplicas();
        } else {
            const auto before = CoordinatorPersistenceProbe::transactions(b).describe(id);
            ASSERT_EQ(b.mutateCoordinatorTransaction(id, before,
                                                     [](auto& snapshot) {
                                                         ++snapshot->producer_epoch;
                                                         return ErrorCode::NONE;
                                                     }),
                      ErrorCode::NONE);
        }
        const auto before = CoordinatorPersistenceProbe::transactions(b).describe(id);
        const auto end = b.logManager()->getLog("__transaction_state", 0)->logEndOffset();
        // Leave only one worker free so producer refusals are deterministic,
        // rather than racing the documented busy-source admission refusal.
        release[0].set_value();
        guard.first_done = true;
        for (auto& result : results)
            ASSERT_EQ(result.wait_for(5s), std::future_status::ready);
        for (size_t i = 0; i < results.size(); ++i) {
            auto result = results[i].get();
            EXPECT_EQ(publishRegistration(result, i == 0 ? protocol::ApiKey::ADD_PARTITIONS_TO_TXN
                                                         : protocol::ApiKey::ADD_OFFSETS_TO_TXN),
                      change_epoch ? ErrorCode::NOT_COORDINATOR : ErrorCode::PRODUCER_FENCED);
        }
        guard.finish();
        EXPECT_EQ(CoordinatorPersistenceProbe::transactions(b).describe(id), before);
        EXPECT_EQ(b.logManager()->getLog("__transaction_state", 0)->logEndOffset(), end);
    }
}

TEST_F(CoordinatorPersistenceTest, KeyedProducerInitializationCommitsIdentityAndRetries) {
    KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
    b.start();
    b.reconcileReplicas();
    const auto id = key(0);
    const auto first = producerInitWire(b, id, 0);
    ASSERT_EQ(first.error, ErrorCode::NONE);
    EXPECT_EQ(first.epoch, 0);
    const auto bumped = producerInitWire(b, id, 3, first.id, first.epoch);
    ASSERT_EQ(bumped.error, ErrorCode::NONE);
    EXPECT_EQ(bumped.id, first.id);
    EXPECT_EQ(bumped.epoch, 1);
    const auto retry = producerInitWire(b, id, 4, first.id, first.epoch);
    EXPECT_EQ(retry.error, ErrorCode::NONE);
    EXPECT_EQ(retry.id, bumped.id);
    EXPECT_EQ(retry.epoch, bumped.epoch);
    auto committed = TransactionStateManager(b.logManager(), 3).loadCommittedPartition(0);
    ASSERT_EQ(committed.size(), 1u);
    EXPECT_EQ(committed[0], *CoordinatorPersistenceProbe::transactions(b).describe(id));
    EXPECT_EQ(committed[0].producer_epoch, 1);
}

TEST_F(CoordinatorPersistenceTest, KeyedProducerInitializationCheckpointFailurePreservesCache) {
    KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
    b.start();
    b.reconcileReplicas();
    const auto id = key(0);
    const auto blocked = dir / "0/__transaction_state-0/checkpoint.meta.tmp";
    fs::create_directory(blocked);
    const auto failed = producerInitWire(b, id, 0);
    EXPECT_EQ(failed.error, ErrorCode::KAFKA_STORAGE_ERROR);
    EXPECT_EQ(failed.id, -1);
    EXPECT_EQ(failed.epoch, -1);
    EXPECT_FALSE(CoordinatorPersistenceProbe::transactions(b).describe(id));
    fs::remove(blocked);
    b.reconcileReplicas();
    ASSERT_TRUE(waitUntil([&] {
        return b.coordinatorLoadStatus(id, protocol::CoordinatorType::TRANSACTION) ==
               ErrorCode::NONE;
    }));
    const auto recovered = producerInitWire(b, id, 4);
    EXPECT_EQ(recovered.error, ErrorCode::NONE);
    EXPECT_GE(recovered.id, 1);
}

TEST_F(CoordinatorPersistenceTest, KeyedProducerInitializationValidatesWithoutAllocating) {
    auto cfg = config();
    cfg.setInt("transaction.max.timeout.ms", 120000);
    KawasanBroker b(cfg, KawasanBroker::CoordinatorPersistenceOnly{format});
    b.start();
    b.reconcileReplicas();
    const auto id = key(0);
    auto* log = b.logManager()->getLog("__transaction_state", 0);
    ASSERT_NE(log, nullptr);
    for (int16_t version : {0, 1, 2, 3, 4}) {
        SCOPED_TRACE(version);
        EXPECT_EQ(producerInitWire(b, "", version).error, ErrorCode::INVALID_REQUEST);
        for (int32_t timeout : {-1, 0, 120001})
            EXPECT_EQ(producerInitWire(b, id, version, -1, -1, timeout).error,
                      ErrorCode::INVALID_TRANSACTION_TIMEOUT);
    }
    for (const auto& [pid, epoch] :
         std::vector<std::pair<int64_t, int16_t>>{{-1, 0}, {1, -1}, {-2, 0}, {1, -2}})
        EXPECT_EQ(producerInitWire(b, id, 4, pid, epoch).error, ErrorCode::INVALID_REQUEST);
    EXPECT_EQ(producerInitWire(b, std::nullopt, 4, -1, 0).error, ErrorCode::INVALID_REQUEST);
    EXPECT_FALSE(fs::exists(dir / "0/meta/producer_id.counter"));
    EXPECT_EQ(log->logEndOffset(), 0);
    const auto first = producerInitWire(b, id, 2);
    ASSERT_EQ(first.error, ErrorCode::NONE);
    EXPECT_EQ(first.id, 1);
    const auto initial = CoordinatorPersistenceProbe::transactions(b).describe(id);
    for (int16_t version : {3, 4}) {
        EXPECT_EQ(producerInitWire(b, id, version, first.id + 1, first.epoch).error,
                  version == 3 ? ErrorCode::INVALID_PRODUCER_EPOCH : ErrorCode::PRODUCER_FENCED);
        EXPECT_EQ(producerInitWire(b, id, version, first.id, 99).error,
                  version == 3 ? ErrorCode::INVALID_PRODUCER_EPOCH : ErrorCode::PRODUCER_FENCED);
    }
    EXPECT_EQ(CoordinatorPersistenceProbe::transactions(b).describe(id), initial);
    EXPECT_EQ(log->logEndOffset(), 1);
    const auto bumped = producerInitWire(b, id, 4, first.id, first.epoch);
    ASSERT_EQ(bumped.error, ErrorCode::NONE);
    EXPECT_EQ(log->logEndOffset(), 2);
    EXPECT_EQ(producerInitWire(b, id, 4, first.id, first.epoch).epoch, bumped.epoch);
    EXPECT_EQ(log->logEndOffset(), 2);  // exact retries are fenced no-ops
    const auto new_instance = producerInitWire(b, id, 1);
    ASSERT_EQ(new_instance.error, ErrorCode::NONE);
    EXPECT_EQ(new_instance.epoch, 2);
    EXPECT_EQ(producerInitWire(b, id, 4, first.id, first.epoch).error, ErrorCode::PRODUCER_FENCED);
    EXPECT_EQ(producerInitWire(b, id, 4, first.id, bumped.epoch).error, ErrorCode::PRODUCER_FENCED);
}

TEST_F(CoordinatorPersistenceTest, KeyedProducerInitializationRefusesPendingCompletion) {
    KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
    b.start();
    b.reconcileReplicas();
    const auto id = key(0);
    auto snapshot = transaction(id);
    snapshot.pending_offsets = {{"group", "data", 0, 10, "pending", 7}};
    ASSERT_EQ(init(b, snapshot), ErrorCode::NONE);
    using State = TransactionCoordinator::State;
    for (auto state : {State::Ongoing, State::PrepareCommit, State::PrepareAbort,
                       State::PrepareEpochFence, State::Dead}) {
        const auto before = CoordinatorPersistenceProbe::transactions(b).describe(id);
        ASSERT_EQ(b.mutateCoordinatorTransaction(id, before,
                                                 [&](auto& proposed) {
                                                     proposed->state = state;
                                                     return ErrorCode::NONE;
                                                 }),
                  ErrorCode::NONE);
        const auto expected = CoordinatorPersistenceProbe::transactions(b).describe(id);
        const auto end = b.logManager()->getLog("__transaction_state", 0)->logEndOffset();
        const auto result =
            producerInitWire(b, id, 4, snapshot.producer_id, snapshot.producer_epoch);
        EXPECT_EQ(result.error, state == State::Dead ? ErrorCode::INVALID_TXN_STATE
                                                     : ErrorCode::CONCURRENT_TRANSACTIONS);
        EXPECT_EQ(result.id, -1);
        EXPECT_EQ(CoordinatorPersistenceProbe::transactions(b).describe(id), expected);
        EXPECT_EQ(b.logManager()->getLog("__transaction_state", 0)->logEndOffset(), end);
    }
    EXPECT_FALSE(fs::exists(dir / "0/meta/producer_id.counter"));
}

TEST_F(CoordinatorPersistenceTest, KeyedProducerRotationRetrySurvivesRestartAndCacheRebuild) {
    const auto id = key(0);
    auto snapshot = transaction(id);
    snapshot.producer_id = 777;
    snapshot.producer_epoch = std::numeric_limits<int16_t>::max() - 1;
    snapshot.state = TransactionCoordinator::State::Empty;
    snapshot.partitions.clear();
    ProducerIdentity rotated{};
    {
        KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
        b.start();
        b.reconcileReplicas();
        ASSERT_EQ(init(b, snapshot), ErrorCode::NONE);
        rotated = producerInitWire(b, id, 4, snapshot.producer_id, snapshot.producer_epoch);
        ASSERT_EQ(rotated.error, ErrorCode::NONE);
        EXPECT_NE(rotated.id, snapshot.producer_id);
        EXPECT_EQ(rotated.epoch, 0);
        const auto retry =
            producerInitWire(b, id, 4, snapshot.producer_id, snapshot.producer_epoch);
        EXPECT_EQ(retry.error, ErrorCode::NONE);
        EXPECT_EQ(retry.id, rotated.id);
        EXPECT_EQ(retry.epoch, 0);
        EXPECT_EQ(b.logManager()->getLog("__transaction_state", 0)->logEndOffset(), 2);
        b.stop();
    }
    fs::remove_all(dir / "0/consumer_offsets");  // disposable cache loss
    KawasanBroker recovered(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
    recovered.start();
    recovered.reconcileReplicas();
    const auto retry =
        producerInitWire(recovered, id, 3, snapshot.producer_id, snapshot.producer_epoch);
    EXPECT_EQ(retry.error, ErrorCode::NONE);
    EXPECT_EQ(retry.id, rotated.id);
    EXPECT_EQ(retry.epoch, 0);
    EXPECT_EQ(recovered.logManager()->getLog("__transaction_state", 0)->logEndOffset(), 2);
    EXPECT_EQ(producerInitWire(recovered, id, 4, rotated.id, 0).epoch, 1);
    EXPECT_EQ(
        producerInitWire(recovered, id, 4, snapshot.producer_id, snapshot.producer_epoch).error,
        ErrorCode::PRODUCER_FENCED);
}

TEST_F(CoordinatorClusterPersistenceTest,
       ThreeBrokerKeyedProducerWireWaitsForIsrAndRefusesWrongOwner) {
    startCluster();
    const auto id = key(1);
    auto& b = *brokers[1];
    // Isolate exact wire success from the periodic acquisition try-lock.
    b.replicaManager()->stop();
    for (int wrong : {0, 2}) {
        EXPECT_EQ(producerInitWire(*brokers[wrong], id).error, ErrorCode::NOT_COORDINATOR);
        EXPECT_FALSE(fs::exists(dir / std::to_string(wrong) / "meta/producer_id.counter"));
    }
    brokers[0]->replicaManager()->stop();
    brokers[2]->replicaManager()->stop();
    auto pending = std::async(std::launch::async, [&] { return producerInitWire(b, id); });
    EXPECT_TRUE(waitUntil(
        [&] { return b.logManager()->getLog("__transaction_state", 1)->logEndOffset() == 1; }));
    EXPECT_EQ(pending.wait_for(50ms), std::future_status::timeout);
    EXPECT_FALSE(CoordinatorPersistenceProbe::transactions(b).describe(id));
    Buffer versions;
    protocol::RequestHeader(protocol::ApiKey::API_VERSIONS, 1, 95, "io-progress").encode(versions);
    protocol::ApiVersionsRequest{}.encode(versions, 1);
    auto progress = test_support::brokerRequest(b.port(), versions);
    EXPECT_EQ(progress.readInt32(), 95);
    brokers[0]->replicaManager()->start();
    brokers[2]->replicaManager()->start();
    ASSERT_EQ(pending.wait_for(10s), std::future_status::ready);
    const auto committed = pending.get();
    EXPECT_EQ(committed.error, ErrorCode::NONE);
    EXPECT_EQ(committed.id, (int64_t{1} << 32) | 1);
    EXPECT_EQ(committed.epoch, 0);
    ASSERT_TRUE(waitUntil([&] {
        for (auto& broker : brokers)
            if (broker->logManager()->getLog("__transaction_state", 1)->highWatermark() < 1)
                return false;
        return true;
    }));
    for (auto& broker : brokers) {
        const auto image =
            TransactionStateManager(broker->logManager(), 3).loadCommittedPartition(1);
        ASSERT_EQ(image.size(), 1u);
        EXPECT_EQ(image[0].producer_id, committed.id);
    }
}

ProducerIdentity publishProducer(RequestDispatcher::DispatchResult& result) {
    auto frame = result.frame;
    if (result.publication_guard)
        result.publication_guard([&](auto status) {
            if (status != ErrorCode::NONE)
                frame = result.publication_error(status);
        });
    Buffer response(frame);
    (void)response.readInt32();  // frame length
    return producerIdentity(std::move(response));
}

TEST_F(CoordinatorPersistenceTest, KeyedProducerResponseFencesChangedEpochAndDrainsShutdown) {
    KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
    b.start();
    b.reconcileReplicas();
    const auto id = key(0);
    std::promise<RequestDispatcher::DispatchResult> delivered;
    EXPECT_TRUE(CoordinatorExecutorProbe::dispatchProtocol(
                    b, protocol::ApiKey::INIT_PRODUCER_ID, 4, producerInitPayload(id),
                    [&](auto result) { delivered.set_value(std::move(result)); })
                    .deferred);
    auto ready = delivered.get_future();
    ASSERT_EQ(ready.wait_for(5s), std::future_status::ready);
    auto result = ready.get();
    EXPECT_EQ(publishProducer(result).error, ErrorCode::NONE);
    ASSERT_EQ(b.metadataController()->updatePartitionLeader("__transaction_state", 0, 0).error_code,
              ErrorCode::NONE);
    b.reconcileReplicas();
    EXPECT_EQ(publishProducer(result).error, ErrorCode::NOT_COORDINATOR);
    const auto acknowledged = CoordinatorPersistenceProbe::transactions(b).describe(id);
    ASSERT_TRUE(acknowledged);
    EXPECT_EQ(acknowledged->producer_epoch, 0);
    auto stopped = std::async(std::launch::async, [&] { b.stop(); });
    EXPECT_TRUE(waitUntil([&] { return CoordinatorExecutorProbe::stopping(b); }));
    EXPECT_EQ(stopped.wait_for(50ms), std::future_status::timeout);
    EXPECT_EQ(publishProducer(result).error, ErrorCode::NOT_COORDINATOR);
    result = {};  // release posted response ticket before shutdown may finish
    EXPECT_EQ(stopped.wait_for(5s), std::future_status::ready);
    stopped.get();
}

TEST_F(CoordinatorPersistenceTest, QueuedKeyedProducerCannotAllocateInReacquiredEpoch) {
    KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
    b.start();
    b.reconcileReplicas();
    const auto id = key(0);
    std::promise<void> release;
    struct ReleaseWorkers {
        std::promise<void>& release;
        bool done = false;
        void finish() {
            if (!done) {
                done = true;
                release.set_value();
            }
        }
        ~ReleaseWorkers() { finish(); }
    };
    auto released = release.get_future().share();
    std::atomic<int> entered{0};
    // This guard must be destroyed before the broker joins its workers.
    ReleaseWorkers guard{release};
    for (int worker = 0; worker < 2; ++worker)
        ASSERT_TRUE(CoordinatorExecutorProbe::submit(b, [&](bool admitted, auto) {
            EXPECT_TRUE(admitted);
            ++entered;
            released.wait();
        }));
    ASSERT_TRUE(waitUntil([&] { return entered == 2; }));
    auto delivered = std::make_shared<std::promise<RequestDispatcher::DispatchResult>>();
    EXPECT_TRUE(CoordinatorExecutorProbe::dispatchProtocol(
                    b, protocol::ApiKey::INIT_PRODUCER_ID, 4, producerInitPayload(id),
                    [delivered](auto result) { delivered->set_value(std::move(result)); })
                    .deferred);
    auto ready = delivered->get_future();
    EXPECT_EQ(ready.wait_for(50ms), std::future_status::timeout);
    EXPECT_EQ(b.metadataController()->updatePartitionLeader("__transaction_state", 0, 0).error_code,
              ErrorCode::NONE);
    b.reconcileReplicas();
    EXPECT_EQ(b.coordinatorLoadStatus(id, protocol::CoordinatorType::TRANSACTION), ErrorCode::NONE);
    // Release exactly once, also on every early assertion return.
    guard.finish();
    ASSERT_EQ(ready.wait_for(5s), std::future_status::ready);
    auto result = ready.get();
    EXPECT_EQ(publishProducer(result).error, ErrorCode::NOT_COORDINATOR);
    EXPECT_FALSE(CoordinatorPersistenceProbe::transactions(b).describe(id));
    EXPECT_FALSE(fs::exists(dir / "0/meta/producer_id.counter"));
    EXPECT_EQ(b.logManager()->getLog("__transaction_state", 0)->logEndOffset(), 0);
}

TEST_F(CoordinatorPersistenceTest, KeyedProducerCounterFailureLeavesNoSourceIdentity) {
    KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
    b.start();
    b.reconcileReplicas();
    const auto id = key(0);
    const auto temporary = dir / "0/meta/producer_id.counter.tmp";
    fs::create_directory(temporary);
    const auto failed = producerInitWire(b, id);
    EXPECT_EQ(failed.error, ErrorCode::KAFKA_STORAGE_ERROR);
    EXPECT_EQ(failed.id, -1);
    EXPECT_FALSE(CoordinatorPersistenceProbe::transactions(b).describe(id));
    EXPECT_EQ(b.logManager()->getLog("__transaction_state", 0)->logEndOffset(), 0);
    fs::remove(temporary);
    const auto retry = producerInitWire(b, id);
    EXPECT_EQ(retry.error, ErrorCode::NONE);
    EXPECT_EQ(retry.id, 1);
    EXPECT_EQ(retry.epoch, 0);
}

TEST_F(CoordinatorPersistenceTest, StagedOffsetWireCommitFetchAndExactDeletion) {
    KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
    b.start();
    b.reconcileReplicas();
    const auto id = key(0);
    ASSERT_EQ(put(b, id, {groupRecord(id)}), ErrorCode::NONE);
    const auto commit = membershipWire<protocol::OffsetCommitResponse>(
        b, protocol::ApiKey::OFFSET_COMMIT, offsetCommit(id), 8);
    ASSERT_EQ(commit.topics().size(), 1);
    EXPECT_EQ(commit.topics()[0].topic, "data");
    ASSERT_EQ(commit.topics()[0].partitions.size(), 1);
    EXPECT_EQ(commit.topics()[0].partitions[0].error, ErrorCode::NONE);
    protocol::OffsetFetchRequest fetch;
    fetch.setRequireStable(true);
    fetch.addGroup({id, std::nullopt, -1, true, {}});
    const auto result =
        membershipWire<protocol::OffsetFetchResponse>(b, protocol::ApiKey::OFFSET_FETCH, fetch, 9);
    ASSERT_EQ(result.groups().size(), 1);
    EXPECT_EQ(result.groups()[0].error_code, ErrorCode::NONE);
    ASSERT_EQ(result.groups()[0].topics.size(), 1);
    EXPECT_EQ(result.groups()[0].topics[0].partitions[0].offset, 123);
    EXPECT_EQ(result.groups()[0].topics[0].partitions[0].committed_leader_epoch, 9);
    auto empty = groupRecord(id);
    empty.group.members.clear();
    empty.group.leader_id.clear();
    empty.group.state = 0;
    auto pending = offsetRecord(id, 456);
    pending.key.kind = GroupRecordKey::Kind::PendingOffset;
    pending.key.transactional_id = "txn";
    pending.key.producer_id = 75;
    pending.key.producer_epoch = 2;
    ASSERT_EQ(put(b, id, {empty, pending}), ErrorCode::NONE);
    const auto unstable =
        membershipWire<protocol::OffsetFetchResponse>(b, protocol::ApiKey::OFFSET_FETCH, fetch, 9);
    ASSERT_EQ(unstable.groups()[0].topics.size(), 1);
    EXPECT_EQ(unstable.groups()[0].topics[0].partitions[0].error,
              ErrorCode::UNSTABLE_OFFSET_COMMIT);
    auto deleted = deleteWire(b, protocol::ApiKey::OFFSET_DELETE, id);
    EXPECT_EQ(static_cast<ErrorCode>(deleted.readInt16()), ErrorCode::NONE);
    deleted.readInt32();
    EXPECT_FALSE(CoordinatorPersistenceProbe::offsets(b).fetchOffset(id, "data", 0));
    EXPECT_EQ(CoordinatorPersistenceProbe::groups(b).pendingCoordinatorOffsets().size(), 1);
    auto all = deleteWire(b, protocol::ApiKey::DELETE_GROUPS, {}, {id});
    all.readInt32();
    ASSERT_EQ(all.readInt32(), 1);
    EXPECT_EQ(all.readString(), id);
    EXPECT_EQ(static_cast<ErrorCode>(all.readInt16()), ErrorCode::NONE);
    EXPECT_TRUE(GroupStateManager(b.logManager(), 3).loadCommittedPartition(0).empty());
    b.stop();
}

TEST_F(CoordinatorPersistenceTest, OffsetCheckpointFailurePreservesKeysAndLiveActivity) {
    KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
    b.start();
    b.reconcileReplicas();
    const auto id = key(0);
    ASSERT_EQ(put(b, id, {groupRecord(id), offsetRecord(id)}), ErrorCode::NONE);
    const auto before =
        CoordinatorAcquisitionProbe::runtime(CoordinatorPersistenceProbe::groups(b), id);
    fs::create_directory(dir / "0/__consumer_offsets-0/checkpoint.meta.tmp");
    const auto commit = membershipWire<protocol::OffsetCommitResponse>(
        b, protocol::ApiKey::OFFSET_COMMIT, offsetCommit(id), 8);
    ASSERT_EQ(commit.topics().size(), 1);
    EXPECT_EQ(commit.topics()[0].topic, "data");
    EXPECT_EQ(commit.topics()[0].partitions[0].error, ErrorCode::KAFKA_STORAGE_ERROR);
    EXPECT_EQ(CoordinatorPersistenceProbe::offsets(b).fetchOffset(id, "data", 0), 42);
    EXPECT_EQ(CoordinatorAcquisitionProbe::runtime(CoordinatorPersistenceProbe::groups(b), id),
              before);
    fs::remove(dir / "0/__consumer_offsets-0/checkpoint.meta.tmp");
    b.stop();
}

GroupRecord emptyGroup(const std::string& id, int64_t timestamp = 123) {
    auto group = groupRecord(id);
    group.group.state = 0;
    group.group.members.clear();
    group.group.leader_id.clear();
    group.group.last_update_timestamp = timestamp;
    return group;
}

template <typename Response>
Response publishProtocol(RequestDispatcher::DispatchResult& result, protocol::ApiKey api,
                         int16_t version) {
    auto frame = result.frame;
    if (result.publication_guard)
        result.publication_guard([&](auto status) {
            if (status != ErrorCode::NONE)
                frame = result.publication_error(status);
        });
    Buffer body(frame);
    (void)body.readInt32();
    protocol::ResponseHeader header;
    header.setFlexible(
        protocol::RequestHeader(api, version, 91, "offset-test").isFlexibleResponseHeader());
    header.decode(body);
    Response response;
    response.decode(body, version);
    EXPECT_EQ(body.remaining(), 0u);
    return response;
}

struct Inspection {
    std::string id;
    ErrorCode error;
    std::string state;
    int64_t producer_id = -1;
    int16_t producer_epoch = -1;
    std::vector<protocol::DescribeGroupsResponse::Member> members;
    int32_t transaction_timeout_ms = 0;
    int64_t transaction_start_time_ms = -1;
    std::vector<TopicPartition> partitions;
};

Buffer inspectionPayload(protocol::ApiKey api, const std::vector<std::string>& keys,
                         int16_t version) {
    Buffer payload;
    if (api == protocol::ApiKey::DESCRIBE_GROUPS) {
        protocol::DescribeGroupsRequest request;
        request.setGroups(keys);
        request.setIncludeAuthorizedOperations(true);
        request.encode(payload, version);
    } else {
        payload.writeCompactArrayLen(static_cast<int32_t>(keys.size()));
        for (const auto& key : keys)
            payload.writeCompactString(key);
        payload.writeEmptyTaggedFields();
    }
    return payload;
}

std::vector<Inspection> decodeInspection(Buffer body, protocol::ApiKey api, int16_t version) {
    protocol::ResponseHeader header;
    header.setFlexible(
        protocol::RequestHeader(api, version, 91, "inspection-test").isFlexibleResponseHeader());
    header.decode(body);
    std::vector<Inspection> entries;
    if (api == protocol::ApiKey::DESCRIBE_GROUPS) {
        protocol::DescribeGroupsResponse response;
        response.decode(body, version);
        for (const auto& group : response.groups()) {
            Inspection entry;
            entry.id = group.group_id;
            entry.error = group.error_code;
            entry.state = group.group_state;
            entry.members = group.members;
            entries.push_back(std::move(entry));
        }
    } else {
        (void)body.readInt32();  // throttle
        const auto count = body.readCompactArrayLen();
        for (int i = 0; i < count; ++i) {
            Inspection entry;
            entry.error = static_cast<ErrorCode>(body.readInt16());
            entry.id = body.readCompactString();
            entry.state = body.readCompactString();
            entry.transaction_timeout_ms = body.readInt32();
            entry.transaction_start_time_ms = body.readInt64();
            entry.producer_id = body.readInt64();
            entry.producer_epoch = body.readInt16();
            const auto topics = body.readCompactArrayLen();
            for (int t = 0; t < topics; ++t) {
                const auto topic = body.readCompactString();
                const auto partitions = body.readCompactArrayLen();
                for (int p = 0; p < partitions; ++p)
                    entry.partitions.push_back({topic, body.readInt32()});
                body.skipTaggedFields();
            }
            body.skipTaggedFields();
            entries.push_back(std::move(entry));
        }
        body.skipTaggedFields();
    }
    EXPECT_EQ(body.remaining(), 0u);
    return entries;
}

std::vector<Inspection> inspectionWire(KawasanBroker& b, protocol::ApiKey api,
                                       const std::vector<std::string>& keys, int16_t version) {
    Buffer payload;
    protocol::RequestHeader(api, version, 91, "inspection-test").encode(payload);
    const auto body = inspectionPayload(api, keys, version);
    payload.writeBytes(body.data(), body.size());
    return decodeInspection(test_support::brokerRequest(b.port(), payload), api, version);
}

std::vector<Inspection> publishInspection(RequestDispatcher::DispatchResult& result,
                                          protocol::ApiKey api, int16_t version) {
    auto frame = result.frame;
    if (result.publication_guard)
        result.publication_guard([&](auto status) {
            if (status != ErrorCode::NONE)
                frame = result.publication_error(status);
        });
    Buffer body(frame);
    (void)body.readInt32();  // frame length
    return decodeInspection(std::move(body), api, version);
}

struct Listing {
    ErrorCode error = ErrorCode::NONE;
    std::vector<std::string> ids;
    std::vector<std::string> states;
    std::vector<std::string> unknown_filters;
};
Buffer listingPayload(protocol::ApiKey api, int16_t version,
                      const std::vector<std::string>& states = {},
                      const std::vector<int64_t>& producers = {}) {
    Buffer payload;
    if (api == protocol::ApiKey::LIST_GROUPS) {
        protocol::ListGroupsRequest request;
        request.setStatesFilter(states);
        request.encode(payload, version);
    } else {
        payload.writeCompactArrayLen(static_cast<int32_t>(states.size()));
        for (const auto& state : states)
            payload.writeCompactString(state);
        payload.writeCompactArrayLen(static_cast<int32_t>(producers.size()));
        for (const auto pid : producers)
            payload.writeInt64(pid);
        payload.writeEmptyTaggedFields();
    }
    return payload;
}
Listing decodeListing(Buffer body, protocol::ApiKey api, int16_t version) {
    protocol::ResponseHeader header;
    header.setFlexible(
        protocol::RequestHeader(api, version, 91, "listing-test").isFlexibleResponseHeader());
    header.decode(body);
    Listing listing;
    if (api == protocol::ApiKey::LIST_GROUPS) {
        protocol::ListGroupsResponse response;
        response.decode(body, version);
        listing.error = response.errorCode();
        for (const auto& group : response.groups()) {
            listing.ids.push_back(group.group_id);
            listing.states.push_back(group.group_state);
        }
    } else {
        (void)body.readInt32();
        listing.error = static_cast<ErrorCode>(body.readInt16());
        const auto unknown = body.readCompactArrayLen();
        for (int i = 0; i < unknown; ++i)
            listing.unknown_filters.push_back(body.readCompactString());
        const auto count = body.readCompactArrayLen();
        for (int i = 0; i < count; ++i) {
            listing.ids.push_back(body.readCompactString());
            (void)body.readInt64();
            listing.states.push_back(body.readCompactString());
            body.skipTaggedFields();
        }
        body.skipTaggedFields();
    }
    EXPECT_EQ(body.remaining(), 0u);
    return listing;
}
Listing listingWire(KawasanBroker& b, protocol::ApiKey api, int16_t version,
                    const std::vector<std::string>& states = {},
                    const std::vector<int64_t>& producers = {}) {
    Buffer payload;
    protocol::RequestHeader(api, version, 91, "listing-test").encode(payload);
    const auto body = listingPayload(api, version, states, producers);
    payload.writeBytes(body.data(), body.size());
    return decodeListing(test_support::brokerRequest(b.port(), payload), api, version);
}
Listing publishListing(RequestDispatcher::DispatchResult& result, protocol::ApiKey api,
                       int16_t version) {
    auto frame = result.frame;
    if (result.publication_guard)
        result.publication_guard([&](auto status) {
            if (status != ErrorCode::NONE)
                frame = result.publication_error(status);
        });
    Buffer body(frame);
    (void)body.readInt32();
    return decodeListing(std::move(body), api, version);
}
TEST_F(CoordinatorPersistenceTest, OwnedListingsWireEnumeratesCommittedSourcesAndFilters) {
    KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
    b.start();
    b.reconcileReplicas();
    for (int p = 0; p < 3; ++p) {
        ASSERT_EQ(put(b, key(p), {groupRecord(key(p))}), ErrorCode::NONE);
        auto snapshot = transaction(key(p));
        snapshot.producer_id += p;
        if (p == 1)
            snapshot.state = TransactionCoordinator::State::PrepareAbort;
        ASSERT_EQ(init(b, snapshot), ErrorCode::NONE);
    }
    const std::set<std::string> expected{key(0), key(1), key(2)};
    for (int16_t version = 0; version <= 4; ++version) {
        const auto groups = listingWire(b, protocol::ApiKey::LIST_GROUPS, version);
        EXPECT_EQ(groups.error, ErrorCode::NONE);
        EXPECT_EQ(std::set(groups.ids.begin(), groups.ids.end()), expected);
    }
    EXPECT_TRUE(listingWire(b, protocol::ApiKey::LIST_GROUPS, 4, {"Empty"}).ids.empty());
    EXPECT_EQ(listingWire(b, protocol::ApiKey::LIST_GROUPS, 4, {"Stable"}).ids.size(), 3u);
    const auto txns = listingWire(b, protocol::ApiKey::LIST_TRANSACTIONS, 0);
    EXPECT_EQ(txns.error, ErrorCode::NONE);
    EXPECT_EQ(std::set(txns.ids.begin(), txns.ids.end()), expected);
    EXPECT_EQ(listingWire(b, protocol::ApiKey::LIST_TRANSACTIONS, 0, {"PrepareAbort"}).ids,
              std::vector<std::string>{key(1)});
    EXPECT_EQ(listingWire(b, protocol::ApiKey::LIST_TRANSACTIONS, 0, {}, {77}).ids,
              std::vector<std::string>{key(2)});
    const auto unknown =
        listingWire(b, protocol::ApiKey::LIST_TRANSACTIONS, 0, {"MadeUp", "Ongoing"}, {75});
    EXPECT_EQ(unknown.error, ErrorCode::NONE);
    EXPECT_EQ(unknown.ids, std::vector<std::string>{key(0)});
    EXPECT_EQ(unknown.unknown_filters, std::vector<std::string>{"MadeUp"});
    b.stop();
}
TEST_F(CoordinatorPersistenceTest, OwnedListingsDefaultFiltersAndDeadTransactionsMatchKafka) {
    KawasanBroker b(config());
    b.start();
    auto dead = transaction(key(0));
    dead.state = TransactionCoordinator::State::Dead;
    CoordinatorPersistenceProbe::transactions(b).restore(dead);
    CoordinatorPersistenceProbe::transactions(b).restore(transaction(key(1)));
    CoordinatorPersistenceProbe::groups(b).replaceCoordinatorPartition(
        0, 3, {groupRecord(key(0))}, CoordinatorPersistenceProbe::retainOffsets(b));
    EXPECT_EQ(listingWire(b, protocol::ApiKey::LIST_GROUPS, 4, {"Stable"}).ids,
              std::vector<std::string>{key(0)});
    EXPECT_TRUE(listingWire(b, protocol::ApiKey::LIST_GROUPS, 4, {"Empty"}).ids.empty());
    const auto all = listingWire(b, protocol::ApiKey::LIST_TRANSACTIONS, 0);
    EXPECT_EQ(all.error, ErrorCode::NONE);
    EXPECT_EQ(all.ids, std::vector<std::string>{key(1)});
    const auto unknown = listingWire(b, protocol::ApiKey::LIST_TRANSACTIONS, 0, {"NoSuchState"});
    EXPECT_EQ(unknown.error, ErrorCode::NONE);
    EXPECT_TRUE(unknown.ids.empty());
    EXPECT_EQ(unknown.unknown_filters, std::vector<std::string>{"NoSuchState"});
    b.stop();
}
TEST_F(CoordinatorPersistenceTest, OwnedListingsAcquisitionOnlyAndProducerInspectionRefuseCaches) {
    KawasanBroker b(config(), KawasanBroker::CoordinatorAcquisitionOnly{format});
    b.start();
    b.reconcileReplicas();
    for (int16_t version = 0; version <= 4; ++version) {
        const auto groups = listingWire(b, protocol::ApiKey::LIST_GROUPS, version);
        EXPECT_EQ(groups.error, ErrorCode::COORDINATOR_LOAD_IN_PROGRESS);
        EXPECT_TRUE(groups.ids.empty());
    }
    const auto txns = listingWire(b, protocol::ApiKey::LIST_TRANSACTIONS, 0);
    EXPECT_EQ(txns.error, ErrorCode::COORDINATOR_LOAD_IN_PROGRESS);
    EXPECT_TRUE(txns.ids.empty());
    Buffer payload;
    protocol::RequestHeader(protocol::ApiKey::DESCRIBE_PRODUCERS, 0, 91, "listing-test")
        .encode(payload);
    payload.writeCompactArrayLen(1);
    payload.writeCompactString("data");
    payload.writeCompactArrayLen(2);
    payload.writeInt32(0);
    payload.writeInt32(0);
    payload.writeEmptyTaggedFields();
    payload.writeEmptyTaggedFields();
    auto body = test_support::brokerRequest(b.port(), payload);
    protocol::ResponseHeader header;
    header.setFlexible(true);
    header.decode(body);
    EXPECT_EQ(body.readInt32(), 0);
    ASSERT_EQ(body.readCompactArrayLen(), 1);
    EXPECT_EQ(body.readCompactString(), "data");
    ASSERT_EQ(body.readCompactArrayLen(), 2);
    for (int i = 0; i < 2; ++i) {
        EXPECT_EQ(body.readInt32(), 0);
        EXPECT_EQ(static_cast<ErrorCode>(body.readInt16()),
                  ErrorCode::COORDINATOR_LOAD_IN_PROGRESS);
        (void)body.readCompactNullableString();
        EXPECT_EQ(body.readCompactArrayLen(), 0);
        body.skipTaggedFields();
    }
    body.skipTaggedFields();
    body.skipTaggedFields();
    EXPECT_EQ(body.remaining(), 0u);
    b.stop();
}

TEST_F(CoordinatorPersistenceTest, OwnedListingsResponsesFenceOriginalEpochsAndDrain) {
    for (const auto api : {protocol::ApiKey::LIST_GROUPS, protocol::ApiKey::LIST_TRANSACTIONS}) {
        auto c = config();
        const auto topic =
            api == protocol::ApiKey::LIST_GROUPS ? "__consumer_offsets" : "__transaction_state";
        c.setString("log.dirs", (dir / topic).string());
        KawasanBroker b(c, KawasanBroker::CoordinatorPersistenceOnly{format});
        b.start();
        b.reconcileReplicas();
        ASSERT_EQ(put(b, key(0), {groupRecord(key(0))}), ErrorCode::NONE);
        ASSERT_EQ(init(b, transaction(key(0))), ErrorCode::NONE);
        const int16_t version = api == protocol::ApiKey::LIST_GROUPS ? 4 : 0;
        std::promise<RequestDispatcher::DispatchResult> delivered;
        EXPECT_TRUE(CoordinatorExecutorProbe::dispatchProtocol(
                        b, api, version, listingPayload(api, version),
                        [&](auto result) { delivered.set_value(std::move(result)); })
                        .deferred);
        auto ready = delivered.get_future();
        ASSERT_EQ(ready.wait_for(5s), std::future_status::ready);
        auto result = ready.get();
        ASSERT_TRUE(result.publication_guard);
        const auto before = publishListing(result, api, version);
        EXPECT_EQ(before.error, ErrorCode::NONE);
        EXPECT_EQ(before.ids, std::vector<std::string>{key(0)});
        fs::create_directory(dir / topic / (std::string(topic) + "-0/checkpoint.meta.tmp"));
        if (api == protocol::ApiKey::LIST_GROUPS) {
            EXPECT_EQ(put(b, key(0), {offsetRecord(key(0))}), ErrorCode::KAFKA_STORAGE_ERROR);
        } else {
            const auto previous = CoordinatorPersistenceProbe::transactions(b).describe(key(0));
            EXPECT_EQ(b.mutateCoordinatorTransaction(key(0), previous,
                                                     [](auto& snapshot) {
                                                         ++snapshot->producer_epoch;
                                                         return ErrorCode::NONE;
                                                     }),
                      ErrorCode::KAFKA_STORAGE_ERROR);
        }
        const auto unreadable = publishListing(result, api, version);
        EXPECT_EQ(unreadable.error, ErrorCode::COORDINATOR_LOAD_IN_PROGRESS);
        EXPECT_TRUE(unreadable.ids.empty());
        ASSERT_EQ(b.metadataController()->updatePartitionLeader(topic, 2, 0).error_code,
                  ErrorCode::NONE);
        b.reconcileReplicas();
        b.acquireCoordinatorPartitions();
        const auto after = publishListing(result, api, version);
        EXPECT_EQ(after.error, ErrorCode::NOT_COORDINATOR);
        EXPECT_TRUE(after.ids.empty());
        auto stopped = std::async(std::launch::async, [&] { b.stop(); });
        EXPECT_TRUE(waitUntil([&] { return CoordinatorExecutorProbe::stopping(b); }));
        EXPECT_EQ(stopped.wait_for(50ms), std::future_status::timeout);
        result = {};
        ASSERT_EQ(stopped.wait_for(5s), std::future_status::ready);
        stopped.get();
    }
}
TEST_F(CoordinatorPersistenceTest, OwnedListingsUnreadableSourceRefusesEntireListing) {
    KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
    b.start();
    b.reconcileReplicas();
    for (int p : {0, 1}) {
        ASSERT_EQ(put(b, key(p), {groupRecord(key(p))}), ErrorCode::NONE);
        ASSERT_EQ(init(b, transaction(key(p))), ErrorCode::NONE);
    }
    fs::create_directory(dir / "0/__consumer_offsets-0/checkpoint.meta.tmp");
    EXPECT_EQ(put(b, key(0), {offsetRecord(key(0))}), ErrorCode::KAFKA_STORAGE_ERROR);
    fs::create_directory(dir / "0/__transaction_state-0/checkpoint.meta.tmp");
    const auto previous = CoordinatorPersistenceProbe::transactions(b).describe(key(0));
    EXPECT_EQ(b.mutateCoordinatorTransaction(key(0), previous,
                                             [](auto& snapshot) {
                                                 ++snapshot->producer_epoch;
                                                 return ErrorCode::NONE;
                                             }),
              ErrorCode::KAFKA_STORAGE_ERROR);
    for (const auto api : {protocol::ApiKey::LIST_GROUPS, protocol::ApiKey::LIST_TRANSACTIONS}) {
        const auto listing = listingWire(b, api, api == protocol::ApiKey::LIST_GROUPS ? 4 : 0);
        EXPECT_EQ(listing.error, ErrorCode::COORDINATOR_LOAD_IN_PROGRESS);
        EXPECT_TRUE(listing.ids.empty());
    }
    b.stop();
}

TEST_F(CoordinatorClusterPersistenceTest,
       OwnedListingsClusterExcludesPoisonedNonownedCachesAndAcquires) {
    startCluster();
    for (int owner = 0; owner < 3; ++owner) {
        ASSERT_EQ(put(*brokers[owner], key(owner), {groupRecord(key(owner))}), ErrorCode::NONE);
        ASSERT_EQ(init(*brokers[owner], transaction(key(owner))), ErrorCode::NONE);
    }
    CoordinatorPersistenceProbe::transactions(*brokers[0]).restore(transaction(key(1)));
    CoordinatorPersistenceProbe::groups(*brokers[0])
        .replaceCoordinatorPartition(1, 3, {groupRecord(key(1))},
                                     CoordinatorPersistenceProbe::retainOffsets(*brokers[0]));
    for (const auto api : {protocol::ApiKey::LIST_GROUPS, protocol::ApiKey::LIST_TRANSACTIONS}) {
        const auto listing =
            listingWire(*brokers[0], api, api == protocol::ApiKey::LIST_GROUPS ? 4 : 0);
        EXPECT_EQ(listing.error, ErrorCode::NONE);
        EXPECT_EQ(listing.ids, std::vector<std::string>{key(0)});
    }
    KawasanBroker* controller = nullptr;
    ASSERT_TRUE(waitUntil([&] {
        for (auto& broker : brokers)
            if (broker->raftNode()->isLeader())
                controller = broker.get();
        return controller != nullptr;
    }));
    std::vector<RequestDispatcher::DispatchResult> before_acquisition;
    for (const auto api : {protocol::ApiKey::LIST_GROUPS, protocol::ApiKey::LIST_TRANSACTIONS}) {
        const auto version = api == protocol::ApiKey::LIST_GROUPS ? 4 : 0;
        std::promise<RequestDispatcher::DispatchResult> delivered;
        EXPECT_TRUE(CoordinatorExecutorProbe::dispatchProtocol(
                        *brokers[1], api, version, listingPayload(api, version),
                        [&](auto result) { delivered.set_value(std::move(result)); })
                        .deferred);
        auto ready = delivered.get_future();
        ASSERT_EQ(ready.wait_for(5s), std::future_status::ready);
        before_acquisition.push_back(ready.get());
        EXPECT_EQ(publishListing(before_acquisition.back(), api, version).error, ErrorCode::NONE);
    }
    for (const auto topic : {"__consumer_offsets", "__transaction_state"})
        ASSERT_EQ(controller->metadataController()->updatePartitionLeader(topic, 0, 1).error_code,
                  ErrorCode::NONE);
    ASSERT_TRUE(waitUntil([&] {
        brokers[1]->reconcileReplicas();
        return brokers[1]->coordinatorLoadStatus(key(0), protocol::CoordinatorType::GROUP) ==
                   ErrorCode::NONE &&
               brokers[1]->coordinatorLoadStatus(key(0), protocol::CoordinatorType::TRANSACTION) ==
                   ErrorCode::NONE;
    }));
    for (const auto api : {protocol::ApiKey::LIST_GROUPS, protocol::ApiKey::LIST_TRANSACTIONS}) {
        const auto listing =
            listingWire(*brokers[1], api, api == protocol::ApiKey::LIST_GROUPS ? 4 : 0);
        EXPECT_EQ(listing.error, ErrorCode::NONE);
        EXPECT_EQ(std::set(listing.ids.begin(), listing.ids.end()),
                  (std::set<std::string>{key(0), key(1)}));
    }
    for (size_t i = 0; i < before_acquisition.size(); ++i) {
        const auto listing = publishListing(
            before_acquisition[i],
            i == 0 ? protocol::ApiKey::LIST_GROUPS : protocol::ApiKey::LIST_TRANSACTIONS,
            i == 0 ? 4 : 0);
        EXPECT_EQ(listing.error, ErrorCode::NOT_COORDINATOR);
        EXPECT_TRUE(listing.ids.empty());
    }
    before_acquisition.clear();
}
TEST_F(CoordinatorClusterPersistenceTest, OwnedListingsEmptyOwnedSetIsFencedAgainstNewAcquisition) {
    startCluster();
    ASSERT_EQ(put(*brokers[0], key(0), {groupRecord(key(0))}), ErrorCode::NONE);
    ASSERT_EQ(init(*brokers[0], transaction(key(0))), ErrorCode::NONE);
    KawasanBroker* controller = nullptr;
    ASSERT_TRUE(waitUntil([&] {
        for (auto& broker : brokers)
            if (broker->raftNode()->isLeader())
                controller = broker.get();
        return controller != nullptr;
    }));
    for (const auto topic : {"__consumer_offsets", "__transaction_state"})
        ASSERT_EQ(controller->metadataController()->updatePartitionLeader(topic, 0, 1).error_code,
                  ErrorCode::NONE);
    ASSERT_TRUE(waitUntil([&] {
        brokers[0]->reconcileReplicas();
        const auto groups = listingWire(*brokers[0], protocol::ApiKey::LIST_GROUPS, 4);
        const auto txns = listingWire(*brokers[0], protocol::ApiKey::LIST_TRANSACTIONS, 0);
        return groups.error == ErrorCode::NONE && groups.ids.empty() &&
               txns.error == ErrorCode::NONE && txns.ids.empty();
    }));
    std::vector<RequestDispatcher::DispatchResult> results;
    for (const auto api : {protocol::ApiKey::LIST_GROUPS, protocol::ApiKey::LIST_TRANSACTIONS}) {
        const auto version = api == protocol::ApiKey::LIST_GROUPS ? 4 : 0;
        std::promise<RequestDispatcher::DispatchResult> delivered;
        EXPECT_TRUE(CoordinatorExecutorProbe::dispatchProtocol(
                        *brokers[0], api, version, listingPayload(api, version),
                        [&](auto result) { delivered.set_value(std::move(result)); })
                        .deferred);
        auto ready = delivered.get_future();
        ASSERT_EQ(ready.wait_for(5s), std::future_status::ready);
        results.push_back(ready.get());
        EXPECT_EQ(publishListing(results.back(), api, version).error, ErrorCode::NONE);
    }
    for (const auto topic : {"__consumer_offsets", "__transaction_state"})
        ASSERT_EQ(controller->metadataController()->updatePartitionLeader(topic, 0, 0).error_code,
                  ErrorCode::NONE);
    ASSERT_TRUE(waitUntil([&] {
        return brokers[0]->coordinatorLoadStatus(key(0), protocol::CoordinatorType::GROUP) ==
                   ErrorCode::NONE &&
               brokers[0]->coordinatorLoadStatus(key(0), protocol::CoordinatorType::TRANSACTION) ==
                   ErrorCode::NONE;
    }));
    for (size_t i = 0; i < results.size(); ++i) {
        const auto listing = publishListing(
            results[i],
            i == 0 ? protocol::ApiKey::LIST_GROUPS : protocol::ApiKey::LIST_TRANSACTIONS,
            i == 0 ? 4 : 0);
        EXPECT_EQ(listing.error, ErrorCode::NOT_COORDINATOR);
        EXPECT_TRUE(listing.ids.empty());
    }
    results.clear();
}
TEST_F(CoordinatorPersistenceTest, OwnedListingsQueuedReadsRefuseNewerEpochs) {
    KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
    b.start();
    b.reconcileReplicas();
    ASSERT_EQ(put(b, key(0), {groupRecord(key(0))}), ErrorCode::NONE);
    ASSERT_EQ(init(b, transaction(key(0))), ErrorCode::NONE);
    std::promise<void> release;
    const auto released = release.get_future().share();
    struct ReleaseWorkers {
        std::promise<void>& release;
        bool done = false;
        void finish() {
            if (!done) {
                done = true;
                release.set_value();
            }
        }
        ~ReleaseWorkers() { finish(); }
    } guard{release};
    const auto entered = std::make_shared<std::atomic<int>>(0);
    for (int worker = 0; worker < 2; ++worker)
        ASSERT_TRUE(CoordinatorExecutorProbe::submit(b, [entered, released](bool, auto) {
            ++*entered;
            released.wait();
        }));
    ASSERT_TRUE(waitUntil([&] { return entered->load() == 2; }));
    std::vector<std::future<RequestDispatcher::DispatchResult>> results;
    for (const auto api : {protocol::ApiKey::LIST_GROUPS, protocol::ApiKey::LIST_TRANSACTIONS}) {
        const auto version = api == protocol::ApiKey::LIST_GROUPS ? 4 : 0;
        auto delivered = std::make_shared<std::promise<RequestDispatcher::DispatchResult>>();
        results.push_back(delivered->get_future());
        EXPECT_TRUE(CoordinatorExecutorProbe::dispatchProtocol(
                        b, api, version, listingPayload(api, version),
                        [delivered](auto result) { delivered->set_value(std::move(result)); })
                        .deferred);
    }
    for (const auto topic : {"__consumer_offsets", "__transaction_state"})
        ASSERT_EQ(b.metadataController()->updatePartitionLeader(topic, 2, 0).error_code,
                  ErrorCode::NONE);
    b.reconcileReplicas();
    b.acquireCoordinatorPartitions();
    guard.finish();
    for (size_t i = 0; i < results.size(); ++i) {
        ASSERT_EQ(results[i].wait_for(5s), std::future_status::ready);
        auto result = results[i].get();
        const auto listing = publishListing(
            result, i == 0 ? protocol::ApiKey::LIST_GROUPS : protocol::ApiKey::LIST_TRANSACTIONS,
            i == 0 ? 4 : 0);
        EXPECT_EQ(listing.error, ErrorCode::NOT_COORDINATOR);
        EXPECT_TRUE(listing.ids.empty());
    }
    b.stop();
}
TEST_F(CoordinatorPersistenceTest, OwnedListingsHoldCompleteSourceSetThroughWriteInitiation) {
    KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
    b.start();
    b.reconcileReplicas();
    ASSERT_EQ(init(b, transaction(key(0))), ErrorCode::NONE);
    std::promise<RequestDispatcher::DispatchResult> delivered;
    EXPECT_TRUE(CoordinatorExecutorProbe::dispatchProtocol(
                    b, protocol::ApiKey::LIST_TRANSACTIONS, 0,
                    listingPayload(protocol::ApiKey::LIST_TRANSACTIONS, 0),
                    [&](auto result) { delivered.set_value(std::move(result)); })
                    .deferred);
    auto ready = delivered.get_future();
    ASSERT_EQ(ready.wait_for(5s), std::future_status::ready);
    auto result = ready.get();
    std::promise<void> entered, release;
    auto publishing = entered.get_future();
    const auto released = release.get_future().share();
    auto publication = std::async(std::launch::async, [&] {
        result.publication_guard([&](auto status) {
            EXPECT_EQ(status, ErrorCode::NONE);
            entered.set_value();
            released.wait();
        });
    });
    const bool started = publishing.wait_for(5s) == std::future_status::ready;
    if (!started)
        release.set_value();
    ASSERT_TRUE(started);
    auto changed = std::async(std::launch::async, [&] {
        return b.metadataController()->updatePartitionLeader("__transaction_state", 2, 0);
    });
    EXPECT_EQ(changed.wait_for(50ms), std::future_status::timeout);
    release.set_value();
    publication.get();
    EXPECT_EQ(changed.get().error_code, ErrorCode::NONE);
    EXPECT_EQ(publishListing(result, protocol::ApiKey::LIST_TRANSACTIONS, 0).error,
              ErrorCode::NOT_COORDINATOR);
    result = {};
    b.stop();
}
TEST_F(CoordinatorPersistenceTest,
       OwnedListingsRestartRebuildsCachesAndKeepsPrepareWithoutRedrive) {
    auto snapshot = transaction(key(0));
    snapshot.state = TransactionCoordinator::State::PrepareAbort;
    {
        KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
        b.start();
        b.reconcileReplicas();
        ASSERT_EQ(put(b, key(0), {groupRecord(key(0))}), ErrorCode::NONE);
        ASSERT_EQ(init(b, snapshot), ErrorCode::NONE);
        b.stop();
    }
    fs::remove_all(dir / "0/consumer_offsets");
    KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
    b.start();
    b.reconcileReplicas();
    const auto end = b.logManager()->getLog("__transaction_state", 0)->logEndOffset();
    EXPECT_EQ(listingWire(b, protocol::ApiKey::LIST_GROUPS, 4).ids,
              std::vector<std::string>{key(0)});
    const auto txns = listingWire(b, protocol::ApiKey::LIST_TRANSACTIONS, 0);
    EXPECT_EQ(txns.error, ErrorCode::NONE);
    EXPECT_EQ(txns.ids, std::vector<std::string>{key(0)});
    EXPECT_EQ(txns.states, std::vector<std::string>{"PrepareAbort"});
    EXPECT_EQ(CoordinatorPersistenceProbe::transactions(b).describe(key(0)), snapshot);
    EXPECT_EQ(b.logManager()->getLog("__transaction_state", 0)->logEndOffset(), end);
    b.stop();
}

TEST_F(CoordinatorPersistenceTest, ProducerInspectionUnknownPartitionsDoNotCreateLogs) {
    KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
    b.start();
    b.reconcileReplicas();
    Buffer payload;
    protocol::RequestHeader(protocol::ApiKey::DESCRIBE_PRODUCERS, 0, 91, "listing-test")
        .encode(payload);
    payload.writeCompactArrayLen(1);
    payload.writeCompactString("data");
    payload.writeCompactArrayLen(1);
    payload.writeInt32(7);
    payload.writeEmptyTaggedFields();
    payload.writeEmptyTaggedFields();
    auto body = test_support::brokerRequest(b.port(), payload);
    protocol::ResponseHeader header;
    header.setFlexible(true);
    header.decode(body);
    EXPECT_EQ(body.readInt32(), 0);
    ASSERT_EQ(body.readCompactArrayLen(), 1);
    EXPECT_EQ(body.readCompactString(), "data");
    ASSERT_EQ(body.readCompactArrayLen(), 1);
    EXPECT_EQ(body.readInt32(), 7);
    EXPECT_EQ(static_cast<ErrorCode>(body.readInt16()), ErrorCode::UNKNOWN_TOPIC_OR_PARTITION);
    (void)body.readCompactNullableString();
    EXPECT_EQ(body.readCompactArrayLen(), 0);
    body.skipTaggedFields();
    body.skipTaggedFields();
    body.skipTaggedFields();
    EXPECT_EQ(body.remaining(), 0u);
    b.stop();
}
TEST_F(CoordinatorPersistenceTest, KeyedInspectionWireReadsCommittedStateAndErrors) {
    KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
    b.start();
    b.reconcileReplicas();
    const auto id = key(0);
    ASSERT_EQ(put(b, id, {groupRecord(id)}), ErrorCode::NONE);
    ASSERT_EQ(init(b, transaction(id)), ErrorCode::NONE);
    for (int16_t version = 0; version <= 5; ++version) {
        const auto entries =
            inspectionWire(b, protocol::ApiKey::DESCRIBE_GROUPS, {id, "missing", id, ""}, version);
        ASSERT_EQ(entries.size(), 4u);
        EXPECT_EQ(entries[0].error, ErrorCode::NONE);
        EXPECT_EQ(entries[0].state, "Stable");
        ASSERT_EQ(entries[0].members.size(), 1u);
        EXPECT_EQ(entries[0].members[0].client_id, "client");
        EXPECT_EQ(entries[0].members[0].member_assignment, std::vector<uint8_t>{128});
        if (version >= 4)
            EXPECT_EQ(entries[0].members[0].group_instance_id, "instance");
        EXPECT_EQ(entries[1].state, "Dead");
        EXPECT_EQ(entries[1].error, ErrorCode::NONE);
        EXPECT_EQ(entries[2].id, id);
        EXPECT_EQ(entries[3].error, ErrorCode::INVALID_GROUP_ID);
    }
    const auto txns =
        inspectionWire(b, protocol::ApiKey::DESCRIBE_TRANSACTIONS, {id, "missing", id, ""}, 0);
    ASSERT_EQ(txns.size(), 4u);
    EXPECT_EQ(txns[0].error, ErrorCode::NONE);
    EXPECT_EQ(txns[0].state, "Ongoing");
    EXPECT_EQ(txns[0].producer_id, 75);
    EXPECT_EQ(txns[0].producer_epoch, 2);
    EXPECT_EQ(txns[0].transaction_timeout_ms, 1000);
    EXPECT_EQ(txns[0].transaction_start_time_ms, 123);
    EXPECT_EQ(txns[0].partitions, (std::vector<TopicPartition>{{"data", 0}}));
    EXPECT_EQ(txns[1].error, ErrorCode::TRANSACTIONAL_ID_NOT_FOUND);
    EXPECT_EQ(txns[1].producer_id, -1);
    EXPECT_EQ(txns[2].id, id);
    EXPECT_EQ(txns[3].error, ErrorCode::INVALID_REQUEST);
    EXPECT_TRUE(inspectionWire(b, protocol::ApiKey::DESCRIBE_GROUPS, {}, 5).empty());
    EXPECT_TRUE(inspectionWire(b, protocol::ApiKey::DESCRIBE_TRANSACTIONS, {}, 0).empty());
    b.stop();
}

TEST_F(CoordinatorPersistenceTest, KeyedInspectionResponsesFenceEachEpochAndDrainShutdown) {
    for (const auto api :
         {protocol::ApiKey::DESCRIBE_GROUPS, protocol::ApiKey::DESCRIBE_TRANSACTIONS}) {
        const auto version = api == protocol::ApiKey::DESCRIBE_GROUPS ? 5 : 0;
        const auto topic =
            api == protocol::ApiKey::DESCRIBE_GROUPS ? "__consumer_offsets" : "__transaction_state";
        // Distinct storage for each broker instance and protocol.
        auto c = config();
        c.setString("log.dirs", (dir / topic).string());
        KawasanBroker b(c, KawasanBroker::CoordinatorPersistenceOnly{format});
        b.start();
        b.reconcileReplicas();
        for (const auto p : {0, 1}) {
            ASSERT_EQ(put(b, key(p), {groupRecord(key(p))}), ErrorCode::NONE);
            ASSERT_EQ(init(b, transaction(key(p))), ErrorCode::NONE);
        }
        std::promise<RequestDispatcher::DispatchResult> delivered;
        EXPECT_TRUE(CoordinatorExecutorProbe::dispatchProtocol(
                        b, api, version, inspectionPayload(api, {key(0), key(0), key(1)}, version),
                        [&](auto result) { delivered.set_value(std::move(result)); })
                        .deferred);
        auto ready = delivered.get_future();
        ASSERT_EQ(ready.wait_for(5s), std::future_status::ready);
        auto result = ready.get();
        const auto before = publishInspection(result, api, version);
        ASSERT_EQ(before.size(), 3u);
        EXPECT_EQ(before[0].error, ErrorCode::NONE);
        ASSERT_EQ(b.metadataController()->updatePartitionLeader(topic, 0, 0).error_code,
                  ErrorCode::NONE);
        b.reconcileReplicas();
        b.acquireCoordinatorPartitions();
        const auto after = publishInspection(result, api, version);
        ASSERT_EQ(after.size(), 3u);
        EXPECT_EQ(after[0].error, ErrorCode::NOT_COORDINATOR);
        EXPECT_EQ(after[1].error, ErrorCode::NOT_COORDINATOR);
        EXPECT_TRUE(after[0].members.empty());
        EXPECT_EQ(after[0].producer_id, -1);
        EXPECT_EQ(after[2].error, ErrorCode::NONE);
        auto stopped = std::async(std::launch::async, [&] { b.stop(); });
        EXPECT_TRUE(waitUntil([&] { return CoordinatorExecutorProbe::stopping(b); }));
        EXPECT_EQ(stopped.wait_for(50ms), std::future_status::timeout);
        result = {};
        ASSERT_EQ(stopped.wait_for(5s), std::future_status::ready);
        stopped.get();
    }
}

TEST_F(CoordinatorClusterPersistenceTest,
       KeyedInspectionClusterRefusesOtherOwnersWithPerKeyErrors) {
    startCluster();
    for (int owner = 0; owner < 2; ++owner) {
        ASSERT_EQ(put(*brokers[owner], key(owner), {groupRecord(key(owner))}), ErrorCode::NONE);
        ASSERT_EQ(init(*brokers[owner], transaction(key(owner))), ErrorCode::NONE);
    }
    // A poisoned nonowned cache must never become visible through the wire.
    auto poisoned = transaction(key(1));
    poisoned.producer_id = 999;
    CoordinatorPersistenceProbe::transactions(*brokers[0]).restore(poisoned);
    for (const auto api :
         {protocol::ApiKey::DESCRIBE_GROUPS, protocol::ApiKey::DESCRIBE_TRANSACTIONS}) {
        const auto version = api == protocol::ApiKey::DESCRIBE_GROUPS ? 5 : 0;
        const auto entries = inspectionWire(*brokers[0], api, {key(0), key(1), ""}, version);
        ASSERT_EQ(entries.size(), 3u);
        EXPECT_EQ(entries[0].id, key(0));
        EXPECT_EQ(entries[0].error, ErrorCode::NONE);
        EXPECT_EQ(entries[1].id, key(1));
        EXPECT_EQ(entries[1].error, ErrorCode::NOT_COORDINATOR);
        EXPECT_EQ(entries[1].producer_id, -1);
        EXPECT_TRUE(entries[1].members.empty());
        EXPECT_EQ(entries[2].error, api == protocol::ApiKey::DESCRIBE_GROUPS
                                        ? ErrorCode::INVALID_GROUP_ID
                                        : ErrorCode::INVALID_REQUEST);
    }
    KawasanBroker* controller = nullptr;
    ASSERT_TRUE(waitUntil([&] {
        for (auto& broker : brokers)
            if (broker->raftNode()->isLeader())
                controller = broker.get();
        return controller != nullptr;
    }));
    for (const auto topic : {"__consumer_offsets", "__transaction_state"})
        ASSERT_EQ(controller->metadataController()->updatePartitionLeader(topic, 0, 1).error_code,
                  ErrorCode::NONE);
    ASSERT_TRUE(waitUntil([&] {
        brokers[1]->reconcileReplicas();
        return brokers[1]->coordinatorLoadStatus(key(0), protocol::CoordinatorType::GROUP) ==
                   ErrorCode::NONE &&
               brokers[1]->coordinatorLoadStatus(key(0), protocol::CoordinatorType::TRANSACTION) ==
                   ErrorCode::NONE;
    }));
    const auto groups = inspectionWire(*brokers[1], protocol::ApiKey::DESCRIBE_GROUPS, {key(0)}, 5);
    ASSERT_EQ(groups.size(), 1u);
    EXPECT_EQ(groups[0].error, ErrorCode::NONE);
    ASSERT_EQ(groups[0].members.size(), 1u);
    EXPECT_EQ(groups[0].members[0].group_instance_id, "instance");
    const auto txns =
        inspectionWire(*brokers[1], protocol::ApiKey::DESCRIBE_TRANSACTIONS, {key(0)}, 0);
    ASSERT_EQ(txns.size(), 1u);
    EXPECT_EQ(txns[0].error, ErrorCode::NONE);
    EXPECT_EQ(txns[0].producer_id, 75);
    EXPECT_EQ(txns[0].state, "Ongoing");
}

TEST_F(CoordinatorPersistenceTest, KeyedInspectionUncertainSourcesRefuseCachedSuccess) {
    KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
    b.start();
    b.reconcileReplicas();
    for (int p : {0, 1}) {
        ASSERT_EQ(put(b, key(p), {groupRecord(key(p))}), ErrorCode::NONE);
        ASSERT_EQ(init(b, transaction(key(p))), ErrorCode::NONE);
    }
    fs::create_directory(dir / "0/__consumer_offsets-0/checkpoint.meta.tmp");
    EXPECT_EQ(put(b, key(0), {offsetRecord(key(0))}), ErrorCode::KAFKA_STORAGE_ERROR);
    fs::create_directory(dir / "0/__transaction_state-0/checkpoint.meta.tmp");
    const auto previous = CoordinatorPersistenceProbe::transactions(b).describe(key(0));
    EXPECT_EQ(b.mutateCoordinatorTransaction(key(0), previous,
                                             [](auto& proposed) {
                                                 ++proposed->producer_epoch;
                                                 return ErrorCode::NONE;
                                             }),
              ErrorCode::KAFKA_STORAGE_ERROR);
    for (const auto api :
         {protocol::ApiKey::DESCRIBE_GROUPS, protocol::ApiKey::DESCRIBE_TRANSACTIONS}) {
        const auto version = api == protocol::ApiKey::DESCRIBE_GROUPS ? 5 : 0;
        const auto entries = inspectionWire(b, api, {key(0), key(1)}, version);
        ASSERT_EQ(entries.size(), 2u);
        EXPECT_EQ(entries[0].error, ErrorCode::COORDINATOR_LOAD_IN_PROGRESS);
        EXPECT_TRUE(entries[0].members.empty());
        EXPECT_EQ(entries[0].producer_id, -1);
        EXPECT_EQ(entries[1].error, ErrorCode::NONE);
    }
    b.stop();
}

TEST_F(CoordinatorPersistenceTest, KeyedInspectionRestartReadsAcquiredCachesWithoutRedrive) {
    const auto id = key(0);
    auto snapshot = transaction(id);
    snapshot.state = TransactionCoordinator::State::PrepareAbort;
    {
        KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
        b.start();
        b.reconcileReplicas();
        ASSERT_EQ(put(b, id, {groupRecord(id)}), ErrorCode::NONE);
        ASSERT_EQ(init(b, snapshot), ErrorCode::NONE);
        b.stop();
    }
    ASSERT_TRUE(fs::exists(dir / "0/consumer_offsets"));
    fs::remove_all(dir / "0/consumer_offsets");
    KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
    b.start();
    b.reconcileReplicas();
    const auto end = b.logManager()->getLog("__transaction_state", 0)->logEndOffset();
    const auto groups = inspectionWire(b, protocol::ApiKey::DESCRIBE_GROUPS, {id}, 5);
    ASSERT_EQ(groups.size(), 1u);
    EXPECT_EQ(groups[0].error, ErrorCode::NONE);
    ASSERT_EQ(groups[0].members.size(), 1u);
    EXPECT_EQ(groups[0].members[0].group_instance_id, "instance");
    const auto txns = inspectionWire(b, protocol::ApiKey::DESCRIBE_TRANSACTIONS, {id}, 0);
    ASSERT_EQ(txns.size(), 1u);
    EXPECT_EQ(txns[0].error, ErrorCode::NONE);
    EXPECT_EQ(txns[0].state, "PrepareAbort");
    EXPECT_EQ(txns[0].producer_id, snapshot.producer_id);
    EXPECT_EQ(CoordinatorPersistenceProbe::transactions(b).describe(id), snapshot);
    EXPECT_EQ(b.logManager()->getLog("__transaction_state", 0)->logEndOffset(), end);
    b.stop();
}

TEST_F(CoordinatorPersistenceTest, KeyedInspectionAcquisitionOnlyKeepsPerKeyRefusals) {
    KawasanBroker b(config(), KawasanBroker::CoordinatorAcquisitionOnly{format});
    b.start();
    b.reconcileReplicas();
    for (int16_t version = 0; version <= 5; ++version) {
        const auto entries =
            inspectionWire(b, protocol::ApiKey::DESCRIBE_GROUPS, {key(0), key(1)}, version);
        ASSERT_EQ(entries.size(), 2u);
        for (const auto& entry : entries) {
            EXPECT_EQ(entry.error, ErrorCode::COORDINATOR_LOAD_IN_PROGRESS);
            EXPECT_TRUE(entry.members.empty());
        }
    }
    const auto txns =
        inspectionWire(b, protocol::ApiKey::DESCRIBE_TRANSACTIONS, {key(0), key(1)}, 0);
    ASSERT_EQ(txns.size(), 2u);
    for (const auto& entry : txns) {
        EXPECT_EQ(entry.error, ErrorCode::COORDINATOR_LOAD_IN_PROGRESS);
        EXPECT_EQ(entry.producer_id, -1);
    }
    b.stop();
}

TEST_F(CoordinatorPersistenceTest, KeyedInspectionQueuedReadsRefuseReacquiredEpoch) {
    KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
    b.start();
    b.reconcileReplicas();
    for (int p : {0, 1}) {
        ASSERT_EQ(put(b, key(p), {groupRecord(key(p))}), ErrorCode::NONE);
        ASSERT_EQ(init(b, transaction(key(p))), ErrorCode::NONE);
    }
    std::array<std::promise<void>, 2> release;
    const std::array<std::shared_future<void>, 2> released{release[0].get_future().share(),
                                                           release[1].get_future().share()};
    struct ReleaseWorkers {
        std::array<std::promise<void>, 2>& release;
        bool first_done = false;
        bool done = false;
        void finish() {
            if (!done) {
                done = true;
                if (!first_done)
                    release[0].set_value();
                release[1].set_value();
            }
        }
        ~ReleaseWorkers() { finish(); }
    } guard{release};
    const auto entered = std::make_shared<std::atomic<int>>(0);
    for (int worker = 0; worker < 2; ++worker)
        ASSERT_TRUE(CoordinatorExecutorProbe::submit(b, [entered, released, worker](bool, auto) {
            ++*entered;
            released[worker].wait();
        }));
    ASSERT_TRUE(waitUntil([&] { return entered->load() == 2; }));
    std::vector<std::future<RequestDispatcher::DispatchResult>> results;
    for (const auto api :
         {protocol::ApiKey::DESCRIBE_GROUPS, protocol::ApiKey::DESCRIBE_TRANSACTIONS}) {
        const auto version = api == protocol::ApiKey::DESCRIBE_GROUPS ? 5 : 0;
        auto delivered = std::make_shared<std::promise<RequestDispatcher::DispatchResult>>();
        results.push_back(delivered->get_future());
        EXPECT_TRUE(CoordinatorExecutorProbe::dispatchProtocol(
                        b, api, version, inspectionPayload(api, {key(0), key(1)}, version),
                        [delivered](auto result) { delivered->set_value(std::move(result)); })
                        .deferred);
    }
    for (const auto topic : {"__consumer_offsets", "__transaction_state"})
        ASSERT_EQ(b.metadataController()->updatePartitionLeader(topic, 0, 0).error_code,
                  ErrorCode::NONE);
    b.reconcileReplicas();
    b.acquireCoordinatorPartitions();
    release[0].set_value();
    guard.first_done = true;
    // Both read jobs must finish before publication: publishing the first while
    // the worker reads the second legitimately trips the busy-acquisition fence.
    for (auto& result : results)
        ASSERT_EQ(result.wait_for(5s), std::future_status::ready);
    for (size_t i = 0; i < results.size(); ++i) {
        auto result = results[i].get();
        const auto entries = publishInspection(
            result,
            i == 0 ? protocol::ApiKey::DESCRIBE_GROUPS : protocol::ApiKey::DESCRIBE_TRANSACTIONS,
            i == 0 ? 5 : 0);
        ASSERT_EQ(entries.size(), 2u);
        EXPECT_EQ(entries[0].error, ErrorCode::NOT_COORDINATOR);
        EXPECT_EQ(entries[0].producer_id, -1);
        EXPECT_TRUE(entries[0].members.empty());
        EXPECT_EQ(entries[1].error, ErrorCode::NONE);
    }
    guard.finish();
    b.stop();
}

TEST_F(CoordinatorPersistenceTest, KeyedInspectionHoldsAllSourcesThroughWriteInitiation) {
    KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
    b.start();
    b.reconcileReplicas();
    for (int p : {0, 1})
        ASSERT_EQ(init(b, transaction(key(p))), ErrorCode::NONE);
    std::promise<RequestDispatcher::DispatchResult> delivered;
    EXPECT_TRUE(
        CoordinatorExecutorProbe::dispatchProtocol(
            b, protocol::ApiKey::DESCRIBE_TRANSACTIONS, 0,
            inspectionPayload(protocol::ApiKey::DESCRIBE_TRANSACTIONS, {key(0), key(0), key(1)}, 0),
            [&](auto result) { delivered.set_value(std::move(result)); })
            .deferred);
    auto ready = delivered.get_future();
    ASSERT_EQ(ready.wait_for(5s), std::future_status::ready);
    auto result = ready.get();
    std::promise<void> entered, release;
    auto publishing = entered.get_future();
    const auto released = release.get_future().share();
    auto publication = std::async(std::launch::async, [&] {
        result.publication_guard([&](auto error) {
            EXPECT_EQ(error, ErrorCode::NONE);
            entered.set_value();
            released.wait();
        });
    });
    const bool started = publishing.wait_for(5s) == std::future_status::ready;
    if (!started)
        release.set_value();
    ASSERT_TRUE(started);
    auto changed = std::async(std::launch::async, [&] {
        return b.metadataController()->updatePartitionLeader("__transaction_state", 1, 0);
    });
    EXPECT_EQ(changed.wait_for(50ms), std::future_status::timeout);
    release.set_value();
    publication.get();
    EXPECT_EQ(changed.get().error_code, ErrorCode::NONE);
    const auto fenced = publishInspection(result, protocol::ApiKey::DESCRIBE_TRANSACTIONS, 0);
    ASSERT_EQ(fenced.size(), 3u);
    EXPECT_EQ(fenced[0].error, ErrorCode::NONE);
    EXPECT_EQ(fenced[1].error, ErrorCode::NONE);
    EXPECT_EQ(fenced[2].error, ErrorCode::NOT_COORDINATOR);
    result = {};
    b.stop();
}

TEST_F(CoordinatorPersistenceTest, KeyedInspectionDefaultMissingAndDeadTransactionsUseKafkaErrors) {
    KawasanBroker b(config());
    b.start();
    auto dead = transaction("expired");
    dead.state = TransactionCoordinator::State::Dead;
    CoordinatorPersistenceProbe::transactions(b).restore(dead);
    const auto txns =
        inspectionWire(b, protocol::ApiKey::DESCRIBE_TRANSACTIONS, {"missing", "expired", ""}, 0);
    ASSERT_EQ(txns.size(), 3u);
    EXPECT_EQ(txns[0].error, ErrorCode::TRANSACTIONAL_ID_NOT_FOUND);
    EXPECT_EQ(txns[1].error, ErrorCode::TRANSACTIONAL_ID_NOT_FOUND);
    EXPECT_EQ(txns[1].producer_id, -1);
    EXPECT_EQ(txns[2].error, ErrorCode::INVALID_REQUEST);
    b.stop();
}

TEST_F(CoordinatorPersistenceTest, OffsetProtocolValidationDoesNotMutateCommittedSource) {
    KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
    b.start();
    b.reconcileReplicas();
    const auto id = key(0);
    ASSERT_EQ(put(b, id, {groupRecord(id), offsetRecord(id)}), ErrorCode::NONE);
    const auto before =
        CoordinatorAcquisitionProbe::runtime(CoordinatorPersistenceProbe::groups(b), id);
    const auto end = b.logManager()->getLog("__consumer_offsets", 0)->logEndOffset();
    auto request = offsetCommit(id);
    request.setMemberId("old-member");
    EXPECT_EQ(membershipWire<protocol::OffsetCommitResponse>(b, protocol::ApiKey::OFFSET_COMMIT,
                                                             request, 8)
                  .topics()[0]
                  .partitions[0]
                  .error,
              ErrorCode::FENCED_INSTANCE_ID);
    request = offsetCommit(id);
    request.setGenerationId(6);
    EXPECT_EQ(membershipWire<protocol::OffsetCommitResponse>(b, protocol::ApiKey::OFFSET_COMMIT,
                                                             request, 8)
                  .topics()[0]
                  .partitions[0]
                  .error,
              ErrorCode::ILLEGAL_GENERATION);
    request = offsetCommit(id);
    request.setGenerationId(-1);
    request.setMemberId("");
    request.setGroupInstanceId(std::nullopt);
    EXPECT_EQ(membershipWire<protocol::OffsetCommitResponse>(b, protocol::ApiKey::OFFSET_COMMIT,
                                                             request, 8)
                  .topics()[0]
                  .partitions[0]
                  .error,
              ErrorCode::UNKNOWN_MEMBER_ID);
    EXPECT_EQ(b.logManager()->getLog("__consumer_offsets", 0)->logEndOffset(), end);
    EXPECT_EQ(CoordinatorAcquisitionProbe::runtime(CoordinatorPersistenceProbe::groups(b), id),
              before);
    const auto manual = key(1);
    request.setGroupId(manual);
    EXPECT_EQ(membershipWire<protocol::OffsetCommitResponse>(b, protocol::ApiKey::OFFSET_COMMIT,
                                                             request, 8)
                  .topics()[0]
                  .partitions[0]
                  .error,
              ErrorCode::NONE);
    EXPECT_EQ(committedGroup(b, manual).state, 0);
    EXPECT_EQ(CoordinatorPersistenceProbe::offsets(b).fetchOffset(manual, "data", 0), 123);
    // v0 manual commits and legacy single-group reads retain their negotiated shape.
    EXPECT_EQ(membershipWire<protocol::OffsetCommitResponse>(b, protocol::ApiKey::OFFSET_COMMIT,
                                                             request, 0)
                  .topics()[0]
                  .partitions[0]
                  .error,
              ErrorCode::NONE);
    protocol::OffsetFetchRequest fetch;
    fetch.setGroupId(manual);
    fetch.setTopics({{"data", {{0}, {1}}}});
    const auto result =
        membershipWire<protocol::OffsetFetchResponse>(b, protocol::ApiKey::OFFSET_FETCH, fetch, 0);
    EXPECT_EQ(result.topics()[0].partitions[0].offset, 123);
    EXPECT_EQ(result.topics()[0].partitions[1].offset, -1);
    b.stop();
}

TEST_F(CoordinatorPersistenceTest, ActiveConsumerDeletionChecksSubscriptionsAndGroupIdentity) {
    KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
    b.start();
    b.reconcileReplicas();
    const auto id = key(0);
    auto group = groupRecord(id);
    Buffer subscription;
    subscription.writeInt16(0);
    subscription.writeInt32(1);
    subscription.writeString("data");
    subscription.writeInt32(-1);
    group.group.members[0].metadata.assign(subscription.data(),
                                           subscription.data() + subscription.size());
    auto other = offsetRecord(id, 90);
    other.key.topic = "old-topic";
    ASSERT_EQ(put(b, id, {group, offsetRecord(id), other}), ErrorCode::NONE);
    auto nonempty = deleteWire(b, protocol::ApiKey::DELETE_GROUPS, {}, {id});
    nonempty.readInt32();
    EXPECT_EQ(nonempty.readInt32(), 1);
    EXPECT_EQ(nonempty.readString(), id);
    EXPECT_EQ(static_cast<ErrorCode>(nonempty.readInt16()), ErrorCode::NON_EMPTY_GROUP);
    Buffer body;
    body.writeString(id);
    body.writeInt32(2);
    for (const auto& topic : {"data", "old-topic"}) {
        body.writeString(topic);
        body.writeInt32(1);
        body.writeInt32(0);
    }
    std::promise<RequestDispatcher::DispatchResult> response;
    EXPECT_TRUE(CoordinatorExecutorProbe::dispatchProtocol(
                    b, protocol::ApiKey::OFFSET_DELETE, 0, body,
                    [&](auto result) { response.set_value(std::move(result)); })
                    .deferred);
    auto future = response.get_future();
    ASSERT_EQ(future.wait_for(5s), std::future_status::ready);
    auto result = future.get();
    const auto deleted =
        publishProtocol<protocol::OffsetDeleteResponse>(result, protocol::ApiKey::OFFSET_DELETE, 0);
    EXPECT_EQ(deleted.errorCode(), ErrorCode::NONE);
    ASSERT_EQ(deleted.topics().size(), 2u);
    EXPECT_EQ(deleted.topics()[0].partitions[0].error_code, ErrorCode::GROUP_SUBSCRIBED_TO_TOPIC);
    EXPECT_EQ(deleted.topics()[1].partitions[0].error_code, ErrorCode::NONE);
    EXPECT_EQ(CoordinatorPersistenceProbe::offsets(b).fetchOffset(id, "data", 0), 42);
    EXPECT_FALSE(CoordinatorPersistenceProbe::offsets(b).fetchOffset(id, "old-topic", 0));
    EXPECT_EQ(committedGroup(b, id), group.group);
    result = {};
    b.stop();
}

TEST_F(CoordinatorPersistenceTest, MultiGroupReadsFenceEachSourceThroughDelivery) {
    KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
    b.start();
    b.reconcileReplicas();
    for (int p : {0, 1})
        ASSERT_EQ(put(b, key(p), {offsetRecord(key(p), 50 + p)}), ErrorCode::NONE);
    protocol::OffsetFetchRequest request;
    for (int p : {0, 1})
        request.addGroup({key(p), std::nullopt, -1, true, {}});
    request.addGroup({key(2), "modern-member", 3, true, {}});
    Buffer payload;
    request.encode(payload, 9);
    std::promise<RequestDispatcher::DispatchResult> delivered;
    EXPECT_TRUE(CoordinatorExecutorProbe::dispatchProtocol(
                    b, protocol::ApiKey::OFFSET_FETCH, 9, payload,
                    [&](auto result) { delivered.set_value(std::move(result)); })
                    .deferred);
    auto future = delivered.get_future();
    ASSERT_EQ(future.wait_for(5s), std::future_status::ready);
    auto result = future.get();
    auto before =
        publishProtocol<protocol::OffsetFetchResponse>(result, protocol::ApiKey::OFFSET_FETCH, 9);
    ASSERT_EQ(before.groups().size(), 3u);
    EXPECT_EQ(before.groups()[0].topics[0].partitions[0].offset, 50);
    EXPECT_EQ(before.groups()[1].topics[0].partitions[0].offset, 51);
    EXPECT_EQ(before.groups()[2].error_code, ErrorCode::UNSUPPORTED_VERSION);
    // Same broker, newer epoch is a different admission, even after reacquisition.
    ASSERT_EQ(b.metadataController()->updatePartitionLeader("__consumer_offsets", 0, 0).error_code,
              ErrorCode::NONE);
    b.reconcileReplicas();
    b.acquireCoordinatorPartitions();
    auto after =
        publishProtocol<protocol::OffsetFetchResponse>(result, protocol::ApiKey::OFFSET_FETCH, 9);
    EXPECT_EQ(after.groups()[0].error_code, ErrorCode::NOT_COORDINATOR);
    EXPECT_TRUE(after.groups()[0].topics.empty());
    EXPECT_EQ(after.groups()[1].error_code, ErrorCode::NONE);
    EXPECT_EQ(after.groups()[1].topics[0].partitions[0].offset, 51);
    EXPECT_EQ(after.groups()[2].error_code, ErrorCode::UNSUPPORTED_VERSION);
    result = {};
    b.stop();
}

TEST_F(CoordinatorPersistenceTest,
       MultiGroupDeletionPreservesPerSourceStorageFailureAndDrainsResponse) {
    KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
    b.start();
    b.reconcileReplicas();
    for (int p : {0, 1})
        ASSERT_EQ(put(b, key(p), {emptyGroup(key(p)), offsetRecord(key(p))}), ErrorCode::NONE);
    fs::create_directory(dir / "0/__consumer_offsets-0/checkpoint.meta.tmp");
    Buffer payload;
    payload.writeInt32(2);
    payload.writeString(key(0));
    payload.writeString(key(1));
    std::promise<RequestDispatcher::DispatchResult> delivered;
    EXPECT_TRUE(CoordinatorExecutorProbe::dispatchProtocol(
                    b, protocol::ApiKey::DELETE_GROUPS, 0, payload,
                    [&](auto result) { delivered.set_value(std::move(result)); })
                    .deferred);
    auto future = delivered.get_future();
    ASSERT_EQ(future.wait_for(5s), std::future_status::ready);
    auto result = future.get();
    const auto deleted =
        publishProtocol<protocol::DeleteGroupsResponse>(result, protocol::ApiKey::DELETE_GROUPS, 0);
    ASSERT_EQ(deleted.results().size(), 2u);
    EXPECT_EQ(deleted.results()[0].group_id, key(0));
    EXPECT_EQ(deleted.results()[0].error_code, ErrorCode::KAFKA_STORAGE_ERROR);
    EXPECT_EQ(deleted.results()[1].error_code, ErrorCode::NONE);
    EXPECT_EQ(CoordinatorPersistenceProbe::offsets(b).fetchOffset(key(0), "data", 0), 42);
    EXPECT_FALSE(CoordinatorPersistenceProbe::offsets(b).fetchOffset(key(1), "data", 0));
    auto stopped = std::async(std::launch::async, [&] { b.stop(); });
    EXPECT_TRUE(waitUntil([&] { return CoordinatorExecutorProbe::stopping(b); }));
    EXPECT_EQ(stopped.wait_for(50ms), std::future_status::timeout);
    const auto fenced =
        publishProtocol<protocol::DeleteGroupsResponse>(result, protocol::ApiKey::DELETE_GROUPS, 0);
    EXPECT_EQ(fenced.results()[0].error_code, ErrorCode::NOT_COORDINATOR);
    EXPECT_EQ(fenced.results()[1].error_code, ErrorCode::NOT_COORDINATOR);
    result = {};
    EXPECT_EQ(stopped.wait_for(5s), std::future_status::ready);
    stopped.get();
    fs::remove(dir / "0/__consumer_offsets-0/checkpoint.meta.tmp");
}

TEST_F(CoordinatorPersistenceTest, OwnedRetentionUsesExactKeysAndPreservesPendingOffsets) {
    KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
    b.start();
    b.reconcileReplicas();
    const auto id = key(0);
    const auto neighbor = id + ":suffix";
    auto pending = offsetRecord(id, 99);
    pending.key.kind = GroupRecordKey::Kind::PendingOffset;
    pending.key.transactional_id = "pending-txn";
    pending.key.producer_id = 90;
    pending.key.producer_epoch = 2;
    auto expired = offsetRecord(id);
    expired.key.partition = 1;
    ASSERT_EQ(put(b, id, {emptyGroup(id), offsetRecord(id), expired, pending}), ErrorCode::NONE);
    auto fresh = offsetRecord(neighbor);
    fresh.offset.expiry_timestamp = std::numeric_limits<int64_t>::max();
    ASSERT_EQ(put(b, neighbor, {emptyGroup(neighbor), fresh}), ErrorCode::NONE);
    ASSERT_EQ(put(b, key(1), {emptyGroup(key(1)), offsetRecord(key(1))}), ErrorCode::NONE);
    ASSERT_EQ(put(b, key(2), {offsetRecord(key(2))}), ErrorCode::NONE);
    // Scheduling uses nonblocking source fences. Keep workers from acquiring
    // those locks until every candidate has been admitted to this sweep.
    struct BlockWorkers {
        std::promise<void> release;
        bool finished = false;
        void finish() {
            if (!finished) {
                release.set_value();
                finished = true;
            }
        }
        ~BlockWorkers() { finish(); }
    } blocked;
    const auto released = blocked.release.get_future().share();
    const auto entered = std::make_shared<std::atomic<int>>(0);
    for (int n = 0; n < 2; ++n)
        ASSERT_TRUE(CoordinatorExecutorProbe::submit(b, [entered, released](bool, auto) {
            ++*entered;
            released.wait();
        }));
    ASSERT_TRUE(waitUntil([&] { return entered->load() == 2; }));
    CoordinatorExecutorProbe::scheduleRetention(b);
    for (const auto& candidate : {id, neighbor, key(1), key(2)})
        EXPECT_TRUE(CoordinatorExecutorProbe::retentionPending(b, candidate));
    blocked.finish();
    ASSERT_TRUE(waitUntil([&] {
        return !CoordinatorExecutorProbe::retentionPending(b, id) &&
               !CoordinatorExecutorProbe::retentionPending(b, neighbor) &&
               !CoordinatorExecutorProbe::retentionPending(b, key(1)) &&
               !CoordinatorExecutorProbe::retentionPending(b, key(2));
    }));
    EXPECT_EQ(CoordinatorPersistenceProbe::offsets(b).fetchOffset(id, "data", 0), 42);
    EXPECT_FALSE(CoordinatorPersistenceProbe::offsets(b).fetchOffset(id, "data", 1));
    EXPECT_EQ(CoordinatorPersistenceProbe::groups(b).pendingCoordinatorOffsets().size(), 1u);
    EXPECT_EQ(CoordinatorPersistenceProbe::offsets(b).fetchOffset(neighbor, "data", 0), 42);
    EXPECT_FALSE(CoordinatorPersistenceProbe::offsets(b).fetchOffset(key(1), "data", 0));
    EXPECT_FALSE(CoordinatorPersistenceProbe::offsets(b).fetchOffset(key(2), "data", 0));
    EXPECT_TRUE(GroupStateManager(b.logManager(), 3).loadCommittedPartition(1).empty());
    b.stop();
}

TEST_F(CoordinatorPersistenceTest, QueuedRetentionRechecksCommitRejoinAndOriginalEpoch) {
    KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
    b.start();
    b.reconcileReplicas();
    for (int p : {0, 1, 2})
        ASSERT_EQ(put(b, key(p), {emptyGroup(key(p)), offsetRecord(key(p))}), ErrorCode::NONE);
    std::promise<void> release;
    auto released = release.get_future().share();
    std::atomic<int> entered{0};
    for (int n = 0; n < 2; ++n)
        ASSERT_TRUE(CoordinatorExecutorProbe::submit(b, [&](bool, auto) {
            ++entered;
            released.wait();
        }));
    const bool workers_ready = waitUntil([&] { return entered.load() == 2; });
    if (!workers_ready)
        release.set_value();
    ASSERT_TRUE(workers_ready);
    CoordinatorExecutorProbe::scheduleRetention(b);
    EXPECT_TRUE(CoordinatorExecutorProbe::retentionPending(b, key(0)));
    auto fresh = offsetRecord(key(0), 77);
    fresh.offset.expiry_timestamp = std::numeric_limits<int64_t>::max();
    EXPECT_EQ(put(b, key(0), {fresh}), ErrorCode::NONE);
    EXPECT_EQ(b.mutateCoordinatorGroupState(
                  key(1),
                  [&](auto& draft) {
                      EXPECT_EQ(draft.handleJoinGroup(joinRequest(key(1))).error, ErrorCode::NONE);
                  }),
              ErrorCode::NONE);
    EXPECT_EQ(b.metadataController()->updatePartitionLeader("__consumer_offsets", 2, 0).error_code,
              ErrorCode::NONE);
    b.reconcileReplicas();
    b.acquireCoordinatorPartitions();
    release.set_value();
    ASSERT_TRUE(waitUntil([&] {
        for (int p : {0, 1, 2})
            if (CoordinatorExecutorProbe::retentionPending(b, key(p)))
                return false;
        return true;
    }));
    EXPECT_EQ(CoordinatorPersistenceProbe::offsets(b).fetchOffset(key(0), "data", 0), 77);
    EXPECT_EQ(CoordinatorPersistenceProbe::offsets(b).fetchOffset(key(1), "data", 0), 42);
    EXPECT_EQ(CoordinatorPersistenceProbe::offsets(b).fetchOffset(key(2), "data", 0), 42);
    CoordinatorExecutorProbe::scheduleRetention(b);
    ASSERT_TRUE(waitUntil([&] { return !CoordinatorExecutorProbe::retentionPending(b, key(2)); }));
    EXPECT_FALSE(CoordinatorPersistenceProbe::offsets(b).fetchOffset(key(2), "data", 0));
    b.stop();
}

TEST_F(CoordinatorClusterPersistenceTest,
       ThreeBrokerOffsetWireWaitsForIsrAndAggregatesWrongOwners) {
    startCluster();
    const auto id = key(1);
    auto& b = *brokers[1];
    // Isolate exact wire success from the periodic acquisition try-lock.
    b.replicaManager()->stop();
    ASSERT_EQ(put(b, id, {groupRecord(id)}), ErrorCode::NONE);
    brokers[0]->replicaManager()->stop();
    brokers[2]->replicaManager()->stop();
    auto pending = std::async(std::launch::async, [&] {
        return membershipWire<protocol::OffsetCommitResponse>(b, protocol::ApiKey::OFFSET_COMMIT,
                                                              offsetCommit(id), 8);
    });
    const bool appended = waitUntil(
        [&] { return b.logManager()->getLog("__consumer_offsets", 1)->logEndOffset() == 3; });
    EXPECT_TRUE(appended);
    EXPECT_EQ(pending.wait_for(50ms), std::future_status::timeout);
    EXPECT_FALSE(CoordinatorPersistenceProbe::offsets(b).fetchOffset(id, "data", 0));
    Buffer versions;
    protocol::RequestHeader(protocol::ApiKey::API_VERSIONS, 1, 95, "io-progress").encode(versions);
    protocol::ApiVersionsRequest{}.encode(versions, 1);
    auto progress = test_support::brokerRequest(b.port(), versions);
    EXPECT_EQ(progress.readInt32(), 95);
    brokers[0]->replicaManager()->start();
    brokers[2]->replicaManager()->start();
    ASSERT_EQ(pending.wait_for(10s), std::future_status::ready);
    const auto committed = pending.get();
    ASSERT_EQ(committed.topics().size(), 1u);
    EXPECT_EQ(committed.topics()[0].partitions[0].error, ErrorCode::NONE);
    protocol::OffsetFetchRequest fetch;
    for (int p : {0, 1, 2})
        fetch.addGroup({key(p), std::nullopt, -1, true, {}});
    const auto read =
        membershipWire<protocol::OffsetFetchResponse>(b, protocol::ApiKey::OFFSET_FETCH, fetch, 9);
    ASSERT_EQ(read.groups().size(), 3u);
    EXPECT_EQ(read.groups()[0].error_code, ErrorCode::NOT_COORDINATOR);
    EXPECT_EQ(read.groups()[1].error_code, ErrorCode::NONE);
    EXPECT_EQ(read.groups()[1].topics[0].partitions[0].offset, 123);
    EXPECT_EQ(read.groups()[2].error_code, ErrorCode::NOT_COORDINATOR);
    auto deletes = deleteWire(b, protocol::ApiKey::DELETE_GROUPS, {}, {key(0), key(1), key(2)});
    deletes.readInt32();
    EXPECT_EQ(deletes.readInt32(), 3);
    for (int p : {0, 1, 2}) {
        EXPECT_EQ(deletes.readString(), key(p));
        EXPECT_EQ(static_cast<ErrorCode>(deletes.readInt16()),
                  p == 1 ? ErrorCode::NON_EMPTY_GROUP : ErrorCode::NOT_COORDINATOR);
    }
}

TEST_F(CoordinatorPersistenceTest, OffsetWireRestartRebuildsCommittedCacheAndExactDeletion) {
    const auto id = key(0);
    {
        KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
        b.start();
        b.reconcileReplicas();
        auto request = offsetCommit(id, 321);
        request.setGenerationId(-1);
        request.setMemberId("");
        request.setGroupInstanceId(std::nullopt);
        EXPECT_EQ(membershipWire<protocol::OffsetCommitResponse>(b, protocol::ApiKey::OFFSET_COMMIT,
                                                                 request, 8)
                      .topics()[0]
                      .partitions[0]
                      .error,
                  ErrorCode::NONE);
        auto retained = offsetRecord(id, 222);
        retained.key.partition = 1;
        ASSERT_EQ(put(b, id, {retained}), ErrorCode::NONE);
        auto deleted = deleteWire(b, protocol::ApiKey::OFFSET_DELETE, id);
        EXPECT_EQ(static_cast<ErrorCode>(deleted.readInt16()), ErrorCode::NONE);
        deleted.readInt32();
        b.stop();
    }
    fs::remove_all(dir / "0/consumer_offsets");
    KawasanBroker recovered(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
    recovered.start();
    recovered.reconcileReplicas();
    protocol::OffsetFetchRequest fetch;
    fetch.addGroup({id, std::nullopt, -1, true, {}});
    const auto result = membershipWire<protocol::OffsetFetchResponse>(
        recovered, protocol::ApiKey::OFFSET_FETCH, fetch, 9);
    ASSERT_EQ(result.groups().size(), 1u);
    EXPECT_EQ(result.groups()[0].error_code, ErrorCode::NONE);
    ASSERT_EQ(result.groups()[0].topics.size(), 1u);
    ASSERT_EQ(result.groups()[0].topics[0].partitions.size(), 1u);
    EXPECT_EQ(result.groups()[0].topics[0].partitions[0].partition, 1);
    EXPECT_EQ(result.groups()[0].topics[0].partitions[0].offset, 222);
    recovered.stop();
}

TEST_F(CoordinatorPersistenceTest, OffsetDeletionCheckpointFailureKeepsCacheAndPendingIdentity) {
    KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
    b.start();
    b.reconcileReplicas();
    const auto id = key(0);
    auto pending = offsetRecord(id, 333);
    pending.key.kind = GroupRecordKey::Kind::PendingOffset;
    pending.key.transactional_id = "txn";
    pending.key.producer_id = 10;
    pending.key.producer_epoch = 2;
    ASSERT_EQ(put(b, id, {emptyGroup(id), offsetRecord(id), pending}), ErrorCode::NONE);
    const auto blocked = dir / "0/__consumer_offsets-0/checkpoint.meta.tmp";
    fs::create_directory(blocked);
    auto failed = deleteWire(b, protocol::ApiKey::OFFSET_DELETE, id);
    EXPECT_EQ(static_cast<ErrorCode>(failed.readInt16()), ErrorCode::KAFKA_STORAGE_ERROR);
    failed.readInt32();
    EXPECT_EQ(failed.readInt32(), 1);
    EXPECT_EQ(failed.readString(), "data");
    EXPECT_EQ(failed.readInt32(), 1);
    EXPECT_EQ(failed.readInt32(), 0);
    EXPECT_EQ(static_cast<ErrorCode>(failed.readInt16()), ErrorCode::KAFKA_STORAGE_ERROR);
    EXPECT_EQ(CoordinatorPersistenceProbe::offsets(b).fetchOffset(id, "data", 0), 42);
    EXPECT_EQ(CoordinatorPersistenceProbe::groups(b).pendingCoordinatorOffsets().size(), 1u);
    fs::remove(blocked);
    b.acquireCoordinatorPartitions();
    EXPECT_FALSE(CoordinatorPersistenceProbe::offsets(b).fetchOffset(id, "data", 0));
    EXPECT_EQ(CoordinatorPersistenceProbe::groups(b).pendingCoordinatorOffsets().size(), 1u);
    protocol::OffsetFetchRequest fetch;
    fetch.setRequireStable(true);
    fetch.addGroup({id, std::nullopt, -1, true, {}});
    const auto stable =
        membershipWire<protocol::OffsetFetchResponse>(b, protocol::ApiKey::OFFSET_FETCH, fetch, 9);
    ASSERT_EQ(stable.groups()[0].topics.size(), 1u);
    EXPECT_EQ(stable.groups()[0].topics[0].partitions[0].error, ErrorCode::UNSTABLE_OFFSET_COMMIT);
    fetch.setRequireStable(false);
    EXPECT_TRUE(
        membershipWire<protocol::OffsetFetchResponse>(b, protocol::ApiKey::OFFSET_FETCH, fetch, 9)
            .groups()[0]
            .topics.empty());
    b.stop();
}

TEST_F(CoordinatorPersistenceTest, QueuedRetentionShutdownDrainsJobsWithoutDeletingCache) {
    KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
    b.start();
    b.reconcileReplicas();
    const auto id = key(0);
    ASSERT_EQ(put(b, id, {emptyGroup(id), offsetRecord(id)}), ErrorCode::NONE);
    auto cache = CoordinatorPersistenceProbe::retainOffsets(b);
    std::promise<void> release;
    auto released = release.get_future().share();
    std::atomic<int> entered{0};
    for (int n = 0; n < 2; ++n)
        EXPECT_TRUE(CoordinatorExecutorProbe::submit(b, [&](bool, auto) {
            ++entered;
            released.wait();
        }));
    const bool ready = waitUntil([&] { return entered.load() == 2; });
    if (!ready)
        release.set_value();
    ASSERT_TRUE(ready);
    CoordinatorExecutorProbe::scheduleRetention(b);
    EXPECT_TRUE(CoordinatorExecutorProbe::retentionPending(b, id));
    auto stopped = std::async(std::launch::async, [&] { b.stop(); });
    EXPECT_TRUE(waitUntil([&] { return CoordinatorExecutorProbe::stopping(b); }));
    EXPECT_EQ(stopped.wait_for(50ms), std::future_status::timeout);
    release.set_value();
    EXPECT_EQ(stopped.wait_for(5s), std::future_status::ready);
    stopped.get();
    EXPECT_FALSE(CoordinatorExecutorProbe::retentionPending(b, id));
    EXPECT_EQ(cache->fetchOffset(id, "data", 0), 42);
}

TEST_F(CoordinatorPersistenceTest, MultiSourceResponseHoldsEveryEpochThroughWriteInitiation) {
    KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
    b.start();
    b.reconcileReplicas();
    for (int p : {0, 1})
        ASSERT_EQ(put(b, key(p), {offsetRecord(key(p))}), ErrorCode::NONE);
    protocol::OffsetFetchRequest request;
    // Repeated groups share one partition mutex; order and entries are retained.
    for (int p : {0, 0, 1})
        request.addGroup({key(p), std::nullopt, -1, true, {}});
    Buffer payload;
    request.encode(payload, 9);
    std::promise<RequestDispatcher::DispatchResult> delivered;
    EXPECT_TRUE(CoordinatorExecutorProbe::dispatchProtocol(
                    b, protocol::ApiKey::OFFSET_FETCH, 9, payload,
                    [&](auto result) { delivered.set_value(std::move(result)); })
                    .deferred);
    auto future = delivered.get_future();
    ASSERT_EQ(future.wait_for(5s), std::future_status::ready);
    auto result = future.get();
    std::promise<void> entered, release;
    auto ready = entered.get_future();
    auto released = release.get_future().share();
    auto publication = std::async(std::launch::async, [&] {
        result.publication_guard([&](auto error) {
            EXPECT_EQ(error, ErrorCode::NONE);
            entered.set_value();
            released.wait();  // Models synchronous socket write initiation.
        });
    });
    const bool publishing = ready.wait_for(5s) == std::future_status::ready;
    if (!publishing)
        release.set_value();
    ASSERT_TRUE(publishing);
    auto changed = std::async(std::launch::async, [&] {
        return b.metadataController()->updatePartitionLeader("__consumer_offsets", 1, 0);
    });
    EXPECT_EQ(changed.wait_for(50ms), std::future_status::timeout);
    release.set_value();
    publication.get();
    EXPECT_EQ(changed.get().error_code, ErrorCode::NONE);
    const auto fenced =
        publishProtocol<protocol::OffsetFetchResponse>(result, protocol::ApiKey::OFFSET_FETCH, 9);
    ASSERT_EQ(fenced.groups().size(), 3u);
    EXPECT_EQ(fenced.groups()[0].error_code, ErrorCode::NONE);
    EXPECT_EQ(fenced.groups()[1].error_code, ErrorCode::NONE);
    EXPECT_EQ(fenced.groups()[2].error_code, ErrorCode::NOT_COORDINATOR);
    result = {};
    b.stop();
}

TEST_F(CoordinatorPersistenceTest, StagedMembershipWireCommitsJoinSyncHeartbeatAndBatchLeave) {
    KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
    b.start();
    b.reconcileReplicas();
    const auto id = key(0);
    auto join = joinRequest(id);
    join.setGroupInstanceId("instance");
    join.setSessionTimeoutMs(120000);
    const auto joined =
        membershipWire<protocol::JoinGroupResponse>(b, protocol::ApiKey::JOIN_GROUP, join, 9);
    ASSERT_EQ(joined.errorCode(), ErrorCode::NONE);
    EXPECT_EQ(committedGroup(b, id).members.at(0).member_id, joined.memberId());
    EXPECT_EQ(committedGroup(b, id).members.at(0).session_timeout_ms, 120000);
    protocol::SyncGroupRequest sync;
    sync.setGroupId(id);
    sync.setMemberId(joined.memberId());
    sync.setGenerationId(joined.generationId());
    sync.setGroupInstanceId("instance");
    sync.setProtocolType("consumer");
    sync.setProtocolName("range");
    sync.setAssignments({{joined.memberId(), {128, 255}}});
    EXPECT_EQ(membershipWire<protocol::SyncGroupResponse>(b, protocol::ApiKey::SYNC_GROUP, sync, 5)
                  .errorCode(),
              ErrorCode::NONE);
    EXPECT_EQ(committedGroup(b, id).members.at(0).assignment, (std::vector<uint8_t>{128, 255}));
    auto heartbeat = heartbeatRequest(id, joined.generationId());
    heartbeat.setMemberId(joined.memberId());
    EXPECT_EQ(
        membershipWire<protocol::HeartbeatResponse>(b, protocol::ApiKey::HEARTBEAT, heartbeat, 4)
            .errorCode(),
        ErrorCode::NONE);
    const auto before = b.logManager()->getLog("__consumer_offsets", 0)->logEndOffset();
    protocol::LeaveGroupRequest leave;
    leave.setGroupId(id);
    leave.addMember({joined.memberId(), "instance", {}});
    leave.addMember({"unknown", {}, {}});
    const auto left =
        membershipWire<protocol::LeaveGroupResponse>(b, protocol::ApiKey::LEAVE_GROUP, leave, 5);
    ASSERT_EQ(left.members().size(), 2u);
    EXPECT_EQ(left.members()[0].error_code, ErrorCode::NONE);
    EXPECT_EQ(left.members()[1].error_code, ErrorCode::UNKNOWN_MEMBER_ID);
    EXPECT_TRUE(committedGroup(b, id).members.empty());
    EXPECT_EQ(b.logManager()->getLog("__consumer_offsets", 0)->logEndOffset(), before + 1);
    b.stop();
}

TEST_F(CoordinatorPersistenceTest, MembershipCheckpointFailureKeepsStorageErrorAndLiveState) {
    KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
    b.start();
    b.reconcileReplicas();
    const auto id = key(0);
    ASSERT_EQ(put(b, id, {groupRecord(id)}), ErrorCode::NONE);
    const auto before =
        CoordinatorAcquisitionProbe::runtime(CoordinatorPersistenceProbe::groups(b), id);
    const auto blocked = dir / "0/__consumer_offsets-0/checkpoint.meta.tmp";
    fs::create_directory(blocked);
    EXPECT_EQ(membershipWire<protocol::HeartbeatResponse>(b, protocol::ApiKey::HEARTBEAT,
                                                          heartbeatRequest(id), 4)
                  .errorCode(),
              ErrorCode::KAFKA_STORAGE_ERROR);
    EXPECT_EQ(CoordinatorAcquisitionProbe::runtime(CoordinatorPersistenceProbe::groups(b), id),
              before);
    EXPECT_EQ(b.coordinatorLoadStatus(id, protocol::CoordinatorType::GROUP),
              ErrorCode::COORDINATOR_LOAD_IN_PROGRESS);
    fs::remove(blocked);
    b.acquireCoordinatorPartitions();
    EXPECT_EQ(membershipWire<protocol::HeartbeatResponse>(b, protocol::ApiKey::HEARTBEAT,
                                                          heartbeatRequest(id), 0)
                  .errorCode(),
              ErrorCode::NONE);
    b.stop();
}

TEST_F(CoordinatorPersistenceTest,
       RecoveredNegotiatedDeadlinesExpireMembersIndependentlyInBackground) {
    const auto id = key(0);
    {
        KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
        b.start();
        b.reconcileReplicas();
        auto r = groupRecord(id);
        r.group.members[0].session_timeout_ms = 120000;
        auto short_member = r.group.members[0];
        short_member.member_id = "short";
        short_member.group_instance_id = "short-instance";
        short_member.session_timeout_ms = 6000;
        r.group.members.push_back(short_member);
        ASSERT_EQ(put(b, id, {r, offsetRecord(id)}), ErrorCode::NONE);
        b.stop();
    }
    KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
    b.start();
    b.reconcileReplicas();
    ASSERT_EQ(committedGroup(b, id).members.size(), 2u);
    auto& groups = CoordinatorPersistenceProbe::groups(b);
    CoordinatorAcquisitionProbe::ageMember(groups, id, "member", 40s);
    CoordinatorAcquisitionProbe::ageMember(groups, id, "short", 10s);
    // Real scheduler, no direct draft timeout call; cache publication follows commitment.
    ASSERT_TRUE(waitUntil([&] { return groups.describeGroups({id})[0].members.size() == 1; }));
    const auto committed = committedGroup(b, id);
    ASSERT_EQ(committed.members.size(), 1u);
    EXPECT_EQ(committed.members[0].member_id, "member");
    EXPECT_EQ(committed.members[0].session_timeout_ms, 120000);
    EXPECT_EQ(groups.getMetrics().member_timeout_total, 1);
    EXPECT_EQ(CoordinatorPersistenceProbe::offsets(b).fetchOffset(id, "data", 0), 42);
    b.stop();
}

TEST_F(CoordinatorClusterPersistenceTest,
       ActualMembershipIsrWaitLeavesIoResponsiveAndPublishesOnlyAfterCommit) {
    startCluster();
    const auto id = key(1);
    auto& b = *brokers[1];
    ASSERT_EQ(put(b, id, {groupRecord(id)}), ErrorCode::NONE);
    // Keep the success publication fence independent of acquisition sweeps;
    // follower Fetch RPCs still advance the owner's HW on its single IO thread.
    b.replicaManager()->stop();
    const auto before =
        CoordinatorAcquisitionProbe::runtime(CoordinatorPersistenceProbe::groups(b), id);
    auto* log = b.logManager()->getLog("__consumer_offsets", 1);
    const auto initial_end = log->logEndOffset();
    brokers[0]->replicaManager()->stop();
    brokers[2]->replicaManager()->stop();
    auto pending = std::async(std::launch::async, [&] {
        return membershipWire<protocol::HeartbeatResponse>(b, protocol::ApiKey::HEARTBEAT,
                                                           heartbeatRequest(id), 4);
    });
    ASSERT_TRUE(waitUntil([&] { return log->logEndOffset() > initial_end; }));
    EXPECT_EQ(pending.wait_for(50ms), std::future_status::timeout);
    EXPECT_EQ(CoordinatorAcquisitionProbe::runtime(CoordinatorPersistenceProbe::groups(b), id),
              before);
    Buffer payload;
    protocol::RequestHeader(protocol::ApiKey::API_VERSIONS, 1, 92, "io-progress").encode(payload);
    protocol::ApiVersionsRequest{}.encode(payload, 1);
    auto progress = test_support::brokerRequest(b.port(), payload);
    EXPECT_EQ(progress.readInt32(), 92);
    EXPECT_EQ(progress.readInt16(), 0);
    brokers[0]->replicaManager()->start();
    brokers[2]->replicaManager()->start();
    ASSERT_EQ(pending.wait_for(10s), std::future_status::ready);
    EXPECT_EQ(pending.get().errorCode(), ErrorCode::NONE);
    EXPECT_GT(
        CoordinatorAcquisitionProbe::runtime(CoordinatorPersistenceProbe::groups(b), id).heartbeat,
        before.heartbeat);
}

TEST_F(CoordinatorPersistenceTest, QueuedTimeoutCannotMutateReacquiredOwnershipEpoch) {
    KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
    b.start();
    b.reconcileReplicas();
    const auto id = key(0);
    auto record = groupRecord(id);
    record.group.members[0].session_timeout_ms = 6000;
    ASSERT_EQ(put(b, id, {record}), ErrorCode::NONE);
    std::promise<void> release;
    auto released = release.get_future().share();
    struct ReleaseOnExit {
        std::promise<void>& promise;
        bool done = false;
        void finish() {
            if (!done) {
                promise.set_value();
                done = true;
            }
        }
        ~ReleaseOnExit() { finish(); }
    } release_on_exit{release};
    std::atomic<int> entered{0};
    for (int i = 0; i < 2; ++i)
        ASSERT_TRUE(CoordinatorExecutorProbe::submit(b, [&](bool, auto) {
            entered.fetch_add(1);
            released.wait();
        }));
    ASSERT_TRUE(waitUntil([&] { return entered.load() == 2; }));
    CoordinatorAcquisitionProbe::ageMember(CoordinatorPersistenceProbe::groups(b), id, "member",
                                           10s);
    CoordinatorExecutorProbe::scheduleTimeouts(b);
    ASSERT_TRUE(CoordinatorExecutorProbe::timeoutPending(b, id));
    ASSERT_EQ(b.metadataController()->updatePartitionLeader("__consumer_offsets", 0, 0).error_code,
              ErrorCode::NONE);
    b.reconcileReplicas();
    ASSERT_EQ(b.coordinatorLoadStatus(id, protocol::CoordinatorType::GROUP), ErrorCode::NONE);
    release_on_exit.finish();
    ASSERT_TRUE(waitUntil([&] { return !CoordinatorExecutorProbe::timeoutPending(b, id); }));
    EXPECT_EQ(b.logManager()->getLog("__consumer_offsets", 0)->logEndOffset(), 1);
    EXPECT_EQ(committedGroup(b, id).members.size(), 1u);
    EXPECT_EQ(CoordinatorPersistenceProbe::groups(b).getMetrics().member_timeout_total, 0);
    b.stop();
}

TEST_F(CoordinatorPersistenceTest, WireProtocolErrorCommitsGenerationAndHeartbeatEffects) {
    KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
    b.start();
    b.reconcileReplicas();
    const auto id = key(0);
    auto record = groupRecord(id);
    record.group.state = 2;
    auto other = record.group.members[0];
    other.member_id = "other";
    other.group_instance_id = "other-instance";
    record.group.members.push_back(other);
    ASSERT_EQ(put(b, id, {record}), ErrorCode::NONE);
    protocol::SyncGroupRequest sync;
    sync.setGroupId(id);
    sync.setGenerationId(7);
    sync.setMemberId("member");
    sync.setAssignments({{"member", {1}}});
    EXPECT_EQ(membershipWire<protocol::SyncGroupResponse>(b, protocol::ApiKey::SYNC_GROUP, sync, 0)
                  .errorCode(),
              ErrorCode::ILLEGAL_GENERATION);
    EXPECT_EQ(committedGroup(b, id).generation, 8);
    auto& groups = CoordinatorPersistenceProbe::groups(b);
    CoordinatorAcquisitionProbe::ageMember(groups, id, "member", 1s);
    const auto before = CoordinatorAcquisitionProbe::runtime(groups, id);
    EXPECT_EQ(membershipWire<protocol::HeartbeatResponse>(b, protocol::ApiKey::HEARTBEAT,
                                                          heartbeatRequest(id, 8), 0)
                  .errorCode(),
              ErrorCode::REBALANCE_IN_PROGRESS);
    EXPECT_GT(CoordinatorAcquisitionProbe::runtime(groups, id).heartbeat, before.heartbeat);
    EXPECT_EQ(committedGroup(b, id).generation, 8);
    b.stop();
}

TEST_F(CoordinatorPersistenceTest, GroupStateProposalsCommitMembershipAndTouchedRuntimeTogether) {
    KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
    b.start();
    b.reconcileReplicas();
    const auto id = key(0);
    ASSERT_EQ(put(b, id, {groupRecord(id), offsetRecord(id)}), ErrorCode::NONE);
    auto& groups = CoordinatorPersistenceProbe::groups(b);
    CoordinatorAcquisitionProbe::ageRuntime(groups, id);
    const auto before = CoordinatorAcquisitionProbe::runtime(groups, id);
    GroupCoordinator::JoinGroupResult joined;
    std::optional<GroupRecord> proposed;
    auto join = joinRequest(id, "second");
    join.setGroupInstanceId("second-instance");
    join.setSessionTimeoutMs(120000);
    ASSERT_EQ(b.mutateCoordinatorGroupState(
                  id,
                  [&](auto& draft) {
                      joined = draft.handleJoinGroup(join, "second-client", "second-host");
                      EXPECT_EQ(groups.getMemberTimeoutMs(), 30000);
                      proposed = draft.record();
                      EXPECT_EQ(CoordinatorAcquisitionProbe::runtime(groups, id), before);
                      EXPECT_EQ(groups.describeGroups({id})[0].members.size(), 1);
                  }),
              ErrorCode::NONE);
    ASSERT_EQ(joined.error, ErrorCode::NONE);
    EXPECT_EQ(joined.generation_id, 8);
    EXPECT_EQ(groups.getMemberTimeoutMs(), 120000);
    ASSERT_TRUE(proposed);
    EXPECT_EQ(committedGroup(b, id), proposed->group);
    EXPECT_EQ(groups.describeGroups({id})[0].members.size(), 2);
    const auto joined_runtime = CoordinatorAcquisitionProbe::runtime(groups, id);
    EXPECT_EQ(joined_runtime.heartbeat, before.heartbeat);  // Only the second member joined.
    EXPECT_GT(joined_runtime.rebalance, before.rebalance);
    EXPECT_EQ(joined_runtime.rebalances, 12);  // Count once, including the draft's transition.
    EXPECT_EQ(CoordinatorPersistenceProbe::offsets(b).fetchOffset(id, "data", 0), 42);

    protocol::SyncGroupRequest sync;
    sync.setGroupId(id);
    sync.setMemberId("member");
    sync.setGenerationId(8);
    sync.setAssignments({{"member", {1, 2}}, {"second", {3, 4}}});
    GroupCoordinator::SyncGroupResult synced;
    ASSERT_EQ(b.mutateCoordinatorGroupState(
                  id,
                  [&](auto& draft) {
                      synced = draft.handleSyncGroup(sync);
                      proposed = draft.record();
                      EXPECT_EQ(CoordinatorAcquisitionProbe::runtime(groups, id), joined_runtime);
                  }),
              ErrorCode::NONE);
    EXPECT_EQ(synced.error, ErrorCode::NONE);
    EXPECT_EQ(synced.assignment, (std::vector<uint8_t>{1, 2}));
    EXPECT_EQ(committedGroup(b, id), proposed->group);
    auto synced_runtime = CoordinatorAcquisitionProbe::runtime(groups, id);
    EXPECT_GT(synced_runtime.heartbeat, joined_runtime.heartbeat);
    EXPECT_EQ(synced_runtime.rebalances, 12);

    CoordinatorAcquisitionProbe::ageRuntime(groups, id);
    auto aged = CoordinatorAcquisitionProbe::runtime(groups, id);
    ErrorCode heartbeat = ErrorCode::INVALID_REQUEST;
    ASSERT_EQ(b.mutateCoordinatorGroupState(
                  id,
                  [&](auto& draft) {
                      heartbeat = draft.handleHeartbeat(heartbeatRequest(id, 8));
                      proposed = draft.record();
                      EXPECT_EQ(CoordinatorAcquisitionProbe::runtime(groups, id), aged);
                  }),
              ErrorCode::NONE);
    EXPECT_EQ(heartbeat, ErrorCode::NONE);
    EXPECT_EQ(committedGroup(b, id), proposed->group);
    const auto refreshed = CoordinatorAcquisitionProbe::runtime(groups, id);
    EXPECT_GT(refreshed.heartbeat, aged.heartbeat);
    EXPECT_EQ(refreshed.rebalance, aged.rebalance);
    EXPECT_EQ(refreshed.rebalances, aged.rebalances);
    // An unrelated offset publication must retain the committed heartbeat.
    ASSERT_EQ(put(b, id, {offsetRecord(id, 99)}), ErrorCode::NONE);
    EXPECT_EQ(CoordinatorAcquisitionProbe::runtime(groups, id), refreshed);
    ErrorCode left = ErrorCode::INVALID_REQUEST;
    ASSERT_EQ(b.mutateCoordinatorGroupState(
                  id,
                  [&](auto& draft) {
                      left = draft.handleLeaveGroup("second");
                      proposed = draft.record();
                      EXPECT_EQ(groups.describeGroups({id})[0].members.size(), 2);
                  }),
              ErrorCode::NONE);
    EXPECT_EQ(left, ErrorCode::NONE);
    EXPECT_EQ(committedGroup(b, id), proposed->group);
    EXPECT_EQ(groups.describeGroups({id})[0].members.size(), 1);
    EXPECT_EQ(groups.describeGroups({id})[0].group_state, "PreparingRebalance");
    EXPECT_EQ(CoordinatorPersistenceProbe::offsets(b).fetchOffset(id, "data", 0), 99);
    b.stop();
}

TEST_F(CoordinatorPersistenceTest, FirstGroupProposalCountsInitialRebalanceAndKeepsStaticIdentity) {
    KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
    b.start();
    b.reconcileReplicas();
    const auto id = key(0);
    auto request = joinRequest(id);
    request.setGroupInstanceId("static");
    GroupCoordinator::JoinGroupResult result;
    ASSERT_EQ(b.mutateCoordinatorGroupState(
                  id,
                  [&](auto& draft) {
                      result = draft.handleJoinGroup(request, "client", "host");
                      EXPECT_TRUE(CoordinatorPersistenceProbe::groups(b).listGroups().empty());
                  }),
              ErrorCode::NONE);
    ASSERT_EQ(result.error, ErrorCode::NONE);
    auto record = committedGroup(b, id);
    ASSERT_EQ(record.members.size(), 1);
    EXPECT_EQ(record.members[0].group_instance_id, "static");
    EXPECT_EQ(record.members[0].member_id, result.member_id);
    EXPECT_EQ(CoordinatorPersistenceProbe::groups(b).getMetrics().groups[0].rebalances_total, 1);
    const auto first_id = result.member_id;
    ASSERT_EQ(b.mutateCoordinatorGroupState(
                  id, [&](auto& draft) { result = draft.handleJoinGroup(request); }),
              ErrorCode::NONE);
    EXPECT_EQ(result.member_id, first_id);
    EXPECT_EQ(committedGroup(b, id).members.size(), 1);
    EXPECT_EQ(CoordinatorPersistenceProbe::groups(b).getMetrics().groups[0].rebalances_total, 1);
    b.stop();
}

TEST_F(CoordinatorPersistenceTest, GroupProposalCheckpointFailureLeavesLiveRuntimeUntouched) {
    KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
    b.start();
    b.reconcileReplicas();
    const auto id = key(0);
    ASSERT_EQ(put(b, id, {groupRecord(id)}), ErrorCode::NONE);
    auto& groups = CoordinatorPersistenceProbe::groups(b);
    CoordinatorAcquisitionProbe::ageRuntime(groups, id);
    const auto before = CoordinatorAcquisitionProbe::runtime(groups, id);
    const auto blocked = dir / "0/__consumer_offsets-0/checkpoint.meta.tmp";
    fs::create_directory(blocked);
    bool invoked = false;
    EXPECT_EQ(b.mutateCoordinatorGroupState(
                  id,
                  [&](auto& draft) {
                      invoked = true;
                      EXPECT_EQ(draft.handleHeartbeat(heartbeatRequest(id)), ErrorCode::NONE);
                  }),
              ErrorCode::KAFKA_STORAGE_ERROR);
    EXPECT_TRUE(invoked);
    EXPECT_EQ(CoordinatorAcquisitionProbe::runtime(groups, id), before);
    EXPECT_EQ(groups.describeGroups({id})[0].members.size(), 1);
    fs::remove(blocked);
    b.stop();
}

TEST_F(CoordinatorPersistenceTest, RejectedGroupProposalCannotChangeOtherGroupsOrPublishDraft) {
    KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
    b.start();
    b.reconcileReplicas();
    const auto id = key(0);
    ASSERT_EQ(put(b, id, {groupRecord(id)}), ErrorCode::NONE);
    auto& groups = CoordinatorPersistenceProbe::groups(b);
    CoordinatorAcquisitionProbe::ageRuntime(groups, id);
    const auto before = CoordinatorAcquisitionProbe::runtime(groups, id);
    const auto leo = b.logManager()->getLog("__consumer_offsets", 0)->logEndOffset();
    EXPECT_EQ(b.mutateCoordinatorGroupState(
                  id,
                  [&](auto& draft) {
                      EXPECT_EQ(draft.handleHeartbeat(heartbeatRequest(id)), ErrorCode::NONE);
                      draft.handleJoinGroup(joinRequest(key(1)));
                  }),
              ErrorCode::KAFKA_STORAGE_ERROR);
    EXPECT_EQ(CoordinatorAcquisitionProbe::runtime(groups, id), before);
    EXPECT_EQ(b.logManager()->getLog("__consumer_offsets", 0)->logEndOffset(), leo);
    EXPECT_EQ(groups.listGroups().size(), 1);
    EXPECT_EQ(b.mutateCoordinatorGroupState(id,
                                            [&](auto& draft) {
                                                draft.handleLeaveGroup("member");
                                                throw std::runtime_error("rejected after drafting");
                                            }),
              ErrorCode::KAFKA_STORAGE_ERROR);
    EXPECT_EQ(CoordinatorAcquisitionProbe::runtime(groups, id), before);
    EXPECT_EQ(b.logManager()->getLog("__consumer_offsets", 0)->logEndOffset(), leo);
    EXPECT_EQ(b.mutateCoordinatorGroupState(id, {}), ErrorCode::INVALID_REQUEST);
    b.stop();
}

TEST_F(CoordinatorPersistenceTest, GroupProtocolErrorsCanCommitTheirStateTransitions) {
    KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
    b.start();
    b.reconcileReplicas();
    const auto id = key(0);
    auto record = groupRecord(id);
    record.group.state = 2;
    auto second = record.group.members[0];
    second.member_id = "second";
    second.group_instance_id = "second-instance";
    record.group.members.push_back(second);
    ASSERT_EQ(put(b, id, {record}), ErrorCode::NONE);
    auto& groups = CoordinatorPersistenceProbe::groups(b);
    CoordinatorAcquisitionProbe::ageRuntime(groups, id);
    const auto before = CoordinatorAcquisitionProbe::runtime(groups, id);
    protocol::SyncGroupRequest sync;
    sync.setGroupId(id);
    sync.setGenerationId(7);
    sync.setMemberId("member");
    sync.setAssignments({{"member", {1}}});  // Assignment computed before second joined.
    GroupCoordinator::SyncGroupResult result;
    ASSERT_EQ(b.mutateCoordinatorGroupState(
                  id,
                  [&](auto& draft) {
                      result = draft.handleSyncGroup(sync);
                      EXPECT_EQ(CoordinatorAcquisitionProbe::runtime(groups, id), before);
                  }),
              ErrorCode::NONE);
    EXPECT_EQ(result.error, ErrorCode::ILLEGAL_GENERATION);
    EXPECT_EQ(committedGroup(b, id).generation, 8);
    EXPECT_EQ(CoordinatorAcquisitionProbe::runtime(groups, id).rebalances, 12);
    CoordinatorAcquisitionProbe::ageRuntime(groups, id);
    const auto aged = CoordinatorAcquisitionProbe::runtime(groups, id);
    ErrorCode heartbeat = ErrorCode::NONE;
    ASSERT_EQ(b.mutateCoordinatorGroupState(
                  id,
                  [&](auto& draft) {
                      heartbeat = draft.handleHeartbeat(heartbeatRequest(id, 8));
                      EXPECT_EQ(CoordinatorAcquisitionProbe::runtime(groups, id), aged);
                  }),
              ErrorCode::NONE);
    EXPECT_EQ(heartbeat, ErrorCode::REBALANCE_IN_PROGRESS);
    EXPECT_GT(CoordinatorAcquisitionProbe::runtime(groups, id).heartbeat, aged.heartbeat);
    EXPECT_EQ(CoordinatorAcquisitionProbe::runtime(groups, id).rebalance, aged.rebalance);
    b.stop();
}

TEST_F(CoordinatorPersistenceTest, GroupTimeoutProposalPublishesEvictionMetricsOnlyOnCommit) {
    KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
    b.start();
    b.reconcileReplicas();
    const auto id = key(0);
    ASSERT_EQ(put(b, id, {groupRecord(id)}), ErrorCode::NONE);
    auto& groups = CoordinatorPersistenceProbe::groups(b);
    groups.setMemberTimeoutMs(1);
    {
        // Staged expiration uses the durable member deadline rather than the legacy maximum.
        auto r = groupRecord(id);
        r.group.members[0].session_timeout_ms = 6000;
        ASSERT_EQ(put(b, id, {r}), ErrorCode::NONE);
    }
    CoordinatorAcquisitionProbe::ageRuntime(groups, id);
    const auto before = CoordinatorAcquisitionProbe::runtime(groups, id);
    const auto metrics_before = groups.getMetrics().member_timeout_total;
    EXPECT_EQ(b.mutateCoordinatorGroupState(
                  id,
                  [&](auto& draft) {
                      draft.checkTimeouts();
                      EXPECT_TRUE(draft.record()->group.members.empty());
                      EXPECT_EQ(CoordinatorAcquisitionProbe::runtime(groups, id), before);
                      EXPECT_EQ(groups.getMetrics().member_timeout_total, metrics_before);
                      throw std::runtime_error("cancel eviction");
                  }),
              ErrorCode::KAFKA_STORAGE_ERROR);
    EXPECT_EQ(groups.getMetrics().member_timeout_total, metrics_before);
    EXPECT_EQ(CoordinatorAcquisitionProbe::runtime(groups, id), before);
    ASSERT_EQ(b.mutateCoordinatorGroupState(id, [&](auto& draft) { draft.checkTimeouts(); }),
              ErrorCode::NONE);
    EXPECT_TRUE(committedGroup(b, id).members.empty());
    EXPECT_EQ(groups.getMetrics().member_timeout_total, metrics_before + 1);
    // Replaying the committed image rebuilds deadlines without recounting timeouts.
    CoordinatorPersistenceProbe::reacquireGroupCache(b, 0);
    EXPECT_EQ(groups.getMetrics().member_timeout_total, metrics_before + 1);
    b.stop();
}

TEST_F(CoordinatorPersistenceTest, RebalanceTimeoutProposalCommitsLeaderlessPreparingState) {
    KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
    b.start();
    b.reconcileReplicas();
    const auto id = key(0);
    auto initial = groupRecord(id);
    initial.group.state = 2;
    initial.group.rebalance_timeout_ms = 1000;
    auto second = initial.group.members[0];
    second.member_id = "second";
    second.group_instance_id = "second-instance";
    initial.group.members.push_back(second);
    ASSERT_EQ(put(b, id, {initial}), ErrorCode::NONE);
    auto& groups = CoordinatorPersistenceProbe::groups(b);
    CoordinatorAcquisitionProbe::ageRuntime(groups, id);
    const auto before = CoordinatorAcquisitionProbe::runtime(groups, id);
    ASSERT_EQ(b.mutateCoordinatorGroupState(
                  id,
                  [&](auto& draft) {
                      draft.checkTimeouts();
                      EXPECT_EQ(CoordinatorAcquisitionProbe::runtime(groups, id), before);
                  }),
              ErrorCode::NONE);
    const auto committed = committedGroup(b, id);
    EXPECT_EQ(committed.state, 1);  // PreparingRebalance, awaiting a new leader's rejoin.
    EXPECT_TRUE(committed.leader_id.empty());
    ASSERT_EQ(committed.members.size(), 1);
    EXPECT_EQ(committed.members[0].member_id, "second");
    EXPECT_EQ(committed.generation, 8);
    EXPECT_EQ(groups.getMetrics().groups[0].rebalances_total, 12);
    EXPECT_EQ(groups.getMetrics().member_timeout_total, 0);
    // Acquisition must accept the durable transitional image as well.
    CoordinatorPersistenceProbe::reacquireGroupCache(b, 0);
    EXPECT_EQ(groups.describeGroups({id})[0].group_state, "PreparingRebalance");
    GroupCoordinator::JoinGroupResult joined;
    ASSERT_EQ(
        b.mutateCoordinatorGroupState(
            id, [&](auto& draft) { joined = draft.handleJoinGroup(joinRequest(id, "second")); }),
        ErrorCode::NONE);
    EXPECT_EQ(joined.error, ErrorCode::NONE);
    EXPECT_EQ(joined.leader_id, "second");
    EXPECT_EQ(joined.generation_id, 8);
    EXPECT_EQ(committedGroup(b, id).state, 2);
    b.stop();
}

TEST_F(CoordinatorPersistenceTest, UnknownGroupProposalPreservesExactOffsetAndPendingKeys) {
    KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
    b.start();
    b.reconcileReplicas();
    const auto id = key(0);
    auto pending = offsetRecord(id, 43);
    pending.key.kind = GroupRecordKey::Kind::PendingOffset;
    pending.key.transactional_id = "txn";
    pending.key.producer_id = 75;
    pending.key.producer_epoch = 2;
    ASSERT_EQ(put(b, id, {offsetRecord(id), pending}), ErrorCode::NONE);
    ErrorCode result = ErrorCode::NONE;
    ASSERT_EQ(b.mutateCoordinatorGroupState(id,
                                            [&](auto& draft) {
                                                EXPECT_FALSE(draft.record());
                                                result =
                                                    draft.handleHeartbeat(heartbeatRequest(id));
                                                EXPECT_FALSE(draft.record());
                                            }),
              ErrorCode::NONE);
    EXPECT_EQ(result, ErrorCode::ILLEGAL_GENERATION);
    EXPECT_TRUE(CoordinatorPersistenceProbe::groups(b).getMetrics().groups.empty());
    EXPECT_EQ(CoordinatorPersistenceProbe::offsets(b).fetchOffset(id, "data", 0), 42);
    EXPECT_EQ(CoordinatorPersistenceProbe::groups(b).pendingCoordinatorOffsets().size(), 1);
    const auto image = GroupStateManager(b.logManager(), 3).loadCommittedPartition(0);
    EXPECT_EQ(image.size(), 2);
    b.stop();
}

TEST_F(CoordinatorClusterPersistenceTest, ThreeBrokerGroupDraftWaitsForIsrAndFencesOwnerChange) {
    startCluster();
    const auto id = key(1);
    auto& b = *brokers[1];
    ASSERT_TRUE(waitUntil([&] {
        return b.coordinatorLoadStatus(id, protocol::CoordinatorType::GROUP) == ErrorCode::NONE;
    }));
    ASSERT_EQ(put(b, id, {groupRecord(id)}), ErrorCode::NONE);
    auto& groups = CoordinatorPersistenceProbe::groups(b);
    CoordinatorAcquisitionProbe::ageRuntime(groups, id);
    const auto before = CoordinatorAcquisitionProbe::runtime(groups, id);
    // Retain metadata quorum but freeze acquisition and data replication.
    for (auto& broker : brokers)
        broker->replicaManager()->stop();
    ErrorCode heartbeat = ErrorCode::INVALID_REQUEST;
    auto pending = std::async(std::launch::async, [&] {
        return b.mutateCoordinatorGroupState(
            id, [&](auto& draft) { heartbeat = draft.handleHeartbeat(heartbeatRequest(id)); });
    });
    ASSERT_TRUE(waitUntil(
        [&] { return b.logManager()->getLog("__consumer_offsets", 1)->logEndOffset() == 2; }));
    EXPECT_EQ(pending.wait_for(20ms), std::future_status::timeout);
    EXPECT_EQ(CoordinatorAcquisitionProbe::runtime(groups, id), before);
    brokers[0]->replicaManager()->start();
    brokers[2]->replicaManager()->start();
    ASSERT_EQ(pending.get(), ErrorCode::NONE);
    EXPECT_EQ(heartbeat, ErrorCode::NONE);
    EXPECT_GT(CoordinatorAcquisitionProbe::runtime(groups, id).heartbeat, before.heartbeat);
    EXPECT_EQ(CoordinatorAcquisitionProbe::runtime(groups, id).rebalance, before.rebalance);

    brokers[0]->replicaManager()->stop();
    brokers[2]->replicaManager()->stop();
    const auto committed = CoordinatorAcquisitionProbe::runtime(groups, id);
    pending = std::async(std::launch::async, [&] {
        return b.mutateCoordinatorGroupState(id, [&](auto& draft) {
            EXPECT_EQ(draft.handleJoinGroup(joinRequest(id, "second")).error, ErrorCode::NONE);
        });
    });
    ASSERT_TRUE(waitUntil(
        [&] { return b.logManager()->getLog("__consumer_offsets", 1)->logEndOffset() == 3; }));
    EXPECT_EQ(CoordinatorAcquisitionProbe::runtime(groups, id), committed);
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
    EXPECT_EQ(CoordinatorAcquisitionProbe::runtime(groups, id), committed);
    EXPECT_EQ(groups.describeGroups({id})[0].members.size(), 1);
    bool invoked = false;
    EXPECT_EQ(b.mutateCoordinatorGroupState(id, [&](auto&) { invoked = true; }),
              ErrorCode::NOT_COORDINATOR);
    EXPECT_FALSE(invoked);
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
    // Migrated staged reads observe only the committed owned cache.
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
    EXPECT_EQ(response.groups()[0].error_code, ErrorCode::NONE);
    ASSERT_EQ(response.groups()[0].topics.size(), 1u);
    EXPECT_EQ(response.groups()[0].topics[0].partitions[0].offset, 42);
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

TEST_F(CoordinatorClusterPersistenceTest, ThreeBrokerCommitSurvivesOwnerLossWithCompleteState) {
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

TEST_F(CoordinatorClusterPersistenceTest,
       IsrTimeoutDoesNotPublishOrAdmitNewMutationsUntilTailResolves) {
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

TEST_F(CoordinatorClusterPersistenceTest, InsufficientIsrBeforeAndAfterAppendCannotAcknowledge) {
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

TEST_F(CoordinatorClusterPersistenceTest, OwnershipLossDuringIsrWaitPreventsCachePublication) {
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

TEST_F(CoordinatorClusterPersistenceTest, ShutdownCancelsIsrWaitBeforeClosingStorage) {
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

RequestDispatcher::HandlerResult heartbeatResult(const RequestDispatcher::RequestContext& context,
                                                 ErrorCode status) {
    RequestDispatcher::HandlerResult result;
    result.payload = CoordinatorExecutorProbe::error(context, status, 0);
    return result;
}
ErrorCode publishStatus(RequestDispatcher::DispatchResult& result) {
    ErrorCode status = ErrorCode::NONE;
    if (result.publication_guard)
        result.publication_guard([&](auto code) { status = code; });
    if (status == ErrorCode::NONE) {
        Buffer frame(result.frame);
        (void)frame.readInt32();
        (void)frame.readInt32();
        status = static_cast<ErrorCode>(frame.readInt16());
    }
    return status;
}
Buffer heartbeatWire(const std::string& id) {
    Buffer payload;
    protocol::RequestHeader(protocol::ApiKey::HEARTBEAT, 0, 71, "executor-test").encode(payload);
    heartbeatRequest(id).encode(payload, 0);
    return payload;
}

TEST_F(CoordinatorClusterPersistenceTest,
       DeferredWireIsrWaitLeavesSingleIoThreadAndReplicaFetchLive) {
    startCluster();
    const auto id = key(1);
    auto& b = *brokers[1];
    ASSERT_TRUE(waitUntil([&] {
        return b.coordinatorLoadStatus(id, protocol::CoordinatorType::GROUP) == ErrorCode::NONE;
    }));
    ASSERT_EQ(put(b, id, {offsetRecord(id, 10)}), ErrorCode::NONE);
    // Freeze owner maintenance after setup so its periodic acquisition sweep
    // cannot race the response's nonblocking publication fence. Follower Fetch
    // RPCs still advance the owner's HW on its single socket IO thread.
    b.replicaManager()->stop();
    brokers[0]->replicaManager()->stop();
    brokers[2]->replicaManager()->stop();
    CoordinatorExecutorProbe::wire(b, id, [&](auto& context) {
        return heartbeatResult(context, put(b, id, {offsetRecord(id, 11)}, 30s));
    });
    auto pending = std::async(std::launch::async, [&] {
        auto request = heartbeatWire(id);
        return test_support::brokerRequest(b.port(), request);
    });
    ASSERT_TRUE(waitUntil(
        [&] { return b.logManager()->getLog("__consumer_offsets", 1)->logEndOffset() == 2; }));
    EXPECT_EQ(pending.wait_for(50ms), std::future_status::timeout);
    auto progress = std::async(std::launch::async, [&] {
        Buffer payload;
        protocol::RequestHeader(protocol::ApiKey::API_VERSIONS, 1, 72, "io-progress")
            .encode(payload);
        protocol::ApiVersionsRequest{}.encode(payload, 1);
        return test_support::brokerRequest(b.port(), payload);
    });
    EXPECT_EQ(progress.wait_for(2s), std::future_status::ready);
    auto versions = progress.get();
    EXPECT_EQ(versions.readInt32(), 72);
    EXPECT_EQ(versions.readInt16(), 0);
    brokers[0]->replicaManager()->start();
    brokers[2]->replicaManager()->start();
    ASSERT_EQ(pending.wait_for(10s), std::future_status::ready);
    auto response = pending.get();
    EXPECT_EQ(response.readInt32(), 71);
    EXPECT_EQ(static_cast<ErrorCode>(response.readInt16()), ErrorCode::NONE);
    EXPECT_EQ(CoordinatorPersistenceProbe::offsets(b).fetchOffset(id, "data", 0), 11);
}

TEST_F(CoordinatorPersistenceTest, DeferredShutdownRejectsQueueAndDrainsWorkerAndResponseTickets) {
    KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
    b.start();
    b.reconcileReplicas();
    const auto id = key(0);
    ASSERT_TRUE(waitUntil([&] {
        return b.coordinatorLoadStatus(id, protocol::CoordinatorType::GROUP) == ErrorCode::NONE;
    }));
    std::promise<void> release;
    auto released = release.get_future().share();
    std::atomic<int> entered{0}, rejected{0};
    auto work = [&](auto& context) {
        ++entered;
        released.wait();
        return heartbeatResult(context, ErrorCode::NONE);
    };
    auto sink = [&](auto result) {
        if (publishStatus(result) == ErrorCode::NOT_COORDINATOR)
            ++rejected;
    };
    auto waitForWorkers = [&](int count) {
        const bool ready = waitUntil([&] { return entered.load() == count; });
        if (!ready) {
            release.set_value();
            b.stop();  // Drain callbacks while their captured fixture state is alive.
        }
        return ready;
    };
    // Admission and worker fencing use try-locks on the same partition. Wait
    // for each worker to leave the fence before admitting the next request.
    EXPECT_TRUE(CoordinatorExecutorProbe::dispatch(b, id, work, sink).deferred);
    ASSERT_TRUE(waitForWorkers(1));
    EXPECT_TRUE(CoordinatorExecutorProbe::dispatch(b, id, work, sink).deferred);
    ASSERT_TRUE(waitForWorkers(2));
    EXPECT_TRUE(CoordinatorExecutorProbe::dispatch(b, id, work, sink).deferred);
    auto stopped = std::async(std::launch::async, [&] { b.stop(); });
    EXPECT_TRUE(waitUntil([&] { return CoordinatorExecutorProbe::stopping(b); }));
    EXPECT_EQ(stopped.wait_for(50ms), std::future_status::timeout);
    release.set_value();
    EXPECT_EQ(stopped.wait_for(5s), std::future_status::ready);
    stopped.get();
    EXPECT_EQ(entered.load(), 2);
    EXPECT_EQ(rejected.load(), 3);
}

TEST_F(CoordinatorPersistenceTest, DeferredShutdownWaitsForRetainedPublicationAndRefusesSuccess) {
    KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
    b.start();
    b.reconcileReplicas();
    const auto id = key(0);
    ASSERT_TRUE(waitUntil([&] {
        return b.coordinatorLoadStatus(id, protocol::CoordinatorType::GROUP) == ErrorCode::NONE;
    }));
    std::promise<RequestDispatcher::DispatchResult> delivered;
    EXPECT_TRUE(
        CoordinatorExecutorProbe::dispatch(
            b, id,
            [&](auto& context) { return heartbeatResult(context, put(b, id, {offsetRecord(id)})); },
            [&](auto result) { delivered.set_value(std::move(result)); })
            .deferred);
    auto ready = delivered.get_future();
    ASSERT_EQ(ready.wait_for(2s), std::future_status::ready);
    auto result = ready.get();
    EXPECT_EQ(CoordinatorPersistenceProbe::offsets(b).fetchOffset(id, "data", 0), 42);
    auto stopped = std::async(std::launch::async, [&] { b.stop(); });
    ASSERT_TRUE(waitUntil([&] { return CoordinatorExecutorProbe::stopping(b); }));
    EXPECT_EQ(stopped.wait_for(50ms), std::future_status::timeout);
    EXPECT_NE(b.logManager()->getLog("__consumer_offsets", 0), nullptr);
    EXPECT_EQ(publishStatus(result), ErrorCode::NOT_COORDINATOR);
    result = {};
    EXPECT_EQ(stopped.wait_for(5s), std::future_status::ready);
    stopped.get();
}

TEST_F(CoordinatorClusterPersistenceTest,
       DeferredWireOwnerLossAfterMutationBeforeDeliveryRefusesSuccess) {
    startCluster();
    const auto id = key(1);
    auto& b = *brokers[1];
    ASSERT_TRUE(waitUntil([&] {
        return b.coordinatorLoadStatus(id, protocol::CoordinatorType::GROUP) == ErrorCode::NONE;
    }));
    std::promise<ErrorCode> committed;
    std::promise<void> release;
    auto released = release.get_future().share();
    CoordinatorExecutorProbe::wire(b, id, [&](auto& context) {
        const auto status = put(b, id, {offsetRecord(id)});
        committed.set_value(status);
        released.wait();
        return heartbeatResult(context, status);
    });
    auto pending = std::async(std::launch::async, [&] {
        auto request = heartbeatWire(id);
        return test_support::brokerRequest(b.port(), request);
    });
    // Destroy this guard before the async request's future. A fatal assertion
    // must release the worker before that future waits for the wire response.
    struct WorkerRelease {
        std::promise<void>& promise;
        bool released = false;
        void release() {
            if (!released) {
                promise.set_value();
                released = true;
            }
        }
        ~WorkerRelease() { release(); }
    } worker_release{release};
    auto durable = committed.get_future();
    ASSERT_EQ(durable.wait_for(5s), std::future_status::ready);
    EXPECT_EQ(durable.get(), ErrorCode::NONE);
    KawasanBroker* controller = nullptr;
    ASSERT_TRUE(waitUntil([&] {
        for (auto& broker : brokers)
            if (broker->raftNode()->isLeader())
                controller = broker.get();
        return controller != nullptr;
    }));
    ASSERT_EQ(controller->metadataController()
                  ->updatePartitionLeader("__consumer_offsets", 1, 0)
                  .error_code,
              ErrorCode::NONE);
    ASSERT_TRUE(waitUntil([&] {
        return b.metadataController()
                   ->describeTopics({"__consumer_offsets"})
                   .front()
                   .partitions[1]
                   .leader == 0;
    }));
    worker_release.release();
    ASSERT_EQ(pending.wait_for(5s), std::future_status::ready);
    auto response = pending.get();
    EXPECT_EQ(response.readInt32(), 71);
    EXPECT_EQ(static_cast<ErrorCode>(response.readInt16()), ErrorCode::NOT_COORDINATOR);
}

TEST_F(CoordinatorPersistenceTest, DeferredReadAdmissionAndWorkerExceptionsUseFencedResponses) {
    KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
    b.start();
    b.reconcileReplicas();
    const auto id = key(0);
    ASSERT_TRUE(waitUntil([&] {
        return b.coordinatorLoadStatus(id, protocol::CoordinatorType::GROUP) == ErrorCode::NONE;
    }));
    ASSERT_EQ(put(b, id, {offsetRecord(id)}), ErrorCode::NONE);
    std::promise<ErrorCode> delivered;
    ASSERT_TRUE(CoordinatorExecutorProbe::dispatch(
                    b, id,
                    [&](auto& context) {
                        EXPECT_EQ(
                            CoordinatorPersistenceProbe::offsets(b).fetchOffset(id, "data", 0), 42);
                        return heartbeatResult(context, ErrorCode::NONE);
                    },
                    [&](auto result) { delivered.set_value(publishStatus(result)); }, true)
                    .deferred);
    EXPECT_EQ(delivered.get_future().get(), ErrorCode::NONE);
    std::promise<ErrorCode> failed;
    ASSERT_TRUE(CoordinatorExecutorProbe::dispatch(
                    b, id,
                    [](auto&) -> RequestDispatcher::HandlerResult {
                        throw std::runtime_error("worker fixture exception");
                    },
                    [&](auto result) { failed.set_value(publishStatus(result)); })
                    .deferred);
    EXPECT_EQ(failed.get_future().get(), ErrorCode::KAFKA_STORAGE_ERROR);
}

TEST_F(CoordinatorPersistenceTest, DeferredMutationCannotUseReacquiredEpochFromOlderAdmission) {
    KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
    b.start();
    b.reconcileReplicas();
    const auto id = key(0);
    ASSERT_EQ(put(b, id, {offsetRecord(id, 10)}), ErrorCode::NONE);
    std::promise<void> entered, release;
    auto released = release.get_future().share();
    std::promise<ErrorCode> mutated, delivered;
    ASSERT_TRUE(CoordinatorExecutorProbe::dispatch(
                    b, id,
                    [&](auto& context) {
                        entered.set_value();
                        released.wait();
                        const auto status = put(b, id, {offsetRecord(id, 11)});
                        mutated.set_value(status);
                        return heartbeatResult(context, status);
                    },
                    [&](auto result) { delivered.set_value(publishStatus(result)); })
                    .deferred);
    ASSERT_EQ(entered.get_future().wait_for(2s), std::future_status::ready);
    EXPECT_EQ(b.metadataController()->updatePartitionLeader("__consumer_offsets", 0, 0).error_code,
              ErrorCode::NONE);
    b.reconcileReplicas();
    EXPECT_EQ(b.coordinatorLoadStatus(id, protocol::CoordinatorType::GROUP), ErrorCode::NONE);
    release.set_value();
    EXPECT_EQ(mutated.get_future().get(), ErrorCode::NOT_COORDINATOR);
    EXPECT_EQ(delivered.get_future().get(), ErrorCode::NOT_COORDINATOR);
    EXPECT_EQ(CoordinatorPersistenceProbe::offsets(b).fetchOffset(id, "data", 0), 10);
    EXPECT_EQ(b.logManager()->getLog("__consumer_offsets", 0)->logEndOffset(), 1);
}

TEST_F(CoordinatorClusterPersistenceTest,
       DeferredShutdownCancelsActiveIsrWorkerBeforeStorageCloses) {
    startCluster();
    const auto id = key(1);
    auto& b = *brokers[1];
    ASSERT_TRUE(waitUntil([&] {
        return b.coordinatorLoadStatus(id, protocol::CoordinatorType::GROUP) == ErrorCode::NONE;
    }));
    ASSERT_EQ(put(b, id, {offsetRecord(id, 10)}), ErrorCode::NONE);
    const auto retained = CoordinatorPersistenceProbe::retainOffsets(b);
    brokers[0]->replicaManager()->stop();
    brokers[2]->replicaManager()->stop();
    std::promise<ErrorCode> delivered;
    ASSERT_TRUE(CoordinatorExecutorProbe::dispatch(
                    b, id,
                    [&](auto& context) {
                        return heartbeatResult(context, put(b, id, {offsetRecord(id, 11)}, 30s));
                    },
                    [&](auto result) { delivered.set_value(publishStatus(result)); })
                    .deferred);
    ASSERT_TRUE(waitUntil(
        [&] { return b.logManager()->getLog("__consumer_offsets", 1)->logEndOffset() == 2; }));
    auto stopped = std::async(std::launch::async, [&] { b.stop(); });
    auto response = delivered.get_future();
    EXPECT_EQ(response.wait_for(2s), std::future_status::ready);
    EXPECT_EQ(response.get(), ErrorCode::NOT_COORDINATOR);
    EXPECT_EQ(stopped.wait_for(5s), std::future_status::ready);
    stopped.get();
    EXPECT_EQ(retained->fetchOffset(id, "data", 0), 10);
}

TEST_F(CoordinatorPersistenceTest, DeferredProducerIdentityAndTransactionReadUseOwnedSource) {
    KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
    b.start();
    b.reconcileReplicas();
    const auto id = key(0);
    std::promise<ErrorCode> delivered;
    std::optional<TransactionCoordinator::TxnSnapshot> created;
    ASSERT_TRUE(CoordinatorExecutorProbe::dispatch(
                    b, id,
                    [&](auto& context) {
                        auto identity = transaction(id);
                        identity.producer_id = CoordinatorExecutorProbe::allocate(b);
                        const auto status = init(b, identity);
                        if (status == ErrorCode::NONE)
                            created = identity;
                        return heartbeatResult(context, status);
                    },
                    [&](auto result) { delivered.set_value(publishStatus(result)); }, false,
                    protocol::CoordinatorType::TRANSACTION)
                    .deferred);
    EXPECT_EQ(delivered.get_future().get(), ErrorCode::NONE);
    ASSERT_TRUE(created);
    EXPECT_EQ(CoordinatorPersistenceProbe::transactions(b).describe(id), created);
    std::promise<ErrorCode> read;
    ASSERT_TRUE(CoordinatorExecutorProbe::dispatch(
                    b, id,
                    [&](auto& context) {
                        EXPECT_EQ(CoordinatorPersistenceProbe::transactions(b).describe(id),
                                  created);
                        return heartbeatResult(context, ErrorCode::NONE);
                    },
                    [&](auto result) { read.set_value(publishStatus(result)); }, true,
                    protocol::CoordinatorType::TRANSACTION)
                    .deferred);
    EXPECT_EQ(read.get_future().get(), ErrorCode::NONE);
}

TEST_F(CoordinatorPersistenceTest, OwnershipRefusalTakesPriorityOverBusyAcquisition) {
    KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
    b.start();
    b.reconcileReplicas();
    const TopicPartition tp{"__consumer_offsets", 0};
    const auto epoch =
        b.metadataController()->describeTopics({tp.topic}).front().partitions[0].leader_epoch;
    ASSERT_EQ(b.metadataController()->updatePartitionLeader(tp.topic, tp.partition, -1).error_code,
              ErrorCode::NONE);
    auto lock = CoordinatorExecutorProbe::holdAcquisition(b);
    auto response = std::async(
        std::launch::async, [&] { return CoordinatorExecutorProbe::fencedStatus(b, tp, epoch); });
    EXPECT_EQ(response.wait_for(2s), std::future_status::ready);
    EXPECT_EQ(response.get(), ErrorCode::NOT_COORDINATOR);
}
}  // namespace

Buffer producersPayload(const std::vector<protocol::DescribeProducersRequest::TopicSpec>& topics) {
    Buffer payload;
    payload.writeCompactArrayLen(static_cast<int32_t>(topics.size()));
    for (const auto& topic : topics) {
        payload.writeCompactString(topic.topic);
        payload.writeCompactArrayLen(static_cast<int32_t>(topic.partitions.size()));
        for (const auto partition : topic.partitions)
            payload.writeInt32(partition);
        payload.writeEmptyTaggedFields();
    }
    payload.writeEmptyTaggedFields();
    return payload;
}
protocol::DescribeProducersResponse producersWire(
    KawasanBroker& b, const std::vector<protocol::DescribeProducersRequest::TopicSpec>& topics) {
    Buffer payload;
    protocol::RequestHeader(protocol::ApiKey::DESCRIBE_PRODUCERS, 0, 91, "replica-test")
        .encode(payload);
    auto input = producersPayload(topics);
    payload.writeBytes(input.data(), input.size());
    auto body = test_support::brokerRequest(b.port(), payload);
    protocol::ResponseHeader header;
    header.setFlexible(true);
    header.decode(body);
    protocol::DescribeProducersResponse response;
    response.decode(body, 0);
    EXPECT_EQ(body.remaining(), 0u);
    return response;
}
storage::RecordBatch inspectionData(int64_t pid = 42, bool transactional = true) {
    storage::RecordBatch batch;
    batch.setProducerId(pid);
    batch.setProducerEpoch(3);
    batch.setBaseSequence(17);
    batch.setAttributes(transactional ? 0x10 : 0);
    Record record("key", "value");
    record.timestamp = 1234;
    batch.addRecord(record);
    batch.setFirstTimestamp(1234);
    batch.setMaxTimestamp(1234);
    return batch;
}
RequestDispatcher::DispatchResult parkedProducers(KawasanBroker& b) {
    std::promise<RequestDispatcher::DispatchResult> delivered;
    EXPECT_TRUE(CoordinatorExecutorProbe::dispatchProtocol(
                    b, protocol::ApiKey::DESCRIBE_PRODUCERS, 0,
                    producersPayload({{"data", {0, 1, 0}}}),
                    [&](auto result) { delivered.set_value(std::move(result)); })
                    .deferred);
    auto ready = delivered.get_future();
    if (ready.wait_for(5s) != std::future_status::ready)
        throw std::runtime_error("inspection timeout");
    return ready.get();
}
TEST_F(CoordinatorPersistenceTest,
       ProducerInspectionWireUsesLogImageAndPreservesErrorsAndDuplicates) {
    KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
    b.start();
    createData(b);
    b.logManager()->getLog("data", 0)->appendBatch(inspectionData());
    const auto count = b.openLogCount();
    const auto response =
        producersWire(b, {{"data", {0, -1, 1, 0}}, {"missing", {0}}, {"data", {0}}});
    ASSERT_EQ(response.topics().size(), 3u);
    const auto& partitions = response.topics()[0].partitions;
    ASSERT_EQ(partitions.size(), 4u);
    EXPECT_EQ(partitions[1].partition, -1);
    EXPECT_EQ(partitions[1].error_code, ErrorCode::UNKNOWN_TOPIC_OR_PARTITION);
    EXPECT_EQ(partitions[2].error_code, ErrorCode::NONE);
    EXPECT_TRUE(partitions[2].active_producers.empty());
    EXPECT_EQ(response.topics()[1].partitions[0].error_code, ErrorCode::UNKNOWN_TOPIC_OR_PARTITION);
    for (const auto* p : {&partitions[0], &partitions[3], &response.topics()[2].partitions[0]}) {
        EXPECT_EQ(p->error_code, ErrorCode::NONE);
        ASSERT_EQ(p->active_producers.size(), 1u);
        const auto& producer = p->active_producers[0];
        EXPECT_EQ(producer.producer_id, 42);
        EXPECT_EQ(producer.producer_epoch, 3);
        EXPECT_EQ(producer.last_sequence, 17);
        EXPECT_EQ(producer.last_timestamp, 1234);
        EXPECT_EQ(producer.coordinator_epoch, -1);
        EXPECT_EQ(producer.current_txn_start_offset, 0);
    }
    EXPECT_EQ(b.openLogCount(), count);
    EXPECT_FALSE(b.logManager()->getLog("missing", 0));
    EXPECT_TRUE(producersWire(b, {}).topics().empty());
    b.stop();
}
TEST_F(CoordinatorPersistenceTest, ProducerInspectionResponsesFenceEachEpochAndDrainShutdown) {
    KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
    b.start();
    createData(b);
    b.logManager()->getLog("data", 0)->appendBatch(inspectionData());
    auto result = parkedProducers(b);
    ASSERT_TRUE(result.publication_guard);
    EXPECT_EQ(publishProtocol<protocol::DescribeProducersResponse>(
                  result, protocol::ApiKey::DESCRIBE_PRODUCERS, 0)
                  .topics()[0]
                  .partitions[0]
                  .error_code,
              ErrorCode::NONE);
    ASSERT_EQ(b.metadataController()->updatePartitionLeader("data", 0, 0).error_code,
              ErrorCode::NONE);
    b.reconcileReplicas();
    auto after = publishProtocol<protocol::DescribeProducersResponse>(
        result, protocol::ApiKey::DESCRIBE_PRODUCERS, 0);
    ASSERT_EQ(after.topics()[0].partitions.size(), 3u);
    for (const auto i : {0, 2}) {
        EXPECT_EQ(after.topics()[0].partitions[i].error_code, ErrorCode::NOT_LEADER_FOR_PARTITION);
        EXPECT_TRUE(after.topics()[0].partitions[i].active_producers.empty());
    }
    EXPECT_EQ(after.topics()[0].partitions[1].error_code, ErrorCode::NONE);
    auto stopped = std::async(std::launch::async, [&] { b.stop(); });
    EXPECT_TRUE(waitUntil([&] { return CoordinatorExecutorProbe::stopping(b); }));
    EXPECT_EQ(stopped.wait_for(50ms), std::future_status::timeout);
    result = {};
    ASSERT_EQ(stopped.wait_for(5s), std::future_status::ready);
    stopped.get();
}
TEST_F(CoordinatorPersistenceTest, ProducerInspectionDeletedTopicCannotExposeOldImage) {
    KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
    b.start();
    createData(b);
    b.logManager()->getLog("data", 0)->appendBatch(inspectionData());
    auto result = parkedProducers(b);
    ASSERT_EQ(b.metadataController()->deleteTopic("data").error_code, ErrorCode::NONE);
    createData(b);
    const auto after = publishProtocol<protocol::DescribeProducersResponse>(
        result, protocol::ApiKey::DESCRIBE_PRODUCERS, 0);
    for (const auto& p : after.topics()[0].partitions) {
        EXPECT_EQ(p.error_code, ErrorCode::NOT_LEADER_FOR_PARTITION);
        EXPECT_TRUE(p.active_producers.empty());
    }
    result = {};
    b.stop();
}
TEST_F(CoordinatorPersistenceTest, ProducerInspectionClosedAndRetainedPrefixRefusePartialImages) {
    auto c = config();
    c.setInt("log.segment.bytes", 1);
    KawasanBroker b(c, KawasanBroker::CoordinatorPersistenceOnly{format});
    b.start();
    createData(b);
    auto* log = b.logManager()->getLog("data", 0);
    log->appendBatch(inspectionData());
    log->appendBatch(inspectionData(43));
    log->truncatePrefix(1);
    auto response = producersWire(b, {{"data", {0, 1}}});
    EXPECT_EQ(response.topics()[0].partitions[0].error_code, ErrorCode::OFFSET_NOT_AVAILABLE);
    EXPECT_TRUE(response.topics()[0].partitions[0].active_producers.empty());
    EXPECT_EQ(response.topics()[0].partitions[1].error_code, ErrorCode::NONE);
    log->close();
    response = producersWire(b, {{"data", {0}}});
    EXPECT_EQ(response.topics()[0].partitions[0].error_code, ErrorCode::KAFKA_STORAGE_ERROR);
    b.stop();
}

TEST_F(CoordinatorPersistenceTest, ProducerInspectionQueuedReadsKeepOriginalEpochAndLogLifetime) {
    KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
    b.start();
    createData(b);
    b.logManager()->getLog("data", 0)->appendBatch(inspectionData());
    std::promise<void> release;
    const auto released = release.get_future().share();
    struct Release {
        std::promise<void>& promise;
        bool done = false;
        void finish() {
            if (!done) {
                done = true;
                promise.set_value();
            }
        }
        ~Release() { finish(); }
    } guard{release};
    std::atomic<int> entered = 0;
    for (int worker = 0; worker < 2; ++worker)
        ASSERT_TRUE(CoordinatorExecutorProbe::submit(b, [&](bool, auto) {
            ++entered;
            released.wait();
        }));
    ASSERT_TRUE(waitUntil([&] { return entered.load() == 2; }));
    std::promise<RequestDispatcher::DispatchResult> delivered;
    EXPECT_TRUE(CoordinatorExecutorProbe::dispatchProtocol(
                    b, protocol::ApiKey::DESCRIBE_PRODUCERS, 0,
                    producersPayload({{"data", {0, 1}}}),
                    [&](auto result) { delivered.set_value(std::move(result)); })
                    .deferred);
    auto ready = delivered.get_future();
    // Queue pins must survive the registry freeing/deleting the original source.
    ASSERT_EQ(b.metadataController()->deleteTopic("data").error_code, ErrorCode::NONE);
    createData(b);
    guard.finish();
    ASSERT_EQ(ready.wait_for(5s), std::future_status::ready);
    auto result = ready.get();
    const auto response = publishProtocol<protocol::DescribeProducersResponse>(
        result, protocol::ApiKey::DESCRIBE_PRODUCERS, 0);
    for (const auto& p : response.topics()[0].partitions) {
        EXPECT_EQ(p.error_code, ErrorCode::NOT_LEADER_FOR_PARTITION);
        EXPECT_TRUE(p.active_producers.empty());
    }
    result = {};
    b.stop();
}

TEST_F(CoordinatorPersistenceTest, ProducerInspectionHoldsAllSourcesThroughWriteInitiation) {
    KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
    b.start();
    createData(b);
    auto result = parkedProducers(b);
    std::promise<void> entered, release;
    auto publishing = entered.get_future();
    const auto released = release.get_future().share();
    auto publication = std::async(std::launch::async, [&] {
        result.publication_guard([&](auto status) {
            EXPECT_EQ(status, ErrorCode::NONE);
            entered.set_value();
            released.wait();
        });
    });
    const bool started = publishing.wait_for(5s) == std::future_status::ready;
    if (!started)
        release.set_value();
    ASSERT_TRUE(started);
    auto changed = std::async(std::launch::async, [&] {
        return b.metadataController()->updatePartitionLeader("data", 1, 0);
    });
    EXPECT_EQ(changed.wait_for(50ms), std::future_status::timeout);
    release.set_value();
    publication.get();
    EXPECT_EQ(changed.get().error_code, ErrorCode::NONE);
    const auto response = publishProtocol<protocol::DescribeProducersResponse>(
        result, protocol::ApiKey::DESCRIBE_PRODUCERS, 0);
    EXPECT_EQ(response.topics()[0].partitions[0].error_code, ErrorCode::NONE);
    EXPECT_EQ(response.topics()[0].partitions[1].error_code, ErrorCode::NOT_LEADER_FOR_PARTITION);
    result = {};
    b.stop();
}

TEST_F(CoordinatorPersistenceTest, ProducerInspectionClosedAfterReadCannotPublishSuccess) {
    KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
    b.start();
    createData(b);
    auto result = parkedProducers(b);
    b.logManager()->getLog("data", 0)->close();
    const auto response = publishProtocol<protocol::DescribeProducersResponse>(
        result, protocol::ApiKey::DESCRIBE_PRODUCERS, 0);
    for (const auto i : {0, 2})
        EXPECT_EQ(response.topics()[0].partitions[i].error_code, ErrorCode::KAFKA_STORAGE_ERROR);
    EXPECT_EQ(response.topics()[0].partitions[1].error_code, ErrorCode::NONE);
    result = {};
    b.stop();
}

TEST_F(CoordinatorClusterPersistenceTest,
       ProducerInspectionClusterReadsRealFollowerAndRefusesUnhostedReplica) {
    startCluster();
    KawasanBroker* controller = nullptr;
    ASSERT_TRUE(waitUntil([&] {
        for (auto& broker : brokers)
            if (broker->raftNode()->isLeader())
                controller = broker.get();
        return controller != nullptr;
    }));
    TopicSpecification spec;
    spec.name = "data";
    spec.num_partitions = 1;
    spec.replication_factor = 2;
    spec.assignments = {{0, 1}};
    ASSERT_EQ(controller->metadataController()->createTopic(spec).error_code, ErrorCode::NONE);
    ASSERT_TRUE(waitUntil([&] {
        for (auto& broker : brokers)
            broker->reconcileReplicas();
        return brokers[0]->logManager()->getLog("data", 0) &&
               brokers[1]->logManager()->getLog("data", 0);
    }));
    auto* leader = brokers[0]->logManager()->getLog("data", 0);
    leader->appendBatch(inspectionData(), false);
    ASSERT_TRUE(waitUntil([&] {
        const auto follower = producersWire(*brokers[1], {{"data", {0}}});
        const auto& p = follower.topics()[0].partitions[0];
        return p.error_code == ErrorCode::NONE && p.active_producers.size() == 1;
    }));
    auto response = producersWire(*brokers[1], {{"data", {0, 0}}});
    for (const auto& p : response.topics()[0].partitions) {
        ASSERT_EQ(p.active_producers.size(), 1u);
        EXPECT_EQ(p.error_code, ErrorCode::NONE);
        EXPECT_EQ(p.active_producers[0].producer_id, 42);
        EXPECT_EQ(p.active_producers[0].producer_epoch, 3);
        EXPECT_EQ(p.active_producers[0].last_sequence, 17);
        EXPECT_EQ(p.active_producers[0].last_timestamp, 1234);
        EXPECT_EQ(p.active_producers[0].current_txn_start_offset, 0);
    }
    storage::RecordBatch marker;
    marker.setProducerId(42);
    marker.setProducerEpoch(3);
    marker.setBaseSequence(-1);
    marker.setAttributes(0x30);
    Record control;
    control.timestamp = 4321;
    control.key = std::vector<uint8_t>{0, 0, 0, 1};
    Buffer value;
    value.writeInt16(0);
    value.writeInt32(11);
    control.value = value.vector();
    marker.addRecord(control);
    marker.setFirstTimestamp(4321);
    marker.setMaxTimestamp(4321);
    leader->appendBatch(marker, false);
    ASSERT_TRUE(waitUntil([&] {
        const auto snapshot = producersWire(*brokers[1], {{"data", {0}}});
        const auto& p = snapshot.topics()[0].partitions[0];
        return p.error_code == ErrorCode::NONE && p.active_producers.size() == 1 &&
               p.active_producers[0].coordinator_epoch == 11 &&
               p.active_producers[0].last_timestamp == 4321 &&
               p.active_producers[0].last_sequence == 17 &&
               p.active_producers[0].current_txn_start_offset == -1;
    }));
    // Metadata creation eagerly opens ordinary logs even on unassigned brokers.
    // Remove that preexisting log; inspection must neither use nor recreate it.
    brokers[2]->logManager()->deleteLog("data", 0);
    const auto count = brokers[2]->openLogCount();
    response = producersWire(*brokers[2], {{"data", {0}}, {"absent", {0}}});
    EXPECT_EQ(response.topics()[0].partitions[0].error_code, ErrorCode::NOT_LEADER_FOR_PARTITION);
    EXPECT_EQ(response.topics()[1].partitions[0].error_code, ErrorCode::UNKNOWN_TOPIC_OR_PARTITION);
    EXPECT_EQ(brokers[2]->openLogCount(), count);
    EXPECT_FALSE(brokers[2]->logManager()->getLog("data", 0));
    // Fence a response read on a follower across an actual Raft leader transfer.
    std::promise<RequestDispatcher::DispatchResult> delivered;
    EXPECT_TRUE(CoordinatorExecutorProbe::dispatchProtocol(
                    *brokers[1], protocol::ApiKey::DESCRIBE_PRODUCERS, 0,
                    producersPayload({{"data", {0}}}),
                    [&](auto result) { delivered.set_value(std::move(result)); })
                    .deferred);
    auto ready = delivered.get_future();
    ASSERT_EQ(ready.wait_for(5s), std::future_status::ready);
    auto result = ready.get();
    brokers[0]->replicaManager()->stop();
    ASSERT_EQ(controller->metadataController()->updatePartitionLeader("data", 0, 1).error_code,
              ErrorCode::NONE);
    ASSERT_TRUE(waitUntil([&] {
        const auto metadata = brokers[1]->metadataController()->describeTopics({"data"});
        return metadata[0].partitions[0].leader == 1;
    }));
    EXPECT_EQ(publishProtocol<protocol::DescribeProducersResponse>(
                  result, protocol::ApiKey::DESCRIBE_PRODUCERS, 0)
                  .topics()[0]
                  .partitions[0]
                  .error_code,
              ErrorCode::NOT_LEADER_FOR_PARTITION);
    result = {};
    brokers[0]->reconcileReplicas();
    EXPECT_EQ(producersWire(*brokers[0], {{"data", {0}}}).topics()[0].partitions[0].error_code,
              ErrorCode::OFFSET_NOT_AVAILABLE);  // New follower must reconcile divergence first.
}

TEST_F(CoordinatorPersistenceTest, AutomaticTransactionTimeoutCommitsPrepareOnlyAndReplays) {
    auto cfg = config();
    cfg.setInt("transaction.abort.timed.out.transaction.cleanup.interval.ms", 1000);
    const auto id = key(0);
    auto original = transaction(id);
    original.previous_producer_id = 75;
    original.previous_producer_epoch = 1;
    original.pending_offsets = {{key(1), "data", 0, 42, "pending", 5}};
    {
        KawasanBroker b(cfg, KawasanBroker::CoordinatorPersistenceOnly{format});
        b.start();
        b.reconcileReplicas();
        ASSERT_EQ(init(b, original), ErrorCode::NONE);
        ASSERT_TRUE(waitUntil(
            [&] {
                const auto s = CoordinatorPersistenceProbe::transactions(b).describe(id);
                return s && s->state == TransactionCoordinator::State::PrepareAbort;
            },
            4s));
        const auto prepared = *CoordinatorPersistenceProbe::transactions(b).describe(id);
        auto expected = original;
        expected.state = TransactionCoordinator::State::PrepareAbort;
        expected.state_start_time_ms = prepared.state_start_time_ms;
        EXPECT_EQ(prepared, expected);
        EXPECT_GT(prepared.state_start_time_ms, original.state_start_time_ms);
        EXPECT_EQ(TransactionStateManager(b.logManager(), 3).loadCommittedPartition(0).front(),
                  prepared);
        EXPECT_EQ(b.logManager()->getLog("__transaction_state", 0)->logEndOffset(), 2);
        EXPECT_FALSE(b.logManager()->getLog("data", 0));
        EXPECT_FALSE(CoordinatorPersistenceProbe::offsets(b).fetchOffset(key(1), "data", 0));
        EXPECT_EQ(CoordinatorPersistenceProbe::transactions(b).getMetrics().aborts_total, 0);
        b.stop();
    }
    KawasanBroker restarted(cfg, KawasanBroker::CoordinatorPersistenceOnly{format});
    restarted.start();
    restarted.reconcileReplicas();
    ASSERT_TRUE(waitUntil([&] {
        return restarted.coordinatorLoadStatus(id, protocol::CoordinatorType::TRANSACTION) ==
               ErrorCode::NONE;
    }));
    EXPECT_EQ(CoordinatorPersistenceProbe::transactions(restarted).describe(id)->state,
              TransactionCoordinator::State::PrepareAbort);
    // Let an actual scheduled scan pass: Prepare states must never run legacy completion.
    std::this_thread::sleep_for(1200ms);
    EXPECT_EQ(restarted.logManager()->getLog("__transaction_state", 0)->logEndOffset(), 2);
    EXPECT_FALSE(restarted.logManager()->getLog("data", 0));
    EXPECT_EQ(CoordinatorPersistenceProbe::transactions(restarted).getMetrics().aborts_total, 0);
    restarted.stop();
}

namespace {
// Release blocked executor tasks even when an assertion exits a test early.
struct TransactionTimeoutQueueBlock {
    std::promise<void> release;
    std::shared_ptr<std::atomic<int>> entered = std::make_shared<std::atomic<int>>(0);
    bool done = false;
    explicit TransactionTimeoutQueueBlock(KawasanBroker& b) {
        const auto released = release.get_future().share();
        for (int i = 0; i < 2; ++i)
            EXPECT_TRUE(
                CoordinatorExecutorProbe::submit(b, [released, count = entered](bool, auto) {
                    ++*count;
                    released.wait();
                }));
    }
    bool ready() {
        return waitUntil([&] { return entered->load() == 2; });
    }
    void finish() {
        if (!done) {
            release.set_value();
            done = true;
        }
    }
    ~TransactionTimeoutQueueBlock() { finish(); }
};
}  // namespace

TEST_F(CoordinatorPersistenceTest, QueuedTransactionTimeoutRejectsEveryChangedSnapshotField) {
    KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
    b.start();
    b.reconcileReplicas();
    const auto id = key(0);
    auto& tc = CoordinatorPersistenceProbe::transactions(b);
    auto expected = transaction(id);
    ASSERT_EQ(init(b, expected), ErrorCode::NONE);
    const std::vector<KawasanBroker::TransactionMutation> changes = {
        [](auto& s) {
            ++s->producer_id;
            return ErrorCode::NONE;
        },
        [](auto& s) {
            ++s->producer_epoch;
            return ErrorCode::NONE;
        },
        [](auto& s) {
            s->state = TransactionCoordinator::State::PrepareCommit;
            return ErrorCode::NONE;
        },
        [](auto& s) {
            s->state_start_time_ms += 1;
            return ErrorCode::NONE;
        },
        [](auto& s) {
            s->transaction_timeout_ms += 1;
            return ErrorCode::NONE;
        },
        [](auto& s) {
            s->partitions.push_back({"other", 1, -1});
            return ErrorCode::NONE;
        },
        [](auto& s) {
            s->pending_offsets.push_back({"group", "data", 0, 42, "pending", 7});
            return ErrorCode::NONE;
        },
        [](auto& s) {
            s->previous_producer_id = s->producer_id;
            s->previous_producer_epoch = s->producer_epoch - 1;
            return ErrorCode::NONE;
        }};
    for (size_t n = 0; n < changes.size(); ++n) {
        SCOPED_TRACE(n);
        // Reset to an expired Ongoing identity for each independent queued job.
        const auto before = tc.describe(id);
        ASSERT_EQ(b.mutateCoordinatorTransaction(id, before,
                                                 [&](auto& s) {
                                                     s = expected;
                                                     return ErrorCode::NONE;
                                                 }),
                  ErrorCode::NONE);
        TransactionTimeoutQueueBlock blocked(b);
        ASSERT_TRUE(blocked.ready());
        CoordinatorExecutorProbe::scheduleTransactionTimeouts(b);
        ASSERT_TRUE(CoordinatorExecutorProbe::transactionTimeoutPending(b, id));
        // A second scan must neither enqueue another job nor replace its snapshot.
        CoordinatorExecutorProbe::scheduleTransactionTimeouts(b);
        ASSERT_EQ(b.mutateCoordinatorTransaction(id, expected, changes[n]), ErrorCode::NONE);
        const auto newer = tc.describe(id);
        const auto end = b.logManager()->getLog("__transaction_state", 0)->logEndOffset();
        blocked.finish();
        ASSERT_TRUE(
            waitUntil([&] { return !CoordinatorExecutorProbe::transactionTimeoutPending(b, id); }));
        EXPECT_EQ(tc.describe(id), newer);
        EXPECT_EQ(b.logManager()->getLog("__transaction_state", 0)->logEndOffset(), end);
    }
    b.stop();
}

TEST_F(CoordinatorPersistenceTest, QueuedTransactionTimeoutRejectsReacquiredOriginalEpoch) {
    KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
    b.start();
    b.reconcileReplicas();
    const auto id = key(0);
    const auto original = transaction(id);
    ASSERT_EQ(init(b, original), ErrorCode::NONE);
    TransactionTimeoutQueueBlock blocked(b);
    ASSERT_TRUE(blocked.ready());
    CoordinatorExecutorProbe::scheduleTransactionTimeouts(b);
    ASSERT_TRUE(CoordinatorExecutorProbe::transactionTimeoutPending(b, id));
    ASSERT_EQ(b.metadataController()->updatePartitionLeader("__transaction_state", 0, 0).error_code,
              ErrorCode::NONE);
    b.reconcileReplicas();
    ASSERT_EQ(b.coordinatorLoadStatus(id, protocol::CoordinatorType::TRANSACTION), ErrorCode::NONE);
    blocked.finish();
    ASSERT_TRUE(
        waitUntil([&] { return !CoordinatorExecutorProbe::transactionTimeoutPending(b, id); }));
    EXPECT_EQ(CoordinatorPersistenceProbe::transactions(b).describe(id), original);
    EXPECT_EQ(b.logManager()->getLog("__transaction_state", 0)->logEndOffset(), 1);
    // A fresh job in the new epoch may commit the still-expired transaction.
    CoordinatorExecutorProbe::scheduleTransactionTimeouts(b);
    ASSERT_TRUE(waitUntil([&] {
        return CoordinatorPersistenceProbe::transactions(b).describe(id)->state ==
               TransactionCoordinator::State::PrepareAbort;
    }));
    b.stop();
}

TEST_F(CoordinatorPersistenceTest, TransactionTimeoutRechecksDeadlineAndRefusesNonOngoingStates) {
    KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
    b.start();
    b.reconcileReplicas();
    const auto id = key(0);
    auto original = transaction(id);
    original.state_start_time_ms = CoordinatorPersistenceProbe::transactions(b).nowMs() + 60000;
    ASSERT_EQ(init(b, original), ErrorCode::NONE);
    EXPECT_EQ(CoordinatorExecutorProbe::prepareTransactionTimeout(b, original), ErrorCode::NONE);
    EXPECT_EQ(b.logManager()->getLog("__transaction_state", 0)->logEndOffset(), 1);
    using State = TransactionCoordinator::State;
    for (const auto state :
         {State::Empty, State::PrepareCommit, State::PrepareAbort, State::CompleteCommit,
          State::CompleteAbort, State::Dead, State::PrepareEpochFence}) {
        const auto before = CoordinatorPersistenceProbe::transactions(b).describe(id);
        ASSERT_EQ(b.mutateCoordinatorTransaction(id, before,
                                                 [&](auto& s) {
                                                     s->state = state;
                                                     s->state_start_time_ms = 123;
                                                     return ErrorCode::NONE;
                                                 }),
                  ErrorCode::NONE);
        const auto current = CoordinatorPersistenceProbe::transactions(b).describe(id);
        const auto end = b.logManager()->getLog("__transaction_state", 0)->logEndOffset();
        CoordinatorExecutorProbe::scheduleTransactionTimeouts(b);
        EXPECT_FALSE(CoordinatorExecutorProbe::transactionTimeoutPending(b, id));
        EXPECT_EQ(CoordinatorExecutorProbe::prepareTransactionTimeout(b, *current),
                  ErrorCode::NONE);
        EXPECT_EQ(CoordinatorPersistenceProbe::transactions(b).describe(id), current);
        EXPECT_EQ(b.logManager()->getLog("__transaction_state", 0)->logEndOffset(), end);
    }
    b.stop();
}

TEST_F(CoordinatorPersistenceTest, TransactionTimeoutKeepsCheckpointFailureAndSourceBarrier) {
    KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
    b.start();
    b.reconcileReplicas();
    const auto id = key(0);
    const auto original = transaction(id);
    ASSERT_EQ(init(b, original), ErrorCode::NONE);
    const auto blocked = dir / "0/__transaction_state-0/checkpoint.meta.tmp";
    fs::create_directory(blocked);
    EXPECT_EQ(CoordinatorExecutorProbe::prepareTransactionTimeout(b, original),
              ErrorCode::KAFKA_STORAGE_ERROR);
    EXPECT_EQ(CoordinatorPersistenceProbe::transactions(b).describe(id), original);
    EXPECT_EQ(b.coordinatorLoadStatus(id, protocol::CoordinatorType::TRANSACTION),
              ErrorCode::COORDINATOR_LOAD_IN_PROGRESS);
    CoordinatorExecutorProbe::scheduleTransactionTimeouts(b);
    EXPECT_FALSE(CoordinatorExecutorProbe::transactionTimeoutPending(b, id));
    fs::remove(blocked);
    b.acquireCoordinatorPartitions();
    ASSERT_EQ(b.coordinatorLoadStatus(id, protocol::CoordinatorType::TRANSACTION), ErrorCode::NONE);
    EXPECT_EQ(CoordinatorPersistenceProbe::transactions(b).describe(id)->state,
              TransactionCoordinator::State::PrepareAbort);
    EXPECT_EQ(CoordinatorPersistenceProbe::transactions(b).getMetrics().aborts_total, 0);
    b.stop();
}

TEST_F(CoordinatorPersistenceTest, QueuedTransactionTimeoutShutdownDrainsWithoutPublication) {
    KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
    b.start();
    b.reconcileReplicas();
    const auto id = key(0);
    const auto original = transaction(id);
    ASSERT_EQ(init(b, original), ErrorCode::NONE);
    TransactionTimeoutQueueBlock blocked(b);
    ASSERT_TRUE(blocked.ready());
    CoordinatorExecutorProbe::scheduleTransactionTimeouts(b);
    ASSERT_TRUE(CoordinatorExecutorProbe::transactionTimeoutPending(b, id));
    auto stopped = std::async(std::launch::async, [&] { b.stop(); });
    EXPECT_TRUE(waitUntil([&] { return CoordinatorExecutorProbe::stopping(b); }));
    EXPECT_EQ(stopped.wait_for(50ms), std::future_status::timeout);
    blocked.finish();
    ASSERT_EQ(stopped.wait_for(5s), std::future_status::ready);
    stopped.get();
    EXPECT_FALSE(CoordinatorExecutorProbe::transactionTimeoutPending(b, id));
    KawasanBroker reopened(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
    reopened.start();
    reopened.reconcileReplicas();
    EXPECT_EQ(CoordinatorPersistenceProbe::transactions(reopened).describe(id), original);
    EXPECT_EQ(reopened.logManager()->getLog("__transaction_state", 0)->logEndOffset(), 1);
    reopened.stop();
}

TEST_F(CoordinatorPersistenceTest, OwnedTransactionTimeoutReplicatesAndIgnoresNonownedCache) {
    startCluster();
    const auto id = key(1);
    const auto original = transaction(id);
    ASSERT_EQ(init(*brokers[1], original), ErrorCode::NONE);
    CoordinatorPersistenceProbe::transactions(*brokers[0]).restore(original);
    CoordinatorExecutorProbe::scheduleTransactionTimeouts(*brokers[0]);
    EXPECT_FALSE(CoordinatorExecutorProbe::transactionTimeoutPending(*brokers[0], id));
    EXPECT_EQ(CoordinatorPersistenceProbe::transactions(*brokers[0]).describe(id), original);
    CoordinatorExecutorProbe::scheduleTransactionTimeouts(*brokers[1]);
    ASSERT_TRUE(waitUntil([&] {
        return CoordinatorPersistenceProbe::transactions(*brokers[1]).describe(id)->state ==
               TransactionCoordinator::State::PrepareAbort;
    }));
    const auto prepared = *CoordinatorPersistenceProbe::transactions(*brokers[1]).describe(id);
    for (auto& broker : brokers)
        ASSERT_TRUE(waitUntil([&] {
            const auto image =
                TransactionStateManager(broker->logManager(), 3).loadCommittedPartition(1);
            return !image.empty() && image.front() == prepared;
        }));
    KawasanBroker* controller = nullptr;
    for (auto& broker : brokers)
        if (broker->raftNode()->isLeader())
            controller = broker.get();
    ASSERT_NE(controller, nullptr);
    ASSERT_EQ(controller->metadataController()
                  ->updatePartitionLeader("__transaction_state", 1, 2)
                  .error_code,
              ErrorCode::NONE);
    ASSERT_TRUE(waitUntil([&] {
        return brokers[2]->coordinatorLoadStatus(id, protocol::CoordinatorType::TRANSACTION) ==
               ErrorCode::NONE;
    }));
    EXPECT_EQ(CoordinatorPersistenceProbe::transactions(*brokers[2]).describe(id), prepared);
    CoordinatorExecutorProbe::scheduleTransactionTimeouts(*brokers[2]);
    EXPECT_FALSE(CoordinatorExecutorProbe::transactionTimeoutPending(*brokers[2], id));
    EXPECT_EQ(CoordinatorPersistenceProbe::transactions(*brokers[2]).getMetrics().aborts_total, 0);
}

TEST_F(CoordinatorPersistenceTest, TransactionTimeoutIsrFailureAndActiveShutdownRemainUnpublished) {
    startCluster();
    auto& b = *brokers[1];
    const auto id = key(1);
    const auto original = transaction(id);
    ASSERT_EQ(init(b, original), ErrorCode::NONE);
    KawasanBroker* controller = nullptr;
    for (auto& broker : brokers)
        if (broker->raftNode()->isLeader())
            controller = broker.get();
    ASSERT_NE(controller, nullptr);
    for (auto& broker : brokers)
        broker->replicaManager()->stop();
    ASSERT_EQ(controller->metadataController()
                  ->updatePartitionISR("__transaction_state", 1, {1})
                  .error_code,
              ErrorCode::NONE);
    ASSERT_TRUE(waitUntil([&] {
        b.reconcileReplicas();
        return b.replicaManager()->getISR({"__transaction_state", 1}).size() == 1;
    }));
    EXPECT_EQ(CoordinatorExecutorProbe::prepareTransactionTimeout(b, original, 100ms),
              ErrorCode::NOT_ENOUGH_REPLICAS);
    EXPECT_EQ(CoordinatorPersistenceProbe::transactions(b).describe(id), original);
    EXPECT_EQ(b.logManager()->getLog("__transaction_state", 1)->logEndOffset(), 1);
    ASSERT_EQ(controller->metadataController()
                  ->updatePartitionISR("__transaction_state", 1, {0, 1, 2})
                  .error_code,
              ErrorCode::NONE);
    ASSERT_TRUE(waitUntil([&] {
        b.reconcileReplicas();
        return b.replicaManager()->getISR({"__transaction_state", 1}).size() == 3;
    }));
    b.replicaManager()->start();
    EXPECT_EQ(CoordinatorExecutorProbe::prepareTransactionTimeout(b, original, 100ms),
              ErrorCode::REQUEST_TIMED_OUT);
    EXPECT_EQ(CoordinatorPersistenceProbe::transactions(b).describe(id), original);
    EXPECT_EQ(b.coordinatorLoadStatus(id, protocol::CoordinatorType::TRANSACTION),
              ErrorCode::COORDINATOR_LOAD_IN_PROGRESS);
    brokers[0]->replicaManager()->start();
    brokers[2]->replicaManager()->start();
    ASSERT_TRUE(waitUntil([&] {
        return b.coordinatorLoadStatus(id, protocol::CoordinatorType::TRANSACTION) ==
               ErrorCode::NONE;
    }));
    auto current = CoordinatorPersistenceProbe::transactions(b).describe(id);
    ASSERT_EQ(current->state, TransactionCoordinator::State::PrepareAbort);
    ASSERT_EQ(b.mutateCoordinatorTransaction(id, current,
                                             [&](auto& s) {
                                                 s = original;
                                                 return ErrorCode::NONE;
                                             }),
              ErrorCode::NONE);
    brokers[0]->replicaManager()->stop();
    brokers[2]->replicaManager()->stop();
    const auto end = b.logManager()->getLog("__transaction_state", 1)->logEndOffset();
    CoordinatorExecutorProbe::scheduleTransactionTimeouts(b);
    ASSERT_TRUE(waitUntil([&] {
        return b.logManager()->getLog("__transaction_state", 1)->logEndOffset() == end + 1;
    }));
    EXPECT_TRUE(CoordinatorExecutorProbe::transactionTimeoutPending(b, id));
    auto stopped = std::async(std::launch::async, [&] { b.stop(); });
    ASSERT_EQ(stopped.wait_for(5s), std::future_status::ready);
    stopped.get();
    EXPECT_FALSE(CoordinatorExecutorProbe::transactionTimeoutPending(b, id));
    EXPECT_EQ(CoordinatorPersistenceProbe::transactions(b).describe(id), original);
}

TEST_F(CoordinatorPersistenceTest, QueuedTransactionTimeoutRechecksRolledBackWallClock) {
    KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
    auto clock = std::make_shared<std::atomic<int64_t>>(2000);
    CoordinatorPersistenceProbe::transactionClockBeforeStart(b, [clock] { return clock->load(); });
    b.start();
    b.reconcileReplicas();
    const auto id = key(0);
    const auto original = transaction(id);
    ASSERT_EQ(init(b, original), ErrorCode::NONE);
    TransactionTimeoutQueueBlock blocked(b);
    ASSERT_TRUE(blocked.ready());
    CoordinatorExecutorProbe::scheduleTransactionTimeouts(b);
    ASSERT_TRUE(CoordinatorExecutorProbe::transactionTimeoutPending(b, id));
    clock->store(100);
    blocked.finish();
    ASSERT_TRUE(
        waitUntil([&] { return !CoordinatorExecutorProbe::transactionTimeoutPending(b, id); }));
    EXPECT_EQ(CoordinatorPersistenceProbe::transactions(b).describe(id), original);
    EXPECT_EQ(b.logManager()->getLog("__transaction_state", 0)->logEndOffset(), 1);
    clock->store(3000);
    CoordinatorExecutorProbe::scheduleTransactionTimeouts(b);
    ASSERT_TRUE(waitUntil([&] {
        return CoordinatorPersistenceProbe::transactions(b).describe(id)->state ==
               TransactionCoordinator::State::PrepareAbort;
    }));
    EXPECT_EQ(CoordinatorPersistenceProbe::transactions(b).describe(id)->state_start_time_ms, 3000);
    b.stop();
}

TEST_F(CoordinatorPersistenceTest, TransactionTimeoutWalWriteAndSyncFailuresRetainOriginalError) {
    for (const bool fail_sync : {false, true}) {
        auto cfg = config();
        const auto root = fs::canonical(fs::temp_directory_path()) / dir.filename() /
                          (fail_sync ? "timeout-sync-failure" : "timeout-write-failure");
        cfg.setString("log.dirs", root.string());
        KawasanBroker b(cfg, KawasanBroker::CoordinatorPersistenceOnly{format});
        b.start();
        b.reconcileReplicas();
        const auto id = key(0);
        const auto original = transaction(id);
        ASSERT_EQ(init(b, original), ErrorCode::NONE);
        const int wal = sourceWalDescriptor(root / "__transaction_state-0");
        ASSERT_GE(wal, 0);
        struct RestoreDescriptor {
            int wal;
            int original;
            std::array<int, 2> pipe{-1, -1};
            int replacement = -1;
            ~RestoreDescriptor() {
                if (original >= 0) {
                    ::dup2(original, wal);
                    ::close(original);
                }
                if (replacement >= 0)
                    ::close(replacement);
                for (const auto fd : pipe)
                    if (fd >= 0)
                        ::close(fd);
            }
        } restore{wal, ::dup(wal)};
        ASSERT_GE(restore.original, 0);
        if (fail_sync) {
            ASSERT_EQ(::pipe(restore.pipe.data()), 0);
            ASSERT_GE(::dup2(restore.pipe[1], wal), 0);
        } else {
            restore.replacement = ::open("/dev/null", O_RDONLY);
            ASSERT_GE(restore.replacement, 0);
            ASSERT_GE(::dup2(restore.replacement, wal), 0);
        }
        EXPECT_EQ(CoordinatorExecutorProbe::prepareTransactionTimeout(b, original),
                  ErrorCode::KAFKA_STORAGE_ERROR);
        EXPECT_EQ(CoordinatorPersistenceProbe::transactions(b).describe(id), original);
        EXPECT_EQ(b.logManager()->getLog("__transaction_state", 0)->logEndOffset(), 1);
        b.acquireCoordinatorPartitions();
        EXPECT_EQ(b.coordinatorLoadStatus(id, protocol::CoordinatorType::TRANSACTION),
                  ErrorCode::COORDINATOR_LOAD_IN_PROGRESS);
        CoordinatorExecutorProbe::scheduleTransactionTimeouts(b);
        EXPECT_FALSE(CoordinatorExecutorProbe::transactionTimeoutPending(b, id));
    }
}

namespace {
std::future<RequestDispatcher::DispatchResult> queuedUnkeyedProducer(KawasanBroker& b) {
    auto delivered = std::make_shared<std::promise<RequestDispatcher::DispatchResult>>();
    auto future = delivered->get_future();
    auto immediate = CoordinatorExecutorProbe::dispatchProtocol(
        b, protocol::ApiKey::INIT_PRODUCER_ID, 4, producerInitPayload(std::nullopt),
        [delivered](auto result) { delivered->set_value(std::move(result)); });
    EXPECT_TRUE(immediate.deferred);
    if (!immediate.deferred)
        delivered->set_value(std::move(immediate));
    return future;
}
}  // namespace

class CoordinatorUnkeyedProducerTest : public CoordinatorPersistenceTest {};

TEST_F(CoordinatorUnkeyedProducerTest, UnkeyedProducerWireVersionsPersistFreshIdsAndRestart) {
    int64_t last = 0;
    {
        KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
        b.start();
        b.reconcileReplicas();
        for (int16_t version : {0, 1, 2, 3, 4}) {
            SCOPED_TRACE(version);
            const auto result = producerInitWire(b, std::nullopt, version, version >= 3 ? 1234 : -1,
                                                 version >= 3 ? 7 : -1, version % 2 ? -1 : 0);
            ASSERT_EQ(result.error, ErrorCode::NONE);
            EXPECT_EQ(result.id, ++last);
            EXPECT_EQ(result.epoch, 0);
            int64_t next = 0;
            std::ifstream counter_file(dir / "0/meta/producer_id.counter");
            counter_file >> next;
            EXPECT_EQ(next, last + 1);
        }
        EXPECT_TRUE(CoordinatorPersistenceProbe::transactions(b).list({}, {}).empty());
        for (int p = 0; p < 3; ++p) {
            EXPECT_EQ(b.logManager()->getLog("__transaction_state", p)->logEndOffset(), 0);
            EXPECT_EQ(b.logManager()->getLog("__consumer_offsets", p)->logEndOffset(), 0);
        }
        b.stop();
    }
    KawasanBroker restarted(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
    restarted.start();
    restarted.reconcileReplicas();
    const auto next = producerInitWire(restarted, std::nullopt);
    EXPECT_EQ(next.error, ErrorCode::NONE);
    EXPECT_EQ(next.id, last + 1);
    EXPECT_EQ(next.epoch, 0);
    restarted.stop();
}

TEST_F(CoordinatorUnkeyedProducerTest,
       UnkeyedProducerInvalidInputsAndAcquisitionOnlyNeverAllocate) {
    {
        KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
        b.start();
        b.reconcileReplicas();
        for (const auto& [id, epoch] :
             std::vector<std::pair<int64_t, int16_t>>{{-1, 0}, {1, -1}, {-2, 0}, {1, -2}}) {
            const auto refused = producerInitWire(b, std::nullopt, 4, id, epoch);
            EXPECT_EQ(refused.error, ErrorCode::INVALID_REQUEST);
            EXPECT_EQ(refused.id, -1);
            EXPECT_EQ(refused.epoch, -1);
        }
        EXPECT_FALSE(fs::exists(dir / "0/meta/producer_id.counter"));
        b.stop();
    }
    KawasanBroker acquisition(config(), KawasanBroker::CoordinatorAcquisitionOnly{format});
    acquisition.start();
    acquisition.reconcileReplicas();
    for (int16_t version : {0, 1, 2, 3, 4}) {
        const auto refused = producerInitWire(acquisition, std::nullopt, version);
        EXPECT_EQ(refused.error, ErrorCode::COORDINATOR_LOAD_IN_PROGRESS);
        EXPECT_EQ(refused.id, -1);
        EXPECT_EQ(refused.epoch, -1);
    }
    EXPECT_FALSE(fs::exists(dir / "0/meta/producer_id.counter"));
    acquisition.stop();
}

TEST_F(CoordinatorUnkeyedProducerTest, UnkeyedProducerCounterFailuresKeepExactWireErrors) {
    KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
    b.start();
    b.reconcileReplicas();
    const auto counter = dir / "0/meta/producer_id.counter";
    const auto blocked = dir / "0/meta/producer_id.counter.tmp";
    fs::create_directory(blocked);
    for (int16_t version : {0, 1, 2, 3, 4}) {
        const auto refused = producerInitWire(b, std::nullopt, version);
        EXPECT_EQ(refused.error, ErrorCode::KAFKA_STORAGE_ERROR);
        EXPECT_EQ(refused.id, -1);
        EXPECT_EQ(refused.epoch, -1);
    }
    EXPECT_FALSE(fs::exists(counter));
    fs::remove(blocked);
    struct CounterPipe {
        std::array<int, 2> fds{-1, -1};
        ~CounterPipe() {
            for (const int fd : fds)
                if (fd >= 0)
                    ::close(fd);
        }
    } counter_pipe;
    ASSERT_EQ(::pipe(counter_pipe.fds.data()), 0);
    for (int16_t version : {0, 1, 2, 3, 4}) {
        // A pipe accepts the counter write but rejects fsync on both platforms.
        // The atomic writer must unlink the failed temporary path and refuse
        // the identity without advancing either durable or in-memory state.
        fs::create_symlink("/dev/fd/" + std::to_string(counter_pipe.fds[1]), blocked);
        const auto refused = producerInitWire(b, std::nullopt, version);
        EXPECT_EQ(refused.error, ErrorCode::KAFKA_STORAGE_ERROR);
        EXPECT_EQ(refused.id, -1);
        EXPECT_EQ(refused.epoch, -1);
        EXPECT_FALSE(fs::exists(counter));
        EXPECT_FALSE(fs::exists(blocked));
    }
    const auto first = producerInitWire(b, std::nullopt);
    ASSERT_EQ(first.error, ErrorCode::NONE);
    EXPECT_EQ(first.id, 1);
    for (const auto& invalid : {std::string("garbage"), std::string("1"),
                                std::to_string(std::numeric_limits<int64_t>::max())}) {
        std::ofstream(counter) << invalid;
        const auto refused = producerInitWire(b, std::nullopt);
        EXPECT_EQ(refused.error, ErrorCode::KAFKA_STORAGE_ERROR);
        EXPECT_EQ(refused.id, -1);
        EXPECT_EQ(refused.epoch, -1);
    }
    std::ofstream(counter) << 2;
    const auto second = producerInitWire(b, std::nullopt);
    EXPECT_EQ(second.error, ErrorCode::NONE);
    EXPECT_EQ(second.id, 2);
    EXPECT_TRUE(CoordinatorPersistenceProbe::transactions(b).list({}, {}).empty());
    b.stop();
}

TEST_F(CoordinatorUnkeyedProducerTest,
       UnkeyedProducerQueueDefersCounterIoAndLeavesSocketResponsive) {
    KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
    b.start();
    b.reconcileReplicas();
    TransactionTimeoutQueueBlock blocked(b);
    ASSERT_TRUE(blocked.ready());
    auto future = queuedUnkeyedProducer(b);
    EXPECT_EQ(future.wait_for(50ms), std::future_status::timeout);
    EXPECT_FALSE(fs::exists(dir / "0/meta/producer_id.counter"));
    Buffer request;
    protocol::RequestHeader(protocol::ApiKey::API_VERSIONS, 1, 92, "io-progress").encode(request);
    protocol::ApiVersionsRequest{}.encode(request, 1);
    const auto response = test_support::brokerRequest(b.port(), request);
    EXPECT_GT(response.size(), 10u);
    blocked.finish();
    ASSERT_EQ(future.wait_for(5s), std::future_status::ready);
    auto result = future.get();
    const auto identity = publishProducer(result);
    EXPECT_EQ(identity.error, ErrorCode::NONE);
    EXPECT_EQ(identity.id, 1);
    result = {};
    b.stop();
}

TEST_F(CoordinatorUnkeyedProducerTest, UnkeyedProducerQueueCapacityRefusesWithoutCounterIo) {
    KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
    b.start();
    b.reconcileReplicas();
    TransactionTimeoutQueueBlock blocked(b);
    ASSERT_TRUE(blocked.ready());
    for (int n = 0; n < 1024; ++n)
        ASSERT_TRUE(CoordinatorExecutorProbe::submit(b, [](bool, auto) {}));
    auto immediate = CoordinatorExecutorProbe::dispatchProtocol(
        b, protocol::ApiKey::INIT_PRODUCER_ID, 4, producerInitPayload(std::nullopt), [](auto) {});
    EXPECT_FALSE(immediate.deferred);
    const auto refused = publishProducer(immediate);
    EXPECT_EQ(refused.error, ErrorCode::COORDINATOR_LOAD_IN_PROGRESS);
    EXPECT_EQ(refused.id, -1);
    EXPECT_EQ(refused.epoch, -1);
    EXPECT_FALSE(fs::exists(dir / "0/meta/producer_id.counter"));
    blocked.finish();
    b.stop();
}

TEST_F(CoordinatorUnkeyedProducerTest, UnkeyedProducerQueuedAndActiveShutdownRefuseAllocation) {
    for (const bool active : {false, true}) {
        SCOPED_TRACE(active);
        auto cfg = config();
        const auto root = dir / (active ? "active" : "queued");
        cfg.setString("log.dirs", root.string());
        KawasanBroker b(cfg, KawasanBroker::CoordinatorPersistenceOnly{format});
        b.start();
        b.reconcileReplicas();
        std::optional<TransactionTimeoutQueueBlock> blocked;
        std::optional<std::unique_lock<std::mutex>> allocation;
        if (active)
            allocation.emplace(CoordinatorExecutorProbe::holdProducerIds(b));
        else {
            blocked.emplace(b);
            ASSERT_TRUE(blocked->ready());
        }
        auto future = queuedUnkeyedProducer(b);
        EXPECT_EQ(future.wait_for(100ms), std::future_status::timeout);
        EXPECT_FALSE(fs::exists(root / "meta/producer_id.counter"));
        auto stopped = std::async(std::launch::async, [&] { b.stop(); });
        EXPECT_TRUE(waitUntil([&] { return CoordinatorExecutorProbe::stopping(b); }));
        EXPECT_EQ(stopped.wait_for(50ms), std::future_status::timeout);
        allocation.reset();
        if (blocked)
            blocked->finish();
        ASSERT_EQ(future.wait_for(5s), std::future_status::ready);
        auto result = future.get();
        const auto refused = publishProducer(result);
        EXPECT_EQ(refused.error, ErrorCode::NOT_COORDINATOR);
        EXPECT_EQ(refused.id, -1);
        EXPECT_EQ(refused.epoch, -1);
        result = {};
        EXPECT_EQ(stopped.wait_for(5s), std::future_status::ready);
        stopped.get();
        EXPECT_FALSE(fs::exists(root / "meta/producer_id.counter"));
    }
}

TEST_F(CoordinatorUnkeyedProducerTest,
       UnkeyedProducerResponseFencesWriteAndDrainsWithoutSourceEpoch) {
    KawasanBroker b(config(), KawasanBroker::CoordinatorPersistenceOnly{format});
    b.start();
    b.reconcileReplicas();
    auto future = queuedUnkeyedProducer(b);
    ASSERT_EQ(future.wait_for(5s), std::future_status::ready);
    auto result = future.get();
    ASSERT_TRUE(result.publication_guard);
    ASSERT_EQ(publishProducer(result).error, ErrorCode::NONE);
    std::promise<void> entered, release;
    auto entered_future = entered.get_future();
    auto released = release.get_future().share();
    auto publishing = std::async(std::launch::async, [&] {
        result.publication_guard([&](auto status) {
            EXPECT_EQ(status, ErrorCode::NONE);
            entered.set_value();
            released.wait();
        });
    });
    const bool ready = entered_future.wait_for(5s) == std::future_status::ready;
    if (!ready)
        release.set_value();
    ASSERT_TRUE(ready);
    auto changed = std::async(std::launch::async, [&] {
        return b.metadataController()->updatePartitionLeader("__transaction_state", 0, 0);
    });
    EXPECT_EQ(changed.wait_for(50ms), std::future_status::timeout);
    release.set_value();
    publishing.get();
    EXPECT_EQ(changed.get().error_code, ErrorCode::NONE);
    // Null IDs have no source owner epoch: this change cannot fence the result.
    EXPECT_EQ(publishProducer(result).error, ErrorCode::NONE);
    auto stopped = std::async(std::launch::async, [&] { b.stop(); });
    EXPECT_TRUE(waitUntil([&] { return CoordinatorExecutorProbe::stopping(b); }));
    EXPECT_EQ(stopped.wait_for(50ms), std::future_status::timeout);
    EXPECT_EQ(publishProducer(result).error, ErrorCode::NOT_COORDINATOR);
    result = {};
    ASSERT_EQ(stopped.wait_for(5s), std::future_status::ready);
    stopped.get();
}

TEST_F(CoordinatorUnkeyedProducerTest,
       UnkeyedProducerClusterRangesIgnoreSourcesAndFenceStaleMetadata) {
    startCluster();
    for (int id = 0; id < 3; ++id) {
        auto& b = *brokers[id];
        const auto first = producerInitWire(b, std::nullopt);
        ASSERT_EQ(first.error, ErrorCode::NONE);
        EXPECT_EQ(first.id, (static_cast<int64_t>(id) << 32) | 1);
        EXPECT_EQ(first.epoch, 0);
        const auto snapshot = transaction(key(id));
        ASSERT_EQ(init(b, snapshot), ErrorCode::NONE);
        fs::create_directory(dir / std::to_string(id) /
                             ("__transaction_state-" + std::to_string(id)) / "checkpoint.meta.tmp");
        ASSERT_EQ(b.mutateCoordinatorTransaction(key(id), snapshot,
                                                 [](auto& proposed) {
                                                     ++proposed->producer_epoch;
                                                     return ErrorCode::NONE;
                                                 }),
                  ErrorCode::KAFKA_STORAGE_ERROR);
        EXPECT_EQ(b.coordinatorLoadStatus(key(id), protocol::CoordinatorType::TRANSACTION),
                  ErrorCode::COORDINATOR_LOAD_IN_PROGRESS);
        const auto second = producerInitWire(b, std::nullopt);
        EXPECT_EQ(second.error, ErrorCode::NONE);
        EXPECT_EQ(second.id, (static_cast<int64_t>(id) << 32) | 2);
    }
    auto future = queuedUnkeyedProducer(*brokers[0]);
    ASSERT_EQ(future.wait_for(5s), std::future_status::ready);
    auto result = future.get();
    EXPECT_EQ(publishProducer(result).error, ErrorCode::NONE);
    TransactionTimeoutQueueBlock blocked(*brokers[0]);
    ASSERT_TRUE(blocked.ready());
    auto queued = queuedUnkeyedProducer(*brokers[0]);
    EXPECT_EQ(queued.wait_for(50ms), std::future_status::timeout);
    for (auto& b : brokers)
        b->raftNode()->stop();
    ASSERT_TRUE(waitUntil([&] { return !CoordinatorExecutorProbe::current(*brokers[0]); }));
    blocked.finish();
    ASSERT_EQ(queued.wait_for(5s), std::future_status::ready);
    auto refused_job = queued.get();
    const auto refused_identity = publishProducer(refused_job);
    EXPECT_EQ(refused_identity.error, ErrorCode::NOT_COORDINATOR);
    EXPECT_EQ(refused_identity.id, -1);
    EXPECT_EQ(refused_identity.epoch, -1);
    refused_job = {};
    const auto stale = producerInitWire(*brokers[0], std::nullopt);
    EXPECT_EQ(stale.error, ErrorCode::NOT_COORDINATOR);
    EXPECT_EQ(stale.id, -1);
    EXPECT_EQ(stale.epoch, -1);
    EXPECT_EQ(publishProducer(result).error, ErrorCode::NOT_COORDINATOR);
    result = {};
    int64_t next = 0;
    std::ifstream counter_file(dir / "0/meta/producer_id.counter");
    counter_file >> next;
    EXPECT_EQ(next, 4);  // Stale requests cannot consume another ID.
}
