#include <gtest/gtest.h>

#include <boost/asio.hpp>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "../sparse_broker_test_client.h"
#include "kawasan/broker/kawasan_broker.h"
#include "kawasan/broker/metadata_controller.h"
#include "kawasan/broker/replica_manager.h"
#include "kawasan/common/config.h"
#include "kawasan/common/logger.h"
#include "kawasan/storage/record_batch.h"

using namespace std::chrono_literals;

namespace {
namespace asio = boost::asio;
namespace fs = std::filesystem;

// The broker derives a peer's Kafka port from its Raft port minus one.
int reservePortPair(asio::io_context& io,
                    std::vector<std::unique_ptr<asio::ip::tcp::acceptor>>& reservations) {
    for (int attempt = 0; attempt < 100; ++attempt) {
        auto kafka = std::make_unique<asio::ip::tcp::acceptor>(io);
        kafka->open(asio::ip::tcp::v4());
        kafka->bind({asio::ip::make_address("127.0.0.1"), 0});
        const auto port = kafka->local_endpoint().port();
        if (port == 65535)
            continue;
        boost::system::error_code error;
        auto raft = std::make_unique<asio::ip::tcp::acceptor>(io);
        raft->open(asio::ip::tcp::v4());
        raft->bind({asio::ip::make_address("127.0.0.1"), static_cast<uint16_t>(port + 1)}, error);
        if (!error) {
            reservations.push_back(std::move(kafka));
            reservations.push_back(std::move(raft));
            return port;
        }
    }
    throw std::runtime_error("Cannot allocate Kafka/Raft port pair");
}

template <typename Predicate>
bool waitUntil(Predicate predicate, std::chrono::seconds timeout = 20s) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    do {
        if (predicate())
            return true;
        std::this_thread::sleep_for(20ms);
    } while (std::chrono::steady_clock::now() < deadline);
    return predicate();
}

class CompactedReplicationTest : public ::testing::Test {
protected:
    void SetUp() override {
        static const bool logger = [] {
            kawasan::Logger::init("warn");
            return true;
        }();
        (void)logger;
        const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
        directory_ =
            fs::temp_directory_path() / ("kawasan-compacted-replica-" + std::to_string(stamp));
        std::vector<int> ports;
        std::string peers;
        for (int id = 0; id < 3; ++id) {
            const int port = reservePortPair(reservation_io_, reservations_);
            ports.push_back(port);
            if (!peers.empty())
                peers += ',';
            peers += std::to_string(id) + ":127.0.0.1:" + std::to_string(port + 1);
        }
        for (int id = 0; id < 3; ++id) {
            kawasan::Config config;
            config.setInt("broker.id", id);
            config.setString("host", "127.0.0.1");
            config.setString("advertised.host", "127.0.0.1");
            config.setInt("port", ports[id]);
            config.setInt("raft.port", ports[id] + 1);
            config.setString("raft.peers", peers);
            config.setInt("monitoring.port", 0);
            config.setInt("network.io_threads", 2);
            config.setInt("offsets.topic.num.partitions", 1);
            config.setInt("transaction.state.topic.num.partitions", 1);
            config.setLong("replica.lag.time.max.ms", 1000);
            config.setString("log.dirs", (directory_ / std::to_string(id)).string());
            configs_.push_back(config);
            brokers_.push_back(std::make_unique<kawasan::broker::KawasanBroker>(config));
        }
        for (size_t id = 0; id < brokers_.size(); ++id) {
            // Future listeners stay reserved so an earlier node's outgoing RPC
            // cannot use their ports as ephemeral client ports.
            reservations_[id * 2].reset();
            reservations_[id * 2 + 1].reset();
            brokers_[id]->start();
        }
        ASSERT_TRUE(waitUntil([&] { return controller() != nullptr; }));
    }

    void TearDown() override {
        for (auto& broker : brokers_)
            broker->stop();
        brokers_.clear();
        if (HasFailure())
            std::cerr << "Retained compacted cluster data: " << directory_ << '\n';
        else
            fs::remove_all(directory_);
    }

    kawasan::broker::MetadataController* controller() {
        for (auto& broker : brokers_) {
            if (broker->raftNode()->isLeader() && broker->raftNode()->hasCurrentMetadata(1000))
                return broker->metadataController();
        }
        return nullptr;
    }

    fs::path directory_;
    asio::io_context reservation_io_;
    std::vector<std::unique_ptr<asio::ip::tcp::acceptor>> reservations_;
    std::vector<std::unique_ptr<kawasan::broker::KawasanBroker>> brokers_;
    std::vector<kawasan::Config> configs_;
};

TEST_F(CompactedReplicationTest, LaggingFollowerCrossesCommittedCompactionGaps) {
    constexpr const char* topic = "compacted-state";
    const kawasan::TopicPartition tp{topic, 0};
    kawasan::broker::TopicSpecification spec;
    spec.name = topic;
    spec.num_partitions = 1;
    spec.replication_factor = 3;
    spec.assignments = {{0, 1, 2}};
    spec.configs = {
        {"cleanup.policy", "compact"}, {"segment.bytes", "1"}, {"min.insync.replicas", "2"}};
    ASSERT_EQ(controller()->createTopic(spec).error_code, kawasan::ErrorCode::NONE);
    ASSERT_TRUE(waitUntil([&] {
        for (auto& broker : brokers_) {
            if (!broker->logManager()->getLog(topic, 0) ||
                !broker->replicaManager()->getFetchOffset(tp).has_value())
                return false;
        }
        return brokers_[0]->replicaManager()->isLeader(tp);
    }));
    // ISR expiry requires an observed fetch. Merely registering a follower
    // does not prove it has reached the leader; stopping before its first fetch
    // leaves it unseen, which computeIsrUpdate deliberately keeps in the ISR.
    ASSERT_TRUE(waitUntil([&] {
        return brokers_[0]->replicaManager()->getFollowerLag(tp, 1).has_value() &&
               brokers_[0]->replicaManager()->getFollowerLag(tp, 2).has_value();
    }));
    brokers_[2]->replicaManager()->stop();
    ASSERT_TRUE(waitUntil([&] {
        const auto metadata = brokers_[0]->metadataController()->describeTopics({topic});
        return metadata.size() == 1 && metadata[0].partitions.size() == 1 &&
               metadata[0].partitions[0].isr == std::vector<kawasan::BrokerId>({0, 1});
    }));
    auto* leader = brokers_[0]->logManager()->getLog(topic, 0);
    for (int index = 0; index < 3; ++index) {
        kawasan::storage::RecordBatch batch;
        batch.addRecord(kawasan::Record("id", "value-" + std::to_string(index)));
        auto write_lock = brokers_[0]->lockPartitionWrites(tp);
        leader->appendBatch(batch, false);
    }
    ASSERT_TRUE(waitUntil([&] { return leader->highWatermark() == 3; }));
    ASSERT_EQ(brokers_[1]->logManager()->getLog(topic, 0)->logEndOffset(), 3);
    leader->cleanup();
    const auto retained = leader->read(0, 4096);
    ASSERT_EQ(retained.size(), 1u);
    ASSERT_EQ(retained[0].baseOffset(), 2);

    brokers_[2]->replicaManager()->start();
    auto* follower = brokers_[2]->logManager()->getLog(topic, 0);
    ASSERT_TRUE(waitUntil([&] {
        return follower->logEndOffset() == 3 && follower->highWatermark() == 3 &&
               brokers_[2]->replicaManager()->getFetchOffset(tp) == 3;
    })) << "Follower must ingest the leader's committed batch at offset 2 after holes 0..1";
    const auto mirrored = follower->read(0, 4096);
    ASSERT_EQ(mirrored.size(), 1u);
    EXPECT_EQ(mirrored[0].baseOffset(), 2);
    EXPECT_EQ(mirrored[0].records()[0].value, retained[0].records()[0].value);
}

TEST_F(CompactedReplicationTest, PromotionReplaysSparseSequencesAndLaterProducer) {
    using namespace kawasan::test_support;
    constexpr const char* topic = "sparse-promotion";
    const kawasan::TopicPartition tp{topic, 0};
    kawasan::broker::TopicSpecification spec;
    spec.name = topic;
    spec.num_partitions = 1;
    spec.replication_factor = 3;
    spec.assignments = {{0, 1, 2}};
    spec.configs = {{"cleanup.policy", "compact"}};
    ASSERT_EQ(controller()->createTopic(spec).error_code, kawasan::ErrorCode::NONE);
    ASSERT_TRUE(waitUntil([&] {
        for (auto& broker : brokers_)
            if (!broker->logManager()->getLog(topic, 0))
                return false;
        return brokers_[0]->replicaManager()->isLeader(tp);
    }));
    auto* leader = brokers_[0]->logManager()->getLog(topic, 0);
    {
        auto write_lock = brokers_[0]->lockPartitionWrites(tp);
        ASSERT_EQ(leader->appendReplicatedBatch(sparseProducerBatch(8 * 1024 * 1024)),
                  kawasan::storage::Log::ReplicaAppendResult::kAppended);
        leader->appendBatch(producerBatch(100, 0), false);
        leader->appendBatch(kawasan::storage::RecordBatch::makeControlBatch(100, 0, 11, true, 0),
                            false);
    }
    ASSERT_TRUE(waitUntil([&] {
        for (auto& broker : brokers_)
            if (broker->logManager()->getLog(topic, 0)->highWatermark() != 12)
                return false;
        return true;
    }));
    ASSERT_EQ(controller()->updatePartitionLeader(topic, 0, 1).error_code,
              kawasan::ErrorCode::NONE);
    ASSERT_TRUE(waitUntil([&] { return brokers_[1]->replicaManager()->isLeader(tp); }));
    const auto retry = brokerProduce(brokers_[1]->port(), topic, producerBatch(100, 0));
    EXPECT_EQ(retry.error_code, kawasan::ErrorCode::NONE);
    EXPECT_EQ(retry.base_offset, 10);
    EXPECT_EQ(brokers_[1]->logManager()->getLog(topic, 0)->logEndOffset(), 12);
    const auto next = brokerProduce(brokers_[1]->port(), topic, producerBatch(99, 10));
    EXPECT_EQ(next.error_code, kawasan::ErrorCode::NONE);
    EXPECT_EQ(next.base_offset, 12);
}

TEST_F(CompactedReplicationTest, FollowerProducerInspectionTracksIngestionOverlapAndRestart) {
    using namespace kawasan;
    using namespace kawasan::test_support;
    constexpr const char* topic = "producer-inspection";
    const TopicPartition tp{topic, 0};
    broker::TopicSpecification spec;
    spec.name = topic;
    spec.num_partitions = 1;
    spec.replication_factor = 3;
    spec.assignments = {{0, 1, 2}};
    spec.configs = {{"cleanup.policy", "compact"}};
    ASSERT_EQ(controller()->createTopic(spec).error_code, ErrorCode::NONE);
    ASSERT_TRUE(waitUntil([&] {
        for (auto& broker : brokers_)
            if (!broker->logManager()->getLog(topic, 0))
                return false;
        return brokers_[0]->replicaManager()->isLeader(tp);
    }));
    auto* leader = brokers_[0]->logManager()->getLog(topic, 0);
    {
        auto write_lock = brokers_[0]->lockPartitionWrites(tp);
        auto first = sparseProducerBatch();
        first.setAttributes(0x10);
        first.setFirstTimestamp(500);
        first.setMaxTimestamp(500);
        ASSERT_EQ(leader->appendReplicatedBatch(first),
                  storage::Log::ReplicaAppendResult::kAppended);
    }
    auto* follower = brokers_[1]->logManager()->getLog(topic, 0);
    ASSERT_TRUE(waitUntil([&] { return follower->logEndOffset() == 10; }));
    ASSERT_FALSE(brokers_[1]->replicaManager()->isLeader(tp));
    auto image = follower->inspectProducerState();
    ASSERT_EQ(image.producers.size(), 1u);
    EXPECT_EQ(image.producers[0], (storage::Log::ProducerState{99, 0, 9, 500, -1, 0}));
    {
        auto write_lock = brokers_[0]->lockPartitionWrites(tp);
        leader->appendBatch(storage::RecordBatch::makeControlBatch(99, 0, 10, false, 700), false);
        auto later = producerBatch(100, 0);
        later.setFirstTimestamp(900);
        later.setMaxTimestamp(900);
        leader->appendBatch(later, false);
    }
    ASSERT_TRUE(waitUntil(
        [&] { return follower->logEndOffset() == 12 && follower->highWatermark() == 12; }));
    const auto expected = leader->inspectProducerState().producers;
    ASSERT_EQ(expected.size(), 2u);
    EXPECT_EQ(expected[0], (storage::Log::ProducerState{99, 0, 9, 700, 0, -1}));
    EXPECT_EQ(expected[1], (storage::Log::ProducerState{100, 0, 0, 900, -1, -1}));
    EXPECT_EQ(follower->inspectProducerState().producers, expected);

    // Keep the metadata epoch, but seed a shorter divergent tail. The resumed
    // real Fetch crosses the leader's sparse batch and exercises kOverlap.
    brokers_[1]->replicaManager()->stop();
    {
        auto write_lock = brokers_[1]->lockPartitionWrites(tp);
        ASSERT_EQ(follower->truncateSuffix(0), 0);
        auto divergent = batchWithWireSpan(producerBatch(42, 0), 4);
        ASSERT_EQ(follower->appendReplicatedBatch(divergent),
                  storage::Log::ReplicaAppendResult::kAppended);
    }
    image = follower->inspectProducerState();
    ASSERT_EQ(image.producers.size(), 1u);
    EXPECT_EQ(image.producers[0].producer_id, 42);
    brokers_[1]->replicaManager()->start();
    ASSERT_TRUE(waitUntil([&] { return follower->logEndOffset() == 12; }));
    EXPECT_EQ(follower->inspectProducerState().producers, expected);

    // Destroy/reopen the actual follower, preserving its source directory.
    brokers_[1].reset();
    brokers_[1] = std::make_unique<broker::KawasanBroker>(configs_[1]);
    brokers_[1]->start();
    ASSERT_TRUE(waitUntil([&] {
        const auto current = brokers_[1]->replicaManager()->getLeader(tp);
        return current == 0 && brokers_[1]->logManager()->getLog(topic, 0) != nullptr;
    }));
    follower = brokers_[1]->logManager()->getLog(topic, 0);
    EXPECT_EQ(follower->inspectProducerState().producers, expected);
    EXPECT_FALSE(brokers_[1]->replicaManager()->isLeader(tp));
}
}  // namespace
