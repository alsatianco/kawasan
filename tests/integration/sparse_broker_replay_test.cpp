#include <gtest/gtest.h>

#include <chrono>
#include <climits>
#include <filesystem>
#include <iostream>
#include <memory>

#include "../sparse_broker_test_client.h"
#include "kawasan/broker/kawasan_broker.h"
#include "kawasan/broker/metadata_controller.h"
#include "kawasan/broker/producer_state_snapshot.h"
#include "kawasan/broker/transaction_coordinator.h"
#include "kawasan/broker/transaction_state_manager.h"
#include "kawasan/common/config.h"
#include "kawasan/common/logger.h"
#include "kawasan/protocol/list_offsets_request.h"

namespace {
using namespace kawasan;
using namespace kawasan::test_support;
namespace fs = std::filesystem;
constexpr const char* kTopic = "sparse-replay";

class SparseBrokerReplayTest : public ::testing::Test {
protected:
    void SetUp() override {
        static const bool logger = [] {
            Logger::init("warn");
            return true;
        }();
        (void)logger;
        directory_ = fs::temp_directory_path() /
                     ("kawasan-sparse-broker-" +
                      std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        config_.setInt("broker.id", 0);
        config_.setString("host", "127.0.0.1");
        config_.setInt("port", 0);
        config_.setInt("raft.port", 0);
        config_.setBool("monitoring.enabled", false);
        config_.setInt("monitoring.port", 0);
        config_.setInt("network.io_threads", 1);
        config_.setInt("offsets.topic.num.partitions", 1);
        config_.setInt("transaction.state.topic.num.partitions", 1);
        config_.setString("log.dirs", directory_.string());
        start();
        broker::TopicSpecification spec;
        spec.name = kTopic;
        spec.num_partitions = 1;
        spec.replication_factor = 1;
        spec.configs = {{"cleanup.policy", "compact"}};
        ASSERT_EQ(broker_->metadataController()->createTopic(spec).error_code, ErrorCode::NONE);
    }
    void TearDown() override {
        broker_.reset();
        if (HasFailure())
            std::cerr << "Retained sparse broker data: " << directory_ << '\n';
        else
            fs::remove_all(directory_);
    }
    void start() {
        broker_ = std::make_unique<broker::KawasanBroker>(config_);
        broker_->start();
    }
    void restart() {
        broker_.reset();
        // Direct storage seeding deliberately bypasses the producer cache. No
        // snapshot is written; verify that restart must use production log replay.
        for (const auto& file : fs::recursive_directory_iterator(directory_))
            ASSERT_NE(file.path().extension(), ".psnap");
        start();
    }
    storage::Log* log() { return broker_->logManager()->getOrCreateLog(kTopic, 0); }
    void seed(const storage::RecordBatch& batch) {
        ASSERT_EQ(log()->appendReplicatedBatch(batch),
                  storage::Log::ReplicaAppendResult::kAppended);
    }
    fs::path directory_;
    Config config_;
    std::unique_ptr<broker::KawasanBroker> broker_;
};

TEST_F(SparseBrokerReplayTest, RestartRestoresAssignedSequenceSpan) {
    seed(sparseProducerBatch());
    restart();
    ASSERT_EQ(log()->logEndOffset(), 10);
    auto next = brokerProduce(broker_->port(), kTopic, producerBatch(99, 10));
    EXPECT_EQ(next.error_code, ErrorCode::NONE);
    EXPECT_EQ(next.base_offset, 10);
    EXPECT_EQ(log()->logEndOffset(), 11);
}

TEST_F(SparseBrokerReplayTest, RestartRestoresProducerHeaderWhenAllRecordsWereCompacted) {
    storage::RecordBatch empty;
    empty.setProducerId(99);
    empty.setProducerEpoch(0);
    empty.setBaseSequence(0);
    seed(batchWithWireSpan(empty, 9));
    restart();
    EXPECT_EQ(brokerLastSequence(broker_->port(), kTopic), 9);
    const auto retry = brokerProduce(broker_->port(), kTopic, producerBatch(99, 0));
    EXPECT_EQ(retry.base_offset, 0);
    EXPECT_EQ(log()->logEndOffset(), 10);
}

TEST_F(SparseBrokerReplayTest, LiveSparseAppendAndSnapshotKeepTheOriginalSequenceRange) {
    const auto first = brokerProduce(broker_->port(), kTopic, sparseProducerBatch());
    ASSERT_EQ(first.error_code, ErrorCode::NONE);
    EXPECT_EQ(log()->logEndOffset(), 10);
    EXPECT_EQ(brokerLastSequence(broker_->port(), kTopic), 9);
    broker_.reset();
    const auto snapshot =
        broker::ProducerStateSnapshot::loadNewest((directory_ / "sparse-replay-0").string());
    ASSERT_TRUE(snapshot.has_value());
    ASSERT_EQ(snapshot->entries.size(), 1u);
    EXPECT_EQ(snapshot->snapshot_offset, 10);
    EXPECT_EQ(snapshot->entries.front().last_sequence, 9);
    EXPECT_EQ(snapshot->entries.front().last_record_count, 2);
    start();
    const auto next = brokerProduce(broker_->port(), kTopic, producerBatch(99, 10));
    EXPECT_EQ(next.error_code, ErrorCode::NONE);
    EXPECT_EQ(next.base_offset, 10);
}

TEST_F(SparseBrokerReplayTest, RestartReplaysSparseTailAfterRealProducerSnapshot) {
    ASSERT_EQ(brokerProduce(broker_->port(), kTopic, producerBatch(99, 0)).error_code,
              ErrorCode::NONE);
    broker_.reset();
    const auto partition_dir = directory_ / "sparse-replay-0";
    const auto snapshot = broker::ProducerStateSnapshot::loadNewest(partition_dir.string());
    ASSERT_TRUE(snapshot.has_value());
    ASSERT_EQ(snapshot->snapshot_offset, 1);
    {
        storage::Log closed(kTopic, 0, partition_dir.string());
        auto batch = sparseProducerBatch(1, 1);
        batch.setBaseOffset(1);
        ASSERT_EQ(closed.appendReplicatedBatch(batch),
                  storage::Log::ReplicaAppendResult::kAppended);
    }
    start();
    const auto next = brokerProduce(broker_->port(), kTopic, producerBatch(99, 11));
    EXPECT_EQ(next.error_code, ErrorCode::NONE);
    EXPECT_EQ(next.base_offset, 11);
}

TEST_F(SparseBrokerReplayTest, RestartTraversesLargeSparseBatchAndIgnoresMarkers) {
    seed(sparseProducerBatch(8 * 1024 * 1024));
    auto tail = producerBatch(100, 0);
    tail.setBaseOffset(10);
    seed(tail);
    seed(storage::RecordBatch::makeControlBatch(100, 0, 11, true, 0));
    restart();
    const auto retry = brokerProduce(broker_->port(), kTopic, producerBatch(100, 0));
    EXPECT_EQ(retry.error_code, ErrorCode::NONE);
    EXPECT_EQ(retry.base_offset, 10);
    EXPECT_EQ(log()->logEndOffset(), 12) << "Retry must not append a duplicate after restart";
    const auto next = brokerProduce(broker_->port(), kTopic, producerBatch(100, 1));
    EXPECT_EQ(next.error_code, ErrorCode::NONE);
    EXPECT_EQ(next.base_offset, 12);
}

TEST_F(SparseBrokerReplayTest, RestartRestoresSequenceWrappingInsideSparseBatch) {
    seed(sparseProducerBatch(1, INT32_MAX - 4));
    restart();
    const auto next = brokerProduce(broker_->port(), kTopic, producerBatch(99, 5));
    EXPECT_EQ(next.error_code, ErrorCode::NONE);
    EXPECT_EQ(next.base_offset, 10);
}

TEST_F(SparseBrokerReplayTest, RestartRestoresMaximumOffsetDelta) {
    // A legal span of INT32_MAX+1 must not narrow to a negative record count.
    seed(sparseProducerBatch(1, 0, INT32_MAX));
    restart();
    EXPECT_EQ(brokerLastSequence(broker_->port(), kTopic), INT32_MAX);
    EXPECT_EQ(log()->logEndOffset(), static_cast<Offset>(INT32_MAX) + 1);
}

TEST_F(SparseBrokerReplayTest, RestartAcceptsSequenceZeroAfterMaximumSequence) {
    seed(sparseProducerBatch(1, INT32_MAX - 9));
    restart();
    const auto next = brokerProduce(broker_->port(), kTopic, producerBatch(99, 0));
    EXPECT_EQ(next.error_code, ErrorCode::NONE);
    EXPECT_EQ(next.base_offset, 10);
    EXPECT_EQ(log()->logEndOffset(), 11);
}

TEST_F(SparseBrokerReplayTest, TimestampScanTraversesSparseChunkBoundary) {
    seed(sparseProducerBatch(64 * 1024));
    auto tail = producerBatch(100, 0);
    tail.setBaseOffset(10);
    tail.setFirstTimestamp(100);
    tail.setMaxTimestamp(100);
    seed(tail);
    Buffer payload;
    protocol::RequestHeader(protocol::ApiKey::LIST_OFFSETS, 5, 1, "sparse-replay").encode(payload);
    protocol::ListOffsetsRequest request;
    protocol::ListOffsetsPartition partition;
    partition.partition = 0;
    partition.timestamp = 100;
    protocol::ListOffsetsTopic topic;
    topic.topic = kTopic;
    topic.partitions.push_back(partition);
    request.addTopic(topic);
    request.encode(payload, 5);
    auto body = brokerRequest(broker_->port(), payload);
    protocol::ResponseHeader header;
    header.decode(body);
    protocol::ListOffsetsResponse response;
    response.decode(body, 5);
    const auto& result = response.topics().at(0).partitions.at(0);
    EXPECT_EQ(result.error_code, ErrorCode::NONE);
    EXPECT_EQ(result.offset, 10);
    EXPECT_EQ(result.timestamp, 100);
}

class SparseMarkerRecoveryTest : public SparseBrokerReplayTest,
                                 public ::testing::WithParamInterface<bool> {};

TEST_P(SparseMarkerRecoveryTest, PrepareRecoveryFindsDurableMarkerAfterSparseBatch) {
    auto batch = sparseProducerBatch(4 * 1024 * 1024);
    batch.setAttributes(1 << 4);
    seed(batch);
    seed(storage::RecordBatch::makeControlBatch(99, 0, 10, GetParam(), 0));
    broker::TransactionCoordinator coordinator;
    coordinator.recordInitProducerId("prepared", 99, 0, 60000);
    coordinator.addPartitions("prepared", {{kTopic, 0, 0}});
    if (GetParam())
        coordinator.prepareCommit("prepared");
    else
        coordinator.prepareAbort("prepared");
    {
        broker::TransactionStateManager state(broker_->logManager(), 1);
        state.persist(*coordinator.describe("prepared"));
    }
    restart();
    EXPECT_EQ(log()->logEndOffset(), 11) << "Recovery must not emit a second durable marker";
    const auto batches = log()->read(10, 4096);
    ASSERT_EQ(batches.size(), 1u);
    EXPECT_TRUE(batches.front().isControlBatch());
    broker::TransactionStateManager recovered(broker_->logManager(), 1);
    const auto snapshots = recovered.loadAll();
    ASSERT_EQ(snapshots.size(), 1u);
    EXPECT_EQ(snapshots.front().state, GetParam()
                                           ? broker::TransactionCoordinator::State::CompleteCommit
                                           : broker::TransactionCoordinator::State::CompleteAbort);
}

INSTANTIATE_TEST_SUITE_P(CommitAndAbort, SparseMarkerRecoveryTest, ::testing::Bool());
}  // namespace
