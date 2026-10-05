#include "kawasan/broker/group_state_manager.h"

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <iostream>

#include "../sparse_record_batch.h"
#include "kawasan/broker/coordinator_routing.h"
#include "kawasan/common/logger.h"
#include "kawasan/storage/log_manager.h"

namespace fs = std::filesystem;
using namespace kawasan;
using namespace kawasan::broker;

namespace {
GroupRecord group() {
    GroupRecord r;
    r.key.group_id = std::string("group:\0", 7);
    r.group = {3,          3,
               "consumer", "range",
               "member",   15000,
               123456,     {{"member", "client", "127.0.0.1", "instance", {0, 255}, {128, 0}}}};
    return r;
}
GroupRecord offset(bool pending = false) {
    auto r = group();
    r.key.kind = pending ? GroupRecordKey::Kind::PendingOffset : GroupRecordKey::Kind::Offset;
    r.key.topic = "topic:1";
    r.key.partition = 2;
    if (pending) {
        r.key.transactional_id = "transaction";
        r.key.producer_id = 42;
        r.key.producer_epoch = 3;
    }
    r.offset = {900, std::string("\xff\0checkpoint", 12), 123456, 999999, 7};
    return r;
}
void expectOffset(const OffsetManager::OffsetMetadata& a, const OffsetManager::OffsetMetadata& b) {
    EXPECT_EQ(a.offset, b.offset);
    EXPECT_EQ(a.metadata, b.metadata);
    EXPECT_EQ(a.commit_timestamp, b.commit_timestamp);
    EXPECT_EQ(a.expiry_timestamp, b.expiry_timestamp);
    EXPECT_EQ(a.committed_leader_epoch, b.committed_leader_epoch);
}
}  // namespace

class GroupStateManagerTest : public ::testing::Test {
protected:
    void SetUp() override {
        static const bool logging = [] {
            Logger::init("warn");
            return true;
        }();
        (void)logging;
        dir = fs::temp_directory_path() /
              ("kawasan-group-state-" +
               std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        fs::create_directories(dir);
    }
    void TearDown() override {
        if (!HasFailure())
            fs::remove_all(dir);
        else
            std::cerr << "Retained fixture: " << dir << '\n';
    }
    fs::path dir;
};

TEST_F(GroupStateManagerTest, CompleteIdentityOffsetsPendingEpochAndTombstonesRoundTrip) {
    for (auto r : {group(), offset(), offset(true)}) {
        auto decoded = GroupStateManager::decode(GroupStateManager::encode(r));
        EXPECT_EQ(decoded.key, r.key);
        EXPECT_FALSE(decoded.tombstone);
        if (r.key.kind == GroupRecordKey::Kind::Group)
            EXPECT_EQ(decoded.group, r.group);
        else
            expectOffset(decoded.offset, r.offset);
        r.tombstone = true;
        auto wire = GroupStateManager::encode(r);
        EXPECT_FALSE(wire.value);
        decoded = GroupStateManager::decode(wire);
        EXPECT_EQ(decoded.key, r.key);
        EXPECT_TRUE(decoded.tombstone);
    }
}

TEST_F(GroupStateManagerTest, CommittedReplayReconstructsIdentityAndPendingCheckpointsAfterReopen) {
    const auto p = coordinatorPartitionFor(group().key.group_id, 2);
    {
        storage::LogManager logs(dir.string());
        auto* log = logs.getOrCreateLog(GroupStateManager::kTopic, p);
        log->append({GroupStateManager::encode(group()), GroupStateManager::encode(offset()),
                     GroupStateManager::encode(offset(true))},
                    true);
        // An uncommitted malformed tail must not be decoded or change state.
        Record bad;
        bad.key = std::vector<uint8_t>{0};
        bad.value = std::vector<uint8_t>{0};
        storage::RecordBatch tail;
        tail.addRecord(bad);
        log->appendBatch(tail, false);
    }
    storage::LogManager logs(dir.string());
    auto* log = logs.getOrCreateLog(GroupStateManager::kTopic, p);
    ASSERT_EQ(log->highWatermark(), 3);
    GroupStateManager manager(&logs, 2);
    const auto image = manager.loadCommittedPartition(p);
    ASSERT_EQ(image.size(), 3u);
    EXPECT_EQ(image.at(group().key).group, group().group);
    expectOffset(image.at(offset().key).offset, offset().offset);
    expectOffset(image.at(offset(true).key).offset, offset(true).offset);
    EXPECT_EQ(logs.openLogCount(), 1u);
    EXPECT_THROW(manager.loadCommittedPartition(1 - p), std::exception);
}

TEST_F(GroupStateManagerTest, PendingEpochsHaveDistinctCompactionKeysAndExactTombstones) {
    storage::LogManager logs(dir.string());
    auto* log = logs.getOrCreateLog(GroupStateManager::kTopic, 0);
    auto newer = offset(true);
    newer.key.producer_epoch++;
    auto gone = offset(true);
    gone.tombstone = true;
    auto updated = offset();
    updated.offset.offset = 1000;
    log->append({GroupStateManager::encode(group()), GroupStateManager::encode(offset()),
                 GroupStateManager::encode(offset(true)), GroupStateManager::encode(newer),
                 GroupStateManager::encode(gone), GroupStateManager::encode(updated)},
                true);
    GroupStateManager manager(&logs, 1);
    auto image = manager.loadCommittedPartition(0);
    EXPECT_EQ(image.size(), 3u);
    EXPECT_FALSE(image.contains(gone.key));
    EXPECT_TRUE(image.contains(newer.key));
    EXPECT_EQ(image.at(offset().key).offset.offset, 1000);
    auto deleted_group = group();
    deleted_group.tombstone = true;
    updated.tombstone = true;
    newer.tombstone = true;
    log->append({GroupStateManager::encode(deleted_group), GroupStateManager::encode(updated),
                 GroupStateManager::encode(newer)},
                true);
    EXPECT_TRUE(manager.loadCommittedPartition(0).empty());
}

TEST_F(GroupStateManagerTest, SparseCommittedBatchesTraverseAssignedSpanAndRespectRecordDeltas) {
    storage::LogManager logs(dir.string());
    auto* log = logs.getOrCreateLog(GroupStateManager::kTopic, 0);
    auto r = GroupStateManager::encode(group());
    r.offset_delta = 5;
    storage::RecordBatch base;
    base.addRecord(r);
    auto batch = test_support::batchWithWireSpan(base, 9);
    ASSERT_EQ(log->appendReplicatedBatch(batch), storage::Log::ReplicaAppendResult::kAppended);
    log->setHighWatermark(6);
    GroupStateManager manager(&logs, 1);
    EXPECT_EQ(manager.loadCommittedPartition(0).size(), 1u);
    log->setHighWatermark(5);
    EXPECT_TRUE(manager.loadCommittedPartition(0).empty());
    log->setHighWatermark(10);
    log->append({GroupStateManager::encode(offset())}, true);
    EXPECT_EQ(manager.loadCommittedPartition(0).size(), 2u);
}

TEST_F(GroupStateManagerTest, MalformedCommittedRecordAndWrongRoutingFailWholeReplay) {
    storage::LogManager logs(dir.string());
    const int p = coordinatorPartitionFor(group().key.group_id, 2);
    auto* log = logs.getOrCreateLog(GroupStateManager::kTopic, 1 - p);
    log->append({GroupStateManager::encode(group())}, true);
    GroupStateManager manager(&logs, 2);
    EXPECT_THROW(manager.loadCommittedPartition(1 - p), std::exception);
    log = logs.getOrCreateLog(GroupStateManager::kTopic, p);
    Record bad;
    bad.key = std::vector<uint8_t>{0};
    bad.value = std::vector<uint8_t>{0};
    log->append({GroupStateManager::encode(group()), bad}, true);
    EXPECT_THROW(manager.loadCommittedPartition(p), std::exception);
}

TEST_F(GroupStateManagerTest, DecodingRejectsUnknownVersionsTruncationTrailingAndOversizedCounts) {
    auto valid = GroupStateManager::encode(group());
    for (size_t n = 0; n < valid.key->size(); ++n) {
        auto wire = valid;
        wire.key->resize(n);
        EXPECT_THROW(GroupStateManager::decode(wire), std::exception) << "key prefix " << n;
    }
    for (size_t n = 0; n < valid.value->size(); ++n) {
        auto wire = valid;
        wire.value->resize(n);
        EXPECT_THROW(GroupStateManager::decode(wire), std::exception) << "value prefix " << n;
    }
    auto wire = valid;
    wire.value->push_back(0);
    EXPECT_THROW(GroupStateManager::decode(wire), std::exception);
    wire = valid;
    wire.key->push_back(0);
    EXPECT_THROW(GroupStateManager::decode(wire), std::exception);
    wire = valid;
    (*wire.value)[0] = 99;
    EXPECT_THROW(GroupStateManager::decode(wire), std::exception);
    wire = valid;
    (*wire.key)[0] = 99;
    EXPECT_THROW(GroupStateManager::decode(wire), std::exception);
    auto invalid = group();
    invalid.group.members.push_back(invalid.group.members[0]);
    EXPECT_THROW(GroupStateManager::encode(invalid), std::exception);
    invalid = group();
    invalid.group.leader_id = "missing";
    EXPECT_THROW(GroupStateManager::encode(invalid), std::exception);
    invalid = offset(true);
    invalid.key.producer_epoch = -1;
    EXPECT_THROW(GroupStateManager::encode(invalid), std::exception);
    invalid = offset();
    invalid.key.group_id.clear();
    EXPECT_THROW(GroupStateManager::encode(invalid), std::exception);
}

TEST_F(GroupStateManagerTest, OversizedMemberCountAndFieldLengthAreBoundedBeforeAllocation) {
    auto wire = GroupStateManager::encode(group());
    // version + generation + state + three length-prefixed identity strings +
    // rebalance timeout + timestamp places the member count at this offset.
    const auto snapshot = group().group;
    const size_t count_offset = 1 + 4 + 1 + 12 + snapshot.protocol_type.size() +
                                snapshot.protocol_name.size() + snapshot.leader_id.size() + 4 + 8;
    for (const uint8_t byte : {uint8_t{0x7f}, uint8_t{0xff}}) {
        auto bad = wire;
        std::fill(bad.value->begin() + count_offset, bad.value->begin() + count_offset + 4, byte);
        EXPECT_THROW(GroupStateManager::decode(bad), std::exception);
    }
    std::fill(wire.key->begin() + 3, wire.key->begin() + 7, uint8_t{0x7f});
    EXPECT_THROW(GroupStateManager::decode(wire), std::exception);
}

TEST_F(GroupStateManagerTest, InvalidCommittedBatchAndUnavailableSourceFailClosed) {
    GroupStateManager absent(nullptr, 1);
    EXPECT_THROW(absent.loadCommittedPartition(0), std::exception);
    EXPECT_THROW(GroupStateManager(nullptr, 0), std::exception);
    storage::LogManager logs(dir.string());
    GroupStateManager manager(&logs, 1);
    EXPECT_THROW(manager.loadCommittedPartition(-1), std::exception);
    EXPECT_THROW(manager.loadCommittedPartition(1), std::exception);
    EXPECT_THROW(manager.loadCommittedPartition(0), std::exception);
    EXPECT_EQ(logs.openLogCount(), 0u);
    auto* log = logs.getOrCreateLog(GroupStateManager::kTopic, 0);
    storage::RecordBatch batch;
    batch.setProducerId(42);
    batch.setAttributes(1 << 4);
    batch.addRecord(GroupStateManager::encode(group()));
    log->appendBatch(batch, true);
    EXPECT_THROW(manager.loadCommittedPartition(0), std::exception);
}

TEST_F(GroupStateManagerTest, VersionOneGroupKeyAndEmptySnapshotHaveStableBytes) {
    auto r = group();
    r.group = {};
    const auto wire = GroupStateManager::encode(r);
    EXPECT_EQ(*wire.key,
              (std::vector<uint8_t>{'G', 1, 0, 0, 0, 0, 7, 'g', 'r', 'o', 'u', 'p', ':', 0}));
    std::vector<uint8_t> expected(34, 0);
    expected[0] = 1;
    EXPECT_EQ(*wire.value, expected);
}
