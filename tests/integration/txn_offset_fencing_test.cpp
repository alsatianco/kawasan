#include <gtest/gtest.h>

#include <array>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <memory>

#include "kawasan/broker/kawasan_broker.h"
#include "kawasan/common/logger.h"
#include "kawasan/common/socket_deadline.h"
#include "kawasan/protocol/create_topics_request.h"
#include "kawasan/protocol/join_group_request.h"
#include "kawasan/protocol/offset_fetch_request.h"
#include "kawasan/protocol/sync_group_request.h"
#include "kawasan/protocol/txn_request.h"

using namespace kawasan;
using namespace kawasan::protocol;

namespace {
class TxnOffsetFencingTest : public ::testing::Test {
protected:
    struct ProducerIdentity {
        int64_t id;
        int16_t epoch;
    };
    void SetUp() override {
        static const bool logging = [] {
            Logger::init("warn");
            return true;
        }();
        (void)logging;
        dir_ = std::filesystem::temp_directory_path() /
               ("kawasan-txn-offset-fence-" +
                std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        Config config;
        config.setInt("broker.id", 0);
        config.setString("host", "127.0.0.1");
        config.setInt("port", 0);
        config.setInt("raft.port", 0);
        config.setString("log.dirs", dir_.string());
        config.setBool("monitoring.enabled", false);
        config.setInt("network.io_threads", 4);
        broker_ = std::make_unique<broker::KawasanBroker>(config);
        broker_->start();
        CreateTopicsRequest create;
        CreatableTopic topic;
        topic.name = "cm4-input";
        topic.num_partitions = 1;
        topic.replication_factor = 1;
        create.addTopic(topic);
        auto body = typed(ApiKey::CREATE_TOPICS, 4, create);
        CreateTopicsResponse response;
        response.decode(body, 4);
        ASSERT_EQ(response.results()[0].error_code, ErrorCode::NONE);
    }
    void TearDown() override {
        broker_.reset();
        if (HasFailure()) {
            std::cerr << "Retained broker data: " << dir_ << '\n';
            return;
        }
        std::filesystem::remove_all(dir_);
    }
    Buffer call(ApiKey key, int16_t version, const Buffer& body) {
        boost::asio::io_context io;
        boost::asio::ip::tcp::socket socket(io);
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        connectWithDeadline(socket, "127.0.0.1", broker_->port(), deadline);
        RequestHeader header(key, version, ++correlation_, "cm4-fence");
        Buffer payload;
        header.encode(payload);
        payload.writeBytes(body.data(), body.size());
        Buffer frame;
        frame.writeInt32(static_cast<int32_t>(payload.size()));
        frame.writeBytes(payload.data(), payload.size());
        auto bytes = frame.takeVector();
        transferWithDeadline(socket, true, bytes.data(), bytes.size(), deadline);
        std::array<uint8_t, 4> length_bytes{};
        transferWithDeadline(socket, false, length_bytes.data(), length_bytes.size(), deadline);
        Buffer length(std::vector<uint8_t>(length_bytes.begin(), length_bytes.end()));
        const auto size = length.readInt32();
        if (size < 0 || size > 1024 * 1024)
            throw std::runtime_error("invalid response frame");
        std::vector<uint8_t> response_bytes(size);
        transferWithDeadline(socket, false, response_bytes.data(), response_bytes.size(), deadline);
        Buffer response(response_bytes);
        ResponseHeader response_header(0, header.isFlexibleResponseHeader());
        response_header.decode(response);
        EXPECT_EQ(response_header.correlationId(), correlation_);
        return Buffer(response.readBytes(response.remaining()));
    }
    template <class Request>
    Buffer typed(ApiKey key, int16_t version, const Request& request) {
        Buffer body;
        request.encode(body, version);
        return call(key, version, body);
    }
    JoinGroupResponse join(const std::string& group,
                           std::optional<std::string> instance = std::nullopt) {
        JoinGroupRequest request;
        request.setGroupId(group);
        request.setSessionTimeoutMs(60000);
        request.setProtocolType("consumer");
        request.setGroupInstanceId(instance);
        Buffer metadata;
        metadata.writeInt16(0);
        metadata.writeInt32(1);
        metadata.writeString("cm4-input");
        metadata.writeInt32(-1);
        request.setGroupProtocols({{"range", metadata.vector()}});
        auto body = typed(ApiKey::JOIN_GROUP, 5, request);
        JoinGroupResponse joined;
        joined.decode(body, 5);
        EXPECT_EQ(joined.errorCode(), ErrorCode::NONE);
        SyncGroupRequest sync;
        sync.setGroupId(group);
        sync.setGenerationId(joined.generationId());
        sync.setMemberId(joined.memberId());
        sync.setGroupInstanceId(instance);
        Buffer assignment;
        assignment.writeInt16(0);
        assignment.writeInt32(1);
        assignment.writeString("cm4-input");
        assignment.writeInt32(1);
        assignment.writeInt32(0);
        assignment.writeInt32(-1);
        sync.setAssignments({{joined.memberId(), assignment.vector()}});
        auto synced = typed(ApiKey::SYNC_GROUP, 3, sync);
        SyncGroupResponse result;
        result.decode(synced, 3);
        EXPECT_EQ(result.errorCode(), ErrorCode::NONE);
        return joined;
    }
    ProducerIdentity init(const std::string& txn) {
        Buffer body;
        body.writeCompactNullableString(txn);
        body.writeInt32(60000);
        body.writeInt64(-1);
        body.writeInt16(-1);
        body.writeEmptyTaggedFields();
        auto response = call(ApiKey::INIT_PRODUCER_ID, 4, body);
        response.readInt32();
        EXPECT_EQ(response.readInt16(), 0);
        ProducerIdentity identity{response.readInt64(), response.readInt16()};
        response.skipTaggedFields();
        EXPECT_EQ(response.remaining(), 0u);
        return identity;
    }
    ErrorCode addOffsets(const std::string& txn, ProducerIdentity identity,
                         const std::string& group, int16_t version = 0) {
        Buffer body;
        if (version >= 3)
            body.writeCompactString(txn);
        else
            body.writeString(txn);
        body.writeInt64(identity.id);
        body.writeInt16(identity.epoch);
        if (version >= 3) {
            body.writeCompactString(group);
            body.writeEmptyTaggedFields();
        } else
            body.writeString(group);
        auto response = call(ApiKey::ADD_OFFSETS_TO_TXN, version, body);
        response.readInt32();
        return static_cast<ErrorCode>(response.readInt16());
    }
    ErrorCode end(const std::string& txn, ProducerIdentity identity, bool commit,
                  int16_t version = 0) {
        Buffer body;
        if (version >= 3)
            body.writeCompactString(txn);
        else
            body.writeString(txn);
        body.writeInt64(identity.id);
        body.writeInt16(identity.epoch);
        body.writeInt8(commit);
        if (version >= 3)
            body.writeEmptyTaggedFields();
        auto response = call(ApiKey::END_TXN, version, body);
        response.readInt32();
        return static_cast<ErrorCode>(response.readInt16());
    }
    TxnOffsetCommitRequest request(const std::string& txn, ProducerIdentity identity,
                                   const std::string& group, const JoinGroupResponse& member,
                                   std::optional<std::string> instance = std::nullopt,
                                   int64_t offset = 7) {
        TxnOffsetCommitRequest result;
        result.setTransactionalId(txn);
        result.setProducerId(identity.id);
        result.setProducerEpoch(identity.epoch);
        result.setGroupId(group);
        result.setGenerationId(member.generationId());
        result.setMemberId(member.memberId());
        result.setGroupInstanceId(instance);
        result.addTopic({"cm4-input", {{0, offset, "cm4-checkpoint", 3}}});
        return result;
    }
    ErrorCode stage(const TxnOffsetCommitRequest& request) {
        auto response = typed(ApiKey::TXN_OFFSET_COMMIT, 3, request);
        response.readInt32();
        EXPECT_EQ(response.readCompactArrayLen(), 1);
        EXPECT_EQ(response.readCompactString(), "cm4-input");
        EXPECT_EQ(response.readCompactArrayLen(), 1);
        EXPECT_EQ(response.readInt32(), 0);
        const auto error = static_cast<ErrorCode>(response.readInt16());
        response.skipTaggedFields();
        response.skipTaggedFields();
        response.skipTaggedFields();
        EXPECT_EQ(response.remaining(), 0u);
        return error;
    }
    OffsetFetchResponse::Partition fetched(const std::string& group, bool require_stable = false) {
        OffsetFetchRequest request;
        OffsetFetchRequest::Group queried;
        queried.group_id = group;
        queried.topics.push_back({"cm4-input", {{0}}});
        request.addGroup(queried);
        request.setRequireStable(require_stable);
        auto body = typed(ApiKey::OFFSET_FETCH, 9, request);
        OffsetFetchResponse response;
        response.decode(body, 9);
        EXPECT_EQ(response.groups().size(), 1u);
        return response.groups().at(0).topics.at(0).partitions.at(0);
    }
    std::vector<OffsetFetchResponse::Topic> fetchedAll(const std::string& group,
                                                       int16_t version = 9,
                                                       bool require_stable = false) {
        OffsetFetchRequest request;
        request.setRequireStable(require_stable);
        if (version >= 8) {
            OffsetFetchRequest::Group queried;
            queried.group_id = group;
            queried.fetch_all_topics = true;
            request.addGroup(queried);
        } else {
            request.setGroupId(group);
            request.setFetchAllTopics(true);
        }
        auto body = typed(ApiKey::OFFSET_FETCH, version, request);
        OffsetFetchResponse response;
        response.decode(body, version);
        EXPECT_EQ(body.remaining(), 0u);
        if (version >= 8) {
            EXPECT_EQ(response.groups().at(0).error_code, ErrorCode::NONE);
            return response.groups().at(0).topics;
        }
        EXPECT_EQ(response.errorCode(), ErrorCode::NONE);
        return response.topics();
    }
    std::filesystem::path dir_;
    std::unique_ptr<broker::KawasanBroker> broker_;
    int32_t correlation_ = 0;
};
}  // namespace

TEST_F(TxnOffsetFencingTest, CommitUsesCurrentGenerationAndAbortPreservesOffsets) {
    const auto joined = join("cm4-group");
    const auto identity = init("cm4-txn");
    ASSERT_EQ(addOffsets("cm4-txn", identity, "cm4-group"), ErrorCode::NONE);
    ASSERT_EQ(stage(request("cm4-txn", identity, "cm4-group", joined)), ErrorCode::NONE);
    EXPECT_EQ(fetched("cm4-group").offset, -1);
    ASSERT_EQ(end("cm4-txn", identity, true, 3), ErrorCode::NONE);
    EXPECT_EQ(fetched("cm4-group").offset, 7);
    EXPECT_EQ(fetched("cm4-group").metadata, "cm4-checkpoint");
    // Offset leaders/epochs must survive the entire state/cache path, not just decoding.
    EXPECT_EQ(fetched("cm4-group").committed_leader_epoch, 3);
    ASSERT_EQ(addOffsets("cm4-txn", identity, "cm4-group", 3), ErrorCode::NONE);
    auto aborted = request("cm4-txn", identity, "cm4-group", joined, std::nullopt, 9);
    aborted.setGenerationId(joined.generationId());
    ASSERT_EQ(stage(aborted), ErrorCode::NONE);
    ASSERT_EQ(end("cm4-txn", identity, false, 3), ErrorCode::NONE);
    EXPECT_EQ(fetched("cm4-group").offset, 7);
}

TEST_F(TxnOffsetFencingTest, InvalidMembershipCannotStageOffsets) {
    const auto joined = join("cm4-static-group", "cm4-instance");
    const auto identity = init("cm4-static-txn");
    ASSERT_EQ(addOffsets("cm4-static-txn", identity, "cm4-static-group"), ErrorCode::NONE);
    auto changed = request("cm4-static-txn", identity, "cm4-static-group", joined, "cm4-instance");
    changed.setGenerationId(joined.generationId() + 1);
    EXPECT_EQ(stage(changed), ErrorCode::ILLEGAL_GENERATION);
    changed.setMemberId("stale-member");
    EXPECT_EQ(stage(changed), ErrorCode::FENCED_INSTANCE_ID);
    changed.setMemberId(joined.memberId());
    changed.setGroupInstanceId("missing-instance");
    EXPECT_EQ(stage(changed), ErrorCode::UNKNOWN_MEMBER_ID);
    changed.setGroupInstanceId(std::nullopt);
    changed.setMemberId("missing-member");
    EXPECT_EQ(stage(changed), ErrorCode::UNKNOWN_MEMBER_ID);
    ASSERT_EQ(end("cm4-static-txn", identity, true), ErrorCode::NONE);
    EXPECT_EQ(fetched("cm4-static-group").offset, -1);
}

TEST_F(TxnOffsetFencingTest, ModernOffsetCommitKeepsProducerFencing) {
    const auto joined = join("cm4-epoch-group");
    const auto old = init("cm4-epoch-txn");
    ASSERT_EQ(addOffsets("cm4-epoch-txn", old, "cm4-epoch-group"), ErrorCode::NONE);
    const auto current = init("cm4-epoch-txn");
    ASSERT_EQ(current.id, old.id);
    ASSERT_GT(current.epoch, old.epoch);
    ASSERT_EQ(addOffsets("cm4-epoch-txn", current, "cm4-epoch-group"), ErrorCode::NONE);
    EXPECT_EQ(stage(request("cm4-epoch-txn", old, "cm4-epoch-group", joined)),
              ErrorCode::INVALID_PRODUCER_EPOCH);
    auto changed = request("cm4-epoch-txn", current, "cm4-epoch-group", joined);
    changed.setProducerId(current.id + 1000);
    EXPECT_EQ(stage(changed), ErrorCode::INVALID_PRODUCER_ID_MAPPING);
    ASSERT_EQ(stage(request("cm4-epoch-txn", current, "cm4-epoch-group", joined)), ErrorCode::NONE);
    ASSERT_EQ(end("cm4-epoch-txn", current, true), ErrorCode::NONE);
    EXPECT_EQ(fetched("cm4-epoch-group").offset, 7);
}

TEST_F(TxnOffsetFencingTest, StableOffsetFetchRejectsPendingCommitUntilResolved) {
    const auto joined = join("cm4-stable-group");
    const auto identity = init("cm4-stable-txn");
    ASSERT_EQ(addOffsets("cm4-stable-txn", identity, "cm4-stable-group", 3), ErrorCode::NONE);
    ASSERT_EQ(stage(request("cm4-stable-txn", identity, "cm4-stable-group", joined)),
              ErrorCode::NONE);
    auto pending = fetched("cm4-stable-group", true);
    EXPECT_EQ(pending.error, ErrorCode::UNSTABLE_OFFSET_COMMIT);
    EXPECT_EQ(pending.offset, -1);
    ASSERT_EQ(end("cm4-stable-txn", identity, true, 3), ErrorCode::NONE);
    EXPECT_EQ(fetched("cm4-stable-group", true).offset, 7);
    ASSERT_EQ(addOffsets("cm4-stable-txn", identity, "cm4-stable-group", 3), ErrorCode::NONE);
    ASSERT_EQ(
        stage(request("cm4-stable-txn", identity, "cm4-stable-group", joined, std::nullopt, 9)),
        ErrorCode::NONE);
    EXPECT_EQ(fetched("cm4-stable-group").offset, 7);
    pending = fetched("cm4-stable-group", true);
    EXPECT_EQ(pending.error, ErrorCode::UNSTABLE_OFFSET_COMMIT);
    EXPECT_EQ(pending.offset, -1);
    EXPECT_EQ(pending.committed_leader_epoch, -1);
    EXPECT_TRUE(pending.metadata.empty());
    const auto all = fetchedAll("cm4-stable-group", 9, true);
    ASSERT_EQ(all.size(), 1u);
    EXPECT_EQ(all[0].partitions[0].error, ErrorCode::UNSTABLE_OFFSET_COMMIT);
    ASSERT_EQ(end("cm4-stable-txn", identity, false, 3), ErrorCode::NONE);
    const auto stable = fetched("cm4-stable-group", true);
    EXPECT_EQ(stable.error, ErrorCode::NONE);
    EXPECT_EQ(stable.offset, 7);
    EXPECT_EQ(stable.committed_leader_epoch, 3);
}

TEST_F(TxnOffsetFencingTest, FetchAllOffsetsUsesExactGroupWithColonAndPreservesMetadata) {
    const auto joined = join("cm4:group");
    const auto identity = init("cm4-all-txn");
    ASSERT_EQ(addOffsets("cm4-all-txn", identity, "cm4:group", 3), ErrorCode::NONE);
    ASSERT_EQ(stage(request("cm4-all-txn", identity, "cm4:group", joined)), ErrorCode::NONE);
    ASSERT_EQ(end("cm4-all-txn", identity, true, 3), ErrorCode::NONE);
    for (int16_t version : {2, 7, 9}) {
        SCOPED_TRACE(version);
        const auto all = fetchedAll("cm4:group", version);
        ASSERT_EQ(all.size(), 1u);
        EXPECT_EQ(all[0].topic, "cm4-input");
        ASSERT_EQ(all[0].partitions.size(), 1u);
        EXPECT_EQ(all[0].partitions[0].offset, 7);
        EXPECT_EQ(all[0].partitions[0].metadata, "cm4-checkpoint");
        if (version >= 5)
            EXPECT_EQ(all[0].partitions[0].committed_leader_epoch, 3);
    }
}
