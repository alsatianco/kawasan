#include <gtest/gtest.h>

#include <array>
#include <chrono>
#include <filesystem>
#include <future>
#include <map>
#include <memory>
#include <thread>

#include "kawasan/broker/kawasan_broker.h"
#include "kawasan/client/producer.h"
#include "kawasan/common/logger.h"
#include "kawasan/common/socket_deadline.h"
#include "kawasan/protocol/admin_misc_requests.h"
#include "kawasan/protocol/create_topics_request.h"
#include "kawasan/protocol/fetch_request.h"
#include "kawasan/protocol/list_offsets_request.h"
#include "kawasan/protocol/offset_fetch_request.h"
#include "kawasan/protocol/produce_request.h"
#include "kawasan/protocol/sasl_request.h"

using namespace kawasan;
using namespace kawasan::protocol;

namespace {
class ClientApiParityTest : public ::testing::Test {
protected:
    void SetUp() override {
        static const bool logging = [] {
            Logger::init("info");
            return true;
        }();
        (void)logging;
        dir_ = std::filesystem::temp_directory_path() /
               ("kawasan-api-parity-" +
                std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        config_.setInt("broker.id", 0);
        config_.setString("host", "127.0.0.1");
        config_.setInt("port", 0);
        config_.setInt("raft.port", 0);
        config_.setBool("monitoring.enabled", false);
        config_.setString("log.dirs", dir_.string());
        config_.setInt("network.io_threads", 4);
    }
    void TearDown() override {
        broker_.reset();
        std::filesystem::remove_all(dir_);
    }
    void start(const std::string& profile = "4.x") {
        config_.setString("compatibility.max.api.version.profile", profile);
        broker_ = std::make_unique<broker::KawasanBroker>(config_);
        broker_->start();
    }
    template <class Request>
    Buffer call(ApiKey key, int16_t version, const Request& request) {
        boost::asio::io_context io;
        boost::asio::ip::tcp::socket socket(io);
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        connectWithDeadline(socket, "127.0.0.1", broker_->port(), deadline);
        RequestHeader header(key, version, 81, "cm3-wire");
        Buffer payload;
        header.encode(payload);
        request.encode(payload, version);
        Buffer frame;
        frame.writeInt32(static_cast<int32_t>(payload.size()));
        frame.writeBytes(payload.data(), payload.size());
        auto bytes = frame.takeVector();
        transferWithDeadline(socket, true, bytes.data(), bytes.size(), deadline);
        std::array<uint8_t, 4> length_bytes{};
        transferWithDeadline(socket, false, length_bytes.data(), length_bytes.size(), deadline);
        Buffer length(std::vector<uint8_t>(length_bytes.begin(), length_bytes.end()));
        const auto size = length.readInt32();
        if (size < 0 || size > 64 * 1024 * 1024)
            throw std::runtime_error("invalid response length");
        std::vector<uint8_t> response_bytes(size);
        transferWithDeadline(socket, false, response_bytes.data(), response_bytes.size(), deadline);
        Buffer response(response_bytes);
        ResponseHeader response_header(0, header.isFlexibleResponseHeader());
        response_header.decode(response);
        if (response_header.correlationId() != 81)
            throw std::runtime_error("wrong correlation ID");
        return Buffer(response.readBytes(response.remaining()));
    }
    void create(const std::string& name) {
        CreateTopicsRequest request;
        request.setTimeoutMs(5000);
        CreatableTopic topic;
        topic.name = name;
        topic.num_partitions = 1;
        topic.replication_factor = 1;
        request.addTopic(topic);
        auto body = call(ApiKey::CREATE_TOPICS, 4, request);
        CreateTopicsResponse response;
        response.decode(body, 4);
        ASSERT_EQ(response.results().size(), 1u);
        ASSERT_EQ(response.results()[0].error_code, ErrorCode::NONE);
    }
    FetchRequest fetch(const std::array<uint8_t, 16>& id, int32_t wait = 0) {
        FetchRequest request;
        request.setMaxWaitMs(wait);
        request.setMinBytes(1);
        request.setMaxBytes(4096);
        FetchTopic topic;
        topic.topic_id = id;
        topic.has_topic_id = true;
        FetchPartition partition{};
        partition.partition = 0;
        partition.fetch_offset = 0;
        partition.partition_max_bytes = 4096;
        topic.partitions.push_back(partition);
        request.addTopic(topic);
        return request;
    }
    void produce(const std::string& topic) {
        client::ProducerConfig config;
        config.bootstrap_servers = "127.0.0.1:" + std::to_string(broker_->port());
        client::Producer producer(config);
        auto result = producer.send(topic, "", "hello").get();
        EXPECT_EQ(result.offset, 0);
    }
    std::map<ApiKey, int16_t> versions() {
        auto body = call(ApiKey::API_VERSIONS, 0, ApiVersionsRequest{});
        ApiVersionsResponse response;
        response.decode(body, 0);
        std::map<ApiKey, int16_t> result;
        for (const auto& entry : response.apiVersions())
            result[entry.api_key] = entry.max_version;
        return result;
    }
    std::filesystem::path dir_;
    Config config_;
    std::unique_ptr<broker::KawasanBroker> broker_;
};
}  // namespace

TEST_F(ClientApiParityTest, ModernProfileAdvertisesCoreParity) {
    start();
    const auto actual = versions();
    for (const auto& [key, maximum] : std::map<ApiKey, int16_t>{{ApiKey::PRODUCE, 11},
                                                                {ApiKey::FETCH, 13},
                                                                {ApiKey::LIST_OFFSETS, 8},
                                                                {ApiKey::OFFSET_FETCH, 9},
                                                                {ApiKey::DELETE_RECORDS, 2},
                                                                {ApiKey::SASL_AUTHENTICATE, 2}}) {
        EXPECT_EQ(actual.at(key), maximum) << static_cast<int>(key);
    }
}

TEST_F(ClientApiParityTest, InvalidProfileFailsBeforeStartingServices) {
    config_.setString("compatibility.max.api.version.profile", "typo");
    EXPECT_THROW(broker::KawasanBroker instance(config_), std::invalid_argument);
}

TEST_F(ClientApiParityTest, UnknownUuidReturnsUnknownTopicIdWithoutAutoCreation) {
    start();
    const auto before = broker_->metadataController()->describeTopics({}).size();
    auto body = call(ApiKey::FETCH, 13, fetch({}));
    FetchResponse response;
    response.decode(body, 13);
    ASSERT_EQ(response.topics().size(), 1u);
    ASSERT_EQ(response.topics()[0].partitions.size(), 1u);
    EXPECT_EQ(static_cast<int16_t>(response.topics()[0].partitions[0].error_code), 100);
    EXPECT_EQ(broker_->metadataController()->describeTopics({}).size(), before);
}

TEST_F(ClientApiParityTest, UuidLongPollWakesOnResolvedPartitionAndReturnsRecords) {
    start();
    create("uuid-wakeup");
    const auto id = broker_->metadataController()->describeTopics({"uuid-wakeup"}).at(0).topic_id;
    auto pending =
        std::async(std::launch::async, [&] { return call(ApiKey::FETCH, 13, fetch(id, 3000)); });
    EXPECT_EQ(pending.wait_for(std::chrono::milliseconds(100)), std::future_status::timeout);
    produce("uuid-wakeup");
    ASSERT_EQ(pending.wait_for(std::chrono::seconds(1)), std::future_status::ready);
    auto body = pending.get();
    FetchResponse response;
    response.decode(body, 13);
    ASSERT_EQ(response.topics().size(), 1u);
    EXPECT_EQ(response.topics()[0].topic_id, id);
    ASSERT_EQ(response.topics()[0].partitions.size(), 1u);
    const auto& partition = response.topics()[0].partitions[0];
    EXPECT_EQ(partition.error_code, ErrorCode::NONE);
    ASSERT_FALSE(partition.record_batches.empty());
    auto batch = storage::RecordBatch::deserialize(partition.record_batches);
    ASSERT_EQ(batch.records().size(), 1u);
    EXPECT_EQ(batch.records()[0].value, (std::vector<uint8_t>{'h', 'e', 'l', 'l', 'o'}));
}

TEST_F(ClientApiParityTest, EarliestLocalAndClassicOffsetFetchV9Work) {
    start();
    create("earliest-local");
    produce("earliest-local");
    ListOffsetsRequest request;
    ListOffsetsTopic topic;
    topic.topic = "earliest-local";
    ListOffsetsPartition partition{};
    partition.partition = 0;
    partition.timestamp = -4;
    topic.partitions.push_back(partition);
    request.addTopic(topic);
    auto body = call(ApiKey::LIST_OFFSETS, 8, request);
    ListOffsetsResponse response;
    response.decode(body, 8);
    ASSERT_EQ(response.topics().size(), 1u);
    EXPECT_EQ(response.topics()[0].partitions[0].error_code, ErrorCode::NONE);
    EXPECT_EQ(response.topics()[0].partitions[0].offset, 0);
    OffsetFetchRequest offsets;
    OffsetFetchRequest::Group group;
    group.group_id = "classic";
    group.topics.push_back({"earliest-local", {{0}}});
    offsets.addGroup(group);
    auto offset_body = call(ApiKey::OFFSET_FETCH, 9, offsets);
    OffsetFetchResponse fetched;
    fetched.decode(offset_body, 9);
    ASSERT_EQ(fetched.groups().size(), 1u);
    EXPECT_EQ(fetched.groups()[0].error_code, ErrorCode::NONE);
    EXPECT_EQ(fetched.groups()[0].topics[0].partitions[0].offset, -1);
}

TEST_F(ClientApiParityTest, OffsetFetchRejectsDeferredKip848Membership) {
    start();
    OffsetFetchRequest request;
    OffsetFetchRequest::Group group;
    group.group_id = "new-protocol";
    group.member_id = "m";
    group.member_epoch = 7;
    group.topics.push_back({"t", {{0}}});
    request.addGroup(group);
    auto body = call(ApiKey::OFFSET_FETCH, 9, request);
    OffsetFetchResponse response;
    response.decode(body, 9);
    ASSERT_EQ(response.groups().size(), 1u);
    EXPECT_EQ(response.groups()[0].error_code, ErrorCode::UNSUPPORTED_VERSION);
}

TEST_F(ClientApiParityTest, LegacyCapsAlsoRejectNewerWireRequests) {
    start("3.x");
    const auto actual = versions();
    for (const auto& [key, maximum] : std::map<ApiKey, int16_t>{{ApiKey::PRODUCE, 9},
                                                                {ApiKey::FETCH, 12},
                                                                {ApiKey::LIST_OFFSETS, 7},
                                                                {ApiKey::OFFSET_FETCH, 8},
                                                                {ApiKey::DELETE_RECORDS, 0},
                                                                {ApiKey::SASL_AUTHENTICATE, 1}}) {
        EXPECT_EQ(actual.at(key), maximum);
    }
    auto body = call(ApiKey::FETCH, 13, fetch({}));
    FetchResponse response;
    ASSERT_NO_THROW(response.decode(body, 13));
    ASSERT_EQ(response.topics().size(), 1u);
    ASSERT_EQ(response.topics()[0].partitions.size(), 1u);
    EXPECT_EQ(response.topics()[0].partitions[0].error_code, ErrorCode::UNSUPPORTED_VERSION);
}

TEST_F(ClientApiParityTest, LegacySaslErrorKeepsRequestedFlexibleBody) {
    start("3.x");
    SaslAuthenticateRequest request;
    request.setAuthBytes({0, 'u', 0, 'p'});
    auto body = call(ApiKey::SASL_AUTHENTICATE, 2, request);
    EXPECT_EQ(body.readInt16(), static_cast<int16_t>(ErrorCode::UNSUPPORTED_VERSION));
    body.reset();
    SaslAuthenticateResponse response;
    ASSERT_NO_THROW(response.decode(body, 2));
    EXPECT_EQ(body.remaining(), 0u);
}

TEST_F(ClientApiParityTest, LegacyDeleteRecordsErrorKeepsRequestedPartitions) {
    start("3.x");
    Buffer classic;
    classic.writeInt32(1);
    classic.writeString("t");
    classic.writeInt32(1);
    classic.writeInt32(0);
    classic.writeInt64(42);
    classic.writeInt32(5000);
    DeleteRecordsRequest request;
    request.decode(classic, 0);
    auto body = call(ApiKey::DELETE_RECORDS, 2, request);
    body.readInt32();  // throttle
    ASSERT_EQ(body.readCompactArrayLen(), 1);
    EXPECT_EQ(body.readCompactString(), "t");
    ASSERT_EQ(body.readCompactArrayLen(), 1);
    EXPECT_EQ(body.readInt32(), 0);
    body.readInt64();  // low watermark
    EXPECT_EQ(body.readInt16(), static_cast<int16_t>(ErrorCode::UNSUPPORTED_VERSION));
    body.skipTaggedFields();
    body.skipTaggedFields();
    body.skipTaggedFields();
    EXPECT_EQ(body.remaining(), 0u);
}

TEST_F(ClientApiParityTest, LegacyOffsetFetchErrorKeepsRequestedGroups) {
    start("3.x");
    OffsetFetchRequest request;
    OffsetFetchRequest::Group group;
    group.group_id = "g";
    group.topics.push_back({"t", {{0}}});
    request.addGroup(group);
    auto body = call(ApiKey::OFFSET_FETCH, 9, request);
    OffsetFetchResponse response;
    ASSERT_NO_THROW(response.decode(body, 9));
    ASSERT_EQ(response.groups().size(), 1u);
    EXPECT_EQ(response.groups()[0].group_id, "g");
    EXPECT_EQ(response.groups()[0].error_code, ErrorCode::UNSUPPORTED_VERSION);
}

TEST_F(ClientApiParityTest, ProduceV10AndV11AppendAtExactOffsets) {
    start();
    create("modern-produce");
    for (int16_t version : {10, 11}) {
        storage::RecordBatch batch;
        Record record;
        record.timestamp = 1;
        record.value = std::vector<uint8_t>{'v'};
        batch.addRecord(record);
        ProduceRequest request;
        request.setAcks(-1);
        ProduceTopicData topic;
        topic.topic = "modern-produce";
        topic.partitions.push_back({0, batch.serialize()});
        request.addTopic(topic);
        auto body = call(ApiKey::PRODUCE, version, request);
        ProduceResponse response;
        response.decode(body, version);
        ASSERT_EQ(response.topics().size(), 1u);
        ASSERT_EQ(response.topics()[0].partitions.size(), 1u);
        EXPECT_EQ(response.topics()[0].partitions[0].error_code, ErrorCode::NONE);
        EXPECT_EQ(response.topics()[0].partitions[0].base_offset, version - 10);
    }
}

TEST_F(ClientApiParityTest, ModernSaslAndDeleteRecordsUseFlexibleBodies) {
    start();
    SaslAuthenticateRequest auth;
    auth.setAuthBytes({0, 'u', 0, 'p'});
    auto auth_body = call(ApiKey::SASL_AUTHENTICATE, 2, auth);
    EXPECT_EQ(auth_body.readInt16(), static_cast<int16_t>(ErrorCode::NONE));
    auth_body.reset();
    SaslAuthenticateResponse authenticated;
    ASSERT_NO_THROW(authenticated.decode(auth_body, 2));
    EXPECT_EQ(auth_body.remaining(), 0u);
    Buffer classic;
    classic.writeInt32(1);
    classic.writeString("absent");
    classic.writeInt32(1);
    classic.writeInt32(0);
    classic.writeInt64(42);
    classic.writeInt32(5000);
    DeleteRecordsRequest request;
    request.decode(classic, 0);
    auto body = call(ApiKey::DELETE_RECORDS, 2, request);
    body.readInt32();
    ASSERT_EQ(body.readCompactArrayLen(), 1);
    EXPECT_EQ(body.readCompactString(), "absent");
    ASSERT_EQ(body.readCompactArrayLen(), 1);
    EXPECT_EQ(body.readInt32(), 0);
    body.readInt64();
    EXPECT_EQ(body.readInt16(), static_cast<int16_t>(ErrorCode::UNKNOWN_TOPIC_OR_PARTITION));
    body.skipTaggedFields();
    body.skipTaggedFields();
    body.skipTaggedFields();
    EXPECT_EQ(body.remaining(), 0u);
}
