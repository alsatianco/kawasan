#include <gtest/gtest.h>

#include <fstream>
#include <nlohmann/json.hpp>
#include <string>
#include <vector>

#include "kawasan/common/buffer.h"
#include "kawasan/protocol/admin_misc_requests.h"
#include "kawasan/protocol/delete_topics_request.h"
#include "kawasan/protocol/describe_configs_request.h"
#include "kawasan/protocol/describe_groups_request.h"
#include "kawasan/protocol/fetch_request.h"
#include "kawasan/protocol/list_offsets_request.h"
#include "kawasan/protocol/offset_fetch_request.h"
#include "kawasan/protocol/produce_request.h"
#include "kawasan/protocol/sasl_request.h"

namespace {
std::vector<uint8_t> golden(const std::string& name) {
    std::ifstream input(KAWASAN_WIRE_FIXTURES);
    const auto fixtures = nlohmann::json::parse(input);
    const auto hex = fixtures.at(name).get<std::string>();
    std::vector<uint8_t> bytes;
    for (size_t i = 0; i < hex.size(); i += 2) {
        bytes.push_back(static_cast<uint8_t>(std::stoul(hex.substr(i, 2), nullptr, 16)));
    }
    return bytes;
}
}  // namespace

// Kafka 4.2 generated codecs supply the bytes; local codec round trips alone
// would miss fields omitted symmetrically by both encode and decode.
TEST(ApiVersionParityTest, DescribeGroupsDecodesKafkaStaticMembership) {
    for (const std::string prefix : {"describe-groups-v", "describe-groups-null-v"}) {
        for (int16_t version : {4, 5}) {
            SCOPED_TRACE(version);
            const auto bytes = golden(prefix + std::to_string(version));
            kawasan::Buffer input(bytes);
            kawasan::protocol::DescribeGroupsResponse response;
            ASSERT_NO_THROW(response.decode(input, version));
            ASSERT_EQ(response.groups().size(), 1u);
            const auto& group = response.groups().front();
            ASSERT_EQ(group.members.size(), 1u);
            EXPECT_EQ(group.members.front().client_id, "client");
            EXPECT_EQ(group.members.front().client_host, "host");
            EXPECT_EQ(group.members.front().member_metadata, (std::vector<uint8_t>{1, 2}));
            EXPECT_EQ(group.members.front().member_assignment, (std::vector<uint8_t>{3, 4}));
            kawasan::Buffer encoded;
            response.encode(encoded, version);
            EXPECT_EQ(std::vector<uint8_t>(encoded.data(), encoded.data() + encoded.size()), bytes);
        }
    }
}

TEST(ApiVersionParityTest, FetchV13DecodesUuidAndForgottenTopicFromKafka) {
    const auto bytes = golden("fetch-request-v13");
    kawasan::Buffer input(bytes);
    kawasan::protocol::FetchRequest request;
    ASSERT_NO_THROW(request.decode(input, 13));
    EXPECT_EQ(input.remaining(), 0u);
    ASSERT_EQ(request.topics().size(), 1u);
    ASSERT_EQ(request.forgottenTopics().size(), 1u);
    EXPECT_EQ(request.rackId(), "rack");
    EXPECT_TRUE(request.forgottenTopics()[0].has_topic_id);
    EXPECT_EQ(request.forgottenTopics()[0].topic_id, request.topics()[0].topic_id);
    EXPECT_EQ(request.forgottenTopics()[0].partitions, std::vector<int32_t>{0});
    EXPECT_EQ(request.topics()[0].topic_id.front(), 1);
    EXPECT_EQ(request.topics()[0].topic_id.back(), 16);
    kawasan::Buffer encoded;
    request.encode(encoded, 13);
    EXPECT_EQ(std::vector<uint8_t>(encoded.data(), encoded.data() + encoded.size()), bytes);
    EXPECT_EQ(request.size(13), bytes.size());
}

TEST(ApiVersionParityTest, FetchV13ResponseEmitsRawUuid) {
    const auto bytes = golden("fetch-response-v13");
    kawasan::Buffer input(bytes);
    kawasan::protocol::FetchResponse response;
    ASSERT_NO_THROW(response.decode(input, 13));
    EXPECT_EQ(input.remaining(), 0u);
    ASSERT_EQ(response.topics().size(), 1u);
    EXPECT_EQ(response.topics()[0].topic_id.front(), 1);
    EXPECT_EQ(response.topics()[0].topic_id.back(), 16);
    kawasan::Buffer encoded;
    response.encode(encoded, 13);
    EXPECT_EQ(std::vector<uint8_t>(encoded.data(), encoded.data() + encoded.size()), bytes);
    EXPECT_EQ(response.size(13), bytes.size());
}

TEST(ApiVersionParityTest, DeleteTopicsV6RequestEmitsRawUuid) {
    const auto bytes = golden("delete-topics-request-v6");
    kawasan::Buffer input(bytes);
    kawasan::protocol::DeleteTopicsRequest request;
    ASSERT_NO_THROW(request.decode(input, 6));
    EXPECT_EQ(input.remaining(), 0u);
    ASSERT_EQ(request.topics().size(), 1u);
    EXPECT_EQ(request.topics()[0].name, "t");
    EXPECT_EQ(request.topics()[0].topic_id.back(), 16);
    kawasan::Buffer encoded;
    request.encode(encoded, 6);
    EXPECT_EQ(std::vector<uint8_t>(encoded.data(), encoded.data() + encoded.size()), bytes);
}

TEST(ApiVersionParityTest, DeleteTopicsV6ResponseEmitsRawUuid) {
    const auto bytes = golden("delete-topics-response-v6");
    kawasan::Buffer input(bytes);
    kawasan::protocol::DeleteTopicsResponse response;
    ASSERT_NO_THROW(response.decode(input, 6));
    EXPECT_EQ(input.remaining(), 0u);
    ASSERT_EQ(response.results().size(), 1u);
    EXPECT_EQ(response.results()[0].name, "t");
    EXPECT_EQ(response.results()[0].error_message, "error");
    kawasan::Buffer encoded;
    response.encode(encoded, 6);
    EXPECT_EQ(std::vector<uint8_t>(encoded.data(), encoded.data() + encoded.size()), bytes);
}

TEST(ApiVersionParityTest, SaslV2RequestMatchesKafka) {
    kawasan::protocol::SaslAuthenticateRequest request;
    request.setAuthBytes({1, 2, 3});
    kawasan::Buffer encoded;
    request.encode(encoded, 2);
    const auto bytes = golden("sasl-request-v2");
    ASSERT_EQ(encoded.vector(), bytes);
    kawasan::Buffer input(bytes);
    kawasan::protocol::SaslAuthenticateRequest decoded;
    ASSERT_NO_THROW(decoded.decode(input, 2));
    EXPECT_EQ(decoded.authBytes(), (std::vector<uint8_t>{1, 2, 3}));
    EXPECT_EQ(input.remaining(), 0u);
}

TEST(ApiVersionParityTest, SaslV2ResponseMatchesKafka) {
    kawasan::protocol::SaslAuthenticateResponse response;
    kawasan::Buffer input(golden("sasl-response-v2"));
    ASSERT_NO_THROW(response.decode(input, 2));
    EXPECT_EQ(input.remaining(), 0u);
    kawasan::Buffer encoded;
    response.encode(encoded, 2);
    EXPECT_EQ(encoded.vector(), golden("sasl-response-v2"));
}

TEST(ApiVersionParityTest, DeleteRecordsV2RequestMatchesKafka) {
    // Seed the public request through its classic decoder to avoid making the
    // pre-fix decoder allocate a huge array from compact Kafka bytes.
    kawasan::Buffer classic;
    classic.writeInt32(1);
    classic.writeString("t");
    classic.writeInt32(1);
    classic.writeInt32(0);
    classic.writeInt64(42);
    classic.writeInt32(5000);
    kawasan::protocol::DeleteRecordsRequest request;
    request.decode(classic, 0);
    kawasan::Buffer encoded;
    request.encode(encoded, 2);
    const auto bytes = golden("delete-records-request-v2");
    ASSERT_EQ(encoded.vector(), bytes);
    kawasan::Buffer input(bytes);
    kawasan::protocol::DeleteRecordsRequest decoded;
    ASSERT_NO_THROW(decoded.decode(input, 2));
    EXPECT_EQ(input.remaining(), 0u);
    ASSERT_EQ(decoded.topics().size(), 1u);
    EXPECT_EQ(decoded.topics()[0].partitions[0].offset, 42);
    EXPECT_EQ(decoded.timeoutMs(), 5000);
}

TEST(ApiVersionParityTest, DeleteRecordsV2ResponseMatchesKafka) {
    kawasan::protocol::DeleteRecordsResponse response;
    response.addTopic({"t", {{0, 42, kawasan::ErrorCode::NONE}}});
    kawasan::Buffer encoded;
    response.encode(encoded, 2);
    const auto bytes = golden("delete-records-response-v2");
    ASSERT_EQ(encoded.vector(), bytes);
    kawasan::Buffer input(bytes);
    kawasan::protocol::DeleteRecordsResponse decoded;
    ASSERT_NO_THROW(decoded.decode(input, 2));
    EXPECT_EQ(input.remaining(), 0u);
    kawasan::Buffer reencoded;
    decoded.encode(reencoded, 2);
    EXPECT_EQ(reencoded.vector(), bytes);
}

TEST(ApiVersionParityTest, OffsetFetchV9RequestMatchesKafka) {
    for (const auto name : {"offset-fetch-request-v9", "offset-fetch-classic-request-v9"}) {
        const auto bytes = golden(name);
        kawasan::Buffer input(bytes);
        kawasan::protocol::OffsetFetchRequest request;
        ASSERT_NO_THROW(request.decode(input, 9));
        EXPECT_EQ(input.remaining(), 0u);
        ASSERT_EQ(request.groups().size(), 1u);
        EXPECT_EQ(request.groups()[0].group_id, "g");
        EXPECT_TRUE(request.requireStable());
        kawasan::Buffer encoded;
        request.encode(encoded, 9);
        EXPECT_EQ(encoded.vector(), bytes);
    }
}

TEST(ApiVersionParityTest, OffsetFetchV9ResponseMatchesKafkaAndResetsDecodedGroups) {
    const auto bytes = golden("offset-fetch-response-v9");
    kawasan::Buffer input(bytes);
    kawasan::protocol::OffsetFetchResponse response;
    ASSERT_NO_THROW(response.decode(input, 9));
    EXPECT_EQ(input.remaining(), 0u);
    kawasan::Buffer repeated(bytes);
    ASSERT_NO_THROW(response.decode(repeated, 9));
    ASSERT_EQ(response.groups().size(), 1u);
    EXPECT_EQ(response.groups()[0].topics[0].partitions[0].offset, 42);
    kawasan::Buffer encoded;
    response.encode(encoded, 9);
    EXPECT_EQ(encoded.vector(), bytes);
}

TEST(ApiVersionParityTest, ProduceV10AndV11MatchKafka) {
    for (int16_t version : {10, 11}) {
        kawasan::Buffer request_input(golden("produce-request-v" + std::to_string(version)));
        kawasan::protocol::ProduceRequest request;
        ASSERT_NO_THROW(request.decode(request_input, version));
        EXPECT_EQ(request_input.remaining(), 0u);
        kawasan::Buffer encoded_request;
        request.encode(encoded_request, version);
        EXPECT_EQ(encoded_request.vector(), golden("produce-request-v" + std::to_string(version)));
        kawasan::Buffer response_input(golden("produce-response-v" + std::to_string(version)));
        kawasan::protocol::ProduceResponse response;
        ASSERT_NO_THROW(response.decode(response_input, version));
        EXPECT_EQ(response_input.remaining(), 0u);
        kawasan::Buffer encoded_response;
        response.encode(encoded_response, version);
        EXPECT_EQ(encoded_response.vector(),
                  golden("produce-response-v" + std::to_string(version)));
    }
}

TEST(ApiVersionParityTest, ListOffsetsV8MatchesKafka) {
    const auto request_bytes = golden("list-offsets-request-v8");
    kawasan::Buffer request_input(request_bytes);
    kawasan::protocol::ListOffsetsRequest request;
    ASSERT_NO_THROW(request.decode(request_input, 8));
    ASSERT_EQ(request.topics().size(), 1u);
    EXPECT_EQ(request.topics()[0].partitions[0].timestamp, -4);
    EXPECT_EQ(request_input.remaining(), 0u);
    kawasan::Buffer encoded_request;
    request.encode(encoded_request, 8);
    EXPECT_EQ(encoded_request.vector(), request_bytes);
    const auto response_bytes = golden("list-offsets-response-v8");
    kawasan::Buffer response_input(response_bytes);
    kawasan::protocol::ListOffsetsResponse response;
    ASSERT_NO_THROW(response.decode(response_input, 8));
    EXPECT_EQ(response_input.remaining(), 0u);
    kawasan::Buffer encoded_response;
    response.encode(encoded_response, 8);
    EXPECT_EQ(encoded_response.vector(), response_bytes);
}

TEST(ApiVersionParityTest, TruncatedOffsetFetchDoesNotAllocateDeclaredGroupsOrTopics) {
    for (int16_t version : {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}) {
        kawasan::Buffer malformed;
        if (version < 6) {
            malformed.writeString("g");
            malformed.writeInt32(4096);
        } else {
            if (version < 8) {
                malformed.writeCompactString("g");
            }
            malformed.writeCompactArrayLen(4096);
        }
        malformed.writeUnsignedVarInt(0);  // cannot contain the declared entries
        kawasan::protocol::OffsetFetchRequest request;
        EXPECT_THROW(request.decode(malformed, version), std::exception);
        // Reject before reserving thousands of objects from a few wire bytes.
        // This probes retained allocation, independent of the exception text.
        EXPECT_LE(request.groups().capacity(), malformed.size());
        EXPECT_LE(request.topics().capacity(), malformed.size());
    }
}

TEST(ApiVersionParityTest, DescribeConfigsUsesKafkaDefaultAndDynamicTopicSources) {
    for (int16_t version : {1, 4}) {
        const auto bytes = golden("describe-configs-response-v" + std::to_string(version));
        kawasan::Buffer input(bytes);
        kawasan::protocol::DescribeConfigsResponse response;
        response.decode(input, version);
        ASSERT_EQ(response.results().size(), 1u);
        ASSERT_EQ(response.results()[0].configs.size(), 2u);
        EXPECT_TRUE(response.results()[0].configs[0].is_default);
        EXPECT_FALSE(response.results()[0].configs[1].is_default);
        kawasan::Buffer encoded;
        response.encode(encoded, version);
        EXPECT_EQ(std::vector<uint8_t>(encoded.data(), encoded.data() + encoded.size()), bytes);
    }
}
