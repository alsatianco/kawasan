#include <gtest/gtest.h>

#include <fstream>
#include <nlohmann/json.hpp>
#include <string>
#include <vector>

#include "kawasan/common/buffer.h"
#include "kawasan/protocol/delete_topics_request.h"
#include "kawasan/protocol/describe_groups_request.h"
#include "kawasan/protocol/fetch_request.h"

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
