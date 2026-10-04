#include <gtest/gtest.h>

#include <fstream>
#include <nlohmann/json.hpp>
#include <string>
#include <vector>

#include "kawasan/common/buffer.h"
#include "kawasan/protocol/describe_groups_request.h"

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
