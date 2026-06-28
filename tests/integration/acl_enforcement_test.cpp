// Phase A5: end-to-end ACL enforcement wiring. When authorizer.enabled=true and
// allow.everyone.if.no.acl.found=false, an anonymous (unauthenticated) client
// with no matching ACL is denied — here exercised through CreateTopics, which is
// authorized for CREATE on the topic. With allow.everyone.if.no.acl.found=true
// the same request succeeds, and (covered elsewhere) with the authorizer
// disabled there is no enforcement at all (the default).
#include <arpa/inet.h>

#include <array>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <string>
#include <vector>

#include <boost/asio.hpp>
#include <gtest/gtest.h>

#include "kawasan/broker/kawasan_broker.h"
#include "kawasan/common/buffer.h"
#include "kawasan/common/config.h"
#include "kawasan/common/logger.h"
#include "kawasan/protocol/create_topics_request.h"
#include "kawasan/protocol/request_header.h"

namespace {

std::string makeLogDir() {
    const auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
    auto tmp = std::filesystem::temp_directory_path() /
               ("kawasan-acl-" + std::to_string(ts));
    std::filesystem::create_directories(tmp);
    return tmp.string();
}

void ensureLogger() {
    static bool initialized = false;
    if (!initialized) {
        kawasan::Logger::init("warn");
        initialized = true;
    }
}

kawasan::Config makeConfig(const std::string& log_dir) {
    kawasan::Config config;
    config.setInt("broker.id", 1);
    config.setString("host", "127.0.0.1");
    config.setInt("port", 0);
    config.setInt("raft.port", 0);
    config.setInt("monitoring.port", 0);
    config.setString("log.dirs", log_dir);
    config.setInt("network.io_threads", 1);
    return config;
}

int32_t decodeLength(const std::array<uint8_t, 4>& bytes) {
    uint32_t value = 0;
    std::memcpy(&value, bytes.data(), sizeof(value));
    return ntohl(value);
}

std::vector<uint8_t> sendKafkaRequest(boost::asio::ip::tcp::socket& socket,
                                      kawasan::Buffer& payload) {
    kawasan::Buffer frame;
    frame.writeInt32(static_cast<int32_t>(payload.size()));
    const auto& payload_bytes = payload.vector();
    frame.writeBytes(payload_bytes.data(), payload_bytes.size());
    const auto& request_bytes = frame.vector();
    boost::asio::write(socket,
                       boost::asio::buffer(request_bytes.data(), request_bytes.size()));

    std::array<uint8_t, 4> size_bytes{};
    boost::asio::read(socket, boost::asio::buffer(size_bytes));
    const auto response_length = decodeLength(size_bytes);
    std::vector<uint8_t> response_body(response_length);
    if (response_length > 0) {
        boost::asio::read(socket, boost::asio::buffer(response_body));
    }
    return response_body;
}

kawasan::ErrorCode createTopicError(kawasan::broker::KawasanBroker& broker,
                                    const std::string& topic) {
    boost::asio::io_context io;
    boost::asio::ip::tcp::socket socket(io);
    socket.connect(boost::asio::ip::tcp::endpoint(
        boost::asio::ip::make_address("127.0.0.1"),
        static_cast<uint16_t>(broker.port())));

    kawasan::Buffer payload;
    kawasan::protocol::RequestHeader header(
        kawasan::protocol::ApiKey::CREATE_TOPICS, /*api_version=*/4,
        /*correlation_id=*/1, "acl-test");
    header.encode(payload);
    kawasan::protocol::CreateTopicsRequest request;
    request.setTimeoutMs(5000);
    kawasan::protocol::CreatableTopic topic_spec;
    topic_spec.name = topic;
    topic_spec.num_partitions = 1;
    topic_spec.replication_factor = 1;
    request.addTopic(topic_spec);
    request.encode(payload, 4);

    auto response_bytes = sendKafkaRequest(socket, payload);
    kawasan::Buffer response_buffer(response_bytes);
    kawasan::protocol::ResponseHeader response_header;
    response_header.decode(response_buffer);
    kawasan::protocol::CreateTopicsResponse response;
    response.decode(response_buffer, 4);
    socket.close();
    EXPECT_EQ(response.results().size(), 1u);
    return response.results().front().error_code;
}

}  // namespace

TEST(AclEnforcementTest, AnonymousDeniedWhenAuthorizerEnabledAndDefaultDeny) {
    ensureLogger();
    const auto dir = makeLogDir();
    auto config = makeConfig(dir);
    config.setBool("authorizer.enabled", true);
    config.setBool("allow.everyone.if.no.acl.found", false);

    kawasan::broker::KawasanBroker broker(config);
    broker.start();
    ASSERT_GT(broker.port(), 0);

    EXPECT_EQ(kawasan::ErrorCode::TOPIC_AUTHORIZATION_FAILED,
              createTopicError(broker, "acl-denied-topic"));

    broker.stop();
    std::filesystem::remove_all(dir);
}

TEST(AclEnforcementTest, AllowedWhenAuthorizerEnabledAndAllowEveryone) {
    ensureLogger();
    const auto dir = makeLogDir();
    auto config = makeConfig(dir);
    config.setBool("authorizer.enabled", true);
    config.setBool("allow.everyone.if.no.acl.found", true);

    kawasan::broker::KawasanBroker broker(config);
    broker.start();
    ASSERT_GT(broker.port(), 0);

    EXPECT_EQ(kawasan::ErrorCode::NONE,
              createTopicError(broker, "acl-allowed-topic"));

    broker.stop();
    std::filesystem::remove_all(dir);
}

// Default (authorizer disabled) must not enforce anything — preserves the
// pre-authorizer behavior so existing deployments are unaffected.
TEST(AclEnforcementTest, NoEnforcementWhenAuthorizerDisabled) {
    ensureLogger();
    const auto dir = makeLogDir();
    auto config = makeConfig(dir);  // authorizer.enabled defaults to false

    kawasan::broker::KawasanBroker broker(config);
    broker.start();
    ASSERT_GT(broker.port(), 0);

    EXPECT_EQ(kawasan::ErrorCode::NONE,
              createTopicError(broker, "acl-default-topic"));

    broker.stop();
    std::filesystem::remove_all(dir);
}
