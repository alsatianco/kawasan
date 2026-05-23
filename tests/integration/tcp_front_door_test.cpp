#include <arpa/inet.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <filesystem>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

#include <boost/asio.hpp>
#include <gtest/gtest.h>

#include "kawasan/broker/kawasan_broker.h"
#include "kawasan/common/buffer.h"
#include "kawasan/common/config.h"
#include "kawasan/common/logger.h"
#include "kawasan/common/types.h"
#include "kawasan/protocol/api_versions.h"
#include "kawasan/protocol/create_topics_request.h"
#include "kawasan/protocol/metadata_request.h"
#include "kawasan/protocol/request_header.h"

namespace {

std::string makeLogDir() {
    const auto timestamp =
        std::chrono::steady_clock::now().time_since_epoch().count();
    auto tmp = std::filesystem::temp_directory_path() /
               ("kawasan-tcp-test-" + std::to_string(timestamp));
    std::filesystem::create_directories(tmp);
    return tmp.string();
}

int32_t decodeLength(const std::array<uint8_t, 4>& bytes) {
    uint32_t value = 0;
    std::memcpy(&value, bytes.data(), sizeof(value));
    return ntohl(value);
}

void ensureLoggerInitialized() {
    static bool initialized = false;
    if (!initialized) {
        kawasan::Logger::init("info");
        initialized = true;
    }
}

kawasan::Config makeConfig(const std::string& log_dir) {
    kawasan::Config config;
    config.setInt("broker.id", 1);
    config.setString("host", "127.0.0.1");
    config.setInt("port", 0);
    // opus2 Item 2: ephemeral Raft port for parallel test isolation.
    config.setInt("raft.port", 0);
    config.setString("log.dirs", log_dir);
    config.setInt("network.io_threads", 1);
    config.setLong("network.max_frame_bytes", 1024 * 1024);
    return config;
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
    boost::asio::read(socket, boost::asio::buffer(response_body));
    return response_body;
}

}  // namespace

TEST(TcpFrontDoorTest, HandlesApiVersionsRequest) {
    ensureLoggerInitialized();

    const auto log_dir = makeLogDir();
    auto config = makeConfig(log_dir);

    kawasan::broker::KawasanBroker broker(config);
    broker.start();

    ASSERT_GT(broker.port(), 0);

    boost::asio::io_context io;
    boost::asio::ip::tcp::socket socket(io);
    auto endpoint = boost::asio::ip::tcp::endpoint(
        boost::asio::ip::make_address("127.0.0.1"),
        static_cast<uint16_t>(broker.port()));
    socket.connect(endpoint);

    kawasan::Buffer payload;
    kawasan::protocol::RequestHeader header(
        kawasan::protocol::ApiKey::API_VERSIONS, /*api_version=*/1,
        /*correlation_id=*/42, "tcp-test");
    header.encode(payload);
    kawasan::protocol::ApiVersionsRequest request;
    request.encode(payload, 1);

    auto response_bytes = sendKafkaRequest(socket, payload);

    kawasan::Buffer response_buffer(response_bytes);
    kawasan::protocol::ResponseHeader response_header;
    response_header.decode(response_buffer);
    kawasan::protocol::ApiVersionsResponse response;
    response.decode(response_buffer, 1);

    ASSERT_EQ(header.correlationId(), response_header.correlationId());
    EXPECT_EQ(kawasan::ErrorCode::NONE, response.errorCode());

    const auto& versions = response.apiVersions();
    auto hasMetadata = std::any_of(
        versions.begin(), versions.end(), [](const auto& entry) {
            return entry.api_key == kawasan::protocol::ApiKey::METADATA;
        });
    EXPECT_TRUE(hasMetadata);

    auto hasApiVersions = std::any_of(
        versions.begin(), versions.end(), [](const auto& entry) {
            return entry.api_key == kawasan::protocol::ApiKey::API_VERSIONS;
        });
    EXPECT_TRUE(hasApiVersions);

    socket.close();
    broker.stop();
    std::filesystem::remove_all(log_dir);
}

TEST(TcpFrontDoorTest, HandlesMetadataRequest) {
    ensureLoggerInitialized();

    const auto log_dir = makeLogDir();
    auto config = makeConfig(log_dir);

    kawasan::broker::KawasanBroker broker(config);
    broker.start();

    ASSERT_GT(broker.port(), 0);

    boost::asio::io_context io;
    boost::asio::ip::tcp::socket socket(io);
    auto endpoint = boost::asio::ip::tcp::endpoint(
        boost::asio::ip::make_address("127.0.0.1"),
        static_cast<uint16_t>(broker.port()));
    socket.connect(endpoint);

    kawasan::Buffer payload;
    kawasan::protocol::RequestHeader header(
        kawasan::protocol::ApiKey::METADATA, /*api_version=*/7,
        /*correlation_id=*/99, "tcp-test");
    header.encode(payload);
    kawasan::protocol::MetadataRequest request;
    request.encode(payload, 7);

    auto response_bytes = sendKafkaRequest(socket, payload);

    kawasan::Buffer response_buffer(response_bytes);
    kawasan::protocol::ResponseHeader response_header;
    response_header.decode(response_buffer);
    kawasan::protocol::MetadataResponse response;
    response.decode(response_buffer, 7);

    ASSERT_EQ(header.correlationId(), response_header.correlationId());
    ASSERT_EQ(response.brokers().size(), 1);
    const auto& broker_info = response.brokers().front();
    EXPECT_EQ(broker_info.id, config.get<int32_t>("broker.id"));
    EXPECT_EQ(broker_info.host, "127.0.0.1");
    EXPECT_EQ(broker_info.port, broker.port());

    // Phase 3.1: __consumer_offsets is auto-created at startup, so the
    // initial Metadata response is non-empty. Filter internal topics
    // (names starting with `__`) before asserting on user-visible state.
    auto count_user_topics = [](const auto& topics) {
        size_t n = 0;
        for (const auto& t : topics) {
            if (t.name.rfind("__", 0) != 0) n++;
        }
        return n;
    };
    EXPECT_EQ(count_user_topics(response.topics()), 0u);

    // Create a topic through the network API.
    kawasan::Buffer create_payload;
    kawasan::protocol::RequestHeader create_header(
        kawasan::protocol::ApiKey::CREATE_TOPICS, /*api_version=*/4,
        /*correlation_id=*/100, "tcp-test");
    create_header.encode(create_payload);
    kawasan::protocol::CreateTopicsRequest create_request;
    create_request.setTimeoutMs(5000);
    kawasan::protocol::CreatableTopic create_topic;
    create_topic.name = "tcp-frontdoor";
    create_topic.num_partitions = 1;
    create_topic.replication_factor = 1;
    create_request.addTopic(create_topic);
    create_request.encode(create_payload, 4);

    auto create_response_bytes = sendKafkaRequest(socket, create_payload);
    kawasan::Buffer create_response_buffer(create_response_bytes);
    kawasan::protocol::ResponseHeader create_response_header;
    create_response_header.decode(create_response_buffer);
    kawasan::protocol::CreateTopicsResponse create_response;
    create_response.decode(create_response_buffer, 4);
    ASSERT_EQ(create_header.correlationId(), create_response_header.correlationId());
    ASSERT_FALSE(create_response.results().empty());
    EXPECT_EQ(kawasan::ErrorCode::NONE, create_response.results().front().error_code);

    // Request metadata again and verify the created topic is visible.
    kawasan::Buffer payload2;
    kawasan::protocol::RequestHeader header2(
        kawasan::protocol::ApiKey::METADATA, /*api_version=*/7,
        /*correlation_id=*/101, "tcp-test");
    header2.encode(payload2);
    kawasan::protocol::MetadataRequest request2;
    request2.encode(payload2, 7);

    auto response_bytes2 = sendKafkaRequest(socket, payload2);
    kawasan::Buffer response_buffer2(response_bytes2);
    kawasan::protocol::ResponseHeader response_header2;
    response_header2.decode(response_buffer2);
    kawasan::protocol::MetadataResponse response2;
    response2.decode(response_buffer2, 7);

    ASSERT_EQ(header2.correlationId(), response_header2.correlationId());
    ASSERT_FALSE(response2.topics().empty());
    // Find the user topic (skip internal `__consumer_offsets`).
    const kawasan::TopicMetadata* user_topic_ptr = nullptr;
    for (const auto& t : response2.topics()) {
        if (t.name.rfind("__", 0) != 0) {
            user_topic_ptr = &t;
            break;
        }
    }
    ASSERT_NE(user_topic_ptr, nullptr);
    const auto& created_topic = *user_topic_ptr;
    EXPECT_EQ(created_topic.name, "tcp-frontdoor");
    ASSERT_FALSE(created_topic.partitions.empty());
    const auto& created_partition = created_topic.partitions.front();
    EXPECT_EQ(created_partition.leader, config.get<int32_t>("broker.id"));
    EXPECT_EQ(created_partition.replicas.size(), 1);
    EXPECT_EQ(created_partition.replicas.front(), config.get<int32_t>("broker.id"));

    socket.close();
    broker.stop();
    std::filesystem::remove_all(log_dir);
}
