#include <arpa/inet.h>
#include <gtest/gtest.h>

#include <array>
#include <boost/asio.hpp>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <thread>

#include "kawasan/broker/coordinator_routing.h"
#include "kawasan/broker/kawasan_broker.h"
#include "kawasan/broker/metadata_controller.h"
#include "kawasan/common/logger.h"
#include "kawasan/protocol/find_coordinator_request.h"
#include "kawasan/protocol/request_header.h"

namespace asio = boost::asio;
using namespace kawasan;
using namespace kawasan::protocol;
using namespace std::chrono_literals;

namespace {
int freeRaftPort() {
    asio::io_context io;
    for (;;) {
        asio::ip::tcp::acceptor raft(io, {asio::ip::make_address("127.0.0.1"), 0});
        const auto port = raft.local_endpoint().port();
        asio::ip::tcp::acceptor kafka(io);
        boost::system::error_code ec;
        kafka.open(asio::ip::tcp::v4());
        kafka.bind({asio::ip::make_address("127.0.0.1"), static_cast<uint16_t>(port - 1)}, ec);
        if (!ec)
            return port;
    }
}

class FindCoordinatorRoutingTest : public ::testing::Test {
protected:
    std::filesystem::path dir;
    std::vector<std::unique_ptr<broker::KawasanBroker>> brokers;
    void SetUp() override {
        static const bool logger = [] {
            Logger::init("warn");
            return true;
        }();
        (void)logger;
        dir = std::filesystem::temp_directory_path() /
              ("kawasan-routing-" +
               std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        std::vector<int> ports;
        std::string peers;
        for (int id = 0; id < 3; ++id) {
            int port;
            do {
                port = freeRaftPort();
            } while (std::any_of(ports.begin(), ports.end(),
                                 [port](int p) { return std::abs(p - port) <= 1; }));
            ports.push_back(port);
            if (id)
                peers += ",";
            peers += std::to_string(id) + ":127.0.0.1:" + std::to_string(ports.back());
        }
        for (int id = 0; id < 3; ++id) {
            Config config;
            config.setInt("broker.id", id);
            config.setString("host", "127.0.0.1");
            config.setInt("port", ports[id] - 1);
            config.setInt("raft.port", ports[id]);
            config.setString("raft.peers", peers);
            config.setString("log.dirs", (dir / std::to_string(id)).string());
            config.setBool("monitoring.enabled", false);
            config.setInt("offsets.topic.num.partitions", 3);
            config.setInt("transaction.state.topic.num.partitions", 2);
            brokers.push_back(std::make_unique<broker::KawasanBroker>(config));
            brokers.back()->start();
        }
        const auto deadline = std::chrono::steady_clock::now() + 15s;
        for (;;) {
            bool ready = true;
            for (const auto& b : brokers) {
                const auto topics = b->metadataController()->describeTopics(
                    {"__consumer_offsets", "__transaction_state"});
                ready &= b->raftNode()->hasCurrentMetadata(1000) && topics.size() == 2 &&
                         topics[0].partitions.size() == 3 && topics[1].partitions.size() == 2;
            }
            if (ready)
                break;
            ASSERT_LT(std::chrono::steady_clock::now(), deadline);
            std::this_thread::sleep_for(20ms);
        }
    }
    void TearDown() override {
        for (auto& b : brokers)
            b->stop();
        brokers.clear();
        std::filesystem::remove_all(dir);
    }
    Buffer exchange(int id, ApiKey api, int16_t version, const Buffer& body) {
        asio::io_context io;
        asio::ip::tcp::socket socket(io);
        socket.connect(
            {asio::ip::make_address("127.0.0.1"), static_cast<uint16_t>(brokers[id]->port())});
        RequestHeader header(api, version, 1, "routing-test");
        Buffer payload;
        header.encode(payload);
        payload.writeBytes(body.vector().data(), body.size());
        Buffer frame;
        frame.writeInt32(static_cast<int32_t>(payload.size()));
        frame.writeBytes(payload.vector().data(), payload.size());
        asio::write(socket, asio::buffer(frame.vector()));
        uint32_t length;
        asio::read(socket, asio::buffer(&length, sizeof(length)));
        std::vector<uint8_t> reply(ntohl(length));
        asio::read(socket, asio::buffer(reply));
        Buffer response(reply);
        EXPECT_EQ(response.readInt32(), 1);
        if (header.isFlexibleResponseHeader())
            response.skipTaggedFields();
        return response;
    }
    FindCoordinatorResponse find(int id, int version, CoordinatorType type,
                                 const std::vector<std::string>& keys) {
        FindCoordinatorRequest request;
        request.setKeyType(type);
        for (const auto& key : keys)
            request.addKey(key);
        Buffer body;
        request.encode(body, version);
        auto bytes = exchange(id, ApiKey::FIND_COORDINATOR, version, body);
        FindCoordinatorResponse response;
        response.decode(bytes, version);
        EXPECT_EQ(bytes.remaining(), 0);
        return response;
    }
};

TEST_F(FindCoordinatorRoutingTest, AllBrokersResolveBothTypesAcrossVersions) {
    for (int version = 0; version <= 4; ++version) {
        for (const auto type : {CoordinatorType::GROUP, CoordinatorType::TRANSACTION}) {
            if (version == 0 && type == CoordinatorType::TRANSACTION)
                continue;
            const int count = type == CoordinatorType::GROUP ? 3 : 2;
            for (int broker = 0; broker < 3; ++broker) {
                const auto response = find(broker, version, type, {"b"});
                const auto owner = broker::coordinatorPartitionFor("b", count);
                if (version == 4) {
                    ASSERT_EQ(response.coordinators().size(), 1);
                    const auto& c = response.coordinators().front();
                    EXPECT_EQ(c.error_code, ErrorCode::NONE);
                    EXPECT_EQ(c.node_id, owner);
                    EXPECT_EQ(c.port, brokers[owner]->port());
                } else {
                    EXPECT_EQ(response.errorCode(), ErrorCode::NONE);
                    EXPECT_EQ(response.nodeId(), owner);
                    EXPECT_EQ(response.port(), brokers[owner]->port());
                }
            }
        }
    }
}

TEST_F(FindCoordinatorRoutingTest, BatchedKeysReportOfflineOwnersIndependently) {
    int controller = 0;
    while (!brokers[controller]->raftNode()->isLeader())
        ++controller;
    ASSERT_EQ(brokers[controller]
                  ->metadataController()
                  ->updatePartitionLeader("__consumer_offsets", 1, -1)
                  .error_code,
              ErrorCode::NONE);
    const auto response = find(controller, 4, CoordinatorType::GROUP, {"a", "b"});
    ASSERT_EQ(response.coordinators().size(), 2);
    EXPECT_EQ(response.coordinators()[0].error_code, ErrorCode::COORDINATOR_NOT_AVAILABLE);
    EXPECT_EQ(response.coordinators()[0].node_id, -1);
    EXPECT_EQ(response.coordinators()[1].error_code, ErrorCode::NONE);
    EXPECT_EQ(response.coordinators()[1].node_id, 2);
    EXPECT_EQ(find(controller, 1, static_cast<CoordinatorType>(99), {"b"}).errorCode(),
              ErrorCode::INVALID_REQUEST);
}

TEST_F(FindCoordinatorRoutingTest, MissingInternalTopicIsUnavailable) {
    int controller = 0;
    while (!brokers[controller]->raftNode()->isLeader())
        ++controller;
    ASSERT_EQ(
        brokers[controller]->metadataController()->deleteTopic("__transaction_state").error_code,
        ErrorCode::NONE);
    EXPECT_EQ(find(controller, 1, CoordinatorType::TRANSACTION, {"b"}).errorCode(),
              ErrorCode::COORDINATOR_NOT_AVAILABLE);
}

TEST_F(FindCoordinatorRoutingTest, SingleNodeKeepsItsOwnEndpoint) {
    Config config;
    config.setInt("broker.id", 7);
    config.setString("host", "127.0.0.1");
    config.setInt("port", 0);
    config.setInt("raft.port", 0);
    config.setString("log.dirs", (dir / "single").string());
    config.setBool("monitoring.enabled", false);
    brokers.push_back(std::make_unique<broker::KawasanBroker>(config));
    brokers.back()->start();
    for (int version = 0; version <= 4; ++version) {
        for (const auto type : {CoordinatorType::GROUP, CoordinatorType::TRANSACTION}) {
            if (version == 0 && type == CoordinatorType::TRANSACTION)
                continue;
            const auto response = find(3, version, type, {"my-group"});
            if (version == 4) {
                ASSERT_EQ(response.coordinators().size(), 1);
                EXPECT_EQ(response.coordinators()[0].node_id, 7);
                EXPECT_EQ(response.coordinators()[0].port, brokers[3]->port());
            } else {
                EXPECT_EQ(response.errorCode(), ErrorCode::NONE);
                EXPECT_EQ(response.nodeId(), 7);
                EXPECT_EQ(response.port(), brokers[3]->port());
            }
        }
    }
}
}  // namespace
