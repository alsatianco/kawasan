// InitProducerId allocation: single-node ids stay 1, 2, 3, ... and survive a
// restart; in a cluster each broker issues ids from its own range (see
// clusterProducerId) so no two brokers ever hand out the same producer id.
#include <arpa/inet.h>
#include <gtest/gtest.h>

#include <array>
#include <boost/asio.hpp>
#include <chrono>
#include <cstring>
#include <filesystem>

#include "kawasan/broker/kawasan_broker.h"
#include "kawasan/broker/producer_id.h"
#include "kawasan/common/buffer.h"
#include "kawasan/common/config.h"
#include "kawasan/common/logger.h"
#include "kawasan/protocol/request_header.h"

namespace asio = boost::asio;

namespace {

int freePort() {
    asio::io_context io;
    asio::ip::tcp::acceptor a(io, asio::ip::tcp::endpoint(asio::ip::make_address("127.0.0.1"), 0));
    return a.local_endpoint().port();
}

kawasan::Config config(const std::string& dir, int32_t broker_id, const std::string& peers) {
    kawasan::Config c;
    c.setInt("broker.id", broker_id);
    c.setString("host", "127.0.0.1");
    c.setInt("port", 0);
    c.setString("log.dirs", dir);
    c.setInt("network.io_threads", 1);
    if (peers.empty()) {
        c.setInt("raft.port", 0);
    } else {
        c.setString("raft.peers", peers);
    }
    return c;
}

struct AllocationResponse {
    kawasan::ErrorCode error;
    int64_t producer_id;
    int16_t producer_epoch;
};

// InitProducerId v0 over a raw socket, including storage-error responses.
AllocationResponse allocationResponse(int32_t port, const std::string& transactional_id = "") {
    asio::io_context io;
    asio::ip::tcp::socket socket(io);
    socket.connect(
        asio::ip::tcp::endpoint(asio::ip::make_address("127.0.0.1"), static_cast<uint16_t>(port)));
    kawasan::Buffer payload;
    kawasan::protocol::RequestHeader header(kawasan::protocol::ApiKey::INIT_PRODUCER_ID, 0, 1,
                                            "pid-test");
    header.encode(payload);
    if (transactional_id.empty())
        payload.writeInt16(-1);  // transactional_id = null
    else
        payload.writeString(transactional_id);
    payload.writeInt32(60000);  // transaction_timeout_ms
    kawasan::Buffer frame;
    frame.writeInt32(static_cast<int32_t>(payload.size()));
    frame.writeBytes(payload.vector().data(), payload.size());
    asio::write(socket, asio::buffer(frame.vector().data(), frame.size()));
    std::array<uint8_t, 4> size_bytes{};
    asio::read(socket, asio::buffer(size_bytes));
    uint32_t net = 0;
    std::memcpy(&net, size_bytes.data(), 4);
    std::vector<uint8_t> body(ntohl(net));
    asio::read(socket, asio::buffer(body));
    kawasan::Buffer buf(body);
    (void)buf.readInt32();  // correlation id
    (void)buf.readInt32();  // throttle_time_ms
    const auto error = static_cast<kawasan::ErrorCode>(buf.readInt16());
    const auto producer_id = buf.readInt64();
    const auto epoch = buf.readInt16();
    EXPECT_EQ(buf.remaining(), 0);
    return {error, producer_id, epoch};
}

int64_t initProducerId(int32_t port) {
    const auto response = allocationResponse(port);
    EXPECT_EQ(response.error, kawasan::ErrorCode::NONE);
    EXPECT_EQ(response.producer_epoch, 0);
    return response.producer_id;
}

class ProducerIdAllocationTest : public ::testing::Test {
protected:
    void SetUp() override {
        static const bool logger_ready = [] {
            kawasan::Logger::init("warn");
            return true;
        }();
        (void)logger_ready;
        const auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
        dir_ = (std::filesystem::temp_directory_path() / ("kawasan-pid-" + std::to_string(ts)))
                   .string();
        std::filesystem::create_directories(dir_);
    }
    void TearDown() override { std::filesystem::remove_all(dir_); }
    std::string dir_;
};

}  // namespace

TEST_F(ProducerIdAllocationTest, SingleNodeIdsAreSequentialAcrossRestarts) {
    {
        kawasan::broker::KawasanBroker broker(config(dir_, 1, ""));
        broker.start();
        EXPECT_EQ(initProducerId(broker.port()), 1);
        EXPECT_EQ(initProducerId(broker.port()), 2);
        broker.stop();
    }
    kawasan::broker::KawasanBroker broker(config(dir_, 1, ""));
    broker.start();
    EXPECT_EQ(initProducerId(broker.port()), 3);
    broker.stop();
}

TEST_F(ProducerIdAllocationTest, CounterFailureCannotAcknowledgeOrCreateTransactionalIdentity) {
    kawasan::broker::KawasanBroker broker(config(dir_, 1, ""));
    broker.start();
    const auto counter = std::filesystem::path(dir_) / "meta/producer_id.counter";
    std::filesystem::create_directory(counter);
    for (const std::string id : {"", "failed-transaction"}) {
        const auto response = allocationResponse(broker.port(), id);
        EXPECT_EQ(response.error, kawasan::ErrorCode::KAFKA_STORAGE_ERROR);
        EXPECT_EQ(response.producer_id, -1);
        EXPECT_EQ(response.producer_epoch, -1);
    }
    std::filesystem::remove(counter);
    // Failed allocations reserve no acknowledged identity or transaction.
    EXPECT_EQ(initProducerId(broker.port()), 1);
    const auto retry = allocationResponse(broker.port(), "failed-transaction");
    EXPECT_EQ(retry.error, kawasan::ErrorCode::NONE);
    EXPECT_EQ(retry.producer_id, 2);
    EXPECT_EQ(retry.producer_epoch, 0);
    broker.stop();
}

TEST_F(ProducerIdAllocationTest, ClusterBrokerIssuesIdsFromItsOwnRange) {
    // Broker 2 of a two-broker cluster whose peer never starts; InitProducerId
    // is answered locally.
    const std::string peers =
        "2:127.0.0.1:" + std::to_string(freePort()) + ",5:127.0.0.1:" + std::to_string(freePort());
    kawasan::broker::KawasanBroker broker(config(dir_, 2, peers));
    broker.start();
    EXPECT_EQ(initProducerId(broker.port()), kawasan::broker::clusterProducerId(2, 1));
    EXPECT_EQ(initProducerId(broker.port()), kawasan::broker::clusterProducerId(2, 2));
    broker.stop();
}

TEST_F(ProducerIdAllocationTest, ClusterCounterFailureRefusesIdentityAcrossRestart) {
    const std::string peers =
        "2:127.0.0.1:" + std::to_string(freePort()) + ",5:127.0.0.1:" + std::to_string(freePort());
    const auto cfg = config(dir_, 2, peers);
    const auto temporary = std::filesystem::path(dir_) / "meta/producer_id.counter.tmp";
    {
        kawasan::broker::KawasanBroker broker(cfg);
        broker.start();
        EXPECT_EQ(initProducerId(broker.port()), kawasan::broker::clusterProducerId(2, 1));
        std::filesystem::create_directory(temporary);
        const auto failed = allocationResponse(broker.port());
        EXPECT_EQ(failed.error, kawasan::ErrorCode::KAFKA_STORAGE_ERROR);
        EXPECT_EQ(failed.producer_id, -1);
        EXPECT_EQ(failed.producer_epoch, -1);
        broker.stop();
    }
    std::filesystem::remove(temporary);
    kawasan::broker::KawasanBroker restarted(cfg);
    restarted.start();
    EXPECT_EQ(initProducerId(restarted.port()), kawasan::broker::clusterProducerId(2, 2));
    restarted.stop();
}

// Actual peer sockets must be destroyed while their Asio context still lives.
TEST_F(ProducerIdAllocationTest, ConnectedClusterBrokersDestroyCleanly) {
    const int p0 = freePort();
    const int p1 = freePort();
    const std::string peers =
        "0:127.0.0.1:" + std::to_string(p0) + ",1:127.0.0.1:" + std::to_string(p1);
    auto c0 = config(dir_ + "/0", 0, peers);
    auto c1 = config(dir_ + "/1", 1, peers);
    c0.setInt("raft.port", p0);
    c1.setInt("raft.port", p1);
    c0.setBool("monitoring.enabled", false);
    c1.setBool("monitoring.enabled", false);
    kawasan::broker::KawasanBroker first(c0);
    kawasan::broker::KawasanBroker second(c1);
    first.start();
    second.start();
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (!(first.raftNode()->hasCurrentMetadata(1000) &&
             second.raftNode()->hasCurrentMetadata(1000)) &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    EXPECT_TRUE(first.raftNode()->hasCurrentMetadata(1000));
    EXPECT_TRUE(second.raftNode()->hasCurrentMetadata(1000));
    first.stop();
    second.stop();
}
