// M8-E2: Fetch (v9+) and ListOffsets (v4+) carry the client's current_leader_epoch.
// A request from a client whose epoch is older than the partition's gets
// FENCED_LEADER_EPOCH (it must refresh metadata); newer gets UNKNOWN_LEADER_EPOCH
// (this broker has not caught up yet); -1 opts out. Driven over a real socket
// against an in-process single-node broker whose partition epoch is bumped by a
// (self-)leader election.
#include <arpa/inet.h>
#include <gtest/gtest.h>

#include <array>
#include <boost/asio.hpp>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <string>
#include <vector>

#include "kawasan/broker/kawasan_broker.h"
#include "kawasan/broker/metadata_controller.h"
#include "kawasan/common/buffer.h"
#include "kawasan/common/config.h"
#include "kawasan/common/logger.h"
#include "kawasan/protocol/create_topics_request.h"
#include "kawasan/protocol/fetch_request.h"
#include "kawasan/protocol/list_offsets_request.h"
#include "kawasan/protocol/request_header.h"

namespace asio = boost::asio;
using kawasan::ErrorCode;

namespace {

constexpr const char* kTopic = "epoch-topic";

std::vector<uint8_t> roundTrip(asio::ip::tcp::socket& socket, kawasan::Buffer& payload) {
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
    return body;
}

class LeaderEpochFencingTest : public ::testing::Test {
protected:
    void SetUp() override {
        static const bool logger_ready = [] {
            kawasan::Logger::init("warn");
            return true;
        }();
        (void)logger_ready;
        const auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
        log_dir_ = (std::filesystem::temp_directory_path() / ("kawasan-epoch-" +
                                                              std::to_string(ts)))
                       .string();
        std::filesystem::create_directories(log_dir_);
        kawasan::Config config;
        config.setInt("broker.id", 1);
        config.setString("host", "127.0.0.1");
        config.setInt("port", 0);
        config.setInt("raft.port", 0);
        config.setString("log.dirs", log_dir_);
        config.setInt("network.io_threads", 1);
        broker_ = std::make_unique<kawasan::broker::KawasanBroker>(config);
        broker_->start();
        socket_ = std::make_unique<asio::ip::tcp::socket>(io_);
        socket_->connect(asio::ip::tcp::endpoint(asio::ip::make_address("127.0.0.1"),
                                                 static_cast<uint16_t>(broker_->port())));

        kawasan::Buffer payload;
        kawasan::protocol::RequestHeader header(kawasan::protocol::ApiKey::CREATE_TOPICS, 4,
                                                corr_++, "e2");
        header.encode(payload);
        kawasan::protocol::CreateTopicsRequest request;
        request.setTimeoutMs(5000);
        kawasan::protocol::CreatableTopic t;
        t.name = kTopic;
        t.num_partitions = 1;
        t.replication_factor = 1;
        request.addTopic(t);
        request.encode(payload, 4);
        (void)roundTrip(*socket_, payload);

        // Bump the partition epoch to 1 (re-electing the same, only replica).
        ASSERT_EQ(broker_->metadataController()->updatePartitionLeader(kTopic, 0, 1).error_code,
                  ErrorCode::NONE);
    }
    void TearDown() override {
        socket_->close();
        broker_->stop();
        std::filesystem::remove_all(log_dir_);
    }

    ErrorCode fetchWithEpoch(int32_t epoch) {
        constexpr int16_t kVersion = 11;
        kawasan::Buffer payload;
        kawasan::protocol::RequestHeader header(kawasan::protocol::ApiKey::FETCH, kVersion,
                                                corr_++, "e2");
        header.encode(payload);
        kawasan::protocol::FetchRequest request;
        request.setReplicaId(-1);
        request.setMaxWaitMs(0);
        request.setMinBytes(0);
        request.setMaxBytes(1024 * 1024);
        kawasan::protocol::FetchPartition fp;
        fp.partition = 0;
        fp.current_leader_epoch = epoch;
        fp.fetch_offset = 0;
        fp.partition_max_bytes = 1024 * 1024;
        kawasan::protocol::FetchTopic ft;
        ft.topic = kTopic;
        ft.partitions.push_back(fp);
        request.addTopic(ft);
        request.encode(payload, kVersion);
        auto body = roundTrip(*socket_, payload);
        kawasan::Buffer buf(body);
        kawasan::protocol::ResponseHeader rh;
        rh.decode(buf);
        kawasan::protocol::FetchResponse response;
        response.decode(buf, kVersion);
        return response.topics().at(0).partitions.at(0).error_code;
    }

    ErrorCode listOffsetsWithEpoch(int32_t epoch) {
        constexpr int16_t kVersion = 5;
        kawasan::Buffer payload;
        kawasan::protocol::RequestHeader header(kawasan::protocol::ApiKey::LIST_OFFSETS,
                                                kVersion, corr_++, "e2");
        header.encode(payload);
        kawasan::protocol::ListOffsetsRequest request;
        kawasan::protocol::ListOffsetsPartition lp;
        lp.partition = 0;
        lp.current_leader_epoch = epoch;
        lp.timestamp = -1;
        kawasan::protocol::ListOffsetsTopic lt;
        lt.topic = kTopic;
        lt.partitions.push_back(lp);
        request.addTopic(lt);
        request.encode(payload, kVersion);
        auto body = roundTrip(*socket_, payload);
        kawasan::Buffer buf(body);
        kawasan::protocol::ResponseHeader rh;
        rh.decode(buf);
        kawasan::protocol::ListOffsetsResponse response;
        response.decode(buf, kVersion);
        return response.topics().at(0).partitions.at(0).error_code;
    }

    std::string log_dir_;
    std::unique_ptr<kawasan::broker::KawasanBroker> broker_;
    asio::io_context io_;
    std::unique_ptr<asio::ip::tcp::socket> socket_;
    int32_t corr_ = 1;
};

}  // namespace

TEST_F(LeaderEpochFencingTest, FetchChecksCurrentLeaderEpoch) {
    EXPECT_EQ(fetchWithEpoch(-1), ErrorCode::NONE);
    EXPECT_EQ(fetchWithEpoch(1), ErrorCode::NONE);
    EXPECT_EQ(fetchWithEpoch(0), ErrorCode::FENCED_LEADER_EPOCH);
    EXPECT_EQ(fetchWithEpoch(2), ErrorCode::UNKNOWN_LEADER_EPOCH);
}

TEST_F(LeaderEpochFencingTest, ListOffsetsChecksCurrentLeaderEpoch) {
    EXPECT_EQ(listOffsetsWithEpoch(-1), ErrorCode::NONE);
    EXPECT_EQ(listOffsetsWithEpoch(1), ErrorCode::NONE);
    EXPECT_EQ(listOffsetsWithEpoch(0), ErrorCode::FENCED_LEADER_EPOCH);
    EXPECT_EQ(listOffsetsWithEpoch(2), ErrorCode::UNKNOWN_LEADER_EPOCH);
}
