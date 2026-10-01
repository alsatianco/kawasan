// PeerClient: the broker-to-broker client behind follower replication, ISR
// changes and (M8-F4) epoch-based divergence detection. Driven against an
// in-process single-node broker over real sockets.
#include "kawasan/broker/peer_client.h"

#include <gtest/gtest.h>

#include <boost/asio.hpp>
#include <chrono>
#include <filesystem>
#include <thread>

#include "kawasan/broker/kawasan_broker.h"
#include "kawasan/broker/metadata_controller.h"
#include "kawasan/client/producer.h"
#include "kawasan/common/config.h"
#include "kawasan/common/logger.h"
#include "kawasan/storage/log_manager.h"

namespace asio = boost::asio;
using kawasan::ErrorCode;
using kawasan::broker::PeerClient;

namespace {

constexpr kawasan::BrokerId kBrokerId = 1;
constexpr const char* kTopic = "peer-topic";

class PeerClientTest : public ::testing::Test {
protected:
    void SetUp() override {
        static const bool logger_ready = [] {
            kawasan::Logger::init("warn");
            return true;
        }();
        (void)logger_ready;
        const auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
        log_dir_ =
            (std::filesystem::temp_directory_path() / ("kawasan-peer-" + std::to_string(ts)))
                .string();
        std::filesystem::create_directories(log_dir_);
        kawasan::Config config;
        config.setInt("broker.id", kBrokerId);
        config.setString("host", "127.0.0.1");
        config.setInt("port", 0);
        config.setInt("raft.port", 0);
        config.setString("log.dirs", log_dir_);
        config.setInt("network.io_threads", 1);
        broker_ = std::make_unique<kawasan::broker::KawasanBroker>(config);
        broker_->start();

        kawasan::client::ProducerConfig pc;
        pc.bootstrap_servers = "127.0.0.1:" + std::to_string(broker_->port());
        pc.acks = 1;
        kawasan::client::Producer producer(pc);
        for (int i = 0; i < 3; ++i) {
            ASSERT_EQ(producer.send(kTopic, "", "v" + std::to_string(i)).get().offset, i);
        }
    }
    void TearDown() override {
        broker_->stop();
        std::filesystem::remove_all(log_dir_);
    }

    std::string log_dir_;
    std::unique_ptr<kawasan::broker::KawasanBroker> broker_;
};

}  // namespace

TEST_F(PeerClientTest, FetchesAsAFollower) {
    PeerClient client("127.0.0.1", broker_->port(), /*self=*/2);
    auto result = client.fetch(kTopic, 0, 0);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->error, ErrorCode::NONE);
    EXPECT_EQ(result->high_watermark, 3);
    EXPECT_FALSE(result->record_batches.empty());
}

// A peer that accepts the connection but never answers (frozen process,
// black-holed network) must fail the call within its deadline, not hang the
// replica-fetcher thread — which also runs the controller's failover sweep.
TEST(PeerClientDeadlineTest, UnresponsivePeerFailsInsteadOfHanging) {
    asio::io_context io;
    asio::ip::tcp::acceptor hole(io,
                                 asio::ip::tcp::endpoint(asio::ip::make_address("127.0.0.1"), 0));
    asio::ip::tcp::socket held(io);
    std::thread accepter([&] {
        boost::system::error_code ec;
        hole.accept(held, ec);
    });
    PeerClient client("127.0.0.1", hole.local_endpoint().port(), /*self=*/2);
    const auto start = std::chrono::steady_clock::now();
    EXPECT_FALSE(client.fetch(kTopic, 0, 0).has_value());
    EXPECT_LT(std::chrono::steady_clock::now() - start, std::chrono::seconds(8));
    hole.close();
    accepter.join();
}

