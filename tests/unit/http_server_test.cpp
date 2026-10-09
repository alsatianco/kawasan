#include <gtest/gtest.h>
#include <boost/asio.hpp>
#include <chrono>
#include <thread>

#include "kawasan/broker/monitoring/http_server.h"

using kawasan::broker::monitoring::HttpServer;
using namespace std::chrono_literals;

TEST(HttpServerTest, StopWithNoClientsDoesNotBlockInAccept) {
    HttpServer server("127.0.0.1", 0);
    server.start();
    ASSERT_TRUE(server.isRunning());
    std::this_thread::sleep_for(25ms);
    const auto started = std::chrono::steady_clock::now();
    server.stop();
    EXPECT_LT(std::chrono::steady_clock::now() - started, 3s);
    EXPECT_FALSE(server.isRunning());
}

TEST(HttpServerTest, StopWithAnIdleClientDoesNotBlockInRecv) {
    boost::asio::io_context io;
    boost::asio::ip::tcp::acceptor reservation(
        io, {boost::asio::ip::make_address("127.0.0.1"), 0});
    const auto endpoint = reservation.local_endpoint();
    reservation.close();
    HttpServer server("127.0.0.1", endpoint.port());
    server.start();
    ASSERT_TRUE(server.isRunning());
    boost::asio::ip::tcp::socket client(io);
    client.connect(endpoint);
    std::this_thread::sleep_for(25ms);
    const auto started = std::chrono::steady_clock::now();
    server.stop();
    EXPECT_LT(std::chrono::steady_clock::now() - started, 3s);
    EXPECT_FALSE(server.isRunning());
}
