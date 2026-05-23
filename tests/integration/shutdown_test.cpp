#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <thread>

#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/write.hpp>

#include "kawasan/broker/network/tcp_server.h"
#include "kawasan/broker/request_dispatcher.h"
#include "kawasan/common/buffer.h"

using namespace kawasan;
using namespace kawasan::broker;
using namespace kawasan::broker::network;

namespace {

// Simple mock dispatcher that simulates slow request processing
class MockSlowDispatcher : public RequestDispatcher {
public:
    MockSlowDispatcher(std::chrono::milliseconds delay)
        : delay_(delay), requests_processed_(0) {}

    DispatchResult dispatch(RequestContext context) override {
        requests_processed_.fetch_add(1, std::memory_order_relaxed);
        
        // Simulate slow processing
        std::this_thread::sleep_for(delay_);

        // Return a minimal response - just echo back a simple frame
        std::vector<uint8_t> frame = {0, 0, 0, 4, 0, 0, 0, 0};  // 4-byte body with zeros

        return DispatchResult{std::move(frame), false, false};
    }

    int64_t getRequestsProcessed() const {
        return requests_processed_.load(std::memory_order_relaxed);
    }

private:
    std::chrono::milliseconds delay_;
    std::atomic<int64_t> requests_processed_;
};

// Helper to send a simple request frame
void sendSimpleRequest(boost::asio::ip::tcp::socket& socket) {
    // Send a minimal request: 4-byte length (8) + 8 dummy bytes
    std::vector<uint8_t> frame = {0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 0};
    
    boost::system::error_code ec;
    boost::asio::write(socket, boost::asio::buffer(frame), ec);
    if (ec) {
        throw std::runtime_error("Failed to send request: " + ec.message());
    }
}

// Helper to receive and validate response
bool receiveResponse(boost::asio::ip::tcp::socket& socket, 
                     std::chrono::milliseconds timeout = std::chrono::milliseconds(5000)) {
    std::array<uint8_t, 4> size_buffer;
    
    // Set socket timeout
    socket.non_blocking(true);
    
    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        boost::system::error_code ec;
        size_t bytes = socket.read_some(boost::asio::buffer(size_buffer), ec);
        
        if (ec == boost::asio::error::would_block) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            continue;
        }
        
        if (ec) {
            return false;  // Connection closed or error
        }
        
        if (bytes == 4) {
            // Got size, now read the body (but we don't need to parse it for this test)
            uint32_t body_size = (static_cast<uint32_t>(size_buffer[0]) << 24) |
                                (static_cast<uint32_t>(size_buffer[1]) << 16) |
                                (static_cast<uint32_t>(size_buffer[2]) << 8) |
                                static_cast<uint32_t>(size_buffer[3]);
            
            std::vector<uint8_t> body(body_size);
            boost::asio::read(socket, boost::asio::buffer(body), ec);
            
            return !ec;  // Success if no error reading body
        }
    }
    
    return false;  // Timeout
}

}  // namespace

class ShutdownTest : public ::testing::Test {
protected:
    void SetUp() override {
        io_context_ = std::make_unique<boost::asio::io_context>();
    }

    void TearDown() override {
        if (server_) {
            server_->stop();
            server_.reset();
        }
        io_context_.reset();
    }

    std::unique_ptr<boost::asio::io_context> io_context_;
    std::unique_ptr<TcpServer> server_;
};

TEST_F(ShutdownTest, GracefulShutdownWaitsForInflightRequests) {
    // Create a dispatcher that processes requests slowly (500ms)
    auto dispatcher = std::make_shared<MockSlowDispatcher>(std::chrono::milliseconds(500));

    // Start server
    server_ = std::make_unique<TcpServer>(
        "localhost", 0, 2, 1024 * 1024, dispatcher);
    server_->start();
    uint16_t port = server_->listeningPort();

    // Connect client and send request
    boost::asio::ip::tcp::socket socket(*io_context_);
    boost::asio::ip::tcp::endpoint endpoint(
        boost::asio::ip::address::from_string("127.0.0.1"), port);
    
    boost::system::error_code ec;
    socket.connect(endpoint, ec);
    ASSERT_FALSE(ec) << "Failed to connect: " << ec.message();

    // Send request (will take 500ms to process)
    sendSimpleRequest(socket);

    // Wait a bit to ensure request is being processed
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Start graceful shutdown in background (with 2s drain timeout)
    auto start_shutdown = std::chrono::steady_clock::now();
    std::thread shutdown_thread([this]() {
        server_->stopGracefully(std::chrono::seconds(2));
    });

    // Try to receive the response (should succeed because graceful shutdown waits)
    bool got_response = receiveResponse(socket, std::chrono::milliseconds(2000));
    auto shutdown_duration = std::chrono::steady_clock::now() - start_shutdown;

    shutdown_thread.join();

    // Verify we got the response
    EXPECT_TRUE(got_response) << "Should have received response during graceful shutdown";
    
    // Verify shutdown took at least the request processing time
    auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(shutdown_duration).count();
    EXPECT_GE(duration_ms, 400) << "Graceful shutdown should wait for in-flight requests";
    
    // Verify request was actually processed
    EXPECT_EQ(dispatcher->getRequestsProcessed(), 1);
}

TEST_F(ShutdownTest, GracefulShutdownForcesCloseAfterTimeout) {
    // Create a dispatcher that processes requests very slowly (5s)
    auto dispatcher = std::make_shared<MockSlowDispatcher>(std::chrono::milliseconds(5000));

    // Start server
    server_ = std::make_unique<TcpServer>(
        "localhost", 0, 2, 1024 * 1024, dispatcher);
    server_->start();
    uint16_t port = server_->listeningPort();

    // Connect client and send request
    boost::asio::ip::tcp::socket socket(*io_context_);
    boost::asio::ip::tcp::endpoint endpoint(
        boost::asio::ip::address::from_string("127.0.0.1"), port);
    
    boost::system::error_code ec;
    socket.connect(endpoint, ec);
    ASSERT_FALSE(ec) << "Failed to connect: " << ec.message();

    // Send request (will take 5s to process)
    sendSimpleRequest(socket);

    // Wait a bit to ensure request is being processed
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Start graceful shutdown with short timeout (1s)
    auto start_shutdown = std::chrono::steady_clock::now();
    std::thread shutdown_thread([this]() {
        server_->stopGracefully(std::chrono::seconds(1));
    });

    shutdown_thread.join();
    auto shutdown_duration = std::chrono::steady_clock::now() - start_shutdown;

    // Verify shutdown completed within reasonable time (should force close after 1s)
    auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(shutdown_duration).count();
    EXPECT_LT(duration_ms, 2000) << "Shutdown should force close after drain timeout";
    EXPECT_GE(duration_ms, 1000) << "Shutdown should respect drain timeout";
    
    // Connection should be closed
    std::array<uint8_t, 1> dummy;
    size_t bytes = socket.read_some(boost::asio::buffer(dummy), ec);
    EXPECT_TRUE(ec || bytes == 0) << "Connection should be closed after forced shutdown";
}

TEST_F(ShutdownTest, GracefulShutdownRejectsNewConnections) {
    // Create a fast dispatcher
    auto dispatcher = std::make_shared<MockSlowDispatcher>(std::chrono::milliseconds(100));

    // Start server
    server_ = std::make_unique<TcpServer>(
        "localhost", 0, 2, 1024 * 1024, dispatcher);
    server_->start();
    uint16_t port = server_->listeningPort();

    // Connect first client
    boost::asio::ip::tcp::socket socket1(*io_context_);
    boost::asio::ip::tcp::endpoint endpoint(
        boost::asio::ip::address::from_string("127.0.0.1"), port);
    
    boost::system::error_code ec;
    socket1.connect(endpoint, ec);
    ASSERT_FALSE(ec) << "First connection should succeed";

    // Send request from first client
    sendSimpleRequest(socket1);

    // Start graceful shutdown
    std::thread shutdown_thread([this]() {
        server_->stopGracefully(std::chrono::seconds(3));
    });

    // Wait a bit to ensure shutdown has started
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // Try to connect second client (should fail or timeout)
    boost::asio::ip::tcp::socket socket2(*io_context_);
    socket2.connect(endpoint, ec);
    
    // Connection should fail (acceptor closed)
    EXPECT_TRUE(ec) << "New connections should be rejected during graceful shutdown";

    shutdown_thread.join();
}

TEST_F(ShutdownTest, ImmediateStopClosesAllConnections) {
    // Create a slow dispatcher
    auto dispatcher = std::make_shared<MockSlowDispatcher>(std::chrono::milliseconds(2000));

    // Start server
    server_ = std::make_unique<TcpServer>(
        "localhost", 0, 2, 1024 * 1024, dispatcher);
    server_->start();
    uint16_t port = server_->listeningPort();

    // Connect client and send request
    boost::asio::ip::tcp::socket socket(*io_context_);
    boost::asio::ip::tcp::endpoint endpoint(
        boost::asio::ip::address::from_string("127.0.0.1"), port);
    
    boost::system::error_code ec;
    socket.connect(endpoint, ec);
    ASSERT_FALSE(ec);

    sendSimpleRequest(socket);

    // Wait a bit
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Immediate stop (no graceful drain)
    auto start_stop = std::chrono::steady_clock::now();
    server_->stop();
    auto stop_duration = std::chrono::steady_clock::now() - start_stop;

    // Should stop quickly (within 500ms, not waiting for 2s request)
    auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(stop_duration).count();
    EXPECT_LT(duration_ms, 500) << "Immediate stop should not wait for requests";

    // Connection should be closed
    std::array<uint8_t, 1> dummy;
    size_t bytes = socket.read_some(boost::asio::buffer(dummy), ec);
    EXPECT_TRUE(ec || bytes == 0) << "Connection should be closed after immediate stop";
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
