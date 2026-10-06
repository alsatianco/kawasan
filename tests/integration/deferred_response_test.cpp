#include <gtest/gtest.h>

#include <boost/asio.hpp>
#include <chrono>
#include <filesystem>
#include <future>

#include "kawasan/broker/kawasan_broker.h"
#include "kawasan/broker/network/tcp_server.h"
#include "kawasan/broker/request_dispatcher.h"
#include "kawasan/common/logger.h"
#include "kawasan/protocol/fetch_request.h"
#include "kawasan/protocol/request_header.h"

using namespace kawasan;
using namespace kawasan::broker;
using namespace std::chrono_literals;
namespace asio = boost::asio;

namespace kawasan::broker {
struct CoordinatorExecutorProbe {
    static RequestDispatcher::DispatchResult dispatch(KawasanBroker& b,
                                                      RequestDispatcher::RequestContext context) {
        return b.request_dispatcher_->dispatch(std::move(context));
    }
};
}  // namespace kawasan::broker

TEST(DeferredResponseTest, CompletionOwnsMetricsWithoutRetainingDispatcher) {
    auto metrics = std::make_shared<metrics::RequestMetrics>();
    std::weak_ptr<metrics::RequestMetrics> weak = metrics;
    auto dispatcher = std::make_shared<RequestDispatcher>(metrics);
    std::function<void(RequestDispatcher::HandlerResult)> complete;
    dispatcher->registerHandler(protocol::ApiKey::HEARTBEAT, 0, 0, [&](auto& context) {
        complete = context.complete;
        RequestDispatcher::HandlerResult parked;
        parked.deferred = true;
        return parked;
    });
    int delivered = 0;
    RequestDispatcher::RequestContext context;
    context.header = {protocol::ApiKey::HEARTBEAT, 0, 7, "deferred-test"};
    context.frame_size_bytes = 20;
    context.deferred_sink = [&](auto) { ++delivered; };
    EXPECT_TRUE(dispatcher->dispatch(std::move(context)).deferred);
    dispatcher.reset();
    metrics.reset();
    ASSERT_FALSE(weak.expired());
    RequestDispatcher::HandlerResult response;
    response.payload.writeInt32(7);
    complete(response);
    complete(response);  // A cancelled/timed-out worker must not double-send.
    EXPECT_EQ(delivered, 1);
    EXPECT_EQ(weak.lock()->snapshot(protocol::ApiKey::HEARTBEAT).requests, 1);
    complete = {};
    EXPECT_TRUE(weak.expired());
}

TEST(DeferredResponseTest, RetainedCompletionIsHarmlessAfterServerDestructionAndRestart) {
    Logger::init("warn");
    auto dispatcher = std::make_shared<RequestDispatcher>(nullptr);
    std::promise<std::function<void(RequestDispatcher::HandlerResult)>> captured;
    dispatcher->registerHandler(protocol::ApiKey::HEARTBEAT, 0, 0, [&](auto& context) {
        captured.set_value(context.complete);
        RequestDispatcher::HandlerResult parked;
        parked.deferred = true;
        return parked;
    });
    asio::io_context io;
    asio::ip::tcp::socket socket(io);
    std::function<void(RequestDispatcher::HandlerResult)> complete;
    {
        network::TcpServer server("127.0.0.1", 0, 2, 1024, dispatcher);
        server.start();
        socket.connect({asio::ip::make_address("127.0.0.1"), server.listeningPort()});
        Buffer payload;
        protocol::RequestHeader(protocol::ApiKey::HEARTBEAT, 0, 7, "test").encode(payload);
        Buffer frame;
        frame.writeInt32(payload.size());
        frame.writeBytes(payload.data(), payload.size());
        asio::write(socket, asio::buffer(frame.vector()));
        auto future = captured.get_future();
        ASSERT_EQ(future.wait_for(2s), std::future_status::ready);
        complete = future.get();
        server.stop();
        server.start();
        server.stop();
    }
    dispatcher.reset();
    RequestDispatcher::HandlerResult response;
    response.payload.writeInt32(7);
    complete(response);
    complete = {};
}

TEST(DeferredResponseTest, StopCancelsSlowWriteAndReleasesResponseLifetime) {
    auto dispatcher = std::make_shared<RequestDispatcher>(nullptr);
    std::promise<std::function<void(RequestDispatcher::HandlerResult)>> captured;
    dispatcher->registerHandler(protocol::ApiKey::HEARTBEAT, 0, 0, [&](auto& context) {
        captured.set_value(context.complete);
        RequestDispatcher::HandlerResult parked;
        parked.deferred = true;
        return parked;
    });
    network::SocketTuning tuning;
    tuning.send_buffer_bytes = 4096;
    network::TcpServer server("127.0.0.1", 0, 2, 1024, dispatcher, nullptr, 0s, {}, tuning);
    server.start();
    asio::io_context io;
    asio::ip::tcp::socket socket(io);
    socket.connect({asio::ip::make_address("127.0.0.1"), server.listeningPort()});
    socket.set_option(asio::socket_base::receive_buffer_size(4096));
    Buffer payload;
    protocol::RequestHeader(protocol::ApiKey::HEARTBEAT, 0, 7, "slow-reader").encode(payload);
    Buffer frame;
    frame.writeInt32(payload.size());
    frame.writeBytes(payload.data(), payload.size());
    asio::write(socket, asio::buffer(frame.vector()));
    auto future = captured.get_future();
    ASSERT_EQ(future.wait_for(2s), std::future_status::ready);
    auto complete = future.get();
    std::promise<void> publishing;
    auto ticket = std::make_shared<int>(1);
    std::weak_ptr<int> weak = ticket;
    RequestDispatcher::HandlerResult response;
    response.lifetime = std::move(ticket);
    response.payload.writeBytes(std::vector<uint8_t>(8 * 1024 * 1024, 0));
    response.publication_guard = [&](const auto& publish) {
        publishing.set_value();
        publish(ErrorCode::NONE);
    };
    complete(std::move(response));
    ASSERT_EQ(publishing.get_future().wait_for(2s), std::future_status::ready);
    EXPECT_FALSE(weak.expired());
    server.stop();  // Client never reads; cancel the write rather than wait for it.
    EXPECT_TRUE(weak.expired());
    complete = {};
}

TEST(DeferredResponseTest, StopDrainsPublicationAlreadyRunningOnIoThread) {
    auto dispatcher = std::make_shared<RequestDispatcher>(nullptr);
    std::promise<void> entered, release;
    auto released = release.get_future().share();
    dispatcher->registerHandler(protocol::ApiKey::HEARTBEAT, 0, 0, [&](auto& context) {
        RequestDispatcher::HandlerResult result;
        result.payload.writeInt32(context.header.correlationId());
        result.publication_guard = [&](const auto& publish) {
            entered.set_value();
            released.wait();
            publish(ErrorCode::NOT_COORDINATOR);
        };
        result.publication_error = [](ErrorCode code) {
            Buffer payload;
            payload.writeInt32(7);
            payload.writeInt16(static_cast<int16_t>(code));
            return payload;
        };
        return result;
    });
    network::TcpServer server("127.0.0.1", 0, 1, 1024, dispatcher);
    server.start();
    asio::io_context io;
    asio::ip::tcp::socket socket(io);
    socket.connect({asio::ip::make_address("127.0.0.1"), server.listeningPort()});
    Buffer payload;
    protocol::RequestHeader(protocol::ApiKey::HEARTBEAT, 0, 7, "publication").encode(payload);
    Buffer frame;
    frame.writeInt32(payload.size());
    frame.writeBytes(payload.data(), payload.size());
    asio::write(socket, asio::buffer(frame.vector()));
    EXPECT_EQ(entered.get_future().wait_for(2s), std::future_status::ready);
    auto stopped = std::async(std::launch::async, [&] { server.stop(); });
    EXPECT_EQ(stopped.wait_for(50ms), std::future_status::timeout);
    release.set_value();
    EXPECT_EQ(stopped.wait_for(2s), std::future_status::ready);
    stopped.get();
}

TEST(DeferredResponseTest, ParkedFetchSnapshotsConnectionPrincipalForLaterAuthorization) {
    const auto dir = std::filesystem::temp_directory_path() /
                     ("kawasan-deferred-principal-" +
                      std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    Config config;
    config.setInt("broker.id", 0);
    config.setInt("port", 0);
    config.setInt("raft.port", 0);
    config.setBool("monitoring.enabled", false);
    config.setString("log.dirs", dir.string());
    config.setBool("authorizer.enabled", true);
    config.setString("super.users", "User:reader");
    KawasanBroker broker(config);
    broker.start();
    TopicSpecification topic;
    topic.name = "principal-snapshot";
    topic.num_partitions = 1;
    topic.replication_factor = 1;
    ASSERT_EQ(broker.metadataController()->createTopic(topic).error_code, ErrorCode::NONE);
    protocol::FetchRequest fetch;
    fetch.setReplicaId(-1);
    fetch.setMaxWaitMs(200);
    fetch.setMinBytes(1);
    fetch.setMaxBytes(1024);
    protocol::FetchPartition partition;
    partition.partition = 0;
    partition.fetch_offset = 0;
    partition.partition_max_bytes = 1024;
    protocol::FetchTopic fetch_topic;
    fetch_topic.topic = topic.name;
    fetch_topic.partitions.push_back(partition);
    fetch.addTopic(fetch_topic);
    RequestDispatcher::RequestContext context;
    context.header = {protocol::ApiKey::FETCH, 4, 7, "principal-test"};
    fetch.encode(context.payload, 4);
    RequestDispatcher::ConnectionContext connection;
    connection.authenticated_principal = "User:reader";
    context.connection = &connection;
    std::promise<RequestDispatcher::DispatchResult> delivered;
    context.deferred_sink = [&](auto result) { delivered.set_value(std::move(result)); };
    ASSERT_TRUE(CoordinatorExecutorProbe::dispatch(broker, std::move(context)).deferred);
    // Emulate connection teardown/reuse without dereferencing released memory.
    connection.authenticated_principal = "User:replacement";
    auto future = delivered.get_future();
    ASSERT_EQ(future.wait_for(2s), std::future_status::ready);
    Buffer body(future.get().frame);
    (void)body.readInt32();
    (void)body.readInt32();
    protocol::FetchResponse response;
    response.decode(body, 4);
    ASSERT_EQ(response.topics().size(), 1);
    EXPECT_EQ(response.topics()[0].partitions[0].error_code, ErrorCode::NONE);
    broker.stop();
    std::filesystem::remove_all(dir);
}
