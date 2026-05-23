#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/asio/steady_timer.hpp>

#include "kawasan/broker/request_dispatcher.h"
#include "kawasan/common/types.h"

namespace kawasan::broker::monitoring { class MetricsCollector; }

namespace kawasan::broker::network {

/// @brief TLS configuration for TCP server
struct TlsConfig {
    bool enabled = false;
    std::string cert_file;
    std::string key_file;
    std::string ca_file;
    std::string key_password;
    bool verify_client = false;
};

/// @brief TCP server that accepts Kafka protocol connections and emits UNSUPPORTED_VERSION responses.
class TcpServer {
public:
    TcpServer(std::string host, int32_t port, size_t io_threads,
              size_t max_frame_size_bytes,
              std::shared_ptr<RequestDispatcher> dispatcher,
              std::shared_ptr<kawasan::broker::monitoring::MetricsCollector> metrics = nullptr,
              std::chrono::seconds idle_timeout = std::chrono::seconds(600),
              TlsConfig tls_config = TlsConfig{});
    ~TcpServer();

    TcpServer(const TcpServer&) = delete;
    TcpServer& operator=(const TcpServer&) = delete;

    void start();
    void stop();
    void stopGracefully(std::chrono::seconds drain_timeout = std::chrono::seconds(30));

    /// @brief Returns the port the server is bound to (useful when config port was 0).
    uint16_t listeningPort() const;

private:
    class TcpSession;

    using tcp = boost::asio::ip::tcp;
    using WorkGuard = boost::asio::executor_work_guard<boost::asio::io_context::executor_type>;

    void doAccept(tcp::acceptor* acceptor);
    std::vector<tcp::endpoint> resolveEndpoints();
    static bool isLocalhost(const std::string& host);
    RequestDispatcher::DispatchResult dispatchRequest(
        RequestDispatcher::RequestContext context);
    void trackSession(const std::shared_ptr<TcpSession>& session);
    void untrackSession(TcpSession* session);
    void startIdleCheckTimer();
    void checkIdleSessions();
    void recordBytesIn(int64_t bytes);
    void recordBytesOut(int64_t bytes);
    void recordConnectionError();

    std::string raw_host_;
    std::string host_;
    int32_t configured_port_;
    size_t io_threads_;
    size_t max_frame_size_bytes_;
    std::shared_ptr<RequestDispatcher> dispatcher_;
    std::shared_ptr<kawasan::broker::monitoring::MetricsCollector> metrics_;
    std::chrono::seconds idle_timeout_;
    std::unique_ptr<boost::asio::steady_timer> idle_check_timer_;
    std::atomic<int64_t> total_connections_{0};
    std::atomic<int64_t> connection_errors_{0};

    // TLS support
    TlsConfig tls_config_;
    std::unique_ptr<boost::asio::ssl::context> ssl_context_;

    mutable std::mutex state_mutex_;
    boost::asio::io_context io_context_;
    std::vector<std::unique_ptr<tcp::acceptor>> acceptors_;
    std::unique_ptr<WorkGuard> work_guard_;
    std::vector<std::thread> workers_;
    std::atomic<bool> running_{false};

    std::mutex sessions_mutex_;
    std::unordered_map<TcpSession*, std::shared_ptr<TcpSession>> sessions_;
};

}  // namespace kawasan::broker::network
