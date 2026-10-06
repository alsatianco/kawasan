#include "kawasan/broker/network/tcp_server.h"

#include <arpa/inet.h>
#include <spdlog/fmt/fmt.h>

#include <algorithm>
#include <array>
#include <boost/asio/bind_executor.hpp>
#include <boost/asio/error.hpp>
#include <boost/asio/ip/address_v4.hpp>
#include <boost/asio/ip/address_v6.hpp>
#include <boost/asio/ip/v6_only.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/write.hpp>
#include <cctype>
#include <chrono>
#include <cstring>
#include <future>
#include <optional>
#include <stdexcept>

#include "kawasan/broker/monitoring/metrics_collector.h"
#include "kawasan/common/buffer.h"
#include "kawasan/common/logger.h"
#include "kawasan/protocol/request_header.h"

namespace kawasan::broker::network {

namespace {

constexpr size_t kDefaultMaxFrameBytes = 16 * 1024 * 1024;  // 16MB

std::string normalizeHost(std::string host) {
    if (host.empty()) {
        return "::";
    }
    if (host.front() == '[' && host.back() == ']') {
        return host.substr(1, host.size() - 2);
    }
    if (host == "*") {
        return "::";
    }
    return host;
}

int32_t readBigEndianInt32(const std::array<uint8_t, 4>& buffer) {
    uint32_t net_value = 0;
    std::memcpy(&net_value, buffer.data(), sizeof(net_value));
    return ntohl(net_value);
}

}  // namespace

void applySocketTuning(boost::asio::ip::tcp::socket& socket, const SocketTuning& tuning) {
    boost::system::error_code ec;
    socket.set_option(boost::asio::ip::tcp::no_delay(tuning.no_delay), ec);
    if (ec) {
        Logger::warn("Failed to set TCP_NODELAY: {}", ec.message());
    }
    socket.set_option(boost::asio::socket_base::keep_alive(tuning.keep_alive), ec);
    if (ec) {
        Logger::warn("Failed to set SO_KEEPALIVE: {}", ec.message());
    }
    if (tuning.send_buffer_bytes > 0) {
        socket.set_option(boost::asio::socket_base::send_buffer_size(tuning.send_buffer_bytes), ec);
        if (ec) {
            Logger::warn("Failed to set SO_SNDBUF to {}: {}", tuning.send_buffer_bytes,
                         ec.message());
        }
    }
    if (tuning.recv_buffer_bytes > 0) {
        socket.set_option(boost::asio::socket_base::receive_buffer_size(tuning.recv_buffer_bytes),
                          ec);
        if (ec) {
            Logger::warn("Failed to set SO_RCVBUF to {}: {}", tuning.recv_buffer_bytes,
                         ec.message());
        }
    }
}

class TcpServer::TcpSession : public std::enable_shared_from_this<TcpSession> {
public:
    TcpSession(boost::asio::ip::tcp::socket socket, TcpServer& server, size_t max_frame_size)
        : socket_(std::move(socket)),
          server_(server),
          strand_(boost::asio::make_strand(server.io_context_)),
          max_frame_size_(std::max<size_t>(1, max_frame_size)) {
        updateActivity();
    }

    ~TcpSession() = default;

    void requestStop(
        monitoring::ConnectionCloseReason reason = monitoring::ConnectionCloseReason::kNormal) {
        boost::asio::post(strand_, [self = shared_from_this(), reason] { self->stop(reason); });
    }

    void startOnStrand() {
        boost::asio::post(strand_, [self = shared_from_this()] { self->start(); });
    }

    void start() {
        if (stopped_)
            return;
        peer_identity_ = describePeer();
        Logger::info("Accepted client connection from {}", peer_identity_);
        updateActivity();
        readFrameSize();
    }

    std::chrono::steady_clock::time_point getLastActivity() const {
        return std::chrono::steady_clock::time_point(
            std::chrono::steady_clock::duration(last_activity_.load()));
    }

    void stop(
        monitoring::ConnectionCloseReason reason = monitoring::ConnectionCloseReason::kNormal) {
        bool expected = false;
        if (!stopped_.compare_exchange_strong(expected, true)) {
            return;
        }

        Logger::debug("Stopping connection {}", peer_identity_);
        server_.recordConnectionClosed(reason);
        boost::system::error_code ec;
        socket_.close(ec);
        server_.untrackSession(this);
    }

private:
    void updateActivity() {
        last_activity_.store(std::chrono::steady_clock::now().time_since_epoch().count());
    }

    void readFrameSize() {
        if (stopped_)
            return;
        auto self = shared_from_this();
        boost::asio::async_read(
            socket_, boost::asio::buffer(size_buffer_),
            boost::asio::bind_executor(strand_, [self](const boost::system::error_code& ec,
                                                       std::size_t bytes_transferred) {
                if (self->stopped_)
                    return;
                if (ec) {
                    self->handleError("reading frame size", ec);
                    return;
                }

                if (bytes_transferred != self->size_buffer_.size()) {
                    self->handleProtocolError("incomplete frame size");
                    return;
                }

                self->updateActivity();
                self->server_.recordBytesIn(bytes_transferred);

                int32_t frame_length = readBigEndianInt32(self->size_buffer_);
                if (frame_length <= 0 ||
                    static_cast<size_t>(frame_length) > self->max_frame_size_) {
                    self->handleProtocolError(fmt::format("invalid frame size {} (max {})",
                                                          frame_length, self->max_frame_size_));
                    return;
                }

                self->frame_buffer_.assign(frame_length, 0);
                self->readFramePayload();
            }));
    }

    void readFramePayload() {
        auto self = shared_from_this();
        boost::asio::async_read(
            socket_, boost::asio::buffer(frame_buffer_),
            boost::asio::bind_executor(strand_, [self](const boost::system::error_code& ec,
                                                       std::size_t bytes_transferred) {
                if (self->stopped_)
                    return;
                if (ec) {
                    self->handleError("reading frame payload", ec);
                    return;
                }

                if (bytes_transferred != self->frame_buffer_.size()) {
                    self->handleProtocolError("incomplete frame payload");
                    return;
                }

                self->updateActivity();
                self->server_.recordBytesIn(bytes_transferred);
                self->processRequest();
            }));
    }

    void processRequest() {
        const size_t frame_size = frame_buffer_.size();
        Buffer buffer(std::move(frame_buffer_));
        protocol::RequestHeader header;

        try {
            header.decode(buffer);
        } catch (const std::exception& ex) {
            const auto& raw = buffer.vector();
            const size_t dump_len = std::min<size_t>(raw.size(), 32);
            std::string hex_dump;
            for (size_t i = 0; i < dump_len; ++i) {
                fmt::format_to(std::back_inserter(hex_dump), "{:02x} ", raw[i]);
            }
            Logger::warn("Failed to decode request header from {}: {} (api_key={}, "
                         "version={}, frame_size={}, first_bytes={})",
                         peer_identity_, ex.what(), static_cast<int16_t>(header.apiKey()),
                         header.apiVersion(), buffer.size(), hex_dump);
            handleProtocolError(fmt::format("invalid request header: {}", ex.what()));
            return;
        }

        Logger::debug("Client {} request api_key={} version={} correlation_id={} client_id={}",
                      peer_identity_, static_cast<int16_t>(header.apiKey()), header.apiVersion(),
                      header.correlationId(), header.clientId());

        RequestDispatcher::RequestContext context;
        context.header = header;
        context.payload = std::move(buffer);
        context.frame_size_bytes = frame_size;
        context.peer_identity = peer_identity_;
        context.connection = &conn_state_;

        context.deferred_sink = [weak = weak_from_this(), gate = server_.delivery_gate_](
                                    RequestDispatcher::DispatchResult result) {
            // A retained completion never owns a socket/executor. Stop seals
            // posting before draining IO, and old generations stay sealed.
            std::lock_guard<std::mutex> lock(gate->mutex);
            if (!gate->accepting)
                return;
            if (auto self = weak.lock())
                boost::asio::post(self->strand_, [self, result = std::move(result)]() mutable {
                    self->deliver(std::move(result));
                });
        };

        auto dispatch_result = server_.dispatchRequest(std::move(context));
        if (dispatch_result.deferred) {
            return;
        }
        deliver(std::move(dispatch_result));
    }

    void deliver(RequestDispatcher::DispatchResult dispatch_result) {
        if (stopped_) {
            return;
        }
        if (dispatch_result.publication_guard) {
            auto guard = std::move(dispatch_result.publication_guard);
            guard([&](ErrorCode status) {
                if (status != ErrorCode::NONE) {
                    if (!dispatch_result.publication_error) {
                        stop();
                        return;
                    }
                    dispatch_result.frame = dispatch_result.publication_error(status);
                    dispatch_result.suppress_response = false;
                }
                deliverImpl(std::move(dispatch_result));
            });
            return;
        }
        deliverImpl(std::move(dispatch_result));
    }

    void deliverImpl(RequestDispatcher::DispatchResult dispatch_result) {
        const bool close_after_write = dispatch_result.close_connection;

        if (close_after_write) {
            Logger::info("Connection {} will be closed after response", peer_identity_);
        }

        if (dispatch_result.suppress_response) {
            if (close_after_write) {
                Logger::info("Closing connection {} (suppressed response, close requested)",
                             peer_identity_);
                stop();
                return;
            }
            readFrameSize();
            return;
        }

        auto response = std::make_shared<std::vector<uint8_t>>(std::move(dispatch_result.frame));

        auto self = shared_from_this();
        boost::asio::async_write(
            socket_, boost::asio::buffer(*response),
            boost::asio::bind_executor(
                strand_,
                [self, response, close_after_write, lifetime = std::move(dispatch_result.lifetime)](
                    const boost::system::error_code& ec, std::size_t bytes_transferred) {
                    (void)lifetime;
                    if (self->stopped_)
                        return;
                    if (ec) {
                        self->handleError("writing response", ec);
                        return;
                    }
                    self->updateActivity();
                    self->server_.recordBytesOut(bytes_transferred);
                    if (close_after_write) {
                        Logger::info("Connection {} closed after sending response (as requested)",
                                     self->peer_identity_);
                        self->stop();
                        return;
                    }
                    self->readFrameSize();
                }));
    }

    void handleError(const std::string& context, const boost::system::error_code& ec) {
        if (stopped_) {
            return;
        }
        if (isGracefulDisconnect(ec)) {
            Logger::info("Connection {} closed while {} ({})", peer_identity_, context,
                         ec.message());
            stop(monitoring::ConnectionCloseReason::kNormal);
            return;
        } else {
            Logger::warn("Connection {} error {}: {}", peer_identity_, context, ec.message());
            server_.recordConnectionError();
        }
        stop(monitoring::ConnectionCloseReason::kIoError);
    }

    void handleProtocolError(const std::string& message) {
        Logger::warn("Protocol error from {}: {}", peer_identity_, message);
        stop(monitoring::ConnectionCloseReason::kProtocolError);
    }

    // Client-side closes are normal (e.g. tools exiting after a single request);
    // treat these connection errors as informational to avoid spamming warnings.
    static bool isGracefulDisconnect(const boost::system::error_code& ec) {
        using boost::asio::error::connection_aborted;
        using boost::asio::error::connection_reset;
        using boost::asio::error::eof;
        using boost::asio::error::operation_aborted;
        return ec == eof || ec == connection_reset || ec == connection_aborted ||
               ec == operation_aborted;
    }

    std::string describePeer() const {
        boost::system::error_code ec;
        auto endpoint = socket_.remote_endpoint(ec);
        if (ec) {
            return "unknown";
        }
        return fmt::format("{}:{}", endpoint.address().to_string(), endpoint.port());
    }

    boost::asio::ip::tcp::socket socket_;
    TcpServer& server_;
    boost::asio::strand<boost::asio::io_context::executor_type> strand_;
    size_t max_frame_size_;
    std::atomic<bool> stopped_{false};
    std::array<uint8_t, 4> size_buffer_{};
    std::vector<uint8_t> frame_buffer_;
    std::string peer_identity_;
    std::atomic<std::chrono::steady_clock::rep> last_activity_{0};
    // Per-connection state (authenticated principal, etc.). Lives for the
    // lifetime of this session and is handed to each request via the context.
    RequestDispatcher::ConnectionContext conn_state_;
};

bool TcpServer::isLocalhost(const std::string& host) {
    std::string lowered(host.size(), '\0');
    std::transform(host.begin(), host.end(), lowered.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return lowered == "localhost";
}

std::vector<TcpServer::tcp::endpoint> TcpServer::resolveEndpoints() {
    std::vector<tcp::endpoint> endpoints;
    const uint16_t port = static_cast<uint16_t>(configured_port_);

    if (isLocalhost(raw_host_)) {
        endpoints.emplace_back(boost::asio::ip::address_v6::loopback(), port);
        endpoints.emplace_back(boost::asio::ip::address_v4::loopback(), port);
        return endpoints;
    }

    // Handle "0.0.0.0" or "::" to bind to all interfaces
    // Use IPv6 any address with v6_only(false) to listen on both IPv4 and IPv6
    if (host_ == "0.0.0.0" || host_ == "::") {
        endpoints.emplace_back(boost::asio::ip::address_v6::any(), port);
        return endpoints;
    }

    boost::system::error_code ec;
    boost::asio::ip::address address = boost::asio::ip::make_address(host_, ec);
    if (!ec) {
        endpoints.emplace_back(address, port);
        return endpoints;
    }

    tcp::resolver resolver(io_context_);
    auto results = resolver.resolve(host_, std::to_string(port), ec);
    if (ec || results.begin() == results.end()) {
        throw std::runtime_error(fmt::format("Failed to resolve host {}: {}", host_, ec.message()));
    }

    for (const auto& entry : results) {
        auto endpoint = entry.endpoint();
        endpoint.port(port);
        endpoints.push_back(endpoint);
    }
    return endpoints;
}

TcpServer::TcpServer(std::string host, int32_t port, size_t io_threads, size_t max_frame_size_bytes,
                     std::shared_ptr<RequestDispatcher> dispatcher,
                     std::shared_ptr<kawasan::broker::monitoring::MetricsCollector> metrics,
                     std::chrono::seconds idle_timeout, TlsConfig tls_config,
                     SocketTuning socket_tuning)
    : raw_host_(std::move(host)),
      host_(normalizeHost(raw_host_)),
      configured_port_(port),
      io_threads_(std::max<size_t>(1, io_threads)),
      max_frame_size_bytes_(max_frame_size_bytes == 0 ? kDefaultMaxFrameBytes
                                                      : max_frame_size_bytes),
      dispatcher_(std::move(dispatcher)),
      metrics_(std::move(metrics)),
      idle_timeout_(idle_timeout),
      tls_config_(std::move(tls_config)),
      socket_tuning_(socket_tuning) {
    // Initialize SSL context if TLS is enabled
    if (tls_config_.enabled) {
        ssl_context_ =
            std::make_unique<boost::asio::ssl::context>(boost::asio::ssl::context::tlsv12_server);

        boost::system::error_code ec;

        // Load server certificate
        ssl_context_->use_certificate_chain_file(tls_config_.cert_file, ec);
        if (ec) {
            throw std::runtime_error(fmt::format("Failed to load certificate file {}: {}",
                                                 tls_config_.cert_file, ec.message()));
        }

        // Load private key
        if (!tls_config_.key_password.empty()) {
            ssl_context_->set_password_callback(
                [pw = tls_config_.key_password](
                    std::size_t, boost::asio::ssl::context::password_purpose) { return pw; });
        }
        ssl_context_->use_private_key_file(tls_config_.key_file, boost::asio::ssl::context::pem,
                                           ec);
        if (ec) {
            throw std::runtime_error(fmt::format("Failed to load private key file {}: {}",
                                                 tls_config_.key_file, ec.message()));
        }

        // Load CA file if provided
        if (!tls_config_.ca_file.empty()) {
            ssl_context_->load_verify_file(tls_config_.ca_file, ec);
            if (ec) {
                throw std::runtime_error(fmt::format("Failed to load CA file {}: {}",
                                                     tls_config_.ca_file, ec.message()));
            }
        }

        // Configure client verification
        if (tls_config_.verify_client) {
            ssl_context_->set_verify_mode(boost::asio::ssl::verify_peer |
                                          boost::asio::ssl::verify_fail_if_no_peer_cert);
        } else {
            ssl_context_->set_verify_mode(boost::asio::ssl::verify_none);
        }

        Logger::info("TLS enabled for TCP server (cert: {}, verify_client: {})",
                     tls_config_.cert_file, tls_config_.verify_client);
    }
}

TcpServer::~TcpServer() {
    stop();
}

void TcpServer::start() {
    std::lock_guard<std::mutex> lock(state_mutex_);
    if (running_) {
        return;
    }

    io_context_.restart();
    delivery_gate_ = std::make_shared<DeliveryGate>();
    auto endpoints = resolveEndpoints();
    if (endpoints.empty()) {
        throw std::runtime_error("No endpoints resolved for TCP server");
    }

    boost::system::error_code ec;
    std::optional<uint16_t> bound_port;
    acceptors_.clear();
    acceptors_.reserve(endpoints.size());
    for (auto endpoint : endpoints) {
        if (bound_port) {
            endpoint.port(*bound_port);
        }

        auto acceptor = std::make_unique<tcp::acceptor>(io_context_);
        acceptor->open(endpoint.protocol(), ec);
        if (ec) {
            throw std::runtime_error(fmt::format("Failed to open acceptor: {}", ec.message()));
        }

        acceptor->set_option(tcp::acceptor::reuse_address(true));
        if (endpoint.protocol() == tcp::v6()) {
            acceptor->set_option(boost::asio::ip::v6_only(false));
        }

        acceptor->bind(endpoint, ec);
        if (ec) {
            throw std::runtime_error(fmt::format("Failed to bind {}:{} - {}",
                                                 endpoint.address().to_string(), endpoint.port(),
                                                 ec.message()));
        }

        acceptor->listen(boost::asio::socket_base::max_listen_connections, ec);
        if (ec) {
            throw std::runtime_error(fmt::format("Failed to listen on {}:{} - {}",
                                                 endpoint.address().to_string(), endpoint.port(),
                                                 ec.message()));
        }

        auto actual_port = acceptor->local_endpoint().port();
        if (!bound_port) {
            bound_port = actual_port;
        } else if (*bound_port != actual_port) {
            throw std::runtime_error("Listener endpoints bound to different ports");
        }

        Logger::info("TCP listener bound to {}:{}", endpoint.address().to_string(),
                     endpoint.port());
        acceptors_.push_back(std::move(acceptor));
    }

    configured_port_ = bound_port.value_or(endpoints.front().port());
    io_context_.restart();
    work_guard_ = std::make_unique<WorkGuard>(boost::asio::make_work_guard(io_context_));

    running_ = true;
    for (auto& acceptor : acceptors_)
        doAccept(acceptor.get());
    startIdleCheckTimer();
    workers_.reserve(io_threads_);
    for (size_t i = 0; i < io_threads_; ++i) {
        workers_.emplace_back([this]() {
            for (;;) {
                try {
                    io_context_.run();
                    break;
                } catch (const std::exception& ex) {
                    Logger::error("I/O worker exception: {}", ex.what());
                } catch (...) {
                    Logger::error("I/O worker failed with an unknown exception");
                }
            }
        });
    }
}

void TcpServer::stop() {
    std::unique_lock<std::mutex> lock(state_mutex_);
    if (!running_)
        return;
    running_ = false;
    {
        std::lock_guard<std::mutex> gate_lock(delivery_gate_->mutex);
        delivery_gate_->accepting = false;
    }
    // Cancel each socket on its own strand. Let run() drain composed operations
    // and posted deliveries; io_context::stop would leave dangling sessions.
    boost::asio::post(control_, [this] {
        if (idle_check_timer_)
            idle_check_timer_->cancel();
        boost::system::error_code ec;
        for (auto& acceptor : acceptors_)
            acceptor->close(ec);
        std::vector<std::shared_ptr<TcpSession>> sessions;
        {
            std::lock_guard<std::mutex> sessions_lock(sessions_mutex_);
            for (auto& [_, session] : sessions_)
                sessions.push_back(session);
        }
        for (auto& session : sessions)
            session->requestStop();
        work_guard_->reset();
    });
    // Serialize stop/start until all handlers that reference this server drain.
    for (auto& worker : workers_)
        if (worker.joinable())
            worker.join();
    workers_.clear();
    idle_check_timer_.reset();
    acceptors_.clear();
    work_guard_.reset();
}

void TcpServer::stopGracefully(std::chrono::seconds drain_timeout) {
    Logger::info("Initiating graceful shutdown with {}s drain timeout", drain_timeout.count());

    std::unique_lock<std::mutex> lock(state_mutex_);
    if (!running_) {
        return;
    }

    auto closed = std::make_shared<std::promise<void>>();
    auto ready = closed->get_future();
    boost::asio::post(control_, [this, closed] {
        boost::system::error_code ec;
        for (auto& acceptor : acceptors_)
            acceptor->close(ec);
        closed->set_value();
    });
    ready.wait();
    lock.unlock();

    // Wait for existing connections to finish (with timeout)
    auto deadline = std::chrono::steady_clock::now() + drain_timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        size_t active_count;
        {
            std::lock_guard<std::mutex> sessions_lock(sessions_mutex_);
            active_count = sessions_.size();
        }

        if (active_count == 0) {
            Logger::info("All connections drained, shutting down");
            break;
        }

        Logger::debug("Waiting for {} active connections to drain", active_count);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    // Force close any remaining connections
    size_t final_count;
    {
        std::lock_guard<std::mutex> sessions_lock(sessions_mutex_);
        final_count = sessions_.size();
        if (final_count > 0) {
            Logger::warn("Forcefully closing {} remaining connections after drain timeout",
                         final_count);
        }
    }

    // Call regular stop to clean up
    stop();
}

uint16_t TcpServer::listeningPort() const {
    std::lock_guard<std::mutex> lock(state_mutex_);
    return static_cast<uint16_t>(configured_port_);
}

void TcpServer::doAccept(tcp::acceptor* acceptor) {
    if (!acceptor || !acceptor->is_open()) {
        return;
    }

    acceptor->async_accept(boost::asio::bind_executor(
        control_, [this, acceptor](const boost::system::error_code& ec, tcp::socket socket) {
            if (ec) {
                if (running_ && acceptor->is_open()) {
                    Logger::error("Accept error: {}", ec.message());
                    doAccept(acceptor);
                }
                return;
            }

            if (!running_)
                return;
            applySocketTuning(socket, socket_tuning_);
            auto session =
                std::make_shared<TcpSession>(std::move(socket), *this, max_frame_size_bytes_);
            trackSession(session);
            session->startOnStrand();
            doAccept(acceptor);
        }));
}

RequestDispatcher::DispatchResult TcpServer::dispatchRequest(
    RequestDispatcher::RequestContext context) {
    if (!dispatcher_) {
        throw std::runtime_error("Request dispatcher not configured");
    }
    return dispatcher_->dispatch(std::move(context));
}

void TcpServer::trackSession(const std::shared_ptr<TcpSession>& session) {
    std::lock_guard<std::mutex> lock(sessions_mutex_);
    sessions_[session.get()] = session;
    total_connections_.fetch_add(1, std::memory_order_relaxed);

    if (metrics_) {
        metrics_->setActiveConnections(sessions_.size());
        metrics_->incrementConnectionsCreated();
    }
}

void TcpServer::untrackSession(TcpSession* session) {
    std::lock_guard<std::mutex> lock(sessions_mutex_);
    sessions_.erase(session);

    if (metrics_) {
        metrics_->setActiveConnections(sessions_.size());
    }
}

void TcpServer::startIdleCheckTimer() {
    if (idle_timeout_.count() == 0) {
        return;  // Idle checking disabled
    }

    // Reap at half the timeout (clamped to [5s, 60s]) so a connection is
    // closed at most ~1.5x its idle timeout after going quiet.
    const auto check_interval =
        std::chrono::seconds(std::clamp<int64_t>(idle_timeout_.count() / 2, 5, 60));
    idle_check_timer_ = std::make_unique<boost::asio::steady_timer>(io_context_);
    idle_check_timer_->expires_after(check_interval);

    idle_check_timer_->async_wait(
        boost::asio::bind_executor(control_, [this](const boost::system::error_code& ec) {
            if (ec) {
                if (ec != boost::asio::error::operation_aborted) {
                    Logger::warn("Idle check timer error: {}", ec.message());
                }
                return;
            }

            checkIdleSessions();

            if (running_) {
                startIdleCheckTimer();  // Reschedule
            }
        }));
}

void TcpServer::checkIdleSessions() {
    auto now = std::chrono::steady_clock::now();
    std::vector<std::shared_ptr<TcpSession>> idle_sessions;

    {
        std::lock_guard<std::mutex> lock(sessions_mutex_);
        for (const auto& [_, session] : sessions_) {
            auto idle_duration =
                std::chrono::duration_cast<std::chrono::seconds>(now - session->getLastActivity());

            if (idle_duration >= idle_timeout_) {
                idle_sessions.push_back(session);
            }
        }
    }

    if (!idle_sessions.empty()) {
        Logger::info("Closing {} idle connections (timeout: {}s)", idle_sessions.size(),
                     idle_timeout_.count());

        for (auto& session : idle_sessions) {
            session->requestStop(monitoring::ConnectionCloseReason::kIdleTimeout);
        }
    }
}

void TcpServer::recordBytesIn(int64_t bytes) {
    if (metrics_) {
        metrics_->incrementBytesIn(bytes);
    }
}

void TcpServer::recordBytesOut(int64_t bytes) {
    if (metrics_) {
        metrics_->incrementBytesOut(bytes);
    }
}

void TcpServer::recordConnectionError() {
    connection_errors_.fetch_add(1, std::memory_order_relaxed);
}

void TcpServer::recordConnectionClosed(monitoring::ConnectionCloseReason reason) {
    if (metrics_) {
        metrics_->incrementConnectionClosed(reason);
    }
}

}  // namespace kawasan::broker::network
