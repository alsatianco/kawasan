// Copyright 2025 Kawasan Project
// Licensed under the Apache License, Version 2.0

#include "kawasan/raft/raft_transport.h"

#include <spdlog/spdlog.h>
#include <sys/socket.h>

#include <algorithm>
#include <stdexcept>

#include "kawasan/common/socket_deadline.h"

using boost::asio::ip::tcp;

namespace kawasan {
namespace raft {

//==============================================================================
// RaftTransport Implementation
//==============================================================================

RaftTransport::RaftTransport(boost::asio::io_context& io_context, int local_peer_id)
    : io_context_(io_context),
      local_peer_id_(local_peer_id),
      running_(false),
      server_lifetime_(std::make_shared<ServerLifetime>()),
      listen_port_(0) {
    server_lifetime_->owner = this;
    spdlog::info("RaftTransport created for peer {}", local_peer_id_);
}

RaftTransport::~RaftTransport() {
    stop();
    {
        std::lock_guard<std::recursive_mutex> lifetime_lock(server_lifetime_->mutex);
        server_lifetime_->owner = nullptr;
    }
    std::map<int, std::unique_ptr<PeerWorker>> workers;
    {
        std::lock_guard<std::mutex> lock(peers_mutex_);
        workers.swap(peer_workers_);
    }
    for (auto& [id, worker] : workers) {
        worker->guard.reset();
        worker->io.stop();
        if (worker->thread.joinable()) {
            worker->thread.join();
        }
    }
}

RaftTransport::PeerWorker::PeerWorker()
    : guard(boost::asio::make_work_guard(io)), thread([this] { io.run(); }) {}

void RaftTransport::start(int port) {
    std::lock_guard<std::recursive_mutex> lifetime_lock(server_lifetime_->mutex);
    std::lock_guard<std::mutex> lock(peers_mutex_);

    if (running_) {
        spdlog::warn("RaftTransport already running on port {}", listen_port_);
        return;
    }

    try {
        // Create acceptor and bind to port
        acceptor_ = std::make_unique<tcp::acceptor>(io_context_, tcp::endpoint(tcp::v4(), port));

        listen_port_ = port;
        running_ = true;
        server_lifetime_->active = true;
        ++server_lifetime_->generation;

        spdlog::info("RaftTransport started listening on port {}", port);

        // Start accepting connections
        do_accept();

    } catch (const std::exception& e) {
        spdlog::error("Failed to start RaftTransport on port {}: {}", port, e.what());
        throw std::runtime_error("Failed to start RaftTransport: " + std::string(e.what()));
    }
}

void RaftTransport::stop() {
    std::lock_guard<std::recursive_mutex> lifetime_lock(server_lifetime_->mutex);
    std::lock_guard<std::mutex> lock(peers_mutex_);

    if (!running_) {
        return;
    }

    spdlog::info("Stopping RaftTransport...");
    running_ = false;
    server_lifetime_->active = false;

    // Close acceptor
    if (acceptor_ && acceptor_->is_open()) {
        boost::system::error_code ec;
        acceptor_->close(ec);
        if (ec) {
            spdlog::warn("Error closing acceptor: {}", ec.message());
        }
    }

    // Native shutdown interrupts worker reads without concurrently closing
    // their Asio socket implementation. Workers release/close it afterward.
    for (auto& [peer_id, peer] : peers_) {
        if (peer.socket && peer.socket->is_open()) {
            ::shutdown(peer.socket->native_handle(), SHUT_RDWR);
        }
        peer.connected = false;
    }

    // Stop accepted chains and send FIN immediately. Sessions retain their
    // socket until pending asynchronous completions release the last reference.
    {
        std::lock_guard<std::mutex> session_lock(sessions_mutex_);
        for (auto& weak_session : active_sessions_) {
            if (auto session = weak_session.lock()) {
                session->shutdownSocket();
            }
        }
        active_sessions_.clear();
    }

    spdlog::info("RaftTransport stopped");
}

bool RaftTransport::is_running() const {
    std::lock_guard<std::mutex> lock(peers_mutex_);
    return running_;
}

void RaftTransport::add_peer(int peer_id, const std::string& host, int port) {
    std::lock_guard<std::mutex> lock(peers_mutex_);

    if (peer_id == local_peer_id_) {
        spdlog::warn("Ignoring attempt to add self as peer (ID: {})", peer_id);
        return;
    }

    PeerInfo peer;
    peer.host = host;
    peer.port = port;
    peer.connected = false;
    peer.retry_count = 0;

    peers_[peer_id] = peer;
    if (!peer_workers_.count(peer_id)) {
        peer_workers_[peer_id] = std::make_unique<PeerWorker>();
    }
    spdlog::info("Added peer {} at {}:{}", peer_id, host, port);
}

void RaftTransport::remove_peer(int peer_id) {
    std::lock_guard<std::mutex> lock(peers_mutex_);

    auto it = peers_.find(peer_id);
    if (it != peers_.end()) {
        // Close connection if open
        if (it->second.socket && it->second.socket->is_open()) {
            boost::system::error_code ec;
            it->second.socket->close(ec);
        }

        peers_.erase(it);
        spdlog::info("Removed peer {}", peer_id);
    }
}

namespace {
using Clock = std::chrono::steady_clock;
}  // namespace

std::vector<uint8_t> RaftTransport::roundTrip(int peer_id, const std::vector<uint8_t>& request,
                                              std::chrono::milliseconds timeout,
                                              std::chrono::steady_clock::time_point enqueued,
                                              const char* rpc_name) {
    const auto deadline = enqueued + timeout;
    // 0A.9: serialize RPCs to the same peer so their frames never interleave
    // on the shared socket.
    std::shared_ptr<std::mutex> rpc_mu;
    {
        std::lock_guard<std::mutex> lock(peers_mutex_);
        auto it = peers_.find(peer_id);
        if (it != peers_.end()) {
            rpc_mu = it->second.rpc_mutex;
        }
    }
    std::unique_lock<std::mutex> rpc_lock;
    if (rpc_mu) {
        rpc_lock = std::unique_lock<std::mutex>(*rpc_mu);
    }

    std::vector<uint8_t> message;
    message.reserve(4 + request.size());
    const auto length = static_cast<uint32_t>(request.size());
    message.push_back((length >> 24) & 0xFF);
    message.push_back((length >> 16) & 0xFF);
    message.push_back((length >> 8) & 0xFF);
    message.push_back(length & 0xFF);
    message.insert(message.end(), request.begin(), request.end());

    // opus2 Item 2: a restarted peer leaves a stale cached socket, so retry
    // once on a fresh connection before failing the RPC.
    std::exception_ptr last_error;
    for (int attempt = 0; attempt < 2; ++attempt) {
        if (Clock::now() >= deadline) {
            // Queued behind a slow RPC to the same peer for longer than the
            // RPC is worth: the caller stopped waiting long ago.
            throw std::runtime_error(std::string(rpc_name) + " to peer " + std::to_string(peer_id) +
                                     " expired before sending");
        }
        try {
            auto socket = get_connection(peer_id, deadline);
            transferWithDeadline(*socket, true, message.data(), message.size(), deadline);
            uint8_t length_buf[4];
            transferWithDeadline(*socket, false, length_buf, 4, deadline);
            const uint32_t response_length = (static_cast<uint32_t>(length_buf[0]) << 24) |
                                             (static_cast<uint32_t>(length_buf[1]) << 16) |
                                             (static_cast<uint32_t>(length_buf[2]) << 8) |
                                             static_cast<uint32_t>(length_buf[3]);
            if (response_length > kMaxMessageSize) {
                throw std::runtime_error("Response too large: " + std::to_string(response_length));
            }
            std::vector<uint8_t> response(response_length);
            transferWithDeadline(*socket, false, response.data(), response.size(), deadline);
            return response;
        } catch (const std::exception& e) {
            last_error = std::current_exception();
            spdlog::debug("{} RPC to peer {} failed (attempt {}): {}", rpc_name, peer_id,
                          attempt + 1, e.what());
            // A timed-out socket may still receive the late response: never
            // reuse it, or the next RPC would read the wrong frame.
            std::lock_guard<std::mutex> lock(peers_mutex_);
            auto it = peers_.find(peer_id);
            if (it != peers_.end()) {
                it->second.connected = false;
                if (it->second.socket) {
                    boost::system::error_code close_ec;
                    it->second.socket->close(close_ec);
                }
                it->second.retry_count = 0;
            }
        }
    }
    std::rethrow_exception(last_error);
}

template <typename Response, typename Decode>
std::future<Response> RaftTransport::sendRpc(int peer_id, std::vector<uint8_t> request,
                                             std::chrono::milliseconds timeout,
                                             const char* rpc_name, Decode decode) {
    auto promise = std::make_shared<std::promise<Response>>();
    auto future = promise->get_future();
    const auto enqueued = Clock::now();
    // Client RPCs never run on the io_context that serves incoming RPCs (two
    // nodes calling each other at once would otherwise both block in a read
    // with no thread left to answer the other), and each peer has its own
    // thread, so a frozen peer only delays RPCs to itself.
    boost::asio::io_context* worker_io = nullptr;
    {
        std::lock_guard<std::mutex> lock(peers_mutex_);
        auto it = peer_workers_.find(peer_id);
        if (it != peer_workers_.end()) {
            worker_io = &it->second->io;
        }
    }
    if (worker_io == nullptr) {
        promise->set_exception(std::make_exception_ptr(
            std::invalid_argument("Unknown peer ID: " + std::to_string(peer_id))));
        return future;
    }
    boost::asio::post(*worker_io, [this, peer_id, request = std::move(request), timeout, rpc_name,
                                   decode, promise, enqueued]() {
        try {
            promise->set_value(decode(roundTrip(peer_id, request, timeout, enqueued, rpc_name)));
        } catch (...) {
            promise->set_exception(std::current_exception());
        }
    });
    return future;
}

std::future<AppendEntriesResponse> RaftTransport::sendAppendEntries(
    int peer_id, const AppendEntriesRequest& request) {
    return sendRpc<AppendEntriesResponse>(
        peer_id, AppendEntriesRequestCodec::encode(request), kRpcTimeout, "AppendEntries",
        [](const std::vector<uint8_t>& data) { return AppendEntriesResponseCodec::decode(data); });
}

std::future<RequestVoteResponse> RaftTransport::sendRequestVote(int peer_id,
                                                                const RequestVoteRequest& request) {
    return sendRpc<RequestVoteResponse>(
        peer_id, RequestVoteRequestCodec::encode(request), kRpcTimeout, "RequestVote",
        [](const std::vector<uint8_t>& data) { return RequestVoteResponseCodec::decode(data); });
}

void RaftTransport::setAppendEntriesHandler(
    std::function<AppendEntriesResponse(const AppendEntriesRequest&)> handler) {
    std::lock_guard<std::mutex> lock(handlers_mutex_);
    append_entries_handler_ = std::move(handler);
    spdlog::debug("AppendEntries handler set");
}

void RaftTransport::setRequestVoteHandler(
    std::function<RequestVoteResponse(const RequestVoteRequest&)> handler) {
    std::lock_guard<std::mutex> lock(handlers_mutex_);
    request_vote_handler_ = std::move(handler);
    spdlog::debug("RequestVote handler set");
}

void RaftTransport::setInstallSnapshotHandler(InstallSnapshotHandler handler) {
    std::lock_guard<std::mutex> lock(handlers_mutex_);
    install_snapshot_handler_ = std::make_unique<InstallSnapshotHandler>(std::move(handler));
    spdlog::debug("InstallSnapshot handler set");
}

std::future<InstallSnapshotResponse> RaftTransport::sendInstallSnapshot(
    int peer_id, const InstallSnapshotRequest& request) {
    return sendRpc<InstallSnapshotResponse>(
        peer_id, request.encode(), kSnapshotRpcTimeout, "InstallSnapshot",
        [](const std::vector<uint8_t>& data) { return InstallSnapshotResponse::decode(data); });
}

//==============================================================================
// Private Methods
//==============================================================================

void RaftTransport::do_accept() {
    if (!running_ || !acceptor_) {
        return;
    }

    auto lifetime = server_lifetime_;
    const auto generation = lifetime->generation;
    acceptor_->async_accept(
        [lifetime, generation](boost::system::error_code ec, tcp::socket socket) {
            std::lock_guard<std::recursive_mutex> lifetime_lock(lifetime->mutex);
            if (!lifetime->active || lifetime->generation != generation) {
                return;
            }
            auto* owner = lifetime->owner;
            if (!ec) {
                auto session = std::make_shared<Session>(std::move(socket), lifetime, generation);
                owner->registerSession(session);
                session->start();
            } else if (ec != boost::asio::error::operation_aborted) {
                spdlog::error("Accept error: {}", ec.message());
            }
            owner->do_accept();
        });
}

void RaftTransport::registerSession(std::shared_ptr<Session> session) {
    std::lock_guard<std::mutex> lock(sessions_mutex_);
    // Compact: drop expired weak_ptrs to keep the vector bounded.
    active_sessions_.erase(
        std::remove_if(active_sessions_.begin(), active_sessions_.end(),
                       [](const std::weak_ptr<Session>& w) { return w.expired(); }),
        active_sessions_.end());
    active_sessions_.push_back(session);
}

std::shared_ptr<tcp::socket> RaftTransport::get_connection(
    int peer_id, std::chrono::steady_clock::time_point deadline) {
    std::string host;
    int port = 0;
    {
        std::lock_guard<std::mutex> lock(peers_mutex_);
        if (!running_) {
            throw std::runtime_error("RaftTransport is stopped");
        }
        auto it = peers_.find(peer_id);
        if (it == peers_.end()) {
            throw std::invalid_argument("Unknown peer ID: " + std::to_string(peer_id));
        }
        PeerInfo& peer = it->second;
        if (peer.socket && peer.socket->is_open() && peer.connected) {
            return peer.socket;
        }
        if (peer.retry_count > 0 && std::chrono::steady_clock::now() <
                                        peer.last_retry + calculate_backoff(peer.retry_count)) {
            throw std::runtime_error("Backing off before reconnecting to peer " +
                                     std::to_string(peer_id));
        }
        host = peer.host;
        port = peer.port;
    }

    // Connect WITHOUT holding peers_mutex_ (every RPC to every peer needs it)
    // and never past the RPC deadline.
    auto socket = std::make_shared<tcp::socket>(io_context_);
    try {
        connectWithDeadline(*socket, host, port, deadline);
    } catch (const std::exception& e) {
        std::lock_guard<std::mutex> lock(peers_mutex_);
        auto it = peers_.find(peer_id);
        if (it != peers_.end()) {
            it->second.retry_count++;
            it->second.last_retry = std::chrono::steady_clock::now();
            it->second.connected = false;
        }
        spdlog::debug("Failed to connect to peer {} at {}:{}: {}", peer_id, host, port, e.what());
        throw std::runtime_error("Failed to connect to peer " + std::to_string(peer_id));
    }

    std::lock_guard<std::mutex> lock(peers_mutex_);
    auto it = peers_.find(peer_id);
    if (!running_) {
        throw std::runtime_error("RaftTransport stopped while connecting");
    }
    if (it == peers_.end()) {
        throw std::invalid_argument("Unknown peer ID: " + std::to_string(peer_id));
    }
    if (it->second.socket) {
        boost::system::error_code ec;
        it->second.socket->close(ec);
    }
    it->second.socket = socket;
    it->second.connected = true;
    it->second.retry_count = 0;
    spdlog::info("Connected to peer {} at {}:{}", peer_id, host, port);
    return socket;
}

std::chrono::milliseconds RaftTransport::calculate_backoff(int retry_count) {
    // Exponential backoff: initial * 2^retry_count, capped at max
    int backoff_ms = kInitialBackoffMs * (1 << std::min(retry_count, 5));
    return std::chrono::milliseconds(std::min(backoff_ms, kMaxBackoffMs));
}

//==============================================================================
// Session Implementation
//==============================================================================

RaftTransport::Session::Session(tcp::socket socket, std::shared_ptr<ServerLifetime> lifetime,
                                uint64_t generation)
    : socket_(std::move(socket)),
      native_fd_(socket_.native_handle()),
      lifetime_(std::move(lifetime)),
      generation_(generation) {}

void RaftTransport::Session::shutdownSocket() {
    if (!stopped_.exchange(true)) {
        ::shutdown(native_fd_, SHUT_RDWR);
    }
}

void RaftTransport::Session::start() {
    do_read_header();
}

void RaftTransport::Session::do_read_header() {
    if (stopped_) {
        return;
    }
    auto self = shared_from_this();

    // Read 4-byte length header
    read_buffer_.resize(4);

    boost::asio::async_read(socket_, boost::asio::buffer(read_buffer_),
                            [this, self](boost::system::error_code ec, std::size_t /*length*/) {
                                if (!ec) {
                                    // Parse length
                                    uint32_t body_length =
                                        (static_cast<uint32_t>(read_buffer_[0]) << 24) |
                                        (static_cast<uint32_t>(read_buffer_[1]) << 16) |
                                        (static_cast<uint32_t>(read_buffer_[2]) << 8) |
                                        static_cast<uint32_t>(read_buffer_[3]);

                                    if (body_length > kMaxMessageSize) {
                                        spdlog::error("Message too large: {} bytes", body_length);
                                        return;
                                    }

                                    do_read_body(body_length);
                                } else if (ec != boost::asio::error::operation_aborted &&
                                           ec != boost::asio::error::eof) {
                                    spdlog::error("Read header error: {}", ec.message());
                                }
                            });
}

void RaftTransport::Session::do_read_body(size_t body_length) {
    if (stopped_) {
        return;
    }
    auto self = shared_from_this();

    read_buffer_.resize(body_length);

    boost::asio::async_read(socket_, boost::asio::buffer(read_buffer_),
                            [this, self](boost::system::error_code ec, std::size_t /*length*/) {
                                if (!ec) {
                                    handle_request(read_buffer_);
                                } else if (ec != boost::asio::error::operation_aborted &&
                                           ec != boost::asio::error::eof) {
                                    spdlog::error("Read body error: {}", ec.message());
                                }
                            });
}

void RaftTransport::Session::handle_request(const std::vector<uint8_t>& data) {
    std::lock_guard<std::recursive_mutex> lifetime_lock(lifetime_->mutex);
    if (!lifetime_->active || lifetime_->generation != generation_ || stopped_) {
        return;
    }
    auto* transport = lifetime_->owner;
    try {
        // Peek at message type (second byte)
        if (data.size() < 2) {
            spdlog::error("Message too short to determine type");
            return;
        }

        uint8_t message_type = data[1];
        std::vector<uint8_t> response_data;

        std::lock_guard<std::mutex> lock(transport->handlers_mutex_);

        switch (static_cast<RaftMessageType>(message_type)) {
            case RaftMessageType::APPEND_ENTRIES_REQ: {
                if (!transport->append_entries_handler_) {
                    spdlog::warn("No AppendEntries handler set");
                    return;
                }

                auto request = AppendEntriesRequestCodec::decode(data);
                auto response = transport->append_entries_handler_(request);
                response_data = AppendEntriesResponseCodec::encode(response);
                break;
            }

            case RaftMessageType::REQUEST_VOTE_REQ: {
                if (!transport->request_vote_handler_) {
                    spdlog::warn("No RequestVote handler set");
                    return;
                }

                auto request = RequestVoteRequestCodec::decode(data);
                auto response = transport->request_vote_handler_(request);
                response_data = RequestVoteResponseCodec::encode(response);
                break;
            }

            case RaftMessageType::INSTALL_SNAPSHOT_REQ: {
                auto* handler_ptr = transport->install_snapshot_handler_.get();
                if (!handler_ptr || !(*handler_ptr)) {
                    spdlog::warn("No InstallSnapshot handler set");
                    return;
                }
                auto request = InstallSnapshotRequest::decode(data);
                auto response = (*handler_ptr)(request);
                response_data = response.encode();
                break;
            }

            default:
                spdlog::error("Unknown message type: {}", message_type);
                return;
        }

        // Send response
        do_write(response_data);

    } catch (const std::exception& e) {
        spdlog::error("Error handling request: {}", e.what());
    }
}

void RaftTransport::Session::do_write(const std::vector<uint8_t>& response) {
    if (stopped_) {
        return;
    }
    auto self = shared_from_this();

    // Prepend 4-byte length header
    uint32_t length = static_cast<uint32_t>(response.size());
    write_buffer_.clear();
    write_buffer_.reserve(4 + response.size());

    // Big-endian length
    write_buffer_.push_back((length >> 24) & 0xFF);
    write_buffer_.push_back((length >> 16) & 0xFF);
    write_buffer_.push_back((length >> 8) & 0xFF);
    write_buffer_.push_back(length & 0xFF);

    write_buffer_.insert(write_buffer_.end(), response.begin(), response.end());

    boost::asio::async_write(
        socket_, boost::asio::buffer(write_buffer_),
        [self](boost::system::error_code ec, std::size_t /*length*/) {
            if (!ec) {
                // Serialize responses before reading the next request, so
                // pipelined requests cannot reuse an in-flight write buffer.
                self->do_read_header();
            } else if (ec != boost::asio::error::operation_aborted && !self->stopped_) {
                spdlog::error("Write error: {}", ec.message());
            }
        });
}

}  // namespace raft
}  // namespace kawasan
