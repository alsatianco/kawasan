// Copyright 2025 Kawasan Project
// Licensed under the Apache License, Version 2.0

#include "kawasan/raft/raft_transport.h"
#include <poll.h>
#include <spdlog/spdlog.h>

#include <algorithm>
#include <cerrno>
#include <stdexcept>

using boost::asio::ip::tcp;

namespace kawasan {
namespace raft {

//==============================================================================
// RaftTransport Implementation
//==============================================================================

RaftTransport::RaftTransport(boost::asio::io_context& io_context, int local_peer_id)
    : io_context_(io_context)
    , local_peer_id_(local_peer_id)
    , running_(false)
    , listen_port_(0)
    , client_guard_(boost::asio::make_work_guard(client_io_)) {
    for (int i = 0; i < kClientThreads; ++i) {
        client_threads_.emplace_back([this] { client_io_.run(); });
    }
    spdlog::info("RaftTransport created for peer {}", local_peer_id_);
}

RaftTransport::~RaftTransport() {
    stop();
    client_guard_.reset();
    client_io_.stop();
    for (auto& t : client_threads_) {
        if (t.joinable()) {
            t.join();
        }
    }
}

void RaftTransport::start(int port) {
    std::lock_guard<std::mutex> lock(peers_mutex_);
    
    if (running_) {
        spdlog::warn("RaftTransport already running on port {}", listen_port_);
        return;
    }
    
    try {
        // Create acceptor and bind to port
        acceptor_ = std::make_unique<tcp::acceptor>(
            io_context_,
            tcp::endpoint(tcp::v4(), port));
        
        listen_port_ = port;
        running_ = true;
        
        spdlog::info("RaftTransport started listening on port {}", port);
        
        // Start accepting connections
        do_accept();
        
    } catch (const std::exception& e) {
        spdlog::error("Failed to start RaftTransport on port {}: {}", port, e.what());
        throw std::runtime_error("Failed to start RaftTransport: " + std::string(e.what()));
    }
}

void RaftTransport::stop() {
    std::lock_guard<std::mutex> lock(peers_mutex_);
    
    if (!running_) {
        return;
    }
    
    spdlog::info("Stopping RaftTransport...");
    running_ = false;
    
    // Close acceptor
    if (acceptor_ && acceptor_->is_open()) {
        boost::system::error_code ec;
        acceptor_->close(ec);
        if (ec) {
            spdlog::warn("Error closing acceptor: {}", ec.message());
        }
    }
    
    // Close all peer connections
    for (auto& [peer_id, peer] : peers_) {
        if (peer.socket && peer.socket->is_open()) {
            boost::system::error_code ec;
            peer.socket->close(ec);
            if (ec) {
                spdlog::warn("Error closing connection to peer {}: {}", peer_id, ec.message());
            }
        }
        peer.connected = false;
    }

    // opus2 Item 2: forcibly close all accepted-side session sockets
    // so connected peers receive an immediate FIN. Skipping this leaves
    // sessions holding their sockets open while their async chain
    // unwinds, which makes peer-restart undetectable until the kernel's
    // own keepalive timer fires (tens of seconds). Done after closing
    // peer connections to minimize the window where a peer might still
    // try to send to us.
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

// Moves `len` bytes over a NON-blocking socket, waiting with poll() but never
// past `deadline`. Asio's own sync read/write wait without any bound, which let
// a frozen or partitioned peer wedge an RPC thread forever.
void transferWithDeadline(tcp::socket& socket, bool writing, uint8_t* data, size_t len,
                          Clock::time_point deadline) {
    size_t done = 0;
    while (done < len) {
        boost::system::error_code ec;
        const size_t n =
            writing ? socket.write_some(boost::asio::buffer(data + done, len - done), ec)
                    : socket.read_some(boost::asio::buffer(data + done, len - done), ec);
        if (!ec) {
            if (n == 0 && !writing) {
                throw std::runtime_error("connection closed by peer");
            }
            done += n;
            continue;
        }
        if (ec != boost::asio::error::would_block && ec != boost::asio::error::try_again) {
            throw std::runtime_error(std::string(writing ? "write" : "read") +
                                     " error: " + ec.message());
        }
        const auto remaining =
            std::chrono::duration_cast<std::chrono::milliseconds>(deadline - Clock::now());
        if (remaining.count() <= 0) {
            throw std::runtime_error(std::string(writing ? "write" : "read") + " timed out");
        }
        pollfd pfd{};
        pfd.fd = socket.native_handle();
        pfd.events = writing ? POLLOUT : POLLIN;
        const int rc = ::poll(&pfd, 1, static_cast<int>(remaining.count()));
        if (rc < 0 && errno != EINTR) {
            throw std::runtime_error("poll failed");
        }
    }
}

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
            throw std::runtime_error(std::string(rpc_name) + " to peer " +
                                     std::to_string(peer_id) + " expired before sending");
        }
        try {
            auto socket = get_connection(peer_id);
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
    // Client RPCs never run on the io_context that serves incoming RPCs: two
    // nodes calling each other at once would otherwise both block in a read
    // with no thread left to answer the other.
    boost::asio::post(client_io_, [this, peer_id, request = std::move(request), timeout,
                                   rpc_name, decode, promise, enqueued]() {
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
    install_snapshot_handler_ =
        std::make_unique<InstallSnapshotHandler>(std::move(handler));
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
    
    acceptor_->async_accept(
        [this](boost::system::error_code ec, tcp::socket socket) {
            if (!ec) {
                spdlog::debug("Accepted new Raft connection from {}:{}",
                    socket.remote_endpoint().address().to_string(),
                    socket.remote_endpoint().port());

                auto session = std::make_shared<Session>(std::move(socket), this);
                // opus2 Item 2: register with the transport so stop()
                // can forcibly close the socket on shutdown. Without
                // this the session would keep the socket open until
                // its async chain finished, leaving connected peers
                // with undetectably-broken cached sockets.
                registerSession(session);
                session->start();
            } else if (ec != boost::asio::error::operation_aborted) {
                spdlog::error("Accept error: {}", ec.message());
            }

            // Continue accepting
            do_accept();
        });
}

void RaftTransport::registerSession(std::shared_ptr<Session> session) {
    std::lock_guard<std::mutex> lock(sessions_mutex_);
    // Compact: drop expired weak_ptrs to keep the vector bounded.
    active_sessions_.erase(
        std::remove_if(active_sessions_.begin(), active_sessions_.end(),
                       [](const std::weak_ptr<Session>& w) {
                           return w.expired();
                       }),
        active_sessions_.end());
    active_sessions_.push_back(session);
}

std::shared_ptr<tcp::socket> RaftTransport::get_connection(int peer_id) {
    std::lock_guard<std::mutex> lock(peers_mutex_);
    
    auto it = peers_.find(peer_id);
    if (it == peers_.end()) {
        throw std::invalid_argument("Unknown peer ID: " + std::to_string(peer_id));
    }
    
    PeerInfo& peer = it->second;
    
    // Check if existing connection is valid
    if (peer.socket && peer.socket->is_open() && peer.connected) {
        return peer.socket;
    }
    
    // Need to connect
    if (!connect_to_peer(peer_id, peer)) {
        throw std::runtime_error("Failed to connect to peer " + std::to_string(peer_id));
    }
    
    return peer.socket;
}

bool RaftTransport::connect_to_peer(int peer_id, PeerInfo& peer) {
    // Check if we need backoff
    if (peer.retry_count > 0) {
        auto now = std::chrono::steady_clock::now();
        auto backoff = calculate_backoff(peer.retry_count);
        auto next_retry = peer.last_retry + backoff;
        
        if (now < next_retry) {
            auto wait_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                next_retry - now).count();
            spdlog::debug("Backing off {} ms before retry to peer {}", wait_ms, peer_id);
            return false;
        }
    }
    
    try {
        // Close existing socket if any
        if (peer.socket) {
            boost::system::error_code ec;
            peer.socket->close(ec);
        }
        
        // Create new socket
        peer.socket = std::make_shared<tcp::socket>(io_context_);
        
        // Resolve endpoint
        tcp::resolver resolver(io_context_);
        auto endpoints = resolver.resolve(peer.host, std::to_string(peer.port));
        
        // Connect synchronously (could be made async in future)
        boost::system::error_code ec;
        boost::asio::connect(*peer.socket, endpoints, ec);
        
        if (ec) {
            spdlog::warn("Failed to connect to peer {} at {}:{}: {}",
                peer_id, peer.host, peer.port, ec.message());
            
            peer.retry_count++;
            peer.last_retry = std::chrono::steady_clock::now();
            peer.connected = false;
            return false;
        }
        
        // Client RPCs drive this socket with poll()-bounded non-blocking IO.
        peer.socket->non_blocking(true, ec);
        if (ec) {
            spdlog::warn("Failed to make socket to peer {} non-blocking: {}", peer_id,
                         ec.message());
            peer.connected = false;
            return false;
        }

        // Success
        peer.connected = true;
        peer.retry_count = 0;
        spdlog::info("Connected to peer {} at {}:{}", peer_id, peer.host, peer.port);
        return true;
        
    } catch (const std::exception& e) {
        spdlog::error("Exception connecting to peer {}: {}", peer_id, e.what());
        peer.retry_count++;
        peer.last_retry = std::chrono::steady_clock::now();
        peer.connected = false;
        return false;
    }
}

std::chrono::milliseconds RaftTransport::calculate_backoff(int retry_count) {
    // Exponential backoff: initial * 2^retry_count, capped at max
    int backoff_ms = kInitialBackoffMs * (1 << std::min(retry_count, 5));
    return std::chrono::milliseconds(std::min(backoff_ms, kMaxBackoffMs));
}

//==============================================================================
// Session Implementation
//==============================================================================

RaftTransport::Session::Session(tcp::socket socket, RaftTransport* transport)
    : socket_(std::move(socket))
    , transport_(transport) {
}

void RaftTransport::Session::shutdownSocket() {
    // opus2 Item 2: idempotent socket shutdown. Called by
    // RaftTransport::stop() to send an immediate FIN to the connected
    // peer. Any in-flight async operations on this socket will fail
    // with operation_aborted; their handlers will run normally on the
    // io_context and drop their shared_from_this reference, releasing
    // this Session object.
    boost::system::error_code ec;
    socket_.shutdown(tcp::socket::shutdown_both, ec);
    socket_.close(ec);
}

void RaftTransport::Session::start() {
    do_read_header();
}

void RaftTransport::Session::do_read_header() {
    auto self = shared_from_this();
    
    // Read 4-byte length header
    read_buffer_.resize(4);
    
    boost::asio::async_read(
        socket_,
        boost::asio::buffer(read_buffer_),
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
    auto self = shared_from_this();
    
    read_buffer_.resize(body_length);
    
    boost::asio::async_read(
        socket_,
        boost::asio::buffer(read_buffer_),
        [this, self](boost::system::error_code ec, std::size_t /*length*/) {
            if (!ec) {
                handle_request(read_buffer_);
                
                // Continue reading next message
                do_read_header();
            } else if (ec != boost::asio::error::operation_aborted &&
                       ec != boost::asio::error::eof) {
                spdlog::error("Read body error: {}", ec.message());
            }
        });
}

void RaftTransport::Session::handle_request(const std::vector<uint8_t>& data) {
    try {
        // Peek at message type (second byte)
        if (data.size() < 2) {
            spdlog::error("Message too short to determine type");
            return;
        }
        
        uint8_t message_type = data[1];
        std::vector<uint8_t> response_data;
        
        std::lock_guard<std::mutex> lock(transport_->handlers_mutex_);
        
        switch (static_cast<RaftMessageType>(message_type)) {
            case RaftMessageType::APPEND_ENTRIES_REQ: {
                if (!transport_->append_entries_handler_) {
                    spdlog::warn("No AppendEntries handler set");
                    return;
                }
                
                auto request = AppendEntriesRequestCodec::decode(data);
                auto response = transport_->append_entries_handler_(request);
                response_data = AppendEntriesResponseCodec::encode(response);
                break;
            }
            
            case RaftMessageType::REQUEST_VOTE_REQ: {
                if (!transport_->request_vote_handler_) {
                    spdlog::warn("No RequestVote handler set");
                    return;
                }

                auto request = RequestVoteRequestCodec::decode(data);
                auto response = transport_->request_vote_handler_(request);
                response_data = RequestVoteResponseCodec::encode(response);
                break;
            }

            case RaftMessageType::INSTALL_SNAPSHOT_REQ: {
                // Phase 5.2: dispatch InstallSnapshot. We pull the
                // handler pointer outside the actual call so even a
                // destructed transport's empty pointer is treated
                // safely.
                auto* handler_ptr = transport_->install_snapshot_handler_.get();
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
        socket_,
        boost::asio::buffer(write_buffer_),
        [self](boost::system::error_code ec, std::size_t /*length*/) {
            if (ec && ec != boost::asio::error::operation_aborted) {
                spdlog::error("Write error: {}", ec.message());
            }
        });
}

} // namespace raft
} // namespace kawasan
