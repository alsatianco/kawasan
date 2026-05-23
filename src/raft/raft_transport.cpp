// Copyright 2025 Kawasan Project
// Licensed under the Apache License, Version 2.0

#include "kawasan/raft/raft_transport.h"
#include <spdlog/spdlog.h>
#include <algorithm>
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
    , listen_port_(0) {
    
    spdlog::info("RaftTransport created for peer {}", local_peer_id_);
}

RaftTransport::~RaftTransport() {
    stop();
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

std::future<AppendEntriesResponse> RaftTransport::sendAppendEntries(
    int peer_id,
    const AppendEntriesRequest& request) {

    auto promise = std::make_shared<std::promise<AppendEntriesResponse>>();
    auto future = promise->get_future();

    // Post work to io_context to avoid blocking
    boost::asio::post(io_context_, [this, peer_id, request, promise]() {
        // 0A.9: lock the per-peer RPC mutex so concurrent RPCs on the same
        // socket cannot interleave their write/read frames.
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

        // opus2 Item 2: single-retry on connection error. When a peer
        // restarts, the first attempt may use a stale cached socket
        // and fail with EOF/broken-pipe. We must reconnect and retry
        // exactly once before failing the RPC. This matches the
        // behavior real Raft transports rely on for membership
        // changes and rolling restarts.
        std::exception_ptr last_error;
        for (int attempt = 0; attempt < 2; ++attempt) {
            try {
                // Get connection to peer
                auto socket = get_connection(peer_id);

                // Serialize request using codec
                std::vector<uint8_t> request_data = AppendEntriesRequestCodec::encode(request);

                // Prepend 4-byte length header
                uint32_t length = static_cast<uint32_t>(request_data.size());
                std::vector<uint8_t> message;
                message.reserve(4 + request_data.size());

                // Big-endian length
                message.push_back((length >> 24) & 0xFF);
                message.push_back((length >> 16) & 0xFF);
                message.push_back((length >> 8) & 0xFF);
                message.push_back(length & 0xFF);

                message.insert(message.end(), request_data.begin(), request_data.end());

                // Send request
                boost::system::error_code ec;
                boost::asio::write(*socket, boost::asio::buffer(message), ec);

                if (ec) {
                    throw std::runtime_error("Write error: " + ec.message());
                }

                // Read response length (4 bytes)
                uint8_t length_buf[4];
                boost::asio::read(*socket, boost::asio::buffer(length_buf, 4), ec);

                if (ec) {
                    throw std::runtime_error("Read length error: " + ec.message());
                }

                uint32_t response_length =
                    (static_cast<uint32_t>(length_buf[0]) << 24) |
                    (static_cast<uint32_t>(length_buf[1]) << 16) |
                    (static_cast<uint32_t>(length_buf[2]) << 8) |
                    static_cast<uint32_t>(length_buf[3]);

                if (response_length > kMaxMessageSize) {
                    throw std::runtime_error("Response too large: " + std::to_string(response_length));
                }

                // Read response body
                std::vector<uint8_t> response_data(response_length);
                boost::asio::read(*socket, boost::asio::buffer(response_data), ec);

                if (ec) {
                    throw std::runtime_error("Read body error: " + ec.message());
                }

                // Deserialize response using codec
                AppendEntriesResponse response = AppendEntriesResponseCodec::decode(response_data);

                // Fulfill promise
                promise->set_value(std::move(response));
                return;

            } catch (const std::exception& e) {
                last_error = std::current_exception();
                if (attempt == 0) {
                    spdlog::warn(
                        "AppendEntries RPC to peer {} failed on first attempt: {} "
                        "— invalidating cached connection and retrying once",
                        peer_id, e.what());
                } else {
                    spdlog::error("AppendEntries RPC to peer {} failed after retry: {}",
                                  peer_id, e.what());
                }

                // Mark connection as bad so get_connection() reconnects on retry.
                std::lock_guard<std::mutex> lock(peers_mutex_);
                auto it = peers_.find(peer_id);
                if (it != peers_.end()) {
                    it->second.connected = false;
                    if (it->second.socket) {
                        boost::system::error_code close_ec;
                        it->second.socket->close(close_ec);
                    }
                    // Reset retry_count so connect_to_peer doesn't apply
                    // its backoff on this immediate reconnect — backoff
                    // is for persistent failures, not stale-cache cases.
                    it->second.retry_count = 0;
                }
            }
        }
        promise->set_exception(last_error);
    });

    return future;
}

std::future<RequestVoteResponse> RaftTransport::sendRequestVote(
    int peer_id,
    const RequestVoteRequest& request) {
    
    auto promise = std::make_shared<std::promise<RequestVoteResponse>>();
    auto future = promise->get_future();
    
    // Post work to io_context to avoid blocking
    boost::asio::post(io_context_, [this, peer_id, request, promise]() {
        // 0A.9: same per-peer serialization as sendAppendEntries.
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
        try {
            // Get connection to peer
            auto socket = get_connection(peer_id);

            // Serialize request using codec
            std::vector<uint8_t> request_data = RequestVoteRequestCodec::encode(request);
            
            // Prepend 4-byte length header
            uint32_t length = static_cast<uint32_t>(request_data.size());
            std::vector<uint8_t> message;
            message.reserve(4 + request_data.size());
            
            // Big-endian length
            message.push_back((length >> 24) & 0xFF);
            message.push_back((length >> 16) & 0xFF);
            message.push_back((length >> 8) & 0xFF);
            message.push_back(length & 0xFF);
            
            message.insert(message.end(), request_data.begin(), request_data.end());
            
            // Send request
            boost::system::error_code ec;
            boost::asio::write(*socket, boost::asio::buffer(message), ec);
            
            if (ec) {
                throw std::runtime_error("Write error: " + ec.message());
            }
            
            // Read response length (4 bytes)
            uint8_t length_buf[4];
            boost::asio::read(*socket, boost::asio::buffer(length_buf, 4), ec);
            
            if (ec) {
                throw std::runtime_error("Read length error: " + ec.message());
            }
            
            uint32_t response_length = 
                (static_cast<uint32_t>(length_buf[0]) << 24) |
                (static_cast<uint32_t>(length_buf[1]) << 16) |
                (static_cast<uint32_t>(length_buf[2]) << 8) |
                static_cast<uint32_t>(length_buf[3]);
            
            if (response_length > kMaxMessageSize) {
                throw std::runtime_error("Response too large: " + std::to_string(response_length));
            }
            
            // Read response body
            std::vector<uint8_t> response_data(response_length);
            boost::asio::read(*socket, boost::asio::buffer(response_data), ec);
            
            if (ec) {
                throw std::runtime_error("Read body error: " + ec.message());
            }
            
            // Deserialize response using codec
            RequestVoteResponse response = RequestVoteResponseCodec::decode(response_data);
            
            // Fulfill promise
            promise->set_value(std::move(response));
            
        } catch (const std::exception& e) {
            spdlog::error("RequestVote RPC to peer {} failed: {}", peer_id, e.what());
            
            // Mark connection as bad
            {
                std::lock_guard<std::mutex> lock(peers_mutex_);
                auto it = peers_.find(peer_id);
                if (it != peers_.end()) {
                    it->second.connected = false;
                }
            }
            
            promise->set_exception(std::current_exception());
        }
    });
    
    return future;
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

    auto promise = std::make_shared<std::promise<InstallSnapshotResponse>>();
    auto future = promise->get_future();

    boost::asio::post(io_context_, [this, peer_id, request, promise]() {
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
        try {
            auto socket = get_connection(peer_id);
            auto request_data = request.encode();
            uint32_t length = static_cast<uint32_t>(request_data.size());
            std::vector<uint8_t> message;
            message.reserve(4 + request_data.size());
            message.push_back((length >> 24) & 0xFF);
            message.push_back((length >> 16) & 0xFF);
            message.push_back((length >> 8) & 0xFF);
            message.push_back(length & 0xFF);
            message.insert(message.end(), request_data.begin(), request_data.end());

            boost::system::error_code ec;
            boost::asio::write(*socket, boost::asio::buffer(message), ec);
            if (ec) throw std::runtime_error("Write error: " + ec.message());

            uint8_t length_buf[4];
            boost::asio::read(*socket, boost::asio::buffer(length_buf, 4), ec);
            if (ec) throw std::runtime_error("Read length error: " + ec.message());

            const uint32_t response_length =
                (static_cast<uint32_t>(length_buf[0]) << 24) |
                (static_cast<uint32_t>(length_buf[1]) << 16) |
                (static_cast<uint32_t>(length_buf[2]) << 8) |
                static_cast<uint32_t>(length_buf[3]);
            if (response_length > kMaxMessageSize) {
                throw std::runtime_error("Response too large");
            }
            std::vector<uint8_t> response_data(response_length);
            boost::asio::read(*socket, boost::asio::buffer(response_data), ec);
            if (ec) throw std::runtime_error("Read body error: " + ec.message());

            auto response = InstallSnapshotResponse::decode(response_data);
            promise->set_value(std::move(response));
        } catch (const std::exception& e) {
            spdlog::error("InstallSnapshot RPC to peer {} failed: {}", peer_id, e.what());
            {
                std::lock_guard<std::mutex> lock(peers_mutex_);
                auto it = peers_.find(peer_id);
                if (it != peers_.end()) {
                    it->second.connected = false;
                }
            }
            promise->set_exception(std::current_exception());
        }
    });

    return future;
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
