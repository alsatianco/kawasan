// Copyright 2025 Kawasan Project
// Licensed under the Apache License, Version 2.0

#pragma once

#include <atomic>
#include <boost/asio.hpp>
#include <chrono>
#include <functional>
#include <future>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "raft_protocol.h"

namespace kawasan {
namespace raft {

/**
 * @brief RaftTransport handles network communication for Raft protocol
 *
 * This class manages TCP connections to peer brokers and implements:
 * - Asynchronous RPC send/receive
 * - Connection pooling and lifecycle management
 * - Automatic retry with exponential backoff
 * - Thread-safe operations
 *
 * The transport operates on a separate port from Kafka protocol traffic
 * (default: 9093 for Raft, 9092 for Kafka).
 */
class RaftTransport {
public:
    /**
     * @brief Construct a RaftTransport with the given IO context
     *
     * @param io_context Boost.Asio IO context for async operations
     * @param local_peer_id This broker's peer ID
     */
    explicit RaftTransport(boost::asio::io_context& io_context, int local_peer_id);

    /**
     * @brief Destructor - ensures clean shutdown
     */
    ~RaftTransport();

    // Prevent copying
    RaftTransport(const RaftTransport&) = delete;
    RaftTransport& operator=(const RaftTransport&) = delete;

    /**
     * @brief Start the transport server on the specified port
     *
     * @param port Port to listen on for incoming Raft RPCs (default: 9093)
     * @throws std::runtime_error if port is already in use or bind fails
     */
    void start(int port = 9093);

    /**
     * @brief Stop the transport server and close all connections
     *
     * This is a graceful shutdown that:
     * - Stops accepting new connections
     * - Drains active incoming handlers before releasing their owner
     * - Interrupts peer sockets; outgoing workers retain their RPC deadlines
     */
    void stop();

    /**
     * @brief Check if transport is currently running
     */
    bool is_running() const;

    /**
     * @brief Add a peer broker to the cluster configuration
     *
     * @param peer_id Unique peer ID (broker ID)
     * @param host Hostname or IP address
     * @param port Raft port (not Kafka port)
     */
    void add_peer(int peer_id, const std::string& host, int port);

    /**
     * @brief Remove a peer from cluster configuration
     *
     * @param peer_id Peer ID to remove
     */
    void remove_peer(int peer_id);

    /**
     * @brief Send AppendEntries RPC to a peer
     *
     * This method:
     * - Serializes the request using raft_protocol
     * - Opens/reuses connection to peer
     * - Retries on transient failures with exponential backoff
     * - Returns a future that resolves to the response
     *
     * @param peer_id Target peer ID
     * @param request AppendEntries request to send
     * @return Future containing the response
     * @throws std::invalid_argument if peer_id is not configured
     */
    std::future<AppendEntriesResponse> sendAppendEntries(int peer_id,
                                                         const AppendEntriesRequest& request);

    /**
     * @brief Send RequestVote RPC to a peer
     *
     * Similar to sendAppendEntries but for vote requests during elections.
     *
     * @param peer_id Target peer ID
     * @param request RequestVote request to send
     * @return Future containing the response
     * @throws std::invalid_argument if peer_id is not configured
     */
    std::future<RequestVoteResponse> sendRequestVote(int peer_id,
                                                     const RequestVoteRequest& request);

    /**
     * @brief Phase 5.2: Send InstallSnapshot RPC to a peer. Used by the
     * leader to ship a snapshot to a follower whose `next_index` has
     * fallen behind the leader's earliest log entry.
     */
    std::future<InstallSnapshotResponse> sendInstallSnapshot(int peer_id,
                                                             const InstallSnapshotRequest& request);

    /**
     * @brief Set the handler for incoming AppendEntries RPCs
     *
     * This handler will be called when a peer sends an AppendEntries RPC.
     * The handler should process the request and return a response.
     *
     * @param handler Function to handle AppendEntries requests
     */
    void setAppendEntriesHandler(
        std::function<AppendEntriesResponse(const AppendEntriesRequest&)> handler);

    /**
     * @brief Set the handler for incoming RequestVote RPCs
     *
     * This handler will be called when a peer sends a RequestVote RPC.
     * The handler should process the request and return a response.
     *
     * @param handler Function to handle RequestVote requests
     */
    void setRequestVoteHandler(
        std::function<RequestVoteResponse(const RequestVoteRequest&)> handler);

    /**
     * @brief Phase 5.2: Set the handler for incoming InstallSnapshot RPCs.
     */
    using InstallSnapshotHandler =
        std::function<InstallSnapshotResponse(const InstallSnapshotRequest&)>;
    void setInstallSnapshotHandler(InstallSnapshotHandler handler);

private:
    /**
     * @brief Information about a peer connection
     */
    struct PeerInfo {
        std::string host;
        int port;
        std::shared_ptr<boost::asio::ip::tcp::socket> socket;
        bool connected = false;
        int retry_count = 0;
        std::chrono::steady_clock::time_point last_retry;
        // 0A.9: serialize concurrent RPCs to the same peer. Without this,
        // multiple in-flight sendAppendEntries / sendRequestVote calls share
        // the connection's socket and interleave their write/read frames,
        // corrupting the wire. Held as shared_ptr so the lambda can capture
        // it even if peers_ is mutated concurrently.
        std::shared_ptr<std::mutex> rpc_mutex = std::make_shared<std::mutex>();
    };

    // Accepted callbacks may outlive the transport. This shared fence drains
    // handlers before stop returns and rejects completions from older starts.
    struct ServerLifetime {
        std::recursive_mutex mutex;
        RaftTransport* owner = nullptr;
        bool active = false;
        uint64_t generation = 0;
    };

    /**
     * @brief Session handling incoming connection
     */
    class Session : public std::enable_shared_from_this<Session> {
    public:
        Session(boost::asio::ip::tcp::socket socket, std::shared_ptr<ServerLifetime> lifetime,
                uint64_t generation);
        void start();
        // Shut down the native descriptor without mutating Asio's socket
        // while composed reads are running on other io_context threads.
        void shutdownSocket();

    private:
        void do_read_header();
        void do_read_body(size_t body_length);
        void handle_request(const std::vector<uint8_t>& data);
        void do_write(const std::vector<uint8_t>& response);

        boost::asio::ip::tcp::socket socket_;
        const boost::asio::ip::tcp::socket::native_handle_type native_fd_;
        std::shared_ptr<ServerLifetime> lifetime_;
        const uint64_t generation_;
        std::atomic<bool> stopped_{false};
        std::vector<uint8_t> read_buffer_;
        std::vector<uint8_t> write_buffer_;
    };

    // Accept new incoming connections
    void do_accept();

    // Get or create (bounded by `deadline`, outside peers_mutex_) the
    // connection to a peer; honors the reconnect backoff.
    std::shared_ptr<boost::asio::ip::tcp::socket> get_connection(
        int peer_id, std::chrono::steady_clock::time_point deadline);

    // Calculate backoff delay based on retry count
    std::chrono::milliseconds calculate_backoff(int retry_count);

    // Member variables
    boost::asio::io_context& io_context_;
    int local_peer_id_;
    bool running_;

    // Server components
    std::shared_ptr<ServerLifetime> server_lifetime_;
    std::unique_ptr<boost::asio::ip::tcp::acceptor> acceptor_;
    int listen_port_;

    // Peer management
    std::map<int, PeerInfo> peers_;
    mutable std::mutex peers_mutex_;

    // opus2 Item 2: track live accepted-side sessions so stop() can
    // close their sockets. Without this, sessions keep the socket
    // open until their async chain unwinds — leaving the connected
    // peer's cached socket undetectably-broken for several seconds.
    std::vector<std::weak_ptr<Session>> active_sessions_;
    std::mutex sessions_mutex_;
    void registerSession(std::shared_ptr<Session> session);

    // RPC handlers
    std::function<AppendEntriesResponse(const AppendEntriesRequest&)> append_entries_handler_;
    std::function<RequestVoteResponse(const RequestVoteRequest&)> request_vote_handler_;
    std::mutex handlers_mutex_;

    std::unique_ptr<InstallSnapshotHandler> install_snapshot_handler_;

    // Client side: outgoing RPCs run on per-peer worker threads (never on the
    // shared io_context that serves incoming RPCs), each bounded by a deadline.
    std::vector<uint8_t> roundTrip(int peer_id, const std::vector<uint8_t>& request,
                                   std::chrono::milliseconds timeout,
                                   std::chrono::steady_clock::time_point enqueued,
                                   const char* rpc_name);
    template <typename Response, typename Decode>
    std::future<Response> sendRpc(int peer_id, std::vector<uint8_t> request,
                                  std::chrono::milliseconds timeout, const char* rpc_name,
                                  Decode decode);
    // One thread per peer runs that peer's outgoing RPCs in order.
    struct PeerWorker {
        PeerWorker();
        boost::asio::io_context io;
        std::optional<boost::asio::executor_work_guard<boost::asio::io_context::executor_type>>
            guard;
        std::thread thread;
    };
    std::map<int, std::unique_ptr<PeerWorker>> peer_workers_;  // guarded by peers_mutex_

    // Configuration
    static constexpr std::chrono::milliseconds kRpcTimeout{1000};
    static constexpr std::chrono::milliseconds kSnapshotRpcTimeout{10000};
    static constexpr int kMaxRetries = 3;
    static constexpr int kInitialBackoffMs = 100;
    static constexpr int kMaxBackoffMs = 5000;
    static constexpr size_t kMaxMessageSize = 10 * 1024 * 1024;  // 10MB
};

}  // namespace raft
}  // namespace kawasan
