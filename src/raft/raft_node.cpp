#include "kawasan/raft/raft_node.h"

#include <algorithm>
#include <cstring>
#include <filesystem>
#include <random>
#include <utility>

#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/write_batch.h>

#include "kawasan/common/logger.h"
#include "kawasan/raft/raft_protocol.h"
#include "kawasan/raft/raft_transport.h"

namespace kawasan::raft {

namespace {

// 0A.7: persistence key scheme.
//   "meta:current_term" / "meta:voted_for" — singletons for Raft term state
//   "log:<8 big-endian bytes>"             — log entries keyed by index
// Big-endian index encoding gives correct sorted iteration in RocksDB so
// loadPersistedState() can replay entries in order.
constexpr const char* kKeyCurrentTerm = "meta:current_term";
constexpr const char* kKeyVotedFor    = "meta:voted_for";
constexpr const char* kLogKeyPrefix   = "log:";

std::string encodeLogKey(int64_t index) {
    std::string out = kLogKeyPrefix;
    uint64_t v = static_cast<uint64_t>(index);
    for (int i = 7; i >= 0; --i) {
        out.push_back(static_cast<char>((v >> (i * 8)) & 0xFF));
    }
    return out;
}

int64_t decodeLogKey(const std::string& key) {
    if (key.size() != std::strlen(kLogKeyPrefix) + 8) return -1;
    const char* p = key.data() + std::strlen(kLogKeyPrefix);
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i) {
        v = (v << 8) | static_cast<uint8_t>(p[i]);
    }
    return static_cast<int64_t>(v);
}

}  // namespace

RaftNode::RaftNode(BrokerId id, const std::vector<PeerInfo>& peers,
                   boost::asio::io_context& io_context, int raft_port,
                   std::string data_dir)
    : id_(id), peers_(peers), io_context_(io_context), raft_port_(raft_port),
      data_dir_(std::move(data_dir)) {
    resetElectionTimeout();
    
    // Initialize transport
    transport_ = std::make_unique<RaftTransport>(io_context_, id_);
    
    // Register handlers for incoming RPCs
    transport_->setRequestVoteHandler(
        [this](const raft::RequestVoteRequest& req) {
            return this->handleRequestVote(req);
        });
    
    transport_->setAppendEntriesHandler(
        [this](const raft::AppendEntriesRequest& req) {
            return this->handleAppendEntries(req);
        });

    // Phase 5.2: InstallSnapshot dispatch.
    transport_->setInstallSnapshotHandler(
        [this](const raft::InstallSnapshotRequest& req) {
            return this->handleInstallSnapshot(req);
        });

    // Configure peers in transport
    for (const auto& peer : peers_) {
        transport_->add_peer(peer.id, peer.host, peer.port);
    }
    
    Logger::info("Initialized Raft node with ID {} (port {}), {} peer(s)", 
                 id_, raft_port_, peers_.size());
}

RaftNode::~RaftNode() {
    stop();
}

void RaftNode::start() {
    running_ = true;

    // 0A.7: open persistent storage and replay any prior Raft state before
    // taking any action that would create new state. Multi-node restart safety
    // requires that current_term/voted_for survive a process restart so we
    // never vote twice in the same term.
    openPersistence();
    loadPersistedState();

    // Start transport server
    transport_->start(raft_port_);

    election_thread_ = std::thread(&RaftNode::electionThread, this);
    heartbeat_thread_ = std::thread(&RaftNode::heartbeatThread, this);
    Logger::info("Started Raft node {} on port {} (term={}, log_size={})", id_,
                 raft_port_, current_term_.load(), log_.size());

    // If no peers, immediately become leader (single-node mode)
    if (peers_.empty()) {
        Logger::info("Single-node mode: becoming leader immediately");
        becomeLeader();
    }
}

void RaftNode::stop() {
    if (running_) {
        running_ = false;

        // Stop transport
        if (transport_) {
            transport_->stop();
        }

        // Phase 5.2: wake the CV-driven election thread immediately so
        // shutdown is bounded by lock contention rather than the election
        // timeout (previously up to 300 ms).
        election_cv_.notify_all();

        if (election_thread_.joinable()) {
            election_thread_.join();
        }
        if (heartbeat_thread_.joinable()) {
            heartbeat_thread_.join();
        }
        // 0A.7: close persistent storage so another process (or this one
        // restarting) can re-open the directory without RocksDB LOCK errors.
        if (persist_db_) {
            persist_db_.reset();
        }
        Logger::info("Stopped Raft node {}", id_);
    }
}

std::future<int64_t> RaftNode::appendCommand(const std::vector<uint8_t>& command,
                                              const std::string& command_type) {
    std::promise<int64_t> promise;
    auto future = promise.get_future();

    if (!isLeader()) {
        promise.set_exception(
            std::make_exception_ptr(std::runtime_error("Not the leader")));
        return future;
    }

    std::lock_guard<std::mutex> lock(log_mutex_);

    LogEntry entry;
    entry.term = current_term_;
    entry.index = log_.empty() ? 1 : log_.back().index + 1;
    entry.data = command;
    entry.command_type = command_type;

    log_.push_back(entry);
    // 0A.7: durably store the entry BEFORE acknowledging — otherwise a leader
    // crash between in-memory append and disk write would lose acknowledged
    // commands in multi-node mode.
    persistLogEntry(entry);

    Logger::debug("Appended command to log at index {}", entry.index);
    promise.set_value(entry.index);

    if (peers_.empty()) {
        commit_index_ = entry.index;
        applyCommittedEntries();
    }

    return future;
}

RequestVoteResponse RaftNode::handleRequestVote(const RequestVoteRequest& request) {
    std::lock_guard<std::mutex> lock(log_mutex_);

    RequestVoteResponse response;
    response.term = current_term_;
    response.vote_granted = false;

    // If request term is greater, become follower
    if (request.term > current_term_) {
        becomeFollower(request.term);
    }

    // Don't grant vote if term is less
    if (request.term < current_term_) {
        return response;
    }

    // Check if we haven't voted or already voted for this candidate
    bool can_vote = (voted_for_ == -1 || voted_for_ == request.candidate_id);

    // Check if candidate's log is at least as up-to-date as ours
    int64_t last_log_index = log_.empty() ? 0 : log_.back().index;
    int64_t last_log_term = log_.empty() ? 0 : log_.back().term;

    bool log_up_to_date = (request.last_log_term > last_log_term) ||
                          (request.last_log_term == last_log_term &&
                           request.last_log_index >= last_log_index);

    if (can_vote && log_up_to_date) {
        voted_for_ = request.candidate_id;
        // 0A.7: persist vote BEFORE acknowledging so a crash cannot lead to a
        // double-vote in the same term on restart (a Raft safety violation).
        persistVotedFor();
        response.vote_granted = true;
        resetElectionTimeout();
        Logger::debug("Granted vote to candidate {} for term {}", request.candidate_id,
                      request.term);
    }

    return response;
}

AppendEntriesResponse RaftNode::handleAppendEntries(const AppendEntriesRequest& request) {
    std::lock_guard<std::mutex> lock(log_mutex_);

    AppendEntriesResponse response;
    response.term = current_term_;
    response.success = false;

    // If request term is greater, become follower
    if (request.term > current_term_) {
        becomeFollower(request.term);
    }

    // Reject if term is less
    if (request.term < current_term_) {
        return response;
    }

    // Reset election timeout on valid heartbeat
    resetElectionTimeout();
    leader_id_ = request.leader_id;

    // If we're a candidate, revert to follower
    if (state_ == NodeState::CANDIDATE) {
        becomeFollower(request.term);
    }

    // Check log consistency
    if (request.prev_log_index > 0) {
        if (log_.size() < static_cast<size_t>(request.prev_log_index) ||
            log_[request.prev_log_index - 1].term != request.prev_log_term) {
            response.last_log_index = log_.empty() ? 0 : log_.back().index;
            return response;
        }
    }

    // Append new entries
    size_t log_insert_pos = request.prev_log_index;
    for (size_t i = 0; i < request.entries.size(); ++i) {
        const auto& entry = request.entries[i];

        // If existing entry conflicts, delete it and all following
        if (log_.size() > log_insert_pos &&
            log_[log_insert_pos].term != entry.term) {
            // 0A.7: also truncate the persisted log so follower restart sees
            // the same history we now consider authoritative.
            const int64_t truncate_from = log_[log_insert_pos].index;
            log_.erase(log_.begin() + log_insert_pos, log_.end());
            truncateLogFrom(truncate_from);
        }

        // Append if not already present
        if (log_.size() <= log_insert_pos) {
            log_.push_back(entry);
            persistLogEntry(entry);  // 0A.7
        }

        ++log_insert_pos;
    }

    // Update commit index
    if (request.leader_commit > commit_index_) {
        int64_t new_commit =
            std::min(request.leader_commit, log_.empty() ? 0L : log_.back().index);
        commit_index_ = new_commit;
        applyCommittedEntries();
    }

    response.success = true;
    response.last_log_index = log_.empty() ? 0 : log_.back().index;
    response.term = current_term_;

    return response;
}

bool RaftNode::shouldSnapshot() const {
    if (state_.load() != NodeState::LEADER) return false;
    std::lock_guard<std::mutex> lock(log_mutex_);
    return static_cast<int64_t>(log_.size()) > snapshot_threshold_;
}

std::vector<uint8_t> RaftNode::generateSnapshot() {
    // Phase 5.2: stub snapshot. A real implementation would call
    // MetadataController::serialize() (or similar) for the state
    // machine. For now we emit a minimal JSON header so any peer
    // accepting the snapshot can log what it received.
    const int64_t last_applied = last_applied_.load();
    const int64_t current_term = current_term_.load();
    std::string snap = "{\"version\":1,\"last_applied\":" +
                       std::to_string(last_applied) +
                       ",\"term\":" + std::to_string(current_term) + "}";
    return std::vector<uint8_t>(snap.begin(), snap.end());
}

void RaftNode::sendSnapshotIfNeeded() {
    if (!shouldSnapshot()) return;

    // Capture log state under lock.
    int64_t last_included_index = 0;
    int64_t last_included_term = 0;
    {
        std::lock_guard<std::mutex> lock(log_mutex_);
        if (log_.empty()) return;
        last_included_index = log_.back().index;
        last_included_term = log_.back().term;
    }

    auto snapshot_data = generateSnapshot();
    InstallSnapshotRequest req;
    req.term = current_term_.load();
    req.leader_id = id_;
    req.last_included_index = last_included_index;
    req.last_included_term = last_included_term;
    req.offset = 0;
    req.data = snapshot_data;
    req.done = true;

    Logger::info("Leader {} initiating snapshot (last_included_index={}, term={}, bytes={})",
                 id_, last_included_index, last_included_term, snapshot_data.size());

    // Ship to all peers. Don't block on the response — failures will be
    // retried on the next snapshot cycle.
    for (auto& peer : peers_) {
        try {
            auto fut = transport_->sendInstallSnapshot(peer.id, req);
            (void)fut;  // Fire-and-forget
        } catch (const std::exception& ex) {
            Logger::warn("Failed to ship snapshot to peer {}: {}", peer.id, ex.what());
        }
    }

    // Truncate local log up to last_included_index. Real Raft would
    // wait for quorum to ack the snapshot first; we truncate
    // optimistically (single-broker case has no peers to wait for).
    {
        std::lock_guard<std::mutex> lock(log_mutex_);
        auto new_end = std::remove_if(
            log_.begin(), log_.end(),
            [&](const LogEntry& e) { return e.index <= last_included_index; });
        log_.erase(new_end, log_.end());
    }
    Logger::info("Leader {} truncated log past index {}", id_, last_included_index);
}

InstallSnapshotResponse RaftNode::handleInstallSnapshot(
    const InstallSnapshotRequest& request) {
    // Phase 5.2: handle InstallSnapshot RPC. Steps per Raft §7:
    //   1. Reply immediately if term < currentTerm
    //   2. Create new snapshot file if first chunk (offset = 0)
    //   3. Write data chunk at offset
    //   4. If done=false, return; wait for more chunks
    //   5. Save snapshot, discard older log entries, reset state machine
    InstallSnapshotResponse response;
    response.term = current_term_.load();

    if (request.term < current_term_.load()) {
        // Stale leader.
        return response;
    }

    // Higher term seen — convert to follower and adopt the new term.
    if (request.term > current_term_.load()) {
        current_term_ = request.term;
        voted_for_ = 0;
        becomeFollower(request.term);
        persistTerm();
        persistVotedFor();
    }
    leader_id_ = request.leader_id;
    resetElectionTimeout();

    // Accumulate chunks. Reset on offset=0 to start a new snapshot.
    std::lock_guard<std::mutex> snap_lock(snapshot_mutex_);
    if (request.offset == 0 || !incoming_snapshot_) {
        incoming_snapshot_ = std::make_unique<SnapshotAccumulator>();
        incoming_snapshot_->last_included_index = request.last_included_index;
        incoming_snapshot_->last_included_term = request.last_included_term;
    }
    // Append chunk at offset (we expect contiguous chunks in practice).
    if (request.offset > 0 &&
        request.offset != static_cast<int64_t>(incoming_snapshot_->data.size())) {
        Logger::warn("InstallSnapshot: unexpected offset {} (have {} bytes)",
                     request.offset, incoming_snapshot_->data.size());
    }
    incoming_snapshot_->data.insert(incoming_snapshot_->data.end(),
                                    request.data.begin(), request.data.end());

    if (!request.done) {
        response.term = current_term_.load();
        return response;
    }

    // Final chunk — install the snapshot.
    Logger::info("InstallSnapshot complete: last_included_index={} term={} bytes={}",
                 incoming_snapshot_->last_included_index,
                 incoming_snapshot_->last_included_term,
                 incoming_snapshot_->data.size());

    // Truncate log entries up to and including last_included_index.
    {
        std::lock_guard<std::mutex> log_lock(log_mutex_);
        auto new_end = std::remove_if(
            log_.begin(), log_.end(), [&](const LogEntry& e) {
                return e.index <= incoming_snapshot_->last_included_index;
            });
        log_.erase(new_end, log_.end());
    }

    // Advance commit and last_applied to the snapshot boundary.
    if (commit_index_.load() < incoming_snapshot_->last_included_index) {
        commit_index_ = incoming_snapshot_->last_included_index;
    }
    if (last_applied_.load() < incoming_snapshot_->last_included_index) {
        last_applied_ = incoming_snapshot_->last_included_index;
    }

    // Snapshot bytes themselves are opaque to Raft; a real implementation
    // would hand them to the state-machine layer (MetadataController in
    // our case). Phase 3.3 will populate that path.

    incoming_snapshot_.reset();
    response.term = current_term_.load();
    return response;
}

void RaftNode::electionThread() {
    // Phase 5.2: CV-driven wake. We wait up to `election_timeout_` on the
    // election_cv_; the wait returns early when a heartbeat arrives or
    // stop() is called (both signal the CV). Worst case the CV times out
    // and we re-check, which is exactly when we'd want to start an
    // election anyway. The old 10 ms busy-poll wasted ~100 wakes/second
    // per broker even in a healthy cluster.
    while (running_) {
        std::unique_lock<std::mutex> lock(election_cv_mutex_);
        // Compute how long to wait based on the most recent heartbeat;
        // re-evaluated each iteration so an updated heartbeat correctly
        // extends the wait.
        auto wait_for = election_timeout_;
        {
            const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - last_heartbeat_);
            if (elapsed < election_timeout_) {
                wait_for = election_timeout_ - elapsed;
            } else {
                wait_for = std::chrono::milliseconds(0);
            }
        }
        // Predicate-based wait: re-check `running_` after wake.
        election_cv_.wait_for(lock, wait_for, [this] { return !running_; });
        lock.unlock();

        if (!running_) break;

        if (state_ != NodeState::LEADER && hasElectionTimedOut()) {
            startElection();
        }
    }
}

void RaftNode::heartbeatThread() {
    // Phase 5.2: snapshot trigger cadence — every 1000 heartbeats
    // (~50 seconds at default 50 ms heartbeat) is a reasonable
    // background interval. Configurable via snapshot_threshold_ which
    // dictates when shouldSnapshot() flips, so this is just the
    // "check frequently" cadence.
    int64_t ticks = 0;
    while (running_) {
        std::this_thread::sleep_for(heartbeat_interval_);
        ticks++;

        if (state_ == NodeState::LEADER) {
            sendHeartbeats();
            // Check for snapshot trigger periodically.
            if (ticks % 1000 == 0) {
                sendSnapshotIfNeeded();
            }
        }
    }
}

void RaftNode::startElection() {
    becomeCandidate();

    const int64_t election_term = current_term_.load();
    Logger::info("Starting election for term {}", election_term);

    int votes_received = 1;  // Vote for self
    const int votes_needed = static_cast<int>((peers_.size() + 1) / 2 + 1);

    // If we're the only node, become leader immediately
    if (peers_.empty()) {
        becomeLeader();
        return;
    }

    // Prepare RequestVote request
    raft::RequestVoteRequest vote_request;
    vote_request.term = election_term;
    vote_request.candidate_id = id_;
    
    std::lock_guard<std::mutex> lock(log_mutex_);
    vote_request.last_log_index = log_.empty() ? 0 : log_.back().index;
    vote_request.last_log_term = log_.empty() ? 0 : log_.back().term;
    
    // Send RequestVote RPCs to all peers
    std::vector<std::future<raft::RequestVoteResponse>> responses;
    for (const auto& peer : peers_) {
        try {
            Logger::debug("Sending RequestVote to peer {} for term {}", 
                         peer.id, election_term);
            responses.push_back(transport_->sendRequestVote(peer.id, vote_request));
        } catch (const std::exception& e) {
            Logger::warn("Failed to send RequestVote to peer {}: {}", 
                        peer.id, e.what());
        }
    }
    
    // Wait for responses with timeout (must unlock before waiting)
    // Note: In production, this should be async to avoid blocking election thread
    const auto timeout = std::chrono::milliseconds(100);
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    
    for (auto& future : responses) {
        const auto remaining = deadline - std::chrono::steady_clock::now();
        if (remaining <= std::chrono::milliseconds(0)) {
            break;
        }
        
        if (future.wait_for(remaining) == std::future_status::ready) {
            try {
                auto response = future.get();
                
                // If we see a higher term, step down
                if (response.term > election_term) {
                    Logger::info("Saw higher term {} during election, stepping down", 
                                response.term);
                    becomeFollower(response.term);
                    return;
                }
                
                // Count vote if granted and term matches
                if (response.vote_granted && response.term == election_term) {
                    votes_received++;
                    Logger::debug("Received vote for term {}, total votes: {}/{}", 
                                 election_term, votes_received, votes_needed);
                }
            } catch (const std::exception& e) {
                Logger::warn("Error getting vote response: {}", e.what());
            }
        }
    }
    
    // Check if we won the election
    if (votes_received >= votes_needed && state_ == NodeState::CANDIDATE) {
        Logger::info("Won election for term {} with {} votes", 
                    election_term, votes_received);
        becomeLeader();
    } else {
        Logger::debug("Lost election for term {}, got {} votes (needed {})", 
                     election_term, votes_received, votes_needed);
    }
}

void RaftNode::sendHeartbeats() {
    if (state_ != NodeState::LEADER) {
        return;
    }
    
    std::lock_guard<std::mutex> lock(log_mutex_);
    const int64_t log_size = static_cast<int64_t>(log_.size());
    const int64_t current_term = current_term_.load();

    // Send AppendEntries to all peers
    for (auto& peer : peers_) {
        raft::AppendEntriesRequest request;
        request.term = current_term;
        request.leader_id = id_;
        request.prev_log_index = peer.next_index - 1;
        request.prev_log_term =
            (request.prev_log_index > 0 && request.prev_log_index <= log_size)
                ? log_[static_cast<size_t>(request.prev_log_index - 1)].term
                : 0;
        request.leader_commit = commit_index_;

        // Include entries that peer doesn't have (if any)
        if (peer.next_index > 0 && peer.next_index <= log_size) {
            size_t start = static_cast<size_t>(peer.next_index - 1);
            for (size_t i = start; i < log_.size(); ++i) {
                request.entries.push_back(log_[i]);
            }
        }

        // Send RPC asynchronously
        try {
            auto response_future = transport_->sendAppendEntries(peer.id, request);
            
            // Process response asynchronously (don't block here)
            // In a more sophisticated implementation, we'd use a callback or separate thread
            // For now, we'll check the future status quickly
            const auto timeout = std::chrono::milliseconds(10);
            if (response_future.wait_for(timeout) == std::future_status::ready) {
                try {
                    auto response = response_future.get();
                    
                    // If peer has higher term, step down
                    if (response.term > current_term) {
                        Logger::info("Peer {} has higher term {}, stepping down", 
                                    peer.id, response.term);
                        becomeFollower(response.term);
                        return;
                    }
                    
                    if (response.success) {
                        // Update peer's match_index and next_index
                        peer.match_index = response.last_log_index;
                        peer.next_index = response.last_log_index + 1;

                        Logger::trace("AppendEntries to peer {} succeeded, match_index={}, next_index={}",
                                     peer.id, peer.match_index, peer.next_index);

                        // Update commit index if majority have replicated
                        updateCommitIndex();
                    } else {
                        // Phase 5.2: use the follower's `last_log_index` as a
                        // hint to jump directly to the divergence point instead
                        // of decrementing by 1. Was O(N) catch-up on lagging
                        // followers; now O(1) per failure. Even if the follower
                        // reported 0 (empty log), we clamp to >=1 so the next
                        // AppendEntries has a valid prev_log_index.
                        const int64_t hint = response.last_log_index;
                        const int64_t new_next = std::max<int64_t>(1, hint + 1);
                        if (new_next < peer.next_index) {
                            peer.next_index = new_next;
                            Logger::debug(
                                "AppendEntries to peer {} failed, jumping next_index to {} "
                                "(follower last_log_index={})",
                                peer.id, peer.next_index, hint);
                        } else if (peer.next_index > 1) {
                            // Hint didn't help (still ahead); fall back to
                            // single-step backoff to make progress.
                            peer.next_index--;
                        }
                    }
                } catch (const std::exception& e) {
                    Logger::warn("Error processing AppendEntries response from peer {}: {}", 
                                peer.id, e.what());
                }
            }
            // If timeout, just continue - will retry on next heartbeat
        } catch (const std::exception& e) {
            Logger::warn("Failed to send AppendEntries to peer {}: {}", peer.id, e.what());
        }
    }
}

void RaftNode::becomeFollower(int64_t term) {
    current_term_ = term;
    state_ = NodeState::FOLLOWER;
    voted_for_ = -1;
    leader_id_ = -1;
    // 0A.7: persist BOTH fields together — Raft safety requires that on
    // restart we never re-grant a vote in a stale term.
    persistTerm();
    persistVotedFor();
    resetElectionTimeout();
    Logger::info("Node {} became follower for term {}", id_, term);
}

void RaftNode::becomeCandidate() {
    state_ = NodeState::CANDIDATE;
    current_term_++;
    voted_for_ = id_;
    // 0A.7
    persistTerm();
    persistVotedFor();
    resetElectionTimeout();
    Logger::info("Node {} became candidate for term {}", id_, current_term_.load());
}

void RaftNode::becomeLeader() {
    state_ = NodeState::LEADER;
    leader_id_ = id_;

    // Initialize peer state
    for (auto& peer : peers_) {
        peer.next_index = log_.empty() ? 1 : log_.back().index + 1;
        peer.match_index = 0;
    }

    Logger::info("Node {} became leader for term {}", id_, current_term_.load());
}

void RaftNode::resetElectionTimeout() {
    // Random timeout between 150-300ms
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(150, 300);

    election_timeout_ = std::chrono::milliseconds(dis(gen));
    last_heartbeat_ = std::chrono::steady_clock::now();
}

bool RaftNode::hasElectionTimedOut() const {
    auto now = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - last_heartbeat_);
    return elapsed >= election_timeout_;
}

void RaftNode::applyCommittedEntries() {
    while (last_applied_ < commit_index_) {
        last_applied_++;
        const int64_t log_size = static_cast<int64_t>(log_.size());
        if (last_applied_ <= log_size) {
            const auto& entry = log_[static_cast<size_t>(last_applied_ - 1)];
            if (commit_callback_) {
                commit_callback_(entry);
            }
        }
    }
}

// 0A.7: persistence helpers. These are intentionally synchronous writes —
// Raft safety hinges on every state mutation reaching stable storage before
// the broker acknowledges. The overhead is small (a few KB/s in steady state).

void RaftNode::openPersistence() {
    if (data_dir_.empty()) {
        // In-memory mode: tests and single-node-only deployments. Document
        // the trade-off in the log so operators are not surprised.
        Logger::warn("RaftNode {} running without persistence (data_dir empty); "
                     "multi-node safety is not guaranteed across restart",
                     id_);
        return;
    }
    try {
        std::filesystem::create_directories(data_dir_);
    } catch (const std::exception& ex) {
        Logger::error("Failed to create Raft data dir {}: {}", data_dir_, ex.what());
        return;
    }
    rocksdb::Options opts;
    opts.create_if_missing = true;
    opts.error_if_exists = false;
    opts.compression = rocksdb::kNoCompression;
    rocksdb::DB* db_raw = nullptr;
    auto status = rocksdb::DB::Open(opts, data_dir_, &db_raw);
    if (!status.ok()) {
        Logger::error("Failed to open Raft persistence at {}: {}", data_dir_,
                      status.ToString());
        return;
    }
    persist_db_.reset(db_raw);
    Logger::info("Opened Raft persistence at {}", data_dir_);
}

void RaftNode::loadPersistedState() {
    if (!persist_db_) {
        return;
    }

    std::string value;
    auto status = persist_db_->Get(rocksdb::ReadOptions(), kKeyCurrentTerm, &value);
    if (status.ok() && value.size() == sizeof(int64_t)) {
        int64_t v = 0;
        std::memcpy(&v, value.data(), sizeof(v));
        current_term_ = v;
    }
    status = persist_db_->Get(rocksdb::ReadOptions(), kKeyVotedFor, &value);
    if (status.ok() && value.size() == sizeof(BrokerId)) {
        BrokerId v = -1;
        std::memcpy(&v, value.data(), sizeof(v));
        voted_for_ = v;
    }

    // Replay log in index order.
    std::lock_guard<std::mutex> lock(log_mutex_);
    std::unique_ptr<rocksdb::Iterator> it(
        persist_db_->NewIterator(rocksdb::ReadOptions()));
    for (it->Seek(kLogKeyPrefix); it->Valid(); it->Next()) {
        const std::string key = it->key().ToString();
        if (key.compare(0, std::strlen(kLogKeyPrefix), kLogKeyPrefix) != 0) {
            break;  // iterated past log namespace
        }
        const int64_t idx = decodeLogKey(key);
        if (idx < 0) continue;
        const std::string raw = it->value().ToString();
        std::vector<uint8_t> bytes(raw.begin(), raw.end());
        try {
            ByteBuffer bb(bytes);
            LogEntry entry = LogEntryCodec::decodeFrom(bb);
            log_.push_back(std::move(entry));
        } catch (const std::exception& ex) {
            Logger::error("Failed to decode persisted log entry at idx={}: {}", idx,
                          ex.what());
        }
    }
    if (!log_.empty()) {
        Logger::info("Restored {} Raft log entries (last index={}, last term={})",
                     log_.size(), log_.back().index, log_.back().term);
    }
}

void RaftNode::persistTerm() {
    if (!persist_db_) return;
    int64_t v = current_term_.load();
    rocksdb::WriteOptions wo;
    wo.sync = true;
    persist_db_->Put(wo, kKeyCurrentTerm,
                     rocksdb::Slice(reinterpret_cast<const char*>(&v), sizeof(v)));
}

void RaftNode::persistVotedFor() {
    if (!persist_db_) return;
    BrokerId v = voted_for_.load();
    rocksdb::WriteOptions wo;
    wo.sync = true;
    persist_db_->Put(wo, kKeyVotedFor,
                     rocksdb::Slice(reinterpret_cast<const char*>(&v), sizeof(v)));
}

void RaftNode::persistLogEntry(const LogEntry& entry) {
    if (!persist_db_) return;
    ByteBuffer bb;
    LogEntryCodec::encodeInto(bb, entry);
    auto encoded = bb.release();
    rocksdb::WriteOptions wo;
    wo.sync = true;
    persist_db_->Put(
        wo, encodeLogKey(entry.index),
        rocksdb::Slice(reinterpret_cast<const char*>(encoded.data()), encoded.size()));
}

void RaftNode::truncateLogFrom(int64_t index) {
    if (!persist_db_) return;
    rocksdb::WriteBatch batch;
    std::unique_ptr<rocksdb::Iterator> it(
        persist_db_->NewIterator(rocksdb::ReadOptions()));
    for (it->Seek(encodeLogKey(index)); it->Valid(); it->Next()) {
        const std::string key = it->key().ToString();
        if (key.compare(0, std::strlen(kLogKeyPrefix), kLogKeyPrefix) != 0) break;
        batch.Delete(key);
    }
    rocksdb::WriteOptions wo;
    wo.sync = true;
    persist_db_->Write(wo, &batch);
}

void RaftNode::updateCommitIndex() {
    // This should be called by leader only
    if (state_ != NodeState::LEADER) {
        return;
    }
    
    // Find the highest index that's been replicated to a majority
    std::lock_guard<std::mutex> lock(log_mutex_);
    const int64_t log_size = static_cast<int64_t>(log_.size());
    
    for (int64_t n = log_size; n > commit_index_; --n) {
        // Count how many peers have replicated up to index n
        int replicas = 1; // Count self
        for (const auto& peer : peers_) {
            if (peer.match_index >= n) {
                replicas++;
            }
        }
        
        // If majority have replicated, and the entry is from current term, commit it
        const int majority = static_cast<int>((peers_.size() + 1) / 2 + 1);
        if (replicas >= majority) {
            // Check that the entry is from current term (Raft safety requirement)
            if (n <= log_size && log_[static_cast<size_t>(n - 1)].term == current_term_) {
                if (n > commit_index_) {
                    Logger::info("Updating commit index from {} to {}", commit_index_.load(), n);
                    commit_index_ = n;
                    applyCommittedEntries();
                }
                break;
            }
        }
    }
}

}  // namespace kawasan::raft
