#include "kawasan/raft/raft_node.h"

#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/write_batch.h>

#include <algorithm>
#include <cstring>
#include <filesystem>
#include <random>
#include <utility>

#include "kawasan/common/logger.h"
#include "kawasan/common/rocksdb_compat.h"
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
constexpr const char* kKeyVotedFor = "meta:voted_for";
constexpr const char* kLogKeyPrefix = "log:";

std::string encodeLogKey(int64_t index) {
    std::string out = kLogKeyPrefix;
    uint64_t v = static_cast<uint64_t>(index);
    for (int i = 7; i >= 0; --i) {
        out.push_back(static_cast<char>((v >> (i * 8)) & 0xFF));
    }
    return out;
}

int64_t decodeLogKey(const std::string& key) {
    if (key.size() != std::strlen(kLogKeyPrefix) + 8)
        return -1;
    const char* p = key.data() + std::strlen(kLogKeyPrefix);
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i) {
        v = (v << 8) | static_cast<uint8_t>(p[i]);
    }
    return static_cast<int64_t>(v);
}

}  // namespace

RaftNode::RaftNode(BrokerId id, const std::vector<PeerInfo>& peers,
                   boost::asio::io_context& io_context, int raft_port, std::string data_dir)
    : id_(id),
      peers_(peers),
      io_context_(io_context),
      raft_port_(raft_port),
      data_dir_(std::move(data_dir)) {
    resetElectionTimeout();

    // Initialize transport
    transport_ = std::make_unique<RaftTransport>(io_context_, id_);

    // Register handlers for incoming RPCs
    transport_->setRequestVoteHandler(
        [this](const raft::RequestVoteRequest& req) { return this->handleRequestVote(req); });

    transport_->setAppendEntriesHandler(
        [this](const raft::AppendEntriesRequest& req) { return this->handleAppendEntries(req); });

    // Phase 5.2: InstallSnapshot dispatch.
    transport_->setInstallSnapshotHandler([this](const raft::InstallSnapshotRequest& req) {
        return this->handleInstallSnapshot(req);
    });

    // Configure peers in transport
    for (const auto& peer : peers_) {
        transport_->add_peer(peer.id, peer.host, peer.port);
    }

    Logger::info("Initialized Raft node with ID {} (port {}), {} peer(s)", id_, raft_port_,
                 peers_.size());
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

    // Start the apply thread BEFORE anything can advance commit_index_ (incl. the
    // single-node becomeLeader below), so committed entries are never stranded.
    apply_running_ = true;
    apply_thread_ = std::thread(&RaftNode::applyThread, this);

    // Start transport server
    transport_->start(raft_port_);

    // Single-node: become leader BEFORE the election thread exists. Doing it
    // after let a loaded machine time out first and run an election, briefly
    // flipping the node to CANDIDATE — a window where metadata writes failed
    // with NOT_CONTROLLER.
    if (peers_.empty()) {
        Logger::info("Single-node mode: becoming leader immediately");
        std::lock_guard<std::mutex> lock(log_mutex_);
        becomeLeader();
    }

    election_thread_ = std::thread(&RaftNode::electionThread, this);
    heartbeat_thread_ = std::thread(&RaftNode::heartbeatThread, this);
    Logger::info("Started Raft node {} on port {} (term={}, log_size={})", id_, raft_port_,
                 current_term_.load(), log_.size());
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
        // Stop the apply thread AFTER the producers (election/heartbeat threads)
        // are joined, so commit_index_ can no longer advance. The worker drains
        // any remaining committed entries (already durable in the Raft log) and
        // exits. Joined here, before persist_db_ is closed, so a draining apply
        // never touches torn-down state.
        {
            std::lock_guard<std::mutex> lk(apply_mutex_);
            apply_running_ = false;
        }
        apply_cv_.notify_one();
        if (apply_thread_.joinable()) {
            apply_thread_.join();
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

    std::lock_guard<std::mutex> lock(log_mutex_);
    if (!isLeader()) {
        promise.set_exception(std::make_exception_ptr(std::runtime_error("Not the leader")));
        return future;
    }

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
    response.vote_granted = false;

    // If request term is greater, become follower (advances current_term_).
    if (request.term > current_term_) {
        becomeFollower(request.term);
    }

    // Report the term AFTER any advance above. Capturing it before becomeFollower
    // was a correctness bug: a peer granting a vote to a higher-term candidate
    // would echo its stale (lower) term, so the candidate's
    // `response.term == election_term` check failed and the granted vote was
    // never counted — the cluster could never elect a leader.
    response.term = current_term_;

    // Don't grant vote if term is less
    if (request.term < current_term_) {
        return response;
    }

    // Check if we haven't voted or already voted for this candidate
    bool can_vote = (voted_for_ == -1 || voted_for_ == request.candidate_id);

    // Check if candidate's log is at least as up-to-date as ours
    int64_t last_log_index = log_.empty() ? 0 : log_.back().index;
    int64_t last_log_term = log_.empty() ? 0 : log_.back().term;

    bool log_up_to_date =
        (request.last_log_term > last_log_term) ||
        (request.last_log_term == last_log_term && request.last_log_index >= last_log_index);

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
    response.success = false;

    // If request term is greater, become follower (advances current_term_).
    if (request.term > current_term_) {
        becomeFollower(request.term);
    }

    // Report the term AFTER any advance, so the leader sees the follower's true
    // current term (same stale-term fix as handleRequestVote).
    response.term = current_term_;

    // Reject if term is less
    if (request.term < current_term_) {
        return response;
    }

    if (state_ == NodeState::CANDIDATE)
        becomeFollower(request.term);

    // Reset election timeout on valid heartbeat
    resetElectionTimeout();
    leader_id_ = request.leader_id;
    {
        // M8-E1: after a gap in leader contact (frozen, partitioned) our applied
        // metadata may be stale; count as current again only once caught up.
        const auto now = std::chrono::steady_clock::now();
        if (now - last_leader_contact_ > kCatchUpAfterContactGap) {
            first_leader_commit_ = -1;
        }
        last_leader_contact_ = now;
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
        if (log_.size() > log_insert_pos && log_[log_insert_pos].term != entry.term) {
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
        int64_t new_commit = std::min(request.leader_commit, log_.empty() ? 0L : log_.back().index);
        commit_index_ = new_commit;
        applyCommittedEntries();
    }

    response.success = true;
    response.last_log_index = log_.empty() ? 0 : log_.back().index;
    response.term = current_term_;
    if (first_leader_commit_ < 0) {
        first_leader_commit_ = request.leader_commit;
    }

    return response;
}

bool RaftNode::shouldSnapshot() const {
    if (state_.load() != NodeState::LEADER)
        return false;
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
    std::string snap = "{\"version\":1,\"last_applied\":" + std::to_string(last_applied) +
                       ",\"term\":" + std::to_string(current_term) + "}";
    return std::vector<uint8_t>(snap.begin(), snap.end());
}

void RaftNode::sendSnapshotIfNeeded() {
    if (!shouldSnapshot())
        return;

    // Capture log state under lock.
    int64_t last_included_index = 0;
    int64_t last_included_term = 0;
    {
        std::lock_guard<std::mutex> lock(log_mutex_);
        if (log_.empty())
            return;
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

    Logger::info("Leader {} initiating snapshot (last_included_index={}, term={}, bytes={})", id_,
                 last_included_index, last_included_term, snapshot_data.size());

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
        auto new_end = std::remove_if(log_.begin(), log_.end(), [&](const LogEntry& e) {
            return e.index <= last_included_index;
        });
        log_.erase(new_end, log_.end());
    }
    Logger::info("Leader {} truncated log past index {}", id_, last_included_index);
}

InstallSnapshotResponse RaftNode::handleInstallSnapshot(const InstallSnapshotRequest& request) {
    // Phase 5.2: handle InstallSnapshot RPC. Steps per Raft §7:
    //   1. Reply immediately if term < currentTerm
    //   2. Create new snapshot file if first chunk (offset = 0)
    //   3. Write data chunk at offset
    //   4. If done=false, return; wait for more chunks
    //   5. Save snapshot, discard older log entries, reset state machine
    InstallSnapshotResponse response;
    {
        std::lock_guard<std::mutex> lock(log_mutex_);
        response.term = current_term_;
        if (request.term < current_term_)
            return response;
        if (request.term > current_term_)
            becomeFollower(request.term);
        response.term = current_term_;
        leader_id_ = request.leader_id;
        resetElectionTimeout();
    }

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
        Logger::warn("InstallSnapshot: unexpected offset {} (have {} bytes)", request.offset,
                     incoming_snapshot_->data.size());
    }
    incoming_snapshot_->data.insert(incoming_snapshot_->data.end(), request.data.begin(),
                                    request.data.end());

    if (!request.done) {
        response.term = current_term_.load();
        return response;
    }

    // Final chunk — install the snapshot.
    Logger::info("InstallSnapshot complete: last_included_index={} term={} bytes={}",
                 incoming_snapshot_->last_included_index, incoming_snapshot_->last_included_term,
                 incoming_snapshot_->data.size());

    // Truncate log entries up to and including last_included_index.
    {
        std::lock_guard<std::mutex> log_lock(log_mutex_);
        auto new_end = std::remove_if(log_.begin(), log_.end(), [&](const LogEntry& e) {
            return e.index <= incoming_snapshot_->last_included_index;
        });
        log_.erase(new_end, log_.end());

        // Advance the snapshot floor and the commit/applied counters together,
        // all under log_mutex_, so the apply thread (which reads them under the
        // same lock) skips the folded indices without ever applying onto a hole
        // and without a non-monotonic last_applied_. apply_floor_ tells the
        // worker "everything up to here is already captured by the snapshot —
        // do not invoke the callback for it".
        const int64_t lii = incoming_snapshot_->last_included_index;
        if (apply_floor_.load() < lii)
            apply_floor_.store(lii);
        if (commit_index_.load() < lii)
            commit_index_.store(lii);
        if (last_applied_.load() < lii)
            last_applied_.store(lii);
    }

    // Snapshot bytes themselves are opaque to Raft; a real implementation
    // would hand them to the state-machine layer (MetadataController in
    // our case). Phase 3.3 will populate that path.

    incoming_snapshot_.reset();
    apply_cv_.notify_one();
    response.term = current_term_.load();
    return response;
}

void RaftNode::electionThread() {
    while (running_) {
        std::chrono::milliseconds wait_for;
        {
            std::lock_guard<std::mutex> lock(log_mutex_);
            wait_for = election_timeout_;
            if (state_ != NodeState::LEADER) {
                const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::steady_clock::now() - last_heartbeat_);
                wait_for = std::max(election_timeout_ - elapsed, std::chrono::milliseconds::zero());
            }
        }
        std::unique_lock<std::mutex> lock(election_cv_mutex_);
        election_cv_.wait_for(lock, wait_for, [this] { return !running_; });
        lock.unlock();
        if (running_)
            startElection();  // rechecks the timer under log_mutex_
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
        } else {
            pending_heartbeats_.clear();
        }
    }
    pending_heartbeats_.clear();
}

void RaftNode::startElection() {
    RequestVoteRequest request;
    {
        std::lock_guard<std::mutex> lock(log_mutex_);
        if (!running_ || state_ == NodeState::LEADER || !hasElectionTimedOut())
            return;
        becomeCandidate();
        request.term = current_term_;
        request.candidate_id = id_;
        request.last_log_index = log_.empty() ? 0 : log_.back().index;
        request.last_log_term = log_.empty() ? 0 : log_.back().term;
        if (peers_.empty()) {
            becomeLeader();
            return;
        }
    }
    Logger::info("Starting election for term {}", request.term);
    int votes = 1;
    const int needed = static_cast<int>((peers_.size() + 1) / 2 + 1);
    std::vector<std::future<RequestVoteResponse>> responses;
    for (const auto& peer : peers_) {
        responses.push_back(transport_->sendRequestVote(peer.id, request));
    }
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(100);
    for (auto& future : responses) {
        const auto remaining = std::max(deadline - std::chrono::steady_clock::now(),
                                        std::chrono::steady_clock::duration::zero());
        if (future.wait_for(remaining) != std::future_status::ready)
            continue;
        try {
            const auto response = future.get();
            std::lock_guard<std::mutex> lock(log_mutex_);
            if (response.term > current_term_) {
                becomeFollower(response.term);
                return;
            }
            if (current_term_ != request.term || state_ != NodeState::CANDIDATE)
                return;
            if (response.vote_granted && response.term == request.term)
                ++votes;
        } catch (const std::exception& e) {
            Logger::debug("RequestVote failed: {}", e.what());
        }
    }
    std::lock_guard<std::mutex> lock(log_mutex_);
    if (running_ && current_term_ == request.term && state_ == NodeState::CANDIDATE &&
        votes >= needed) {
        becomeLeader();
    }
}

void RaftNode::sendHeartbeats() {
    // Harvest all ready replies without waiting for an unresponsive peer.
    for (auto it = pending_heartbeats_.begin(); it != pending_heartbeats_.end();) {
        if (it->second.response.wait_for(std::chrono::milliseconds(0)) !=
            std::future_status::ready) {
            ++it;
            continue;
        }
        try {
            const auto response = it->second.response.get();
            std::lock_guard<std::mutex> lock(log_mutex_);
            if (response.term > current_term_) {
                becomeFollower(response.term);
            } else if (state_ == NodeState::LEADER && current_term_ == it->second.term) {
                for (auto& peer : peers_) {
                    if (peer.id != it->first)
                        continue;
                    peer.last_ack = std::chrono::steady_clock::now();
                    if (response.success) {
                        // Credit only the entries in this RPC, not a follower's
                        // potentially divergent extra tail.
                        peer.match_index = it->second.last_index;
                        peer.next_index = peer.match_index + 1;
                        updateCommitIndex();
                    } else {
                        const auto next = std::max<int64_t>(1, response.last_log_index + 1);
                        peer.next_index = next < peer.next_index
                                              ? next
                                              : std::max<int64_t>(1, peer.next_index - 1);
                    }
                    break;
                }
            }
        } catch (const std::exception& e) {
            Logger::debug("AppendEntries to peer {} failed: {}", it->first, e.what());
        }
        it = pending_heartbeats_.erase(it);
    }
    struct Send {
        BrokerId peer;
        AppendEntriesRequest request;
    };
    std::vector<Send> sends;
    {
        std::lock_guard<std::mutex> lock(log_mutex_);
        if (state_ != NodeState::LEADER)
            return;
        for (const auto& peer : peers_) {
            if (pending_heartbeats_.count(peer.id))
                continue;
            AppendEntriesRequest request;
            request.term = current_term_;
            request.leader_id = id_;
            request.prev_log_index = peer.next_index - 1;
            request.prev_log_term = request.prev_log_index > 0
                                        ? log_[static_cast<size_t>(request.prev_log_index - 1)].term
                                        : 0;
            request.leader_commit = commit_index_;
            for (size_t i = static_cast<size_t>(peer.next_index - 1); i < log_.size(); ++i) {
                request.entries.push_back(log_[i]);
            }
            sends.push_back({peer.id, std::move(request)});
        }
    }
    for (const auto& send : sends) {
        try {
            const auto last =
                send.request.prev_log_index + static_cast<int64_t>(send.request.entries.size());
            pending_heartbeats_.emplace(send.peer, PendingHeartbeat{send.request.term, last,
                                                                    transport_->sendAppendEntries(
                                                                        send.peer, send.request)});
        } catch (const std::exception& e) {
            Logger::debug("AppendEntries dispatch failed: {}", e.what());
        }
    }
}

void RaftNode::becomeFollower(int64_t term) {
    if (term < current_term_)
        return;
    const bool new_term = term > current_term_;
    current_term_ = term;
    state_ = NodeState::FOLLOWER;
    first_leader_commit_ = -1;  // M8-E1: catch up with the new leadership first
    if (new_term)
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
    {
        // Initialize peer state before any heartbeat can observe LEADER.
        const auto now = std::chrono::steady_clock::now();
        for (auto& peer : peers_) {
            peer.next_index = log_.empty() ? 1 : log_.back().index + 1;
            peer.match_index = 0;
            // Grace window: a new controller must not judge a peer dead before
            // it has had a full liveness window to answer this leader.
            peer.last_ack = now;
        }
        if (!peers_.empty()) {
            // Raft §5.4.2: commit a no-op of our own term so entries committed
            // by the previous leader (e.g. a failover) are applied promptly
            // rather than when the next command happens to arrive.
            LogEntry noop;
            noop.term = current_term_;
            noop.index = log_.empty() ? 1 : log_.back().index + 1;
            noop.command_type = "noop";
            log_.push_back(noop);
            persistLogEntry(noop);
        }
        state_ = NodeState::LEADER;
        leader_id_ = id_;
    }

    Logger::info("Node {} became leader for term {}", id_, current_term_.load());
}

bool RaftNode::hasCurrentMetadata(int64_t lease_ms) const {
    std::lock_guard<std::mutex> lock(log_mutex_);
    const auto now = std::chrono::steady_clock::now();
    const auto lease = std::chrono::milliseconds(lease_ms);
    switch (state_.load()) {
        case NodeState::LEADER: {
            size_t acked = 1;  // ourselves
            for (const auto& peer : peers_) {
                if (now - peer.last_ack <= lease) {
                    ++acked;
                }
            }
            return acked * 2 > peers_.size() + 1;
        }
        case NodeState::FOLLOWER:
            return first_leader_commit_ >= 0 && last_applied_.load() >= first_leader_commit_ &&
                   now - last_leader_contact_ <= lease;
        case NodeState::CANDIDATE:
            return false;
    }
    return false;
}

int64_t RaftNode::lastLogIndex() const {
    std::lock_guard<std::mutex> lock(log_mutex_);
    return log_.empty() ? 0 : log_.back().index;
}

std::map<BrokerId, int64_t> RaftNode::peerAckAgesMs() const {
    std::map<BrokerId, int64_t> ages;
    std::lock_guard<std::mutex> lock(log_mutex_);
    if (state_ != NodeState::LEADER) {
        return ages;
    }
    const auto now = std::chrono::steady_clock::now();
    for (const auto& peer : peers_) {
        ages[peer.id] =
            std::chrono::duration_cast<std::chrono::milliseconds>(now - peer.last_ack).count();
    }
    return ages;
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

void RaftNode::setCommitCallback(std::function<void(const LogEntry&)> callback) {
    std::lock_guard<std::mutex> lock(log_mutex_);
    commit_callback_ = std::move(callback);
}

void RaftNode::applyCommittedEntries() {
    // The heavy state-machine application now runs on the dedicated apply
    // thread (applyThread), OFF log_mutex_. Callers (which hold log_mutex_)
    // only need to wake it; the actual apply happens asynchronously. notify_one
    // does not block and does not touch log_mutex_.
    apply_cv_.notify_one();
}

void RaftNode::applyThread() {
    while (true) {
        {
            std::unique_lock<std::mutex> wl(apply_mutex_);
            // Timed wait: the producers advance commit_index_ (atomic) WITHOUT
            // holding apply_mutex_, so an untimed wait could miss a wakeup. The
            // 100ms cap guarantees the worker re-checks and drains regardless.
            apply_cv_.wait_for(wl, std::chrono::milliseconds(100), [this] {
                return !apply_running_.load() || last_applied_.load() < commit_index_.load();
            });
        }

        // Drain every currently-committed entry, one at a time, in index order.
        while (true) {
            LogEntry entry;
            std::function<void(const LogEntry&)> cb;
            int64_t idx = 0;
            bool have_entry = false;
            {
                std::lock_guard<std::mutex> lk(log_mutex_);
                int64_t from = last_applied_.load();
                // Skip indices already captured by an installed snapshot.
                if (apply_floor_.load() > from) {
                    from = apply_floor_.load();
                    last_applied_.store(from);
                }
                const int64_t to = commit_index_.load();
                if (from >= to)
                    break;  // fully drained
                idx = from + 1;
                const int64_t log_size = static_cast<int64_t>(log_.size());
                if (idx > log_size) {
                    // Entry isn't in memory — folded into a snapshot or
                    // truncated after a prior apply. Advance with no callback.
                    last_applied_.store(to);
                    continue;
                }
                cb = commit_callback_;
                if (!cb) {
                    if (!apply_running_.load()) {
                        // Stopping with no state machine attached (it detached
                        // first): nothing can apply these entries now. They stay
                        // durable in the log and are re-delivered next start.
                        return;
                    }
                    // Callback not installed yet (startup window): leave the
                    // entry unapplied and re-wait (timed) until it's set.
                    break;
                }
                entry = log_[static_cast<size_t>(idx - 1)];  // copy BY VALUE under lock
                have_entry = true;
            }

            if (have_entry) {
                try {
                    cb(entry);  // HEAVY work (RocksDB/metadata) — NO lock held
                } catch (const std::exception& e) {
                    Logger::error("commit_callback_ threw at index {}: {}", idx, e.what());
                } catch (...) {
                    Logger::error("commit_callback_ threw at index {}", idx);
                }
                // Advance last_applied_ UNDER log_mutex_, monotonically, never
                // below the snapshot floor (the snapshot writer also raises both
                // under log_mutex_, so they never go non-monotonic).
                std::lock_guard<std::mutex> lk(log_mutex_);
                const int64_t want = std::max(idx, apply_floor_.load());
                if (want > last_applied_.load()) {
                    last_applied_.store(want);
                }
            }
        }

        if (!apply_running_.load() && last_applied_.load() >= commit_index_.load()) {
            return;  // stop requested and fully drained
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
    std::unique_ptr<rocksdb::DB> db_raw;
    auto status = openRocksDb(opts, data_dir_, db_raw);
    if (!status.ok()) {
        Logger::error("Failed to open Raft persistence at {}: {}", data_dir_, status.ToString());
        return;
    }
    persist_db_ = std::move(db_raw);
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
    std::unique_ptr<rocksdb::Iterator> it(persist_db_->NewIterator(rocksdb::ReadOptions()));
    for (it->Seek(kLogKeyPrefix); it->Valid(); it->Next()) {
        const std::string key = it->key().ToString();
        if (key.compare(0, std::strlen(kLogKeyPrefix), kLogKeyPrefix) != 0) {
            break;  // iterated past log namespace
        }
        const int64_t idx = decodeLogKey(key);
        if (idx < 0)
            continue;
        const std::string raw = it->value().ToString();
        std::vector<uint8_t> bytes(raw.begin(), raw.end());
        try {
            ByteBuffer bb(bytes);
            LogEntry entry = LogEntryCodec::decodeFrom(bb);
            log_.push_back(std::move(entry));
        } catch (const std::exception& ex) {
            Logger::error("Failed to decode persisted log entry at idx={}: {}", idx, ex.what());
        }
    }
    if (!log_.empty()) {
        Logger::info("Restored {} Raft log entries (last index={}, last term={})", log_.size(),
                     log_.back().index, log_.back().term);
    }
}

void RaftNode::persistTerm() {
    if (!persist_db_)
        return;
    int64_t v = current_term_.load();
    rocksdb::WriteOptions wo;
    wo.sync = true;
    persist_db_->Put(wo, kKeyCurrentTerm,
                     rocksdb::Slice(reinterpret_cast<const char*>(&v), sizeof(v)));
}

void RaftNode::persistVotedFor() {
    if (!persist_db_)
        return;
    BrokerId v = voted_for_.load();
    rocksdb::WriteOptions wo;
    wo.sync = true;
    persist_db_->Put(wo, kKeyVotedFor,
                     rocksdb::Slice(reinterpret_cast<const char*>(&v), sizeof(v)));
}

void RaftNode::persistLogEntry(const LogEntry& entry) {
    if (!persist_db_)
        return;
    ByteBuffer bb;
    LogEntryCodec::encodeInto(bb, entry);
    auto encoded = bb.release();
    rocksdb::WriteOptions wo;
    wo.sync = true;
    persist_db_->Put(wo, encodeLogKey(entry.index),
                     rocksdb::Slice(reinterpret_cast<const char*>(encoded.data()), encoded.size()));
}

void RaftNode::truncateLogFrom(int64_t index) {
    if (!persist_db_)
        return;
    rocksdb::WriteBatch batch;
    std::unique_ptr<rocksdb::Iterator> it(persist_db_->NewIterator(rocksdb::ReadOptions()));
    for (it->Seek(encodeLogKey(index)); it->Valid(); it->Next()) {
        const std::string key = it->key().ToString();
        if (key.compare(0, std::strlen(kLogKeyPrefix), kLogKeyPrefix) != 0)
            break;
        batch.Delete(key);
    }
    rocksdb::WriteOptions wo;
    wo.sync = true;
    persist_db_->Write(wo, &batch);
}

void RaftNode::updateCommitIndex() {
    // PRECONDITION: the caller (sendHeartbeats) already holds log_mutex_. This
    // function must NOT re-lock it — std::mutex is non-recursive, so re-locking
    // here self-deadlocked the heartbeat thread the first time an AppendEntries
    // succeeded, so commit_index_ never advanced and no command ever committed
    // (createTopic/metadata writes hung forever in a multi-broker cluster).
    // This should be called by leader only.
    if (state_ != NodeState::LEADER) {
        return;
    }

    // Find the highest index that's been replicated to a majority
    const int64_t log_size = static_cast<int64_t>(log_.size());

    for (int64_t n = log_size; n > commit_index_; --n) {
        // Count how many peers have replicated up to index n
        int replicas = 1;  // Count self
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
