#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <future>
#include <thread>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include <boost/asio.hpp>

#include "kawasan/common/types.h"
#include "kawasan/raft/raft_protocol.h"

// Forward declarations
namespace rocksdb { class DB; }
namespace kawasan::raft {
class RaftTransport;
}

namespace kawasan::raft {

/// @brief State of a Raft node
enum class NodeState { FOLLOWER, CANDIDATE, LEADER };

/// @brief Raft peer information
struct PeerInfo {
    BrokerId id;
    std::string host;
    int32_t port;
    int64_t next_index = 1;
    int64_t match_index = 0;
};

/// @brief Raft node for consensus
class RaftNode {
public:
    /// @param id local broker id
    /// @param peers list of other Raft peers (empty for single-node)
    /// @param io_context shared Asio io_context for transport
    /// @param raft_port TCP port for Raft RPCs
    /// @param data_dir 0A.7: directory for persistent Raft state. If empty
    ///                  (the default), state is kept in-memory only — safe for
    ///                  single-node restarts but not Raft-safe for multi-node.
    ///                  When provided, current_term, voted_for, and log
    ///                  entries are persisted to a RocksDB instance there.
    RaftNode(BrokerId id, const std::vector<PeerInfo>& peers,
             boost::asio::io_context& io_context, int raft_port = 9093,
             std::string data_dir = "");
    ~RaftNode();

    // Non-copyable/movable
    RaftNode(const RaftNode&) = delete;
    RaftNode& operator=(const RaftNode&) = delete;
    RaftNode(RaftNode&&) = delete;
    RaftNode& operator=(RaftNode&&) = delete;

    /// @brief Starts the Raft node
    void start();

    /// @brief Stops the Raft node
    void stop();

    /// @brief Appends a command to the log (leader only)
    /// @param command The command data
    /// @param command_type Type of command
    /// @return Future with the log index
    std::future<int64_t> appendCommand(const std::vector<uint8_t>& command,
                                       const std::string& command_type);

    /// @brief Handles RequestVote RPC
    RequestVoteResponse handleRequestVote(const RequestVoteRequest& request);

    /// @brief Handles AppendEntries RPC
    AppendEntriesResponse handleAppendEntries(const AppendEntriesRequest& request);

    /// @brief Phase 5.2: Handles InstallSnapshot RPC. Used by the leader
    /// to ship a snapshot to a follower whose log has fallen behind the
    /// leader's earliest log entry. Our implementation accepts the
    /// snapshot chunk-by-chunk, persists the cumulative bytes, and once
    /// `done=true` truncates the local log and updates last_applied to
    /// the snapshot's last_included_index. Snapshot data interpretation
    /// is deferred to the state-machine layer (Phase 3.3 transactions
    /// will populate this with real state).
    InstallSnapshotResponse handleInstallSnapshot(const InstallSnapshotRequest& request);

    /// @brief Phase 5.2: returns true if the log has grown past the
    /// configured threshold and a snapshot should be generated. The
    /// leader's heartbeat thread checks this periodically; when it
    /// returns true the leader generates a snapshot and ships it to
    /// peers whose `next_index` is below the snapshot's
    /// `last_included_index`.
    bool shouldSnapshot() const;

    /// @brief Phase 5.2: configurable snapshot trigger threshold.
    /// Default 10000 log entries.
    void setSnapshotThreshold(int64_t threshold) { snapshot_threshold_ = threshold; }

    /// @brief Phase 5.2: generate a snapshot of the local state. Bytes
    /// are opaque to Raft — a state-machine layer would supply the
    /// actual MetadataController serialization. For now returns a
    /// minimal stub `{"version":1,"last_applied":N}` so the leader-side
    /// send orchestration can be exercised.
    std::vector<uint8_t> generateSnapshot();

    /// @brief Phase 5.2: leader-side snapshot send. Called periodically
    /// from the heartbeat thread when `shouldSnapshot()` is true.
    /// Generates a snapshot, sends it to every peer whose `next_index`
    /// is below the snapshot boundary, and truncates the local log
    /// after successful replication.
    void sendSnapshotIfNeeded();

    /// @brief Returns the current state
    NodeState state() const { return state_.load(); }

    /// @brief Returns the current term
    int64_t currentTerm() const { return current_term_.load(); }

    /// @brief Returns the current leader ID
    BrokerId leaderId() const { return leader_id_.load(); }

    /// @brief Returns whether this node is the leader
    bool isLeader() const { return state_.load() == NodeState::LEADER; }

    /// @brief Returns the commit index
    int64_t commitIndex() const { return commit_index_.load(); }

    /// @brief Sets the commit callback. Taken under log_mutex_ (out-of-line) so
    /// the apply thread never reads a torn std::function while it's being set.
    void setCommitCallback(std::function<void(const LogEntry&)> callback);

private:
    void electionThread();
    void heartbeatThread();
    void startElection();
    void sendHeartbeats();
    void becomeFollower(int64_t term);
    void becomeCandidate();
    void becomeLeader();
    void resetElectionTimeout();
    bool hasElectionTimedOut() const;
    /// @brief Wakes the apply thread (a commit_index_ advance may be pending).
    /// Heavy state-machine application no longer runs inline under log_mutex_.
    void applyCommittedEntries();
    /// @brief Dedicated single-consumer thread that applies committed log
    /// entries to the state machine (via commit_callback_) OFF log_mutex_, in
    /// strict index order, exactly once. Copying entries out under the lock and
    /// applying outside it keeps the heavy work (RocksDB log creation, metadata
    /// persistence) from stalling Raft heartbeats/replication.
    void applyThread();
    void updateCommitIndex();

    BrokerId id_;
    std::vector<PeerInfo> peers_;
    std::atomic<NodeState> state_{NodeState::FOLLOWER};
    std::atomic<int64_t> current_term_{0};
    std::atomic<BrokerId> voted_for_{-1};
    std::atomic<BrokerId> leader_id_{-1};
    std::atomic<int64_t> commit_index_{0};
    std::atomic<int64_t> last_applied_{0};

    std::vector<LogEntry> log_;
    mutable std::mutex log_mutex_;

    std::chrono::steady_clock::time_point last_heartbeat_;
    std::chrono::milliseconds election_timeout_{150};
    std::chrono::milliseconds heartbeat_interval_{50};

    std::atomic<bool> running_{false};
    std::thread election_thread_;
    std::thread heartbeat_thread_;

    // Apply pipeline: a single dedicated thread applies committed entries to the
    // state machine off log_mutex_ (see applyThread). apply_mutex_ guards ONLY
    // the CV handshake and is NEVER held together with log_mutex_. apply_floor_
    // is the highest index folded into an installed snapshot; entries at/below it
    // are skipped (no callback) since the snapshot already captured that state.
    std::thread apply_thread_;
    mutable std::mutex apply_mutex_;
    std::condition_variable apply_cv_;
    std::atomic<bool> apply_running_{false};
    std::atomic<int64_t> apply_floor_{0};

    // Phase 5.2: condition-variable-driven wake for the election thread.
    // Instead of polling every 10 ms (the §G10 "busy loop"), the election
    // thread waits up to (election_timeout_ - elapsed) on this CV; a
    // received heartbeat or a stop call signals it. Worst-case it still
    // wakes when the timeout fires, but the common case (steady leader)
    // wakes only on demand and stop is immediate.
    mutable std::mutex election_cv_mutex_;
    std::condition_variable election_cv_;

    std::function<void(const LogEntry&)> commit_callback_;

    // Network transport for Raft RPCs
    std::unique_ptr<RaftTransport> transport_;
    boost::asio::io_context& io_context_;
    int raft_port_;

    // 0A.7: persistent Raft state. Optional — empty data_dir means in-memory
    // only (safe for single-node, unsafe for multi-node restart).
    std::string data_dir_;
    std::unique_ptr<rocksdb::DB> persist_db_;

    // Phase 5.2: in-flight snapshot reception state. Cleared after a
    // `done=true` chunk is processed and the snapshot is applied.
    struct SnapshotAccumulator {
        int64_t last_included_index = 0;
        int64_t last_included_term = 0;
        std::vector<uint8_t> data;
    };
    mutable std::mutex snapshot_mutex_;
    std::unique_ptr<SnapshotAccumulator> incoming_snapshot_;
    // Phase 5.2: leader-side snapshot trigger threshold (log entries).
    int64_t snapshot_threshold_ = 10000;
    void openPersistence();
    void loadPersistedState();
    void persistTerm();
    void persistVotedFor();
    void persistLogEntry(const LogEntry& entry);
    void truncateLogFrom(int64_t index);
};

}  // namespace kawasan::raft
