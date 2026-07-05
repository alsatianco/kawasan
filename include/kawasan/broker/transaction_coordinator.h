#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace kawasan::broker {

// TransactionCoordinator: single-node transactional state machine.
//
// Implemented: InitProducerId, AddPartitionsToTxn, AddOffsetsToTxn,
// TxnOffsetCommit (staged, KIP-447), and a genuinely two-phase EndTxn
// (prepareCommit/completeCommit and prepareAbort/completeAbort). The broker
// emits control records to participating partitions and tracks LSO via the
// IsolationTracker. Snapshots are persisted to __transaction_state by the
// TransactionStateManager and replayed on startup (M1), so state — including
// mid-EndTxn Prepare* states and staged offsets — survives a restart.
//
// State values mirror Kafka's `TransactionState`:
//   Empty, Ongoing, PrepareCommit, PrepareAbort, CompleteCommit,
//   CompleteAbort, Dead, PrepareEpochFence
//
// M2 added producer-epoch fencing (InitProducerId bumps the epoch and fences
// the prior instance; the txn APIs reject stale epochs with
// INVALID_PRODUCER_EPOCH) and transaction-timeout auto-abort (a broker sweep
// aborts Ongoing txns past transaction.timeout.ms). NOT yet done:
// WriteTxnMarkers (API 27) for multi-broker marker fan-out, and multi-broker
// replication of __transaction_state (needs follower fetch).
class TransactionCoordinator {
public:
    /// @brief Wall-clock source (epoch ms). Injectable so the M2 timeout
    /// sweep is deterministically unit-testable. Defaults to system_clock.
    using Clock = std::function<int64_t()>;

    TransactionCoordinator() = default;
    explicit TransactionCoordinator(Clock clock) : clock_(std::move(clock)) {}

    enum class State {
        Empty,
        Ongoing,
        PrepareCommit,
        PrepareAbort,
        CompleteCommit,
        CompleteAbort,
        Dead,
        PrepareEpochFence,
    };

    /// @brief Phase EX-6: pending consumer-group offset, staged by
    /// TxnOffsetCommit and applied on EndTxn(commit=true). KIP-447:
    /// transactional offset commits must NOT be visible to other
    /// consumers until the transaction commits.
    struct PendingOffset {
        std::string group_id;
        std::string topic;
        int32_t partition = 0;
        int64_t offset = 0;
        std::string metadata;
    };

    /// @brief M1: a partition participating in a transaction, plus the
    /// log-end offset captured when it was added (`first_offset`). The
    /// first_offset is the LSO hold — read_committed consumers must not
    /// advance past it until the txn completes — and MUST be persisted so
    /// startup replay can re-register the in-flight txn in the
    /// IsolationTracker at the same offset. `first_offset < 0` means "no
    /// LSO hold for this partition" (e.g. the __consumer_offsets partition
    /// added by AddOffsetsToTxn, which still gets a control marker).
    struct TxnPartition {
        std::string topic;
        int32_t partition = 0;
        int64_t first_offset = -1;
    };

    struct TxnSnapshot {
        std::string transactional_id;
        int64_t producer_id = -1;
        int16_t producer_epoch = -1;
        int32_t transaction_timeout_ms = 0;
        State state = State::Empty;
        int64_t state_start_time_ms = 0;
        // Phase 3.3 / M1: partitions added to the in-flight transaction via
        // AddPartitionsToTxn, each with its captured first_offset. EndTxn
        // fires control records to each.
        std::vector<TxnPartition> partitions;
        // Phase EX-6: offsets staged by TxnOffsetCommit. Applied to the
        // OffsetManager only when the transaction commits; discarded on
        // abort. This gates consumer-group offset visibility on the
        // transaction outcome — required for Streams EOS v2.
        std::vector<PendingOffset> pending_offsets;
    };

    /// @brief Records that an InitProducerId call established (or
    /// re-established) the producer for this transactional_id.
    void recordInitProducerId(const std::string& transactional_id, int64_t producer_id,
                              int16_t producer_epoch, int32_t transaction_timeout_ms);

    /// @brief Phase 3.3: AddPartitionsToTxn registers partitions with
    /// the in-flight transaction. State transitions Empty → Ongoing.
    void addPartitions(const std::string& transactional_id,
                       const std::vector<TxnPartition>& partitions);

    /// @brief M1 two-phase commit, step 1: Ongoing → PrepareCommit. Keeps
    /// partitions AND pending_offsets intact so the persisted PrepareCommit
    /// snapshot is complete and a crash-recovery re-drive can finish the
    /// commit. Returns participating partitions for control-record emission.
    std::vector<TxnPartition> prepareCommit(const std::string& transactional_id);

    /// @brief M1 two-phase commit, step 2: PrepareCommit → CompleteCommit.
    /// Clears partitions and pending_offsets and bumps the commit counter.
    /// Call AFTER control records are emitted and staged offsets applied.
    void completeCommit(const std::string& transactional_id);

    /// @brief M1 two-phase abort, step 1: Ongoing → PrepareAbort. Keeps
    /// partitions and pending_offsets so the persisted snapshot is complete.
    /// Returns participating partitions for control-record emission.
    std::vector<TxnPartition> prepareAbort(const std::string& transactional_id);

    /// @brief M1 two-phase abort, step 2: PrepareAbort → CompleteAbort.
    /// Clears partitions and pending_offsets (aborts discard staged offsets)
    /// and bumps the abort counter.
    void completeAbort(const std::string& transactional_id);

    /// @brief M1: restore a snapshot verbatim into the coordinator during
    /// startup replay of __transaction_state. Unlike recordInitProducerId
    /// (which forces state=Empty) this preserves the exact persisted state,
    /// including Prepare* and the per-partition first_offsets. Does not
    /// touch metric counters.
    void restore(const TxnSnapshot& snapshot);

    /// @brief Phase EX-6: stage offsets from TxnOffsetCommit. The
    /// offsets are buffered in the in-flight txn snapshot and applied
    /// to the OffsetManager only when commitTxn() fires. If abortTxn()
    /// fires first, the staged offsets are discarded — this is what
    /// makes consumer-group offset commits truly transactional and is
    /// required for Streams EOS v2 (KIP-447).
    void stagePendingOffsets(const std::string& transactional_id,
                             std::vector<PendingOffset> offsets);

    /// @brief Phase EX-6: drain staged offsets for the given txn. Called
    /// during commitTxn() processing to apply offsets to OffsetManager.
    /// Returns the moved-out list (caller takes ownership). Idempotent
    /// — subsequent calls return empty.
    std::vector<PendingOffset> drainPendingOffsets(const std::string& transactional_id);

    /// @brief Lists all known transactional IDs, optionally filtered by
    /// state and producer_id.
    std::vector<TxnSnapshot> list(const std::vector<std::string>& state_filters,
                                  const std::vector<int64_t>& producer_id_filters) const;

    /// @brief Returns the snapshot for a single transactional_id, or
    /// nullopt if unknown.
    std::optional<TxnSnapshot> describe(const std::string& transactional_id) const;

    /// @brief M2: snapshots of all Ongoing transactions whose deadline has
    /// passed (transaction_timeout_ms > 0 and now_ms - state_start_time_ms >
    /// transaction_timeout_ms). The background sweep aborts each so a hung
    /// transaction never blocks read_committed consumers forever.
    std::vector<TxnSnapshot> expiredOngoing(int64_t now_ms) const;

    /// @brief M2: the coordinator's current wall-clock (epoch ms), via the
    /// injected clock. Used by the broker sweep to compute deadlines.
    int64_t nowMs() const { return clock_(); }

    /// @brief Returns the state-name string Kafka uses on the wire.
    static const char* stateName(State s);

    /// @brief Phase EX-1 (§6.3): Prometheus metrics snapshot.
    struct Metrics {
        int64_t in_progress;        // gauge: txns currently in Ongoing state
        int64_t commits_total;      // counter
        int64_t aborts_total;       // counter
        int64_t state_loads_total;  // counter: InitProducerId calls
    };
    Metrics getMetrics() const;

private:
    static int64_t systemNowMs() {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::system_clock::now().time_since_epoch())
            .count();
    }

    Clock clock_ = &TransactionCoordinator::systemNowMs;
    mutable std::mutex mutex_;
    std::unordered_map<std::string, TxnSnapshot> txns_;
    // Phase EX-1 metrics.
    std::atomic<int64_t> commits_total_{0};
    std::atomic<int64_t> aborts_total_{0};
    std::atomic<int64_t> state_loads_total_{0};
};

}  // namespace kawasan::broker
