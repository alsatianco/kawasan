#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace kawasan::broker {

// Phase 4.1k/4.1l: minimal in-memory TransactionCoordinator scaffolding.
//
// We don't yet implement the full Kafka transaction protocol (Phase 3.3
// is the real lift — AddPartitionsToTxn, EndTxn, WriteTxnMarkers,
// TxnOffsetCommit, control records, LSO, etc.). But we *do* track basic
// transactional_id state so that:
//   - `InitProducerId` for a non-null transactional_id records the
//     (txn_id, producer_id, epoch) tuple
//   - `ListTransactions` enumerates known transactional IDs with their
//     current state
//   - `DescribeTransactions` returns the per-txn snapshot
//
// State values mirror Kafka's `TransactionState`:
//   Empty, Ongoing, PrepareCommit, PrepareAbort, CompleteCommit,
//   CompleteAbort, Dead, PrepareEpochFence
//
// Without actual transactional API handlers, every transactional ID
// stays in "Empty" after InitProducerId. That's still a real, queryable
// state — better than a stub that lies about transaction support.
class TransactionCoordinator {
public:
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

    struct TxnSnapshot {
        std::string transactional_id;
        int64_t producer_id = -1;
        int16_t producer_epoch = -1;
        int32_t transaction_timeout_ms = 0;
        State state = State::Empty;
        int64_t state_start_time_ms = 0;
        // Phase 3.3: partitions added to the in-flight transaction via
        // AddPartitionsToTxn. EndTxn fires control records to each.
        std::vector<std::pair<std::string, int32_t>> partitions;
        // Phase EX-6: offsets staged by TxnOffsetCommit. Applied to the
        // OffsetManager only when the transaction commits; discarded on
        // abort. This gates consumer-group offset visibility on the
        // transaction outcome — required for Streams EOS v2.
        std::vector<PendingOffset> pending_offsets;
    };

    /// @brief Records that an InitProducerId call established (or
    /// re-established) the producer for this transactional_id.
    void recordInitProducerId(const std::string& transactional_id,
                              int64_t producer_id,
                              int16_t producer_epoch,
                              int32_t transaction_timeout_ms);

    /// @brief Phase 3.3: AddPartitionsToTxn registers partitions with
    /// the in-flight transaction. State transitions Empty → Ongoing.
    void addPartitions(const std::string& transactional_id,
                       const std::vector<std::pair<std::string, int32_t>>& partitions);

    /// @brief Phase 3.3: EndTxn(committed=true) transitions
    /// Ongoing → PrepareCommit → CompleteCommit. Returns the list of
    /// partitions that participated so the caller can emit control
    /// records.
    std::vector<std::pair<std::string, int32_t>> commitTxn(
        const std::string& transactional_id);

    /// @brief Phase 3.3: EndTxn(committed=false) transitions
    /// Ongoing → PrepareAbort → CompleteAbort. Returns participating
    /// partitions for control-record emission.
    std::vector<std::pair<std::string, int32_t>> abortTxn(
        const std::string& transactional_id);

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
    std::vector<PendingOffset> drainPendingOffsets(
        const std::string& transactional_id);

    /// @brief Lists all known transactional IDs, optionally filtered by
    /// state and producer_id.
    std::vector<TxnSnapshot> list(
        const std::vector<std::string>& state_filters,
        const std::vector<int64_t>& producer_id_filters) const;

    /// @brief Returns the snapshot for a single transactional_id, or
    /// nullopt if unknown.
    std::optional<TxnSnapshot> describe(const std::string& transactional_id) const;

    /// @brief Returns the state-name string Kafka uses on the wire.
    static const char* stateName(State s);

    /// @brief Phase EX-1 (§6.3): Prometheus metrics snapshot.
    struct Metrics {
        int64_t in_progress;          // gauge: txns currently in Ongoing state
        int64_t commits_total;        // counter
        int64_t aborts_total;         // counter
        int64_t state_loads_total;    // counter: InitProducerId calls
    };
    Metrics getMetrics() const;

private:
    mutable std::mutex mutex_;
    std::unordered_map<std::string, TxnSnapshot> txns_;
    // Phase EX-1 metrics.
    std::atomic<int64_t> commits_total_{0};
    std::atomic<int64_t> aborts_total_{0};
    std::atomic<int64_t> state_loads_total_{0};
};

}  // namespace kawasan::broker
