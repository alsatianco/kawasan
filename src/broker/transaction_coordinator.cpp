#include "kawasan/broker/transaction_coordinator.h"

#include <algorithm>
#include <utility>

namespace kawasan::broker {

namespace {

int64_t nowMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

}  // namespace

void TransactionCoordinator::recordInitProducerId(
    const std::string& transactional_id, int64_t producer_id,
    int16_t producer_epoch, int32_t transaction_timeout_ms) {
    if (transactional_id.empty()) return;
    state_loads_total_.fetch_add(1, std::memory_order_relaxed);
    std::lock_guard<std::mutex> lock(mutex_);
    auto& s = txns_[transactional_id];
    s.transactional_id = transactional_id;
    s.producer_id = producer_id;
    s.producer_epoch = producer_epoch;
    s.transaction_timeout_ms = transaction_timeout_ms;
    if (s.state == State::Empty || s.state == State::Dead) {
        s.state_start_time_ms = nowMs();
    }
    // Phase 3.3: InitProducerId resets the transaction to Empty (any
    // previous in-flight transaction is abandoned). Partition list is
    // cleared because a new txn won't share partitions.
    s.state = State::Empty;
    s.partitions.clear();
}

void TransactionCoordinator::addPartitions(
    const std::string& transactional_id,
    const std::vector<std::pair<std::string, int32_t>>& partitions) {
    if (transactional_id.empty()) return;
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = txns_.find(transactional_id);
    if (it == txns_.end()) return;
    auto& s = it->second;
    // Phase 3.3: state transition. Empty → Ongoing on the first
    // AddPartitionsToTxn of a new transaction.
    if (s.state == State::Empty || s.state == State::CompleteCommit ||
        s.state == State::CompleteAbort) {
        s.state = State::Ongoing;
        s.state_start_time_ms = nowMs();
        s.partitions.clear();
    }
    // De-dup: AddPartitionsToTxn can be called multiple times in one
    // txn for different partitions, or the same partition multiple times.
    for (const auto& p : partitions) {
        bool exists = false;
        for (const auto& existing : s.partitions) {
            if (existing == p) {
                exists = true;
                break;
            }
        }
        if (!exists) s.partitions.push_back(p);
    }
}

std::vector<std::pair<std::string, int32_t>>
TransactionCoordinator::commitTxn(const std::string& transactional_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = txns_.find(transactional_id);
    if (it == txns_.end()) return {};
    auto& s = it->second;
    // Phase 3.3: Ongoing → PrepareCommit → CompleteCommit. We collapse
    // the two-step prepare/complete because there's no replication to
    // coordinate (single-broker case). A multi-broker implementation
    // would persist PrepareCommit to __transaction_state, await quorum,
    // then transition to CompleteCommit.
    auto partitions = s.partitions;
    s.state = State::CompleteCommit;
    s.state_start_time_ms = nowMs();
    s.partitions.clear();
    commits_total_.fetch_add(1, std::memory_order_relaxed);
    return partitions;
}

std::vector<std::pair<std::string, int32_t>>
TransactionCoordinator::abortTxn(const std::string& transactional_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = txns_.find(transactional_id);
    if (it == txns_.end()) return {};
    auto& s = it->second;
    auto partitions = s.partitions;
    s.state = State::CompleteAbort;
    s.state_start_time_ms = nowMs();
    s.partitions.clear();
    // Phase EX-6: discard staged offsets — aborts MUST NOT make
    // consumer-group offset commits visible.
    s.pending_offsets.clear();
    aborts_total_.fetch_add(1, std::memory_order_relaxed);
    return partitions;
}

void TransactionCoordinator::stagePendingOffsets(
    const std::string& transactional_id,
    std::vector<PendingOffset> offsets) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = txns_.find(transactional_id);
    if (it == txns_.end()) return;
    auto& staged = it->second.pending_offsets;
    staged.reserve(staged.size() + offsets.size());
    for (auto& o : offsets) {
        staged.push_back(std::move(o));
    }
}

std::vector<TransactionCoordinator::PendingOffset>
TransactionCoordinator::drainPendingOffsets(
    const std::string& transactional_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = txns_.find(transactional_id);
    if (it == txns_.end()) return {};
    return std::exchange(it->second.pending_offsets, {});
}

TransactionCoordinator::Metrics TransactionCoordinator::getMetrics() const {
    Metrics m{};
    {
        std::lock_guard<std::mutex> lock(mutex_);
        for (const auto& [_, s] : txns_) {
            if (s.state == State::Ongoing) m.in_progress++;
        }
    }
    m.commits_total = commits_total_.load(std::memory_order_relaxed);
    m.aborts_total = aborts_total_.load(std::memory_order_relaxed);
    m.state_loads_total = state_loads_total_.load(std::memory_order_relaxed);
    return m;
}

std::vector<TransactionCoordinator::TxnSnapshot>
TransactionCoordinator::list(
    const std::vector<std::string>& state_filters,
    const std::vector<int64_t>& producer_id_filters) const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<TxnSnapshot> out;
    out.reserve(txns_.size());
    for (const auto& [_, s] : txns_) {
        if (!state_filters.empty()) {
            const std::string name = stateName(s.state);
            if (std::find(state_filters.begin(), state_filters.end(), name) ==
                state_filters.end()) {
                continue;
            }
        }
        if (!producer_id_filters.empty()) {
            if (std::find(producer_id_filters.begin(),
                          producer_id_filters.end(),
                          s.producer_id) == producer_id_filters.end()) {
                continue;
            }
        }
        out.push_back(s);
    }
    return out;
}

std::optional<TransactionCoordinator::TxnSnapshot>
TransactionCoordinator::describe(const std::string& transactional_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = txns_.find(transactional_id);
    if (it == txns_.end()) return std::nullopt;
    return it->second;
}

const char* TransactionCoordinator::stateName(State s) {
    switch (s) {
        case State::Empty: return "Empty";
        case State::Ongoing: return "Ongoing";
        case State::PrepareCommit: return "PrepareCommit";
        case State::PrepareAbort: return "PrepareAbort";
        case State::CompleteCommit: return "CompleteCommit";
        case State::CompleteAbort: return "CompleteAbort";
        case State::Dead: return "Dead";
        case State::PrepareEpochFence: return "PrepareEpochFence";
    }
    return "Unknown";
}

}  // namespace kawasan::broker
