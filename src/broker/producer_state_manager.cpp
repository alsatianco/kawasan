#include "kawasan/broker/producer_state_manager.h"

#include <climits>

namespace kawasan::broker {

namespace {

// Phase 2.1: Kafka sequence numbers wrap at INT32_MAX. The "next expected"
// after INT32_MAX is 0. We compute (last + 1) mod (INT32_MAX + 1) to get
// the next expected, then check equality. A real client should never get
// here in practice (INT32_MAX ≈ 2.1 B batches), but the math is the same
// as Kafka's `RecordBatch.incrementSequence`.
int32_t incrementSequence(int32_t seq) {
    if (seq == INT32_MAX)
        return 0;
    return seq + 1;
}

}  // namespace

ProducerStateManager::CheckResult ProducerStateManager::check(
    const std::string& topic, PartitionId partition, int64_t producer_id, int16_t producer_epoch,
    int32_t base_sequence, int32_t record_count) const {
    CheckResult result;

    // Non-idempotent producer: nothing to track. Accept.
    if (producer_id < 0)
        return result;

    std::lock_guard<std::mutex> lock(mutex_);
    Key key{topic, partition, producer_id};
    auto it = states_.find(key);
    if (it == states_.end()) {
        // First batch seen from this producer. Accept any base_sequence
        // that's non-negative; producers typically start at 0 but clients
        // may "warm up" with a non-zero sequence (e.g. after fencing).
        return result;
    }
    const State& s = it->second;

    if (producer_epoch < s.last_epoch) {
        // Fenced by a newer producer instance.
        result.error = ErrorCode::INVALID_PRODUCER_EPOCH;
        return result;
    }
    if (producer_epoch > s.last_epoch) {
        // Newer epoch — the producer was re-initialized. Sequence
        // tracking restarts; accept this batch.
        return result;
    }

    // Same epoch: check sequence against last seen.
    if (base_sequence == s.last_base_sequence) {
        // Exact retry of the most recent batch. Duplicate — return the
        // original offset so the client treats this as a successful
        // replay.
        result.error = ErrorCode::DUPLICATE_SEQUENCE_NUMBER;
        result.duplicate_offset = s.last_base_offset;
        return result;
    }
    if (base_sequence <= s.last_sequence) {
        // Older sequence range — also a duplicate (client retried an
        // even-older batch). We don't have its offset; report the
        // duplicate-only error.
        result.error = ErrorCode::DUPLICATE_SEQUENCE_NUMBER;
        result.duplicate_offset = -1;
        return result;
    }

    const int32_t expected_next = incrementSequence(s.last_sequence);
    if (base_sequence != expected_next) {
        // Producer skipped sequences — refuse.
        result.error = ErrorCode::OUT_OF_ORDER_SEQUENCE_NUMBER;
        return result;
    }

    (void)record_count;
    return result;
}

void ProducerStateManager::recordAppend(const std::string& topic, PartitionId partition,
                                        int64_t producer_id, int16_t producer_epoch,
                                        int32_t base_sequence, int32_t record_count,
                                        Offset base_offset) {
    if (producer_id < 0 || record_count <= 0)
        return;
    std::lock_guard<std::mutex> lock(mutex_);
    Key key{topic, partition, producer_id};
    State& s = states_[key];
    s.last_epoch = producer_epoch;
    s.last_base_sequence = base_sequence;
    s.last_record_count = record_count;
    s.last_sequence = base_sequence + (record_count - 1);
    s.last_base_offset = base_offset;
}

std::vector<ProducerStateManager::ActiveProducer> ProducerStateManager::listProducers(
    const std::string& topic, PartitionId partition) const {
    std::vector<ActiveProducer> out;
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto& [key, state] : states_) {
        if (key.topic == topic && key.partition == partition) {
            out.push_back(
                {key.producer_id, state.last_epoch, state.last_sequence, state.last_base_offset});
        }
    }
    return out;
}

std::vector<ProducerStateManager::SnapshotEntry> ProducerStateManager::snapshotEntries(
    const std::string& topic, PartitionId partition) const {
    std::vector<SnapshotEntry> out;
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto& [key, state] : states_) {
        if (key.topic == topic && key.partition == partition) {
            out.push_back({key.producer_id, state.last_epoch, state.last_sequence,
                           state.last_base_sequence, state.last_record_count,
                           state.last_base_offset});
        }
    }
    return out;
}

void ProducerStateManager::restoreEntries(const std::string& topic, PartitionId partition,
                                          const std::vector<SnapshotEntry>& entries) {
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto& e : entries) {
        State s;
        s.last_epoch = e.last_epoch;
        s.last_sequence = e.last_sequence;
        s.last_base_sequence = e.last_base_sequence;
        s.last_record_count = e.last_record_count;
        s.last_base_offset = e.last_base_offset;
        states_[Key{topic, partition, e.producer_id}] = s;
    }
}

void ProducerStateManager::clearPartition(const std::string& topic, PartitionId partition) {
    std::lock_guard<std::mutex> lock(mutex_);
    std::erase_if(states_, [&](const auto& entry) {
        return entry.first.topic == topic && entry.first.partition == partition;
    });
}

void ProducerStateManager::clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    evictions_total_.fetch_add(static_cast<int64_t>(states_.size()), std::memory_order_relaxed);
    states_.clear();
}

ProducerStateManager::Metrics ProducerStateManager::getMetrics() const {
    Metrics m{};
    std::lock_guard<std::mutex> lock(mutex_);
    m.entries = static_cast<int64_t>(states_.size());
    m.evictions_total = evictions_total_.load(std::memory_order_relaxed);
    // Count distinct producer_ids — a single pid may appear under
    // multiple (topic, partition) keys; that's the metric the doc
    // mandates so we deduplicate.
    std::unordered_map<int64_t, char> seen;
    seen.reserve(states_.size());
    for (const auto& [key, _] : states_) {
        seen[key.producer_id] = 1;
    }
    m.producer_id_count = static_cast<int64_t>(seen.size());
    return m;
}

}  // namespace kawasan::broker
