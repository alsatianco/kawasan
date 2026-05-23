#include "kawasan/broker/isolation_tracker.h"

#include <algorithm>
#include <limits>

namespace kawasan::broker {

void IsolationTracker::recordInFlightTxn(int64_t producer_id,
                                         const std::string& topic,
                                         PartitionId partition,
                                         Offset current_log_end) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto& state = partitions_[PartitionKey{topic, partition}];
    // Idempotent: don't move the first_offset if this producer is
    // already tracked (multiple batches in one txn share first_offset).
    state.in_flight.emplace(producer_id, current_log_end);
}

void IsolationTracker::commitInFlightTxns(
    const std::vector<std::pair<std::string, int32_t>>& partitions,
    int64_t producer_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto& [topic, part] : partitions) {
        auto it = partitions_.find(PartitionKey{topic, part});
        if (it == partitions_.end()) continue;
        it->second.in_flight.erase(producer_id);
    }
}

void IsolationTracker::abortInFlightTxns(
    const std::vector<std::pair<std::string, int32_t>>& partitions,
    int64_t producer_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto& [topic, part] : partitions) {
        auto it = partitions_.find(PartitionKey{topic, part});
        if (it == partitions_.end()) continue;
        auto& state = it->second;
        auto pit = state.in_flight.find(producer_id);
        if (pit == state.in_flight.end()) continue;
        // Move from in-flight to aborted ring.
        const Offset first_offset = pit->second;
        state.in_flight.erase(pit);
        state.aborted.push_back({producer_id, first_offset});
        while (state.aborted.size() > max_aborted_per_partition_) {
            state.aborted.pop_front();
        }
    }
}

Offset IsolationTracker::lastStableOffset(const std::string& topic,
                                          PartitionId partition,
                                          Offset high_watermark) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = partitions_.find(PartitionKey{topic, partition});
    if (it == partitions_.end() || it->second.in_flight.empty()) {
        return high_watermark;
    }
    Offset lso = high_watermark;
    for (const auto& [_, first_offset] : it->second.in_flight) {
        lso = std::min(lso, first_offset);
    }
    return lso;
}

std::vector<IsolationTracker::AbortedTxn>
IsolationTracker::abortedTransactions(const std::string& topic,
                                      PartitionId partition,
                                      Offset fetch_offset) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = partitions_.find(PartitionKey{topic, partition});
    if (it == partitions_.end()) return {};
    std::vector<AbortedTxn> out;
    out.reserve(it->second.aborted.size());
    for (const auto& a : it->second.aborted) {
        // Only include aborts whose records could still be in the fetch
        // window (first_offset >= fetch_offset means the consumer
        // hasn't read past them yet).
        if (a.first_offset >= fetch_offset) {
            out.push_back(a);
        }
    }
    // Kafka returns these sorted by first_offset ascending.
    std::sort(out.begin(), out.end(),
              [](const AbortedTxn& a, const AbortedTxn& b) {
                  return a.first_offset < b.first_offset;
              });
    return out;
}

size_t IsolationTracker::inFlightCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    size_t n = 0;
    for (const auto& [_, state] : partitions_) {
        n += state.in_flight.size();
    }
    return n;
}

}  // namespace kawasan::broker
