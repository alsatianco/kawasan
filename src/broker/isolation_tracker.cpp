#include "kawasan/broker/isolation_tracker.h"

#include <algorithm>
#include <limits>
#include <map>

#include "kawasan/storage/log.h"

namespace kawasan::broker {

void IsolationTracker::recordInFlightTxn(int64_t producer_id, const std::string& topic,
                                         PartitionId partition, Offset current_log_end) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto& state = partitions_[PartitionKey{topic, partition}];
    // Idempotent: don't move the first_offset if this producer is
    // already tracked (multiple batches in one txn share first_offset).
    state.in_flight.emplace(producer_id, current_log_end);
}

void IsolationTracker::commitInFlightTxns(
    const std::vector<std::pair<std::string, int32_t>>& partitions, int64_t producer_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto& [topic, part] : partitions) {
        auto it = partitions_.find(PartitionKey{topic, part});
        if (it == partitions_.end())
            continue;
        it->second.in_flight.erase(producer_id);
    }
}

void IsolationTracker::abortInFlightTxns(
    const std::vector<std::pair<std::string, int32_t>>& partitions, int64_t producer_id,
    Offset last_offset) {
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto& [topic, part] : partitions) {
        auto it = partitions_.find(PartitionKey{topic, part});
        if (it == partitions_.end())
            continue;
        auto& state = it->second;
        auto pit = state.in_flight.find(producer_id);
        if (pit == state.in_flight.end())
            continue;
        // Move from in-flight to aborted ring.
        const Offset first_offset = pit->second;
        state.in_flight.erase(pit);
        const bool recovered =
            std::any_of(state.aborted.begin(), state.aborted.end(),
                        [producer_id, last_offset](const AbortedTxn& txn) {
                            return txn.producer_id == producer_id && txn.last_offset == last_offset;
                        });
        if (!recovered)
            state.aborted.push_back({producer_id, first_offset, last_offset});
    }
}

void IsolationTracker::recoverAbortedTransactions(const std::string& topic, PartitionId partition,
                                                  storage::Log& log) {
    std::map<std::pair<int64_t, int16_t>, Offset> starts;
    std::vector<AbortedTxn> aborted;
    const Offset end = log.logEndOffset();
    Offset offset = log.logStartOffset();
    while (offset < end) {
        const auto batches = log.read(offset, 4 * 1024 * 1024);
        if (batches.empty())
            break;
        Offset next = offset;
        for (const auto& batch : batches) {
            next = std::max(next, batch.baseOffset() + batch.lastOffsetDelta() + 1);
            const auto producer = std::make_pair(batch.producerId(), batch.producerEpoch());
            if (!batch.isControlBatch()) {
                if (batch.isTransactional())
                    starts.emplace(producer, batch.baseOffset());
                continue;
            }
            for (const auto& record : batch.records()) {
                if (!record.key || record.key->size() != 4)
                    continue;
                const auto& key = *record.key;
                if (key[0] != 0 || key[1] != 0 || key[2] != 0 || key[3] > 1)
                    continue;
                const auto start = starts.find(producer);
                if (key[3] == 0 && start != starts.end())
                    aborted.push_back({batch.producerId(), start->second, batch.baseOffset()});
                starts.erase(producer);
            }
        }
        if (next <= offset)
            break;
        offset = next;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    partitions_[PartitionKey{topic, partition}].aborted = std::move(aborted);
}

Offset IsolationTracker::lastStableOffset(const std::string& topic, PartitionId partition,
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

std::vector<IsolationTracker::AbortedTxn> IsolationTracker::abortedTransactions(
    const std::string& topic, PartitionId partition, Offset fetch_offset, Offset log_start_offset) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = partitions_.find(PartitionKey{topic, partition});
    if (it == partitions_.end())
        return {};
    std::erase_if(it->second.aborted, [log_start_offset](const AbortedTxn& txn) {
        return txn.last_offset < log_start_offset;
    });
    std::vector<AbortedTxn> out;
    out.reserve(it->second.aborted.size());
    for (const auto& a : it->second.aborted) {
        // A consumer may start in the middle of an aborted transaction.
        if (a.last_offset >= fetch_offset) {
            out.push_back(a);
        }
    }
    // Kafka returns these sorted by first_offset ascending.
    std::sort(out.begin(), out.end(), [](const AbortedTxn& a, const AbortedTxn& b) {
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
