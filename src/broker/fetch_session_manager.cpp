#include "kawasan/broker/fetch_session_manager.h"

namespace kawasan::broker {

namespace {
constexpr int32_t kInvalidSessionId = 0;
constexpr int32_t kFinalEpoch = -1;
constexpr int16_t kInvalidFetchSessionIdError = 70;
constexpr int16_t kInvalidFetchSessionEpochError = 71;
}  // namespace

FetchSessionManager::ValidationResult
FetchSessionManager::validate(int32_t requested_session_id,
                              int32_t requested_session_epoch) {
    ValidationResult r{};
    r.error = 0;
    std::lock_guard<std::mutex> lock(mutex_);

    if (requested_session_id == kInvalidSessionId &&
        requested_session_epoch == kInvalidSessionId) {
        // No session in use — single, "sessionless" fetch. Return
        // session_id = 0 so the client knows we're not tracking.
        r.session_id = 0;
        r.session_epoch = 0;
        r.is_new_session = false;
        r.is_full_fetch = true;
        return r;
    }

    if (requested_session_id == kInvalidSessionId) {
        // Client requests a new session (session_id=0, session_epoch=0
        // is technically the same case as sessionless, but some clients
        // use epoch=0 with id=0 to request session establishment; we
        // allocate one).
        Session s;
        s.session_id = next_session_id_.fetch_add(1);
        s.session_epoch = 1;
        s.last_used = std::chrono::steady_clock::now();
        sessions_[s.session_id] = s;
        r.session_id = s.session_id;
        r.session_epoch = s.session_epoch;
        r.is_new_session = true;
        r.is_full_fetch = true;
        return r;
    }

    // Existing session_id provided — validate.
    auto it = sessions_.find(requested_session_id);
    if (it == sessions_.end()) {
        r.is_error = true;
        r.error = kInvalidFetchSessionIdError;
        return r;
    }

    if (requested_session_epoch == kFinalEpoch) {
        // Client signals session close.
        sessions_.erase(it);
        r.session_id = requested_session_id;
        r.session_epoch = kFinalEpoch;
        r.is_full_fetch = true;
        return r;
    }

    if (requested_session_epoch != it->second.session_epoch) {
        r.is_error = true;
        r.error = kInvalidFetchSessionEpochError;
        return r;
    }

    // Valid session reference. Bump epoch and update last_used.
    it->second.session_epoch++;
    it->second.last_used = std::chrono::steady_clock::now();
    r.session_id = it->second.session_id;
    r.session_epoch = it->second.session_epoch;
    r.is_full_fetch = true;  // Until we implement incremental
    return r;
}

void FetchSessionManager::recordFetch(
    int32_t session_id,
    const std::set<std::pair<std::string, int32_t>>& partitions) {
    if (session_id == kInvalidSessionId) return;
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = sessions_.find(session_id);
    if (it == sessions_.end()) return;
    it->second.partitions = partitions;
    it->second.last_used = std::chrono::steady_clock::now();
}

void FetchSessionManager::recordPartitionOffset(
    int32_t session_id, const std::string& topic, int32_t partition,
    int64_t last_offset) {
    if (session_id == kInvalidSessionId) return;
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = sessions_.find(session_id);
    if (it == sessions_.end()) return;
    const std::string key = topic + ":" + std::to_string(partition);
    it->second.last_seen_offset[key] = last_offset;
    it->second.last_used = std::chrono::steady_clock::now();
}

int64_t FetchSessionManager::lastSeenOffset(
    int32_t session_id, const std::string& topic, int32_t partition) const {
    if (session_id == kInvalidSessionId) return -1;
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = sessions_.find(session_id);
    if (it == sessions_.end()) return -1;
    const std::string key = topic + ":" + std::to_string(partition);
    auto oit = it->second.last_seen_offset.find(key);
    if (oit == it->second.last_seen_offset.end()) return -1;
    return oit->second;
}

size_t FetchSessionManager::evictIdle(int64_t max_idle_ms) {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto now = std::chrono::steady_clock::now();
    size_t evicted = 0;
    for (auto it = sessions_.begin(); it != sessions_.end();) {
        const auto idle = std::chrono::duration_cast<std::chrono::milliseconds>(
                              now - it->second.last_used)
                              .count();
        if (idle > max_idle_ms) {
            it = sessions_.erase(it);
            ++evicted;
        } else {
            ++it;
        }
    }
    if (evicted > 0) {
        evictions_total_.fetch_add(static_cast<int64_t>(evicted),
                                   std::memory_order_relaxed);
    }
    return evicted;
}

size_t FetchSessionManager::size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return sessions_.size();
}

void FetchSessionManager::recordHitOrMiss(bool hit) {
    if (hit) {
        incremental_hits_total_.fetch_add(1, std::memory_order_relaxed);
    } else {
        incremental_misses_total_.fetch_add(1, std::memory_order_relaxed);
    }
}

FetchSessionManager::Metrics FetchSessionManager::getMetrics() const {
    Metrics m{};
    {
        std::lock_guard<std::mutex> lock(mutex_);
        m.session_count = static_cast<int64_t>(sessions_.size());
    }
    m.evictions_total = evictions_total_.load(std::memory_order_relaxed);
    m.incremental_hits_total = incremental_hits_total_.load(std::memory_order_relaxed);
    m.incremental_misses_total = incremental_misses_total_.load(std::memory_order_relaxed);
    const int64_t denom = m.incremental_hits_total + m.incremental_misses_total;
    m.incremental_hit_ratio =
        (denom > 0) ? (static_cast<double>(m.incremental_hits_total) / denom) : 0.0;
    return m;
}

}  // namespace kawasan::broker
