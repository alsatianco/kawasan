#include "kawasan/broker/quota_manager.h"

#include <algorithm>

namespace kawasan::broker {

namespace {
constexpr int32_t kMaxThrottleMs = 30000;  // cap a single throttle at 30s
constexpr auto kWindow = std::chrono::seconds(1);
}  // namespace

QuotaManager::QuotaManager(int64_t producer_bytes_per_sec, int64_t consumer_bytes_per_sec)
    : producer_bytes_per_sec_(producer_bytes_per_sec),
      consumer_bytes_per_sec_(consumer_bytes_per_sec) {}

int32_t QuotaManager::recordAndThrottleMs(Type type, const std::string& client_id,
                                          size_t bytes) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (type == Type::kProducer) {
        return checkLocked(producer_windows_, producer_bytes_per_sec_, client_id, bytes);
    }
    return checkLocked(consumer_windows_, consumer_bytes_per_sec_, client_id, bytes);
}

int32_t QuotaManager::checkLocked(std::unordered_map<std::string, Window>& windows,
                                  int64_t quota, const std::string& client_id,
                                  size_t bytes) {
    if (quota <= 0) {
        return 0;  // quota disabled (unlimited)
    }
    const auto now = std::chrono::steady_clock::now();
    Window& w = windows[client_id];
    if (w.start.time_since_epoch().count() == 0 || (now - w.start) >= kWindow) {
        w.start = now;
        w.bytes = 0;
    }
    w.bytes += static_cast<int64_t>(bytes);
    if (w.bytes <= quota) {
        return 0;
    }
    // Over quota: delay proportional to the overage so the average rate
    // converges to the quota.
    const int64_t over = w.bytes - quota;
    const int64_t throttle = (over * 1000) / quota;
    return static_cast<int32_t>(std::min<int64_t>(throttle, kMaxThrottleMs));
}

}  // namespace kawasan::broker
