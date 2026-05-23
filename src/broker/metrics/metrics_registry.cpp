#include "kawasan/broker/metrics/metrics_registry.h"

#include <spdlog/spdlog.h>

namespace kawasan::broker::metrics {

void MetricsRegistry::recordConsumerLag(const std::string& group_id, const std::string& topic,
                                        int32_t partition, int64_t lag) {
    std::lock_guard lock(mutex_);
    LagKey key{group_id, topic, partition};
    consumer_lags_[key] = lag;
    spdlog::trace("Recorded consumer lag: group={}, topic={}, partition={}, lag={}", group_id,
                  topic, partition, lag);
}

int64_t MetricsRegistry::getConsumerLag(const std::string& group_id, const std::string& topic,
                                        int32_t partition) const {
    std::lock_guard lock(mutex_);
    LagKey key{group_id, topic, partition};
    auto it = consumer_lags_.find(key);
    if (it != consumer_lags_.end()) {
        return it->second;
    }
    return -1;  // Not found
}

std::map<std::string, std::map<std::string, std::map<int32_t, int64_t>>>
MetricsRegistry::getAllConsumerLags() const {
    std::lock_guard lock(mutex_);
    std::map<std::string, std::map<std::string, std::map<int32_t, int64_t>>> result;

    for (const auto& [key, lag] : consumer_lags_) {
        result[key.group_id][key.topic][key.partition] = lag;
    }

    return result;
}

void MetricsRegistry::clearGroupMetrics(const std::string& group_id) {
    std::lock_guard lock(mutex_);
    for (auto it = consumer_lags_.begin(); it != consumer_lags_.end();) {
        if (it->first.group_id == group_id) {
            it = consumer_lags_.erase(it);
        } else {
            ++it;
        }
    }
    spdlog::debug("Cleared metrics for group: {}", group_id);
}

}  // namespace kawasan::broker::metrics
