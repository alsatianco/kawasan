#include "kawasan/broker/metrics/request_metrics.h"

#include "kawasan/common/logger.h"

namespace kawasan::broker::metrics {

void RequestMetrics::record(protocol::ApiKey api_key, uint64_t bytes_in,
                            uint64_t bytes_out) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto& entry = counters_[keyToInt(api_key)];
    entry.requests += 1;
    entry.bytes_in += bytes_in;
    entry.bytes_out += bytes_out;
    Logger::debug("API {}: requests={} bytes_in={} bytes_out={}",
                  static_cast<int16_t>(api_key), entry.requests, entry.bytes_in,
                  entry.bytes_out);
}

RequestMetrics::Counters RequestMetrics::snapshot(protocol::ApiKey api_key) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = counters_.find(keyToInt(api_key));
    if (it == counters_.end()) {
        return {};
    }
    return it->second;
}

}  // namespace kawasan::broker::metrics

