#pragma once

#include <cstdint>
#include <mutex>
#include <unordered_map>

#include "kawasan/protocol/api_keys.h"

namespace kawasan::broker::metrics {

/// Tracks simple per-API request counters for observability.
class RequestMetrics {
public:
    struct Counters {
        uint64_t requests = 0;
        uint64_t bytes_in = 0;
        uint64_t bytes_out = 0;
    };

    void record(protocol::ApiKey api_key, uint64_t bytes_in, uint64_t bytes_out);
    Counters snapshot(protocol::ApiKey api_key) const;

private:
    int16_t keyToInt(protocol::ApiKey key) const {
        return static_cast<int16_t>(key);
    }

    mutable std::mutex mutex_;
    std::unordered_map<int16_t, Counters> counters_;
};

}  // namespace kawasan::broker::metrics

