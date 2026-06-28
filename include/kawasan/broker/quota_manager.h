#pragma once

#include <chrono>
#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>

namespace kawasan::broker {

/// @brief Per-client byte-rate quotas (Phase A6). Tracks produce/consume byte
/// rates per client-id and returns a throttle delay (ms) when a client exceeds
/// its configured rate, which the broker echoes in the response's
/// throttle_time_ms so well-behaved clients back off — preventing a single
/// noisy client from starving the broker.
///
/// Quotas are disabled (unlimited) when the configured bytes/sec is <= 0, which
/// is the default, so this is a no-op unless an operator opts in via
/// quota.producer.default / quota.consumer.default.
///
/// The algorithm is a simple fixed 1-second window: throttle =
/// (bytes_in_window - quota) / quota * 1000ms, capped. It is intentionally
/// modest — enough to make clients back off — not a full reproduction of
/// Kafka's multi-sample sliding window.
class QuotaManager {
public:
    enum class Type { kProducer, kConsumer };

    QuotaManager(int64_t producer_bytes_per_sec, int64_t consumer_bytes_per_sec);

    /// @brief Records `bytes` against the client's quota of the given type and
    /// returns the throttle delay in milliseconds (0 if under quota or the quota
    /// is disabled). Thread-safe.
    int32_t recordAndThrottleMs(Type type, const std::string& client_id, size_t bytes);

    bool producerQuotaEnabled() const { return producer_bytes_per_sec_ > 0; }
    bool consumerQuotaEnabled() const { return consumer_bytes_per_sec_ > 0; }

private:
    struct Window {
        std::chrono::steady_clock::time_point start{};
        int64_t bytes = 0;
    };

    int32_t checkLocked(std::unordered_map<std::string, Window>& windows, int64_t quota,
                        const std::string& client_id, size_t bytes);

    int64_t producer_bytes_per_sec_;
    int64_t consumer_bytes_per_sec_;
    std::mutex mutex_;
    std::unordered_map<std::string, Window> producer_windows_;
    std::unordered_map<std::string, Window> consumer_windows_;
};

}  // namespace kawasan::broker
