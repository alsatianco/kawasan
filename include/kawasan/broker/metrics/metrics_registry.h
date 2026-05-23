#pragma once

#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace kawasan::broker::metrics {

/// @brief Registry for broker metrics including consumer lag, throughput, etc.
class MetricsRegistry {
public:
    MetricsRegistry() = default;
    ~MetricsRegistry() = default;

    MetricsRegistry(const MetricsRegistry&) = delete;
    MetricsRegistry& operator=(const MetricsRegistry&) = delete;
    MetricsRegistry(MetricsRegistry&&) = delete;
    MetricsRegistry& operator=(MetricsRegistry&&) = delete;

    /// @brief Stores consumer lag for a specific group, topic, and partition
    /// @param group_id Consumer group ID
    /// @param topic Topic name
    /// @param partition Partition ID
    /// @param lag Lag in number of messages
    void recordConsumerLag(const std::string& group_id, const std::string& topic,
                           int32_t partition, int64_t lag);

    /// @brief Gets consumer lag for a specific group, topic, and partition
    /// @param group_id Consumer group ID
    /// @param topic Topic name
    /// @param partition Partition ID
    /// @return Lag value, or -1 if not found
    int64_t getConsumerLag(const std::string& group_id, const std::string& topic,
                           int32_t partition) const;

    /// @brief Gets all consumer lag metrics
    /// @return Map of (group_id, topic, partition) -> lag
    std::map<std::string, std::map<std::string, std::map<int32_t, int64_t>>>
    getAllConsumerLags() const;

    /// @brief Clears metrics for a specific consumer group (e.g., when group is deleted)
    /// @param group_id Consumer group ID to clear
    void clearGroupMetrics(const std::string& group_id);

private:
    struct LagKey {
        std::string group_id;
        std::string topic;
        int32_t partition;

        bool operator<(const LagKey& other) const {
            if (group_id != other.group_id) return group_id < other.group_id;
            if (topic != other.topic) return topic < other.topic;
            return partition < other.partition;
        }
    };

    mutable std::mutex mutex_;
    std::map<LagKey, int64_t> consumer_lags_;
};

}  // namespace kawasan::broker::metrics
