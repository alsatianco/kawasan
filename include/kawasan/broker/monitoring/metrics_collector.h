#pragma once

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace kawasan::broker::monitoring {

/// @brief Metrics collector for Prometheus exposition
class MetricsCollector {
public:
    MetricsCollector();
    ~MetricsCollector() = default;

    // Counter metrics
    void incrementMessagesProduced(int64_t count = 1);
    void incrementMessagesConsumed(int64_t count = 1);
    void incrementBytesIn(int64_t bytes);
    void incrementBytesOut(int64_t bytes);
    void incrementRequestsTotal(const std::string& api_key);
    void incrementRequestErrors(const std::string& api_key);

    // Gauge metrics
    void setActiveConnections(int64_t count);
    void setTopicCount(int64_t count);
    void setPartitionCount(int64_t count);
    void setConsumerGroupCount(int64_t count);
    void setDiskUsageBytes(int64_t bytes);
    void setMemoryUsageBytes(int64_t bytes);

    // Histogram metrics (latency tracking)
    void recordProduceLatency(double milliseconds);
    void recordFetchLatency(double milliseconds);
    void recordRequestLatency(const std::string& api_key, double milliseconds);

    // Consumer lag metrics
    void setConsumerLag(const std::string& group, const std::string& topic, 
                       int32_t partition, int64_t lag);
    std::unordered_map<std::string, int64_t> getConsumerLagMetrics() const;

    /// @brief Export metrics in Prometheus text format
    std::string exportPrometheus() const;

    // Phase EX-1 (§6.3): subsystem providers. The MetricsCollector
    // doesn't own these — it holds raw pointers and queries them on
    // each /metrics scrape. Pointers must outlive the MetricsCollector.
    using ProducerStateProvider = std::function<std::string()>;
    using FetchSessionProvider = std::function<std::string()>;
    using TransactionProvider = std::function<std::string()>;
    using GroupProvider = std::function<std::string()>;
    using LogCleanerProvider = std::function<std::string()>;
    void setProducerStateProvider(ProducerStateProvider p) { producer_state_provider_ = std::move(p); }
    void setFetchSessionProvider(FetchSessionProvider p) { fetch_session_provider_ = std::move(p); }
    void setTransactionProvider(TransactionProvider p) { transaction_provider_ = std::move(p); }
    void setGroupProvider(GroupProvider p) { group_provider_ = std::move(p); }
    void setLogCleanerProvider(LogCleanerProvider p) { log_cleaner_provider_ = std::move(p); }

private:
    struct HistogramBucket {
        std::atomic<uint64_t> count{0};
        double upper_bound;
        
        HistogramBucket(double ub) : count(0), upper_bound(ub) {}
        HistogramBucket(const HistogramBucket& other) 
            : count(other.count.load()), upper_bound(other.upper_bound) {}
        HistogramBucket& operator=(const HistogramBucket&) = delete;
    };

    struct Histogram {
        std::vector<HistogramBucket> buckets;
        std::atomic<uint64_t> count{0};
        std::atomic<double> sum{0.0};

        void observe(double value);
    };

    // Counter metrics
    std::atomic<int64_t> messages_produced_{0};
    std::atomic<int64_t> messages_consumed_{0};
    std::atomic<int64_t> bytes_in_{0};
    std::atomic<int64_t> bytes_out_{0};
    
    // Gauge metrics
    std::atomic<int64_t> active_connections_{0};
    std::atomic<int64_t> topic_count_{0};
    std::atomic<int64_t> partition_count_{0};
    std::atomic<int64_t> consumer_group_count_{0};
    std::atomic<int64_t> disk_usage_bytes_{0};
    std::atomic<int64_t> memory_usage_bytes_{0};

    // Request metrics by API key
    mutable std::mutex api_metrics_mutex_;
    std::unordered_map<std::string, std::atomic<int64_t>> requests_total_;
    std::unordered_map<std::string, std::atomic<int64_t>> request_errors_;

    // Latency histograms
    Histogram produce_latency_;
    Histogram fetch_latency_;
    mutable std::mutex latency_mutex_;
    std::unordered_map<std::string, Histogram> request_latency_;

    // Consumer lag metrics (group.topic.partition -> lag)
    mutable std::mutex consumer_lag_mutex_;
    std::unordered_map<std::string, std::atomic<int64_t>> consumer_lag_;

    std::chrono::steady_clock::time_point start_time_;

    // Phase EX-1 (§6.3): pluggable providers for per-subsystem metrics.
    ProducerStateProvider producer_state_provider_;
    FetchSessionProvider fetch_session_provider_;
    TransactionProvider transaction_provider_;
    GroupProvider group_provider_;
    LogCleanerProvider log_cleaner_provider_;

    std::string formatHistogram(const std::string& name, const std::string& help,
                               const Histogram& hist, const std::string& labels = "") const;
};

}  // namespace kawasan::broker::monitoring
