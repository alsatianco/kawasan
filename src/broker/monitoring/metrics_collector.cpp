#include "kawasan/broker/monitoring/metrics_collector.h"

#include <iomanip>
#include <sstream>

namespace kawasan::broker::monitoring {

MetricsCollector::MetricsCollector() : start_time_(std::chrono::steady_clock::now()) {
    // Initialize histograms with standard latency buckets (in milliseconds)
    std::vector<double> latency_buckets = {1.0,   2.5,   5.0,   10.0,   25.0,   50.0,
                                           100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0};

    for (double bound : latency_buckets) {
        produce_latency_.buckets.emplace_back(bound);
        fetch_latency_.buckets.emplace_back(bound);
    }
    // Add +Inf bucket
    produce_latency_.buckets.emplace_back(std::numeric_limits<double>::infinity());
    fetch_latency_.buckets.emplace_back(std::numeric_limits<double>::infinity());
}

void MetricsCollector::incrementMessagesProduced(int64_t count) {
    messages_produced_.fetch_add(count, std::memory_order_relaxed);
}

void MetricsCollector::incrementMessagesConsumed(int64_t count) {
    messages_consumed_.fetch_add(count, std::memory_order_relaxed);
}

void MetricsCollector::incrementBytesIn(int64_t bytes) {
    bytes_in_.fetch_add(bytes, std::memory_order_relaxed);
}

void MetricsCollector::incrementBytesOut(int64_t bytes) {
    bytes_out_.fetch_add(bytes, std::memory_order_relaxed);
}

void MetricsCollector::incrementConnectionsCreated() {
    connections_created_.fetch_add(1, std::memory_order_relaxed);
}

void MetricsCollector::incrementConnectionClosed(ConnectionCloseReason reason) {
    switch (reason) {
        case ConnectionCloseReason::kNormal:
            connections_closed_normal_.fetch_add(1, std::memory_order_relaxed);
            break;
        case ConnectionCloseReason::kIdleTimeout:
            connections_closed_idle_timeout_.fetch_add(1, std::memory_order_relaxed);
            break;
        case ConnectionCloseReason::kProtocolError:
            connections_closed_protocol_error_.fetch_add(1, std::memory_order_relaxed);
            break;
        case ConnectionCloseReason::kIoError:
            connections_closed_io_error_.fetch_add(1, std::memory_order_relaxed);
            break;
    }
}

void MetricsCollector::incrementRequestsTotal(const std::string& api_key) {
    std::lock_guard<std::mutex> lock(api_metrics_mutex_);
    requests_total_[api_key].fetch_add(1, std::memory_order_relaxed);
}

void MetricsCollector::incrementRequestErrors(const std::string& api_key) {
    std::lock_guard<std::mutex> lock(api_metrics_mutex_);
    request_errors_[api_key].fetch_add(1, std::memory_order_relaxed);
}

void MetricsCollector::setActiveConnections(int64_t count) {
    active_connections_.store(count, std::memory_order_relaxed);
}

void MetricsCollector::setTopicCount(int64_t count) {
    topic_count_.store(count, std::memory_order_relaxed);
}

void MetricsCollector::setPartitionCount(int64_t count) {
    partition_count_.store(count, std::memory_order_relaxed);
}

void MetricsCollector::setConsumerGroupCount(int64_t count) {
    consumer_group_count_.store(count, std::memory_order_relaxed);
}

void MetricsCollector::setDiskUsageBytes(int64_t bytes) {
    disk_usage_bytes_.store(bytes, std::memory_order_relaxed);
}

void MetricsCollector::setMemoryUsageBytes(int64_t bytes) {
    memory_usage_bytes_.store(bytes, std::memory_order_relaxed);
}

void MetricsCollector::Histogram::observe(double value) {
    count.fetch_add(1, std::memory_order_relaxed);

    // Atomic update of sum (may have slight race conditions but acceptable for metrics)
    double current = sum.load(std::memory_order_relaxed);
    while (!sum.compare_exchange_weak(current, current + value, std::memory_order_relaxed)) {
        // Retry until successful
    }

    for (auto& bucket : buckets) {
        if (value <= bucket.upper_bound) {
            bucket.count.fetch_add(1, std::memory_order_relaxed);
        }
    }
}

void MetricsCollector::recordProduceLatency(double milliseconds) {
    produce_latency_.observe(milliseconds);
}

void MetricsCollector::recordFetchLatency(double milliseconds) {
    fetch_latency_.observe(milliseconds);
}

void MetricsCollector::recordRequestLatency(const std::string& api_key, double milliseconds) {
    std::lock_guard<std::mutex> lock(latency_mutex_);

    auto& hist = request_latency_[api_key];
    if (hist.buckets.empty()) {
        // Initialize histogram for this API key
        std::vector<double> latency_buckets = {1.0,   2.5,   5.0,   10.0,   25.0,   50.0,
                                               100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0};
        for (double bound : latency_buckets) {
            hist.buckets.emplace_back(bound);
        }
        hist.buckets.emplace_back(std::numeric_limits<double>::infinity());
    }

    hist.observe(milliseconds);
}

void MetricsCollector::setConsumerLag(const std::string& group, const std::string& topic,
                                      int32_t partition, int64_t lag) {
    std::lock_guard<std::mutex> lock(consumer_lag_mutex_);
    std::string key = group + "." + topic + "." + std::to_string(partition);
    consumer_lag_[key].store(lag, std::memory_order_relaxed);
}

std::unordered_map<std::string, int64_t> MetricsCollector::getConsumerLagMetrics() const {
    std::lock_guard<std::mutex> lock(consumer_lag_mutex_);
    std::unordered_map<std::string, int64_t> result;
    for (const auto& [key, value] : consumer_lag_) {
        result[key] = value.load(std::memory_order_relaxed);
    }
    return result;
}

std::string MetricsCollector::formatHistogram(const std::string& name, const std::string& help,
                                              const Histogram& hist,
                                              const std::string& labels) const {
    std::ostringstream oss;

    oss << "# HELP " << name << " " << help << "\n";
    oss << "# TYPE " << name << " histogram\n";

    std::string label_str = labels.empty() ? "" : "{" + labels + "}";

    for (const auto& bucket : hist.buckets) {
        oss << name << "_bucket{" << labels;
        if (!labels.empty())
            oss << ",";
        oss << "le=\"";
        if (std::isinf(bucket.upper_bound)) {
            oss << "+Inf";
        } else {
            oss << bucket.upper_bound;
        }
        oss << "\"} " << bucket.count.load(std::memory_order_relaxed) << "\n";
    }

    oss << name << "_sum" << label_str << " " << std::fixed << std::setprecision(3)
        << hist.sum.load(std::memory_order_relaxed) << "\n";
    oss << name << "_count" << label_str << " " << hist.count.load(std::memory_order_relaxed)
        << "\n";

    return oss.str();
}

std::string MetricsCollector::exportPrometheus() const {
    std::ostringstream oss;

    // Uptime
    auto now = std::chrono::steady_clock::now();
    auto uptime = std::chrono::duration_cast<std::chrono::seconds>(now - start_time_).count();
    oss << "# HELP kawasan_broker_uptime_seconds Broker uptime in seconds\n";
    oss << "# TYPE kawasan_broker_uptime_seconds gauge\n";
    oss << "kawasan_broker_uptime_seconds " << uptime << "\n\n";

    // Counter metrics
    oss << "# HELP kawasan_messages_produced_total Total number of messages produced\n";
    oss << "# TYPE kawasan_messages_produced_total counter\n";
    oss << "kawasan_messages_produced_total " << messages_produced_.load(std::memory_order_relaxed)
        << "\n\n";

    oss << "# HELP kawasan_messages_consumed_total Total number of messages consumed\n";
    oss << "# TYPE kawasan_messages_consumed_total counter\n";
    oss << "kawasan_messages_consumed_total " << messages_consumed_.load(std::memory_order_relaxed)
        << "\n\n";

    oss << "# HELP kawasan_bytes_in_total Total bytes received\n";
    oss << "# TYPE kawasan_bytes_in_total counter\n";
    oss << "kawasan_bytes_in_total " << bytes_in_.load(std::memory_order_relaxed) << "\n\n";

    oss << "# HELP kawasan_bytes_out_total Total bytes sent\n";
    oss << "# TYPE kawasan_bytes_out_total counter\n";
    oss << "kawasan_bytes_out_total " << bytes_out_.load(std::memory_order_relaxed) << "\n\n";

    // Gauge metrics
    oss << "# HELP kawasan_active_connections Current number of active connections\n";
    oss << "# TYPE kawasan_active_connections gauge\n";
    oss << "kawasan_active_connections " << active_connections_.load(std::memory_order_relaxed)
        << "\n\n";

    oss << "# HELP kawasan_connections_created_total Total client connections accepted\n";
    oss << "# TYPE kawasan_connections_created_total counter\n";
    oss << "kawasan_connections_created_total "
        << connections_created_.load(std::memory_order_relaxed) << "\n\n";

    oss << "# HELP kawasan_connections_closed_total Client connections closed, by reason\n";
    oss << "# TYPE kawasan_connections_closed_total counter\n";
    oss << "kawasan_connections_closed_total{reason=\"normal\"} "
        << connections_closed_normal_.load(std::memory_order_relaxed) << "\n";
    oss << "kawasan_connections_closed_total{reason=\"idle_timeout\"} "
        << connections_closed_idle_timeout_.load(std::memory_order_relaxed) << "\n";
    oss << "kawasan_connections_closed_total{reason=\"protocol_error\"} "
        << connections_closed_protocol_error_.load(std::memory_order_relaxed) << "\n";
    oss << "kawasan_connections_closed_total{reason=\"io_error\"} "
        << connections_closed_io_error_.load(std::memory_order_relaxed) << "\n\n";

    oss << "# HELP kawasan_topics Total number of topics\n";
    oss << "# TYPE kawasan_topics gauge\n";
    oss << "kawasan_topics " << topic_count_.load(std::memory_order_relaxed) << "\n\n";

    oss << "# HELP kawasan_partitions Total number of partitions\n";
    oss << "# TYPE kawasan_partitions gauge\n";
    oss << "kawasan_partitions " << partition_count_.load(std::memory_order_relaxed) << "\n\n";

    oss << "# HELP kawasan_consumer_groups Total number of consumer groups\n";
    oss << "# TYPE kawasan_consumer_groups gauge\n";
    oss << "kawasan_consumer_groups " << consumer_group_count_.load(std::memory_order_relaxed)
        << "\n\n";

    oss << "# HELP kawasan_disk_usage_bytes Disk usage in bytes\n";
    oss << "# TYPE kawasan_disk_usage_bytes gauge\n";
    oss << "kawasan_disk_usage_bytes " << disk_usage_bytes_.load(std::memory_order_relaxed)
        << "\n\n";

    oss << "# HELP kawasan_memory_usage_bytes Memory usage in bytes\n";
    oss << "# TYPE kawasan_memory_usage_bytes gauge\n";
    oss << "kawasan_memory_usage_bytes " << memory_usage_bytes_.load(std::memory_order_relaxed)
        << "\n\n";

    // Request metrics by API
    {
        std::lock_guard<std::mutex> lock(api_metrics_mutex_);

        if (!requests_total_.empty()) {
            oss << "# HELP kawasan_requests_total Total number of requests by API\n";
            oss << "# TYPE kawasan_requests_total counter\n";
            for (const auto& [api, count] : requests_total_) {
                oss << "kawasan_requests_total{api=\"" << api << "\"} "
                    << count.load(std::memory_order_relaxed) << "\n";
            }
            oss << "\n";
        }

        if (!request_errors_.empty()) {
            oss << "# HELP kawasan_request_errors_total Total number of request errors by API\n";
            oss << "# TYPE kawasan_request_errors_total counter\n";
            for (const auto& [api, count] : request_errors_) {
                oss << "kawasan_request_errors_total{api=\"" << api << "\"} "
                    << count.load(std::memory_order_relaxed) << "\n";
            }
            oss << "\n";
        }
    }

    // Latency histograms
    oss << formatHistogram("kawasan_produce_latency_ms", "Produce request latency in milliseconds",
                           produce_latency_);
    oss << "\n";

    oss << formatHistogram("kawasan_fetch_latency_ms", "Fetch request latency in milliseconds",
                           fetch_latency_);
    oss << "\n";

    // Per-API latency histograms
    {
        std::lock_guard<std::mutex> lock(latency_mutex_);
        for (const auto& [api, hist] : request_latency_) {
            oss << formatHistogram("kawasan_request_latency_ms",
                                   "Request latency in milliseconds by API", hist,
                                   "api=\"" + api + "\"");
            oss << "\n";
        }
    }

    // Consumer lag metrics
    {
        std::lock_guard<std::mutex> lock(consumer_lag_mutex_);
        if (!consumer_lag_.empty()) {
            oss << "# HELP kawasan_consumer_lag Consumer group lag (log end offset - committed "
                   "offset)\n";
            oss << "# TYPE kawasan_consumer_lag gauge\n";
            for (const auto& [key, lag] : consumer_lag_) {
                // Parse key: group.topic.partition
                size_t first_dot = key.find('.');
                size_t last_dot = key.rfind('.');
                if (first_dot != std::string::npos && last_dot != std::string::npos &&
                    first_dot != last_dot) {
                    std::string group = key.substr(0, first_dot);
                    std::string topic = key.substr(first_dot + 1, last_dot - first_dot - 1);
                    std::string partition = key.substr(last_dot + 1);

                    oss << "kawasan_consumer_lag{group=\"" << group << "\",topic=\"" << topic
                        << "\",partition=\"" << partition << "\"} "
                        << lag.load(std::memory_order_relaxed) << "\n";
                }
            }
            oss << "\n";
        }
    }

    // Phase EX-1 (§6.3): per-subsystem metrics block. Each provider
    // returns a pre-formatted Prometheus text fragment so MetricsCollector
    // doesn't have to know about each subsystem's internal types.
    if (producer_state_provider_)
        oss << producer_state_provider_();
    if (fetch_session_provider_)
        oss << fetch_session_provider_();
    if (transaction_provider_)
        oss << transaction_provider_();
    if (group_provider_)
        oss << group_provider_();
    if (log_cleaner_provider_)
        oss << log_cleaner_provider_();

    return oss.str();
}

}  // namespace kawasan::broker::monitoring
