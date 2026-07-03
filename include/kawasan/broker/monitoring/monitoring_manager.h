#pragma once

#include <memory>
#include <string>

#include "kawasan/broker/monitoring/http_server.h"
#include "kawasan/broker/monitoring/metrics_collector.h"

namespace kawasan::broker::monitoring {

/// @brief Monitoring manager for health checks and metrics
class MonitoringManager {
public:
    MonitoringManager(const std::string& host, int port);
    ~MonitoringManager();

    // Non-copyable/movable
    MonitoringManager(const MonitoringManager&) = delete;
    MonitoringManager& operator=(const MonitoringManager&) = delete;
    MonitoringManager(MonitoringManager&&) = delete;
    MonitoringManager& operator=(MonitoringManager&&) = delete;

    /// @brief Start the monitoring HTTP server
    void start();

    /// @brief Stop the monitoring HTTP server
    void stop();

    /// @brief Get the metrics collector
    MetricsCollector* metricsCollector() { return metrics_collector_.get(); }

    /// @brief Shared handle for subsystems (e.g. TcpServer) that may outlive
    /// a single scrape but not the manager itself.
    std::shared_ptr<MetricsCollector> sharedMetricsCollector() { return metrics_collector_; }

    /// @brief Set broker health status
    void setBrokerHealthy(bool healthy) { broker_healthy_ = healthy; }

    /// @brief Set broker ready status
    void setBrokerReady(bool ready) { broker_ready_ = ready; }

private:
    HttpResponse handleHealth(const HttpRequest& request);
    HttpResponse handleReadiness(const HttpRequest& request);
    HttpResponse handleLiveness(const HttpRequest& request);
    HttpResponse handleMetrics(const HttpRequest& request);

    std::unique_ptr<HttpServer> http_server_;
    std::shared_ptr<MetricsCollector> metrics_collector_;

    bool broker_healthy_ = false;
    bool broker_ready_ = false;
};

}  // namespace kawasan::broker::monitoring
