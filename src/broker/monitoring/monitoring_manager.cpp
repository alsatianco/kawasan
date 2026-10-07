#include "kawasan/broker/monitoring/monitoring_manager.h"

#include <nlohmann/json.hpp>

#include "kawasan/common/logger.h"

using json = nlohmann::json;

namespace kawasan::broker::monitoring {

MonitoringManager::MonitoringManager(const std::string& host, int port)
    : http_server_(std::make_unique<HttpServer>(host, port)),
      metrics_collector_(std::make_shared<MetricsCollector>()) {
    
    // Register HTTP handlers
    http_server_->registerHandler("/health", 
        [this](const HttpRequest& req) { return handleHealth(req); });
    http_server_->registerHandler("/healthz", 
        [this](const HttpRequest& req) { return handleHealth(req); });
    http_server_->registerHandler("/readiness", 
        [this](const HttpRequest& req) { return handleReadiness(req); });
    http_server_->registerHandler("/ready", 
        [this](const HttpRequest& req) { return handleReadiness(req); });
    http_server_->registerHandler("/liveness", 
        [this](const HttpRequest& req) { return handleLiveness(req); });
    http_server_->registerHandler("/live", 
        [this](const HttpRequest& req) { return handleLiveness(req); });
    http_server_->registerHandler("/metrics", 
        [this](const HttpRequest& req) { return handleMetrics(req); });
}

MonitoringManager::~MonitoringManager() {
    stop();
}

void MonitoringManager::start() {
    Logger::info("Starting monitoring manager");
    http_server_->start();
}

void MonitoringManager::stop() {
    Logger::info("Stopping monitoring manager");
    http_server_->stop();
}

HttpResponse MonitoringManager::handleHealth(const HttpRequest& /*request*/) {
    HttpResponse response;
    response.content_type = "application/json";

    const bool healthy = broker_healthy_.load();
    const bool ready = broker_ready_.load();

    json health_status = {
        {"status", healthy && ready ? "UP" : "DOWN"},
        {"healthy", healthy},
        {"ready", ready}
    };

    response.body = health_status.dump(2);
    response.status_code = (healthy && ready) ? 200 : 503;
    response.status_text = (healthy && ready) ? "OK" : "Service Unavailable";

    return response;
}

HttpResponse MonitoringManager::handleReadiness(const HttpRequest& /*request*/) {
    HttpResponse response;
    response.content_type = "application/json";

    const bool ready = broker_ready_.load();

    json readiness_status = {
        {"status", ready ? "READY" : "NOT_READY"},
        {"ready", ready}
    };

    response.body = readiness_status.dump(2);
    response.status_code = ready ? 200 : 503;
    response.status_text = ready ? "OK" : "Service Unavailable";

    return response;
}

HttpResponse MonitoringManager::handleLiveness(const HttpRequest& /*request*/) {
    HttpResponse response;
    response.content_type = "application/json";

    const bool healthy = broker_healthy_.load();

    json liveness_status = {
        {"status", healthy ? "ALIVE" : "DEAD"},
        {"alive", healthy}
    };

    response.body = liveness_status.dump(2);
    response.status_code = healthy ? 200 : 503;
    response.status_text = healthy ? "OK" : "Service Unavailable";

    return response;
}

HttpResponse MonitoringManager::handleMetrics(const HttpRequest& /*request*/) {
    HttpResponse response;
    response.content_type = "text/plain; version=0.0.4";
    response.body = metrics_collector_->exportPrometheus();
    response.status_code = 200;
    response.status_text = "OK";

    return response;
}

}  // namespace kawasan::broker::monitoring
