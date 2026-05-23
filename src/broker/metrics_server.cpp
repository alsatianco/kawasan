#include "metrics_server.h"
#include "metrics_registry.h"
#include "broker.h"
#include "group_coordinator.h"
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <sstream>
#include <iostream>
#include <cstring>

namespace kawasan {

MetricsServer::MetricsServer(int port, std::shared_ptr<MetricsRegistry> registry, Broker* broker)
    : port_(port), registry_(registry), broker_(broker), running_(false), server_fd_(-1) {
}

MetricsServer::~MetricsServer() {
    stop();
}

void MetricsServer::start() {
    if (running_) {
        return;
    }

    // Create socket
    server_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd_ < 0) {
        std::cerr << "Failed to create metrics server socket" << std::endl;
        return;
    }

    // Set socket options
    int opt = 1;
    if (setsockopt(server_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        std::cerr << "Failed to set socket options" << std::endl;
        close(server_fd_);
        return;
    }

    // Bind socket
    struct sockaddr_in address;
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(port_);

    if (bind(server_fd_, (struct sockaddr*)&address, sizeof(address)) < 0) {
        std::cerr << "Failed to bind metrics server to port " << port_ << std::endl;
        close(server_fd_);
        return;
    }

    // Listen
    if (listen(server_fd_, 10) < 0) {
        std::cerr << "Failed to listen on metrics server socket" << std::endl;
        close(server_fd_);
        return;
    }

    running_ = true;
    server_thread_ = std::thread(&MetricsServer::run, this);
    
    std::cout << "Metrics server started on port " << port_ << std::endl;
}

void MetricsServer::stop() {
    if (!running_) {
        return;
    }

    running_ = false;
    
    if (server_fd_ >= 0) {
        close(server_fd_);
        server_fd_ = -1;
    }

    if (server_thread_.joinable()) {
        server_thread_.join();
    }
    
    std::cout << "Metrics server stopped" << std::endl;
}

void MetricsServer::run() {
    while (running_) {
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);
        
        int client_fd = accept(server_fd_, (struct sockaddr*)&client_addr, &client_len);
        if (client_fd < 0) {
            if (running_) {
                std::cerr << "Failed to accept connection on metrics server" << std::endl;
            }
            continue;
        }

        handleRequest(client_fd);
        close(client_fd);
    }
}

void MetricsServer::handleRequest(int client_fd) {
    // Read HTTP request (simplified - just read and ignore)
    char buffer[4096];
    ssize_t bytes_read = read(client_fd, buffer, sizeof(buffer) - 1);
    if (bytes_read <= 0) {
        return;
    }
    buffer[bytes_read] = '\0';

    // Check if it's a GET /metrics request
    std::string request(buffer);
    if (request.find("GET /metrics") == std::string::npos) {
        // Return 404
        std::string response = "HTTP/1.1 404 Not Found\r\n"
                               "Content-Length: 0\r\n"
                               "\r\n";
        write(client_fd, response.c_str(), response.length());
        return;
    }

    // Compute consumer lag metrics before generating response
    if (broker_ && broker_->getGroupCoordinator()) {
        broker_->getGroupCoordinator()->computeAndRecordConsumerLag();
    }

    // Generate metrics JSON
    std::string metrics_json = generateMetricsJson();

    // Send HTTP response
    std::ostringstream response;
    response << "HTTP/1.1 200 OK\r\n"
             << "Content-Type: application/json\r\n"
             << "Content-Length: " << metrics_json.length() << "\r\n"
             << "Access-Control-Allow-Origin: *\r\n"
             << "\r\n"
             << metrics_json;

    std::string response_str = response.str();
    write(client_fd, response_str.c_str(), response_str.length());
}

std::string MetricsServer::generateMetricsJson() {
    std::ostringstream json;
    json << "{\n";
    json << "  \"consumer_lag\": {\n";

    auto lag_metrics = registry_->getAllMetrics();
    bool first = true;

    for (const auto& [key, value] : lag_metrics) {
        if (!first) {
            json << ",\n";
        }
        first = false;

        // Key format: "consumer_lag.group.topic.partition"
        json << "    \"" << key << "\": " << value;
    }

    json << "\n  }\n";
    json << "}\n";

    return json.str();
}

} // namespace kawasan
