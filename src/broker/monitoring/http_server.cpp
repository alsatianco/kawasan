#include "kawasan/broker/monitoring/http_server.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstring>
#include <sstream>

#include "kawasan/common/logger.h"

namespace kawasan::broker::monitoring {

std::string HttpResponse::toString() const {
    std::ostringstream oss;
    oss << "HTTP/1.1 " << status_code << " " << status_text << "\r\n";
    oss << "Content-Type: " << content_type << "\r\n";
    oss << "Content-Length: " << body.size() << "\r\n";
    oss << "Connection: close\r\n";
    oss << "\r\n";
    oss << body;
    return oss.str();
}

HttpServer::HttpServer(const std::string& host, int port)
    : host_(host), port_(port) {
}

HttpServer::~HttpServer() {
    stop();
}

void HttpServer::registerHandler(const std::string& path, RequestHandler handler) {
    std::lock_guard<std::mutex> lock(handlers_mutex_);
    handlers_[path] = std::move(handler);
}

void HttpServer::start() {
    if (running_.exchange(true)) {
        return;  // Already running
    }

    // Create socket
    server_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd_ < 0) {
        Logger::error("Failed to create HTTP server socket: {}", std::strerror(errno));
        running_ = false;
        return;
    }

    // Set socket options
    int opt = 1;
    if (setsockopt(server_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        Logger::warn("Failed to set SO_REUSEADDR on HTTP server socket");
    }

    // Bind socket
    struct sockaddr_in addr;
    std::memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port_);
    
    if (host_ == "0.0.0.0" || host_ == "*") {
        addr.sin_addr.s_addr = INADDR_ANY;
    } else {
        if (inet_pton(AF_INET, host_.c_str(), &addr.sin_addr) <= 0) {
            Logger::error("Invalid HTTP server host address: {}", host_);
            close(server_fd_);
            server_fd_ = -1;
            running_ = false;
            return;
        }
    }

    if (bind(server_fd_, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        Logger::error("Failed to bind HTTP server socket to {}:{}: {}", 
                     host_, port_, std::strerror(errno));
        close(server_fd_);
        server_fd_ = -1;
        running_ = false;
        return;
    }

    // Listen
    if (listen(server_fd_, 10) < 0) {
        Logger::error("Failed to listen on HTTP server socket: {}", std::strerror(errno));
        close(server_fd_);
        server_fd_ = -1;
        running_ = false;
        return;
    }

    Logger::info("HTTP server listening on {}:{}", host_, port_);

    // Start server thread
    server_thread_ = std::thread([this]() { serverLoop(); });
}

void HttpServer::stop() {
    if (!running_.exchange(false)) {
        return;  // Not running
    }

    if (server_fd_ >= 0) {
        shutdown(server_fd_, SHUT_RDWR);
        close(server_fd_);
        server_fd_ = -1;
    }

    if (server_thread_.joinable()) {
        server_thread_.join();
    }

    Logger::info("HTTP server stopped");
}

void HttpServer::serverLoop() {
    while (running_) {
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);
        
        int client_fd = accept(server_fd_, (struct sockaddr*)&client_addr, &client_len);
        if (client_fd < 0) {
            if (running_) {
                Logger::warn("Failed to accept HTTP client connection: {}", std::strerror(errno));
            }
            continue;
        }

        // Handle client in the same thread (simple approach for monitoring)
        handleClient(client_fd);
        close(client_fd);
    }
}

void HttpServer::handleClient(int client_fd) {
    char buffer[4096];
    ssize_t bytes_read = recv(client_fd, buffer, sizeof(buffer) - 1, 0);
    
    if (bytes_read <= 0) {
        return;
    }

    buffer[bytes_read] = '\0';
    std::string request_data(buffer, bytes_read);

    try {
        HttpRequest request = parseRequest(request_data);
        HttpResponse response = handleRequest(request);
        
        std::string response_str = response.toString();
        send(client_fd, response_str.c_str(), response_str.size(), 0);
    } catch (const std::exception& e) {
        Logger::warn("Error handling HTTP request: {}", e.what());
        
        HttpResponse error_response;
        error_response.status_code = 500;
        error_response.status_text = "Internal Server Error";
        error_response.body = "Internal Server Error\n";
        
        std::string response_str = error_response.toString();
        send(client_fd, response_str.c_str(), response_str.size(), 0);
    }
}

HttpRequest HttpServer::parseRequest(const std::string& request_data) {
    HttpRequest request;
    
    std::istringstream iss(request_data);
    iss >> request.method >> request.path >> request.version;
    
    return request;
}

HttpResponse HttpServer::handleRequest(const HttpRequest& request) {
    std::lock_guard<std::mutex> lock(handlers_mutex_);
    
    auto it = handlers_.find(request.path);
    if (it != handlers_.end()) {
        return it->second(request);
    }

    // 404 Not Found
    HttpResponse response;
    response.status_code = 404;
    response.status_text = "Not Found";
    response.body = "Not Found\n";
    return response;
}

}  // namespace kawasan::broker::monitoring
