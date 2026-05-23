#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <string>
#include <thread>

namespace kawasan::broker::monitoring {

struct HttpRequest {
    std::string method;
    std::string path;
    std::string version;
};

struct HttpResponse {
    int status_code = 200;
    std::string status_text = "OK";
    std::string content_type = "text/plain";
    std::string body;

    std::string toString() const;
};

/// @brief Simple HTTP server for health checks and metrics
class HttpServer {
public:
    using RequestHandler = std::function<HttpResponse(const HttpRequest&)>;

    HttpServer(const std::string& host, int port);
    ~HttpServer();

    // Non-copyable/movable
    HttpServer(const HttpServer&) = delete;
    HttpServer& operator=(const HttpServer&) = delete;
    HttpServer(HttpServer&&) = delete;
    HttpServer& operator=(HttpServer&&) = delete;

    /// @brief Register a handler for a specific path
    void registerHandler(const std::string& path, RequestHandler handler);

    /// @brief Start the HTTP server
    void start();

    /// @brief Stop the HTTP server
    void stop();

    /// @brief Returns whether the server is running
    bool isRunning() const { return running_; }

private:
    void serverLoop();
    void handleClient(int client_fd);
    HttpRequest parseRequest(const std::string& request_data);
    HttpResponse handleRequest(const HttpRequest& request);

    std::string host_;
    int port_;
    int server_fd_ = -1;
    std::atomic<bool> running_{false};
    std::thread server_thread_;
    
    std::unordered_map<std::string, RequestHandler> handlers_;
    std::mutex handlers_mutex_;
};

}  // namespace kawasan::broker::monitoring
