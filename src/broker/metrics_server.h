#pragma once

#include <string>
#include <memory>
#include <atomic>
#include <thread>

namespace kawasan {

class MetricsRegistry;
class Broker;

class MetricsServer {
public:
    MetricsServer(int port, std::shared_ptr<MetricsRegistry> registry, Broker* broker);
    ~MetricsServer();

    void start();
    void stop();

private:
    void run();
    void handleRequest(int client_fd);
    std::string generateMetricsJson();

    int port_;
    std::shared_ptr<MetricsRegistry> registry_;
    Broker* broker_;
    std::atomic<bool> running_;
    int server_fd_;
    std::thread server_thread_;
};

} // namespace kawasan
