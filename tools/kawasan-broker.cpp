#include <csignal>
#include <iostream>

#include <boost/program_options.hpp>

#include "kawasan/broker/kawasan_broker.h"
#include "kawasan/common/config.h"
#include "kawasan/common/logger.h"

namespace po = boost::program_options;

std::atomic<bool> running{true};
kawasan::broker::KawasanBroker* broker_instance = nullptr;

void signalHandler(int signal) {
    if (signal == SIGINT || signal == SIGTERM) {
        std::cout << "\nReceived signal " << signal << ", shutting down..." << std::endl;
        running = false;
        if (broker_instance) {
            broker_instance->stop();
        }
    }
}

int main(int argc, char* argv[]) {
    try {
        po::options_description desc("Kawasan Broker Options");
        desc.add_options()("help,h", "Show help message")(
            "config,c", po::value<std::string>(), "Configuration file path")(
            "broker-id", po::value<int32_t>(), "Broker ID")(
            "host", po::value<std::string>(), "Broker host")(
            "port,p", po::value<int32_t>(), "Broker port")(
            "log-dir", po::value<std::string>(),
            "Log directory")("log-level",
                             po::value<std::string>()->default_value("info"),
                             "Log level (trace, debug, info, warn, error, critical)");

        po::variables_map vm;
        po::store(po::parse_command_line(argc, argv, desc), vm);
        po::notify(vm);

        if (vm.count("help")) {
            std::cout << "Kawasan Kafka Broker\n\n";
            std::cout << desc << std::endl;
            return 0;
        }

        // Initialize logger
        std::string log_level = vm["log-level"].as<std::string>();
        kawasan::Logger::init(log_level);

        kawasan::Logger::info("Starting Kawasan Broker...");
        kawasan::Logger::info("Version: 0.1.0");

        // Create configuration
        kawasan::Config config;

        if (vm.count("config")) {
            std::string config_file = vm["config"].as<std::string>();
            config.load(config_file);
            kawasan::Logger::info("Loaded configuration from: {}", config_file);
        }

        // Override with command-line options (only if explicitly provided)
        if (vm.count("broker-id")) {
            config.setInt("broker.id", vm["broker-id"].as<int32_t>());
        }
        if (vm.count("host")) {
            config.setString("host", vm["host"].as<std::string>());
        }
        if (vm.count("port")) {
            config.setInt("port", vm["port"].as<int32_t>());
        }
        if (vm.count("log-dir")) {
            config.setString("log.dirs", vm["log-dir"].as<std::string>());
        }

        // Validate configuration
        kawasan::Logger::info("Validating configuration...");
        config.validate();
        kawasan::Logger::info("Configuration validated successfully");

        // Create and start broker
        kawasan::broker::KawasanBroker broker(config);
        broker_instance = &broker;

        // Install signal handlers
        std::signal(SIGINT, signalHandler);
        std::signal(SIGTERM, signalHandler);

        broker.start();

        kawasan::Logger::info("Broker started successfully. Press Ctrl+C to stop.");

        // Wait for shutdown signal
        while (running && broker.isRunning()) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }

        kawasan::Logger::info("Shutting down broker...");
        broker.stop();
        kawasan::Logger::info("Broker stopped successfully");

        return 0;

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
}

