#include <iostream>
#include <string>

#include "kawasan/client/producer.h"
#include "kawasan/common/logger.h"

int main(int argc, char** argv) {
    try {
        kawasan::Logger::init("info");

        std::string topic = argc > 1 ? argv[1] : "kawasan-demo";
        std::string key = argc > 2 ? argv[2] : "";
        std::string value = argc > 3 ? argv[3] : "Hello, Kawasan!";
        std::string bootstrap = argc > 4 ? argv[4] : "localhost:9092";

        kawasan::client::ProducerConfig config;
        config.bootstrap_servers = bootstrap;

        kawasan::client::Producer producer(config);

        std::cout << "Producing to " << topic << " (bootstrap=" << bootstrap << ")..." << std::endl;

        auto future = producer.send(topic, key, value);
        auto metadata = future.get();

        std::cout << "Message delivered: topic=" << metadata.topic << " partition="
                  << metadata.partition << " offset=" << metadata.offset << std::endl;

        producer.close();
        return 0;

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
}
