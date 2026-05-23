#include <arpa/inet.h>

#include <array>
#include <cstring>
#include <functional>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <boost/asio.hpp>
#include <boost/program_options.hpp>

#include "kawasan/common/buffer.h"
#include "kawasan/common/error.h"
#include "kawasan/protocol/api_keys.h"
#include "kawasan/protocol/create_topics_request.h"
#include "kawasan/protocol/delete_topics_request.h"
#include "kawasan/protocol/metadata_request.h"
#include "kawasan/protocol/request_header.h"

namespace po = boost::program_options;

namespace {

struct Endpoint {
    std::string host;
    uint16_t port;
};

Endpoint parseBootstrapServer(const std::string& value) {
    if (value.empty()) {
        throw std::runtime_error("bootstrap-server must not be empty");
    }

    std::string host;
    std::string port_str;
    if (value.front() == '[') {
        auto bracket = value.find(']');
        if (bracket == std::string::npos) {
            throw std::runtime_error("Invalid IPv6 bootstrap-server format");
        }
        host = value.substr(1, bracket - 1);
        if (bracket + 1 >= value.size() || value[bracket + 1] != ':') {
            throw std::runtime_error("Expected port separator after IPv6 host");
        }
        port_str = value.substr(bracket + 2);
    } else {
        auto colon = value.rfind(':');
        if (colon == std::string::npos) {
            throw std::runtime_error("bootstrap-server must be in host:port form");
        }
        host = value.substr(0, colon);
        port_str = value.substr(colon + 1);
    }

    int port = std::stoi(port_str);
    if (port <= 0 || port > 65535) {
        throw std::runtime_error("bootstrap-server port must be between 1 and 65535");
    }

    return {host, static_cast<uint16_t>(port)};
}

class KafkaConnection {
public:
    KafkaConnection(const std::string& host, uint16_t port)
        : socket_(io_context_) {
        boost::asio::ip::tcp::resolver resolver(io_context_);
        auto endpoints = resolver.resolve(host, std::to_string(port));
        boost::asio::connect(socket_, endpoints);
    }

    kawasan::Buffer invoke(kawasan::protocol::ApiKey api_key, int16_t api_version,
                          const std::function<void(kawasan::Buffer&)>& encode_request) {
        kawasan::Buffer payload;
        encode_request(payload);

        kawasan::protocol::RequestHeader header(
            api_key, api_version, nextCorrelationId(), client_id_);

        kawasan::Buffer frame;
        header.encode(frame);
        if (payload.size() > 0) {
            frame.writeBytes(payload.data(), payload.size());
        }

        const int32_t length = static_cast<int32_t>(frame.size());
        std::vector<uint8_t> send_buffer(sizeof(int32_t) + frame.size());
        uint32_t net_length = htonl(static_cast<uint32_t>(length));
        std::memcpy(send_buffer.data(), &net_length, sizeof(net_length));
        if (length > 0) {
            std::memcpy(send_buffer.data() + sizeof(int32_t), frame.data(), frame.size());
        }

        boost::asio::write(socket_, boost::asio::buffer(send_buffer));

        std::array<uint8_t, 4> size_buffer{};
        boost::asio::read(socket_, boost::asio::buffer(size_buffer));
        uint32_t net_response = 0;
        std::memcpy(&net_response, size_buffer.data(), sizeof(net_response));
        int32_t response_size = static_cast<int32_t>(ntohl(net_response));
        if (response_size <= 0) {
            throw std::runtime_error("Broker returned empty response");
        }

        std::vector<uint8_t> response_bytes(static_cast<size_t>(response_size));
        boost::asio::read(socket_, boost::asio::buffer(response_bytes));

        kawasan::Buffer response(std::move(response_bytes));
        kawasan::protocol::ResponseHeader response_header;
        response_header.decode(response);
        return response;
    }

private:
    int32_t nextCorrelationId() { return ++correlation_id_; }

    boost::asio::io_context io_context_;
    boost::asio::ip::tcp::socket socket_;
    int32_t correlation_id_ = 0;
    std::string client_id_ = "kawasan-topics";
};

std::string errorToString(kawasan::ErrorCode code) {
    return kawasan::KawasanException::toString(code);
}

bool handleList(KafkaConnection& connection, const std::optional<std::string>& topic) {
    constexpr int16_t kMetadataVersion = 7;
    kawasan::protocol::MetadataRequest request;
    if (topic) {
        request.addTopic(*topic);
    }

    auto payload = connection.invoke(
        kawasan::protocol::ApiKey::METADATA, kMetadataVersion,
        [&](kawasan::Buffer& buffer) { request.encode(buffer, kMetadataVersion); });

    kawasan::protocol::MetadataResponse response;
    response.decode(payload, kMetadataVersion);

    if (response.topics().empty()) {
        std::cout << "No topics found\n";
        return true;
    }

    for (const auto& topic_metadata : response.topics()) {
        if (topic_metadata.error_code != kawasan::ErrorCode::NONE) {
            std::cout << topic_metadata.name << " (error: "
                      << errorToString(topic_metadata.error_code) << ")\n";
            continue;
        }

        std::cout << topic_metadata.name << "\n";
        for (const auto& partition : topic_metadata.partitions) {
            std::cout << "  partition " << partition.partition << " leader "
                      << partition.leader << " replicas [";
            for (size_t i = 0; i < partition.replicas.size(); ++i) {
                std::cout << partition.replicas[i];
                if (i + 1 < partition.replicas.size()) {
                    std::cout << ",";
                }
            }
            std::cout << "]\n";
        }
    }
    return true;
}

bool handleCreate(KafkaConnection& connection, const std::string& topic, int32_t partitions,
                  int16_t replication_factor) {
    constexpr int16_t kCreateTopicsVersion = 4;
    kawasan::protocol::CreateTopicsRequest request;
    request.setTimeoutMs(30000);
    kawasan::protocol::CreatableTopic creatable;
    creatable.name = topic;
    creatable.num_partitions = partitions;
    creatable.replication_factor = replication_factor;
    request.addTopic(creatable);

    auto payload = connection.invoke(
        kawasan::protocol::ApiKey::CREATE_TOPICS, kCreateTopicsVersion,
        [&](kawasan::Buffer& buffer) { request.encode(buffer, kCreateTopicsVersion); });

    kawasan::protocol::CreateTopicsResponse response;
    response.decode(payload, kCreateTopicsVersion);

    bool success = true;
    for (const auto& result : response.results()) {
        if (result.error_code == kawasan::ErrorCode::NONE) {
            std::cout << "Created topic '" << result.name << "'\n";
        } else {
            success = false;
            std::cout << "Failed to create '" << result.name << "': "
                      << errorToString(result.error_code);
            if (!result.error_message.empty()) {
                std::cout << " (" << result.error_message << ")";
            }
            std::cout << "\n";
        }
    }
    return success;
}

bool handleDelete(KafkaConnection& connection, const std::string& topic) {
    constexpr int16_t kDeleteTopicsVersion = 2;
    kawasan::protocol::DeleteTopicsRequest request;
    request.setTimeoutMs(30000);
    request.addTopic(topic);

    auto payload = connection.invoke(
        kawasan::protocol::ApiKey::DELETE_TOPICS, kDeleteTopicsVersion,
        [&](kawasan::Buffer& buffer) { request.encode(buffer, kDeleteTopicsVersion); });

    kawasan::protocol::DeleteTopicsResponse response;
    response.decode(payload, kDeleteTopicsVersion);

    bool success = true;
    for (const auto& result : response.results()) {
        if (result.error_code == kawasan::ErrorCode::NONE) {
            std::cout << "Deleted topic '" << result.name << "'\n";
        } else {
            success = false;
            std::cout << "Failed to delete '" << result.name << "': "
                      << errorToString(result.error_code);
            if (!result.error_message.empty()) {
                std::cout << " (" << result.error_message << ")";
            }
            std::cout << "\n";
        }
    }
    return success;
}

}  // namespace

int main(int argc, char* argv[]) {
    try {
        po::options_description desc("Kawasan topics CLI");
        desc.add_options()("help,h", "Show help message")(
            "bootstrap-server",
            po::value<std::string>()->default_value("localhost:9092"),
            "Bootstrap host:port")("list", "List topics")(
            "create", "Create a topic")("delete", "Delete a topic")(
            "topic", po::value<std::string>(), "Topic name")(
            "partitions", po::value<int32_t>()->default_value(1),
            "Number of partitions")(
            "replication-factor", po::value<int16_t>()->default_value(1),
            "Replication factor");

        po::variables_map vm;
        po::store(po::parse_command_line(argc, argv, desc), vm);
        po::notify(vm);

        if (vm.count("help")) {
            std::cout << desc << std::endl;
            return 0;
        }

        const bool list = vm.count("list") > 0;
        const bool create = vm.count("create") > 0;
        const bool del = vm.count("delete") > 0;
        const int action_count = static_cast<int>(list) + static_cast<int>(create) +
                                 static_cast<int>(del);
        if (action_count != 1) {
            throw std::runtime_error("Specify exactly one of --list, --create, or --delete");
        }

        auto endpoint =
            parseBootstrapServer(vm["bootstrap-server"].as<std::string>());

        KafkaConnection connection(endpoint.host, endpoint.port);

        bool success = false;
        if (list) {
            std::optional<std::string> topic;
            if (vm.count("topic")) {
                topic = vm["topic"].as<std::string>();
            }
            success = handleList(connection, topic);
        } else if (create) {
            if (!vm.count("topic")) {
                throw std::runtime_error("--topic is required for --create");
            }
            const std::string topic_name = vm["topic"].as<std::string>();
            const int32_t partitions = vm["partitions"].as<int32_t>();
            const int16_t replication_factor =
                vm["replication-factor"].as<int16_t>();
            success = handleCreate(connection, topic_name, partitions,
                                   replication_factor);
        } else if (del) {
            if (!vm.count("topic")) {
                throw std::runtime_error("--topic is required for --delete");
            }
            const std::string topic_name = vm["topic"].as<std::string>();
            success = handleDelete(connection, topic_name);
        }

        return success ? 0 : 1;
    } catch (const std::exception& ex) {
        std::cerr << "Error: " << ex.what() << std::endl;
        return 1;
    }
}
