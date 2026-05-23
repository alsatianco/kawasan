#include <arpa/inet.h>

#include <array>
#include <cstring>
#include <functional>
#include <iostream>
#include <map>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <boost/asio.hpp>
#include <boost/program_options.hpp>
#include <nlohmann/json.hpp>

#include "kawasan/common/buffer.h"
#include "kawasan/common/error.h"
#include "kawasan/protocol/api_keys.h"
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
    std::string client_id_ = "kawasan-metadata-check";
};

std::string computeMetadataChecksum(const kawasan::protocol::MetadataResponse& response) {
    nlohmann::json json;
    json["cluster_id"] = response.clusterId();
    
    // Add topics in sorted order for deterministic checksum
    json["topics"] = nlohmann::json::array();
    std::vector<std::string> topic_names;
    for (const auto& topic : response.topics()) {
        if (topic.error_code == kawasan::ErrorCode::NONE) {
            topic_names.push_back(topic.name);
        }
    }
    std::sort(topic_names.begin(), topic_names.end());
    
    for (const auto& topic_name : topic_names) {
        // Find the topic in response
        const kawasan::TopicMetadata* topic_ptr = nullptr;
        for (const auto& t : response.topics()) {
            if (t.name == topic_name) {
                topic_ptr = &t;
                break;
            }
        }
        if (!topic_ptr) continue;
        
        nlohmann::json topic_json;
        topic_json["name"] = topic_ptr->name;
        topic_json["is_internal"] = topic_ptr->is_internal;
        
        // Add partitions in sorted order
        topic_json["partitions"] = nlohmann::json::array();
        for (const auto& partition : topic_ptr->partitions) {
            if (partition.error_code == kawasan::ErrorCode::NONE) {
                nlohmann::json part_json;
                part_json["partition"] = partition.partition;
                part_json["leader"] = partition.leader;
                part_json["leader_epoch"] = partition.leader_epoch;
                part_json["replicas"] = partition.replicas;
                part_json["isr"] = partition.isr;
                part_json["offline_replicas"] = partition.offline_replicas;
                topic_json["partitions"].push_back(std::move(part_json));
            }
        }
        
        json["topics"].push_back(std::move(topic_json));
    }
    
    // Compute hash of the canonical JSON
    std::string canonical = json.dump();
    std::hash<std::string> hasher;
    size_t hash_value = hasher(canonical);
    
    // Convert to hex string
    std::stringstream ss;
    ss << std::hex << std::setfill('0') << std::setw(16) << hash_value;
    return ss.str();
}

struct BrokerChecksum {
    std::string host;
    uint16_t port;
    int32_t broker_id;
    std::string checksum;
    bool success;
    std::string error_message;
};

BrokerChecksum fetchMetadataChecksum(const std::string& host, uint16_t port) {
    BrokerChecksum result;
    result.host = host;
    result.port = port;
    result.success = false;
    
    try {
        KafkaConnection connection(host, port);
        
        // Fetch metadata for all topics
        constexpr int16_t kMetadataVersion = 7;
        kawasan::protocol::MetadataRequest request;
        
        auto payload = connection.invoke(
            kawasan::protocol::ApiKey::METADATA, kMetadataVersion,
            [&](kawasan::Buffer& buffer) { request.encode(buffer, kMetadataVersion); });
        
        kawasan::protocol::MetadataResponse response;
        response.decode(payload, kMetadataVersion);
        
        result.broker_id = response.controllerId();
        result.checksum = computeMetadataChecksum(response);
        result.success = true;
    } catch (const std::exception& ex) {
        result.error_message = ex.what();
        result.success = false;
    }
    
    return result;
}

bool checkConsistency(const std::vector<std::string>& brokers) {
    std::vector<BrokerChecksum> results;
    
    std::cout << "Fetching metadata from " << brokers.size() << " broker(s)...\n\n";
    
    for (const auto& broker_str : brokers) {
        try {
            auto endpoint = parseBootstrapServer(broker_str);
            std::cout << "Connecting to " << endpoint.host << ":" << endpoint.port << "... ";
            
            auto result = fetchMetadataChecksum(endpoint.host, endpoint.port);
            if (result.success) {
                std::cout << "OK\n";
                std::cout << "  Broker ID: " << result.broker_id << "\n";
                std::cout << "  Checksum:  " << result.checksum << "\n\n";
            } else {
                std::cout << "FAILED\n";
                std::cout << "  Error: " << result.error_message << "\n\n";
            }
            results.push_back(result);
        } catch (const std::exception& ex) {
            std::cout << "FAILED\n";
            std::cout << "  Error: " << ex.what() << "\n\n";
        }
    }
    
    // Check consistency
    if (results.empty()) {
        std::cout << "No successful metadata fetches. Cannot verify consistency.\n";
        return false;
    }
    
    // Filter successful results
    std::vector<BrokerChecksum> successful;
    for (const auto& result : results) {
        if (result.success) {
            successful.push_back(result);
        }
    }
    
    if (successful.empty()) {
        std::cout << "No successful metadata fetches. Cannot verify consistency.\n";
        return false;
    }
    
    if (successful.size() == 1) {
        std::cout << "Only one broker responded successfully. Consistency check skipped.\n";
        return true;
    }
    
    // Compare checksums
    std::cout << "Consistency Check Results:\n";
    std::cout << "==========================\n\n";
    
    const std::string& reference_checksum = successful[0].checksum;
    bool all_consistent = true;
    
    for (size_t i = 1; i < successful.size(); ++i) {
        if (successful[i].checksum != reference_checksum) {
            all_consistent = false;
            std::cout << "DIVERGENCE DETECTED!\n";
            std::cout << "  Broker " << successful[0].broker_id 
                      << " (" << successful[0].host << ":" << successful[0].port << "): " 
                      << reference_checksum << "\n";
            std::cout << "  Broker " << successful[i].broker_id 
                      << " (" << successful[i].host << ":" << successful[i].port << "): " 
                      << successful[i].checksum << "\n\n";
        }
    }
    
    if (all_consistent) {
        std::cout << "✓ All brokers have consistent metadata\n";
        std::cout << "  Checksum: " << reference_checksum << "\n";
        std::cout << "  Brokers checked: " << successful.size() << "\n";
    } else {
        std::cout << "\n✗ Metadata inconsistency detected across brokers\n";
    }
    
    return all_consistent;
}

}  // namespace

int main(int argc, char* argv[]) {
    po::options_description desc("Kawasan Metadata Consistency Check Tool");
    desc.add_options()
        ("help,h", "Show this help message")
        ("brokers,b", po::value<std::vector<std::string>>()->multitoken(),
         "Broker addresses in host:port format (can specify multiple)")
        ("bootstrap-server", po::value<std::string>(),
         "Single broker address (alias for --brokers)");

    po::variables_map vm;
    try {
        po::store(po::parse_command_line(argc, argv, desc), vm);
        po::notify(vm);
    } catch (const std::exception& ex) {
        std::cerr << "Error parsing arguments: " << ex.what() << "\n\n";
        std::cerr << desc << "\n";
        return 1;
    }

    if (vm.count("help")) {
        std::cout << desc << "\n";
        std::cout << "Examples:\n";
        std::cout << "  Check single broker:\n";
        std::cout << "    kawasan-metadata-check --bootstrap-server localhost:9092\n\n";
        std::cout << "  Check multiple brokers:\n";
        std::cout << "    kawasan-metadata-check --brokers localhost:9092 localhost:9093 localhost:9094\n\n";
        return 0;
    }

    std::vector<std::string> brokers;
    
    if (vm.count("bootstrap-server")) {
        brokers.push_back(vm["bootstrap-server"].as<std::string>());
    }
    
    if (vm.count("brokers")) {
        auto broker_list = vm["brokers"].as<std::vector<std::string>>();
        brokers.insert(brokers.end(), broker_list.begin(), broker_list.end());
    }
    
    if (brokers.empty()) {
        std::cerr << "Error: No brokers specified. Use --brokers or --bootstrap-server.\n\n";
        std::cerr << desc << "\n";
        return 1;
    }

    try {
        bool consistent = checkConsistency(brokers);
        return consistent ? 0 : 1;
    } catch (const std::exception& ex) {
        std::cerr << "Fatal error: " << ex.what() << "\n";
        return 1;
    }
}
