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
#include "kawasan/protocol/describe_groups_request.h"
#include "kawasan/protocol/list_groups_request.h"
// #include "kawasan/protocol/delete_groups_request.h"  // TODO: Not implemented yet
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
    std::string client_id_ = "kawasan-groups";
};

std::string errorToString(kawasan::ErrorCode code) {
    return kawasan::KawasanException::toString(code);
}

bool handleList(KafkaConnection& connection) {
    constexpr int16_t kListGroupsVersion = 3;
    kawasan::protocol::ListGroupsRequest request;

    auto payload = connection.invoke(
        kawasan::protocol::ApiKey::LIST_GROUPS, kListGroupsVersion,
        [&](kawasan::Buffer& buffer) { request.encode(buffer, kListGroupsVersion); });

    kawasan::protocol::ListGroupsResponse response;
    response.decode(payload, kListGroupsVersion);

    if (response.errorCode() != kawasan::ErrorCode::NONE) {
        std::cerr << "Error listing groups: " << errorToString(response.errorCode()) << "\n";
        return false;
    }

    if (response.groups().empty()) {
        std::cout << "No consumer groups found\n";
        return true;
    }

    std::cout << "Consumer groups:\n";
    for (const auto& group : response.groups()) {
        std::cout << "  " << group.group_id << " (" << group.protocol_type << ")\n";
    }
    return true;
}

bool handleDescribe(KafkaConnection& connection, const std::string& group_id) {
    constexpr int16_t kDescribeGroupsVersion = 2;
    kawasan::protocol::DescribeGroupsRequest request;
    std::vector<std::string> groups = {group_id};
    request.setGroups(groups);

    auto payload = connection.invoke(
        kawasan::protocol::ApiKey::DESCRIBE_GROUPS, kDescribeGroupsVersion,
        [&](kawasan::Buffer& buffer) { request.encode(buffer, kDescribeGroupsVersion); });

    kawasan::protocol::DescribeGroupsResponse response;
    response.decode(payload, kDescribeGroupsVersion);

    if (response.groups().empty()) {
        std::cerr << "No group information returned\n";
        return false;
    }

    const auto& group = response.groups()[0];
    
    if (group.error_code != kawasan::ErrorCode::NONE) {
        std::cerr << "Error describing group: " << errorToString(group.error_code) << "\n";
        return false;
    }

    std::cout << "Group: " << group.group_id << "\n";
    std::cout << "  State: " << group.group_state << "\n";
    std::cout << "  Protocol Type: " << group.protocol_type << "\n";
    std::cout << "  Protocol Data: " << group.protocol_data << "\n";
    std::cout << "  Members: " << group.members.size() << "\n";

    for (const auto& member : group.members) {
        std::cout << "\n  Member:\n";
        std::cout << "    Member ID: " << member.member_id << "\n";
        std::cout << "    Client ID: " << member.client_id << "\n";
        std::cout << "    Client Host: " << member.client_host << "\n";
        std::cout << "    Metadata: " << member.member_metadata.size() << " bytes\n";
        std::cout << "    Assignment: " << member.member_assignment.size() << " bytes\n";
    }

    return true;
}

/* TODO: Implement when DeleteGroupsRequest/Response are available
bool handleDelete(KafkaConnection& connection, const std::string& group_id) {
    constexpr int16_t kDeleteGroupsVersion = 2;
    kawasan::protocol::DeleteGroupsRequest request;
    request.addGroupId(group_id);

    auto payload = connection.invoke(
        kawasan::protocol::ApiKey::DELETE_GROUPS, kDeleteGroupsVersion,
        [&](kawasan::Buffer& buffer) { request.encode(buffer, kDeleteGroupsVersion); });

    kawasan::protocol::DeleteGroupsResponse response;
    response.decode(payload, kDeleteGroupsVersion);

    if (response.results().empty()) {
        std::cerr << "No deletion result returned\n";
        return false;
    }

    const auto& result = response.results()[0];
    
    if (result.error_code == kawasan::ErrorCode::NONE) {
        std::cout << "Successfully deleted group: " << result.group_id << "\n";
        return true;
    } else {
        std::cerr << "Failed to delete group '" << result.group_id << "': "
                  << errorToString(result.error_code) << "\n";
        return false;
    }
}
*/

}  // namespace

int main(int argc, char* argv[]) {
    try {
        std::string bootstrap_server;
        std::string list_flag;
        std::string describe_group;
        // std::string delete_group;  // TODO: Not implemented yet

        po::options_description desc("kawasan-groups - Kawasan consumer group management tool");
        desc.add_options()
            ("help,h", "Show help message")
            ("bootstrap-server", po::value<std::string>(&bootstrap_server)->default_value("localhost:9092"),
             "Kafka broker to connect to")
            ("list", po::bool_switch(), "List all consumer groups")
            ("describe", po::value<std::string>(&describe_group), "Describe a consumer group");
            // ("delete", po::value<std::string>(&delete_group), "Delete a consumer group");  // TODO: Not implemented yet

        po::variables_map vm;
        po::store(po::parse_command_line(argc, argv, desc), vm);
        po::notify(vm);

        if (vm.count("help")) {
            std::cout << desc << "\n";
            std::cout << "\nExamples:\n";
            std::cout << "  kawasan-groups --list\n";
            std::cout << "  kawasan-groups --describe my-group\n";
            // std::cout << "  kawasan-groups --delete my-group\n";  // TODO: Not implemented yet
            return 0;
        }

        auto endpoint = parseBootstrapServer(bootstrap_server);
        KafkaConnection connection(endpoint.host, endpoint.port);

        bool list_groups = vm["list"].as<bool>();
        bool has_describe = vm.count("describe") > 0;
        // bool has_delete = vm.count("delete") > 0;  // TODO: Not implemented yet

        int operation_count = (list_groups ? 1 : 0) + (has_describe ? 1 : 0); // + (has_delete ? 1 : 0);
        if (operation_count == 0) {
            std::cerr << "Error: No operation specified. Use --list or --describe\n";  // or --delete
            std::cerr << "Use --help for usage information\n";
            return 1;
        }
        if (operation_count > 1) {
            std::cerr << "Error: Only one operation can be specified at a time\n";
            return 1;
        }

        if (list_groups) {
            return handleList(connection) ? 0 : 1;
        } else if (has_describe) {
            return handleDescribe(connection, describe_group) ? 0 : 1;
        } // else if (has_delete) {
        //     return handleDelete(connection, delete_group) ? 0 : 1;
        // }

        return 0;

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    }
}
