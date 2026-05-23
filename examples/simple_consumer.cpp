#include <arpa/inet.h>

#include <array>
#include <chrono>
#include <cstring>
#include <functional>
#include <iostream>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <boost/asio.hpp>

#include "kawasan/common/buffer.h"
#include "kawasan/common/error.h"
#include "kawasan/common/logger.h"
#include "kawasan/common/types.h"
#include "kawasan/protocol/fetch_request.h"
#include "kawasan/protocol/request_header.h"
#include "kawasan/storage/record_batch.h"

namespace {

class SimpleConsumer {
public:
    SimpleConsumer(std::string bootstrap, std::string client_id)
        : bootstrap_(std::move(bootstrap)), client_id_(std::move(client_id)) {}

    void connect() {
        if (connected_) {
            return;
        }
        auto [host, port] = parseBootstrap(bootstrap_);
        host_ = std::move(host);
        port_ = port;

        boost::asio::ip::tcp::endpoint endpoint(
            boost::asio::ip::make_address(host_), static_cast<uint16_t>(port_));
        socket_.connect(endpoint);
        connected_ = true;
    }

    std::vector<kawasan::storage::RecordBatch> fetch(const std::string& topic,
                                                    kawasan::PartitionId partition,
                                                    kawasan::Offset offset) {
        if (!connected_) {
            connect();
        }

        kawasan::protocol::FetchTopic topic_data;
        topic_data.topic = topic;
        kawasan::protocol::FetchPartition partition_data;
        partition_data.partition = partition;
        partition_data.fetch_offset = offset;
        partition_data.partition_max_bytes = 1024 * 1024;  // 1MB per fetch
        topic_data.partitions.push_back(partition_data);

        kawasan::protocol::FetchRequest request;
        request.setReplicaId(-1);
        request.setMaxWaitMs(500);
        request.setMinBytes(1);
        request.setMaxBytes(1024 * 1024);
        request.addTopic(topic_data);

        const auto correlation_id = next_correlation_id_++;
        auto payload = sendRequest(
            kawasan::protocol::ApiKey::FETCH, /*api_version=*/5,
            [&](kawasan::Buffer& buffer) { request.encode(buffer, 5); }, correlation_id);

        kawasan::Buffer response_buffer = awaitResponse(std::move(payload), correlation_id);
        kawasan::protocol::FetchResponse response;
        response.decode(response_buffer, 5);

        for (const auto& topic_response : response.topics()) {
            if (topic_response.topic != topic) {
                continue;
            }
            for (const auto& partition_response : topic_response.partitions) {
                if (partition_response.partition != partition) {
                    continue;
                }
                if (partition_response.error_code != kawasan::ErrorCode::NONE) {
                    throw kawasan::KawasanException(partition_response.error_code);
                }

                std::vector<kawasan::storage::RecordBatch> batches;
                if (partition_response.record_batches.empty()) {
                    return batches;
                }

                kawasan::Buffer batches_buffer(partition_response.record_batches);
                while (batches_buffer.remaining() > 0) {
                    batches.push_back(
                        kawasan::storage::RecordBatch::deserialize(batches_buffer));
                }
                return batches;
            }
        }

        throw kawasan::KawasanException(kawasan::ErrorCode::UNKNOWN_TOPIC_OR_PARTITION,
                                      "Topic or partition missing from fetch response");
    }

private:
    std::pair<std::string, uint16_t> parseBootstrap(const std::string& servers) const {
        if (servers.empty()) {
            return {"127.0.0.1", 9092};
        }

        auto comma_pos = servers.find(',');
        auto first = servers.substr(0, comma_pos);
        auto colon_pos = first.find(':');
        uint16_t port = 9092;
        if (colon_pos != std::string::npos) {
            auto port_str = first.substr(colon_pos + 1);
            if (!port_str.empty()) {
                port = static_cast<uint16_t>(std::stoi(port_str));
            }
            first = first.substr(0, colon_pos);
        }
        if (first.empty()) {
            first = "127.0.0.1";
        }
        return {first, port};
    }

    std::vector<uint8_t> sendRequest(kawasan::protocol::ApiKey api_key, int16_t api_version,
                                     const std::function<void(kawasan::Buffer&)>& encoder,
                                     kawasan::CorrelationId correlation_id) {
        kawasan::Buffer buffer;
        kawasan::protocol::RequestHeader header(api_key, api_version, correlation_id, client_id_);
        header.encode(buffer);
        encoder(buffer);

        const int32_t payload_size = static_cast<int32_t>(buffer.size());
        std::vector<uint8_t> frame(sizeof(int32_t) + payload_size);
        int32_t net_size = htonl(payload_size);
        std::memcpy(frame.data(), &net_size, sizeof(net_size));
        if (payload_size > 0) {
            std::memcpy(frame.data() + sizeof(int32_t), buffer.data(), buffer.size());
        }

        boost::asio::write(socket_, boost::asio::buffer(frame));

        std::array<uint8_t, 4> size_bytes{};
        boost::asio::read(socket_, boost::asio::buffer(size_bytes));
        uint32_t net_length = 0;
        std::memcpy(&net_length, size_bytes.data(), size_bytes.size());
        const int32_t response_size = ntohl(static_cast<int32_t>(net_length));

        std::vector<uint8_t> response(static_cast<size_t>(response_size));
        if (response_size > 0) {
            boost::asio::read(socket_, boost::asio::buffer(response));
        }
        return response;
    }

    kawasan::Buffer awaitResponse(std::vector<uint8_t> payload,
                                 kawasan::CorrelationId expected) const {
        kawasan::Buffer buffer(std::move(payload));
        kawasan::protocol::ResponseHeader header;
        header.decode(buffer);
        if (header.correlationId() != expected) {
            throw kawasan::KawasanException(kawasan::ErrorCode::CORRUPT_MESSAGE,
                                          "Correlation ID mismatch");
        }
        return buffer;
    }

    std::string bootstrap_;
    std::string client_id_;
    boost::asio::io_context io_context_;
    boost::asio::ip::tcp::socket socket_{io_context_};
    bool connected_ = false;
    std::string host_;
    uint16_t port_ = 0;
    kawasan::CorrelationId next_correlation_id_ = 1;
};

std::string toString(const std::optional<std::vector<uint8_t>>& bytes) {
    if (!bytes) {
        return {};
    }
    return std::string(bytes->begin(), bytes->end());
}

}  // namespace

int main(int argc, char** argv) {
    try {
        kawasan::Logger::init("info");

        std::string topic = argc > 1 ? argv[1] : "kawasan-demo";
        std::string bootstrap = argc > 2 ? argv[2] : "localhost:9092";
        kawasan::PartitionId partition = argc > 3 ? static_cast<kawasan::PartitionId>(std::stoi(argv[3]))
                                                 : 0;
        kawasan::Offset offset = argc > 4 ? std::stoll(argv[4]) : 0;

        std::cout << "Consuming from topic=" << topic << " partition=" << partition
                  << " bootstrap=" << bootstrap << " starting_offset=" << offset << std::endl;

        SimpleConsumer consumer(bootstrap, "simple-consumer");
        consumer.connect();

        while (true) {
            try {
                auto batches = consumer.fetch(topic, partition, offset);
                std::size_t delivered = 0;
                for (const auto& batch : batches) {
                    const auto& records = batch.records();
                    for (std::size_t i = 0; i < records.size(); ++i) {
                        const auto& record = records[i];
                        const auto record_offset =
                            batch.baseOffset() + static_cast<kawasan::Offset>(i);
                        auto key = toString(record.key);
                        auto value = toString(record.value);

                        std::cout << "[" << topic << ":" << partition << " offset=" << record_offset
                                  << "] ";
                        if (!key.empty()) {
                            std::cout << "key=" << key << " ";
                        }
                        std::cout << "value=" << value << std::endl;
                        ++delivered;
                    }
                    offset = batch.baseOffset() + static_cast<kawasan::Offset>(records.size());
                }

                if (delivered == 0) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(250));
                }
            } catch (const kawasan::KawasanException& ex) {
                if (ex.code() == kawasan::ErrorCode::OFFSET_OUT_OF_RANGE) {
                    std::cerr << "Offset out of range, resetting to 0" << std::endl;
                    offset = 0;
                    continue;
                }
                throw;
            }
        }
    } catch (const std::exception& ex) {
        std::cerr << "Consumer error: " << ex.what() << std::endl;
        return 1;
    }

    return 0;
}
