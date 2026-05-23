#include "kawasan/client/producer.h"

#include <arpa/inet.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cctype>
#include <cstring>
#include <exception>
#include <stdexcept>
#include <utility>
#include <vector>

#include <boost/asio/connect.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/write.hpp>

#include "kawasan/common/error.h"
#include "kawasan/common/logger.h"
#include "kawasan/protocol/metadata_request.h"
#include "kawasan/protocol/produce_request.h"
#include "kawasan/protocol/request_header.h"
#include "kawasan/storage/record_batch.h"

namespace kawasan::client {
namespace {

std::string trim(const std::string& input) {
    const auto is_space = [](unsigned char c) { return std::isspace(c) != 0; };
    auto begin = std::find_if_not(input.begin(), input.end(), is_space);
    auto end = std::find_if_not(input.rbegin(), input.rend(), is_space).base();
    if (begin >= end) {
        return "";
    }
    return std::string(begin, end);
}

std::vector<uint8_t> toBytes(const std::string& value) {
    return std::vector<uint8_t>(value.begin(), value.end());
}

Timestamp wallClockMillis() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

}  // namespace

Producer::Producer(const ProducerConfig& config) : config_(config) {
    auto [host, port] = parseBootstrapServers(config_.bootstrap_servers);
    host_ = std::move(host);
    port_ = port;
    Logger::info("Initialized Producer (client_id={}, target={}:{})", config_.client_id, host_,
                 port_);
}

Producer::~Producer() {
    close();
}

std::future<RecordMetadata> Producer::send(const std::string& topic, const std::string& key,
                                            const std::string& value) {
    std::promise<RecordMetadata> promise;
    auto future = promise.get_future();

    // Only lock for connection and metadata operations to reduce contention
    PartitionId partition;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        ensureConnected();
        TopicState& state = topicState(topic);
        partition = selectPartition(topic, state, key);
    }
    
    try {
        const Timestamp now_ms = wallClockMillis();

        Record record;
        record.timestamp = now_ms;
        if (!key.empty()) {
            record.key = toBytes(key);
        }
        record.value = toBytes(value);

        storage::RecordBatch batch;
        batch.addRecord(record);
        std::vector<uint8_t> batch_bytes = batch.serialize();

        protocol::ProducePartitionData partition_data;
        partition_data.partition = partition;
        partition_data.record_batch = std::move(batch_bytes);

        protocol::ProduceTopicData topic_data;
        topic_data.topic = topic;
        topic_data.partitions.push_back(std::move(partition_data));

        protocol::ProduceRequest produce_request;
        produce_request.setAcks(config_.acks);
        produce_request.setTimeoutMs(config_.timeout_ms);
        produce_request.addTopic(topic_data);

        const bool expect_response = config_.acks != 0;
        
        // Lock only for correlation ID increment and socket I/O
        CorrelationId correlation_id;
        std::vector<uint8_t> response_bytes;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            correlation_id = next_correlation_id_++;
            response_bytes = sendRequest(
                protocol::ApiKey::PRODUCE, kProduceApiVersion,
                [&](Buffer& buffer) { produce_request.encode(buffer, kProduceApiVersion); },
                expect_response, correlation_id);
        }

        RecordMetadata metadata;
        metadata.topic = topic;
        metadata.partition = partition;
        metadata.timestamp = now_ms;
        metadata.offset = -1;

        if (expect_response) {
            Buffer buffer = awaitResponse(std::move(response_bytes), correlation_id);
            protocol::ProduceResponse produce_response;
            produce_response.decode(buffer, kProduceApiVersion);

            const auto& topics = produce_response.topics();
            auto topic_it =
                std::find_if(topics.begin(), topics.end(),
                             [&](const protocol::ProduceTopicResponse& response_topic) {
                                 return response_topic.topic == topic;
                             });
            if (topic_it == topics.end()) {
                throw ProtocolException(ErrorCode::INVALID_REQUEST,
                                        "Produce response missing topic " + topic);
            }

            auto partition_it = std::find_if(
                topic_it->partitions.begin(), topic_it->partitions.end(),
                [&](const protocol::ProducePartitionResponse& partition_response) {
                    return partition_response.partition == partition;
                });
            if (partition_it == topic_it->partitions.end()) {
                throw ProtocolException(ErrorCode::INVALID_REQUEST,
                                        "Produce response missing partition " +
                                            std::to_string(partition));
            }

            if (partition_it->error_code != ErrorCode::NONE) {
                throw KawasanException(partition_it->error_code,
                                      "Broker rejected produce request");
            }

            metadata.offset = partition_it->base_offset;
            if (partition_it->log_append_time >= 0) {
                metadata.timestamp = partition_it->log_append_time;
            }
        }

        promise.set_value(metadata);
    } catch (const std::exception& ex) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            disconnect();
        }
        promise.set_exception(std::current_exception());
    }

    return future;
}

void Producer::flush() {
    Logger::debug("Flushing producer client");
}

void Producer::close() {
    std::lock_guard<std::mutex> lock(mutex_);
    disconnect();
    Logger::info("Closed producer");
}

void Producer::ensureConnected() {
    if (connected_ && socket_.is_open()) {
        return;
    }

    boost::system::error_code ec;
    socket_.close(ec);

    boost::asio::ip::tcp::resolver resolver(io_context_);
    const auto endpoint = resolver.resolve(host_, std::to_string(port_));
    boost::asio::connect(socket_, endpoint);
    connected_ = true;
}

void Producer::disconnect() {
    boost::system::error_code ec;
    socket_.close(ec);
    connected_ = false;
}

std::pair<std::string, uint16_t> Producer::parseBootstrapServers(
    const std::string& servers) const {
    if (servers.empty()) {
        throw ConfigException(ErrorCode::INVALID_CONFIG, "bootstrap servers not configured");
    }

    auto first = servers;
    auto comma_pos = servers.find(',');
    if (comma_pos != std::string::npos) {
        first = servers.substr(0, comma_pos);
    }
    first = trim(first);
    auto colon_pos = first.rfind(':');
    uint16_t port = 9092;
    if (colon_pos != std::string::npos) {
        auto port_str = trim(first.substr(colon_pos + 1));
        if (!port_str.empty()) {
            port = static_cast<uint16_t>(std::stoi(port_str));
        }
        first = trim(first.substr(0, colon_pos));
    }
    if (first.empty()) {
        first = "localhost";
    }
    return {first, port};
}

Producer::TopicState Producer::fetchTopicState(const std::string& topic) {
    const CorrelationId correlation_id = next_correlation_id_++;
    auto response_bytes = sendRequest(
        protocol::ApiKey::METADATA, kMetadataApiVersion,
        [&](Buffer& buffer) {
            protocol::MetadataRequest request({topic});
            request.encode(buffer, kMetadataApiVersion);
        },
        true, correlation_id);

    Buffer buffer = awaitResponse(std::move(response_bytes), correlation_id);
    protocol::MetadataResponse response;
    response.decode(buffer, kMetadataApiVersion);

    const auto& topics = response.topics();
    auto topic_it =
        std::find_if(topics.begin(), topics.end(),
                     [&](const TopicMetadata& metadata) { return metadata.name == topic; });

    if (topic_it == topics.end()) {
        throw KawasanException(ErrorCode::UNKNOWN_TOPIC_OR_PARTITION,
                              "Topic " + topic + " not found");
    }
    if (topic_it->error_code != ErrorCode::NONE) {
        throw KawasanException(topic_it->error_code,
                              "Topic metadata error for " + topic);
    }
    if (topic_it->partitions.empty()) {
        throw KawasanException(ErrorCode::INVALID_PARTITIONS,
                              "Topic " + topic + " has no partitions");
    }

    TopicState state;
    state.partitions = static_cast<int32_t>(topic_it->partitions.size());
    state.next_partition = 0;
    return state;
}

Producer::TopicState& Producer::topicState(const std::string& topic) {
    auto it = topic_cache_.find(topic);
    if (it == topic_cache_.end()) {
        it = topic_cache_.emplace(topic, fetchTopicState(topic)).first;
    }
    return it->second;
}

PartitionId Producer::selectPartition(const std::string& topic, TopicState& state,
                                      const std::string& key) {
    if (state.partitions <= 0) {
        throw KawasanException(ErrorCode::INVALID_PARTITIONS,
                              "Topic " + topic + " has no partitions");
    }

    if (!key.empty()) {
        std::hash<std::string> hasher;
        const auto hashed = static_cast<uint64_t>(hasher(key));
        return static_cast<PartitionId>(hashed % state.partitions);
    }

    const PartitionId partition =
        static_cast<PartitionId>(state.next_partition % state.partitions);
    state.next_partition = (state.next_partition + 1) % state.partitions;
    return partition;
}

std::vector<uint8_t> Producer::sendRequest(protocol::ApiKey api_key, int16_t api_version,
                                           const std::function<void(Buffer&)>& encoder,
                                           bool expect_response, CorrelationId correlation_id) {
    Buffer buffer;
    protocol::RequestHeader header(api_key, api_version, correlation_id, config_.client_id);
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

    if (!expect_response) {
        return {};
    }

    std::array<uint8_t, 4> size_bytes{};
    boost::asio::read(socket_, boost::asio::buffer(size_bytes));
    uint32_t net_length = 0;
    std::memcpy(&net_length, size_bytes.data(), size_bytes.size());
    const int32_t response_size = ntohl(static_cast<int32_t>(net_length));
    if (response_size < 0) {
        throw ProtocolException(ErrorCode::CORRUPT_MESSAGE,
                                "Negative response length received");
    }

    std::vector<uint8_t> response(static_cast<size_t>(response_size));
    if (response_size > 0) {
        boost::asio::read(socket_, boost::asio::buffer(response));
    }
    return response;
}

Buffer Producer::awaitResponse(std::vector<uint8_t> payload,
                               CorrelationId expected_correlation_id) const {
    Buffer buffer(std::move(payload));
    protocol::ResponseHeader header;
    header.decode(buffer);
    if (header.correlationId() != expected_correlation_id) {
        throw ProtocolException(ErrorCode::CORRUPT_MESSAGE,
                                "Correlation ID mismatch in response");
    }
    return buffer;
}

}  // namespace kawasan::client
