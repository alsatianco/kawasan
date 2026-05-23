#pragma once

#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "kawasan/common/buffer.h"
#include "kawasan/common/config.h"
#include "kawasan/common/types.h"
#include "kawasan/protocol/api_keys.h"

namespace kawasan::client {

/// @brief Producer configuration
struct ProducerConfig {
    std::string bootstrap_servers = "localhost:9092";
    std::string client_id = "kawasan-producer";
    int16_t acks = -1;
    int32_t timeout_ms = 30000;
    int32_t batch_size = 16384;
    CompressionType compression = CompressionType::NONE;
};

/// @brief Record metadata returned after send
struct RecordMetadata {
    std::string topic;
    PartitionId partition;
    Offset offset;
    Timestamp timestamp;
};

/// @brief Kafka producer client
class Producer {
public:
    explicit Producer(const ProducerConfig& config);
    ~Producer();

    // Disable copy, allow move
    Producer(const Producer&) = delete;
    Producer& operator=(const Producer&) = delete;
    Producer(Producer&&) = delete;
    Producer& operator=(Producer&&) = delete;

    /// @brief Sends a record asynchronously
    /// @param topic Topic name
    /// @param key Record key
    /// @param value Record value
    /// @return Future with record metadata
    std::future<RecordMetadata> send(const std::string& topic, const std::string& key,
                                      const std::string& value);

    /// @brief Flushes pending records
    void flush();

    /// @brief Closes the producer
    void close();

private:
    struct TopicState {
        int32_t partitions = 1;
        PartitionId next_partition = 0;
    };

    static constexpr int16_t kMetadataApiVersion = 4;
    static constexpr int16_t kProduceApiVersion = 2;

    void ensureConnected();
    void disconnect();
    std::pair<std::string, uint16_t> parseBootstrapServers(const std::string& servers) const;
    TopicState fetchTopicState(const std::string& topic);
    TopicState& topicState(const std::string& topic);
    PartitionId selectPartition(const std::string& topic, TopicState& state,
                                const std::string& key);
    std::vector<uint8_t> sendRequest(
        protocol::ApiKey api_key, int16_t api_version,
        const std::function<void(Buffer&)>& encoder, bool expect_response,
        CorrelationId correlation_id);
    Buffer awaitResponse(std::vector<uint8_t> payload,
                         CorrelationId expected_correlation_id) const;

    ProducerConfig config_;
    boost::asio::io_context io_context_;
    boost::asio::ip::tcp::socket socket_{io_context_};
    std::string host_;
    uint16_t port_ = 0;
    bool connected_ = false;
    CorrelationId next_correlation_id_ = 1;
    std::unordered_map<std::string, TopicState> topic_cache_;
    std::mutex mutex_;
};

}  // namespace kawasan::client
