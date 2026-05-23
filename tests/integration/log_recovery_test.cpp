#include <arpa/inet.h>

#include <array>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <map>
#include <string>
#include <vector>

#include <boost/asio.hpp>
#include <gtest/gtest.h>

#include "kawasan/broker/kawasan_broker.h"
#include "kawasan/client/producer.h"
#include "kawasan/common/buffer.h"
#include "kawasan/common/config.h"
#include "kawasan/common/logger.h"
#include "kawasan/common/types.h"
#include "kawasan/protocol/create_topics_request.h"
#include "kawasan/protocol/fetch_request.h"
#include "kawasan/protocol/request_header.h"
#include "kawasan/storage/record_batch.h"

namespace {

std::string makeLogDir() {
    const auto timestamp =
        std::chrono::steady_clock::now().time_since_epoch().count();
    auto tmp = std::filesystem::temp_directory_path() /
               ("kawasan-log-recovery-" + std::to_string(timestamp));
    std::filesystem::create_directories(tmp);
    return tmp.string();
}

void ensureLoggerInitialized() {
    static bool initialized = false;
    if (!initialized) {
        kawasan::Logger::init("info");
        initialized = true;
    }
}

kawasan::Config makeConfig(const std::string& log_dir) {
    kawasan::Config config;
    config.setInt("broker.id", 1);
    config.setString("host", "127.0.0.1");
    config.setInt("port", 0);
    // opus2 Item 2: ephemeral Raft port for parallel test isolation.
    config.setInt("raft.port", 0);
    config.setString("log.dirs", log_dir);
    config.setInt("network.io_threads", 1);
    config.setLong("network.max_frame_bytes", 1024 * 1024);
    return config;
}

int32_t decodeLength(const std::array<uint8_t, 4>& bytes) {
    uint32_t value = 0;
    std::memcpy(&value, bytes.data(), sizeof(value));
    return ntohl(value);
}

std::vector<uint8_t> sendKafkaRequest(boost::asio::ip::tcp::socket& socket,
                                      kawasan::Buffer& payload) {
    kawasan::Buffer frame;
    frame.writeInt32(static_cast<int32_t>(payload.size()));
    const auto& payload_bytes = payload.vector();
    frame.writeBytes(payload_bytes.data(), payload_bytes.size());
    const auto& request_bytes = frame.vector();
    boost::asio::write(socket,
                       boost::asio::buffer(request_bytes.data(), request_bytes.size()));

    std::array<uint8_t, 4> size_bytes{};
    boost::asio::read(socket, boost::asio::buffer(size_bytes));
    const auto response_length = decodeLength(size_bytes);
    std::vector<uint8_t> response_body(response_length);
    if (response_length > 0) {
        boost::asio::read(socket, boost::asio::buffer(response_body));
    }
    return response_body;
}

std::vector<kawasan::storage::RecordBatch> decodeRecordBatches(
    const std::vector<uint8_t>& data) {
    // For Fetch v0-3 the broker returns a legacy MessageSet.
    std::vector<kawasan::storage::RecordBatch> batches;
    if (data.empty()) {
        return batches;
    }
    kawasan::storage::RecordBatch batch;
    kawasan::Buffer buffer(data);
    while (buffer.remaining() >= 12) {
        const int64_t offset = buffer.readInt64();
        const int32_t msg_size = buffer.readInt32();
        if (msg_size < 0 || buffer.remaining() < static_cast<size_t>(msg_size)) {
            break;
        }
        buffer.readInt32();  // CRC
        const int8_t magic = buffer.readInt8();
        buffer.readInt8();   // attributes
        kawasan::Timestamp timestamp = 0;
        if (magic >= 1) {
            timestamp = buffer.readInt64();
        }
        const int32_t key_len = buffer.readInt32();
        std::optional<std::vector<uint8_t>> key;
        if (key_len >= 0) {
            key = buffer.readBytes(static_cast<size_t>(key_len));
        }
        const int32_t val_len = buffer.readInt32();
        std::optional<std::vector<uint8_t>> value;
        if (val_len >= 0) {
            value = buffer.readBytes(static_cast<size_t>(val_len));
        }
        kawasan::Record record;
        record.timestamp = timestamp;
        record.key = std::move(key);
        record.value = std::move(value);
        batch.addRecord(record);
        (void)offset;
    }
    if (!batch.records().empty()) {
        batches.push_back(std::move(batch));
    }
    return batches;
}

std::map<std::string, int64_t> readCheckpoint(const std::filesystem::path& path) {
    std::map<std::string, int64_t> values;
    std::ifstream in(path);
    std::string line;
    while (std::getline(in, line)) {
        const auto pos = line.find('=');
        if (pos == std::string::npos) {
            continue;
        }
        const auto key = line.substr(0, pos);
        const auto value = line.substr(pos + 1);
        values[key] = std::stoll(value);
    }
    return values;
}

boost::asio::ip::tcp::endpoint makeEndpoint(uint16_t port) {
    return boost::asio::ip::tcp::endpoint(
        boost::asio::ip::make_address("127.0.0.1"), port);
}

}  // namespace

TEST(LogRecoveryTest, RecoversHighWatermarkAndDataAfterRestart) {
    ensureLoggerInitialized();

    const auto log_dir = makeLogDir();
    auto config = makeConfig(log_dir);

    constexpr const char* kTopic = "log-recovery-topic";

    // Start first broker and produce data.
    kawasan::broker::KawasanBroker broker(config);
    broker.start();
    ASSERT_GT(broker.port(), 0);

    {
        boost::asio::io_context io;
        boost::asio::ip::tcp::socket socket(io);
        auto endpoint = makeEndpoint(static_cast<uint16_t>(broker.port()));
        socket.connect(endpoint);

        kawasan::Buffer create_payload;
        kawasan::protocol::RequestHeader create_header(
            kawasan::protocol::ApiKey::CREATE_TOPICS, /*api_version=*/4,
            /*correlation_id=*/1, "log-recovery-test");
        create_header.encode(create_payload);
        kawasan::protocol::CreateTopicsRequest create_request;
        create_request.setTimeoutMs(5000);
        kawasan::protocol::CreatableTopic create_topic;
        create_topic.name = kTopic;
        create_topic.num_partitions = 1;
        create_topic.replication_factor = 1;
        create_request.addTopic(create_topic);
        create_request.encode(create_payload, 4);

        auto create_response_bytes = sendKafkaRequest(socket, create_payload);
        kawasan::Buffer create_response_buffer(create_response_bytes);
        kawasan::protocol::ResponseHeader create_response_header;
        create_response_header.decode(create_response_buffer);
        kawasan::protocol::CreateTopicsResponse create_response;
        create_response.decode(create_response_buffer, 4);

        ASSERT_EQ(create_header.correlationId(), create_response_header.correlationId());
        ASSERT_EQ(create_response.results().size(), 1u);
        EXPECT_EQ(kawasan::ErrorCode::NONE, create_response.results().front().error_code);
        socket.close();
    }

    {
        kawasan::client::ProducerConfig producer_config;
        producer_config.bootstrap_servers =
            "127.0.0.1:" + std::to_string(broker.port());
        producer_config.client_id = "log-recovery-producer";
        kawasan::client::Producer producer(producer_config);
        producer.send(kTopic, "", "one").get();
        producer.send(kTopic, "", "two").get();
        producer.send(kTopic, "", "three").get();
    }

    broker.stop();

    // Restart broker with same log directory.
    kawasan::broker::KawasanBroker restarted(config);
    restarted.start();
    ASSERT_GT(restarted.port(), 0);

    boost::asio::io_context io;
    boost::asio::ip::tcp::socket socket(io);
    auto endpoint = makeEndpoint(static_cast<uint16_t>(restarted.port()));
    socket.connect(endpoint);

    kawasan::Buffer fetch_payload;
    kawasan::protocol::RequestHeader fetch_header(
        kawasan::protocol::ApiKey::FETCH, /*api_version=*/3,
        /*correlation_id=*/42, "log-recovery-test");
    fetch_header.encode(fetch_payload);

    kawasan::protocol::FetchRequest fetch_request;
    fetch_request.setReplicaId(-1);
    fetch_request.setMaxWaitMs(500);
    fetch_request.setMinBytes(1);
    fetch_request.setMaxBytes(1024 * 1024);

    kawasan::protocol::FetchPartition fetch_partition;
    fetch_partition.partition = 0;
    fetch_partition.fetch_offset = 0;
    fetch_partition.partition_max_bytes = 1024 * 1024;

    kawasan::protocol::FetchTopic fetch_topic;
    fetch_topic.topic = kTopic;
    fetch_topic.partitions.push_back(fetch_partition);
    fetch_request.addTopic(fetch_topic);
    fetch_request.encode(fetch_payload, 3);

    auto fetch_response_bytes = sendKafkaRequest(socket, fetch_payload);
    kawasan::Buffer fetch_response_buffer(fetch_response_bytes);
    kawasan::protocol::ResponseHeader fetch_response_header;
    fetch_response_header.decode(fetch_response_buffer);
    kawasan::protocol::FetchResponse fetch_response;
    fetch_response.decode(fetch_response_buffer, 3);

    ASSERT_EQ(fetch_header.correlationId(), fetch_response_header.correlationId());
    ASSERT_EQ(fetch_response.topics().size(), 1u);
    ASSERT_EQ(fetch_response.topics().front().partitions.size(), 1u);
    const auto& partition_response = fetch_response.topics().front().partitions.front();
    EXPECT_EQ(kawasan::ErrorCode::NONE, partition_response.error_code);
    EXPECT_EQ(0, partition_response.partition);
    EXPECT_GE(partition_response.high_watermark, 3);

    auto batches = decodeRecordBatches(partition_response.record_batches);
    std::vector<std::string> values;
    for (const auto& batch : batches) {
        for (const auto& record : batch.records()) {
            if (record.value) {
                values.emplace_back(record.value->begin(), record.value->end());
            }
        }
    }
    EXPECT_EQ(std::vector<std::string>({"one", "two", "three"}), values);

    socket.close();
    restarted.stop();

    const auto checkpoint_path =
        std::filesystem::path(log_dir) / "log-recovery-topic-0" / "checkpoint.meta";
    ASSERT_TRUE(std::filesystem::exists(checkpoint_path));
    auto checkpoint = readCheckpoint(checkpoint_path);
    ASSERT_TRUE(checkpoint.count("log_end_offset"));
    ASSERT_TRUE(checkpoint.count("log_start_offset"));
    ASSERT_TRUE(checkpoint.count("high_watermark"));
    EXPECT_EQ(3, checkpoint["log_end_offset"]);
    EXPECT_EQ(0, checkpoint["log_start_offset"]);
    EXPECT_EQ(3, checkpoint["high_watermark"]);

    std::filesystem::remove_all(log_dir);
}
