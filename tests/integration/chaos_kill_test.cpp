// M9: real SIGKILL without destructors; exact records, producer sequences and
// transactional epochs must survive. The parent never starts broker threads.
#include <fcntl.h>
#include <gtest/gtest.h>
#include <poll.h>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include <array>
#include <boost/asio.hpp>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <map>
#include <stdexcept>
#include <string>
#include <vector>

#include "kawasan/broker/kawasan_broker.h"
#include "kawasan/common/buffer.h"
#include "kawasan/common/config.h"
#include "kawasan/common/logger.h"
#include "kawasan/protocol/create_topics_request.h"
#include "kawasan/protocol/fetch_request.h"
#include "kawasan/protocol/produce_request.h"
#include "kawasan/protocol/request_header.h"
#include "kawasan/storage/record_batch.h"

namespace {
namespace asio = boost::asio;
using kawasan::Buffer;
using kawasan::ErrorCode;
using kawasan::protocol::ApiKey;
constexpr const char* TOPIC = "chaos-kill";

class CrashBroker {
public:
    CrashBroker() {
        dir_ = std::filesystem::temp_directory_path() /
               ("kawasan-chaos-kill-" +
                std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        std::filesystem::create_directories(dir_);
    }
    ~CrashBroker() {
        killAndWait();
        if (testing::Test::HasFailure()) {
            std::cerr << "Crash evidence retained at " << dir_ << '\n';
        } else {
            std::filesystem::remove_all(dir_);
        }
    }
    uint16_t start() {
        int ready[2];
        if (pipe(ready) != 0)
            throw std::runtime_error("pipe failed");
        pid_ = fork();
        if (pid_ == 0) {
            close(ready[0]);
            int fd = open((dir_ / "broker.log").c_str(), O_WRONLY | O_CREAT | O_APPEND, 0600);
            if (fd >= 0) {
                dup2(fd, STDOUT_FILENO);
                dup2(fd, STDERR_FILENO);
                close(fd);
            }
            try {
                kawasan::Logger::init("warn");
                kawasan::Config config;
                config.setInt("broker.id", 0);
                config.setString("host", "127.0.0.1");
                config.setInt("port", 0);
                config.setInt("raft.port", 0);
                config.setString("log.dirs", dir_.string());
                config.setString("log.durability", "sync");
                config.setBool("monitoring.enabled", false);
                config.setInt("network.io_threads", 1);
                config.setInt("offsets.topic.num.partitions", 1);
                config.setInt("transaction.state.topic.num.partitions", 1);
                config.setLong("producer.state.snapshot.interval.ms", 25);
                kawasan::broker::KawasanBroker broker(config);
                broker.start();
                const auto port = static_cast<uint16_t>(broker.port());
                if (write(ready[1], &port, sizeof(port)) != sizeof(port))
                    _exit(3);
                close(ready[1]);
                while (true)
                    pause();
            } catch (...) {
                _exit(2);
            }
        }
        close(ready[1]);
        if (pid_ < 0) {
            close(ready[0]);
            throw std::runtime_error("fork failed");
        }
        pollfd wait{ready[0], POLLIN, 0};
        uint16_t port = 0;
        const bool success = poll(&wait, 1, 15000) > 0 &&
                             read(ready[0], &port, sizeof(port)) == sizeof(port) && port != 0;
        close(ready[0]);
        if (!success)
            throw std::runtime_error("child broker did not become ready");
        return port;
    }
    void killAndWait() {
        if (pid_ > 0) {
            kill(pid_, SIGKILL);
            int status = 0;
            while (waitpid(pid_, &status, 0) < 0 && errno == EINTR) {
            }
            pid_ = -1;
        }
    }

private:
    std::filesystem::path dir_;
    pid_t pid_ = -1;
};

Buffer request(ApiKey key, int16_t version) {
    Buffer payload;
    kawasan::protocol::RequestHeader(key, version, 1, "m9-kill-test").encode(payload);
    return payload;
}

Buffer roundTrip(asio::ip::tcp::socket& socket, const Buffer& payload) {
    Buffer frame;
    frame.writeInt32(static_cast<int32_t>(payload.size()));
    frame.writeBytes(payload.data(), payload.size());
    asio::write(socket, asio::buffer(frame.vector()));
    std::array<uint8_t, 4> length{};
    asio::read(socket, asio::buffer(length));
    Buffer size(std::vector<uint8_t>(length.begin(), length.end()));
    const int32_t count = size.readInt32();
    if (count < 4 || count > (8 << 20))
        throw std::runtime_error("invalid response frame");
    std::vector<uint8_t> bytes(count);
    asio::read(socket, asio::buffer(bytes));
    Buffer response(std::move(bytes));
    kawasan::protocol::ResponseHeader header;
    header.decode(response);
    if (header.correlationId() != 1)
        throw std::runtime_error("wrong correlation ID");
    return response;
}

asio::ip::tcp::socket connect(asio::io_context& io, uint16_t port) {
    asio::ip::tcp::socket socket(io);
    socket.connect({asio::ip::make_address("127.0.0.1"), port});
    return socket;
}

void createTopic(asio::ip::tcp::socket& socket) {
    auto payload = request(ApiKey::CREATE_TOPICS, 4);
    kawasan::protocol::CreateTopicsRequest create;
    kawasan::protocol::CreatableTopic topic;
    topic.name = TOPIC;
    topic.num_partitions = 1;
    topic.replication_factor = 1;
    create.addTopic(topic);
    create.encode(payload, 4);
    auto response = roundTrip(socket, payload);
    kawasan::protocol::CreateTopicsResponse result;
    result.decode(response, 4);
    ASSERT_EQ(result.results().size(), 1u);
    ASSERT_EQ(result.results().front().error_code, ErrorCode::NONE);
}

std::pair<int64_t, int16_t> initTxn(asio::ip::tcp::socket& socket) {
    auto payload = request(ApiKey::INIT_PRODUCER_ID, 0);
    payload.writeString("m9-durable-epoch");
    payload.writeInt32(60000);
    auto response = roundTrip(socket, payload);
    response.readInt32();
    EXPECT_EQ(static_cast<ErrorCode>(response.readInt16()), ErrorCode::NONE);
    const auto pid = response.readInt64();
    const auto epoch = response.readInt16();
    return {pid, epoch};
}

kawasan::protocol::ProducePartitionResponse produce(asio::ip::tcp::socket& socket, int sequence) {
    kawasan::storage::RecordBatch batch;
    batch.setMagic(2);
    batch.setProducerId(4242);
    batch.setProducerEpoch(0);
    batch.setBaseSequence(sequence);
    const std::string key = "run:" + std::to_string(sequence);
    kawasan::Record record;
    record.key = std::vector<uint8_t>(key.begin(), key.end());
    record.value = record.key;
    record.timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
                           std::chrono::system_clock::now().time_since_epoch())
                           .count();
    batch.setFirstTimestamp(record.timestamp);
    batch.setMaxTimestamp(record.timestamp);
    batch.addRecord(record);
    Buffer bytes;
    batch.encode(bytes);
    kawasan::protocol::ProduceRequest produce;
    produce.setAcks(-1);
    produce.addTopic({TOPIC, {{0, bytes.vector()}}});
    auto payload = request(ApiKey::PRODUCE, 3);
    produce.encode(payload, 3);
    auto response = roundTrip(socket, payload);
    kawasan::protocol::ProduceResponse result;
    result.decode(response, 3);
    if (result.topics().size() != 1 || result.topics()[0].partitions.size() != 1)
        throw std::runtime_error("invalid produce response");
    return result.topics()[0].partitions[0];
}

TEST(ChaosKillTest, AckedOffsetsProducerReplayAndTxnEpochSurviveSigkill) {
    CrashBroker child;
    std::map<int64_t, std::string> ledger;
    std::pair<int64_t, int16_t> txn;
    {
        const auto port = child.start();
        asio::io_context io;
        auto socket = connect(io, port);
        createTopic(socket);
        txn = initTxn(socket);
        for (int sequence = 0; sequence < 50; ++sequence) {
            auto result = produce(socket, sequence);
            ASSERT_EQ(result.error_code, ErrorCode::NONE);
            ASSERT_EQ(result.base_offset, sequence);
            ledger.emplace(result.base_offset, "run:" + std::to_string(sequence));
        }
    }
    child.killAndWait();  // No stop(), snapshot-on-shutdown, or RocksDB destructor.
    {
        const auto port = child.start();
        asio::io_context io;
        auto socket = connect(io, port);
        auto recovered_txn = initTxn(socket);
        ASSERT_EQ(recovered_txn.first, txn.first);
        ASSERT_EQ(recovered_txn.second, txn.second + 1);
        auto duplicate = produce(socket, 49);
        ASSERT_EQ(duplicate.error_code, ErrorCode::NONE);
        ASSERT_EQ(duplicate.base_offset, 49);  // Not a second copy at offset 50.
        auto next = produce(socket, 50);
        ASSERT_EQ(next.error_code, ErrorCode::NONE);
        ASSERT_EQ(next.base_offset, 50);
        ledger.emplace(50, "run:50");
        kawasan::protocol::FetchRequest fetch;
        fetch.setIsolationLevel(1);
        fetch.setMaxWaitMs(0);
        kawasan::protocol::FetchTopic fetch_topic;
        fetch_topic.topic = TOPIC;
        kawasan::protocol::FetchPartition fetch_partition;
        fetch_partition.partition = 0;
        fetch_partition.fetch_offset = 0;
        fetch_partition.partition_max_bytes = 1 << 20;
        fetch_topic.partitions.push_back(fetch_partition);
        fetch.addTopic(fetch_topic);
        auto payload = request(ApiKey::FETCH, 4);
        fetch.encode(payload, 4);
        auto response = roundTrip(socket, payload);
        kawasan::protocol::FetchResponse result;
        result.decode(response, 4);
        ASSERT_EQ(result.topics().size(), 1u);
        ASSERT_EQ(result.topics()[0].partitions.size(), 1u);
        const auto& partition = result.topics()[0].partitions[0];
        ASSERT_EQ(partition.error_code, ErrorCode::NONE);
        ASSERT_EQ(partition.high_watermark, 51);
        Buffer batches(partition.record_batches);
        std::map<int64_t, std::string> actual;
        while (batches.remaining() > 0) {
            kawasan::storage::RecordBatch batch;
            batch.decode(batches);
            for (size_t i = 0; i < batch.records().size(); ++i) {
                const auto& record = batch.records()[i];
                ASSERT_TRUE(record.value.has_value());
                actual.emplace(batch.baseOffset() + i,
                               std::string(record.value->begin(), record.value->end()));
            }
        }
        ASSERT_EQ(actual, ledger);
    }
}
}  // namespace
