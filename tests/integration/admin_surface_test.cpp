// CM-5: admin surface truthing, driven against an in-process KawasanBroker over
// a real TCP socket. Covers the two behavioral changes:
//   - DescribeLogDirs reports the real on-disk size per partition, groups
//     partitions under one entry per topic, and honors the topic filter.
//   - CreatePartitions validates (unknown topic, non-increase) and honors
//     validate_only as a true dry-run (validate but do not apply).
#include <arpa/inet.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <boost/asio.hpp>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <string>
#include <vector>

#include "kawasan/broker/kawasan_broker.h"
#include "kawasan/client/producer.h"
#include "kawasan/common/buffer.h"
#include "kawasan/common/config.h"
#include "kawasan/common/logger.h"
#include "kawasan/common/types.h"
#include "kawasan/protocol/admin_misc_requests.h"
#include "kawasan/protocol/create_topics_request.h"
#include "kawasan/protocol/request_header.h"

namespace asio = boost::asio;
namespace proto = kawasan::protocol;

namespace {

void ensureLogger() {
    static bool init = false;
    if (!init) {
        kawasan::Logger::init("warn");
        init = true;
    }
}

std::string makeLogDir() {
    const auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
    auto p = std::filesystem::temp_directory_path() / ("kawasan-admin-surf-" + std::to_string(ts));
    std::filesystem::create_directories(p);
    return p.string();
}

kawasan::Config makeConfig(const std::string& log_dir) {
    kawasan::Config config;
    config.setInt("broker.id", 1);
    config.setString("host", "127.0.0.1");
    config.setInt("port", 0);
    config.setInt("raft.port", 0);
    config.setString("log.dirs", log_dir);
    config.setInt("network.io_threads", 1);
    config.setLong("network.max_frame_bytes", 4 * 1024 * 1024);
    return config;
}

std::vector<uint8_t> sendKafkaRequest(asio::ip::tcp::socket& socket, kawasan::Buffer& payload) {
    kawasan::Buffer frame;
    frame.writeInt32(static_cast<int32_t>(payload.size()));
    const auto& pb = payload.vector();
    frame.writeBytes(pb.data(), pb.size());
    const auto& rb = frame.vector();
    asio::write(socket, asio::buffer(rb.data(), rb.size()));

    std::array<uint8_t, 4> size_bytes{};
    asio::read(socket, asio::buffer(size_bytes));
    uint32_t net = 0;
    std::memcpy(&net, size_bytes.data(), 4);
    std::vector<uint8_t> body(ntohl(net));
    if (!body.empty()) {
        asio::read(socket, asio::buffer(body));
    }
    return body;
}

asio::ip::tcp::socket connect(asio::io_context& io, int32_t port) {
    asio::ip::tcp::socket socket(io);
    socket.connect(
        asio::ip::tcp::endpoint(asio::ip::make_address("127.0.0.1"), static_cast<uint16_t>(port)));
    return socket;
}

void createTopic(asio::ip::tcp::socket& socket, const std::string& topic, int32_t partitions,
                 int32_t corr) {
    kawasan::Buffer payload;
    proto::RequestHeader header(proto::ApiKey::CREATE_TOPICS, /*api_version=*/4, corr, "cm5-test");
    header.encode(payload);
    proto::CreateTopicsRequest request;
    request.setTimeoutMs(5000);
    proto::CreatableTopic t;
    t.name = topic;
    t.num_partitions = partitions;
    t.replication_factor = 1;
    request.addTopic(t);
    request.encode(payload, 4);

    auto resp = sendKafkaRequest(socket, payload);
    kawasan::Buffer buf(resp);
    proto::ResponseHeader rh;
    rh.decode(buf);
    proto::CreateTopicsResponse cr;
    cr.decode(buf, 4);
    ASSERT_EQ(cr.results().size(), 1u);
    ASSERT_EQ(kawasan::ErrorCode::NONE, cr.results().front().error_code);
}

proto::DescribeLogDirsResponse describeLogDirs(asio::ip::tcp::socket& socket,
                                               const proto::DescribeLogDirsRequest& request,
                                               int32_t corr) {
    kawasan::Buffer payload;
    proto::RequestHeader header(proto::ApiKey::DESCRIBE_LOG_DIRS, /*api_version=*/0, corr,
                                "cm5-test");
    header.encode(payload);
    request.encode(payload, 0);

    auto resp = sendKafkaRequest(socket, payload);
    kawasan::Buffer buf(resp);
    proto::ResponseHeader rh;
    rh.decode(buf);
    proto::DescribeLogDirsResponse dr;
    dr.decode(buf, 0);
    return dr;
}

kawasan::ErrorCode createPartitions(asio::ip::tcp::socket& socket, const std::string& topic,
                                    int32_t count, bool validate_only, int32_t corr) {
    kawasan::Buffer payload;
    proto::RequestHeader header(proto::ApiKey::CREATE_PARTITIONS, /*api_version=*/0, corr,
                                "cm5-test");
    header.encode(payload);
    proto::CreatePartitionsRequest request;
    request.setTimeoutMs(5000);
    request.setValidateOnly(validate_only);
    proto::CreatePartitionsRequest::TopicSpec spec;
    spec.topic = topic;
    spec.count = count;
    request.addTopic(spec);
    request.encode(payload, 0);

    auto resp = sendKafkaRequest(socket, payload);
    kawasan::Buffer buf(resp);
    proto::ResponseHeader rh;
    rh.decode(buf);
    proto::CreatePartitionsResponse cr;
    cr.decode(buf, 0);
    EXPECT_EQ(cr.results().size(), 1u);
    return cr.results().front().error_code;
}

// Find a topic's info in a DescribeLogDirs response (searches the single log dir).
const proto::DescribeLogDirsResponse::TopicInfo* findTopic(const proto::DescribeLogDirsResponse& r,
                                                           const std::string& topic) {
    // decode() exposes the log dirs via a getter added below; iterate them.
    for (const auto& dir : r.logDirs()) {
        for (const auto& ti : dir.topics) {
            if (ti.topic == topic) {
                return &ti;
            }
        }
    }
    return nullptr;
}

}  // namespace

TEST(AdminSurfaceTest, DescribeLogDirsReportsRealSizeGroupedByTopicAndHonorsFilter) {
    ensureLogger();
    const auto log_dir = makeLogDir();
    kawasan::broker::KawasanBroker broker(makeConfig(log_dir));
    broker.start();
    ASSERT_GT(broker.port(), 0);

    asio::io_context io;
    auto admin = connect(io, broker.port());
    constexpr const char* kTopic = "logdir-topic";
    createTopic(admin, kTopic, /*partitions=*/2, /*corr=*/1);

    // Produce records so the topic has real on-disk bytes (a fixed key lands
    // them on one of the two partitions).
    kawasan::client::ProducerConfig pc;
    pc.bootstrap_servers = "127.0.0.1:" + std::to_string(broker.port());
    pc.client_id = "cm5-producer";
    pc.acks = 1;
    kawasan::client::Producer producer(pc);
    for (int i = 0; i < 5; ++i) {
        producer.send(kTopic, "k", "some-value-" + std::to_string(i)).get();
    }

    // fetch_all: the topic appears once with both partitions, each with real size.
    proto::DescribeLogDirsRequest all;  // default fetch_all
    auto r_all = describeLogDirs(admin, all, /*corr=*/10);
    const auto* ti = findTopic(r_all, kTopic);
    ASSERT_NE(ti, nullptr) << "topic must appear in DescribeLogDirs";
    EXPECT_EQ(ti->partitions.size(), 2u) << "both partitions grouped under one topic entry";
    int64_t total = 0;
    for (const auto& pi : ti->partitions) {
        EXPECT_GE(pi.size_bytes, 0);
        EXPECT_EQ(pi.offset_lag, 0) << "single-node leader is caught up (HW==LEO)";
        EXPECT_FALSE(pi.is_future);
        total += pi.size_bytes;
    }
    // Records land on one partition (fixed key), so the topic's total on-disk
    // size — not necessarily every partition — must be non-zero.
    EXPECT_GT(total, 0) << "produced records must show up as real on-disk bytes";

    // Filter to a single partition of our topic: internal topics must be absent.
    proto::DescribeLogDirsRequest filtered;
    proto::DescribeLogDirsRequest::Topic ft;
    ft.topic = kTopic;
    ft.partitions = {0};
    filtered.addTopic(ft);
    auto r_filtered = describeLogDirs(admin, filtered, /*corr=*/11);
    const auto* fti = findTopic(r_filtered, kTopic);
    ASSERT_NE(fti, nullptr);
    EXPECT_EQ(fti->partitions.size(), 1u) << "only the requested partition";
    EXPECT_EQ(fti->partitions.front().partition, 0);
    EXPECT_EQ(findTopic(r_filtered, "__consumer_offsets"), nullptr)
        << "filter must exclude unrequested (internal) topics";

    admin.close();
    broker.stop();
    std::filesystem::remove_all(log_dir);
}

TEST(AdminSurfaceTest, CreatePartitionsValidatesAndHonorsValidateOnly) {
    ensureLogger();
    const auto log_dir = makeLogDir();
    kawasan::broker::KawasanBroker broker(makeConfig(log_dir));
    broker.start();
    ASSERT_GT(broker.port(), 0);

    asio::io_context io;
    auto admin = connect(io, broker.port());
    constexpr const char* kTopic = "cp-topic";
    createTopic(admin, kTopic, /*partitions=*/1, /*corr=*/1);

    // Unknown topic -> UNKNOWN_TOPIC_OR_PARTITION.
    EXPECT_EQ(createPartitions(admin, "no-such-topic", 3, /*validate_only=*/false, 10),
              kawasan::ErrorCode::UNKNOWN_TOPIC_OR_PARTITION);

    // Non-increase (count <= current) -> INVALID_PARTITIONS.
    EXPECT_EQ(createPartitions(admin, kTopic, 1, /*validate_only=*/false, 11),
              kawasan::ErrorCode::INVALID_PARTITIONS);

    // validate_only=true passes validation but must NOT apply...
    EXPECT_EQ(createPartitions(admin, kTopic, 3, /*validate_only=*/true, 12),
              kawasan::ErrorCode::NONE);
    // ...proven by the real increase to 3 still succeeding (would be
    // INVALID_PARTITIONS if the dry-run had already applied it).
    EXPECT_EQ(createPartitions(admin, kTopic, 3, /*validate_only=*/false, 13),
              kawasan::ErrorCode::NONE);
    // Now it really has 3 partitions, so a repeat is a non-increase.
    EXPECT_EQ(createPartitions(admin, kTopic, 3, /*validate_only=*/false, 14),
              kawasan::ErrorCode::INVALID_PARTITIONS);

    admin.close();
    broker.stop();
    std::filesystem::remove_all(log_dir);
}
