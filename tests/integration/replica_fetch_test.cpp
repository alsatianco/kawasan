// M4 e2e: leader-side replica-fetch protocol, driven against an in-process
// KawasanBroker over a real TCP socket + the client producer library. There is
// no second broker; a "follower" is simulated by issuing a Fetch with
// replica_id >= 0, exactly as a real follower broker would.
//
// Covers the leader-side behavior that makes RF>1 correct while keeping a
// single-node partition byte-identical:
//   - A consumer (replica_id = -1) sees records only up to the HIGH WATERMARK;
//     the un-committed tail of a replicated partition is invisible.
//   - A follower fetch (replica_id >= 0) reports its position, which advances
//     the leader's high watermark, after which the consumer sees the record.
//   - An acks=all produce is rejected with NOT_ENOUGH_REPLICAS when the ISR is
//     smaller than min.insync.replicas.
//   - P4: an acks=all produce waiting on the ISR is parked off the IO thread and
//     completes on follower catch-up / ISR shrink, or times out.
#include <arpa/inet.h>
#include <gtest/gtest.h>

#include <array>
#include <boost/asio.hpp>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "kawasan/broker/kawasan_broker.h"
#include "kawasan/broker/replica_manager.h"
#include "kawasan/client/producer.h"
#include "kawasan/common/buffer.h"
#include "kawasan/common/config.h"
#include "kawasan/common/error.h"
#include "kawasan/common/logger.h"
#include "kawasan/common/types.h"
#include "kawasan/protocol/create_topics_request.h"
#include "kawasan/protocol/fetch_request.h"
#include "kawasan/protocol/produce_request.h"
#include "kawasan/protocol/request_header.h"
#include "kawasan/storage/record_batch.h"

namespace asio = boost::asio;

namespace {

constexpr int32_t kBrokerId = 1;
constexpr int32_t kFollowerId = 99;

void ensureLogger() {
    static bool init = false;
    if (!init) {
        kawasan::Logger::init("warn");
        init = true;
    }
}

std::string makeLogDir() {
    const auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
    auto p = std::filesystem::temp_directory_path() / ("kawasan-repl-fetch-" + std::to_string(ts));
    std::filesystem::create_directories(p);
    return p.string();
}

kawasan::Config makeConfig(const std::string& log_dir, int32_t min_insync_replicas) {
    kawasan::Config config;
    config.setInt("broker.id", kBrokerId);
    config.setString("host", "127.0.0.1");
    config.setInt("port", 0);
    config.setInt("raft.port", 0);
    config.setString("log.dirs", log_dir);
    config.setInt("network.io_threads", 1);
    config.setLong("network.max_frame_bytes", 1024 * 1024);
    config.setInt("min.insync.replicas", min_insync_replicas);
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

void createTopic(asio::ip::tcp::socket& socket, const std::string& topic, int32_t corr) {
    kawasan::Buffer payload;
    kawasan::protocol::RequestHeader header(kawasan::protocol::ApiKey::CREATE_TOPICS,
                                            /*api_version=*/4, corr, "m4-test");
    header.encode(payload);
    kawasan::protocol::CreateTopicsRequest request;
    request.setTimeoutMs(5000);
    kawasan::protocol::CreatableTopic t;
    t.name = topic;
    t.num_partitions = 1;
    t.replication_factor = 1;
    request.addTopic(t);
    request.encode(payload, 4);

    auto resp = sendKafkaRequest(socket, payload);
    kawasan::Buffer buf(resp);
    kawasan::protocol::ResponseHeader rh;
    rh.decode(buf);
    kawasan::protocol::CreateTopicsResponse cr;
    cr.decode(buf, 4);
    ASSERT_EQ(cr.results().size(), 1u);
    ASSERT_EQ(kawasan::ErrorCode::NONE, cr.results().front().error_code);
}

struct FetchOutcome {
    kawasan::ErrorCode error;
    kawasan::Offset high_watermark;
    std::vector<std::string> values;  // decoded record values (legacy MessageSet, v3)
};

// Decode the v0-3 legacy MessageSet the broker returns for Fetch<=3.
std::vector<std::string> decodeValues(const std::vector<uint8_t>& data) {
    std::vector<std::string> values;
    kawasan::Buffer buffer(data);
    while (buffer.remaining() >= 12) {
        (void)buffer.readInt64();  // offset
        const int32_t msg_size = buffer.readInt32();
        if (msg_size < 0 || buffer.remaining() < static_cast<size_t>(msg_size)) {
            break;
        }
        buffer.readInt32();  // CRC
        const int8_t magic = buffer.readInt8();
        buffer.readInt8();  // attributes
        if (magic >= 1) {
            buffer.readInt64();  // timestamp
        }
        const int32_t key_len = buffer.readInt32();
        if (key_len >= 0) {
            buffer.readBytes(static_cast<size_t>(key_len));
        }
        const int32_t val_len = buffer.readInt32();
        if (val_len >= 0) {
            auto v = buffer.readBytes(static_cast<size_t>(val_len));
            values.emplace_back(v.begin(), v.end());
        }
    }
    return values;
}

FetchOutcome fetchAs(asio::ip::tcp::socket& socket, const std::string& topic, int32_t replica_id,
                     kawasan::Offset fetch_offset, int32_t corr) {
    kawasan::Buffer payload;
    kawasan::protocol::RequestHeader header(kawasan::protocol::ApiKey::FETCH, /*api_version=*/3,
                                            corr, "m4-test");
    header.encode(payload);

    kawasan::protocol::FetchRequest request;
    request.setReplicaId(replica_id);
    request.setMaxWaitMs(200);
    request.setMinBytes(0);  // don't long-poll waiting for min_bytes in the test
    request.setMaxBytes(1024 * 1024);

    kawasan::protocol::FetchPartition fp;
    fp.partition = 0;
    fp.fetch_offset = fetch_offset;
    fp.partition_max_bytes = 1024 * 1024;
    kawasan::protocol::FetchTopic ft;
    ft.topic = topic;
    ft.partitions.push_back(fp);
    request.addTopic(ft);
    request.encode(payload, 3);

    auto resp = sendKafkaRequest(socket, payload);
    kawasan::Buffer buf(resp);
    kawasan::protocol::ResponseHeader rh;
    rh.decode(buf);
    kawasan::protocol::FetchResponse fr;
    fr.decode(buf, 3);

    FetchOutcome out;
    const auto& pr = fr.topics().front().partitions.front();
    out.error = pr.error_code;
    out.high_watermark = pr.high_watermark;
    out.values = decodeValues(pr.record_batches);
    return out;
}

asio::ip::tcp::socket connect(asio::io_context& io, int32_t port) {
    asio::ip::tcp::socket socket(io);
    socket.connect(
        asio::ip::tcp::endpoint(asio::ip::make_address("127.0.0.1"), static_cast<uint16_t>(port)));
    return socket;
}

// Sends an acks=all Produce (v3) of one record WITHOUT reading the response, so
// the caller can do other work on other connections while it is outstanding.
void sendAcksAllProduce(asio::ip::tcp::socket& socket, const std::string& topic,
                        const std::string& value, int32_t timeout_ms, int32_t corr) {
    kawasan::storage::RecordBatch batch;
    batch.setMagic(2);
    batch.setFirstTimestamp(0);
    kawasan::Record record;
    record.timestamp = 0;
    record.value = std::vector<uint8_t>(value.begin(), value.end());
    batch.addRecord(record);

    kawasan::Buffer payload;
    kawasan::protocol::RequestHeader header(kawasan::protocol::ApiKey::PRODUCE, /*api_version=*/3,
                                            corr, "p4-test");
    header.encode(payload);
    kawasan::protocol::ProduceRequest request;
    request.setAcks(-1);
    request.setTimeoutMs(timeout_ms);
    kawasan::protocol::ProducePartitionData pd;
    pd.partition = 0;
    pd.record_batch = batch.serialize();
    kawasan::protocol::ProduceTopicData td;
    td.topic = topic;
    td.partitions.push_back(pd);
    request.addTopic(td);
    request.encode(payload, 3);

    kawasan::Buffer frame;
    frame.writeInt32(static_cast<int32_t>(payload.size()));
    const auto& pb = payload.vector();
    frame.writeBytes(pb.data(), pb.size());
    const auto& rb = frame.vector();
    asio::write(socket, asio::buffer(rb.data(), rb.size()));
}

kawasan::ErrorCode readProduceError(asio::ip::tcp::socket& socket) {
    std::array<uint8_t, 4> size_bytes{};
    asio::read(socket, asio::buffer(size_bytes));
    uint32_t net = 0;
    std::memcpy(&net, size_bytes.data(), 4);
    std::vector<uint8_t> body(ntohl(net));
    asio::read(socket, asio::buffer(body));
    kawasan::Buffer buf(body);
    kawasan::protocol::ResponseHeader rh;
    rh.decode(buf);
    kawasan::protocol::ProduceResponse pr;
    pr.decode(buf, 3);
    return pr.topics().front().partitions.front().error_code;
}

}  // namespace

// Consumer sees only committed (below-HW) records; a follower fetch advances the
// high watermark, after which the consumer sees the previously-hidden record.
TEST(ReplicaFetchTest, ConsumerBoundedByHighWatermarkFollowerAdvancesIt) {
    ensureLogger();
    const auto log_dir = makeLogDir();
    kawasan::broker::KawasanBroker broker(makeConfig(log_dir, /*min_insync=*/1));
    broker.start();
    ASSERT_GT(broker.port(), 0);

    asio::io_context io;
    auto admin = connect(io, broker.port());
    constexpr const char* kTopic = "repl-topic";
    createTopic(admin, kTopic, /*corr=*/1);

    // Produce two records with acks=1 (leader-only ack, no ISR wait).
    kawasan::client::ProducerConfig pc;
    pc.bootstrap_servers = "127.0.0.1:" + std::to_string(broker.port());
    pc.client_id = "m4-producer";
    pc.acks = 1;
    kawasan::client::Producer producer(pc);

    // m0 is produced while the ISR is just {leader}, so it commits (HW -> 1).
    ASSERT_EQ(producer.send(kTopic, "", "m0").get().offset, 0);

    // Now the partition has a (simulated) second replica in the ISR that has not
    // yet fetched anything — mirroring a freshly-added follower.
    const kawasan::TopicPartition tp{kTopic, 0};
    ASSERT_NE(broker.replicaManager(), nullptr);
    broker.replicaManager()->updateISR(tp, {kBrokerId, kFollowerId});

    // m1 is produced with the follower behind, so the leader must NOT advance
    // the high watermark past it: the record is persisted (offset 1) but not yet
    // committed.
    ASSERT_EQ(producer.send(kTopic, "", "m1").get().offset, 1);

    // A consumer sees only m0 (HW is still 1); m1 is above the watermark.
    auto c1 = fetchAs(admin, kTopic, /*replica_id=*/-1, /*offset=*/0, /*corr=*/10);
    EXPECT_EQ(c1.error, kawasan::ErrorCode::NONE);
    EXPECT_EQ(c1.high_watermark, 1);
    EXPECT_EQ(c1.values, (std::vector<std::string>{"m0"}));

    // The follower fetches from offset 2 (its log-end), reporting that it now
    // holds offsets [0,2). This advances the leader's high watermark to 2.
    auto f = fetchAs(admin, kTopic, /*replica_id=*/kFollowerId, /*offset=*/2, /*corr=*/11);
    EXPECT_EQ(f.error, kawasan::ErrorCode::NONE);
    EXPECT_EQ(f.high_watermark, 2);

    // The consumer now sees both records — m1 became visible once committed.
    auto c2 = fetchAs(admin, kTopic, /*replica_id=*/-1, /*offset=*/0, /*corr=*/12);
    EXPECT_EQ(c2.error, kawasan::ErrorCode::NONE);
    EXPECT_EQ(c2.high_watermark, 2);
    EXPECT_EQ(c2.values, (std::vector<std::string>{"m0", "m1"}));

    admin.close();
    broker.stop();
    std::filesystem::remove_all(log_dir);
}

// acks=all is rejected with NOT_ENOUGH_REPLICAS when the ISR has fewer members
// than min.insync.replicas (here 2 on a single-node broker: ISR = {leader}).
TEST(ReplicaFetchTest, AcksAllRejectedBelowMinInsyncReplicas) {
    ensureLogger();
    const auto log_dir = makeLogDir();
    kawasan::broker::KawasanBroker broker(makeConfig(log_dir, /*min_insync=*/2));
    broker.start();
    ASSERT_GT(broker.port(), 0);

    asio::io_context io;
    auto admin = connect(io, broker.port());
    constexpr const char* kTopic = "min-isr-topic";
    createTopic(admin, kTopic, /*corr=*/1);

    kawasan::client::ProducerConfig pc;
    pc.bootstrap_servers = "127.0.0.1:" + std::to_string(broker.port());
    pc.client_id = "m4-producer";
    pc.acks = -1;  // all
    pc.timeout_ms = 3000;
    kawasan::client::Producer producer(pc);

    kawasan::ErrorCode got = kawasan::ErrorCode::NONE;
    try {
        producer.send(kTopic, "", "x").get();
        FAIL() << "acks=all produce should have been rejected below min.insync.replicas";
    } catch (const kawasan::KawasanException& e) {
        got = e.code();
    }
    EXPECT_EQ(got, kawasan::ErrorCode::NOT_ENOUGH_REPLICAS);

    admin.close();
    broker.stop();
    std::filesystem::remove_all(log_dir);
}

namespace {

// Single IO thread + a topic whose ISR includes a (simulated) follower that has
// not fetched yet, so an acks=all produce must wait for replication.
struct ParkedProduceFixture {
    std::string log_dir = makeLogDir();
    kawasan::broker::KawasanBroker broker{makeConfig(log_dir, /*min_insync=*/1)};
    asio::io_context io;
    std::unique_ptr<asio::ip::tcp::socket> admin;
    const std::string topic;
    const kawasan::TopicPartition tp;

    explicit ParkedProduceFixture(std::string name) : topic(std::move(name)), tp{topic, 0} {
        ensureLogger();
        broker.start();
        admin = std::make_unique<asio::ip::tcp::socket>(connect(io, broker.port()));
        createTopic(*admin, topic, /*corr=*/1);
        // The seed produce registers the partition with the replica manager;
        // then a follower that has not fetched yet joins the ISR.
        kawasan::client::ProducerConfig pc;
        pc.bootstrap_servers = "127.0.0.1:" + std::to_string(broker.port());
        pc.client_id = "p4-seed";
        pc.acks = 1;
        kawasan::client::Producer seed(pc);
        EXPECT_EQ(seed.send(topic, "", "seed").get().offset, 0);
        broker.replicaManager()->updateISR(tp, {kBrokerId, kFollowerId});
    }
    ~ParkedProduceFixture() {
        admin->close();
        broker.stop();
        std::filesystem::remove_all(log_dir);
    }
};

}  // namespace

// P4: an acks=all produce waiting on the ISR must NOT hold the (only) network IO
// thread — a follower fetch on another connection is served meanwhile, advances
// the high watermark, and that completes the parked produce successfully.
TEST(ReplicaFetchTest, AcksAllParkedOffIoThreadCompletesWhenFollowerCatchesUp) {
    ParkedProduceFixture f("p4-follower-catchup");

    auto producer = connect(f.io, f.broker.port());
    const auto start = std::chrono::steady_clock::now();
    sendAcksAllProduce(producer, f.topic, "m1", /*timeout_ms=*/5000, /*corr=*/20);
    // Let the produce reach the broker and append (offset 1) before the follower
    // reports having both records.
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    auto follower = connect(f.io, f.broker.port());
    auto fr = fetchAs(follower, f.topic, /*replica_id=*/kFollowerId, /*offset=*/2, /*corr=*/21);
    EXPECT_EQ(fr.error, kawasan::ErrorCode::NONE);
    EXPECT_EQ(fr.high_watermark, 2);

    EXPECT_EQ(readProduceError(producer), kawasan::ErrorCode::NONE);
    EXPECT_LT(std::chrono::steady_clock::now() - start, std::chrono::seconds(2));
    follower.close();
    producer.close();
}

// P4: with the follower never catching up, the parked produce answers
// REQUEST_TIMED_OUT at its timeout — and other requests are served meanwhile.
TEST(ReplicaFetchTest, AcksAllParkedTimesOutWhenFollowerNeverCatchesUp) {
    ParkedProduceFixture f("p4-timeout");

    auto producer = connect(f.io, f.broker.port());
    const auto start = std::chrono::steady_clock::now();
    sendAcksAllProduce(producer, f.topic, "m1", /*timeout_ms=*/1500, /*corr=*/30);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    auto consumer = connect(f.io, f.broker.port());
    const auto fetch_start = std::chrono::steady_clock::now();
    auto c = fetchAs(consumer, f.topic, /*replica_id=*/-1, /*offset=*/0, /*corr=*/31);
    EXPECT_EQ(c.error, kawasan::ErrorCode::NONE);
    EXPECT_EQ(c.high_watermark, 1);  // m1 is not committed
    EXPECT_LT(std::chrono::steady_clock::now() - fetch_start, std::chrono::milliseconds(1000));

    EXPECT_EQ(readProduceError(producer), kawasan::ErrorCode::REQUEST_TIMED_OUT);
    EXPECT_GE(std::chrono::steady_clock::now() - start, std::chrono::milliseconds(1400));
    consumer.close();
    producer.close();
}

// P4: an ISR shrink that drops the lagging follower commits the parked batch
// immediately (HW advances on the ISR change and wakes the purgatory).
TEST(ReplicaFetchTest, AcksAllParkedCompletesWhenIsrShrinks) {
    ParkedProduceFixture f("p4-isr-shrink");

    auto producer = connect(f.io, f.broker.port());
    const auto start = std::chrono::steady_clock::now();
    sendAcksAllProduce(producer, f.topic, "m1", /*timeout_ms=*/10000, /*corr=*/40);
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    f.broker.replicaManager()->updateISR(f.tp, {kBrokerId});

    EXPECT_EQ(readProduceError(producer), kawasan::ErrorCode::NONE);
    EXPECT_LT(std::chrono::steady_clock::now() - start, std::chrono::seconds(2));
    EXPECT_EQ(f.broker.replicaManager()->getHighWatermark(f.tp).value_or(-1), 2);
    producer.close();
}
