// M2 e2e: producer-epoch fencing + transaction-timeout auto-abort, driven over
// a real TCP socket against an in-process KawasanBroker (no external client).
// Requests/responses are hand-encoded/decoded at the v0 wire layout so the test
// exercises the exact broker dispatch + handler path a real client hits.
//
// Covers:
//   - InitProducerId for a known transactional_id REUSES the producer_id and
//     BUMPS the epoch (the fencing mechanism).
//   - A stale-epoch AddPartitionsToTxn is rejected with INVALID_PRODUCER_EPOCH,
//     while the current epoch is accepted.
//   - A transaction left Ongoing past transaction.timeout.ms is auto-aborted by
//     the sweep, so a later EndTxn(commit) returns INVALID_TXN_STATE.
#include <arpa/inet.h>
#include <gtest/gtest.h>

#include <array>
#include <boost/asio.hpp>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <string>
#include <thread>
#include <vector>

#include "kawasan/broker/kawasan_broker.h"
#include "kawasan/common/buffer.h"
#include "kawasan/common/config.h"
#include "kawasan/common/logger.h"
#include "kawasan/common/types.h"
#include "kawasan/protocol/api_keys.h"
#include "kawasan/protocol/request_header.h"

using kawasan::Buffer;
using kawasan::ErrorCode;
using kawasan::protocol::ApiKey;
using kawasan::protocol::RequestHeader;
using kawasan::protocol::ResponseHeader;
namespace asio = boost::asio;

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
    auto p = std::filesystem::temp_directory_path() / ("kawasan-txn-fence-" + std::to_string(ts));
    std::filesystem::create_directories(p);
    return p.string();
}

kawasan::Config makeConfig(const std::string& log_dir, int64_t sweep_ms) {
    kawasan::Config config;
    config.setInt("broker.id", 1);
    config.setString("host", "127.0.0.1");
    config.setInt("port", 0);
    config.setInt("raft.port", 0);
    config.setString("log.dirs", log_dir);
    config.setInt("network.io_threads", 1);
    config.setLong("network.max_frame_bytes", 1024 * 1024);
    config.setLong("transaction.abort.timed.out.transaction.cleanup.interval.ms", sweep_ms);
    return config;
}

// Frame a payload (4-byte big-endian length prefix), send, read the framed
// response body back.
std::vector<uint8_t> roundTrip(asio::ip::tcp::socket& socket, const Buffer& payload) {
    Buffer frame;
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
    asio::read(socket, asio::buffer(body));
    return body;
}

Buffer header(ApiKey key, int16_t version, int32_t corr) {
    Buffer b;
    RequestHeader h(key, version, corr, "m2-test");
    h.encode(b);
    return b;
}

// --- InitProducerId v0: req {NULLABLE_STRING txn, INT32 timeout};
//     resp {INT32 throttle, INT16 error, INT64 pid, INT16 epoch} ---
struct InitResult {
    ErrorCode error;
    int64_t producer_id;
    int16_t producer_epoch;
};
InitResult initProducerId(asio::ip::tcp::socket& s, int32_t corr, const std::string& txn,
                          int32_t timeout_ms) {
    Buffer p = header(ApiKey::INIT_PRODUCER_ID, 0, corr);
    p.writeString(txn);
    p.writeInt32(timeout_ms);
    Buffer resp(roundTrip(s, p));
    ResponseHeader rh;
    rh.decode(resp);
    InitResult r;
    (void)resp.readInt32();  // throttle
    r.error = static_cast<ErrorCode>(resp.readInt16());
    r.producer_id = resp.readInt64();
    r.producer_epoch = resp.readInt16();
    return r;
}

// --- AddPartitionsToTxn v0: req {STRING txn, INT64 pid, INT16 epoch,
//     ARRAY[STRING topic, ARRAY[INT32 partition]]};
//     resp {INT32 throttle, ARRAY[STRING topic, ARRAY[INT32 idx, INT16 error]]} ---
ErrorCode addPartitions(asio::ip::tcp::socket& s, int32_t corr, const std::string& txn, int64_t pid,
                        int16_t epoch, const std::string& topic, int32_t partition) {
    Buffer p = header(ApiKey::ADD_PARTITIONS_TO_TXN, 0, corr);
    p.writeString(txn);
    p.writeInt64(pid);
    p.writeInt16(epoch);
    p.writeInt32(1);  // 1 topic
    p.writeString(topic);
    p.writeInt32(1);  // 1 partition
    p.writeInt32(partition);
    Buffer resp(roundTrip(s, p));
    ResponseHeader rh;
    rh.decode(resp);
    (void)resp.readInt32();  // throttle
    const int32_t tc = resp.readInt32();
    EXPECT_EQ(tc, 1);
    (void)resp.readString();  // topic
    const int32_t pc = resp.readInt32();
    EXPECT_EQ(pc, 1);
    (void)resp.readInt32();  // partition index
    return static_cast<ErrorCode>(resp.readInt16());
}

// --- EndTxn v0: req {STRING txn, INT64 pid, INT16 epoch, INT8 committed};
//     resp {INT32 throttle, INT16 error} ---
ErrorCode endTxn(asio::ip::tcp::socket& s, int32_t corr, const std::string& txn, int64_t pid,
                 int16_t epoch, bool committed) {
    Buffer p = header(ApiKey::END_TXN, 0, corr);
    p.writeString(txn);
    p.writeInt64(pid);
    p.writeInt16(epoch);
    p.writeInt8(committed ? 1 : 0);
    Buffer resp(roundTrip(s, p));
    ResponseHeader rh;
    rh.decode(resp);
    (void)resp.readInt32();  // throttle
    return static_cast<ErrorCode>(resp.readInt16());
}

asio::ip::tcp::socket connect(asio::io_context& io, uint16_t port) {
    asio::ip::tcp::socket s(io);
    s.connect(asio::ip::tcp::endpoint(asio::ip::make_address("127.0.0.1"), port));
    return s;
}

}  // namespace

TEST(TransactionFencingTest, InitProducerIdBumpsEpochAndFencesStaleProducer) {
    ensureLogger();
    const auto dir = makeLogDir();
    kawasan::broker::KawasanBroker broker(makeConfig(dir, /*sweep_ms=*/10000));
    broker.start();
    ASSERT_GT(broker.port(), 0);

    asio::io_context io;
    auto s = connect(io, static_cast<uint16_t>(broker.port()));
    const std::string txn = "fence-x";

    // First InitProducerId → epoch 0.
    auto first = initProducerId(s, 1, txn, 60000);
    ASSERT_EQ(first.error, ErrorCode::NONE);
    EXPECT_EQ(first.producer_epoch, 0);

    // Second InitProducerId for the SAME txn id → same pid, epoch bumped to 1.
    auto second = initProducerId(s, 2, txn, 60000);
    ASSERT_EQ(second.error, ErrorCode::NONE);
    EXPECT_EQ(second.producer_id, first.producer_id) << "producer_id must be reused";
    EXPECT_EQ(second.producer_epoch, 1) << "epoch must bump on re-init (fencing)";

    // A stale-epoch (0) AddPartitions from the fenced producer is rejected.
    EXPECT_EQ(addPartitions(s, 3, txn, first.producer_id, /*epoch=*/0, "ft", 0),
              ErrorCode::INVALID_PRODUCER_EPOCH);

    // The current epoch (1) is accepted.
    EXPECT_EQ(addPartitions(s, 4, txn, second.producer_id, /*epoch=*/1, "ft", 0), ErrorCode::NONE);

    broker.stop();
    std::filesystem::remove_all(dir);
}

TEST(TransactionFencingTest, HungTransactionIsAutoAbortedByTimeout) {
    ensureLogger();
    const auto dir = makeLogDir();
    // Fast sweep so the test doesn't wait long.
    kawasan::broker::KawasanBroker broker(makeConfig(dir, /*sweep_ms=*/500));
    broker.start();
    ASSERT_GT(broker.port(), 0);

    asio::io_context io;
    auto s = connect(io, static_cast<uint16_t>(broker.port()));
    const std::string txn = "hung-1";

    // transaction.timeout.ms = 1000; open a txn and then never commit.
    auto init = initProducerId(s, 1, txn, /*timeout_ms=*/1000);
    ASSERT_EQ(init.error, ErrorCode::NONE);
    ASSERT_EQ(addPartitions(s, 2, txn, init.producer_id, init.producer_epoch, "ht", 0),
              ErrorCode::NONE);

    // Wait past timeout (1000ms) + sweep interval (500ms) with margin.
    std::this_thread::sleep_for(std::chrono::milliseconds(2500));

    // The sweep has aborted the txn; a late commit must not succeed.
    EXPECT_EQ(endTxn(s, 3, txn, init.producer_id, init.producer_epoch, /*committed=*/true),
              ErrorCode::INVALID_TXN_STATE);

    broker.stop();
    std::filesystem::remove_all(dir);
}
