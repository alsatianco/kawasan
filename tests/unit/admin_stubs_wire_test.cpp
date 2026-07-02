// CM-2: golden-bytes lock for the four v0-flexible admin APIs.
//
// DescribeProducers (61), DescribeTransactions (65), ListTransactions (66),
// and AlterPartition (56) are flexible from v0 (they post-date KIP-482), so
// their BODIES must use compact strings/arrays and tagged fields — matching
// the flexible request-header gating in request_header.cpp. The expected
// byte sequences below are hand-derived from the Kafka 4.x message schemas
// (clients/src/main/resources/common/message/*.json); the client-matrix CI
// (CM-1) validates the same wire shapes end-to-end against real clients.
#include <gtest/gtest.h>

#include <vector>

#include "kawasan/common/buffer.h"
#include "kawasan/protocol/admin_stubs.h"

using kawasan::Buffer;
using namespace kawasan::protocol;

namespace {

std::vector<uint8_t> snapshot(const Buffer& b) {
    return std::vector<uint8_t>(b.data(), b.data() + b.size());
}

template <typename T>
std::vector<uint8_t> encodeBytes(const T& msg, int16_t v) {
    Buffer buf;
    msg.encode(buf, v);
    return snapshot(buf);
}

template <typename T>
T decodeBytes(const std::vector<uint8_t>& bytes, int16_t v) {
    Buffer buf(bytes);
    T msg;
    msg.decode(buf, v);
    return msg;
}

// Decode golden bytes then re-encode: must reproduce the golden bytes.
template <typename T>
void expectRoundTrip(const std::vector<uint8_t>& golden, int16_t v) {
    auto msg = decodeBytes<T>(golden, v);
    EXPECT_EQ(encodeBytes(msg, v), golden);
}

}  // namespace

// ---- DescribeProducers (61) v0 ----

// Topics: [{name:"t", partition_indexes:[0]}]
const std::vector<uint8_t> kDescribeProducersRequestV0 = {
    0x02,                    // topics: compact array, count 1
    0x02, 0x74,              //   name "t"
    0x02,                    //   partition_indexes: compact array, count 1
    0x00, 0x00, 0x00, 0x00,  //     partition 0
    0x00,                    //   topic tagged fields
    0x00,                    // request tagged fields
};

TEST(AdminStubsWire, DescribeProducersRequestV0Decode) {
    auto req = decodeBytes<DescribeProducersRequest>(kDescribeProducersRequestV0, 0);
    ASSERT_EQ(req.topics().size(), 1u);
    EXPECT_EQ(req.topics()[0].topic, "t");
    ASSERT_EQ(req.topics()[0].partitions.size(), 1u);
    EXPECT_EQ(req.topics()[0].partitions[0], 0);
}

TEST(AdminStubsWire, DescribeProducersRequestV0RoundTrip) {
    expectRoundTrip<DescribeProducersRequest>(kDescribeProducersRequestV0, 0);
}

// throttle 0; topics [{name:"t", partitions:[{index 0, error 0, msg null,
// producers:[{pid 5, epoch 1, last_seq 7, last_ts 100, coord_epoch 0,
// txn_start -1}]}]}]
const std::vector<uint8_t> kDescribeProducersResponseV0 = {
    0x00, 0x00, 0x00, 0x00,                          // throttle_time_ms
    0x02,                                            // topics count 1
    0x02, 0x74,                                      //   name "t"
    0x02,                                            //   partitions count 1
    0x00, 0x00, 0x00, 0x00,                          //     partition_index 0
    0x00, 0x00,                                      //     error_code 0
    0x00,                                            //     error_message null
    0x02,                                            //     active_producers count 1
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05,  //       producer_id 5
    0x00, 0x00, 0x00, 0x01,                          //       producer_epoch 1
    0x00, 0x00, 0x00, 0x07,                          //       last_sequence 7
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x64,  //       last_timestamp 100
    0x00, 0x00, 0x00, 0x00,                          //       coordinator_epoch 0
    0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,  //       current_txn_start_offset -1
    0x00,                                            //       producer tagged fields
    0x00,                                            //     partition tagged fields
    0x00,                                            //   topic tagged fields
    0x00,                                            // response tagged fields
};

TEST(AdminStubsWire, DescribeProducersResponseV0Encode) {
    DescribeProducersResponse resp;
    resp.setThrottleTimeMs(0);
    DescribeProducersResponse::TopicResult t;
    t.topic = "t";
    DescribeProducersResponse::PartitionResult p;
    p.partition = 0;
    DescribeProducersResponse::ActiveProducer ap;
    ap.producer_id = 5;
    ap.producer_epoch = 1;
    ap.last_sequence = 7;
    ap.last_timestamp = 100;
    ap.coordinator_epoch = 0;
    ap.current_txn_start_offset = -1;
    p.active_producers.push_back(ap);
    t.partitions.push_back(std::move(p));
    resp.addTopic(std::move(t));

    EXPECT_EQ(encodeBytes(resp, 0), kDescribeProducersResponseV0);
}

TEST(AdminStubsWire, DescribeProducersResponseV0RoundTrip) {
    expectRoundTrip<DescribeProducersResponse>(kDescribeProducersResponseV0, 0);
}

// ---- ListTransactions (66) v0 ----

// state_filters ["Ongoing"], producer_id_filters [42]
const std::vector<uint8_t> kListTransactionsRequestV0 = {
    0x02,                                            // state_filters count 1
    0x08, 0x4f, 0x6e, 0x67, 0x6f, 0x69, 0x6e, 0x67,  //   "Ongoing"
    0x02,                                            // producer_id_filters count 1
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2a,  //   42
    0x00,                                            // request tagged fields
};

TEST(AdminStubsWire, ListTransactionsRequestV0Decode) {
    auto req = decodeBytes<ListTransactionsRequest>(kListTransactionsRequestV0, 0);
    ASSERT_EQ(req.stateFilters().size(), 1u);
    EXPECT_EQ(req.stateFilters()[0], "Ongoing");
    ASSERT_EQ(req.producerIdFilters().size(), 1u);
    EXPECT_EQ(req.producerIdFilters()[0], 42);
}

TEST(AdminStubsWire, ListTransactionsRequestV0RoundTrip) {
    expectRoundTrip<ListTransactionsRequest>(kListTransactionsRequestV0, 0);
}

// throttle 0, error 0, no unknown filters, states [{"tid", 9, "Ongoing"}]
const std::vector<uint8_t> kListTransactionsResponseV0 = {
    0x00, 0x00, 0x00, 0x00,                          // throttle_time_ms
    0x00, 0x00,                                      // error_code
    0x01,                                            // unknown_state_filters: empty
    0x02,                                            // transaction_states count 1
    0x04, 0x74, 0x69, 0x64,                          //   transactional_id "tid"
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x09,  //   producer_id 9
    0x08, 0x4f, 0x6e, 0x67, 0x6f, 0x69, 0x6e, 0x67,  //   transaction_state "Ongoing"
    0x00,                                            //   state tagged fields
    0x00,                                            // response tagged fields
};

TEST(AdminStubsWire, ListTransactionsResponseV0Encode) {
    ListTransactionsResponse resp;
    resp.setThrottleTimeMs(0);
    resp.setErrorCode(kawasan::ErrorCode::NONE);
    resp.addState({"tid", 9, "Ongoing"});
    EXPECT_EQ(encodeBytes(resp, 0), kListTransactionsResponseV0);
}

TEST(AdminStubsWire, ListTransactionsResponseV0RoundTrip) {
    expectRoundTrip<ListTransactionsResponse>(kListTransactionsResponseV0, 0);
}

// ---- DescribeTransactions (65) v0 ----

// transactional_ids ["tid"]
const std::vector<uint8_t> kDescribeTransactionsRequestV0 = {
    0x02,                    // transactional_ids count 1
    0x04, 0x74, 0x69, 0x64,  //   "tid"
    0x00,                    // request tagged fields
};

TEST(AdminStubsWire, DescribeTransactionsRequestV0Decode) {
    auto req = decodeBytes<DescribeTransactionsRequest>(kDescribeTransactionsRequestV0, 0);
    ASSERT_EQ(req.transactionalIds().size(), 1u);
    EXPECT_EQ(req.transactionalIds()[0], "tid");
}

TEST(AdminStubsWire, DescribeTransactionsRequestV0RoundTrip) {
    expectRoundTrip<DescribeTransactionsRequest>(kDescribeTransactionsRequestV0, 0);
}

// throttle 0; states [{error 0, "tid", "Ongoing", timeout 60000,
// start_time 123, pid 9, epoch 1 (int16), topics [{"t", [0]}]}]
const std::vector<uint8_t> kDescribeTransactionsResponseV0 = {
    0x00, 0x00, 0x00, 0x00,                          // throttle_time_ms
    0x02,                                            // transaction_states count 1
    0x00, 0x00,                                      //   error_code
    0x04, 0x74, 0x69, 0x64,                          //   transactional_id "tid"
    0x08, 0x4f, 0x6e, 0x67, 0x6f, 0x69, 0x6e, 0x67,  //   transaction_state "Ongoing"
    0x00, 0x00, 0xea, 0x60,                          //   transaction_timeout_ms 60000
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x7b,  //   transaction_start_time_ms 123
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x09,  //   producer_id 9
    0x00, 0x01,                                      //   producer_epoch 1 (INT16)
    0x02,                                            //   topics count 1
    0x02, 0x74,                                      //     topic "t"
    0x02,                                            //     partitions count 1
    0x00, 0x00, 0x00, 0x00,                          //       partition 0
    0x00,                                            //     topic tagged fields
    0x00,                                            //   state tagged fields
    0x00,                                            // response tagged fields
};

TEST(AdminStubsWire, DescribeTransactionsResponseV0Encode) {
    DescribeTransactionsResponse resp;
    resp.setThrottleTimeMs(0);
    DescribeTransactionsResponse::State s;
    s.error_code = kawasan::ErrorCode::NONE;
    s.transactional_id = "tid";
    s.state = "Ongoing";
    s.transaction_timeout_ms = 60000;
    s.transaction_start_time_ms = 123;
    s.producer_id = 9;
    s.producer_epoch = 1;
    s.topics.push_back({"t", {0}});
    resp.addState(std::move(s));

    EXPECT_EQ(encodeBytes(resp, 0), kDescribeTransactionsResponseV0);
}

TEST(AdminStubsWire, DescribeTransactionsResponseV0RoundTrip) {
    expectRoundTrip<DescribeTransactionsResponse>(kDescribeTransactionsResponseV0, 0);
}

// ---- AlterPartition (56) v0 ----

// broker 1, broker_epoch -1, topics [{"t", [{index 0, leader_epoch 5,
// new_isr [0,1], partition_epoch 2}]}]
const std::vector<uint8_t> kAlterPartitionRequestV0 = {
    0x00, 0x00, 0x00, 0x01,                          // broker_id 1
    0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,  // broker_epoch -1
    0x02,                                            // topics count 1
    0x02, 0x74,                                      //   topic_name "t"
    0x02,                                            //   partitions count 1
    0x00, 0x00, 0x00, 0x00,                          //     partition_index 0
    0x00, 0x00, 0x00, 0x05,                          //     leader_epoch 5
    0x03,                                            //     new_isr count 2
    0x00, 0x00, 0x00, 0x00,                          //       0
    0x00, 0x00, 0x00, 0x01,                          //       1
    0x00, 0x00, 0x00, 0x02,                          //     partition_epoch 2
    0x00,                                            //     partition tagged fields
    0x00,                                            //   topic tagged fields
    0x00,                                            // request tagged fields
};

TEST(AdminStubsWire, AlterPartitionRequestV0Decode) {
    auto req = decodeBytes<AlterPartitionRequest>(kAlterPartitionRequestV0, 0);
    EXPECT_EQ(req.broker_id, 1);
    EXPECT_EQ(req.broker_epoch, -1);
    ASSERT_EQ(req.topics.size(), 1u);
    EXPECT_EQ(req.topics[0].topic_name, "t");
    ASSERT_EQ(req.topics[0].partitions.size(), 1u);
    const auto& p = req.topics[0].partitions[0];
    EXPECT_EQ(p.partition_index, 0);
    EXPECT_EQ(p.leader_epoch, 5);
    EXPECT_EQ(p.new_isr, (std::vector<int32_t>{0, 1}));
    EXPECT_EQ(p.partition_epoch, 2);
}

TEST(AdminStubsWire, AlterPartitionRequestV0RoundTrip) {
    expectRoundTrip<AlterPartitionRequest>(kAlterPartitionRequestV0, 0);
}

// throttle 0, error 0, topics [{"t", [{index 0, error 0, leader_id 0,
// leader_epoch 5, isr [0,1], partition_epoch 2}]}]
const std::vector<uint8_t> kAlterPartitionResponseV0 = {
    0x00, 0x00, 0x00, 0x00,  // throttle_time_ms
    0x00, 0x00,              // error_code
    0x02,                    // topics count 1
    0x02, 0x74,              //   topic_name "t"
    0x02,                    //   partitions count 1
    0x00, 0x00, 0x00, 0x00,  //     partition_index 0
    0x00, 0x00,              //     error_code 0
    0x00, 0x00, 0x00, 0x00,  //     leader_id 0
    0x00, 0x00, 0x00, 0x05,  //     leader_epoch 5
    0x03,                    //     isr count 2
    0x00, 0x00, 0x00, 0x00,  //       0
    0x00, 0x00, 0x00, 0x01,  //       1
    0x00, 0x00, 0x00, 0x02,  //     partition_epoch 2
    0x00,                    //     partition tagged fields
    0x00,                    //   topic tagged fields
    0x00,                    // response tagged fields
};

TEST(AdminStubsWire, AlterPartitionResponseV0Encode) {
    AlterPartitionResponse resp;
    resp.setThrottleTimeMs(0);
    resp.setErrorCode(kawasan::ErrorCode::NONE);
    AlterPartitionResponse::TopicResult t;
    t.topic_name = "t";
    AlterPartitionResponse::PartitionResult p;
    p.partition_index = 0;
    p.error_code = kawasan::ErrorCode::NONE;
    p.leader_id = 0;
    p.leader_epoch = 5;
    p.isr = {0, 1};
    p.partition_epoch = 2;
    t.partitions.push_back(std::move(p));
    resp.addTopic(std::move(t));

    EXPECT_EQ(encodeBytes(resp, 0), kAlterPartitionResponseV0);
}

TEST(AdminStubsWire, AlterPartitionResponseV0RoundTrip) {
    expectRoundTrip<AlterPartitionResponse>(kAlterPartitionResponseV0, 0);
}
