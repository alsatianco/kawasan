#pragma once

#include <array>
#include <boost/asio.hpp>
#include <stdexcept>

#include "kawasan/common/buffer.h"
#include "kawasan/protocol/produce_request.h"
#include "kawasan/protocol/request_header.h"
#include "sparse_record_batch.h"

namespace kawasan::test_support {
// Send real wire requests so assertions exercise the broker's recovered cache.
inline Buffer brokerRequest(int port, Buffer& payload) {
    boost::asio::io_context io;
    boost::asio::ip::tcp::socket socket(io);
    socket.connect({boost::asio::ip::make_address("127.0.0.1"), static_cast<uint16_t>(port)});
    Buffer frame;
    frame.writeInt32(static_cast<int32_t>(payload.size()));
    frame.writeBytes(payload.vector().data(), payload.size());
    boost::asio::write(socket, boost::asio::buffer(frame.vector()));
    std::array<uint8_t, 4> size{};
    boost::asio::read(socket, boost::asio::buffer(size));
    Buffer length(std::vector<uint8_t>(size.begin(), size.end()));
    const auto body_size = length.readInt32();
    if (body_size < 0 || body_size > 16 * 1024 * 1024)
        throw std::runtime_error("Invalid broker response size");
    std::vector<uint8_t> body(body_size);
    boost::asio::read(socket, boost::asio::buffer(body));
    return Buffer(body);
}

inline protocol::ProducePartitionResponse brokerProduce(int port, const std::string& topic,
                                                        const storage::RecordBatch& batch) {
    Buffer payload;
    protocol::RequestHeader(protocol::ApiKey::PRODUCE, 3, 1, "sparse-replay").encode(payload);
    protocol::ProduceRequest request;
    request.setAcks(1);
    request.addTopic({topic, {{0, batch.serialize()}}});
    request.encode(payload, 3);
    auto body = brokerRequest(port, payload);
    protocol::ResponseHeader header;
    header.decode(body);
    protocol::ProduceResponse response;
    response.decode(body, 3);
    return response.topics().at(0).partitions.at(0);
}

inline int32_t brokerLastSequence(int port, const std::string& topic) {
    Buffer payload;
    protocol::RequestHeader(protocol::ApiKey::DESCRIBE_PRODUCERS, 0, 1, "sparse-replay")
        .encode(payload);
    payload.writeCompactArrayLen(1);
    payload.writeCompactString(topic);
    payload.writeCompactArrayLen(1);
    payload.writeInt32(0);
    payload.writeEmptyTaggedFields();
    payload.writeEmptyTaggedFields();
    auto body = brokerRequest(port, payload);
    (void)body.readInt32();  // correlation id
    body.skipTaggedFields();
    (void)body.readInt32();  // throttle
    if (body.readCompactArrayLen() != 1 || body.readCompactString() != topic ||
        body.readCompactArrayLen() != 1 || body.readInt32() != 0 || body.readInt16() != 0)
        throw std::runtime_error("Unexpected DescribeProducers partition");
    (void)body.readCompactNullableString();
    if (body.readCompactArrayLen() != 1 || body.readInt64() != 99 || body.readInt32() != 0)
        throw std::runtime_error("Expected producer 99 at epoch 0");
    return body.readInt32();
}

inline storage::RecordBatch sparseProducerBatch(size_t payload_bytes = 1, int32_t sequence = 0,
                                                int32_t last_delta = 9) {
    storage::RecordBatch batch;
    batch.setProducerId(99);
    batch.setProducerEpoch(0);
    batch.setBaseSequence(sequence);
    Record first("first", std::string(payload_bytes, 'x'));
    first.offset_delta = 2;
    Record last("last", "retained");
    last.offset_delta = 9;
    batch.addRecord(first);
    batch.addRecord(last);
    return batchWithWireSpan(batch, last_delta);
}

inline storage::RecordBatch producerBatch(int64_t producer_id, int32_t sequence) {
    storage::RecordBatch batch;
    batch.setProducerId(producer_id);
    batch.setProducerEpoch(0);
    batch.setBaseSequence(sequence);
    batch.addRecord(Record("next", "value"));
    return batch;
}
}  // namespace kawasan::test_support
