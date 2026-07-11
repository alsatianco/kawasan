#include "kawasan/broker/peer_client.h"

#include <arpa/inet.h>

#include <array>
#include <cstring>

#include "kawasan/common/buffer.h"
#include "kawasan/common/logger.h"
#include "kawasan/protocol/api_keys.h"
#include "kawasan/protocol/fetch_request.h"
#include "kawasan/protocol/request_header.h"

namespace kawasan::broker {

namespace {
// Fetch v4: leader returns raw RecordBatch bytes (appendable as-is) and the
// request/response headers stay non-flexible (< v12), so the default header
// codecs apply.
constexpr int16_t kFetchVersion = 4;
}  // namespace

PeerClient::PeerClient(std::string host, int32_t port, BrokerId self_broker_id)
    : host_(std::move(host)), port_(port), self_broker_id_(self_broker_id) {}

PeerClient::~PeerClient() {
    disconnect();
}

void PeerClient::ensureConnected() {
    if (connected_ && socket_.is_open()) {
        return;
    }
    boost::system::error_code ec;
    socket_.close(ec);
    boost::asio::ip::tcp::resolver resolver(io_context_);
    const auto endpoints = resolver.resolve(host_, std::to_string(port_));
    boost::asio::connect(socket_, endpoints);
    connected_ = true;
}

void PeerClient::disconnect() {
    boost::system::error_code ec;
    socket_.close(ec);
    connected_ = false;
}

std::optional<PeerClient::FetchResult> PeerClient::fetch(const std::string& topic,
                                                         PartitionId partition,
                                                         Offset fetch_offset) {
    try {
        ensureConnected();

        const int32_t corr = ++correlation_id_;
        Buffer payload;
        protocol::RequestHeader header(protocol::ApiKey::FETCH, kFetchVersion, corr,
                                       "kawasan-replica-" + std::to_string(self_broker_id_));
        header.encode(payload);

        protocol::FetchRequest request;
        request.setReplicaId(self_broker_id_);  // >= 0 => follower fetch on the leader
        request.setMaxWaitMs(0);                // followers do not long-poll
        request.setMinBytes(0);
        request.setMaxBytes(8 * 1024 * 1024);
        protocol::FetchPartition fp;
        fp.partition = partition;
        fp.fetch_offset = fetch_offset;
        fp.partition_max_bytes = 8 * 1024 * 1024;
        protocol::FetchTopic ft;
        ft.topic = topic;
        ft.partitions.push_back(fp);
        request.addTopic(ft);
        request.encode(payload, kFetchVersion);

        // Frame: 4-byte big-endian length prefix + payload.
        const int32_t payload_size = static_cast<int32_t>(payload.size());
        std::vector<uint8_t> frame(sizeof(int32_t) + payload_size);
        const int32_t net_size = htonl(payload_size);
        std::memcpy(frame.data(), &net_size, sizeof(net_size));
        if (payload_size > 0) {
            std::memcpy(frame.data() + sizeof(int32_t), payload.data(), payload.size());
        }
        boost::asio::write(socket_, boost::asio::buffer(frame));

        std::array<uint8_t, 4> size_bytes{};
        boost::asio::read(socket_, boost::asio::buffer(size_bytes));
        uint32_t net_len = 0;
        std::memcpy(&net_len, size_bytes.data(), size_bytes.size());
        const int32_t resp_size = ntohl(net_len);
        if (resp_size < 0) {
            disconnect();
            return std::nullopt;
        }
        std::vector<uint8_t> resp_body(static_cast<size_t>(resp_size));
        if (resp_size > 0) {
            boost::asio::read(socket_, boost::asio::buffer(resp_body));
        }

        Buffer resp(std::move(resp_body));
        protocol::ResponseHeader resp_header;
        resp_header.decode(resp);
        protocol::FetchResponse fetch_response;
        fetch_response.decode(resp, kFetchVersion);

        FetchResult result;
        // We requested exactly one topic/partition; tolerate an empty response.
        if (fetch_response.topics().empty() || fetch_response.topics().front().partitions.empty()) {
            result.error = ErrorCode::NONE;
            return result;
        }
        const auto& pr = fetch_response.topics().front().partitions.front();
        result.error = pr.error_code;
        result.high_watermark = pr.high_watermark;
        result.log_start_offset = pr.log_start_offset;
        result.record_batches = pr.record_batches;
        return result;
    } catch (const std::exception& e) {
        Logger::warn("PeerClient fetch to {}:{} failed: {}", host_, port_, e.what());
        disconnect();
        return std::nullopt;
    }
}

}  // namespace kawasan::broker
