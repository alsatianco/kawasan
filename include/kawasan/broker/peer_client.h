#pragma once

#include <boost/asio.hpp>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "kawasan/common/error.h"
#include "kawasan/common/types.h"

namespace kawasan::broker {

/// @brief M5: a minimal broker-to-broker Kafka client used by the follower
/// replica fetcher to pull data from a partition leader. It sends a Fetch
/// request with `replica_id = this broker's id` (>= 0), which the leader treats
/// as a follower fetch (serves up to the log-end-offset, records the follower's
/// position, no long-poll). Synchronous and blocking — it runs on the dedicated
/// replica-fetcher thread, one connection per peer.
///
/// Modeled on the producer client's framed request/response (4-byte big-endian
/// length prefix + Kafka request/response headers). Fetch v4 is used on purpose:
/// at v4+ the leader returns raw RecordBatch bytes (appendable as-is), and the
/// header stays non-flexible (< v12), so the default response-header decode
/// applies.
class PeerClient {
public:
    PeerClient(std::string host, int32_t port, BrokerId self_broker_id);
    ~PeerClient();

    PeerClient(const PeerClient&) = delete;
    PeerClient& operator=(const PeerClient&) = delete;

    /// @brief One partition's data fetched from the leader.
    struct FetchResult {
        ErrorCode error = ErrorCode::NONE;
        Offset high_watermark = 0;
        Offset log_start_offset = 0;
        std::vector<uint8_t> record_batches;  // raw serialized batches (may be empty)
    };

    /// @brief Fetch from the leader for one (topic, partition) starting at
    /// `fetch_offset`. Returns nullopt on a connection/protocol error (the caller
    /// should back off and retry); a partition-level Kafka error is reported via
    /// FetchResult::error.
    std::optional<FetchResult> fetch(const std::string& topic, PartitionId partition,
                                     Offset fetch_offset);

    /// @brief M6: propose an ISR change for one partition to the controller
    /// (AlterPartition, API 56 — a flexible-from-v0 API). Returns the committed
    /// error code (NONE on success, NOT_CONTROLLER if this peer is not the active
    /// controller, or a fencing error), or nullopt on a connection/protocol error.
    std::optional<ErrorCode> alterPartition(const std::string& topic, PartitionId partition,
                                            int32_t leader_epoch,
                                            const std::vector<BrokerId>& new_isr,
                                            int32_t partition_epoch);

    const std::string& host() const { return host_; }
    int32_t port() const { return port_; }

private:
    void ensureConnected();
    void disconnect();

    std::string host_;
    int32_t port_;
    BrokerId self_broker_id_;
    boost::asio::io_context io_context_;
    boost::asio::ip::tcp::socket socket_{io_context_};
    bool connected_ = false;
    int32_t correlation_id_ = 0;
};

}  // namespace kawasan::broker
