#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "kawasan/broker/transaction_coordinator.h"

namespace kawasan::storage {
class LogManager;
}

namespace kawasan::broker {

/// @brief M1: durable persistence of transaction-coordinator state to the
/// internal `__transaction_state` topic, and replay of it on startup.
///
/// Each transaction's latest TxnSnapshot is appended as a single keyed record
/// (key = transactional_id, value = serialized snapshot) to the
/// __transaction_state partition selected by coordinatorPartitionFor. This
/// preserves the existing byte-hash/unsigned-modulo placement contract; changing
/// it requires a migration. Each ID's records remain ordered within one partition. The topic is
/// compacted, so only the latest snapshot per transactional_id survives long-term; replay
/// defensively takes the last-written record per key regardless.
///
/// Records are appended with producer_id = -1 (plain, non-idempotent append)
/// so the producer-state replay that also scans this topic does not
/// misinterpret them as idempotent-producer data.
class TransactionStateManager {
public:
    static constexpr const char* kTopic = "__transaction_state";

    TransactionStateManager(storage::LogManager* log_manager, int num_partitions);

    /// @brief Serialize a snapshot to the on-disk record value (versioned,
    /// length-prefixed binary; UTF-8-agnostic).
    static std::vector<uint8_t> serialize(const TransactionCoordinator::TxnSnapshot& s);

    /// @brief Inverse of serialize(). Throws std::runtime_error on a malformed
    /// or unknown-version payload.
    static TransactionCoordinator::TxnSnapshot deserialize(const std::vector<uint8_t>& bytes);

    /// @brief The __transaction_state partition a transactional_id routes to.
    static int partitionFor(const std::string& transactional_id, int num_partitions);

    /// @brief Append the snapshot to its routed __transaction_state partition.
    /// No-op if the log manager is unavailable. The append fsyncs per the
    /// log's durability mode, so on return the record is durable.
    void persist(const TransactionCoordinator::TxnSnapshot& snapshot);

    /// @brief Scan every __transaction_state partition and return the latest
    /// snapshot per transactional_id (last write in log order wins).
    std::vector<TransactionCoordinator::TxnSnapshot> loadAll();

    /// @brief M10 acquisition primitive: replay only this partition below its
    /// captured high watermark. Requires an existing local log; malformed
    /// committed records fail recovery rather than producing a partial cache.
    /// Legacy startup continues to use loadAll until format/ownership integration.
    std::vector<TransactionCoordinator::TxnSnapshot> loadCommittedPartition(int partition);

private:
    storage::LogManager* log_manager_;
    int num_partitions_;
};

}  // namespace kawasan::broker
