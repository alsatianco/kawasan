#include "kawasan/broker/transaction_state_manager.h"

#include <algorithm>
#include <stdexcept>
#include <unordered_map>

#include "kawasan/broker/coordinator_routing.h"
#include "kawasan/common/buffer.h"
#include "kawasan/common/logger.h"
#include "kawasan/common/types.h"
#include "kawasan/storage/log.h"
#include "kawasan/storage/log_manager.h"
#include "kawasan/storage/record_batch.h"

namespace kawasan::broker {

namespace {

constexpr int8_t kSnapshotVersion = 2;

// Length-prefixed (INT32) byte field — UTF-8-agnostic, so PendingOffset
// metadata (arbitrary bytes) round-trips safely, unlike JSON.
void putBytes(Buffer& buf, const std::string& s) {
    buf.writeInt32(static_cast<int32_t>(s.size()));
    if (!s.empty()) {
        buf.writeBytes(reinterpret_cast<const uint8_t*>(s.data()), s.size());
    }
}

std::string getBytes(Buffer& buf) {
    const int32_t len = buf.readInt32();
    if (len < 0)
        throw std::runtime_error("txn-state: negative field length");
    if (len == 0)
        return {};
    auto v = buf.readBytes(static_cast<size_t>(len));
    return std::string(v.begin(), v.end());
}

}  // namespace

TransactionStateManager::TransactionStateManager(storage::LogManager* log_manager,
                                                 int num_partitions)
    : log_manager_(log_manager), num_partitions_(std::max(1, num_partitions)) {}

std::vector<uint8_t> TransactionStateManager::serialize(
    const TransactionCoordinator::TxnSnapshot& s) {
    Buffer buf;
    // Preserve v1 bytes for snapshots without leader-epoch metadata.
    const int8_t version =
        std::any_of(s.pending_offsets.begin(), s.pending_offsets.end(),
                    [](const auto& offset) { return offset.committed_leader_epoch != -1; })
            ? kSnapshotVersion
            : 1;
    buf.writeInt8(version);
    putBytes(buf, s.transactional_id);
    buf.writeInt64(s.producer_id);
    buf.writeInt16(s.producer_epoch);
    buf.writeInt32(s.transaction_timeout_ms);
    buf.writeInt8(static_cast<int8_t>(s.state));
    buf.writeInt64(s.state_start_time_ms);

    buf.writeInt32(static_cast<int32_t>(s.partitions.size()));
    for (const auto& p : s.partitions) {
        putBytes(buf, p.topic);
        buf.writeInt32(p.partition);
        buf.writeInt64(p.first_offset);
    }

    buf.writeInt32(static_cast<int32_t>(s.pending_offsets.size()));
    for (const auto& po : s.pending_offsets) {
        putBytes(buf, po.group_id);
        putBytes(buf, po.topic);
        buf.writeInt32(po.partition);
        buf.writeInt64(po.offset);
        putBytes(buf, po.metadata);
        if (version >= 2)
            buf.writeInt32(po.committed_leader_epoch);
    }
    return std::vector<uint8_t>(buf.data(), buf.data() + buf.size());
}

TransactionCoordinator::TxnSnapshot TransactionStateManager::deserialize(
    const std::vector<uint8_t>& bytes) {
    Buffer buf(bytes);
    TransactionCoordinator::TxnSnapshot s;
    const int8_t version = buf.readInt8();
    if (version != 1 && version != kSnapshotVersion) {
        throw std::runtime_error("txn-state: unknown snapshot version " + std::to_string(version));
    }
    s.transactional_id = getBytes(buf);
    s.producer_id = buf.readInt64();
    s.producer_epoch = buf.readInt16();
    s.transaction_timeout_ms = buf.readInt32();
    s.state = static_cast<TransactionCoordinator::State>(buf.readInt8());
    s.state_start_time_ms = buf.readInt64();

    const int32_t pcount = buf.readInt32();
    if (pcount < 0)
        throw std::runtime_error("txn-state: negative partition count");
    s.partitions.reserve(static_cast<size_t>(pcount));
    for (int32_t i = 0; i < pcount; ++i) {
        TransactionCoordinator::TxnPartition p;
        p.topic = getBytes(buf);
        p.partition = buf.readInt32();
        p.first_offset = buf.readInt64();
        s.partitions.push_back(std::move(p));
    }

    const int32_t ocount = buf.readInt32();
    if (ocount < 0)
        throw std::runtime_error("txn-state: negative offset count");
    s.pending_offsets.reserve(static_cast<size_t>(ocount));
    for (int32_t i = 0; i < ocount; ++i) {
        TransactionCoordinator::PendingOffset po;
        po.group_id = getBytes(buf);
        po.topic = getBytes(buf);
        po.partition = buf.readInt32();
        po.offset = buf.readInt64();
        po.metadata = getBytes(buf);
        po.committed_leader_epoch = version >= 2 ? buf.readInt32() : -1;
        s.pending_offsets.push_back(std::move(po));
    }
    return s;
}

int TransactionStateManager::partitionFor(const std::string& transactional_id, int num_partitions) {
    return coordinatorPartitionFor(transactional_id, num_partitions);
}

void TransactionStateManager::persist(const TransactionCoordinator::TxnSnapshot& snapshot) {
    if (!log_manager_ || snapshot.transactional_id.empty())
        return;
    const int partition = partitionFor(snapshot.transactional_id, num_partitions_);
    auto* log = log_manager_->getOrCreateLog(kTopic, partition);
    if (!log) {
        Logger::warn("txn-state: no log for {}-{}, snapshot for '{}' dropped", kTopic, partition,
                     snapshot.transactional_id);
        return;
    }
    Record record;
    record.key =
        std::vector<uint8_t>(snapshot.transactional_id.begin(), snapshot.transactional_id.end());
    const auto value = serialize(snapshot);
    record.value = value;
    // producer_id stays -1 (plain append) so producer-state replay ignores it.
    // force_sync: transaction state must be durable across a power loss even
    // when the broker's data durability is async.
    try {
        log->append({record}, /*force_sync=*/true);
    } catch (const std::exception& ex) {
        Logger::warn("txn-state: failed to persist snapshot for '{}': {}",
                     snapshot.transactional_id, ex.what());
    }
}

std::vector<TransactionCoordinator::TxnSnapshot> TransactionStateManager::loadAll() {
    std::vector<TransactionCoordinator::TxnSnapshot> out;
    if (!log_manager_)
        return out;

    // Preserve first-seen order of transactional_ids while overwriting with the
    // latest snapshot per key (last write in log order wins).
    std::unordered_map<std::string, size_t> index;
    for (int p = 0; p < num_partitions_; ++p) {
        auto* log = log_manager_->getOrCreateLog(kTopic, p);
        if (!log)
            continue;
        const Offset end = log->logEndOffset();
        Offset off = log->logStartOffset();
        constexpr size_t kChunkBytes = 8 * 1024 * 1024;
        while (off < end) {
            std::vector<storage::RecordBatch> batches;
            try {
                batches = log->read(off, kChunkBytes);
            } catch (const std::exception& ex) {
                Logger::warn("txn-state: read failed on {}-{} at {}: {}", kTopic, p, off,
                             ex.what());
                break;
            }
            if (batches.empty())
                break;
            Offset next = off;
            for (const auto& batch : batches) {
                next = std::max(next,
                                batch.baseOffset() + static_cast<Offset>(batch.records().size()));
                for (const auto& rec : batch.records()) {
                    if (!rec.value)
                        continue;
                    try {
                        auto snap = deserialize(*rec.value);
                        if (snap.transactional_id.empty())
                            continue;
                        auto it = index.find(snap.transactional_id);
                        if (it == index.end()) {
                            index.emplace(snap.transactional_id, out.size());
                            out.push_back(std::move(snap));
                        } else {
                            out[it->second] = std::move(snap);
                        }
                    } catch (const std::exception& ex) {
                        Logger::warn("txn-state: skipping malformed record on {}-{}: {}", kTopic, p,
                                     ex.what());
                    }
                }
            }
            if (next <= off)
                break;  // forward-progress guard
            off = next;
        }
    }
    return out;
}

}  // namespace kawasan::broker
