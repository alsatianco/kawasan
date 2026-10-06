#include "kawasan/broker/group_state_manager.h"

#include <algorithm>
#include <limits>
#include <set>
#include <stdexcept>

#include "kawasan/broker/coordinator_routing.h"
#include "kawasan/common/buffer.h"
#include "kawasan/storage/log_manager.h"

namespace kawasan::broker {
namespace {
constexpr int8_t KEY_MAGIC = 'G';
constexpr int8_t VERSION = 1;
using Kind = GroupRecordKey::Kind;

[[noreturn]] void reject(const char* reason) {
    throw std::runtime_error(std::string("group-state: ") + reason);
}

void putBytes(Buffer& buffer, const std::vector<uint8_t>& value) {
    if (value.size() > INT32_MAX)
        reject("field too large");
    buffer.writeBytes(value);
}
void putString(Buffer& buffer, const std::string& value) {
    if (value.size() > INT32_MAX)
        reject("field too large");
    buffer.writeInt32(static_cast<int32_t>(value.size()));
    if (!value.empty())
        buffer.writeBytes(reinterpret_cast<const uint8_t*>(value.data()), value.size());
}
std::vector<uint8_t> getBytes(Buffer& buffer) {
    const auto size = buffer.readInt32();
    if (size < 0 || static_cast<size_t>(size) > buffer.remaining())
        reject("invalid field length");
    return buffer.readBytes(static_cast<size_t>(size));
}
std::string getString(Buffer& buffer) {
    const auto bytes = getBytes(buffer);
    return {bytes.begin(), bytes.end()};
}
std::vector<uint8_t> bytes(const Buffer& buffer) {
    return {buffer.data(), buffer.data() + buffer.size()};
}
void validateKey(const GroupRecordKey& key) {
    if (key.group_id.empty() || key.kind < Kind::Group || key.kind > Kind::PendingOffset)
        reject("invalid group key");
    if (key.kind == Kind::Group) {
        if (!key.topic.empty() || key.partition != -1)
            reject("noncanonical group key");
    } else if (key.topic.empty() || key.partition < 0)
        reject("invalid offset key");
    if (key.kind == Kind::PendingOffset) {
        if (key.transactional_id.empty() || key.producer_id < 0 || key.producer_epoch < 0)
            reject("invalid pending producer identity");
    } else if (!key.transactional_id.empty() || key.producer_id != -1 || key.producer_epoch != -1)
        reject("noncanonical producer identity");
}
void validateGroup(const GroupSnapshot& group) {
    if (group.generation < 0 || group.state < 0 || group.state > 4 ||
        group.rebalance_timeout_ms < 0 || group.last_update_timestamp < 0 ||
        group.members.size() > INT32_MAX)
        reject("invalid group snapshot");
    std::set<std::string> members;
    std::set<std::string> instances;
    for (const auto& member : group.members) {
        if (member.member_id.empty() || !members.insert(member.member_id).second)
            reject("duplicate or missing member identity");
        if (member.group_instance_id && (member.group_instance_id->empty() ||
                                         !instances.insert(*member.group_instance_id).second))
            reject("duplicate or missing static member identity");
    }
    // Rebalance timeout evicts the leader and leaves surviving members in
    // PreparingRebalance until one rejoins and takes leadership. This is a
    // durable transitional state; other phases still require a member leader.
    const bool awaiting_leader = group.state == 1 && group.leader_id.empty();
    if ((group.members.empty() && !group.leader_id.empty()) ||
        (!group.members.empty() && !members.contains(group.leader_id) && !awaiting_leader))
        reject("missing group leader identity");
}
void validateOffset(const OffsetManager::OffsetMetadata& offset) {
    if (offset.offset < 0 || offset.commit_timestamp < 0 || offset.expiry_timestamp < -1 ||
        offset.committed_leader_epoch < -1)
        reject("invalid offset metadata");
}
}  // namespace

GroupStateManager::GroupStateManager(storage::LogManager* logs, int32_t partitions)
    : logs_(logs), partitions_(partitions) {
    if (partitions <= 0)
        reject("invalid partition count");
}

Record GroupStateManager::encode(const GroupRecord& record) {
    validateKey(record.key);
    Buffer key;
    key.writeInt8(KEY_MAGIC);
    key.writeInt8(VERSION);
    key.writeInt8(static_cast<int8_t>(record.key.kind));
    putString(key, record.key.group_id);
    if (record.key.kind != Kind::Group) {
        putString(key, record.key.topic);
        key.writeInt32(record.key.partition);
    }
    if (record.key.kind == Kind::PendingOffset) {
        putString(key, record.key.transactional_id);
        key.writeInt64(record.key.producer_id);
        key.writeInt16(record.key.producer_epoch);
    }
    Record result;
    result.key = bytes(key);
    if (record.tombstone)
        return result;
    Buffer value;
    value.writeInt8(VERSION);
    if (record.key.kind == Kind::Group) {
        const auto& group = record.group;
        validateGroup(group);
        value.writeInt32(group.generation);
        value.writeInt8(group.state);
        putString(value, group.protocol_type);
        putString(value, group.protocol_name);
        putString(value, group.leader_id);
        value.writeInt32(group.rebalance_timeout_ms);
        value.writeInt64(group.last_update_timestamp);
        value.writeInt32(static_cast<int32_t>(group.members.size()));
        for (const auto& member : group.members) {
            putString(value, member.member_id);
            putString(value, member.client_id);
            putString(value, member.client_host);
            value.writeInt8(member.group_instance_id.has_value() ? 1 : 0);
            if (member.group_instance_id)
                putString(value, *member.group_instance_id);
            putBytes(value, member.metadata);
            putBytes(value, member.assignment);
        }
    } else {
        validateOffset(record.offset);
        value.writeInt64(record.offset.offset);
        putString(value, record.offset.metadata);
        value.writeInt64(record.offset.commit_timestamp);
        value.writeInt64(record.offset.expiry_timestamp);
        value.writeInt32(record.offset.committed_leader_epoch);
    }
    result.value = bytes(value);
    return result;
}

GroupRecord GroupStateManager::decode(const Record& record) {
    if (!record.key)
        reject("missing record key");
    Buffer key(*record.key);
    if (key.readInt8() != KEY_MAGIC || key.readInt8() != VERSION)
        reject("unsupported record key");
    GroupRecord result;
    result.key.kind = static_cast<Kind>(key.readInt8());
    if (result.key.kind < Kind::Group || result.key.kind > Kind::PendingOffset)
        reject("unknown record kind");
    result.key.group_id = getString(key);
    if (result.key.kind != Kind::Group) {
        result.key.topic = getString(key);
        result.key.partition = key.readInt32();
    }
    if (result.key.kind == Kind::PendingOffset) {
        result.key.transactional_id = getString(key);
        result.key.producer_id = key.readInt64();
        result.key.producer_epoch = key.readInt16();
    }
    if (key.remaining())
        reject("trailing key bytes");
    validateKey(result.key);
    if (!record.value) {
        result.tombstone = true;
        return result;
    }
    Buffer value(*record.value);
    if (value.readInt8() != VERSION)
        reject("unsupported record value");
    if (result.key.kind == Kind::Group) {
        auto& group = result.group;
        group.generation = value.readInt32();
        group.state = value.readInt8();
        group.protocol_type = getString(value);
        group.protocol_name = getString(value);
        group.leader_id = getString(value);
        group.rebalance_timeout_ms = value.readInt32();
        group.last_update_timestamp = value.readInt64();
        const int32_t count = value.readInt32();
        if (count < 0 || static_cast<size_t>(count) > value.remaining() / 21)
            reject("invalid member count");
        group.members.reserve(static_cast<size_t>(count));
        for (int32_t i = 0; i < count; ++i) {
            GroupSnapshot::Member member;
            member.member_id = getString(value);
            member.client_id = getString(value);
            member.client_host = getString(value);
            const int8_t has_instance = value.readInt8();
            if (has_instance < 0 || has_instance > 1)
                reject("invalid static identity flag");
            if (has_instance)
                member.group_instance_id = getString(value);
            member.metadata = getBytes(value);
            member.assignment = getBytes(value);
            group.members.push_back(std::move(member));
        }
        validateGroup(group);
    } else {
        result.offset.offset = value.readInt64();
        result.offset.metadata = getString(value);
        result.offset.commit_timestamp = value.readInt64();
        result.offset.expiry_timestamp = value.readInt64();
        result.offset.committed_leader_epoch = value.readInt32();
        validateOffset(result.offset);
    }
    if (value.remaining())
        reject("trailing value bytes");
    return result;
}

GroupStateManager::PartitionImage GroupStateManager::loadCommittedPartition(
    int32_t partition) const {
    if (partition < 0 || partition >= partitions_)
        reject("invalid replay partition");
    if (!logs_)
        reject("no log manager");
    auto* log = logs_->getLog(kTopic, partition);
    if (!log)
        reject("missing local partition");
    const auto end = log->highWatermark();
    if (end > log->logEndOffset())
        reject("HW exceeds log end");
    auto offset = log->logStartOffset();
    PartitionImage image;
    while (offset < end) {
        const auto batches = log->read(offset, 8 * 1024 * 1024);
        if (batches.empty())
            reject("unreadable committed prefix");
        auto next = offset;
        for (const auto& batch : batches) {
            if (batch.baseOffset() >= end)
                break;
            const auto delta = batch.lastOffsetDelta();
            if (delta < 0 || batch.baseOffset() < 0 ||
                batch.baseOffset() > std::numeric_limits<Offset>::max() - delta - 1 ||
                batch.isControlBatch() || batch.isTransactional() || batch.producerId() != -1)
                reject("invalid coordinator batch");
            next = std::max(next, batch.baseOffset() + delta + 1);
            int32_t previous = -1;
            for (const auto& record : batch.records()) {
                if (record.offset_delta <= previous || record.offset_delta > delta)
                    reject("invalid record offset");
                previous = record.offset_delta;
                const auto absolute = batch.baseOffset() + record.offset_delta;
                if (absolute < offset || absolute >= end)
                    continue;
                auto decoded = decode(record);
                if (coordinatorPartitionFor(decoded.key.group_id, partitions_) != partition)
                    reject("group key in wrong partition");
                if (decoded.tombstone)
                    image.erase(decoded.key);
                else
                    image[decoded.key] = std::move(decoded);
            }
        }
        if (next <= offset)
            reject("replay made no progress");
        offset = next;
    }
    return image;
}
}  // namespace kawasan::broker
