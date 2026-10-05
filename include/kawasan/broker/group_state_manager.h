#pragma once

#include <map>
#include <optional>
#include <string>
#include <vector>

#include "kawasan/broker/offset_manager.h"
#include "kawasan/common/types.h"

namespace kawasan::storage {
class LogManager;
}

namespace kawasan::broker {

// Complete clustered group identity. Heartbeat/rebalance deadlines are rebuilt
// on acquisition, since steady-clock timestamps cannot survive a broker move.
struct GroupSnapshot {
    struct Member {
        std::string member_id;
        std::string client_id;
        std::string client_host;
        std::optional<std::string> group_instance_id;
        std::vector<uint8_t> metadata;
        std::vector<uint8_t> assignment;
        bool operator==(const Member&) const = default;
    };
    int32_t generation = 0;
    int8_t state = 0;  // GroupCoordinator Empty .. Dead (0..4).
    std::string protocol_type;
    std::string protocol_name;
    std::string leader_id;
    int32_t rebalance_timeout_ms = 0;
    int64_t last_update_timestamp = 0;
    std::vector<Member> members;
    bool operator==(const GroupSnapshot&) const = default;
};

struct GroupRecordKey {
    enum class Kind : int8_t { Group = 0, Offset = 1, PendingOffset = 2 };
    Kind kind = Kind::Group;
    std::string group_id;
    std::string topic;
    int32_t partition = -1;
    // Pending records are group-owned and fence a specific producer epoch.
    std::string transactional_id;
    int64_t producer_id = -1;
    int16_t producer_epoch = -1;
    auto operator<=>(const GroupRecordKey&) const = default;
};

struct GroupRecord {
    GroupRecordKey key;
    // Tombstones erase this exact key. DeleteGroup must append tombstones for
    // its group, offset and pending keys together; compaction remains per-key.
    bool tombstone = false;
    GroupSnapshot group;
    OffsetManager::OffsetMetadata offset{};
};

// Format-v1 primitives, deliberately unused by legacy runtime persistence.
class GroupStateManager {
public:
    static constexpr const char* kTopic = "__consumer_offsets";
    GroupStateManager(storage::LogManager* logs, int32_t partitions);
    static Record encode(const GroupRecord& record);
    static GroupRecord decode(const Record& record);
    using PartitionImage = std::map<GroupRecordKey, GroupRecord>;
    // Captures HW, requires an existing local log and fails on malformed
    // committed records. Caller must hold the acquisition/ownership fence.
    PartitionImage loadCommittedPartition(int32_t partition) const;

private:
    storage::LogManager* logs_;
    int32_t partitions_;
};
}  // namespace kawasan::broker
