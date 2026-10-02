#pragma once

#include <map>
#include <optional>
#include <string>
#include <vector>

#include "kawasan/common/types.h"

namespace kawasan::broker {

/// @brief Topic definition provided by admin operations.
struct TopicSpecification {
    std::string name;
    int32_t num_partitions = -1;
    int16_t replication_factor = -1;
    std::vector<std::vector<BrokerId>> assignments;
    std::map<std::string, std::string> configs;
};

/// @brief Result of topic mutation commands.
struct TopicOperationResult {
    ErrorCode error_code = ErrorCode::NONE;
    std::string error_message;
    TopicMetadata topic_metadata;
    bool has_metadata = false;

    /// @brief Error-result shorthand. (Partial designated initializers of
    /// this struct trip -Wmissing-designated-field-initializers on newer
    /// clang; use this instead.)
    static TopicOperationResult failure(ErrorCode code, std::string message) {
        TopicOperationResult result;
        result.error_code = code;
        result.error_message = std::move(message);
        return result;
    }
};

/// @brief Metadata command types replicated via Raft.
enum class MetadataCommandType {
    CREATE_TOPIC,
    DELETE_TOPIC,
    UPDATE_ISR,
    INCREASE_PARTITIONS,
    UPDATE_LEADER,  // M7: elect a new partition leader (bumps leader_epoch)
};

/// @brief Command payload stored in the Raft log.
struct MetadataCommand {
    MetadataCommandType type = MetadataCommandType::CREATE_TOPIC;
    TopicSpecification topic_spec;
    std::string topic_name;
    // For UPDATE_ISR command
    PartitionId partition_id = -1;
    std::vector<BrokerId> isr;
    // For INCREASE_PARTITIONS command: new total partition count.
    int32_t new_partition_count = 0;
    // For UPDATE_LEADER command (M7): the newly-elected leader for partition_id.
    BrokerId leader = -1;
    // -1 preserves replay of historical commands; new decisions carry a version.
    int32_t expected_partition_epoch = -1;
};

}  // namespace kawasan::broker
