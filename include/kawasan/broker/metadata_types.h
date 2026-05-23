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
};

/// @brief Metadata command types replicated via Raft.
enum class MetadataCommandType {
    CREATE_TOPIC,
    DELETE_TOPIC,
    UPDATE_ISR,
    INCREASE_PARTITIONS,
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
};

}  // namespace kawasan::broker

