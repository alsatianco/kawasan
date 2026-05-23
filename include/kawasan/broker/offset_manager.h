#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <rocksdb/db.h>

namespace kawasan::broker {

/// @brief Manages persistent storage of consumer group offsets using RocksDB.
///
/// This class provides thread-safe operations for committing and fetching consumer
/// offsets, as well as managing group metadata. All data is persisted to RocksDB
/// and survives broker restarts.
///
/// Storage schema:
/// - Offset keys: "offset:<group_id>:<topic>:<partition>"
/// - Group keys: "group:<group_id>"
/// - Values are stored as JSON for flexibility and debugability
///
/// See docs/CONSUMER_OFFSET_STORAGE.md for detailed design.
class OffsetManager {
public:
    /// @brief Constructs an OffsetManager with the specified RocksDB path.
    /// @param db_path Path to RocksDB database directory
    /// @throws std::runtime_error if RocksDB cannot be opened
    explicit OffsetManager(const std::string& db_path);
    
    /// @brief Destructor - closes RocksDB connections
    ~OffsetManager();

    // Prevent copying and moving
    OffsetManager(const OffsetManager&) = delete;
    OffsetManager& operator=(const OffsetManager&) = delete;
    OffsetManager(OffsetManager&&) = delete;
    OffsetManager& operator=(OffsetManager&&) = delete;

    /// @brief Metadata associated with a committed offset
    struct OffsetMetadata {
        int64_t offset;              ///< Committed offset value
        std::string metadata;        ///< Consumer-provided metadata
        int64_t commit_timestamp;    ///< When offset was committed (ms since epoch)
        int64_t expiry_timestamp;    ///< When offset should expire (ms since epoch)
    };

    /// @brief Data for a single offset commit
    struct OffsetCommitData {
        std::string topic;
        int32_t partition;
        int64_t offset;
        std::string metadata;
    };

    /// @brief Key for identifying an offset
    struct OffsetKey {
        std::string group_id;
        std::string topic;
        int32_t partition;

        bool operator==(const OffsetKey& other) const {
            return group_id == other.group_id && 
                   topic == other.topic && 
                   partition == other.partition;
        }
    };

    //
    // Offset Operations
    //

    /// @brief Commits an offset for a consumer group partition.
    /// @param group_id Consumer group ID
    /// @param topic Topic name
    /// @param partition Partition number
    /// @param offset Offset value to commit
    /// @param metadata Optional consumer metadata
    /// @throws std::runtime_error on RocksDB write failure
    void commitOffset(
        const std::string& group_id,
        const std::string& topic,
        int32_t partition,
        int64_t offset,
        const std::string& metadata = "");

    /// @brief Fetches a committed offset (simple version).
    /// @param group_id Consumer group ID
    /// @param topic Topic name
    /// @param partition Partition number
    /// @return Committed offset value, or std::nullopt if not found
    std::optional<int64_t> fetchOffset(
        const std::string& group_id,
        const std::string& topic,
        int32_t partition) const;

    /// @brief Fetches a committed offset with full metadata.
    /// @param group_id Consumer group ID
    /// @param topic Topic name
    /// @param partition Partition number
    /// @return Offset metadata, or std::nullopt if not found
    std::optional<OffsetMetadata> fetchOffsetWithMetadata(
        const std::string& group_id,
        const std::string& topic,
        int32_t partition) const;

    /// @brief Commits multiple offsets atomically using RocksDB WriteBatch.
    /// @param group_id Consumer group ID
    /// @param offsets Vector of offset commit data
    /// @throws std::runtime_error on RocksDB write failure
    void commitOffsetBatch(
        const std::string& group_id,
        const std::vector<OffsetCommitData>& offsets);

    //
    // Group Management
    //

    /// @brief Member state in a consumer group
    struct MemberMetadata {
        std::string member_id;
        std::string client_id;
        std::string client_host;
        std::vector<uint8_t> metadata;
        std::vector<uint8_t> assignment;
    };

    /// @brief Complete group metadata
    struct GroupMetadata {
        std::string state;           ///< Group state: "Stable", "Dead", "Empty", etc.
        std::string protocol_type;   ///< Protocol type (e.g., "consumer")
        std::string protocol;        ///< Selected protocol name
        std::vector<MemberMetadata> members;
        int32_t generation;          ///< Current generation ID
        int64_t last_update_timestamp; ///< Last update time (ms since epoch)
    };

    /// @brief Persists group metadata to RocksDB.
    /// @param group_id Consumer group ID
    /// @param metadata Group metadata to persist
    /// @throws std::runtime_error on RocksDB write failure
    void saveGroupMetadata(const std::string& group_id, const GroupMetadata& metadata);

    /// @brief Loads group metadata from RocksDB.
    /// @param group_id Consumer group ID
    /// @return Group metadata, or std::nullopt if not found
    std::optional<GroupMetadata> loadGroupMetadata(const std::string& group_id) const;

    /// @brief Deletes all offsets and metadata for a consumer group.
    /// @param group_id Consumer group ID
    /// @throws std::runtime_error on RocksDB write failure
    void deleteGroup(const std::string& group_id);

    /// @brief Phase 4.1f: deletes a single (group, topic, partition) offset.
    /// @return true if a stored offset was deleted, false if it didn't exist.
    bool deleteOffset(const std::string& group_id,
                      const std::string& topic,
                      int32_t partition);

    /// @brief Lists all consumer groups that have committed offsets.
    /// @return Vector of group IDs
    std::vector<std::string> listGroups() const;

    /// @brief Lists all offset keys for a specific group.
    /// @param group_id Consumer group ID
    /// @return Vector of offset keys
    std::vector<OffsetKey> listOffsetsForGroup(const std::string& group_id) const;

    /// @brief Fetches all committed offsets for a specific group.
    /// @param group_id Consumer group ID
    /// @return Map of (topic, partition) -> committed offset
    std::map<std::pair<std::string, int32_t>, int64_t> fetchAllOffsets(
        const std::string& group_id) const;

    //
    // Cleanup (Future Enhancement)
    //

    /// @brief Deletes expired offsets based on retention policy.
    /// @param now_millis Current time in milliseconds since epoch
    /// @return Number of offsets deleted
    /// @note This will be called by a background thread in M3 Week 10
    size_t deleteExpiredOffsets(int64_t now_millis);

    /// @brief Gets the configured retention period in milliseconds.
    /// @return Retention period (default: 7 days)
    int64_t getRetentionMs() const { return retention_ms_; }

    /// @brief Sets the retention period in milliseconds.
    /// @param retention_ms Retention period
    void setRetentionMs(int64_t retention_ms) { retention_ms_ = retention_ms; }

private:
    /// @brief Constructs a RocksDB key for an offset.
    static std::string makeOffsetKey(
        const std::string& group_id,
        const std::string& topic,
        int32_t partition);

    /// @brief Constructs a RocksDB key prefix for a group's offsets.
    static std::string makeGroupOffsetPrefix(const std::string& group_id);

    /// @brief Constructs a RocksDB key for group metadata (future use).
    static std::string makeGroupMetadataKey(const std::string& group_id);

    /// @brief Parses an offset key back into components.
    static std::optional<OffsetKey> parseOffsetKey(const std::string& key);

    /// @brief Serializes offset metadata to JSON.
    static std::string serializeOffsetMetadata(const OffsetMetadata& metadata);

    /// @brief Deserializes offset metadata from JSON.
    static std::optional<OffsetMetadata> deserializeOffsetMetadata(const std::string& json);

    std::unique_ptr<rocksdb::DB> db_;
    int64_t retention_ms_;  ///< Offset retention period (default: 7 days)
};

}  // namespace kawasan::broker
