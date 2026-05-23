# Consumer Offset Storage Design

**Created**: November 19, 2025  
**Status**: Implementation Phase  
**Related Milestone**: M3 Week 9 - Offset Persistence

---

## 1. Overview

This document describes the design for persisting consumer group offsets and metadata to disk using RocksDB. This implementation replaces the current in-memory offset storage in `GroupCoordinator` and enables consumer groups to survive broker restarts.

### Goals

1. **Persistence**: Consumer offsets must survive broker restarts
2. **Performance**: Offset commits should not significantly impact throughput
3. **Consistency**: Offset reads must reflect all committed writes
4. **Simplicity**: Use RocksDB (already a dependency) rather than internal topics

### Non-Goals (Future Work)

- Compaction and log-based storage (Kafka's `__consumer_offsets` approach)
- Cross-broker offset replication (requires Raft networking from M2)
- Offset expiration and cleanup (scheduled for M3 Week 10)

---

## 2. Storage Backend

### Choice: RocksDB Column Family

**Decision**: Use a dedicated RocksDB column family for consumer offsets.

**Rationale**:
- ✅ **Simplicity**: Avoids implementing internal topic mechanics
- ✅ **Performance**: Native key-value lookups, built-in batching
- ✅ **Atomicity**: RocksDB WriteBatch provides atomic multi-key commits
- ✅ **Already integrated**: Kawasan already uses RocksDB for log storage
- ✅ **Development velocity**: Faster to implement than internal topic approach

**Alternative considered**: Internal `__consumer_offsets` topic (Kafka-compatible)
- ❌ More complex: requires implementing compaction, replication-aware writes
- ❌ Deferred to future milestone when full Kafka compatibility is prioritized

---

## 3. Schema Design

### 3.1 Offset Storage

Each committed offset is stored as a separate key-value pair in RocksDB.

**Key Format**:
```
offset:<group_id>:<topic>:<partition>
```

**Value Format** (JSON):
```json
{
  "offset": 12345,
  "metadata": "consumer-metadata-string",
  "commit_timestamp": 1700419200000,
  "expiry_timestamp": 1701024000000
}
```

**Field Descriptions**:
- `offset` (int64): Committed offset value
- `metadata` (string): Optional consumer-provided metadata (e.g., commit reason, consumer instance info)
- `commit_timestamp` (int64): Unix timestamp (milliseconds) when offset was committed
- `expiry_timestamp` (int64): Unix timestamp when this offset should expire (commit_timestamp + retention period)

**Example Key**:
```
offset:my-consumer-group:orders-topic:2
```

**Key Design Rationale**:
- Prefix `offset:` enables efficient range scans (e.g., all offsets for a group)
- Hierarchical structure supports partial queries (all groups, all offsets for a topic, etc.)
- Fixed separator `:` simplifies parsing (assumes group/topic names don't contain `:`)

### 3.2 Group Metadata Storage

Group state (generation ID, protocol, members) is stored separately to allow atomic group updates.

**Key Format**:
```
group:<group_id>
```

**Value Format** (JSON):
```json
{
  "generation_id": 5,
  "protocol_type": "consumer",
  "protocol_name": "range",
  "leader_id": "my-group-member-1",
  "state": "Stable",
  "members": [
    {
      "member_id": "my-group-member-1",
      "client_id": "my-consumer",
      "client_host": "192.168.1.10",
      "metadata": "base64-encoded-metadata",
      "assignment": "base64-encoded-assignment"
    }
  ],
  "last_modified": 1700419200000
}
```

**Field Descriptions**:
- `generation_id` (int32): Current generation of the group (incremented on rebalance)
- `protocol_type` (string): Protocol type (typically "consumer")
- `protocol_name` (string): Selected protocol (e.g., "range", "roundrobin")
- `leader_id` (string): Member ID of the group leader
- `state` (string): Group state ("Empty", "Stable", "PreparingRebalance", "Dead")
- `members` (array): List of group members with their metadata and assignments
- `last_modified` (int64): Unix timestamp of last metadata update

**Example Key**:
```
group:my-consumer-group
```

---

## 4. RocksDB Configuration

### Column Family Setup

```cpp
// Create dedicated column family for consumer offsets
rocksdb::ColumnFamilyOptions offset_cf_options;
offset_cf_options.OptimizeForPointLookup(64);  // MB of block cache for lookups
offset_cf_options.max_write_buffer_number = 2;
offset_cf_options.write_buffer_size = 16 * 1024 * 1024;  // 16 MB

// Open with column family
std::vector<rocksdb::ColumnFamilyDescriptor> column_families = {
    {rocksdb::kDefaultColumnFamilyName, rocksdb::ColumnFamilyOptions()},
    {"consumer_offsets", offset_cf_options}
};
```

### Write Options

```cpp
rocksdb::WriteOptions write_opts;
write_opts.sync = true;  // fsync for durability (configurable via broker config)
write_opts.disableWAL = false;  // Keep WAL for crash recovery
```

### Read Options

```cpp
rocksdb::ReadOptions read_opts;
read_opts.verify_checksums = true;  // Verify data integrity
```

---

## 5. API Design

### 5.1 OffsetManager Interface

```cpp
namespace kawasan::broker {

class OffsetManager {
public:
    // Constructor: opens RocksDB at given path
    explicit OffsetManager(const std::string& db_path);
    ~OffsetManager();

    // Offset operations
    void commitOffset(
        const std::string& group_id,
        const std::string& topic,
        int32_t partition,
        int64_t offset,
        const std::string& metadata = "");

    std::optional<int64_t> fetchOffset(
        const std::string& group_id,
        const std::string& topic,
        int32_t partition) const;

    struct OffsetMetadata {
        int64_t offset;
        std::string metadata;
        int64_t commit_timestamp;
        int64_t expiry_timestamp;
    };

    std::optional<OffsetMetadata> fetchOffsetWithMetadata(
        const std::string& group_id,
        const std::string& topic,
        int32_t partition) const;

    // Group metadata operations
    void saveGroupMetadata(
        const std::string& group_id,
        const GroupMetadata& metadata);

    std::optional<GroupMetadata> loadGroupMetadata(
        const std::string& group_id) const;

    // Batch operations (for atomic commits)
    void commitOffsetBatch(
        const std::string& group_id,
        const std::vector<OffsetCommitData>& offsets);

    // Group deletion
    void deleteGroup(const std::string& group_id);

    // Query operations
    std::vector<std::string> listGroups() const;
    std::vector<OffsetKey> listOffsetsForGroup(const std::string& group_id) const;

    // Cleanup (for expiration - future enhancement)
    size_t deleteExpiredOffsets(int64_t now_millis);

private:
    std::unique_ptr<rocksdb::DB> db_;
    rocksdb::ColumnFamilyHandle* offset_cf_;
    rocksdb::ColumnFamilyHandle* group_cf_;
};

}  // namespace kawasan::broker
```

### 5.2 Usage Example

```cpp
// Initialize OffsetManager
OffsetManager offset_mgr("/var/kawasan/data/offsets");

// Commit offset
offset_mgr.commitOffset("my-group", "orders", 0, 12345, "auto-commit");

// Fetch offset
auto offset = offset_mgr.fetchOffset("my-group", "orders", 0);
if (offset.has_value()) {
    std::cout << "Last committed offset: " << *offset << std::endl;
}

// Batch commit (atomic)
std::vector<OffsetCommitData> batch = {
    {"orders", 0, 12345, ""},
    {"orders", 1, 67890, ""},
    {"shipments", 0, 111, ""}
};
offset_mgr.commitOffsetBatch("my-group", batch);

// Delete group (removes all offsets and metadata)
offset_mgr.deleteGroup("my-group");
```

---

## 6. Integration with GroupCoordinator

### Current Flow (In-Memory)

```
handleOffsetCommit() 
  → GroupState::committed_offsets[key] = value  (in-memory map)
  → return success
```

### New Flow (Persistent)

```
handleOffsetCommit()
  → offsetManager->commitOffset(group, topic, partition, offset, metadata)
    → RocksDB::Put(key, JSON(value))
    → return success
  → return success
```

### Migration Strategy

1. **Phase 1** (This milestone):
   - Add `OffsetManager` member to `GroupCoordinator`
   - Replace in-memory `committed_offsets` map with `OffsetManager` calls
   - Keep in-memory group state (members, assignments) as-is for now

2. **Phase 2** (M3 Week 10):
   - Persist group metadata to RocksDB
   - Remove in-memory group state entirely
   - Implement group expiration

### Code Changes

**`group_coordinator.h`**:
```cpp
class GroupCoordinator {
public:
    explicit GroupCoordinator(std::shared_ptr<OffsetManager> offset_manager);
    
    // ... existing methods ...
    
private:
    std::shared_ptr<OffsetManager> offset_manager_;  // NEW
    
    struct GroupState {
        // Remove: std::unordered_map<OffsetKey, OffsetValue> committed_offsets;
        // Offsets now stored in OffsetManager
    };
};
```

**`group_coordinator.cpp`**:
```cpp
void GroupCoordinator::handleOffsetCommit(...) {
    // ... validation logic ...
    
    // OLD: group->committed_offsets[key] = value;
    // NEW:
    offset_manager_->commitOffset(
        request.groupId(),
        topic_request.topic,
        partition_request.partition,
        partition_request.offset,
        partition_request.metadata
    );
}
```

---

## 7. Retention and Expiration

### Retention Policy

**Configuration**:
```properties
# broker.properties
offsets.retention.minutes=10080  # 7 days (default)
```

**Implementation** (M3 Week 10):
- Background thread in `OffsetManager` runs every 10 minutes
- Scans all offset keys with `expiry_timestamp < now`
- Deletes expired offsets using RocksDB batch delete
- Logs count of deleted offsets

**Formula**:
```
expiry_timestamp = commit_timestamp + (offsets.retention.minutes * 60 * 1000)
```

### Group Cleanup

**Conditions for deletion**:
- No active members (empty member list)
- No offset commits in retention window
- Group state is "Dead" or "Empty"

**Safety**:
- Never delete groups with active members
- Always log group deletions at WARN level

---

## 8. Performance Considerations

### Write Performance

**Optimization: Batch Commits**
- Use `rocksdb::WriteBatch` for multi-partition offset commits
- Reduces fsync overhead from O(N) to O(1)

**Benchmark Target**:
- 10,000 offset commits/sec (single broker)
- <1ms p99 latency for commit

### Read Performance

**Optimization: Read Caching**
- RocksDB block cache (64 MB default)
- LRU cache for frequently accessed offsets
- Consider in-memory cache layer if needed

**Benchmark Target**:
- 50,000 offset fetches/sec
- <0.5ms p99 latency for fetch

### Disk Space

**Estimation**:
- Key size: ~50 bytes (group ID + topic + partition)
- Value size: ~150 bytes (JSON with metadata)
- Total per offset: ~200 bytes

**Example**:
- 100 consumer groups × 100 topics × 10 partitions = 100,000 offsets
- Disk usage: 100,000 × 200 bytes = ~20 MB
- With retention and overhead: ~50 MB

---

## 9. Error Handling

### Write Failures

**Scenarios**:
- Disk full
- RocksDB corruption
- I/O errors

**Handling**:
```cpp
try {
    offset_manager_->commitOffset(...);
    return ErrorCode::NONE;
} catch (const std::exception& e) {
    Logger::error("Failed to commit offset: {}", e.what());
    return ErrorCode::STORAGE_ERROR;  // or COORDINATOR_NOT_AVAILABLE
}
```

### Read Failures

**Scenarios**:
- Key not found (normal - return default offset)
- RocksDB read error (log and return error)

**Handling**:
```cpp
auto offset = offset_manager_->fetchOffset(...);
if (!offset.has_value()) {
    // Not an error - offset never committed, return 0
    return 0;
}
```

### Recovery from Corruption

**Strategy**:
1. Detect corruption on startup (RocksDB repair)
2. If unrepairable, delete and start fresh
3. Log at FATAL level - consumer groups will lose offsets
4. Document recovery procedure in ops guide

---

## 10. Testing Strategy

### Unit Tests

**File**: `tests/unit/offset_manager_test.cpp`

**Test Cases**:
1. Commit and fetch single offset
2. Commit and fetch multiple offsets (different groups)
3. Overwrite existing offset
4. Fetch non-existent offset (returns std::nullopt)
5. Batch commit atomicity
6. Delete group removes all offsets
7. List groups returns all groups
8. List offsets for group

### Integration Tests

**File**: `tests/integration/offset_persistence_test.cpp`

**Test Scenarios**:
1. **Restart Test**:
   - Start broker
   - Commit offsets via Kafka protocol
   - Stop broker
   - Start broker
   - Fetch offsets - verify they match

2. **Multiple Groups**:
   - Create 3 consumer groups
   - Commit offsets for each
   - Restart broker
   - Verify all groups' offsets persist

3. **Large Offset Values**:
   - Commit offset near INT64_MAX
   - Verify correct retrieval

4. **Concurrent Commits**:
   - Multiple consumer threads commit simultaneously
   - Verify no data corruption

### End-to-End Tests

**File**: `scripts/tests/test_simple_offset_commit.py` (enhanced)

**New Test**:
```python
def test_offset_persistence_across_restart():
    # Start broker
    broker = start_broker()
    
    # Commit offset
    producer.send('test-topic', b'msg')
    consumer.commit()
    committed_offset = consumer.committed(partition)
    
    # Restart broker
    broker.stop()
    broker = start_broker()
    
    # Verify offset persisted
    new_consumer = create_consumer(group_id='same-group')
    assert new_consumer.committed(partition) == committed_offset
```

---

## 11. Migration and Rollout

### Compatibility

**Backward Compatibility**:
- New code reads from RocksDB first
- If not found, checks in-memory map (for smooth transition)
- After first restart, all offsets are in RocksDB

**Forward Compatibility**:
- Rolling back requires clearing RocksDB offset data
- Document in release notes: "Warning: Downgrading will lose committed offsets"

### Deployment Steps

1. Deploy new broker binary
2. Restart broker (graceful shutdown recommended)
3. Offsets stored in memory are lost (expected behavior change)
4. New offsets persist across restarts

### Monitoring

**Metrics to Add**:
- `offset_commit_count` - total commits
- `offset_commit_latency_ms` - p50/p99/p999
- `offset_fetch_count` - total fetches
- `offset_storage_size_bytes` - RocksDB disk usage
- `offset_cleanup_count` - expired offsets deleted

---

## 12. Future Enhancements

### Phase 1: Current Implementation (M3 Week 9)
- ✅ RocksDB-backed offset storage
- ✅ Basic persistence and fetch
- ✅ Integration with GroupCoordinator

### Phase 2: Group Metadata (M3 Week 10)
- Persist group state (generation, members, protocol)
- Implement group expiration
- Full state recovery on restart

### Phase 3: Optimization (M4+)
- In-memory caching layer
- Batch write optimization
- Compaction strategy

### Phase 4: Replication (M2+)
- Replicate offsets across brokers via Raft
- Consistent offset reads from followers

### Phase 5: Kafka Compatibility (M10+)
- Migrate to `__consumer_offsets` internal topic
- Full Kafka protocol compatibility for offset storage

---

## 13. References

### Kafka Documentation
- [KIP-101: Consumer Offset Storage](https://cwiki.apache.org/confluence/display/KAFKA/KIP-101+-+Alter+Replication+Protocol+to+use+Leader+Epoch+rather+than+High+Watermark+for+Truncation)
- [Kafka Consumer Internals](https://kafka.apache.org/documentation/#consumerconfigs)

### RocksDB Documentation
- [RocksDB Column Families](https://github.com/facebook/rocksdb/wiki/Column-Families)
- [RocksDB WriteBatch](https://github.com/facebook/rocksdb/wiki/Basic-Operations#atomic-updates)

### Related Kawasan Documents
- `docs/ARCHITECTURE.md` - Overall system architecture
- `docs/PROJECT_STATUS.md` - Current implementation status
- `plan.md` - Implementation roadmap

---

## 14. Appendix: Key Examples

### Example 1: Basic Offset Commit

**Operation**: Consumer commits offset for partition
```
Group: "my-group"
Topic: "orders"
Partition: 2
Offset: 12345
```

**RocksDB Key**:
```
offset:my-group:orders:2
```

**RocksDB Value**:
```json
{
  "offset": 12345,
  "metadata": "",
  "commit_timestamp": 1700419200000,
  "expiry_timestamp": 1701024000000
}
```

### Example 2: Group Metadata

**Operation**: Save group metadata after rebalance
```
Group: "my-group"
Members: ["consumer-1", "consumer-2"]
Generation: 5
```

**RocksDB Key**:
```
group:my-group
```

**RocksDB Value**:
```json
{
  "generation_id": 5,
  "protocol_type": "consumer",
  "protocol_name": "range",
  "leader_id": "consumer-1",
  "state": "Stable",
  "members": [
    {
      "member_id": "consumer-1",
      "client_id": "my-app",
      "client_host": "192.168.1.10",
      "metadata": "...",
      "assignment": "..."
    },
    {
      "member_id": "consumer-2",
      "client_id": "my-app",
      "client_host": "192.168.1.11",
      "metadata": "...",
      "assignment": "..."
    }
  ],
  "last_modified": 1700419200000
}
```

---

## 15. Decision Log

| Date | Decision | Rationale |
|------|----------|-----------|
| 2025-11-19 | Use RocksDB column family instead of internal topic | Faster implementation, sufficient for M3 goals |
| 2025-11-19 | JSON value format | Human-readable, flexible schema evolution |
| 2025-11-19 | Separate keys for offsets vs group metadata | Allows atomic group updates, simpler queries |
| 2025-11-19 | sync=true for writes | Prioritize durability over throughput for M3 |
| 2025-11-19 | Defer group metadata persistence to Week 10 | Incremental implementation, offsets are higher priority |

---

**End of Document**
