# Raft Protocol Specification

**Version**: 1.0  
**Created**: November 19, 2025  
**Status**: Implementation Complete

---

## Overview

This document specifies the binary wire protocol used for Raft RPC communication in Kawasan. The protocol is designed to be simple, efficient, and extensible for future enhancements.

### Design Goals

1. **Simplicity**: Easy to implement and debug
2. **Efficiency**: Minimal overhead for high-throughput replication
3. **Extensibility**: Version field allows protocol evolution
4. **Compatibility**: Big-endian (network order) for cross-platform compatibility
5. **Safety**: Built-in validation and error handling

---

## Protocol Format

### Message Structure

All Raft RPC messages follow this structure:

```
+----------+-------------+------------------+
| Version  | MessageType |     Payload      |
| (1 byte) |  (1 byte)   |   (variable)     |
+----------+-------------+------------------+
```

- **Version**: Protocol version (currently `1`)
- **MessageType**: Type of RPC message (see Message Types below)
- **Payload**: Message-specific data (big-endian encoding)

### Encoding Rules

1. **Byte Order**: All multi-byte integers use big-endian (network order)
2. **Fixed-size fields**: Placed before variable-size fields
3. **Variable-size data**: Length-prefixed (4-byte signed int32 length, then data)
4. **Strings**: UTF-8 encoded, length-prefixed
5. **Booleans**: Single byte (0 = false, 1 = true)
6. **Arrays**: Count-prefixed (4-byte signed int32 count, then elements)

### Data Types

| Type | Size | Encoding |
|------|------|----------|
| `uint8_t` | 1 byte | Direct value |
| `int32_t` | 4 bytes | Big-endian signed integer |
| `int64_t` | 8 bytes | Big-endian signed long |
| `bool` | 1 byte | 0 or 1 |
| `string` | 4 + N bytes | int32 length + UTF-8 bytes |
| `bytes` | 4 + N bytes | int32 length + raw bytes |

---

## Message Types

### 1. RequestVote RPC

**Purpose**: Invoked by candidates to gather votes during leader election.

#### RequestVoteRequest (Type = 1)

**Format**:
```
+----------+-------------+------+--------------+----------------+---------------+
| Version  | MessageType | Term | Candidate ID | Last Log Index | Last Log Term |
| (1 byte) |  (1 byte)   | (8)  |     (4)      |      (8)       |      (8)      |
+----------+-------------+------+--------------+----------------+---------------+
```

**Fields**:
- `term` (int64): Candidate's term
- `candidate_id` (int32): Candidate requesting vote (BrokerId)
- `last_log_index` (int64): Index of candidate's last log entry
- `last_log_term` (int64): Term of candidate's last log entry

**Semantics**:
- Sent by candidate during election to all peers
- Receiver grants vote if:
  - Request term ≥ receiver's current term
  - Receiver hasn't voted in this term, or already voted for this candidate
  - Candidate's log is at least as up-to-date as receiver's log

#### RequestVoteResponse (Type = 2)

**Format**:
```
+----------+-------------+------+--------------+
| Version  | MessageType | Term | Vote Granted |
| (1 byte) |  (1 byte)   | (8)  |    (1)       |
+----------+-------------+------+--------------+
```

**Fields**:
- `term` (int64): Current term for candidate to update itself
- `vote_granted` (bool): True if candidate received vote

**Semantics**:
- Response to RequestVoteRequest
- If `term > candidate's term`, candidate becomes follower
- If `vote_granted = true`, candidate counts this vote
- Candidate needs majority to become leader

---

### 2. AppendEntries RPC

**Purpose**: Invoked by leader to replicate log entries and provide heartbeat.

#### AppendEntriesRequest (Type = 3)

**Format**:
```
+----------+-------------+------+-----------+----------------+---------------+--------------+----------------+
| Version  | MessageType | Term | Leader ID | Prev Log Index | Prev Log Term | Leader Commit| Entry Count (N)|
| (1 byte) |  (1 byte)   | (8)  |    (4)    |      (8)       |      (8)      |      (8)     |      (4)       |
+----------+-------------+------+-----------+----------------+---------------+--------------+----------------+
              [Entry 1]  [Entry 2]  ...  [Entry N]
```

**Fields**:
- `term` (int64): Leader's term
- `leader_id` (int32): Leader's broker ID (for followers to redirect clients)
- `prev_log_index` (int64): Index of log entry immediately preceding new ones
- `prev_log_term` (int64): Term of `prev_log_index` entry
- `leader_commit` (int64): Leader's commit index
- `entries` (array): Log entries to store (empty for heartbeat; may send more than one for efficiency)

**LogEntry Format**:
```
+------+-------+--------------+-------------+--------+
| Term | Index | Command Type | Data Length |  Data  |
| (8)  |  (8)  |   (4 + N)    |     (4)     | (var)  |
+------+-------+--------------+-------------+--------+
```

- `term` (int64): Term when entry was received by leader
- `index` (int64): Position in log
- `command_type` (string): Type of command (e.g., "CREATE_TOPIC", "DELETE_TOPIC")
- `data` (bytes): Serialized command data

**Semantics**:
- Sent by leader to all followers
- Empty `entries` = heartbeat (prevents election timeout)
- Receiver rejects if:
  - `term < receiver's term`
  - Log doesn't contain entry at `prev_log_index` with `prev_log_term`
- On success, follower:
  - Appends entries to log
  - Updates commit index to `min(leader_commit, index of last new entry)`

#### AppendEntriesResponse (Type = 4)

**Format**:
```
+----------+-------------+------+---------+----------------+
| Version  | MessageType | Term | Success | Last Log Index |
| (1 byte) |  (1 byte)   | (8)  |   (1)   |      (8)       |
+----------+-------------+------+---------+----------------+
```

**Fields**:
- `term` (int64): Current term for leader to update itself
- `success` (bool): True if follower contained entry matching `prev_log_index` and `prev_log_term`
- `last_log_index` (int64): Index of last log entry (for leader to update `next_index`)

**Semantics**:
- Response to AppendEntriesRequest
- If `success = false`, leader decrements `next_index` and retries
- If `success = true`, leader updates `match_index` and `next_index` for this follower

---

### 3. InstallSnapshot RPC

**Purpose**: Invoked by leader to send snapshot chunks to followers that are too far behind.

#### InstallSnapshotRequest (Type = 5)

**Format**:
```
+----------+-------------+------+-----------+--------------------+-------------------+--------+------+-------------+--------+
| Version  | MessageType | Term | Leader ID | Last Included Index| Last Included Term| Offset | Done | Data Length |  Data  |
| (1 byte) |  (1 byte)   | (8)  |    (4)    |         (8)        |        (8)        |  (8)   | (1)  |     (4)     | (var)  |
+----------+-------------+------+-----------+--------------------+-------------------+--------+------+-------------+--------+
```

**Fields**:
- `term` (int64): Leader's term
- `leader_id` (int32): Leader's broker ID
- `last_included_index` (int64): Snapshot replaces all entries up through and including this index
- `last_included_term` (int64): Term of `last_included_index`
- `offset` (int64): Byte offset where chunk is positioned in the snapshot file
- `done` (bool): True if this is the last chunk
- `data` (bytes): Raw bytes of the snapshot chunk

**Semantics**:
- Used when follower is too far behind and leader has discarded log entries
- Leader sends snapshot in chunks (to avoid large messages)
- Follower:
  - Discards entire log if `last_included_index ≥ last log index`
  - Discards prefix of log if snapshot overlaps
  - Saves snapshot to disk
  - Updates state machine with snapshot contents

#### InstallSnapshotResponse (Type = 6)

**Format**:
```
+----------+-------------+------+
| Version  | MessageType | Term |
| (1 byte) |  (1 byte)   | (8)  |
+----------+-------------+------+
```

**Fields**:
- `term` (int64): Current term for leader to update itself

**Semantics**:
- Simple acknowledgment
- Leader continues sending next chunk if `done = false`
- If `term > leader's term`, leader becomes follower

---

## State Machine Transitions

### Node States

Raft nodes can be in one of three states:

1. **Follower**: 
   - Passive state, responds to RPCs from leaders and candidates
   - If election timeout elapses without heartbeat → becomes candidate

2. **Candidate**: 
   - Actively seeking votes
   - Increments term, votes for self, sends RequestVote RPCs to all peers
   - If receives majority votes → becomes leader
   - If receives AppendEntries from valid leader → becomes follower
   - If election timeout elapses → starts new election

3. **Leader**: 
   - Handles all client requests
   - Sends periodic heartbeats (empty AppendEntries) to all followers
   - Replicates log entries to followers
   - If discovers higher term → becomes follower

### State Transition Diagram

```
┌──────────┐
│ Follower │◄────────────────────────┐
└─────┬────┘                         │
      │ timeout, start election     │ discover current leader
      │                              │ or higher term
      ▼                              │
┌───────────┐                  ┌──────────┐
│ Candidate │─────────────────►│  Leader  │
└───────────┘  receive votes   └──────────┘
      │         from majority        │
      │                              │
      └──────────────────────────────┘
       timeout, start new election
```

---

## Error Handling

### Protocol Errors

1. **Version Mismatch**:
   - Error: `ProtocolException("Unsupported protocol version: X")`
   - Action: Close connection, log error
   - Future: Negotiation handshake for version compatibility

2. **Wrong Message Type**:
   - Error: `ProtocolException("Wrong message type: expected X")`
   - Action: Ignore message, log warning
   - Reason: Possible concurrent state change

3. **Buffer Underflow**:
   - Error: `ProtocolException("Buffer underflow: need X bytes, have Y")`
   - Action: Close connection, log error
   - Reason: Incomplete/truncated message

4. **Invalid Field Values**:
   - Error: `ProtocolException("Invalid string/bytes length: X")`
   - Action: Close connection, log error
   - Reason: Corrupted message or malicious input

### Application-Level Errors

1. **Stale Term**:
   - Condition: `request.term < receiver.current_term`
   - Action: Reject request, return current term
   - Requester updates its term and becomes follower

2. **Log Inconsistency**:
   - Condition: Follower's log doesn't match at `prev_log_index`
   - Action: AppendEntries returns `success = false`
   - Leader decrements `next_index` and retries

3. **Split Brain**:
   - Condition: Multiple nodes claim to be leader
   - Detection: Receiving AppendEntries from another leader
   - Resolution: Node with lower term becomes follower

### Network Errors

1. **Connection Timeout**:
   - Retry with exponential backoff (50ms, 100ms, 200ms, ...)
   - Max retries: 3 (configurable)
   - Mark peer as unreachable after max retries

2. **Connection Refused**:
   - Peer node is down or not listening
   - Action: Wait for peer to reconnect (heartbeat will resume)

3. **Partial Write/Read**:
   - TCP guarantees: Retry until complete message sent/received
   - Timeout: 30 seconds per message (configurable)

---

## Implementation Notes

### Performance Considerations

1. **Message Batching**:
   - Leader can send multiple log entries in one AppendEntries RPC
   - Balance between latency and throughput
   - Recommended: Batch size = 100 entries or 1MB, whichever comes first

2. **Heartbeat Frequency**:
   - Recommended: 50ms interval (configurable)
   - Must be << election timeout to prevent spurious elections
   - Election timeout: 150-300ms randomized

3. **Snapshot Chunking**:
   - Chunk size: 64KB (configurable)
   - Allows snapshots to be sent without blocking other RPCs
   - Progress can be tracked/resumed

### Security Considerations

1. **Authentication** (future):
   - Add HMAC or signature to each message
   - Validate sender identity before processing RPC

2. **Encryption** (future):
   - Use TLS for all Raft connections
   - Separate TLS config from Kafka protocol TLS

3. **Rate Limiting** (future):
   - Limit RPC rate per peer to prevent DoS
   - Drop excessive RequestVote RPCs during election storm

---

## Testing

### Unit Tests

1. **Serialization Round-Trip**:
   - Encode → Decode for all message types
   - Verify fields match original

2. **Error Cases**:
   - Invalid version, wrong message type
   - Buffer underflow, negative lengths
   - Verify exceptions thrown correctly

3. **Edge Cases**:
   - Empty log entries array (heartbeat)
   - Large log entries (1MB+)
   - Maximum field values (int64 max)

### Integration Tests

1. **Leader Election**:
   - 3 nodes, kill leader, verify new leader elected
   - Verify RequestVote RPCs exchanged correctly

2. **Log Replication**:
   - Append entries on leader, verify replication to followers
   - Verify AppendEntries RPCs with correct prev_log_index/term

3. **Snapshot Transfer**:
   - Leader with compact log sends snapshot to new follower
   - Verify InstallSnapshot RPCs with chunking

---

## Future Extensions

### Version 2 Enhancements (Planned)

1. **Compression**:
   - Add compression flag to header
   - Support Snappy/LZ4/Zstd for large payloads

2. **Pre-Vote**:
   - Add PreVote phase to prevent disruptive candidates
   - New message types: `PRE_VOTE_REQ`, `PRE_VOTE_RESP`

3. **Joint Consensus**:
   - Support for configuration changes (add/remove nodes)
   - New message types for configuration log entries

4. **Read-Only Requests**:
   - Optimize read-heavy workloads
   - Verify leadership without log append

---

## References

1. **Raft Paper**: [In Search of an Understandable Consensus Algorithm](https://raft.github.io/raft.pdf)
2. **Raft Website**: https://raft.github.io/
3. **Kafka KRaft**: [KIP-500: Replace ZooKeeper with a Self-Managed Metadata Quorum](https://cwiki.apache.org/confluence/display/KAFKA/KIP-500)

---

## Changelog

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | 2025-11-19 | Initial protocol specification and implementation |

---

## Appendix: Message Size Calculations

### RequestVoteRequest
- Header: 2 bytes
- Payload: 8 + 4 + 8 + 8 = 28 bytes
- **Total**: 30 bytes

### RequestVoteResponse
- Header: 2 bytes
- Payload: 8 + 1 = 9 bytes
- **Total**: 11 bytes

### AppendEntriesRequest (empty, heartbeat)
- Header: 2 bytes
- Payload: 8 + 4 + 8 + 8 + 8 + 4 = 40 bytes
- **Total**: 42 bytes

### AppendEntriesRequest (with 1 entry of 1KB)
- Header: 2 bytes
- Payload: 40 bytes (fixed) + (8 + 8 + 4 + len(command_type) + 4 + 1024) ≈ 1088 bytes
- **Total**: ≈ 1130 bytes

### InstallSnapshotRequest (64KB chunk)
- Header: 2 bytes
- Payload: 8 + 4 + 8 + 8 + 8 + 1 + 4 + 65536 = 65577 bytes
- **Total**: 65579 bytes

---

**End of Document**
