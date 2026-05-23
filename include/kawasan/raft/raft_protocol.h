#pragma once

#include <cstdint>
#include <vector>
#include <string>
#include <cstring>
#include <stdexcept>

#include "kawasan/common/types.h"

namespace kawasan::raft {

/// @brief Raft log entry
struct LogEntry {
    int64_t term;
    int64_t index;
    std::vector<uint8_t> data;
    std::string command_type;
};

/// @brief Request vote RPC
struct RequestVoteRequest {
    int64_t term;
    int candidate_id;
    int64_t last_log_index;
    int64_t last_log_term;
};

/// @brief Request vote response
struct RequestVoteResponse {
    int64_t term;
    bool vote_granted;
};

/// @brief Append entries RPC
struct AppendEntriesRequest {
    int64_t term;
    int leader_id;
    int64_t prev_log_index;
    int64_t prev_log_term;
    std::vector<LogEntry> entries;
    int64_t leader_commit;
};

/// @brief Append entries response
struct AppendEntriesResponse {
    int64_t term;
    bool success;
    int64_t last_log_index;
};

/**
 * @file raft_protocol.h
 * @brief Raft RPC protocol serialization/deserialization
 * 
 * This file defines the binary wire format for Raft RPC messages.
 * All messages use a simple binary format with the following structure:
 * 
 * Message Format:
 *   [Version: 1 byte] [MessageType: 1 byte] [Payload]
 * 
 * Version: Protocol version (currently 1)
 * MessageType: Type of RPC message
 * Payload: Message-specific data (big-endian)
 * 
 * Design Principles:
 * - Simple binary encoding for efficiency
 * - Version field for future compatibility
 * - Big-endian byte order (network order)
 * - Fixed-size fields first, variable-size fields last
 * - Length-prefixed variable-size data
 */

// Protocol version
constexpr uint8_t RAFT_PROTOCOL_VERSION = 1;

// Message types
enum class RaftMessageType : uint8_t {
    REQUEST_VOTE_REQ = 1,
    REQUEST_VOTE_RESP = 2,
    APPEND_ENTRIES_REQ = 3,
    APPEND_ENTRIES_RESP = 4,
    INSTALL_SNAPSHOT_REQ = 5,
    INSTALL_SNAPSHOT_RESP = 6
};

/**
 * @brief Exception thrown on protocol errors
 */
class ProtocolException : public std::runtime_error {
public:
    explicit ProtocolException(const std::string& msg) 
        : std::runtime_error(msg) {}
};

/**
 * @brief Buffer for encoding/decoding binary data
 */
class ByteBuffer {
public:
    ByteBuffer() = default;
    explicit ByteBuffer(std::vector<uint8_t> data) : data_(std::move(data)) {}
    
    // Write methods (append to buffer)
    void writeUInt8(uint8_t value);
    void writeInt32(int32_t value);
    void writeInt64(int64_t value);
    void writeBool(bool value);
    void writeString(const std::string& value);
    void writeBytes(const std::vector<uint8_t>& value);
    
    // Read methods (consume from buffer)
    uint8_t readUInt8();
    int32_t readInt32();
    int64_t readInt64();
    bool readBool();
    std::string readString();
    std::vector<uint8_t> readBytes();
    
    // Buffer management
    const std::vector<uint8_t>& data() const { return data_; }
    std::vector<uint8_t> release() { return std::move(data_); }
    size_t remaining() const { return data_.size() - pos_; }
    bool hasRemaining() const { return pos_ < data_.size(); }
    void reset() { pos_ = 0; }
    
private:
    std::vector<uint8_t> data_;
    size_t pos_ = 0;
    
    void ensureRemaining(size_t bytes);
};

/**
 * @brief Install snapshot RPC request
 * Used to send snapshot data to followers that are too far behind
 */
struct InstallSnapshotRequest {
    int64_t term;                      // Leader's term
    BrokerId leader_id;                 // Leader's broker ID
    int64_t last_included_index;        // Snapshot replaces all entries up to this index
    int64_t last_included_term;         // Term of last_included_index
    int64_t offset;                     // Byte offset where chunk is positioned
    std::vector<uint8_t> data;          // Snapshot chunk data
    bool done;                          // True if this is the last chunk
    
    // Serialization
    std::vector<uint8_t> encode() const;
    static InstallSnapshotRequest decode(const std::vector<uint8_t>& bytes);
};

/**
 * @brief Install snapshot RPC response
 */
struct InstallSnapshotResponse {
    int64_t term;                      // Current term for leader to update itself
    
    // Serialization
    std::vector<uint8_t> encode() const;
    static InstallSnapshotResponse decode(const std::vector<uint8_t>& bytes);
};

/**
 * @brief Serialization functions for RequestVoteRequest
 */
namespace RequestVoteRequestCodec {
    std::vector<uint8_t> encode(const RequestVoteRequest& req);
    RequestVoteRequest decode(const std::vector<uint8_t>& bytes);
}

/**
 * @brief Serialization functions for RequestVoteResponse
 */
namespace RequestVoteResponseCodec {
    std::vector<uint8_t> encode(const RequestVoteResponse& resp);
    RequestVoteResponse decode(const std::vector<uint8_t>& bytes);
}

/**
 * @brief Serialization functions for AppendEntriesRequest
 */
namespace AppendEntriesRequestCodec {
    std::vector<uint8_t> encode(const AppendEntriesRequest& req);
    AppendEntriesRequest decode(const std::vector<uint8_t>& bytes);
}

/**
 * @brief Serialization functions for AppendEntriesResponse
 */
namespace AppendEntriesResponseCodec {
    std::vector<uint8_t> encode(const AppendEntriesResponse& resp);
    AppendEntriesResponse decode(const std::vector<uint8_t>& bytes);
}

/**
 * @brief Helper to encode LogEntry
 */
namespace LogEntryCodec {
    void encodeInto(ByteBuffer& buffer, const LogEntry& entry);
    LogEntry decodeFrom(ByteBuffer& buffer);
}

}  // namespace kawasan::raft
