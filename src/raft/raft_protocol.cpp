#include "kawasan/raft/raft_protocol.h"
#include <arpa/inet.h>  // for htonl, ntohl

namespace kawasan::raft {

// Helper functions for byte order conversion (big-endian/network order)
namespace {
    inline int64_t host_to_network_64(int64_t value) {
        // Convert host byte order to network byte order (64-bit)
        #if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
            return __builtin_bswap64(value);
        #else
            return value;
        #endif
    }
    
    inline int64_t network_to_host_64(int64_t value) {
        // Convert network byte order to host byte order (64-bit)
        return host_to_network_64(value);  // Same operation
    }
}

// ============================================================================
// ByteBuffer Implementation
// ============================================================================

void ByteBuffer::writeUInt8(uint8_t value) {
    data_.push_back(value);
}

void ByteBuffer::writeInt32(int32_t value) {
    uint32_t net_value = htonl(static_cast<uint32_t>(value));
    const uint8_t* bytes = reinterpret_cast<const uint8_t*>(&net_value);
    data_.insert(data_.end(), bytes, bytes + sizeof(net_value));
}

void ByteBuffer::writeInt64(int64_t value) {
    uint64_t net_value = host_to_network_64(static_cast<uint64_t>(value));
    const uint8_t* bytes = reinterpret_cast<const uint8_t*>(&net_value);
    data_.insert(data_.end(), bytes, bytes + sizeof(net_value));
}

void ByteBuffer::writeBool(bool value) {
    writeUInt8(value ? 1 : 0);
}

void ByteBuffer::writeString(const std::string& value) {
    // Write length as int32, then string bytes
    writeInt32(static_cast<int32_t>(value.size()));
    data_.insert(data_.end(), value.begin(), value.end());
}

void ByteBuffer::writeBytes(const std::vector<uint8_t>& value) {
    // Write length as int32, then bytes
    writeInt32(static_cast<int32_t>(value.size()));
    data_.insert(data_.end(), value.begin(), value.end());
}

uint8_t ByteBuffer::readUInt8() {
    ensureRemaining(1);
    return data_[pos_++];
}

int32_t ByteBuffer::readInt32() {
    ensureRemaining(4);
    uint32_t net_value;
    std::memcpy(&net_value, &data_[pos_], sizeof(net_value));
    pos_ += 4;
    return static_cast<int32_t>(ntohl(net_value));
}

int64_t ByteBuffer::readInt64() {
    ensureRemaining(8);
    uint64_t net_value;
    std::memcpy(&net_value, &data_[pos_], sizeof(net_value));
    pos_ += 8;
    return static_cast<int64_t>(network_to_host_64(net_value));
}

bool ByteBuffer::readBool() {
    return readUInt8() != 0;
}

std::string ByteBuffer::readString() {
    int32_t length = readInt32();
    if (length < 0) {
        throw ProtocolException("Invalid string length: " + std::to_string(length));
    }
    ensureRemaining(length);
    std::string result(reinterpret_cast<const char*>(&data_[pos_]), length);
    pos_ += length;
    return result;
}

std::vector<uint8_t> ByteBuffer::readBytes() {
    int32_t length = readInt32();
    if (length < 0) {
        throw ProtocolException("Invalid bytes length: " + std::to_string(length));
    }
    ensureRemaining(length);
    std::vector<uint8_t> result(data_.begin() + pos_, data_.begin() + pos_ + length);
    pos_ += length;
    return result;
}

void ByteBuffer::ensureRemaining(size_t bytes) {
    if (remaining() < bytes) {
        throw ProtocolException("Buffer underflow: need " + std::to_string(bytes) + 
                                " bytes, have " + std::to_string(remaining()));
    }
}

// ============================================================================
// LogEntry Codec
// ============================================================================

void LogEntryCodec::encodeInto(ByteBuffer& buffer, const LogEntry& entry) {
    buffer.writeInt64(entry.term);
    buffer.writeInt64(entry.index);
    buffer.writeString(entry.command_type);
    buffer.writeBytes(entry.data);
}

LogEntry LogEntryCodec::decodeFrom(ByteBuffer& buffer) {
    LogEntry entry;
    entry.term = buffer.readInt64();
    entry.index = buffer.readInt64();
    entry.command_type = buffer.readString();
    entry.data = buffer.readBytes();
    return entry;
}

// ============================================================================
// RequestVoteRequest Codec
// ============================================================================

std::vector<uint8_t> RequestVoteRequestCodec::encode(const RequestVoteRequest& req) {
    ByteBuffer buffer;
    
    // Header
    buffer.writeUInt8(RAFT_PROTOCOL_VERSION);
    buffer.writeUInt8(static_cast<uint8_t>(RaftMessageType::REQUEST_VOTE_REQ));
    
    // Payload
    buffer.writeInt64(req.term);
    buffer.writeInt32(req.candidate_id);
    buffer.writeInt64(req.last_log_index);
    buffer.writeInt64(req.last_log_term);
    
    return buffer.release();
}

RequestVoteRequest RequestVoteRequestCodec::decode(const std::vector<uint8_t>& bytes) {
    ByteBuffer buffer(bytes);
    
    // Verify header
    uint8_t version = buffer.readUInt8();
    if (version != RAFT_PROTOCOL_VERSION) {
        throw ProtocolException("Unsupported protocol version: " + std::to_string(version));
    }
    
    uint8_t msg_type = buffer.readUInt8();
    if (msg_type != static_cast<uint8_t>(RaftMessageType::REQUEST_VOTE_REQ)) {
        throw ProtocolException("Wrong message type: expected REQUEST_VOTE_REQ");
    }
    
    // Decode payload
    RequestVoteRequest req;
    req.term = buffer.readInt64();
    req.candidate_id = buffer.readInt32();
    req.last_log_index = buffer.readInt64();
    req.last_log_term = buffer.readInt64();
    
    return req;
}

// ============================================================================
// RequestVoteResponse Codec
// ============================================================================

std::vector<uint8_t> RequestVoteResponseCodec::encode(const RequestVoteResponse& resp) {
    ByteBuffer buffer;
    
    // Header
    buffer.writeUInt8(RAFT_PROTOCOL_VERSION);
    buffer.writeUInt8(static_cast<uint8_t>(RaftMessageType::REQUEST_VOTE_RESP));
    
    // Payload
    buffer.writeInt64(resp.term);
    buffer.writeBool(resp.vote_granted);
    
    return buffer.release();
}

RequestVoteResponse RequestVoteResponseCodec::decode(const std::vector<uint8_t>& bytes) {
    ByteBuffer buffer(bytes);
    
    // Verify header
    uint8_t version = buffer.readUInt8();
    if (version != RAFT_PROTOCOL_VERSION) {
        throw ProtocolException("Unsupported protocol version: " + std::to_string(version));
    }
    
    uint8_t msg_type = buffer.readUInt8();
    if (msg_type != static_cast<uint8_t>(RaftMessageType::REQUEST_VOTE_RESP)) {
        throw ProtocolException("Wrong message type: expected REQUEST_VOTE_RESP");
    }
    
    // Decode payload
    RequestVoteResponse resp;
    resp.term = buffer.readInt64();
    resp.vote_granted = buffer.readBool();
    
    return resp;
}

// ============================================================================
// AppendEntriesRequest Codec
// ============================================================================

std::vector<uint8_t> AppendEntriesRequestCodec::encode(const AppendEntriesRequest& req) {
    ByteBuffer buffer;
    
    // Header
    buffer.writeUInt8(RAFT_PROTOCOL_VERSION);
    buffer.writeUInt8(static_cast<uint8_t>(RaftMessageType::APPEND_ENTRIES_REQ));
    
    // Payload
    buffer.writeInt64(req.term);
    buffer.writeInt32(req.leader_id);
    buffer.writeInt64(req.prev_log_index);
    buffer.writeInt64(req.prev_log_term);
    buffer.writeInt64(req.leader_commit);
    
    // Encode entries array
    buffer.writeInt32(static_cast<int32_t>(req.entries.size()));
    for (const auto& entry : req.entries) {
        LogEntryCodec::encodeInto(buffer, entry);
    }
    
    return buffer.release();
}

AppendEntriesRequest AppendEntriesRequestCodec::decode(const std::vector<uint8_t>& bytes) {
    ByteBuffer buffer(bytes);
    
    // Verify header
    uint8_t version = buffer.readUInt8();
    if (version != RAFT_PROTOCOL_VERSION) {
        throw ProtocolException("Unsupported protocol version: " + std::to_string(version));
    }
    
    uint8_t msg_type = buffer.readUInt8();
    if (msg_type != static_cast<uint8_t>(RaftMessageType::APPEND_ENTRIES_REQ)) {
        throw ProtocolException("Wrong message type: expected APPEND_ENTRIES_REQ");
    }
    
    // Decode payload
    AppendEntriesRequest req;
    req.term = buffer.readInt64();
    req.leader_id = buffer.readInt32();
    req.prev_log_index = buffer.readInt64();
    req.prev_log_term = buffer.readInt64();
    req.leader_commit = buffer.readInt64();
    
    // Decode entries array
    int32_t entry_count = buffer.readInt32();
    if (entry_count < 0) {
        throw ProtocolException("Invalid entry count: " + std::to_string(entry_count));
    }
    
    req.entries.reserve(entry_count);
    for (int32_t i = 0; i < entry_count; ++i) {
        req.entries.push_back(LogEntryCodec::decodeFrom(buffer));
    }
    
    return req;
}

// ============================================================================
// AppendEntriesResponse Codec
// ============================================================================

std::vector<uint8_t> AppendEntriesResponseCodec::encode(const AppendEntriesResponse& resp) {
    ByteBuffer buffer;
    
    // Header
    buffer.writeUInt8(RAFT_PROTOCOL_VERSION);
    buffer.writeUInt8(static_cast<uint8_t>(RaftMessageType::APPEND_ENTRIES_RESP));
    
    // Payload
    buffer.writeInt64(resp.term);
    buffer.writeBool(resp.success);
    buffer.writeInt64(resp.last_log_index);
    
    return buffer.release();
}

AppendEntriesResponse AppendEntriesResponseCodec::decode(const std::vector<uint8_t>& bytes) {
    ByteBuffer buffer(bytes);
    
    // Verify header
    uint8_t version = buffer.readUInt8();
    if (version != RAFT_PROTOCOL_VERSION) {
        throw ProtocolException("Unsupported protocol version: " + std::to_string(version));
    }
    
    uint8_t msg_type = buffer.readUInt8();
    if (msg_type != static_cast<uint8_t>(RaftMessageType::APPEND_ENTRIES_RESP)) {
        throw ProtocolException("Wrong message type: expected APPEND_ENTRIES_RESP");
    }
    
    // Decode payload
    AppendEntriesResponse resp;
    resp.term = buffer.readInt64();
    resp.success = buffer.readBool();
    resp.last_log_index = buffer.readInt64();
    
    return resp;
}

// ============================================================================
// InstallSnapshotRequest Implementation
// ============================================================================

std::vector<uint8_t> InstallSnapshotRequest::encode() const {
    ByteBuffer buffer;
    
    // Header
    buffer.writeUInt8(RAFT_PROTOCOL_VERSION);
    buffer.writeUInt8(static_cast<uint8_t>(RaftMessageType::INSTALL_SNAPSHOT_REQ));
    
    // Payload
    buffer.writeInt64(term);
    buffer.writeInt32(leader_id);
    buffer.writeInt64(last_included_index);
    buffer.writeInt64(last_included_term);
    buffer.writeInt64(offset);
    buffer.writeBool(done);
    buffer.writeBytes(data);
    
    return buffer.release();
}

InstallSnapshotRequest InstallSnapshotRequest::decode(const std::vector<uint8_t>& bytes) {
    ByteBuffer buffer(bytes);
    
    // Verify header
    uint8_t version = buffer.readUInt8();
    if (version != RAFT_PROTOCOL_VERSION) {
        throw ProtocolException("Unsupported protocol version: " + std::to_string(version));
    }
    
    uint8_t msg_type = buffer.readUInt8();
    if (msg_type != static_cast<uint8_t>(RaftMessageType::INSTALL_SNAPSHOT_REQ)) {
        throw ProtocolException("Wrong message type: expected INSTALL_SNAPSHOT_REQ");
    }
    
    // Decode payload
    InstallSnapshotRequest req;
    req.term = buffer.readInt64();
    req.leader_id = buffer.readInt32();
    req.last_included_index = buffer.readInt64();
    req.last_included_term = buffer.readInt64();
    req.offset = buffer.readInt64();
    req.done = buffer.readBool();
    req.data = buffer.readBytes();
    
    return req;
}

// ============================================================================
// InstallSnapshotResponse Implementation
// ============================================================================

std::vector<uint8_t> InstallSnapshotResponse::encode() const {
    ByteBuffer buffer;
    
    // Header
    buffer.writeUInt8(RAFT_PROTOCOL_VERSION);
    buffer.writeUInt8(static_cast<uint8_t>(RaftMessageType::INSTALL_SNAPSHOT_RESP));
    
    // Payload
    buffer.writeInt64(term);
    
    return buffer.release();
}

InstallSnapshotResponse InstallSnapshotResponse::decode(const std::vector<uint8_t>& bytes) {
    ByteBuffer buffer(bytes);
    
    // Verify header
    uint8_t version = buffer.readUInt8();
    if (version != RAFT_PROTOCOL_VERSION) {
        throw ProtocolException("Unsupported protocol version: " + std::to_string(version));
    }
    
    uint8_t msg_type = buffer.readUInt8();
    if (msg_type != static_cast<uint8_t>(RaftMessageType::INSTALL_SNAPSHOT_RESP)) {
        throw ProtocolException("Wrong message type: expected INSTALL_SNAPSHOT_RESP");
    }
    
    // Decode payload
    InstallSnapshotResponse resp;
    resp.term = buffer.readInt64();
    
    return resp;
}

}  // namespace kawasan::raft
