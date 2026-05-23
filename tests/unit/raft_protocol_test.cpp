#include <gtest/gtest.h>
#include "kawasan/raft/raft_protocol.h"

using namespace kawasan::raft;

// ============================================================================
// ByteBuffer Tests
// ============================================================================

TEST(ByteBufferTest, WriteAndReadUInt8) {
    ByteBuffer buffer;
    buffer.writeUInt8(42);
    buffer.writeUInt8(255);
    buffer.writeUInt8(0);
    
    buffer.reset();
    EXPECT_EQ(buffer.readUInt8(), 42);
    EXPECT_EQ(buffer.readUInt8(), 255);
    EXPECT_EQ(buffer.readUInt8(), 0);
    EXPECT_FALSE(buffer.hasRemaining());
}

TEST(ByteBufferTest, WriteAndReadInt32) {
    ByteBuffer buffer;
    buffer.writeInt32(0);
    buffer.writeInt32(12345);
    buffer.writeInt32(-67890);
    buffer.writeInt32(2147483647);  // INT32_MAX
    buffer.writeInt32(-2147483648); // INT32_MIN
    
    buffer.reset();
    EXPECT_EQ(buffer.readInt32(), 0);
    EXPECT_EQ(buffer.readInt32(), 12345);
    EXPECT_EQ(buffer.readInt32(), -67890);
    EXPECT_EQ(buffer.readInt32(), 2147483647);
    EXPECT_EQ(buffer.readInt32(), -2147483648);
}

TEST(ByteBufferTest, WriteAndReadInt64) {
    ByteBuffer buffer;
    buffer.writeInt64(0);
    buffer.writeInt64(123456789012345LL);
    buffer.writeInt64(-987654321098765LL);
    buffer.writeInt64(9223372036854775807LL);  // INT64_MAX
    
    buffer.reset();
    EXPECT_EQ(buffer.readInt64(), 0);
    EXPECT_EQ(buffer.readInt64(), 123456789012345LL);
    EXPECT_EQ(buffer.readInt64(), -987654321098765LL);
    EXPECT_EQ(buffer.readInt64(), 9223372036854775807LL);
}

TEST(ByteBufferTest, WriteAndReadBool) {
    ByteBuffer buffer;
    buffer.writeBool(true);
    buffer.writeBool(false);
    buffer.writeBool(true);
    
    buffer.reset();
    EXPECT_TRUE(buffer.readBool());
    EXPECT_FALSE(buffer.readBool());
    EXPECT_TRUE(buffer.readBool());
}

TEST(ByteBufferTest, WriteAndReadString) {
    ByteBuffer buffer;
    buffer.writeString("");
    buffer.writeString("hello");
    buffer.writeString("world with spaces and 日本語");
    
    buffer.reset();
    EXPECT_EQ(buffer.readString(), "");
    EXPECT_EQ(buffer.readString(), "hello");
    EXPECT_EQ(buffer.readString(), "world with spaces and 日本語");
}

TEST(ByteBufferTest, WriteAndReadBytes) {
    ByteBuffer buffer;
    buffer.writeBytes({});
    buffer.writeBytes({1, 2, 3});
    buffer.writeBytes({255, 0, 128});
    
    buffer.reset();
    EXPECT_EQ(buffer.readBytes(), std::vector<uint8_t>{});
    EXPECT_EQ(buffer.readBytes(), std::vector<uint8_t>({1, 2, 3}));
    EXPECT_EQ(buffer.readBytes(), std::vector<uint8_t>({255, 0, 128}));
}

TEST(ByteBufferTest, BufferUnderflow) {
    ByteBuffer buffer;
    buffer.writeInt32(42);
    
    buffer.reset();
    buffer.readInt32();
    
    EXPECT_THROW(buffer.readInt32(), ProtocolException);
    EXPECT_THROW(buffer.readUInt8(), ProtocolException);
}

TEST(ByteBufferTest, InvalidLength) {
    ByteBuffer buffer;
    buffer.writeInt32(-1);  // Invalid length
    buffer.writeInt32(0);   // String data (empty)
    
    buffer.reset();
    EXPECT_THROW(buffer.readString(), ProtocolException);
}

// ============================================================================
// RequestVoteRequest Tests
// ============================================================================

TEST(RequestVoteRequestTest, EncodeDecodeRoundTrip) {
    RequestVoteRequest original;
    original.term = 5;
    original.candidate_id = 2;
    original.last_log_index = 10;
    original.last_log_term = 4;
    
    auto encoded = RequestVoteRequestCodec::encode(original);
    auto decoded = RequestVoteRequestCodec::decode(encoded);
    
    EXPECT_EQ(decoded.term, original.term);
    EXPECT_EQ(decoded.candidate_id, original.candidate_id);
    EXPECT_EQ(decoded.last_log_index, original.last_log_index);
    EXPECT_EQ(decoded.last_log_term, original.last_log_term);
}

TEST(RequestVoteRequestTest, VersionValidation) {
    std::vector<uint8_t> invalid_version = {99, 1, 0, 0, 0, 0, 0, 0, 0, 5};
    EXPECT_THROW(RequestVoteRequestCodec::decode(invalid_version), ProtocolException);
}

TEST(RequestVoteRequestTest, MessageTypeValidation) {
    std::vector<uint8_t> wrong_type = {1, 99, 0, 0, 0, 0, 0, 0, 0, 5};
    EXPECT_THROW(RequestVoteRequestCodec::decode(wrong_type), ProtocolException);
}

// ============================================================================
// RequestVoteResponse Tests
// ============================================================================

TEST(RequestVoteResponseTest, EncodeDecodeRoundTrip) {
    RequestVoteResponse original;
    original.term = 7;
    original.vote_granted = true;
    
    auto encoded = RequestVoteResponseCodec::encode(original);
    auto decoded = RequestVoteResponseCodec::decode(encoded);
    
    EXPECT_EQ(decoded.term, original.term);
    EXPECT_EQ(decoded.vote_granted, original.vote_granted);
}

TEST(RequestVoteResponseTest, VoteNotGranted) {
    RequestVoteResponse original;
    original.term = 3;
    original.vote_granted = false;
    
    auto encoded = RequestVoteResponseCodec::encode(original);
    auto decoded = RequestVoteResponseCodec::decode(encoded);
    
    EXPECT_EQ(decoded.term, original.term);
    EXPECT_FALSE(decoded.vote_granted);
}

// ============================================================================
// AppendEntriesRequest Tests
// ============================================================================

TEST(AppendEntriesRequestTest, EncodeDecodeEmptyEntries) {
    // Heartbeat message (no entries)
    AppendEntriesRequest original;
    original.term = 10;
    original.leader_id = 1;
    original.prev_log_index = 5;
    original.prev_log_term = 9;
    original.leader_commit = 4;
    original.entries = {};  // Empty for heartbeat
    
    auto encoded = AppendEntriesRequestCodec::encode(original);
    auto decoded = AppendEntriesRequestCodec::decode(encoded);
    
    EXPECT_EQ(decoded.term, original.term);
    EXPECT_EQ(decoded.leader_id, original.leader_id);
    EXPECT_EQ(decoded.prev_log_index, original.prev_log_index);
    EXPECT_EQ(decoded.prev_log_term, original.prev_log_term);
    EXPECT_EQ(decoded.leader_commit, original.leader_commit);
    EXPECT_TRUE(decoded.entries.empty());
}

TEST(AppendEntriesRequestTest, EncodeDecodeWithEntries) {
    AppendEntriesRequest original;
    original.term = 15;
    original.leader_id = 0;
    original.prev_log_index = 10;
    original.prev_log_term = 14;
    original.leader_commit = 9;
    
    // Add log entries
    LogEntry entry1;
    entry1.term = 15;
    entry1.index = 11;
    entry1.command_type = "CREATE_TOPIC";
    entry1.data = {1, 2, 3, 4, 5};
    
    LogEntry entry2;
    entry2.term = 15;
    entry2.index = 12;
    entry2.command_type = "DELETE_TOPIC";
    entry2.data = {10, 20, 30};
    
    original.entries.push_back(entry1);
    original.entries.push_back(entry2);
    
    auto encoded = AppendEntriesRequestCodec::encode(original);
    auto decoded = AppendEntriesRequestCodec::decode(encoded);
    
    EXPECT_EQ(decoded.term, original.term);
    EXPECT_EQ(decoded.leader_id, original.leader_id);
    EXPECT_EQ(decoded.prev_log_index, original.prev_log_index);
    EXPECT_EQ(decoded.prev_log_term, original.prev_log_term);
    EXPECT_EQ(decoded.leader_commit, original.leader_commit);
    ASSERT_EQ(decoded.entries.size(), 2);
    
    EXPECT_EQ(decoded.entries[0].term, entry1.term);
    EXPECT_EQ(decoded.entries[0].index, entry1.index);
    EXPECT_EQ(decoded.entries[0].command_type, entry1.command_type);
    EXPECT_EQ(decoded.entries[0].data, entry1.data);
    
    EXPECT_EQ(decoded.entries[1].term, entry2.term);
    EXPECT_EQ(decoded.entries[1].index, entry2.index);
    EXPECT_EQ(decoded.entries[1].command_type, entry2.command_type);
    EXPECT_EQ(decoded.entries[1].data, entry2.data);
}

TEST(AppendEntriesRequestTest, LargeEntry) {
    AppendEntriesRequest original;
    original.term = 20;
    original.leader_id = 1;
    original.prev_log_index = 100;
    original.prev_log_term = 19;
    original.leader_commit = 99;
    
    LogEntry large_entry;
    large_entry.term = 20;
    large_entry.index = 101;
    large_entry.command_type = "LARGE_COMMAND";
    large_entry.data.resize(1024 * 1024);  // 1MB
    for (size_t i = 0; i < large_entry.data.size(); ++i) {
        large_entry.data[i] = static_cast<uint8_t>(i % 256);
    }
    
    original.entries.push_back(large_entry);
    
    auto encoded = AppendEntriesRequestCodec::encode(original);
    auto decoded = AppendEntriesRequestCodec::decode(encoded);
    
    ASSERT_EQ(decoded.entries.size(), 1);
    EXPECT_EQ(decoded.entries[0].term, large_entry.term);
    EXPECT_EQ(decoded.entries[0].index, large_entry.index);
    EXPECT_EQ(decoded.entries[0].command_type, large_entry.command_type);
    EXPECT_EQ(decoded.entries[0].data.size(), large_entry.data.size());
    EXPECT_EQ(decoded.entries[0].data, large_entry.data);
}

// ============================================================================
// AppendEntriesResponse Tests
// ============================================================================

TEST(AppendEntriesResponseTest, EncodeDecodeSuccess) {
    AppendEntriesResponse original;
    original.term = 12;
    original.success = true;
    original.last_log_index = 15;
    
    auto encoded = AppendEntriesResponseCodec::encode(original);
    auto decoded = AppendEntriesResponseCodec::decode(encoded);
    
    EXPECT_EQ(decoded.term, original.term);
    EXPECT_TRUE(decoded.success);
    EXPECT_EQ(decoded.last_log_index, original.last_log_index);
}

TEST(AppendEntriesResponseTest, EncodeDecodeFailure) {
    AppendEntriesResponse original;
    original.term = 8;
    original.success = false;
    original.last_log_index = 10;
    
    auto encoded = AppendEntriesResponseCodec::encode(original);
    auto decoded = AppendEntriesResponseCodec::decode(encoded);
    
    EXPECT_EQ(decoded.term, original.term);
    EXPECT_FALSE(decoded.success);
    EXPECT_EQ(decoded.last_log_index, original.last_log_index);
}

// ============================================================================
// InstallSnapshotRequest Tests
// ============================================================================

TEST(InstallSnapshotRequestTest, EncodeDecodeFirstChunk) {
    InstallSnapshotRequest original;
    original.term = 25;
    original.leader_id = 2;
    original.last_included_index = 1000;
    original.last_included_term = 24;
    original.offset = 0;
    original.done = false;
    original.data.resize(64 * 1024);  // 64KB chunk
    for (size_t i = 0; i < original.data.size(); ++i) {
        original.data[i] = static_cast<uint8_t>(i % 256);
    }
    
    auto encoded = original.encode();
    auto decoded = InstallSnapshotRequest::decode(encoded);
    
    EXPECT_EQ(decoded.term, original.term);
    EXPECT_EQ(decoded.leader_id, original.leader_id);
    EXPECT_EQ(decoded.last_included_index, original.last_included_index);
    EXPECT_EQ(decoded.last_included_term, original.last_included_term);
    EXPECT_EQ(decoded.offset, original.offset);
    EXPECT_FALSE(decoded.done);
    EXPECT_EQ(decoded.data, original.data);
}

TEST(InstallSnapshotRequestTest, EncodeDecodeLastChunk) {
    InstallSnapshotRequest original;
    original.term = 30;
    original.leader_id = 1;
    original.last_included_index = 2000;
    original.last_included_term = 29;
    original.offset = 128 * 1024;  // 128KB offset
    original.done = true;
    original.data = {1, 2, 3, 4, 5};  // Small final chunk
    
    auto encoded = original.encode();
    auto decoded = InstallSnapshotRequest::decode(encoded);
    
    EXPECT_EQ(decoded.term, original.term);
    EXPECT_EQ(decoded.leader_id, original.leader_id);
    EXPECT_EQ(decoded.last_included_index, original.last_included_index);
    EXPECT_EQ(decoded.last_included_term, original.last_included_term);
    EXPECT_EQ(decoded.offset, original.offset);
    EXPECT_TRUE(decoded.done);
    EXPECT_EQ(decoded.data, original.data);
}

// ============================================================================
// InstallSnapshotResponse Tests
// ============================================================================

TEST(InstallSnapshotResponseTest, EncodeDecodeRoundTrip) {
    InstallSnapshotResponse original;
    original.term = 35;
    
    auto encoded = original.encode();
    auto decoded = InstallSnapshotResponse::decode(encoded);
    
    EXPECT_EQ(decoded.term, original.term);
}

// ============================================================================
// Edge Cases and Error Handling
// ============================================================================

TEST(RaftProtocolTest, MaxInt64Values) {
    RequestVoteRequest req;
    req.term = 9223372036854775807LL;  // INT64_MAX
    req.candidate_id = 2147483647;     // INT32_MAX
    req.last_log_index = 9223372036854775807LL;
    req.last_log_term = 9223372036854775807LL;
    
    auto encoded = RequestVoteRequestCodec::encode(req);
    auto decoded = RequestVoteRequestCodec::decode(encoded);
    
    EXPECT_EQ(decoded.term, req.term);
    EXPECT_EQ(decoded.candidate_id, req.candidate_id);
    EXPECT_EQ(decoded.last_log_index, req.last_log_index);
    EXPECT_EQ(decoded.last_log_term, req.last_log_term);
}

TEST(RaftProtocolTest, EmptyData) {
    LogEntry entry;
    entry.term = 5;
    entry.index = 10;
    entry.command_type = "";
    entry.data = {};
    
    AppendEntriesRequest req;
    req.term = 5;
    req.leader_id = 0;
    req.prev_log_index = 9;
    req.prev_log_term = 4;
    req.leader_commit = 8;
    req.entries.push_back(entry);
    
    auto encoded = AppendEntriesRequestCodec::encode(req);
    auto decoded = AppendEntriesRequestCodec::decode(encoded);
    
    ASSERT_EQ(decoded.entries.size(), 1);
    EXPECT_EQ(decoded.entries[0].command_type, "");
    EXPECT_TRUE(decoded.entries[0].data.empty());
}

TEST(RaftProtocolTest, TruncatedMessage) {
    RequestVoteRequest req;
    req.term = 5;
    req.candidate_id = 2;
    req.last_log_index = 10;
    req.last_log_term = 4;
    
    auto encoded = RequestVoteRequestCodec::encode(req);
    
    // Truncate the message
    encoded.resize(encoded.size() - 5);
    
    EXPECT_THROW(RequestVoteRequestCodec::decode(encoded), ProtocolException);
}

TEST(RaftProtocolTest, ProtocolVersionHeaderSize) {
    RequestVoteRequest req;
    req.term = 1;
    req.candidate_id = 0;
    req.last_log_index = 0;
    req.last_log_term = 0;
    
    auto encoded = RequestVoteRequestCodec::encode(req);
    
    // Verify header is present and correct
    ASSERT_GE(encoded.size(), 2);
    EXPECT_EQ(encoded[0], RAFT_PROTOCOL_VERSION);
    EXPECT_EQ(encoded[1], static_cast<uint8_t>(RaftMessageType::REQUEST_VOTE_REQ));
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
