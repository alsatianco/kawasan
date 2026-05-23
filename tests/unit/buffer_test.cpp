#include <gtest/gtest.h>
#include <limits>

#include "kawasan/common/buffer.h"

namespace kawasan {

TEST(BufferTest, WriteAndReadInt8) {
    Buffer buffer;
    buffer.writeInt8(42);
    buffer.writeInt8(-10);

    EXPECT_EQ(buffer.size(), 2);
    EXPECT_EQ(buffer.readInt8(), 42);
    EXPECT_EQ(buffer.readInt8(), -10);
}

TEST(BufferTest, WriteAndReadInt16) {
    Buffer buffer;
    buffer.writeInt16(1234);
    buffer.writeInt16(-5678);

    EXPECT_EQ(buffer.readInt16(), 1234);
    EXPECT_EQ(buffer.readInt16(), -5678);
}

TEST(BufferTest, WriteAndReadInt32) {
    Buffer buffer;
    buffer.writeInt32(123456789);
    buffer.writeInt32(-987654321);

    EXPECT_EQ(buffer.readInt32(), 123456789);
    EXPECT_EQ(buffer.readInt32(), -987654321);
}

TEST(BufferTest, WriteAndReadInt64) {
    Buffer buffer;
    constexpr int64_t kPositive = 1234567890123456789LL;
    constexpr int64_t kNegative = -1234567890123456789LL;

    buffer.writeInt64(kPositive);
    buffer.writeInt64(kNegative);

    EXPECT_EQ(buffer.readInt64(), kPositive);
    EXPECT_EQ(buffer.readInt64(), kNegative);
}

TEST(BufferTest, WriteAndReadString) {
    Buffer buffer;
    buffer.writeString("Hello");
    buffer.writeString("World");

    EXPECT_EQ(buffer.readString(), "Hello");
    EXPECT_EQ(buffer.readString(), "World");
}

TEST(BufferTest, WriteAndReadNullableString) {
    Buffer buffer;
    buffer.writeNullableString("Test");
    buffer.writeNullableString(std::nullopt);

    auto str1 = buffer.readNullableString();
    ASSERT_TRUE(str1.has_value());
    EXPECT_EQ(*str1, "Test");

    auto str2 = buffer.readNullableString();
    EXPECT_FALSE(str2.has_value());
}

TEST(BufferTest, Position) {
    Buffer buffer;
    buffer.writeInt32(123);
    EXPECT_EQ(buffer.size(), 4);
    EXPECT_EQ(buffer.position(), 0);

    buffer.readInt16();
    EXPECT_EQ(buffer.position(), 2);
    EXPECT_EQ(buffer.remaining(), 2);

    buffer.reset();
    EXPECT_EQ(buffer.position(), 0);
}

// 0A.5: flexible-versions infrastructure tests.

TEST(BufferTest, CompactStringRoundTrip) {
    Buffer buffer;
    buffer.writeCompactString("hello");
    buffer.writeCompactString("");
    buffer.writeCompactString(std::string(127, 'x'));  // length forces 2-byte varint
    EXPECT_EQ(buffer.readCompactString(), "hello");
    EXPECT_EQ(buffer.readCompactString(), "");
    EXPECT_EQ(buffer.readCompactString(), std::string(127, 'x'));
}

TEST(BufferTest, CompactNullableStringDistinguishesNullFromEmpty) {
    Buffer buffer;
    buffer.writeCompactNullableString(std::nullopt);
    buffer.writeCompactNullableString(std::string(""));
    buffer.writeCompactNullableString(std::string("present"));
    EXPECT_FALSE(buffer.readCompactNullableString().has_value());
    auto empty = buffer.readCompactNullableString();
    ASSERT_TRUE(empty.has_value());
    EXPECT_EQ(*empty, "");
    auto present = buffer.readCompactNullableString();
    ASSERT_TRUE(present.has_value());
    EXPECT_EQ(*present, "present");
}

TEST(BufferTest, CompactBytesRoundTrip) {
    Buffer buffer;
    std::vector<uint8_t> payload{0x00, 0x01, 0xFE, 0xFF};
    buffer.writeCompactBytes(payload);
    auto out = buffer.readCompactBytes();
    EXPECT_EQ(out, payload);
}

TEST(BufferTest, CompactArrayLenAndNull) {
    Buffer buffer;
    buffer.writeCompactArrayLen(0);
    buffer.writeCompactArrayLen(3);
    buffer.writeCompactArrayLen(-1);
    EXPECT_EQ(buffer.readCompactArrayLen(), 0);
    EXPECT_EQ(buffer.readCompactArrayLen(), 3);
    EXPECT_EQ(buffer.readCompactArrayLen(), -1);
}

TEST(BufferTest, EmptyTaggedFieldsRoundTrip) {
    Buffer buffer;
    buffer.writeInt8(7);
    buffer.writeEmptyTaggedFields();
    buffer.writeInt8(9);
    EXPECT_EQ(buffer.readInt8(), 7);
    buffer.skipTaggedFields();
    EXPECT_EQ(buffer.readInt8(), 9);
}

TEST(BufferTest, TaggedFieldsWithEntriesAreSkipped) {
    // Manually construct two tagged-field entries: tag=5/len=3/data + tag=7/len=0/data.
    Buffer buffer;
    buffer.writeInt8(0xAA);
    buffer.writeUnsignedVarInt(2);           // count
    buffer.writeUnsignedVarInt(5);            // tag
    buffer.writeUnsignedVarInt(3);            // length
    buffer.writeInt8(0x11);
    buffer.writeInt8(0x22);
    buffer.writeInt8(0x33);
    buffer.writeUnsignedVarInt(7);            // tag
    buffer.writeUnsignedVarInt(0);            // empty length
    buffer.writeInt8(0xBB);
    EXPECT_EQ(buffer.readInt8(), static_cast<int8_t>(0xAA));
    buffer.skipTaggedFields();
    EXPECT_EQ(buffer.readInt8(), static_cast<int8_t>(0xBB));
}

}  // namespace kawasan

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
