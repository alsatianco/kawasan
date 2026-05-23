#include <gtest/gtest.h>

#include "kawasan/broker/producer_state_manager.h"

using kawasan::broker::ProducerStateManager;
using kawasan::ErrorCode;

namespace {

constexpr int64_t kPid = 1000;
constexpr int16_t kEpoch = 0;
const std::string kTopic = "test-topic";
constexpr int32_t kPart = 0;

TEST(ProducerStateManagerTest, NonIdempotentProducerAccepted) {
    ProducerStateManager psm;
    auto r = psm.check(kTopic, kPart, /*pid=*/-1, /*epoch=*/-1,
                       /*base_seq=*/-1, /*records=*/5);
    EXPECT_EQ(r.error, ErrorCode::NONE);
}

TEST(ProducerStateManagerTest, FirstBatchAccepted) {
    ProducerStateManager psm;
    auto r = psm.check(kTopic, kPart, kPid, kEpoch, /*base_seq=*/0, /*records=*/5);
    EXPECT_EQ(r.error, ErrorCode::NONE);
}

TEST(ProducerStateManagerTest, SequentialBatchesAccepted) {
    ProducerStateManager psm;
    psm.recordAppend(kTopic, kPart, kPid, kEpoch, 0, 5, /*offset=*/100);
    // Next expected sequence is 5.
    auto r = psm.check(kTopic, kPart, kPid, kEpoch, /*base_seq=*/5, /*records=*/3);
    EXPECT_EQ(r.error, ErrorCode::NONE);
}

TEST(ProducerStateManagerTest, DuplicateBatchReturnsOriginalOffset) {
    ProducerStateManager psm;
    psm.recordAppend(kTopic, kPart, kPid, kEpoch, 0, 5, /*offset=*/100);
    auto r = psm.check(kTopic, kPart, kPid, kEpoch, /*base_seq=*/0, /*records=*/5);
    EXPECT_EQ(r.error, ErrorCode::DUPLICATE_SEQUENCE_NUMBER);
    EXPECT_EQ(r.duplicate_offset, 100);
}

TEST(ProducerStateManagerTest, OutOfOrderSequenceRejected) {
    ProducerStateManager psm;
    psm.recordAppend(kTopic, kPart, kPid, kEpoch, 0, 5, /*offset=*/100);
    // Skip to seq=10 (expected 5).
    auto r = psm.check(kTopic, kPart, kPid, kEpoch, /*base_seq=*/10, /*records=*/1);
    EXPECT_EQ(r.error, ErrorCode::OUT_OF_ORDER_SEQUENCE_NUMBER);
}

TEST(ProducerStateManagerTest, FencedByNewerEpoch) {
    ProducerStateManager psm;
    psm.recordAppend(kTopic, kPart, kPid, /*epoch=*/5, 0, 5, /*offset=*/100);
    // Older epoch tries to write — should be fenced.
    auto r = psm.check(kTopic, kPart, kPid, /*epoch=*/3, /*base_seq=*/5, /*records=*/1);
    EXPECT_EQ(r.error, ErrorCode::INVALID_PRODUCER_EPOCH);
}

TEST(ProducerStateManagerTest, NewerEpochResetsSequenceTracking) {
    ProducerStateManager psm;
    psm.recordAppend(kTopic, kPart, kPid, /*epoch=*/3, 0, 5, /*offset=*/100);
    // Newer epoch with arbitrary sequence — should accept.
    auto r = psm.check(kTopic, kPart, kPid, /*epoch=*/5, /*base_seq=*/42, /*records=*/1);
    EXPECT_EQ(r.error, ErrorCode::NONE);
}

TEST(ProducerStateManagerTest, DifferentPartitionsTrackedSeparately) {
    ProducerStateManager psm;
    psm.recordAppend(kTopic, /*part=*/0, kPid, kEpoch, 0, 5, /*offset=*/100);
    // Partition 1 has never seen this producer — should accept seq=0.
    auto r = psm.check(kTopic, /*part=*/1, kPid, kEpoch, /*base_seq=*/0, /*records=*/1);
    EXPECT_EQ(r.error, ErrorCode::NONE);
}

}  // namespace
