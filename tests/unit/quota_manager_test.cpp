// Phase A6: per-client byte-rate quota throttling. Tests use single-call bursts
// within one window so the throttle is deterministic without waiting on wall
// clock.
#include <gtest/gtest.h>

#include "kawasan/broker/quota_manager.h"

using kawasan::broker::QuotaManager;

TEST(QuotaManagerTest, DisabledQuotaNeverThrottles) {
    QuotaManager qm(/*producer=*/0, /*consumer=*/0);
    EXPECT_EQ(0, qm.recordAndThrottleMs(QuotaManager::Type::kProducer, "c", 10'000'000));
    EXPECT_FALSE(qm.producerQuotaEnabled());
    EXPECT_FALSE(qm.consumerQuotaEnabled());
}

TEST(QuotaManagerTest, UnderQuotaDoesNotThrottle) {
    QuotaManager qm(/*producer=*/1000, /*consumer=*/0);
    EXPECT_TRUE(qm.producerQuotaEnabled());
    EXPECT_EQ(0, qm.recordAndThrottleMs(QuotaManager::Type::kProducer, "c", 500));
}

TEST(QuotaManagerTest, OverQuotaThrottlesProportionally) {
    QuotaManager qm(/*producer=*/1000, /*consumer=*/0);
    // 3000 bytes in a 1s window against a 1000 B/s quota => 2000 over =>
    // throttle = 2000*1000/1000 = 2000ms.
    EXPECT_EQ(2000, qm.recordAndThrottleMs(QuotaManager::Type::kProducer, "c", 3000));
}

TEST(QuotaManagerTest, ThrottleIsCapped) {
    QuotaManager qm(/*producer=*/1, /*consumer=*/0);
    // Enormous overage would compute a huge delay; it must be capped at 30s.
    EXPECT_EQ(30000, qm.recordAndThrottleMs(QuotaManager::Type::kProducer, "c", 1'000'000));
}

TEST(QuotaManagerTest, PerClientIsolation) {
    QuotaManager qm(/*producer=*/1000, /*consumer=*/0);
    EXPECT_GT(qm.recordAndThrottleMs(QuotaManager::Type::kProducer, "noisy", 5000), 0);
    // A different client is unaffected by the noisy one.
    EXPECT_EQ(0, qm.recordAndThrottleMs(QuotaManager::Type::kProducer, "quiet", 100));
}

TEST(QuotaManagerTest, ProducerAndConsumerQuotasAreSeparate) {
    QuotaManager qm(/*producer=*/0, /*consumer=*/1000);
    EXPECT_FALSE(qm.producerQuotaEnabled());
    EXPECT_TRUE(qm.consumerQuotaEnabled());
    // Producer disabled => never throttles even on a huge burst.
    EXPECT_EQ(0, qm.recordAndThrottleMs(QuotaManager::Type::kProducer, "c", 1'000'000));
    // Consumer enabled => throttles when over.
    EXPECT_GT(qm.recordAndThrottleMs(QuotaManager::Type::kConsumer, "c", 5000), 0);
}
