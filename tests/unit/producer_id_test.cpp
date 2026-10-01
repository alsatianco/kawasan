#include "kawasan/broker/producer_id.h"

#include <gtest/gtest.h>

#include <set>

using kawasan::broker::clusterProducerId;

TEST(ProducerIdTest, RangesNeverOverlapAcrossBrokers) {
    std::set<int64_t> seen;
    for (kawasan::BrokerId broker : {0, 1, 2, 7}) {
        for (int64_t seq : {int64_t{1}, int64_t{2}, int64_t{3}, int64_t{1000}, int64_t{0x7FFFFFFF}}) {
            const int64_t pid = clusterProducerId(broker, seq);
            EXPECT_GE(pid, 0);
            EXPECT_TRUE(seen.insert(pid).second) << broker << "/" << seq;
        }
    }
}

TEST(ProducerIdTest, EncodesBrokerAndSequence) {
    EXPECT_EQ(clusterProducerId(0, 5), 5);
    EXPECT_EQ(clusterProducerId(2, 1), (int64_t{2} << 32) | 1);
}
