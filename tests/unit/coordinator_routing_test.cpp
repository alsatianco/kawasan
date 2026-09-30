#include "kawasan/broker/coordinator_routing.h"

#include <gtest/gtest.h>

#include "kawasan/broker/transaction_state_manager.h"

using kawasan::broker::coordinatorPartitionFor;

// Expected values computed independently with Java String.hashCode semantics
// (32-bit wrap), reduced as unsigned mod N.
TEST(CoordinatorRoutingTest, KnownKeysRouteToKnownPartitions) {
    EXPECT_EQ(coordinatorPartitionFor("", 16), 0);
    EXPECT_EQ(coordinatorPartitionFor("console-consumer-38063", 16), 7);
    EXPECT_EQ(coordinatorPartitionFor("console-consumer-38063", 50), 21);
    EXPECT_EQ(coordinatorPartitionFor("txn-1", 16), 14);
}

// "my-group" hashes negative in Java (-1906497762): the unsigned reduction must
// still land in range and match what existing on-disk placement used.
TEST(CoordinatorRoutingTest, NegativeJavaHashReducesUnsigned) {
    EXPECT_EQ(coordinatorPartitionFor("my-group", 16), 14);
    EXPECT_EQ(coordinatorPartitionFor("my-group", 50), 34);
}

TEST(CoordinatorRoutingTest, NonPositivePartitionCountTreatedAsOne) {
    EXPECT_EQ(coordinatorPartitionFor("anything", 0), 0);
    EXPECT_EQ(coordinatorPartitionFor("anything", -3), 0);
    EXPECT_EQ(coordinatorPartitionFor("a", 1), 0);
}

TEST(CoordinatorRoutingTest, TransactionStateManagerDelegates) {
    for (const char* key : {"", "my-group", "console-consumer-38063", "txn-1", "x"}) {
        for (int n : {1, 3, 16, 50}) {
            EXPECT_EQ(kawasan::broker::TransactionStateManager::partitionFor(key, n),
                      coordinatorPartitionFor(key, n))
                << key << " mod " << n;
        }
    }
}
