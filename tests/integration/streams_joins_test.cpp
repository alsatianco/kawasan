#include "kawasan/streams/streams_builder.h"
#include "kawasan/streams/windows.h"
#include "kawasan/streams/processors.h"
#include <gtest/gtest.h>
#include <chrono>
#include <optional>
#include <string>

using namespace kawasan::streams;
using namespace std::chrono;

// ============================================================================
// KTable Join Tests
// ============================================================================

class KTableJoinTest : public ::testing::Test {
protected:
    StreamsBuilder builder;
};

TEST_F(KTableJoinTest, TableTableInnerJoin) {
    // Create two tables
    auto userTable = builder.table<std::string, std::string>("users");
    auto addressTable = builder.table<std::string, std::string>("addresses");

    // Join them
    auto joinedTable = userTable.join<std::string, std::string>(
        addressTable,
        [](const std::string& user, const std::string& address) {
            return user + " lives at " + address;
        });

    auto topology = builder.build();
    auto& nodes = topology.nodes();

    // Verify the join node was created
    bool hasJoinNode = false;
    for (const auto& [name, node] : nodes) {
        if (name.find("KTABLE-JOIN") != std::string::npos) {
            hasJoinNode = true;
            break;
        }
    }
    EXPECT_TRUE(hasJoinNode);
}

TEST_F(KTableJoinTest, TableTableLeftJoin) {
    auto userTable = builder.table<std::string, std::string>("users");
    auto addressTable = builder.table<std::string, std::string>("addresses");

    // Left join - all users, with optional address
    auto joinedTable = userTable.leftJoin<std::string, std::string>(
        addressTable,
        [](const std::string& user, const std::optional<std::string>& address) {
            if (address) {
                return user + " lives at " + *address;
            }
            return user + " has no address";
        });

    auto topology = builder.build();

    // Verify the left join node was created
    bool hasLeftJoinNode = false;
    for (const auto& [name, node] : topology.nodes()) {
        if (name.find("KTABLE-LEFTJOIN") != std::string::npos) {
            hasLeftJoinNode = true;
            break;
        }
    }
    EXPECT_TRUE(hasLeftJoinNode);
}

TEST_F(KTableJoinTest, TableTableOuterJoin) {
    auto userTable = builder.table<std::string, std::string>("users");
    auto addressTable = builder.table<std::string, std::string>("addresses");

    // Outer join - all records from both tables
    auto joinedTable = userTable.outerJoin<std::string, std::string>(
        addressTable,
        [](const std::optional<std::string>& user,
           const std::optional<std::string>& address) {
            std::string result;
            if (user) {
                result += "User: " + *user;
            } else {
                result += "User: unknown";
            }
            result += ", ";
            if (address) {
                result += "Address: " + *address;
            } else {
                result += "Address: unknown";
            }
            return result;
        });

    auto topology = builder.build();

    // Verify the outer join node was created
    bool hasOuterJoinNode = false;
    for (const auto& [name, node] : topology.nodes()) {
        if (name.find("KTABLE-OUTERJOIN") != std::string::npos) {
            hasOuterJoinNode = true;
            break;
        }
    }
    EXPECT_TRUE(hasOuterJoinNode);
}

TEST_F(KTableJoinTest, TableMapValuesAndJoin) {
    auto userTable = builder.table<std::string, std::string>("users");
    auto addressTable = builder.table<std::string, std::string>("addresses");

    // Transform user names to uppercase, then join
    auto transformed = userTable.mapValues<std::string>(
        [](const std::string& name) {
            std::string upper = name;
            for (auto& c : upper) c = std::toupper(c);
            return upper;
        });

    auto joined = transformed.join<std::string, std::string>(
        addressTable,
        [](const std::string& user, const std::string& address) {
            return user + ": " + address;
        });

    auto topology = builder.build();
    auto& nodes = topology.nodes();

    // Should have source, mapValues, and join nodes
    EXPECT_GE(nodes.size(), 4);  // 2 sources + mapValues + join
}

// ============================================================================
// KStream-KStream Join Tests
// ============================================================================

class StreamStreamJoinTest : public ::testing::Test {
protected:
    StreamsBuilder builder;
};

TEST_F(StreamStreamJoinTest, StreamStreamInnerJoin) {
    // Create two streams (e.g., clicks and impressions)
    auto clicks = builder.stream<std::string, std::string>("clicks");
    auto impressions = builder.stream<std::string, std::string>("impressions");

    // Join them within a 5-second window
    auto joined = clicks.join<std::string, std::string>(
        impressions,
        [](const std::string& click, const std::string& impression) {
            return "Click: " + click + ", Impression: " + impression;
        },
        JoinWindows::of(seconds(5)));

    auto topology = builder.build();

    // Verify the join node was created
    bool hasJoinNode = false;
    for (const auto& [name, node] : topology.nodes()) {
        if (name.find("KSTREAM-JOIN") != std::string::npos) {
            hasJoinNode = true;
            break;
        }
    }
    EXPECT_TRUE(hasJoinNode);
}

TEST_F(StreamStreamJoinTest, StreamStreamLeftJoin) {
    auto orders = builder.stream<std::string, std::string>("orders");
    auto payments = builder.stream<std::string, std::string>("payments");

    // Left join - all orders, with optional payment
    auto joined = orders.leftJoin<std::string, std::string>(
        payments,
        [](const std::string& order, const std::optional<std::string>& payment) {
            if (payment) {
                return "Order " + order + " paid: " + *payment;
            }
            return "Order " + order + " unpaid";
        },
        JoinWindows::of(seconds(10)));

    auto topology = builder.build();

    // Verify the left join node was created
    bool hasLeftJoinNode = false;
    for (const auto& [name, node] : topology.nodes()) {
        if (name.find("KSTREAM-LEFTJOIN") != std::string::npos) {
            hasLeftJoinNode = true;
            break;
        }
    }
    EXPECT_TRUE(hasLeftJoinNode);
}

TEST_F(StreamStreamJoinTest, StreamStreamOuterJoin) {
    auto streamA = builder.stream<std::string, std::string>("stream-a");
    auto streamB = builder.stream<std::string, std::string>("stream-b");

    // Outer join - records from both streams
    auto joined = streamA.outerJoin<std::string, std::string>(
        streamB,
        [](const std::optional<std::string>& a,
           const std::optional<std::string>& b) {
            std::string result = "A: ";
            result += a ? *a : "null";
            result += ", B: ";
            result += b ? *b : "null";
            return result;
        },
        JoinWindows::of(seconds(5)));

    auto topology = builder.build();

    // Verify the outer join node was created
    bool hasOuterJoinNode = false;
    for (const auto& [name, node] : topology.nodes()) {
        if (name.find("KSTREAM-OUTERJOIN") != std::string::npos) {
            hasOuterJoinNode = true;
            break;
        }
    }
    EXPECT_TRUE(hasOuterJoinNode);
}

TEST_F(StreamStreamJoinTest, JoinWithAsymmetricWindow) {
    auto clicks = builder.stream<std::string, std::string>("clicks");
    auto impressions = builder.stream<std::string, std::string>("impressions");

    // Asymmetric window: look back 10s, forward 5s
    auto windows = JoinWindows::of(seconds(5)).asymmetric(seconds(10), seconds(5));

    auto joined = clicks.join<std::string, std::string>(
        impressions,
        [](const std::string& click, const std::string& impression) {
            return click + " -> " + impression;
        },
        windows);

    auto topology = builder.build();
    EXPECT_TRUE(topology.validate());
}

// ============================================================================
// KStream-KTable Join Tests
// ============================================================================

class StreamTableJoinTest : public ::testing::Test {
protected:
    StreamsBuilder builder;
};

TEST_F(StreamTableJoinTest, StreamTableInnerJoin) {
    // Transaction stream joins with user profile table
    auto transactions = builder.stream<std::string, std::string>("transactions");
    auto userProfiles = builder.table<std::string, std::string>("user-profiles");

    // Each transaction looks up user profile
    auto enriched = transactions.join<std::string, std::string>(
        userProfiles,
        [](const std::string& txn, const std::string& profile) {
            return "Transaction: " + txn + ", User: " + profile;
        });

    auto topology = builder.build();

    // Verify the join node was created
    bool hasJoinNode = false;
    for (const auto& [name, node] : topology.nodes()) {
        if (name.find("KSTREAM-KTABLE-JOIN") != std::string::npos) {
            hasJoinNode = true;
            break;
        }
    }
    EXPECT_TRUE(hasJoinNode);
}

TEST_F(StreamTableJoinTest, StreamTableLeftJoin) {
    auto events = builder.stream<std::string, std::string>("events");
    auto metadata = builder.table<std::string, std::string>("metadata");

    // Left join - all events, with optional metadata
    auto enriched = events.leftJoin<std::string, std::string>(
        metadata,
        [](const std::string& event, const std::optional<std::string>& meta) {
            if (meta) {
                return event + " [" + *meta + "]";
            }
            return event + " [no metadata]";
        });

    auto topology = builder.build();

    // Verify the left join node was created
    bool hasLeftJoinNode = false;
    for (const auto& [name, node] : topology.nodes()) {
        if (name.find("KSTREAM-KTABLE-LEFTJOIN") != std::string::npos) {
            hasLeftJoinNode = true;
            break;
        }
    }
    EXPECT_TRUE(hasLeftJoinNode);
}

TEST_F(StreamTableJoinTest, StreamTableJoinWithFilter) {
    auto transactions = builder.stream<std::string, std::string>("transactions");
    auto userProfiles = builder.table<std::string, std::string>("user-profiles");

    // Filter transactions first, then join
    auto filtered = transactions.filter(
        [](const std::string& /*key*/, const std::string& value) {
            return value.find("important") != std::string::npos;
        });

    auto enriched = filtered.join<std::string, std::string>(
        userProfiles,
        [](const std::string& txn, const std::string& profile) {
            return txn + " by " + profile;
        });

    auto topology = builder.build();
    auto& nodes = topology.nodes();

    // Should have: source, filter, table-source, join
    EXPECT_GE(nodes.size(), 4);
}

// ============================================================================
// Complex Join Topology Tests
// ============================================================================

class ComplexJoinTest : public ::testing::Test {
protected:
    StreamsBuilder builder;
};

TEST_F(ComplexJoinTest, MultipleJoinsInTopology) {
    // Create streams and tables
    auto orders = builder.stream<std::string, std::string>("orders");
    auto payments = builder.stream<std::string, std::string>("payments");
    auto customers = builder.table<std::string, std::string>("customers");

    // First, join orders with payments
    auto orderPayments = orders.join<std::string, std::string>(
        payments,
        [](const std::string& order, const std::string& payment) {
            return order + "|" + payment;
        },
        JoinWindows::of(seconds(60)));

    // Then enrich with customer info
    auto enriched = orderPayments.join<std::string, std::string>(
        customers,
        [](const std::string& orderPayment, const std::string& customer) {
            return orderPayment + "|" + customer;
        });

    auto topology = builder.build();
    EXPECT_TRUE(topology.validate());

    // Count node types
    int streamJoins = 0;
    int tableJoins = 0;
    for (const auto& [name, node] : topology.nodes()) {
        if (name.find("KSTREAM-JOIN") != std::string::npos) streamJoins++;
        if (name.find("KSTREAM-KTABLE-JOIN") != std::string::npos) tableJoins++;
    }
    EXPECT_EQ(streamJoins, 1);
    EXPECT_EQ(tableJoins, 1);
}

TEST_F(ComplexJoinTest, JoinFollowedByAggregation) {
    auto clicks = builder.stream<std::string, std::string>("clicks");
    auto impressions = builder.stream<std::string, std::string>("impressions");

    // Join clicks with impressions
    auto joined = clicks.join<std::string, std::string>(
        impressions,
        [](const std::string& click, const std::string& impression) {
            return click + "+" + impression;
        },
        JoinWindows::of(seconds(30)));

    // Then aggregate by key
    joined.groupByKey().count();

    auto topology = builder.build();
    EXPECT_TRUE(topology.validate());

    // Should have join and aggregate nodes
    bool hasJoin = false, hasAggregate = false;
    for (const auto& [name, node] : topology.nodes()) {
        if (name.find("KSTREAM-JOIN") != std::string::npos) hasJoin = true;
        // count() creates an AGGREGATE node internally
        if (name.find("AGGREGATE") != std::string::npos) hasAggregate = true;
    }
    EXPECT_TRUE(hasJoin);
    EXPECT_TRUE(hasAggregate);
}

TEST_F(ComplexJoinTest, TableToStreamAndJoin) {
    auto table1 = builder.table<std::string, std::string>("table1");
    auto stream2 = builder.stream<std::string, std::string>("stream2");

    // Convert table to stream, then join with another stream
    auto stream1 = table1.toStream();

    auto joined = stream1.join<std::string, std::string>(
        stream2,
        [](const std::string& v1, const std::string& v2) {
            return v1 + " + " + v2;
        },
        JoinWindows::of(seconds(10)));

    auto topology = builder.build();
    EXPECT_TRUE(topology.validate());

    // Should have toStream and join nodes
    bool hasToStream = false, hasJoin = false;
    for (const auto& [name, node] : topology.nodes()) {
        if (name.find("KTABLE-TOSTREAM") != std::string::npos) hasToStream = true;
        if (name.find("KSTREAM-JOIN") != std::string::npos) hasJoin = true;
    }
    EXPECT_TRUE(hasToStream);
    EXPECT_TRUE(hasJoin);
}

// ============================================================================
// JoinWindows Configuration Tests
// ============================================================================

class JoinWindowsConfigTest : public ::testing::Test {};

TEST_F(JoinWindowsConfigTest, SymmetricWindow) {
    auto windows = JoinWindows::of(seconds(5));

    // Symmetric: -5000 to +5000
    EXPECT_EQ(windows.beforeMs(), -5000);
    EXPECT_EQ(windows.afterMs(), 5000);
}

TEST_F(JoinWindowsConfigTest, AsymmetricWindow) {
    auto windows = JoinWindows::of(seconds(5))
        .asymmetric(seconds(10), seconds(3));

    EXPECT_EQ(windows.beforeMs(), -10000);
    EXPECT_EQ(windows.afterMs(), 3000);
}

TEST_F(JoinWindowsConfigTest, WindowWithGrace) {
    auto windows = JoinWindows::of(seconds(5)).grace(seconds(2));

    EXPECT_EQ(windows.graceMs(), 2000);
}

TEST_F(JoinWindowsConfigTest, IsWithinWindowCheck) {
    auto windows = JoinWindows::of(seconds(5));

    // Timestamp 10000, check various other timestamps
    EXPECT_TRUE(windows.isWithinWindow(10000, 10000));   // Same time
    EXPECT_TRUE(windows.isWithinWindow(10000, 5000));    // 5000ms before (at boundary)
    EXPECT_TRUE(windows.isWithinWindow(10000, 15000));   // 5000ms after (at boundary)
    EXPECT_FALSE(windows.isWithinWindow(10000, 4999));   // Just outside before
    EXPECT_FALSE(windows.isWithinWindow(10000, 15001)); // Just outside after
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
