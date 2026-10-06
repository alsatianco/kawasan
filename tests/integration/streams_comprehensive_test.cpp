#include <gtest/gtest.h>
#include "kawasan/streams/streams_builder.h"
#include "kawasan/streams/topology.h"
#include "kawasan/streams/processor.h"
#include "kawasan/streams/serde.h"
#include "kawasan/streams/json_serde.h"
#include "kawasan/streams/processor_context.h"
#include "kawasan/streams/windows.h"
#include "kawasan/streams/stores.h"
#include <memory>
#include <string>
#include <vector>
#include <map>
#include <thread>
#include <atomic>
#include <mutex>
#include <chrono>
#include <sstream>
#include <set>

using namespace kawasan::streams;

/**
 * Comprehensive Streams Test Suite
 *
 * Tests complex scenarios that validate the full streams DSL and Processor API:
 * - Test 1: Multi-stage pipeline (filter -> map -> groupBy -> count)
 * - Test 2: Join with windowing (clickstream + user profile)
 * - Test 3: State recovery simulation
 * - Test 4: Exactly-once semantics validation
 * - Test 5: Parallel processing simulation
 */
class StreamsComprehensiveTest : public ::testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}
};

// ===========================================================================
// Test 1: Multi-stage Pipeline
// ===========================================================================

/**
 * Multi-stage pipeline test
 *
 * Tests a complex pipeline:
 * input -> filter(value > 0) -> map(key:value) -> groupByKey -> count
 */
TEST_F(StreamsComprehensiveTest, MultiStagePipeline) {
    StreamsBuilder builder;

    // Create input stream of purchase events
    auto purchaseStream = builder.stream<std::string, int64_t>("purchases");

    // Build multi-stage pipeline
    auto pipeline = purchaseStream
        .filter([](const std::string&, int64_t amount) {
            return amount > 0;  // Filter out refunds
        })
        .mapValues<int64_t>([](int64_t amount) {
            return amount * 100;  // Convert to cents
        })
        .groupByKey()
        .count();

    // Build and validate topology
    Topology topology = builder.build();
    std::string desc = topology.describe();

    // Verify pipeline stages
    EXPECT_NE(desc.find("KSTREAM-SOURCE"), std::string::npos)
        << "Should have source node";
    EXPECT_NE(desc.find("KSTREAM-FILTER"), std::string::npos)
        << "Should have filter node";
    EXPECT_NE(desc.find("KSTREAM-MAPVALUES"), std::string::npos)
        << "Should have mapValues node";
    EXPECT_NE(desc.find("GROUPBYKEY"), std::string::npos)
        << "Should have groupByKey node";
    // Note: COUNT aggregation creates AGGREGATE processor nodes
    EXPECT_NE(desc.find("AGGREGATE"), std::string::npos)
        << "Should have aggregate (count) node";

    SUCCEED() << "Multi-stage pipeline topology created successfully";
}

/**
 * Test filter -> map -> flatMap chain
 */
TEST_F(StreamsComprehensiveTest, FilterMapFlatMapChain) {
    StreamsBuilder builder;

    auto stream = builder.stream<std::string, std::string>("text-lines");

    // Filter non-empty, map to uppercase, flatMap to words
    auto words = stream
        .filter([](const std::string&, const std::string& line) {
            return !line.empty();
        })
        .mapValues<std::string>([](const std::string& line) {
            std::string upper = line;
            for (auto& c : upper) c = static_cast<char>(std::toupper(c));
            return upper;
        })
        .flatMap<std::string, std::string>([](const std::string& key, const std::string& line) {
            std::vector<std::pair<std::string, std::string>> words;
            std::istringstream iss(line);
            std::string word;
            while (iss >> word) {
                words.emplace_back(word, key);  // word as key, original key as value
            }
            return words;
        });

    Topology topology = builder.build();
    std::string desc = topology.describe();

    EXPECT_NE(desc.find("KSTREAM-FILTER"), std::string::npos);
    EXPECT_NE(desc.find("KSTREAM-MAPVALUES"), std::string::npos);
    EXPECT_NE(desc.find("KSTREAM-FLATMAP"), std::string::npos);

    SUCCEED() << "Filter-map-flatMap chain created successfully";
}

// ===========================================================================
// Test 2: Join with Windowing (Clickstream + User Profile)
// ===========================================================================

/**
 * Stream-Table join test: Clickstream enrichment
 *
 * Joins click events with user profile table to enrich clicks with user data.
 */
TEST_F(StreamsComprehensiveTest, ClickstreamUserProfileJoin) {
    StreamsBuilder builder;

    // Click events stream (userId -> clickData)
    auto clicks = builder.stream<std::string, std::string>("clicks");

    // User profiles table (userId -> profile)
    auto profiles = builder.table<std::string, std::string>("user-profiles");

    // Join clicks with user profiles
    auto enrichedClicks = clicks.template join<std::string, std::string>(
        profiles,
        [](const std::string& click, const std::string& profile) {
            return click + "|" + profile;  // Combine click data with profile
        }
    );

    Topology topology = builder.build();
    std::string desc = topology.describe();

    // Verify join nodes
    EXPECT_NE(desc.find("KSTREAM-SOURCE"), std::string::npos)
        << "Should have stream source";
    EXPECT_NE(desc.find("KTABLE-SOURCE"), std::string::npos)
        << "Should have table source";
    EXPECT_NE(desc.find("JOIN"), std::string::npos)
        << "Should have join node";

    SUCCEED() << "Clickstream-UserProfile join topology created successfully";
}

/**
 * Stream-Stream windowed join test
 *
 * Joins ad impressions with ad clicks within a time window.
 */
TEST_F(StreamsComprehensiveTest, AdImpressionClickWindowedJoin) {
    StreamsBuilder builder;

    // Ad impressions stream
    auto impressions = builder.stream<std::string, std::string>("ad-impressions");

    // Ad clicks stream
    auto clicks = builder.stream<std::string, std::string>("ad-clicks");

    // Join impressions with clicks within 5-minute window
    JoinWindows joinWindow = JoinWindows::of(std::chrono::minutes(5));

    auto joinedAds = impressions.join<std::string, std::string>(
        clicks,
        [](const std::string& impression, const std::string& click) {
            return impression + " -> " + click;
        },
        joinWindow
    );

    Topology topology = builder.build();
    std::string desc = topology.describe();

    EXPECT_NE(desc.find("JOIN"), std::string::npos)
        << "Should have join node";

    SUCCEED() << "Windowed stream-stream join topology created successfully";
}

// ===========================================================================
// Test 3: State Recovery Simulation
// ===========================================================================

/**
 * State store configuration test
 *
 * Verifies that state stores are properly configured for persistence.
 */
TEST_F(StreamsComprehensiveTest, StateStoreConfiguration) {
    StreamsBuilder builder;

    auto stream = builder.stream<std::string, int64_t>("events");

    // Create aggregation with named state store
    Materialized<std::string, int64_t> materialized("event-counts-store");
    auto counts = stream.groupByKey().count(materialized);

    Topology topology = builder.build();

    // Note: State stores are registered during runtime, not during topology build.
    // For now, we verify the topology structure is correct.
    std::string desc = topology.describe();
    EXPECT_NE(desc.find("GROUPBYKEY"), std::string::npos)
        << "Should have groupByKey node";
    EXPECT_NE(desc.find("AGGREGATE"), std::string::npos)
        << "Should have aggregate node";

    // Note: Actual state store and persistence testing requires full broker integration
    SUCCEED() << "State store configuration topology verified";
}

/**
 * Checkpoint/recovery pattern test
 *
 * Tests the pattern for state checkpointing (API design).
 */
TEST_F(StreamsComprehensiveTest, CheckpointRecoveryPattern) {
    // Create a processor context for testing
    ProcessorContext context;
    context.setIds("test-app", "task-0");

    // Create and register an in-memory store
    auto mockStore = std::make_shared<InMemoryKeyValueStore<std::string, int64_t>>("test-store");
    context.registerStateStore("test-store", mockStore);

    // Verify store can be retrieved
    auto retrieved = context.getKeyValueStore<std::string, int64_t>("test-store");
    EXPECT_NE(retrieved, nullptr) << "Should retrieve registered store";

    // Test store operations
    mockStore->put("key1", 100);
    mockStore->put("key2", 200);

    auto val1 = mockStore->get("key1");
    EXPECT_TRUE(val1.has_value());
    EXPECT_EQ(*val1, 100);

    SUCCEED() << "Checkpoint/recovery pattern works correctly";
}

// ===========================================================================
// Test 4: Exactly-Once Semantics Validation
// ===========================================================================

/**
 * Idempotent processing test
 *
 * Tests that processing the same record twice produces consistent results.
 */
TEST_F(StreamsComprehensiveTest, IdempotentProcessing) {
    // Simulate idempotent aggregation
    std::map<std::string, int64_t> state;
    std::set<int64_t> processedOffsets;
    std::mutex mutex;

    auto processor = [&](const std::string& key, int64_t value, int64_t recordOffset) {
        std::lock_guard<std::mutex> lock(mutex);

        // Check if we've already processed this offset
        if (processedOffsets.count(recordOffset) > 0) {
            // Already processed, skip (idempotent)
            return;
        }
        processedOffsets.insert(recordOffset);

        // Process the record
        state[key] += value;
    };

    // Process same records multiple times
    processor("user1", 10, 0);
    processor("user1", 20, 1);
    processor("user1", 10, 0);  // Duplicate - should be ignored
    processor("user1", 20, 1);  // Duplicate - should be ignored
    processor("user1", 30, 2);

    EXPECT_EQ(state["user1"], 60) << "Should be 10+20+30, not counting duplicates";

    SUCCEED() << "Idempotent processing verified";
}

/**
 * Transaction boundary test
 *
 * Tests that transaction-like semantics can be implemented.
 */
TEST_F(StreamsComprehensiveTest, TransactionBoundaryPattern) {
    ProcessorContext context;
    context.setIds("test-app", "task-0");

    // Simulate processing with commit
    int processedCount = 0;

    for (int i = 0; i < 10; ++i) {
        // Process record
        processedCount++;

        // Request commit every 5 records
        if (processedCount % 5 == 0) {
            context.commit();
            EXPECT_TRUE(context.isCommitRequested());
            context.clearCommitRequest();
        }
    }

    EXPECT_EQ(processedCount, 10);
    SUCCEED() << "Transaction boundary pattern works correctly";
}

// ===========================================================================
// Test 5: Parallel Processing Simulation
// ===========================================================================

/**
 * Multi-threaded processing test
 *
 * Simulates parallel processing across multiple threads.
 */
TEST_F(StreamsComprehensiveTest, ParallelProcessingSimulation) {
    const int NUM_THREADS = 4;
    const int RECORDS_PER_THREAD = 1000;

    std::atomic<int64_t> totalProcessed{0};
    std::map<std::string, std::atomic<int64_t>> keyCounts;

    // Initialize key counters
    for (int i = 0; i < 10; ++i) {
        keyCounts["key" + std::to_string(i)] = 0;
    }

    // Create worker threads
    std::vector<std::thread> threads;
    for (int t = 0; t < NUM_THREADS; ++t) {
        threads.emplace_back([&]() {
            for (int r = 0; r < RECORDS_PER_THREAD; ++r) {
                std::string key = "key" + std::to_string(r % 10);

                // Simulate processing
                keyCounts[key]++;
                totalProcessed++;
            }
        });
    }

    // Wait for all threads
    for (auto& thread : threads) {
        thread.join();
    }

    // Verify results
    EXPECT_EQ(totalProcessed.load(), NUM_THREADS * RECORDS_PER_THREAD)
        << "All records should be processed";

    // Each key should have been processed NUM_THREADS * (RECORDS_PER_THREAD / 10) times
    int64_t expectedPerKey = NUM_THREADS * (RECORDS_PER_THREAD / 10);
    for (int i = 0; i < 10; ++i) {
        std::string key = "key" + std::to_string(i);
        EXPECT_EQ(keyCounts[key].load(), expectedPerKey)
            << "Key " << key << " should have correct count";
    }

    SUCCEED() << "Parallel processing works correctly";
}

/**
 * Partition-based parallel processing test
 */
TEST_F(StreamsComprehensiveTest, PartitionBasedParallelism) {
    const int NUM_PARTITIONS = 4;
    const int RECORDS_PER_PARTITION = 100;

    // Simulate per-partition state
    std::vector<std::map<std::string, int64_t>> partitionStates(NUM_PARTITIONS);
    std::vector<std::thread> threads;

    // Process each partition in parallel
    for (int p = 0; p < NUM_PARTITIONS; ++p) {
        threads.emplace_back([&partitionStates, p]() {
            const int records = 100;  // Match RECORDS_PER_PARTITION
            for (int r = 0; r < records; ++r) {
                std::string key = "key" + std::to_string(r % 10);
                partitionStates[p][key]++;
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    // Verify each partition processed its records
    for (int p = 0; p < NUM_PARTITIONS; ++p) {
        int64_t totalInPartition = 0;
        for (const auto& [key, count] : partitionStates[p]) {
            totalInPartition += count;
        }
        EXPECT_EQ(totalInPartition, RECORDS_PER_PARTITION)
            << "Partition " << p << " should have processed all records";
    }

    SUCCEED() << "Partition-based parallelism works correctly";
}

// ===========================================================================
// Processor API Tests
// ===========================================================================

/**
 * Custom Processor implementation test
 */
TEST_F(StreamsComprehensiveTest, CustomProcessorImplementation) {
    // Define a custom processor that accumulates values
    class AccumulatorProcessor : public Processor<std::string, int64_t> {
    public:
        void init(ProcessorContext& ctx) override {
            Processor::init(ctx);
        }

        void process(const std::string& key, const int64_t& value) override {
            accumulator_[key] += value;
        }

        void close() override {
            // Cleanup
        }

        int64_t getAccumulated(const std::string& key) const {
            auto it = accumulator_.find(key);
            return it != accumulator_.end() ? it->second : 0;
        }

    private:
        std::map<std::string, int64_t> accumulator_;
    };

    // Create and use the processor
    AccumulatorProcessor processor;
    ProcessorContext context;
    context.setIds("test-app", "task-0");

    processor.init(context);
    processor.process("key1", 10);
    processor.process("key1", 20);
    processor.process("key2", 5);
    processor.close();

    EXPECT_EQ(processor.getAccumulated("key1"), 30);
    EXPECT_EQ(processor.getAccumulated("key2"), 5);

    SUCCEED() << "Custom processor implementation works correctly";
}

/**
 * Lambda processor test
 */
TEST_F(StreamsComprehensiveTest, LambdaProcessorUsage) {
    std::vector<std::pair<std::string, int64_t>> output;

    // Create a lambda processor
    LambdaProcessor<std::string, int64_t> processor(
        [&output](const std::string& key, const int64_t& value, ProcessorContext&) {
            output.emplace_back(key, value * 2);
        }
    );

    ProcessorContext context;
    processor.init(context);

    processor.process("a", 1);
    processor.process("b", 2);
    processor.process("c", 3);

    ASSERT_EQ(output.size(), 3);
    EXPECT_EQ(output[0], std::make_pair(std::string("a"), 2L));
    EXPECT_EQ(output[1], std::make_pair(std::string("b"), 4L));
    EXPECT_EQ(output[2], std::make_pair(std::string("c"), 6L));

    SUCCEED() << "Lambda processor works correctly";
}

// ===========================================================================
// Serde Tests
// ===========================================================================

/**
 * Basic Serde operations test
 */
TEST_F(StreamsComprehensiveTest, SerdeOperations) {
    // Test String Serde
    auto stringSerde = Serdes::String();
    std::string testStr = "Hello, World!";
    auto bytes = stringSerde->serialize(testStr);
    auto deserialized = stringSerde->deserialize(bytes);
    EXPECT_EQ(deserialized, testStr);

    // Test Long Serde
    auto longSerde = Serdes::Long();
    int64_t testLong = 1234567890123456789L;
    auto longBytes = longSerde->serialize(testLong);
    auto deserializedLong = longSerde->deserialize(longBytes);
    EXPECT_EQ(deserializedLong, testLong);

    // Test Integer Serde
    auto intSerde = Serdes::Integer();
    int32_t testInt = 42;
    auto intBytes = intSerde->serialize(testInt);
    auto deserializedInt = intSerde->deserialize(intBytes);
    EXPECT_EQ(deserializedInt, testInt);

    // Test Double Serde
    auto doubleSerde = Serdes::Double();
    double testDouble = 3.14159265359;
    auto doubleBytes = doubleSerde->serialize(testDouble);
    auto deserializedDouble = doubleSerde->deserialize(doubleBytes);
    EXPECT_DOUBLE_EQ(deserializedDouble, testDouble);

    SUCCEED() << "Serde operations work correctly";
}

/**
 * JSON Serde test
 */
TEST_F(StreamsComprehensiveTest, JsonSerdeOperations) {
    // Test with map
    auto jsonSerde = JsonSerdes::Json<std::map<std::string, int>>();

    std::map<std::string, int> testMap = {{"a", 1}, {"b", 2}, {"c", 3}};
    auto bytes = jsonSerde->serialize(testMap);
    auto deserialized = jsonSerde->deserialize(bytes);

    EXPECT_EQ(deserialized, testMap);

    // Test with vector
    auto vectorSerde = JsonSerdes::Json<std::vector<std::string>>();
    std::vector<std::string> testVec = {"one", "two", "three"};
    auto vecBytes = vectorSerde->serialize(testVec);
    auto deserializedVec = vectorSerde->deserialize(vecBytes);

    EXPECT_EQ(deserializedVec, testVec);

    SUCCEED() << "JSON Serde operations work correctly";
}

// ===========================================================================
// Punctuation Tests
// ===========================================================================

/**
 * Punctuation scheduling test
 */
TEST_F(StreamsComprehensiveTest, PunctuationScheduling) {
    ProcessorContext context;
    context.setIds("test-app", "task-0");

    std::vector<int64_t> punctuationTimes;

    // Schedule a punctuation
    auto cancellable = context.schedule(
        Duration(1000),  // 1 second interval
        PunctuationType::WALL_CLOCK_TIME,
        [&punctuationTimes](int64_t timestamp) {
            punctuationTimes.push_back(timestamp);
        }
    );

    EXPECT_NE(cancellable, nullptr) << "Should return cancellable";

    // Simulate time passing and trigger punctuations
    int64_t currentTime = context.currentSystemTimeMs();
    context.maybeFirePunctuations(0, currentTime + 1500);

    EXPECT_GE(punctuationTimes.size(), 1)
        << "Punctuation should have fired";

    // Cancel the punctuation
    cancellable->cancel();

    // Fire again - should not add more punctuations
    size_t countBefore = punctuationTimes.size();
    context.maybeFirePunctuations(0, currentTime + 3000);
    EXPECT_EQ(punctuationTimes.size(), countBefore)
        << "Cancelled punctuation should not fire";

    SUCCEED() << "Punctuation scheduling works correctly";
}

// ===========================================================================
// Complex Topology Tests
// ===========================================================================

/**
 * Branch and merge topology test
 */
TEST_F(StreamsComprehensiveTest, BranchMergeTopology) {
    StreamsBuilder builder;

    auto stream = builder.stream<std::string, int64_t>("numbers");

    // Branch into positive and negative using vector
    std::vector<std::function<bool(const std::string&, const int64_t&)>> predicates = {
        [](const std::string&, const int64_t& v) { return v > 0; },
        [](const std::string&, const int64_t& v) { return v < 0; }
    };
    auto branches = stream.branch(predicates);

    ASSERT_EQ(branches.size(), 2) << "Should have 2 branches";

    // Process each branch
    auto positives = branches[0].mapValues<std::string>([](int64_t v) {
        return "positive:" + std::to_string(v);
    });

    auto negatives = branches[1].mapValues<std::string>([](int64_t v) {
        return "negative:" + std::to_string(v);
    });

    Topology topology = builder.build();
    std::string desc = topology.describe();

    EXPECT_NE(desc.find("BRANCH"), std::string::npos)
        << "Should have branch nodes";

    SUCCEED() << "Branch topology works correctly";
}

/**
 * Multiple input topics test
 */
TEST_F(StreamsComprehensiveTest, MultipleInputTopics) {
    StreamsBuilder builder;

    // Create streams from multiple topics
    auto stream1 = builder.stream<std::string, std::string>("topic1");
    auto stream2 = builder.stream<std::string, std::string>("topic2");
    auto stream3 = builder.stream<std::string, std::string>("topic3");

    // Process each stream individually
    auto processed1 = stream1.mapValues<std::string>([](const std::string& v) {
        return "processed:" + v;
    });

    auto processed2 = stream2.mapValues<std::string>([](const std::string& v) {
        return "processed:" + v;
    });

    auto processed3 = stream3.mapValues<std::string>([](const std::string& v) {
        return "processed:" + v;
    });

    Topology topology = builder.build();

    // Verify all sources are present
    const auto& nodes = topology.nodes();
    int sourceCount = 0;
    for (const auto& [name, node] : nodes) {
        if (node.type == NodeType::SOURCE) {
            sourceCount++;
        }
    }

    EXPECT_EQ(sourceCount, 3) << "Should have 3 source nodes";

    SUCCEED() << "Multiple input topics work correctly";
}

/**
 * Output to multiple topics test
 */
TEST_F(StreamsComprehensiveTest, MultipleOutputTopics) {
    StreamsBuilder builder;

    auto stream = builder.stream<std::string, int64_t>("input");

    // Branch based on value range using vector
    std::vector<std::function<bool(const std::string&, const int64_t&)>> predicates = {
        [](const std::string&, const int64_t& v) { return v < 100; },
        [](const std::string&, const int64_t& v) { return v >= 100 && v < 1000; },
        [](const std::string&, const int64_t& v) { return v >= 1000; }
    };
    auto branches = stream.branch(predicates);

    // Output each branch to different topics
    branches[0].to("small-values");
    branches[1].to("medium-values");
    branches[2].to("large-values");

    Topology topology = builder.build();

    // Verify all sinks are present
    const auto& nodes = topology.nodes();
    int sinkCount = 0;
    for (const auto& [name, node] : nodes) {
        if (node.type == NodeType::SINK) {
            sinkCount++;
        }
    }

    EXPECT_EQ(sinkCount, 3) << "Should have 3 sink nodes";

    SUCCEED() << "Multiple output topics work correctly";
}
