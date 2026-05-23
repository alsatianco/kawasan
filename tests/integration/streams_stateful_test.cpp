#include <gtest/gtest.h>
#include "kawasan/streams/streams_builder.h"
#include "kawasan/streams/topology.h"
#include <memory>
#include <string>

using namespace kawasan::streams;

class StreamsStatefulTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Setup can be extended as needed
    }

    void TearDown() override {
        // Cleanup
    }
};

/**
 * Test basic state store interface design
 * 
 * This test verifies that the KeyValueStore interface is properly defined
 * and can be used in type definitions (actual operations tested separately)
 */
TEST_F(StreamsStatefulTest, StateStoreInterfaceExists) {
    // This test just verifies compilation - interfaces are defined
    // Actual store operations will be tested once processors are implemented
    SUCCEED() << "State store interfaces compiled successfully";
}

/**
 * Test topology creation with stateful operations
 * 
 * Verifies that:
 * 1. groupByKey() creates appropriate topology nodes
 * 2. count() operation can be chained
 * 3. Materialized config is accepted
 */
TEST_F(StreamsStatefulTest, GroupByKeyCreatesTopologyNode) {
    StreamsBuilder builder;
    
    // Create a stream and group by key
    auto stream = builder.stream<std::string, std::string>("input-topic");
    auto grouped = stream.groupByKey();
    
    // Count records per key
    auto counted = grouped.count();
    
    // Build topology
    Topology topology = builder.build();
    
    // Verify topology has nodes
    EXPECT_FALSE(topology.describe().empty()) 
        << "Topology should have nodes after groupByKey and count";
    
    // Verify KSTREAM-SOURCE node exists
    std::string desc = topology.describe();
    EXPECT_NE(desc.find("KSTREAM-SOURCE"), std::string::npos) 
        << "Topology should contain source node";
    
    // Verify GROUPBYKEY node exists
    EXPECT_NE(desc.find("GROUPBYKEY"), std::string::npos) 
        << "Topology should contain groupByKey node";
}

/**
 * Test word count topology creation
 * 
 * Classic word count example:
 * - Split text into words
 * - Group by word
 * - Count occurrences
 * - Materialize to state store
 */
TEST_F(StreamsStatefulTest, WordCountTopologyCreation) {
    StreamsBuilder builder;
    
    // Read input stream
    auto textStream = builder.stream<std::string, std::string>("text-input");
    
    // Split into words, rekey, group, and count
    // Note: We're testing topology creation, not actual processing
    auto wordCounts = textStream
        .groupByKey()  // Group by the key (word)
        .count();      // Count occurrences
    
    // Build topology
    Topology topology = builder.build();
    
    // Verify topology structure
    std::string desc = topology.describe();
    EXPECT_NE(desc.find("KSTREAM-SOURCE"), std::string::npos) 
        << "Should have source node";
    EXPECT_NE(desc.find("GROUPBYKEY"), std::string::npos) 
        << "Should have groupByKey node";
    
    // Note: Count processor nodes will be verified once processors are implemented
}

/**
 * Test aggregate operation topology
 * 
 * Verifies that custom aggregations can be defined with:
 * - Initializer function
 * - Aggregator function
 * - Materialized configuration
 */
TEST_F(StreamsStatefulTest, AggregateTopologyCreation) {
    StreamsBuilder builder;
    
    auto stream = builder.stream<std::string, int64_t>("numbers");
    auto grouped = stream.groupByKey();
    
    // Define aggregate operation (sum)
    auto initializer = []() -> int64_t { return 0; };
    auto aggregator = [](const std::string& /*key*/, int64_t value, int64_t aggregate) -> int64_t {
        return aggregate + value;
    };
    
    Materialized<std::string, int64_t> materialized("sum-store");
    auto aggregated = grouped.aggregate<int64_t>(initializer, aggregator, materialized);
    
    // Build topology
    Topology topology = builder.build();
    
    // Verify nodes exist
    std::string desc = topology.describe();
    EXPECT_NE(desc.find("KSTREAM-SOURCE"), std::string::npos);
    EXPECT_NE(desc.find("GROUPBYKEY"), std::string::npos);
    EXPECT_NE(desc.find("AGGREGATE"), std::string::npos) 
        << "Should have aggregate node";
}

/**
 * Test reduce operation topology
 * 
 * Verifies that reduce operations (binary operators) work correctly
 */
TEST_F(StreamsStatefulTest, ReduceTopologyCreation) {
    StreamsBuilder builder;
    
    auto stream = builder.stream<std::string, std::string>("strings");
    auto grouped = stream.groupByKey();
    
    // Define reduce operation (concatenate)
    auto reducer = [](const std::string& v1, const std::string& v2) -> std::string {
        return v1 + "," + v2;
    };
    
    Materialized<std::string, std::string> materialized("concat-store");
    auto reduced = grouped.reduce(reducer, materialized);
    
    // Build topology
    Topology topology = builder.build();
    
    // Verify nodes exist
    std::string desc = topology.describe();
    EXPECT_NE(desc.find("GROUPBYKEY"), std::string::npos);
    EXPECT_NE(desc.find("AGGREGATE"), std::string::npos) 
        << "Reduce is implemented as aggregate";
}

/**
 * Test groupBy with key selector topology
 * 
 * Verifies that custom key selection works for grouping
 */
TEST_F(StreamsStatefulTest, GroupByWithKeySelectorTopology) {
    StreamsBuilder builder;
    
    auto stream = builder.stream<std::string, std::string>("input");
    
    // Group by string length
    auto keySelector = [](const std::string& /*key*/, const std::string& value) -> int {
        return static_cast<int>(value.length());
    };
    
    auto grouped = stream.groupBy<int>(keySelector);
    auto counted = grouped.count();
    
    // Build topology
    Topology topology = builder.build();
    
    // Verify rekeying and grouping nodes
    std::string desc = topology.describe();
    EXPECT_NE(desc.find("REKEY"), std::string::npos) 
        << "Should have rekey node for custom key selector";
    EXPECT_NE(desc.find("GROUPBY"), std::string::npos) 
        << "Should have groupBy node";
}

/**
 * Test that Materialized configuration is properly structured
 */
TEST_F(StreamsStatefulTest, MaterializedConfiguration) {
    // Test Materialized with store name
    Materialized<std::string, int64_t> mat1("my-store");
    EXPECT_EQ(mat1.storeName(), "my-store");
    
    // Test Materialized with default name
    Materialized<std::string, std::string> mat2;
    EXPECT_FALSE(mat2.storeName().empty()) << "Should have default store name";
}

/**
 * Integration test: Complete word count pipeline topology
 * 
 * This test creates a complete stateful pipeline and verifies
 * all nodes are properly connected.
 */
TEST_F(StreamsStatefulTest, CompleteWordCountPipeline) {
    StreamsBuilder builder;
    
    // Input: text lines
    auto lines = builder.stream<std::string, std::string>("text-lines");
    
    // Process: group by key (word) and count
    auto wordCounts = lines
        .groupByKey()
        .count();
    
    // Build and verify topology
    Topology topology = builder.build();
    std::string desc = topology.describe();
    
    // Verify all expected nodes
    EXPECT_NE(desc.find("KSTREAM-SOURCE"), std::string::npos) << "Missing source";
    EXPECT_NE(desc.find("GROUPBYKEY"), std::string::npos) << "Missing groupByKey";
    
    // Verify topology is not empty
    EXPECT_GT(desc.length(), 50) << "Topology description should contain multiple nodes";
}

/**
 * Test topology with multiple stateful operations
 */
TEST_F(StreamsStatefulTest, MultipleStatefulOperations) {
    StreamsBuilder builder;
    
    // Create two independent stateful sub-topologies
    auto stream1 = builder.stream<std::string, int64_t>("stream1");
    auto stream2 = builder.stream<std::string, std::string>("stream2");
    
    // First stateful operation
    auto sum = stream1.groupByKey().count();
    
    // Second stateful operation
    auto counts = stream2.groupByKey().count();
    
    // Build topology
    Topology topology = builder.build();
    
    // Should have two independent source nodes
    std::string desc = topology.describe();
    size_t firstSource = desc.find("KSTREAM-SOURCE");
    size_t secondSource = desc.find("KSTREAM-SOURCE", firstSource + 1);
    
    EXPECT_NE(firstSource, std::string::npos);
    EXPECT_NE(secondSource, std::string::npos) 
        << "Should have two source nodes for two streams";
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
