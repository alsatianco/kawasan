/**
 * Kawasan Connect Framework Tests
 *
 * Tests for the Kafka Connect-compatible framework:
 * - Connector lifecycle (start, stop, pause, resume)
 * - Task management
 * - Offset storage (file-based and in-memory)
 * - Source and sink connectors
 */

#include <gtest/gtest.h>
#include "kawasan/connect/connector.h"
#include "kawasan/connect/worker.h"
#include "kawasan/connect/offset_storage.h"
#include <filesystem>
#include <fstream>
#include <thread>
#include <chrono>

using namespace kawasan::connect;

// ============================================================================
// Properties Tests
// ============================================================================

TEST(PropertiesTest, BasicOperations) {
    Properties props;

    props.set("key1", "value1");
    props.set("key2", "42");
    props.set("key3", "true");

    EXPECT_EQ(props.get("key1"), "value1");
    EXPECT_EQ(props.get("key2"), "42");
    EXPECT_EQ(props.get("nonexistent"), "");
    EXPECT_EQ(props.get("nonexistent", "default"), "default");

    EXPECT_EQ(props.getInt("key2"), 42);
    EXPECT_EQ(props.getInt("nonexistent", 99), 99);

    EXPECT_TRUE(props.getBool("key3"));
    EXPECT_FALSE(props.getBool("nonexistent"));

    EXPECT_TRUE(props.contains("key1"));
    EXPECT_FALSE(props.contains("nonexistent"));
}

TEST(PropertiesTest, LongValues) {
    Properties props;

    props.set("big", "9223372036854775807");  // Max int64
    props.set("small", "-1000000000000");

    EXPECT_EQ(props.getLong("big"), 9223372036854775807LL);
    EXPECT_EQ(props.getLong("small"), -1000000000000LL);
    EXPECT_EQ(props.getLong("nonexistent", 123), 123);
}

TEST(PropertiesTest, BooleanValues) {
    Properties props;

    props.set("true1", "true");
    props.set("true2", "1");
    props.set("true3", "yes");
    props.set("false1", "false");
    props.set("false2", "0");
    props.set("false3", "no");

    EXPECT_TRUE(props.getBool("true1"));
    EXPECT_TRUE(props.getBool("true2"));
    EXPECT_TRUE(props.getBool("true3"));
    EXPECT_FALSE(props.getBool("false1"));
    EXPECT_FALSE(props.getBool("false2"));
    EXPECT_FALSE(props.getBool("false3"));
}

// ============================================================================
// Connector Factory Tests
// ============================================================================

TEST(ConnectorFactoryTest, RegisterAndCreate) {
    // Built-in connectors should be registered
    EXPECT_TRUE(ConnectorFactory::isRegistered("ConsoleSink"));
    EXPECT_TRUE(ConnectorFactory::isRegistered("GeneratorSource"));
    EXPECT_FALSE(ConnectorFactory::isRegistered("NonExistent"));

    // Create connector
    auto connector = ConnectorFactory::create("ConsoleSink");
    ASSERT_NE(connector, nullptr);
    EXPECT_EQ(connector->type(), "sink");
    EXPECT_EQ(connector->version(), "1.0.0");
}

TEST(ConnectorFactoryTest, ListConnectors) {
    auto connectors = ConnectorFactory::registeredConnectors();

    EXPECT_GE(connectors.size(), 2u);

    bool hasConsoleSink = false;
    bool hasGeneratorSource = false;

    for (const auto& name : connectors) {
        if (name == "ConsoleSink") hasConsoleSink = true;
        if (name == "GeneratorSource") hasGeneratorSource = true;
    }

    EXPECT_TRUE(hasConsoleSink);
    EXPECT_TRUE(hasGeneratorSource);
}

// ============================================================================
// Custom Test Connector
// ============================================================================

class TestSourceConnector : public SourceConnector {
public:
    void start(const Properties& config) override {
        config_ = config;
        started_ = true;
    }

    std::vector<Properties> taskConfigs(int maxTasks) override {
        std::vector<Properties> configs;
        for (int i = 0; i < maxTasks; ++i) {
            Properties tc;
            tc.setAll(config_.getAll());
            tc.set("task.id", std::to_string(i));
            configs.push_back(tc);
        }
        return configs;
    }

    void stop() override {
        started_ = false;
    }

    std::string version() const override { return "test-1.0"; }

    bool isStarted() const { return started_; }

private:
    bool started_ = false;
};

TEST(ConnectorTest, SourceConnectorLifecycle) {
    TestSourceConnector connector;

    EXPECT_EQ(connector.type(), "source");
    EXPECT_FALSE(connector.isStarted());

    Properties config;
    config.set("name", "test-connector");
    config.set("topic", "test-topic");
    config.set("tasks.max", "2");

    connector.start(config);
    EXPECT_TRUE(connector.isStarted());

    auto taskConfigs = connector.taskConfigs(2);
    EXPECT_EQ(taskConfigs.size(), 2u);
    EXPECT_EQ(taskConfigs[0].get("topic"), "test-topic");
    EXPECT_EQ(taskConfigs[0].get("task.id"), "0");
    EXPECT_EQ(taskConfigs[1].get("task.id"), "1");

    connector.stop();
    EXPECT_FALSE(connector.isStarted());
}

// ============================================================================
// Offset Storage Tests
// ============================================================================

TEST(InMemoryOffsetStorageTest, StoreAndLoad) {
    InMemoryOffsetStorage storage;
    storage.init();

    std::map<std::string, std::string> partition = {
        {"filename", "/var/log/app.log"}
    };
    std::map<std::string, std::string> offset = {
        {"position", "12345"},
        {"line", "100"}
    };

    // Store offset
    storage.store("my-connector", partition, offset);

    // Load offset
    auto loaded = storage.load("my-connector", partition);

    EXPECT_EQ(loaded["position"], "12345");
    EXPECT_EQ(loaded["line"], "100");

    // Load non-existent
    std::map<std::string, std::string> otherPartition = {{"filename", "other.log"}};
    auto empty = storage.load("my-connector", otherPartition);
    EXPECT_TRUE(empty.empty());
}

TEST(InMemoryOffsetStorageTest, LoadAll) {
    InMemoryOffsetStorage storage;
    storage.init();

    // Store multiple partitions
    storage.store("conn1", {{"file", "a.log"}}, {{"pos", "100"}});
    storage.store("conn1", {{"file", "b.log"}}, {{"pos", "200"}});
    storage.store("conn2", {{"file", "c.log"}}, {{"pos", "300"}});

    // Load all for conn1
    auto all = storage.loadAll("conn1");
    EXPECT_EQ(all.size(), 2u);

    // Load all for conn2
    auto all2 = storage.loadAll("conn2");
    EXPECT_EQ(all2.size(), 1u);

    // Load all for non-existent
    auto empty = storage.loadAll("nonexistent");
    EXPECT_TRUE(empty.empty());
}

TEST(InMemoryOffsetStorageTest, Remove) {
    InMemoryOffsetStorage storage;
    storage.init();

    storage.store("conn1", {{"file", "a.log"}}, {{"pos", "100"}});
    storage.store("conn2", {{"file", "b.log"}}, {{"pos", "200"}});

    // Remove conn1
    storage.remove("conn1");

    EXPECT_TRUE(storage.loadAll("conn1").empty());
    EXPECT_FALSE(storage.loadAll("conn2").empty());
}

TEST(FileOffsetStorageTest, PersistenceRoundTrip) {
    std::string testFile = "/tmp/test-offsets-" +
        std::to_string(std::chrono::system_clock::now().time_since_epoch().count()) + ".json";

    // Clean up if exists
    if (std::filesystem::exists(testFile)) {
        std::filesystem::remove(testFile);
    }

    // Store some offsets
    {
        FileOffsetStorage storage(testFile);
        storage.init();

        storage.store("connector1", {{"partition", "0"}}, {{"offset", "100"}});
        storage.store("connector1", {{"partition", "1"}}, {{"offset", "200"}});
        storage.store("connector2", {{"topic", "test"}}, {{"position", "999"}});

        storage.flush();
        storage.close();
    }

    // Verify file exists
    EXPECT_TRUE(std::filesystem::exists(testFile));

    // Reload and verify
    {
        FileOffsetStorage storage(testFile);
        storage.init();

        auto offset1 = storage.load("connector1", {{"partition", "0"}});
        EXPECT_EQ(offset1["offset"], "100");

        auto offset2 = storage.load("connector1", {{"partition", "1"}});
        EXPECT_EQ(offset2["offset"], "200");

        auto offset3 = storage.load("connector2", {{"topic", "test"}});
        EXPECT_EQ(offset3["position"], "999");

        storage.close();
    }

    // Clean up
    std::filesystem::remove(testFile);
}

TEST(OffsetStorageFactoryTest, CreateByType) {
    auto fileStorage = OffsetStorageFactory::create("file", {{"file.path", "/tmp/test.json"}});
    ASSERT_NE(fileStorage, nullptr);

    auto memStorage = OffsetStorageFactory::create("memory");
    ASSERT_NE(memStorage, nullptr);

    auto defaultStorage = OffsetStorageFactory::create("unknown");
    ASSERT_NE(defaultStorage, nullptr);  // Falls back to in-memory
}

// ============================================================================
// Source Record Tests
// ============================================================================

TEST(SourceRecordTest, Construction) {
    std::map<std::string, std::string> partition = {{"file", "log.txt"}};
    std::map<std::string, std::string> offset = {{"pos", "123"}};

    SourceRecord record(partition, offset, "my-topic", "test-value");

    EXPECT_EQ(record.topic, "my-topic");
    EXPECT_EQ(record.value, "test-value");
    EXPECT_FALSE(record.key.has_value());
    EXPECT_EQ(record.sourcePartition["file"], "log.txt");
    EXPECT_EQ(record.sourceOffset["pos"], "123");
    EXPECT_GT(record.timestamp, 0);
}

TEST(SourceRecordTest, WithKey) {
    std::map<std::string, std::string> partition = {{"file", "log.txt"}};
    std::map<std::string, std::string> offset = {{"pos", "123"}};

    SourceRecord record(partition, offset, "my-topic", "my-key", "my-value");

    EXPECT_TRUE(record.key.has_value());
    EXPECT_EQ(*record.key, "my-key");
    EXPECT_EQ(record.value, "my-value");
}

// ============================================================================
// Sink Record Tests
// ============================================================================

TEST(SinkRecordTest, Construction) {
    SinkRecord record("topic", 0, 123, "value");

    EXPECT_EQ(record.topic, "topic");
    EXPECT_EQ(record.partition, 0);
    EXPECT_EQ(record.offset, 123);
    EXPECT_EQ(record.value, "value");
    EXPECT_FALSE(record.key.has_value());
}

TEST(SinkRecordTest, WithKey) {
    SinkRecord record("topic", 1, 456, "key", "value");

    EXPECT_TRUE(record.key.has_value());
    EXPECT_EQ(*record.key, "key");
    EXPECT_EQ(record.partition, 1);
}

// ============================================================================
// Generator Source Task Tests
// ============================================================================

TEST(GeneratorSourceTaskTest, GeneratesRecords) {
    GeneratorSourceTask task;

    Properties config;
    config.set("topic", "test-topic");
    config.set("interval.ms", "10");  // Fast for testing
    config.set("key.prefix", "k-");
    config.set("value.prefix", "v-");

    task.start(config);

    // Poll for records
    auto records1 = task.poll();
    ASSERT_EQ(records1.size(), 1u);
    EXPECT_EQ(records1[0].topic, "test-topic");
    EXPECT_EQ(*records1[0].key, "k-0");
    EXPECT_EQ(records1[0].value, "v-0");

    auto records2 = task.poll();
    ASSERT_EQ(records2.size(), 1u);
    EXPECT_EQ(*records2[0].key, "k-1");

    task.stop();
}

TEST(GeneratorSourceTaskTest, RespectsMaxRecords) {
    GeneratorSourceTask task;

    Properties config;
    config.set("topic", "test-topic");
    config.set("interval.ms", "1");
    config.set("max.records", "3");

    task.start(config);

    // Should generate exactly 3 records
    task.poll();  // Record 0
    task.poll();  // Record 1
    task.poll();  // Record 2

    // Should return empty after max reached
    auto empty = task.poll();
    EXPECT_TRUE(empty.empty());

    task.stop();
}

// ============================================================================
// Console Sink Task Tests
// ============================================================================

TEST(ConsoleSinkTaskTest, PutsRecords) {
    ConsoleSinkTask task;

    Properties config;
    config.set("prefix", "[TEST] ");

    task.start(config);

    std::vector<SinkRecord> records = {
        SinkRecord("topic", 0, 1, "key1", "value1"),
        SinkRecord("topic", 0, 2, "key2", "value2")
    };

    // This just prints to stdout - verify it doesn't crash
    EXPECT_NO_THROW(task.put(records));

    task.stop();
}

// ============================================================================
// Connect Worker Tests
// ============================================================================

TEST(ConnectWorkerTest, StartStop) {
    WorkerConfig config;
    config.offsetStorageType = "memory";
    config.offsetFlushIntervalMs = 100;

    ConnectWorker worker(config);

    EXPECT_FALSE(worker.isRunning());

    worker.start();
    EXPECT_TRUE(worker.isRunning());

    // Let it run briefly
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    worker.stop();
    EXPECT_FALSE(worker.isRunning());
}

TEST(ConnectWorkerTest, ConnectorManagement) {
    WorkerConfig config;
    config.offsetStorageType = "memory";
    config.offsetFlushIntervalMs = 1000;

    ConnectWorker worker(config);
    worker.start();

    // Start a connector
    Properties connConfig;
    connConfig.set("name", "test-generator");
    connConfig.set("connector.class", "GeneratorSource");
    connConfig.set("topic", "generated");
    connConfig.set("tasks.max", "1");
    connConfig.set("interval.ms", "100");
    connConfig.set("max.records", "5");

    std::string error = worker.startConnector("test-generator", connConfig);
    EXPECT_EQ(error, "");

    // Check connector status
    auto status = worker.getConnectorStatus("test-generator");
    EXPECT_EQ(status.name, "test-generator");
    EXPECT_EQ(status.status, ConnectorStatus::RUNNING);
    EXPECT_EQ(status.type, "source");
    EXPECT_EQ(status.tasks.size(), 1u);

    // Get all connectors
    auto names = worker.getConnectorNames();
    EXPECT_EQ(names.size(), 1u);
    EXPECT_EQ(names[0], "test-generator");

    // Stop connector
    error = worker.stopConnector("test-generator");
    EXPECT_EQ(error, "");

    names = worker.getConnectorNames();
    EXPECT_TRUE(names.empty());

    worker.stop();
}

TEST(ConnectWorkerTest, DuplicateConnectorRejected) {
    WorkerConfig config;
    config.offsetStorageType = "memory";

    ConnectWorker worker(config);
    worker.start();

    Properties connConfig;
    connConfig.set("name", "dup-test");
    connConfig.set("connector.class", "GeneratorSource");
    connConfig.set("topic", "test");

    // First should succeed
    std::string error = worker.startConnector("dup-test", connConfig);
    EXPECT_EQ(error, "");

    // Second should fail
    error = worker.startConnector("dup-test", connConfig);
    EXPECT_NE(error, "");
    EXPECT_NE(error.find("already exists"), std::string::npos);

    worker.stopConnector("dup-test");
    worker.stop();
}

TEST(ConnectWorkerTest, InvalidConnectorClass) {
    WorkerConfig config;
    config.offsetStorageType = "memory";

    ConnectWorker worker(config);
    worker.start();

    Properties connConfig;
    connConfig.set("name", "invalid");
    connConfig.set("connector.class", "NonExistentConnector");

    std::string error = worker.startConnector("invalid", connConfig);
    EXPECT_NE(error, "");
    EXPECT_NE(error.find("Unknown connector"), std::string::npos);

    worker.stop();
}

TEST(ConnectWorkerTest, MissingRequiredConfig) {
    WorkerConfig config;
    config.offsetStorageType = "memory";

    ConnectWorker worker(config);
    worker.start();

    // Missing connector.class
    Properties badConfig;
    badConfig.set("name", "test");

    std::string error = worker.startConnector("test", badConfig);
    EXPECT_NE(error, "");
    EXPECT_NE(error.find("connector.class"), std::string::npos);

    worker.stop();
}

TEST(ConnectWorkerTest, PauseResume) {
    WorkerConfig config;
    config.offsetStorageType = "memory";

    ConnectWorker worker(config);
    worker.start();

    Properties connConfig;
    connConfig.set("name", "pause-test");
    connConfig.set("connector.class", "GeneratorSource");
    connConfig.set("topic", "test");
    connConfig.set("interval.ms", "1000");

    worker.startConnector("pause-test", connConfig);

    // Pause
    std::string error = worker.pauseConnector("pause-test");
    EXPECT_EQ(error, "");

    auto status = worker.getConnectorStatus("pause-test");
    EXPECT_EQ(status.status, ConnectorStatus::PAUSED);

    // Resume
    error = worker.resumeConnector("pause-test");
    EXPECT_EQ(error, "");

    status = worker.getConnectorStatus("pause-test");
    EXPECT_EQ(status.status, ConnectorStatus::RUNNING);

    worker.stopConnector("pause-test");
    worker.stop();
}

// ============================================================================
// Connector Validation Tests
// ============================================================================

TEST(GeneratorSourceConnectorTest, Validation) {
    GeneratorSourceConnector connector;

    // Missing name
    Properties missingName;
    missingName.set("connector.class", "GeneratorSource");
    missingName.set("topic", "test");
    EXPECT_NE(connector.validate(missingName), "");

    // Missing topic
    Properties missingTopic;
    missingTopic.set("name", "test");
    missingTopic.set("connector.class", "GeneratorSource");
    EXPECT_NE(connector.validate(missingTopic), "");

    // Valid
    Properties valid;
    valid.set("name", "test");
    valid.set("connector.class", "GeneratorSource");
    valid.set("topic", "test");
    EXPECT_EQ(connector.validate(valid), "");
}

TEST(ConsoleSinkConnectorTest, Validation) {
    ConsoleSinkConnector connector;

    // Missing topics
    Properties missingTopics;
    missingTopics.set("name", "test");
    missingTopics.set("connector.class", "ConsoleSink");
    EXPECT_NE(connector.validate(missingTopics), "");

    // Valid
    Properties valid;
    valid.set("name", "test");
    valid.set("connector.class", "ConsoleSink");
    valid.set("topics", "test-topic");
    EXPECT_EQ(connector.validate(valid), "");
}

// ============================================================================
// Integration Test
// ============================================================================

TEST(ConnectIntegrationTest, EndToEndSourceToConsole) {
    // This test verifies the complete flow:
    // 1. Start worker
    // 2. Create a generator source (produces records)
    // 3. Verify records are generated (we check via task status)
    // 4. Clean shutdown

    WorkerConfig config;
    config.offsetStorageType = "memory";
    config.pollIntervalMs = 10;
    config.offsetFlushIntervalMs = 100;

    ConnectWorker worker(config);
    worker.start();

    // Start generator source
    Properties sourceConfig;
    sourceConfig.set("name", "integration-source");
    sourceConfig.set("connector.class", "GeneratorSource");
    sourceConfig.set("topic", "integration-topic");
    sourceConfig.set("tasks.max", "1");
    sourceConfig.set("interval.ms", "50");
    sourceConfig.set("max.records", "10");

    std::string error = worker.startConnector("integration-source", sourceConfig);
    ASSERT_EQ(error, "") << "Failed to start connector: " << error;

    // Let it run for a bit to generate some records
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    // Check status
    auto status = worker.getConnectorStatus("integration-source");
    EXPECT_EQ(status.status, ConnectorStatus::RUNNING);
    EXPECT_EQ(status.tasks.size(), 1u);

    // Stop and verify clean shutdown
    error = worker.stopConnector("integration-source");
    EXPECT_EQ(error, "");

    worker.stop();
    EXPECT_FALSE(worker.isRunning());
}
