#include <gtest/gtest.h>
#include "kawasan/connect/connector.h"
#include "kawasan/connect/file_source_connector.h"
#include "kawasan/connect/offset_storage.h"
#include <fstream>
#include <filesystem>
#include <chrono>
#include <thread>

using namespace kawasan::connect;

namespace {

// Helper to create temporary test directory
class TempDir {
public:
    TempDir() {
        path_ = std::filesystem::temp_directory_path() /
            ("connect_test_" + std::to_string(
                std::chrono::steady_clock::now().time_since_epoch().count()));
        std::filesystem::create_directories(path_);
    }

    ~TempDir() {
        std::filesystem::remove_all(path_);
    }

    std::string path() const { return path_.string(); }
    std::string file(const std::string& name) const {
        return (path_ / name).string();
    }

private:
    std::filesystem::path path_;
};

// Helper to write test files
void writeFile(const std::string& path, const std::string& content) {
    std::ofstream f(path);
    f << content;
}

void appendToFile(const std::string& path, const std::string& content) {
    std::ofstream f(path, std::ios::app);
    f << content;
}

}  // namespace

// ============================================================================
// FileSourceTask Tests
// ============================================================================

class FileSourceTaskTest : public ::testing::Test {
protected:
    void SetUp() override {
        tempDir_ = std::make_unique<TempDir>();
    }

    void TearDown() override {
        tempDir_.reset();
    }

    std::unique_ptr<TempDir> tempDir_;
};

TEST_F(FileSourceTaskTest, ReadsStringLines) {
    // Create test file
    std::string testFile = tempDir_->file("test.txt");
    writeFile(testFile,
        "line 1\n"
        "line 2\n"
        "line 3\n");

    // Create and start task
    FileSourceTask task;
    Properties config;
    config.set("name", "file-test");
    config.set("file.path", testFile);
    config.set("topic", "test-topic");
    config.set("schema", "STRING");
    config.set("batch.size", "10");

    task.start(config);

    // Poll for records
    auto records = task.poll();

    ASSERT_EQ(records.size(), 3);
    EXPECT_EQ(records[0].value, "line 1");
    EXPECT_EQ(records[1].value, "line 2");
    EXPECT_EQ(records[2].value, "line 3");
    EXPECT_EQ(records[0].topic, "test-topic");

    task.stop();
}

TEST_F(FileSourceTaskTest, ParsesJsonLines) {
    // Create test file with JSON
    std::string testFile = tempDir_->file("test.json");
    writeFile(testFile,
        R"({"id": 1, "name": "Alice"})" "\n"
        R"({"id": 2, "name": "Bob"})" "\n"
        R"({"id": 3, "name": "Charlie"})" "\n");

    // Create and start task
    FileSourceTask task;
    Properties config;
    config.set("name", "json-test");
    config.set("file.path", testFile);
    config.set("topic", "json-topic");
    config.set("schema", "JSON");

    task.start(config);

    auto records = task.poll();

    ASSERT_EQ(records.size(), 3);

    // Values should be the original JSON strings
    EXPECT_NE(records[0].value.find("Alice"), std::string::npos);
    EXPECT_NE(records[1].value.find("Bob"), std::string::npos);
    EXPECT_NE(records[2].value.find("Charlie"), std::string::npos);

    task.stop();
}

TEST_F(FileSourceTaskTest, ParsesCsvLines) {
    // Create test file with CSV
    std::string testFile = tempDir_->file("test.csv");
    writeFile(testFile,
        "id,name,email\n"
        "1,Alice,alice@example.com\n"
        "2,Bob,bob@example.com\n"
        "3,Charlie,charlie@example.com\n");

    // Create and start task
    FileSourceTask task;
    Properties config;
    config.set("name", "csv-test");
    config.set("file.path", testFile);
    config.set("topic", "csv-topic");
    config.set("schema", "CSV");
    config.set("csv.has.header", "true");
    config.set("csv.delimiter", ",");

    task.start(config);

    auto records = task.poll();

    ASSERT_EQ(records.size(), 3);

    // Values should be JSON with header keys
    nlohmann::json j1 = nlohmann::json::parse(records[0].value);
    EXPECT_EQ(j1["id"], "1");
    EXPECT_EQ(j1["name"], "Alice");
    EXPECT_EQ(j1["email"], "alice@example.com");

    // Key should be first field
    EXPECT_EQ(records[0].key.value_or(""), "1");
    EXPECT_EQ(records[1].key.value_or(""), "2");

    task.stop();
}

TEST_F(FileSourceTaskTest, HandlesQuotedCsv) {
    std::string testFile = tempDir_->file("quoted.csv");
    writeFile(testFile,
        "name,description\n"
        "\"Alice\",\"Hello, World\"\n"
        "\"Bob\",\"He said \"\"Hi\"\"\"\n");

    FileSourceTask task;
    Properties config;
    config.set("name", "quoted-csv-test");
    config.set("file.path", testFile);
    config.set("topic", "csv-topic");
    config.set("schema", "CSV");

    task.start(config);

    auto records = task.poll();

    ASSERT_EQ(records.size(), 2);

    nlohmann::json j1 = nlohmann::json::parse(records[0].value);
    EXPECT_EQ(j1["description"], "Hello, World");

    nlohmann::json j2 = nlohmann::json::parse(records[1].value);
    EXPECT_EQ(j2["description"], "He said \"Hi\"");

    task.stop();
}

TEST_F(FileSourceTaskTest, TracksBytePosition) {
    std::string testFile = tempDir_->file("position.txt");
    writeFile(testFile,
        "line 1\n"
        "line 2\n"
        "line 3\n");

    FileSourceTask task;
    Properties config;
    config.set("name", "position-test");
    config.set("file.path", testFile);
    config.set("topic", "test-topic");
    config.set("batch.size", "1");

    task.start(config);

    // Read first line
    auto records1 = task.poll();
    ASSERT_EQ(records1.size(), 1);
    EXPECT_EQ(records1[0].value, "line 1");

    int64_t pos1 = task.getCurrentPosition();
    EXPECT_GT(pos1, 0);

    // Read second line
    auto records2 = task.poll();
    ASSERT_EQ(records2.size(), 1);
    EXPECT_EQ(records2[0].value, "line 2");

    int64_t pos2 = task.getCurrentPosition();
    EXPECT_GT(pos2, pos1);

    // Check offset in source record
    auto offset = records2[0].sourceOffset;
    EXPECT_EQ(offset["position"], std::to_string(pos2));
    EXPECT_EQ(offset["line"], "2");

    task.stop();
}

TEST_F(FileSourceTaskTest, ResumesFromOffset) {
    std::string testFile = tempDir_->file("resume.txt");
    writeFile(testFile,
        "line 1\n"
        "line 2\n"
        "line 3\n");

    // First read - get position after line 1
    FileSourceTask task1;
    Properties config1;
    config1.set("name", "resume-test");
    config1.set("file.path", testFile);
    config1.set("topic", "test-topic");
    config1.set("batch.size", "1");

    task1.start(config1);
    auto records1 = task1.poll();
    int64_t position = task1.getCurrentPosition();
    task1.stop();

    // Second read - resume from saved position
    FileSourceTask task2;
    Properties config2;
    config2.set("name", "resume-test");
    config2.set("file.path", testFile);
    config2.set("topic", "test-topic");
    config2.set("batch.size", "10");
    config2.set("_offset.position", std::to_string(position));
    config2.set("_offset.line", "1");

    task2.start(config2);
    auto records2 = task2.poll();

    // Should only get lines 2 and 3
    ASSERT_EQ(records2.size(), 2);
    EXPECT_EQ(records2[0].value, "line 2");
    EXPECT_EQ(records2[1].value, "line 3");

    task2.stop();
}

TEST_F(FileSourceTaskTest, HandlesEmptyFile) {
    std::string testFile = tempDir_->file("empty.txt");
    writeFile(testFile, "");

    FileSourceTask task;
    Properties config;
    config.set("name", "empty-test");
    config.set("file.path", testFile);
    config.set("topic", "test-topic");

    task.start(config);

    auto records = task.poll();
    EXPECT_TRUE(records.empty());

    task.stop();
}

TEST_F(FileSourceTaskTest, HandlesMissingFile) {
    FileSourceTask task;
    Properties config;
    config.set("name", "missing-test");
    config.set("file.path", tempDir_->file("nonexistent.txt"));
    config.set("topic", "test-topic");

    task.start(config);

    auto records = task.poll();
    EXPECT_TRUE(records.empty());

    task.stop();
}

TEST_F(FileSourceTaskTest, HandlesMalformedJson) {
    std::string testFile = tempDir_->file("malformed.json");
    writeFile(testFile,
        R"({"valid": true})" "\n"
        "not valid json\n"
        R"({"also": "valid"})" "\n");

    FileSourceTask task;
    Properties config;
    config.set("name", "malformed-test");
    config.set("file.path", testFile);
    config.set("topic", "json-topic");
    config.set("schema", "JSON");

    task.start(config);

    auto records = task.poll();

    // Should skip the malformed line
    ASSERT_EQ(records.size(), 2);
    EXPECT_NE(records[0].value.find("valid"), std::string::npos);
    EXPECT_NE(records[1].value.find("also"), std::string::npos);

    task.stop();
}

TEST_F(FileSourceTaskTest, BatchesRecords) {
    // Create file with many lines
    std::string testFile = tempDir_->file("batch.txt");
    std::ofstream f(testFile);
    for (int i = 0; i < 100; ++i) {
        f << "line " << i << "\n";
    }
    f.close();

    FileSourceTask task;
    Properties config;
    config.set("name", "batch-test");
    config.set("file.path", testFile);
    config.set("topic", "test-topic");
    config.set("batch.size", "25");

    task.start(config);

    // First poll should return 25 records
    auto records1 = task.poll();
    EXPECT_EQ(records1.size(), 25);

    // Second poll should return next 25
    auto records2 = task.poll();
    EXPECT_EQ(records2.size(), 25);

    task.stop();
}

// ============================================================================
// FileSourceConnector Tests
// ============================================================================

TEST_F(FileSourceTaskTest, ConnectorValidatesConfig) {
    FileSourceConnector connector;

    // Missing name
    Properties config1;
    config1.set("file.path", "/tmp/test.txt");
    config1.set("topic", "test");
    EXPECT_FALSE(connector.validate(config1).empty());

    // Missing file.path
    Properties config2;
    config2.set("name", "test");
    config2.set("topic", "test");
    EXPECT_FALSE(connector.validate(config2).empty());

    // Missing topic
    Properties config3;
    config3.set("name", "test");
    config3.set("file.path", "/tmp/test.txt");
    EXPECT_FALSE(connector.validate(config3).empty());

    // Valid config
    Properties config4;
    config4.set("name", "test");
    config4.set("file.path", "/tmp/test.txt");
    config4.set("topic", "test");
    EXPECT_TRUE(connector.validate(config4).empty());

    // Invalid schema
    Properties config5;
    config5.set("name", "test");
    config5.set("file.path", "/tmp/test.txt");
    config5.set("topic", "test");
    config5.set("schema", "INVALID");
    EXPECT_FALSE(connector.validate(config5).empty());
}

TEST_F(FileSourceTaskTest, ConnectorDistributesFiles) {
    // Create multiple test files
    writeFile(tempDir_->file("a.txt"), "content a");
    writeFile(tempDir_->file("b.txt"), "content b");
    writeFile(tempDir_->file("c.txt"), "content c");

    FileSourceConnector connector;
    Properties config;
    config.set("name", "multi-file-test");
    config.set("file.path", tempDir_->path() + "/*.txt");
    config.set("topic", "test-topic");

    connector.start(config);

    // Request 2 tasks for 3 files
    auto taskConfigs = connector.taskConfigs(2);

    EXPECT_EQ(taskConfigs.size(), 2);

    // Each task should have at least one file
    for (const auto& tc : taskConfigs) {
        EXPECT_FALSE(tc.get("file.path").empty());
    }

    connector.stop();
}

// ============================================================================
// Integration Tests
// ============================================================================

TEST_F(FileSourceTaskTest, EndToEndFileSource) {
    // Create test file with 1000 lines
    std::string testFile = tempDir_->file("e2e.txt");
    {
        std::ofstream f(testFile);
        for (int i = 0; i < 1000; ++i) {
            f << "message " << i << "\n";
        }
    }

    // Create and start task
    FileSourceTask task;
    Properties config;
    config.set("name", "e2e-test");
    config.set("file.path", testFile);
    config.set("topic", "e2e-topic");
    config.set("batch.size", "100");

    task.start(config);

    // Read all records
    std::vector<SourceRecord> allRecords;
    while (true) {
        auto records = task.poll();
        if (records.empty()) break;
        allRecords.insert(allRecords.end(), records.begin(), records.end());
    }

    task.stop();

    // Verify all lines were read
    ASSERT_EQ(allRecords.size(), 1000);

    // Verify order
    for (int i = 0; i < 1000; ++i) {
        std::string expected = "message " + std::to_string(i);
        EXPECT_EQ(allRecords[i].value, expected);
    }
}

TEST_F(FileSourceTaskTest, OffsetTrackingRestart) {
    // Create test file
    std::string testFile = tempDir_->file("restart.txt");
    {
        std::ofstream f(testFile);
        for (int i = 0; i < 100; ++i) {
            f << "line " << i << "\n";
        }
    }

    // Create offset storage
    auto offsetStorage = std::make_unique<InMemoryOffsetStorage>();
    offsetStorage->init();

    // First run - read half the file
    {
        FileSourceTask task;
        Properties config;
        config.set("name", "restart-test");
        config.set("file.path", testFile);
        config.set("topic", "test-topic");
        config.set("batch.size", "50");

        task.start(config);

        auto records = task.poll();
        ASSERT_EQ(records.size(), 50);

        // Save offset
        for (const auto& record : records) {
            offsetStorage->store("restart-test", record.sourcePartition, record.sourceOffset);
        }

        task.stop();
    }

    // Get saved offset
    auto allOffsets = offsetStorage->loadAll("restart-test");
    EXPECT_FALSE(allOffsets.empty());

    // Second run - resume from saved offset
    {
        // Find the saved position
        std::string savedPosition;
        for (const auto& [key, offset] : allOffsets) {
            savedPosition = offset.at("position");
        }

        FileSourceTask task;
        Properties config;
        config.set("name", "restart-test");
        config.set("file.path", testFile);
        config.set("topic", "test-topic");
        config.set("batch.size", "100");
        config.set("_offset.position", savedPosition);
        config.set("_offset.line", "50");

        task.start(config);

        auto records = task.poll();

        // Should only get remaining 50 lines
        ASSERT_EQ(records.size(), 50);
        EXPECT_EQ(records[0].value, "line 50");
        EXPECT_EQ(records[49].value, "line 99");

        task.stop();
    }
}

TEST_F(FileSourceTaskTest, FileAppend) {
    // Create initial file
    std::string testFile = tempDir_->file("append.txt");
    writeFile(testFile, "line 1\nline 2\n");

    FileSourceTask task;
    Properties config;
    config.set("name", "append-test");
    config.set("file.path", testFile);
    config.set("topic", "test-topic");
    config.set("batch.size", "10");

    task.start(config);

    // Read initial content
    auto records1 = task.poll();
    ASSERT_EQ(records1.size(), 2);

    // Append more content
    appendToFile(testFile, "line 3\nline 4\n");

    // Read new content
    auto records2 = task.poll();
    ASSERT_EQ(records2.size(), 2);
    EXPECT_EQ(records2[0].value, "line 3");
    EXPECT_EQ(records2[1].value, "line 4");

    task.stop();
}

TEST_F(FileSourceTaskTest, LargeFile) {
    // Create large file (10MB+)
    std::string testFile = tempDir_->file("large.txt");
    {
        std::ofstream f(testFile);
        std::string line(1000, 'x');  // 1KB per line
        for (int i = 0; i < 10000; ++i) {
            f << i << ":" << line << "\n";
        }
    }

    FileSourceTask task;
    Properties config;
    config.set("name", "large-test");
    config.set("file.path", testFile);
    config.set("topic", "test-topic");
    config.set("batch.size", "1000");

    task.start(config);

    int totalRecords = 0;
    while (true) {
        auto records = task.poll();
        if (records.empty()) break;
        totalRecords += records.size();
    }

    task.stop();

    EXPECT_EQ(totalRecords, 10000);
}

// ============================================================================
// Factory Registration Tests
// ============================================================================

TEST(ConnectorFactoryTest, FileSourceRegistered) {
    EXPECT_TRUE(ConnectorFactory::isRegistered("FileSource"));

    auto connector = ConnectorFactory::create("FileSource");
    ASSERT_NE(connector, nullptr);
    EXPECT_EQ(connector->type(), "source");
    EXPECT_EQ(connector->version(), "1.0.0");
}

TEST(TaskFactoryTest, FileSourceTaskCreation) {
    auto task = TaskFactory::createSourceTask("FileSource");
    ASSERT_NE(task, nullptr);
    EXPECT_EQ(task->version(), "1.0.0");
}

// Main function
int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
