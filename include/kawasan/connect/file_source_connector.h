#pragma once

#include "connector.h"
#include <fstream>
#include <filesystem>
#include <regex>
#include <set>
#include <nlohmann/json.hpp>

namespace kawasan {
namespace connect {

/**
 * Schema type for parsing file content.
 */
enum class FileSchema {
    STRING,  // One line = one record (raw string)
    JSON,    // Parse each line as JSON
    CSV      // Parse each line as CSV
};

/**
 * File metadata for tracking.
 */
struct FileInfo {
    std::string path;
    int64_t size = 0;
    int64_t modTime = 0;
    int64_t inode = 0;  // For rotation detection
};

/**
 * File Source Task.
 *
 * Reads lines from file(s) and produces them as records.
 * Tracks byte position for resumption after restart.
 */
class FileSourceTask : public SourceTask {
public:
    FileSourceTask() = default;
    ~FileSourceTask() override;

    void start(const Properties& config) override;
    void stop() override;
    std::string version() const override { return "1.0.0"; }

    std::vector<SourceRecord> poll() override;
    void commit() override;
    void commitRecord(const SourceRecord& record) override;

    // Get current position for testing
    int64_t getCurrentPosition() const { return position_; }
    const std::string& getCurrentFile() const { return currentFilePath_; }

private:
    // Configuration
    std::string filePath_;
    std::string topic_;
    FileSchema schema_ = FileSchema::STRING;
    std::string csvDelimiter_ = ",";
    std::vector<std::string> csvHeaders_;
    bool csvHasHeader_ = true;
    int batchSize_ = 100;
    int64_t pollIntervalMs_ = 100;

    // State
    std::string currentFilePath_;
    std::ifstream currentFile_;
    int64_t position_ = 0;
    int64_t lineNumber_ = 0;
    bool running_ = false;
    bool headerSkipped_ = false;

    // File tracking for rotation detection
    FileInfo lastFileInfo_;

    // Methods
    bool openFile();
    void closeFile();
    bool checkFileRotation();
    std::vector<std::string> findMatchingFiles(const std::string& pattern);

    std::string readLine();
    SourceRecord parseStringLine(const std::string& line);
    SourceRecord parseJsonLine(const std::string& line);
    SourceRecord parseCsvLine(const std::string& line);
    std::vector<std::string> splitCsv(const std::string& line);

    std::map<std::string, std::string> makePartition() const;
    std::map<std::string, std::string> makeOffset() const;
};

/**
 * File Source Connector.
 *
 * Configuration properties:
 * - file.path: Path to file(s), supports glob patterns
 * - topic: Destination topic name
 * - tasks.max: Number of tasks (default: 1)
 * - schema: STRING, JSON, or CSV (default: STRING)
 * - csv.delimiter: CSV field delimiter (default: ",")
 * - csv.has.header: Whether CSV has header row (default: true)
 * - batch.size: Records per poll (default: 100)
 *
 * Example configuration:
 *   name=file-source
 *   connector.class=FileSource
 *   file.path=/var/log/app.log
 *   topic=app-logs
 *   tasks.max=1
 */
class FileSourceConnector : public SourceConnector {
public:
    void start(const Properties& config) override;
    std::vector<Properties> taskConfigs(int maxTasks) override;
    void stop() override;
    std::string version() const override { return "1.0.0"; }
    std::string validate(const Properties& config) override;

private:
    std::vector<std::string> expandFilePaths(const std::string& pattern);
};

/**
 * Multi-file source task that handles multiple files.
 *
 * Processes files in order, tracking position across files.
 */
class MultiFileSourceTask : public SourceTask {
public:
    void start(const Properties& config) override;
    void stop() override;
    std::string version() const override { return "1.0.0"; }

    std::vector<SourceRecord> poll() override;
    void commit() override;

private:
    std::vector<std::string> filePaths_;
    size_t currentFileIndex_ = 0;
    std::unique_ptr<FileSourceTask> currentTask_;
    std::string topic_;
    Properties baseConfig_;
    bool running_ = false;

    bool moveToNextFile();
};

}  // namespace connect
}  // namespace kawasan
