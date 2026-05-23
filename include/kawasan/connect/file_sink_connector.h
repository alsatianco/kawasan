#pragma once

#include "kawasan/connect/connector.h"
#include <fstream>
#include <filesystem>
#include <mutex>
#include <chrono>

namespace kawasan {
namespace connect {

/**
 * Output format for file sink.
 */
enum class FileSinkFormat {
    TEXT,       // Plain text, one record per line (value only)
    JSON,       // JSON Lines format (full record as JSON)
    CSV         // CSV format with headers
};

/**
 * File rolling policy.
 */
enum class RollingPolicy {
    NONE,       // Single file, no rolling
    SIZE,       // Roll when file reaches size limit
    TIME,       // Roll at time intervals
    SIZE_TIME   // Roll on size OR time, whichever comes first
};

/**
 * FileSinkTask - Writes records to file(s).
 *
 * Features:
 * - Multiple output formats (TEXT, JSON, CSV)
 * - Partition-based file naming
 * - File rolling (size or time based)
 * - Configurable flush behavior
 */
class FileSinkTask : public SinkTask {
public:
    ~FileSinkTask() override;

    std::string version() const override { return "1.0.0"; }
    void start(const Properties& config) override;
    void stop() override;
    void put(const std::vector<SinkRecord>& records) override;
    void flush(const std::map<std::string, int64_t>& currentOffsets) override;
    void open(const std::vector<std::pair<std::string, int32_t>>& partitions) override;
    void close(const std::vector<std::pair<std::string, int32_t>>& partitions) override;

private:
    // File management
    struct OutputFile {
        std::string path;
        std::ofstream stream;
        int64_t bytesWritten = 0;
        int64_t recordsWritten = 0;
        int64_t openedAt = 0;
        int32_t partition = 0;
        bool headerWritten = false;
    };

    // Get or create output file for partition
    OutputFile& getOutputFile(int32_t partition);

    // Close and optionally roll a file
    void closeFile(OutputFile& file);
    void rollFile(OutputFile& file);

    // Check if file should be rolled
    bool shouldRoll(const OutputFile& file) const;

    // Generate file path for partition
    std::string generateFilePath(int32_t partition) const;

    // Write a single record to file
    void writeRecord(OutputFile& file, const SinkRecord& record);

    // Format record based on output format
    std::string formatRecord(const SinkRecord& record) const;

    // Write CSV header if needed
    void writeCsvHeader(OutputFile& file);

    // Get current timestamp in milliseconds
    int64_t currentTimeMs() const;

    // Configuration
    std::string basePath_;
    std::string topic_;
    FileSinkFormat format_ = FileSinkFormat::TEXT;
    RollingPolicy rollingPolicy_ = RollingPolicy::NONE;
    int64_t maxFileSize_ = 100 * 1024 * 1024;  // 100MB default
    int64_t rollIntervalMs_ = 3600000;          // 1 hour default
    bool appendMode_ = false;
    int flushIntervalRecords_ = 100;
    std::string fileExtension_;
    std::string csvDelimiter_ = ",";
    std::vector<std::string> csvHeaders_;

    // State
    bool running_ = false;
    std::mutex fileMutex_;
    std::map<int32_t, OutputFile> outputFiles_;
    int fileCounter_ = 0;
};

/**
 * FileSinkConnector - Connector for writing to files.
 *
 * Configuration:
 * - file.path: Base directory for output files (required)
 * - topic: Source topic (required)
 * - format: Output format - TEXT, JSON, CSV (default: TEXT)
 * - rolling.policy: NONE, SIZE, TIME, SIZE_TIME (default: NONE)
 * - rolling.max.size: Max file size in bytes (default: 100MB)
 * - rolling.interval.ms: Roll interval in ms (default: 1 hour)
 * - append.mode: Append to existing files (default: false)
 * - flush.records: Flush every N records (default: 100)
 * - csv.delimiter: CSV delimiter (default: ,)
 * - csv.headers: Comma-separated header names for CSV
 */
class FileSinkConnector : public SinkConnector {
public:
    std::string version() const override { return "1.0.0"; }
    void start(const Properties& config) override;
    std::vector<Properties> taskConfigs(int maxTasks) override;
    void stop() override;
    std::string validate(const Properties& config) override;

private:
    Properties config_;
};

}  // namespace connect
}  // namespace kawasan
