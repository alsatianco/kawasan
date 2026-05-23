#include "kawasan/connect/file_sink_connector.h"
#include <spdlog/spdlog.h>
#include <nlohmann/json.hpp>
#include <sstream>
#include <iomanip>
#include <ctime>

namespace kawasan {
namespace connect {

// ============================================================================
// FileSinkTask Implementation
// ============================================================================

FileSinkTask::~FileSinkTask() {
    stop();
}

void FileSinkTask::start(const Properties& config) {
    basePath_ = config.get("file.path");
    topic_ = config.get("topic");

    // Parse format
    std::string formatStr = config.get("format", "TEXT");
    if (formatStr == "JSON") {
        format_ = FileSinkFormat::JSON;
        fileExtension_ = ".json";
    } else if (formatStr == "CSV") {
        format_ = FileSinkFormat::CSV;
        fileExtension_ = ".csv";
    } else {
        format_ = FileSinkFormat::TEXT;
        fileExtension_ = ".txt";
    }

    // Parse rolling policy
    std::string rollingStr = config.get("rolling.policy", "NONE");
    if (rollingStr == "SIZE") {
        rollingPolicy_ = RollingPolicy::SIZE;
    } else if (rollingStr == "TIME") {
        rollingPolicy_ = RollingPolicy::TIME;
    } else if (rollingStr == "SIZE_TIME") {
        rollingPolicy_ = RollingPolicy::SIZE_TIME;
    } else {
        rollingPolicy_ = RollingPolicy::NONE;
    }

    // Rolling settings
    maxFileSize_ = config.getLong("rolling.max.size", 100 * 1024 * 1024);
    rollIntervalMs_ = config.getLong("rolling.interval.ms", 3600000);

    // Other settings
    appendMode_ = config.getBool("append.mode", false);
    flushIntervalRecords_ = config.getInt("flush.records", 100);
    csvDelimiter_ = config.get("csv.delimiter", ",");

    // Parse CSV headers if provided
    std::string headersStr = config.get("csv.headers", "");
    if (!headersStr.empty()) {
        std::stringstream ss(headersStr);
        std::string header;
        while (std::getline(ss, header, ',')) {
            // Trim whitespace
            size_t start = header.find_first_not_of(" \t");
            size_t end = header.find_last_not_of(" \t");
            if (start != std::string::npos) {
                csvHeaders_.push_back(header.substr(start, end - start + 1));
            }
        }
    }

    // Create base directory if it doesn't exist
    std::filesystem::create_directories(basePath_);

    running_ = true;
    spdlog::info("FileSinkTask started: path={}, topic={}, format={}, rolling={}",
        basePath_, topic_, formatStr, rollingStr);
}

void FileSinkTask::stop() {
    if (!running_) return;

    running_ = false;

    std::lock_guard<std::mutex> lock(fileMutex_);
    for (auto& [partition, file] : outputFiles_) {
        closeFile(file);
    }
    outputFiles_.clear();

    spdlog::info("FileSinkTask stopped");
}

void FileSinkTask::put(const std::vector<SinkRecord>& records) {
    if (!running_) return;

    std::lock_guard<std::mutex> lock(fileMutex_);

    for (const auto& record : records) {
        OutputFile& file = getOutputFile(record.partition);

        // Check if we need to roll the file
        if (shouldRoll(file)) {
            rollFile(file);
        }

        writeRecord(file, record);
    }
}

void FileSinkTask::flush(const std::map<std::string, int64_t>& currentOffsets) {
    (void)currentOffsets;

    std::lock_guard<std::mutex> lock(fileMutex_);
    for (auto& [partition, file] : outputFiles_) {
        if (file.stream.is_open()) {
            file.stream.flush();
        }
    }

    spdlog::debug("FileSinkTask flushed all files");
}

void FileSinkTask::open(const std::vector<std::pair<std::string, int32_t>>& partitions) {
    std::lock_guard<std::mutex> lock(fileMutex_);

    for (const auto& [topic, partition] : partitions) {
        (void)topic;
        // Pre-create output file for partition
        getOutputFile(partition);
    }

    spdlog::info("FileSinkTask opened {} partitions", partitions.size());
}

void FileSinkTask::close(const std::vector<std::pair<std::string, int32_t>>& partitions) {
    std::lock_guard<std::mutex> lock(fileMutex_);

    for (const auto& [topic, partition] : partitions) {
        (void)topic;
        auto it = outputFiles_.find(partition);
        if (it != outputFiles_.end()) {
            closeFile(it->second);
            outputFiles_.erase(it);
        }
    }

    spdlog::info("FileSinkTask closed {} partitions", partitions.size());
}

FileSinkTask::OutputFile& FileSinkTask::getOutputFile(int32_t partition) {
    auto it = outputFiles_.find(partition);
    if (it != outputFiles_.end() && it->second.stream.is_open()) {
        return it->second;
    }

    // Create new output file
    OutputFile file;
    file.partition = partition;
    file.path = generateFilePath(partition);
    file.openedAt = currentTimeMs();

    // Open file
    std::ios_base::openmode mode = std::ios::out | std::ios::binary;
    if (appendMode_) {
        mode |= std::ios::app;
    }

    file.stream.open(file.path, mode);
    if (!file.stream.is_open()) {
        spdlog::error("FileSinkTask cannot open file: {}", file.path);
        throw std::runtime_error("Cannot open output file: " + file.path);
    }

    // Write CSV header if needed
    if (format_ == FileSinkFormat::CSV && !file.headerWritten && !appendMode_) {
        writeCsvHeader(file);
    }

    spdlog::info("FileSinkTask opened file: {} for partition {}", file.path, partition);

    outputFiles_[partition] = std::move(file);
    return outputFiles_[partition];
}

void FileSinkTask::closeFile(OutputFile& file) {
    if (file.stream.is_open()) {
        file.stream.flush();
        file.stream.close();
        spdlog::debug("FileSinkTask closed file: {} ({} records, {} bytes)",
            file.path, file.recordsWritten, file.bytesWritten);
    }
}

void FileSinkTask::rollFile(OutputFile& file) {
    closeFile(file);

    fileCounter_++;
    file.path = generateFilePath(file.partition);
    file.bytesWritten = 0;
    file.recordsWritten = 0;
    file.openedAt = currentTimeMs();
    file.headerWritten = false;

    file.stream.open(file.path, std::ios::out | std::ios::binary);
    if (!file.stream.is_open()) {
        spdlog::error("FileSinkTask cannot open rolled file: {}", file.path);
        throw std::runtime_error("Cannot open rolled file: " + file.path);
    }

    // Write CSV header for new file
    if (format_ == FileSinkFormat::CSV) {
        writeCsvHeader(file);
    }

    spdlog::info("FileSinkTask rolled to new file: {}", file.path);
}

bool FileSinkTask::shouldRoll(const OutputFile& file) const {
    if (rollingPolicy_ == RollingPolicy::NONE) {
        return false;
    }

    if (rollingPolicy_ == RollingPolicy::SIZE ||
        rollingPolicy_ == RollingPolicy::SIZE_TIME) {
        if (file.bytesWritten >= maxFileSize_) {
            return true;
        }
    }

    if (rollingPolicy_ == RollingPolicy::TIME ||
        rollingPolicy_ == RollingPolicy::SIZE_TIME) {
        if (currentTimeMs() - file.openedAt >= rollIntervalMs_) {
            return true;
        }
    }

    return false;
}

std::string FileSinkTask::generateFilePath(int32_t partition) const {
    // Get current time for filename
    auto now = std::chrono::system_clock::now();
    auto time = std::chrono::system_clock::to_time_t(now);
    std::tm tm = *std::localtime(&time);

    std::ostringstream ss;
    ss << basePath_ << "/";
    ss << topic_ << "-";
    ss << partition << "-";
    ss << std::put_time(&tm, "%Y%m%d-%H%M%S");

    if (rollingPolicy_ != RollingPolicy::NONE && fileCounter_ > 0) {
        ss << "-" << fileCounter_;
    }

    ss << fileExtension_;

    return ss.str();
}

void FileSinkTask::writeRecord(OutputFile& file, const SinkRecord& record) {
    std::string formatted = formatRecord(record);
    file.stream << formatted << "\n";

    file.bytesWritten += formatted.size() + 1;
    file.recordsWritten++;

    // Periodic flush
    if (flushIntervalRecords_ > 0 &&
        file.recordsWritten % flushIntervalRecords_ == 0) {
        file.stream.flush();
    }
}

std::string FileSinkTask::formatRecord(const SinkRecord& record) const {
    switch (format_) {
        case FileSinkFormat::JSON: {
            nlohmann::json j;
            j["topic"] = record.topic;
            j["partition"] = record.partition;
            j["offset"] = record.offset;
            if (record.key.has_value()) {
                j["key"] = record.key.value();
            }
            j["value"] = record.value;
            j["timestamp"] = record.timestamp;
            if (!record.headers.empty()) {
                j["headers"] = record.headers;
            }
            return j.dump();
        }

        case FileSinkFormat::CSV: {
            std::ostringstream ss;
            bool first = true;

            // If no headers defined, use: key, value, timestamp
            if (csvHeaders_.empty()) {
                ss << (record.key.value_or("")) << csvDelimiter_;
                // Escape CSV value
                std::string escapedValue = record.value;
                bool needsQuotes = escapedValue.find(csvDelimiter_[0]) != std::string::npos ||
                                   escapedValue.find('"') != std::string::npos ||
                                   escapedValue.find('\n') != std::string::npos;
                if (needsQuotes) {
                    // Escape quotes by doubling them
                    size_t pos = 0;
                    while ((pos = escapedValue.find('"', pos)) != std::string::npos) {
                        escapedValue.insert(pos, "\"");
                        pos += 2;
                    }
                    ss << "\"" << escapedValue << "\"";
                } else {
                    ss << escapedValue;
                }
                ss << csvDelimiter_ << record.timestamp;
            } else {
                // Try to parse value as JSON and extract fields
                try {
                    auto j = nlohmann::json::parse(record.value);
                    for (const auto& header : csvHeaders_) {
                        if (!first) ss << csvDelimiter_;
                        first = false;

                        if (header == "_key") {
                            ss << record.key.value_or("");
                        } else if (header == "_timestamp") {
                            ss << record.timestamp;
                        } else if (header == "_offset") {
                            ss << record.offset;
                        } else if (j.contains(header)) {
                            std::string val;
                            if (j[header].is_string()) {
                                val = j[header].get<std::string>();
                            } else {
                                val = j[header].dump();
                            }
                            // Escape if needed
                            if (val.find(csvDelimiter_[0]) != std::string::npos ||
                                val.find('"') != std::string::npos) {
                                size_t pos = 0;
                                while ((pos = val.find('"', pos)) != std::string::npos) {
                                    val.insert(pos, "\"");
                                    pos += 2;
                                }
                                ss << "\"" << val << "\"";
                            } else {
                                ss << val;
                            }
                        }
                    }
                } catch (...) {
                    // Value is not JSON, just output as-is
                    ss << record.value;
                }
            }

            return ss.str();
        }

        case FileSinkFormat::TEXT:
        default:
            return record.value;
    }
}

void FileSinkTask::writeCsvHeader(OutputFile& file) {
    if (file.headerWritten) return;

    std::ostringstream ss;
    if (csvHeaders_.empty()) {
        // Default headers
        ss << "key" << csvDelimiter_ << "value" << csvDelimiter_ << "timestamp";
    } else {
        bool first = true;
        for (const auto& header : csvHeaders_) {
            if (!first) ss << csvDelimiter_;
            first = false;
            ss << header;
        }
    }

    file.stream << ss.str() << "\n";
    file.bytesWritten += ss.str().size() + 1;
    file.headerWritten = true;
}

int64_t FileSinkTask::currentTimeMs() const {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

// ============================================================================
// FileSinkConnector Implementation
// ============================================================================

void FileSinkConnector::start(const Properties& config) {
    config_ = config;
    spdlog::info("FileSinkConnector started");
}

std::vector<Properties> FileSinkConnector::taskConfigs(int maxTasks) {
    std::vector<Properties> configs;

    // For file sink, typically one task per partition
    // For simplicity, we create maxTasks identical task configs
    for (int i = 0; i < maxTasks; ++i) {
        Properties taskConfig;
        taskConfig.setAll(config_.getAll());
        taskConfig.set("task.id", std::to_string(i));
        configs.push_back(taskConfig);
    }

    return configs;
}

void FileSinkConnector::stop() {
    spdlog::info("FileSinkConnector stopped");
}

std::string FileSinkConnector::validate(const Properties& config) {
    std::string baseError = SinkConnector::validate(config);
    if (!baseError.empty()) return baseError;

    if (!config.contains("file.path")) {
        return "Missing required property: file.path";
    }

    if (!config.contains("topic")) {
        return "Missing required property: topic";
    }

    // Validate format
    std::string format = config.get("format", "TEXT");
    if (format != "TEXT" && format != "JSON" && format != "CSV") {
        return "Invalid format: " + format + " (must be TEXT, JSON, or CSV)";
    }

    // Validate rolling policy
    std::string rolling = config.get("rolling.policy", "NONE");
    if (rolling != "NONE" && rolling != "SIZE" && rolling != "TIME" && rolling != "SIZE_TIME") {
        return "Invalid rolling.policy: " + rolling +
            " (must be NONE, SIZE, TIME, or SIZE_TIME)";
    }

    return "";
}

// ============================================================================
// Static registration
// ============================================================================

namespace {

struct FileSinkRegistrar {
    FileSinkRegistrar() {
        ConnectorFactory::registerConnector<FileSinkConnector>("FileSink");
        TaskFactory::registerSinkTask("FileSink", []() {
            return std::make_unique<FileSinkTask>();
        });
    }
} fileSinkRegistrar;

}  // namespace

}  // namespace connect
}  // namespace kawasan
