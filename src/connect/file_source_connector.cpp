#include "kawasan/connect/file_source_connector.h"
#include <spdlog/spdlog.h>
#include <glob.h>
#include <sys/stat.h>
#include <sstream>
#include <algorithm>

namespace kawasan {
namespace connect {

// ============================================================================
// FileSourceTask Implementation
// ============================================================================

FileSourceTask::~FileSourceTask() {
    closeFile();
}

void FileSourceTask::start(const Properties& config) {
    filePath_ = config.get("file.path");
    topic_ = config.get("topic");

    // Parse schema type
    std::string schemaStr = config.get("schema", "STRING");
    if (schemaStr == "JSON") {
        schema_ = FileSchema::JSON;
    } else if (schemaStr == "CSV") {
        schema_ = FileSchema::CSV;
    } else {
        schema_ = FileSchema::STRING;
    }

    // CSV settings
    csvDelimiter_ = config.get("csv.delimiter", ",");
    csvHasHeader_ = config.getBool("csv.has.header", true);

    // Other settings
    batchSize_ = config.getInt("batch.size", 100);
    pollIntervalMs_ = config.getLong("poll.interval.ms", 100);

    // Load starting position from offset (if resuming)
    position_ = config.getLong("_offset.position", 0);
    lineNumber_ = config.getLong("_offset.line", 0);
    std::string savedFile = config.get("_offset.file", "");

    if (!savedFile.empty()) {
        currentFilePath_ = savedFile;
    } else {
        currentFilePath_ = filePath_;
    }

    // Check if position is non-zero, meaning we're resuming
    if (position_ > 0) {
        headerSkipped_ = true;  // Header was already processed
    }

    running_ = true;
    spdlog::info("FileSourceTask started: file={}, topic={}, schema={}, position={}",
        currentFilePath_, topic_, schemaStr, position_);
}

void FileSourceTask::stop() {
    running_ = false;
    closeFile();
    spdlog::info("FileSourceTask stopped");
}

std::vector<SourceRecord> FileSourceTask::poll() {
    std::vector<SourceRecord> records;

    if (!running_) return records;

    // Open file if needed
    if (!currentFile_.is_open()) {
        if (!openFile()) {
            return records;
        }
    }

    // Check for file rotation
    if (checkFileRotation()) {
        spdlog::info("FileSourceTask detected file rotation, reopening");
        closeFile();
        position_ = 0;
        lineNumber_ = 0;
        headerSkipped_ = false;
        if (!openFile()) {
            return records;
        }
    }

    // Clear any previous EOF state and check if file has grown
    if (currentFile_.eof()) {
        struct stat st;
        if (stat(currentFilePath_.c_str(), &st) == 0 &&
            st.st_size > static_cast<off_t>(position_)) {
            // File has grown since last read, clear EOF and seek to refresh buffer
            currentFile_.clear();
            currentFile_.seekg(position_);
        }
    }

    // Read lines up to batch size
    int count = 0;
    while (count < batchSize_ && running_) {
        std::string line = readLine();
        if (line.empty() && currentFile_.eof()) {
            break;  // End of file
        }

        if (line.empty()) continue;

        // Skip CSV header if needed
        if (schema_ == FileSchema::CSV && csvHasHeader_ && !headerSkipped_) {
            csvHeaders_ = splitCsv(line);
            headerSkipped_ = true;
            continue;
        }

        try {
            SourceRecord record;
            switch (schema_) {
                case FileSchema::JSON:
                    record = parseJsonLine(line);
                    break;
                case FileSchema::CSV:
                    record = parseCsvLine(line);
                    break;
                default:
                    record = parseStringLine(line);
                    break;
            }

            records.push_back(std::move(record));
            count++;

        } catch (const std::exception& e) {
            spdlog::warn("FileSourceTask failed to parse line {}: {}",
                lineNumber_, e.what());
        }
    }

    return records;
}

void FileSourceTask::commit() {
    // Called after records are produced
    spdlog::debug("FileSourceTask committed at position {}, line {}",
        position_, lineNumber_);
}

void FileSourceTask::commitRecord(const SourceRecord& record) {
    (void)record;  // Individual record commit not needed for file source
}

bool FileSourceTask::openFile() {
    // Find the actual file (handle glob patterns)
    std::vector<std::string> files = findMatchingFiles(currentFilePath_);
    if (files.empty()) {
        spdlog::debug("FileSourceTask no matching files for: {}", currentFilePath_);
        return false;
    }

    // Use first matching file
    std::string actualPath = files[0];

    currentFile_.open(actualPath, std::ios::binary);
    if (!currentFile_.is_open()) {
        spdlog::error("FileSourceTask cannot open file: {}", actualPath);
        return false;
    }

    currentFilePath_ = actualPath;

    // Seek to saved position if resuming
    if (position_ > 0) {
        currentFile_.seekg(position_);
        if (currentFile_.fail()) {
            spdlog::warn("FileSourceTask seek failed, starting from beginning");
            currentFile_.clear();
            currentFile_.seekg(0);
            position_ = 0;
            lineNumber_ = 0;
        }
    }

    // Store file info for rotation detection
    struct stat st;
    if (stat(actualPath.c_str(), &st) == 0) {
        lastFileInfo_.path = actualPath;
        lastFileInfo_.size = st.st_size;
        lastFileInfo_.modTime = st.st_mtime;
        lastFileInfo_.inode = st.st_ino;
    }

    spdlog::info("FileSourceTask opened file: {} at position {}",
        actualPath, position_);
    return true;
}

void FileSourceTask::closeFile() {
    if (currentFile_.is_open()) {
        currentFile_.close();
    }
}

bool FileSourceTask::checkFileRotation() {
    if (currentFilePath_.empty()) return false;

    struct stat st;
    if (stat(currentFilePath_.c_str(), &st) != 0) {
        // File doesn't exist - might have been rotated/deleted
        return true;
    }

    // Check inode change (file was replaced)
    if (lastFileInfo_.inode != 0 && static_cast<int64_t>(st.st_ino) != lastFileInfo_.inode) {
        return true;
    }

    // Check if file was truncated (size decreased)
    if (st.st_size < lastFileInfo_.size && position_ > st.st_size) {
        return true;
    }

    // Update last known size
    lastFileInfo_.size = st.st_size;

    return false;
}

std::vector<std::string> FileSourceTask::findMatchingFiles(const std::string& pattern) {
    std::vector<std::string> result;

    // Check if it's a glob pattern
    if (pattern.find('*') != std::string::npos ||
        pattern.find('?') != std::string::npos) {

        glob_t globResult;
        int ret = glob(pattern.c_str(), GLOB_TILDE, nullptr, &globResult);

        if (ret == 0) {
            for (size_t i = 0; i < globResult.gl_pathc; ++i) {
                result.push_back(globResult.gl_pathv[i]);
            }
            globfree(&globResult);
        }
    } else {
        // Plain path - check if exists
        if (std::filesystem::exists(pattern)) {
            result.push_back(pattern);
        }
    }

    // Sort by modification time (oldest first)
    std::sort(result.begin(), result.end(), [](const std::string& a, const std::string& b) {
        struct stat stA, stB;
        stat(a.c_str(), &stA);
        stat(b.c_str(), &stB);
        return stA.st_mtime < stB.st_mtime;
    });

    return result;
}

std::string FileSourceTask::readLine() {
    std::string line;
    if (std::getline(currentFile_, line)) {
        // Track position after read
        position_ = currentFile_.tellg();
        lineNumber_++;

        // Remove trailing CR if present (Windows line endings)
        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }
    }
    return line;
}

SourceRecord FileSourceTask::parseStringLine(const std::string& line) {
    return SourceRecord(
        makePartition(),
        makeOffset(),
        topic_,
        line
    );
}

SourceRecord FileSourceTask::parseJsonLine(const std::string& line) {
    // Validate it's valid JSON
    auto j = nlohmann::json::parse(line);

    // Extract key if present
    std::optional<std::string> key;
    if (j.contains("key") && j["key"].is_string()) {
        key = j["key"].get<std::string>();
    }

    SourceRecord record;
    record.sourcePartition = makePartition();
    record.sourceOffset = makeOffset();
    record.topic = topic_;
    record.key = key;
    record.value = line;  // Keep original JSON string
    record.timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();

    return record;
}

SourceRecord FileSourceTask::parseCsvLine(const std::string& line) {
    auto fields = splitCsv(line);

    // Convert to JSON object using headers
    nlohmann::json j;
    for (size_t i = 0; i < fields.size() && i < csvHeaders_.size(); ++i) {
        j[csvHeaders_[i]] = fields[i];
    }

    SourceRecord record;
    record.sourcePartition = makePartition();
    record.sourceOffset = makeOffset();
    record.topic = topic_;
    record.value = j.dump();
    record.timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();

    // Use first field as key if available
    if (!fields.empty()) {
        record.key = fields[0];
    }

    return record;
}

std::vector<std::string> FileSourceTask::splitCsv(const std::string& line) {
    std::vector<std::string> result;
    std::string field;
    bool inQuotes = false;

    for (size_t i = 0; i < line.size(); ++i) {
        char c = line[i];

        if (c == '"') {
            if (inQuotes && i + 1 < line.size() && line[i + 1] == '"') {
                // Escaped quote
                field += '"';
                ++i;
            } else {
                inQuotes = !inQuotes;
            }
        } else if (c == csvDelimiter_[0] && !inQuotes) {
            result.push_back(field);
            field.clear();
        } else {
            field += c;
        }
    }

    result.push_back(field);
    return result;
}

std::map<std::string, std::string> FileSourceTask::makePartition() const {
    return {
        {"file", currentFilePath_},
        {"topic", topic_}
    };
}

std::map<std::string, std::string> FileSourceTask::makeOffset() const {
    return {
        {"position", std::to_string(position_)},
        {"line", std::to_string(lineNumber_)},
        {"file", currentFilePath_}
    };
}

// ============================================================================
// FileSourceConnector Implementation
// ============================================================================

void FileSourceConnector::start(const Properties& config) {
    config_ = config;
    spdlog::info("FileSourceConnector started");
}

std::vector<Properties> FileSourceConnector::taskConfigs(int maxTasks) {
    std::vector<Properties> configs;

    std::string filePath = config_.get("file.path");
    auto files = expandFilePaths(filePath);

    if (files.empty()) {
        // No files found, but still create one task to watch
        Properties taskConfig;
        taskConfig.setAll(config_.getAll());
        taskConfig.set("task.id", "0");
        configs.push_back(taskConfig);
        return configs;
    }

    // Distribute files across tasks
    int numTasks = std::min(maxTasks, static_cast<int>(files.size()));
    int filesPerTask = (files.size() + numTasks - 1) / numTasks;

    for (int i = 0; i < numTasks; ++i) {
        Properties taskConfig;
        taskConfig.setAll(config_.getAll());
        taskConfig.set("task.id", std::to_string(i));

        // Assign files to this task
        std::string taskFiles;
        for (int j = i * filesPerTask;
             j < std::min(static_cast<int>((i + 1) * filesPerTask), static_cast<int>(files.size()));
             ++j) {
            if (!taskFiles.empty()) taskFiles += ",";
            taskFiles += files[j];
        }

        if (!taskFiles.empty()) {
            taskConfig.set("file.path", taskFiles);
        }

        configs.push_back(taskConfig);
    }

    return configs;
}

void FileSourceConnector::stop() {
    spdlog::info("FileSourceConnector stopped");
}

std::string FileSourceConnector::validate(const Properties& config) {
    std::string baseError = SourceConnector::validate(config);
    if (!baseError.empty()) return baseError;

    if (!config.contains("file.path")) {
        return "Missing required property: file.path";
    }

    if (!config.contains("topic")) {
        return "Missing required property: topic";
    }

    // Validate schema if provided
    std::string schema = config.get("schema", "STRING");
    if (schema != "STRING" && schema != "JSON" && schema != "CSV") {
        return "Invalid schema type: " + schema + " (must be STRING, JSON, or CSV)";
    }

    return "";
}

std::vector<std::string> FileSourceConnector::expandFilePaths(const std::string& pattern) {
    std::vector<std::string> result;

    // Handle comma-separated patterns
    std::stringstream ss(pattern);
    std::string singlePattern;

    while (std::getline(ss, singlePattern, ',')) {
        // Trim whitespace
        size_t start = singlePattern.find_first_not_of(" \t");
        size_t end = singlePattern.find_last_not_of(" \t");
        if (start != std::string::npos) {
            singlePattern = singlePattern.substr(start, end - start + 1);
        }

        // Use glob to expand pattern
        glob_t globResult;
        int ret = glob(singlePattern.c_str(), GLOB_TILDE, nullptr, &globResult);

        if (ret == 0) {
            for (size_t i = 0; i < globResult.gl_pathc; ++i) {
                result.push_back(globResult.gl_pathv[i]);
            }
            globfree(&globResult);
        } else if (ret == GLOB_NOMATCH) {
            // No match, but if it's not a pattern, include it anyway
            if (singlePattern.find('*') == std::string::npos &&
                singlePattern.find('?') == std::string::npos) {
                result.push_back(singlePattern);
            }
        }
    }

    return result;
}

// ============================================================================
// MultiFileSourceTask Implementation
// ============================================================================

void MultiFileSourceTask::start(const Properties& config) {
    topic_ = config.get("topic");
    baseConfig_ = config;

    // Parse file list
    std::string fileList = config.get("file.path");
    std::stringstream ss(fileList);
    std::string file;
    while (std::getline(ss, file, ',')) {
        size_t start = file.find_first_not_of(" \t");
        size_t end = file.find_last_not_of(" \t");
        if (start != std::string::npos) {
            filePaths_.push_back(file.substr(start, end - start + 1));
        }
    }

    currentFileIndex_ = 0;
    running_ = true;

    spdlog::info("MultiFileSourceTask started with {} files", filePaths_.size());

    // Start first file
    if (!filePaths_.empty()) {
        moveToNextFile();
    }
}

void MultiFileSourceTask::stop() {
    running_ = false;
    if (currentTask_) {
        currentTask_->stop();
        currentTask_.reset();
    }
    spdlog::info("MultiFileSourceTask stopped");
}

std::vector<SourceRecord> MultiFileSourceTask::poll() {
    if (!running_ || !currentTask_) {
        return {};
    }

    auto records = currentTask_->poll();

    // Check if current file is exhausted and move to next
    if (records.empty()) {
        if (moveToNextFile()) {
            // Try polling the new file
            return currentTask_->poll();
        }
    }

    return records;
}

void MultiFileSourceTask::commit() {
    if (currentTask_) {
        currentTask_->commit();
    }
}

bool MultiFileSourceTask::moveToNextFile() {
    // Stop current task if any
    if (currentTask_) {
        currentTask_->stop();
        currentTask_.reset();
        currentFileIndex_++;
    }

    // Check if there are more files
    if (currentFileIndex_ >= filePaths_.size()) {
        return false;
    }

    // Create task for next file
    currentTask_ = std::make_unique<FileSourceTask>();

    Properties taskConfig = baseConfig_;
    taskConfig.set("file.path", filePaths_[currentFileIndex_]);

    currentTask_->start(taskConfig);

    spdlog::info("MultiFileSourceTask moved to file {}/{}: {}",
        currentFileIndex_ + 1, filePaths_.size(), filePaths_[currentFileIndex_]);

    return true;
}

// ============================================================================
// Static registration
// ============================================================================

namespace {

struct FileSourceRegistrar {
    FileSourceRegistrar() {
        ConnectorFactory::registerConnector<FileSourceConnector>("FileSource");
        TaskFactory::registerSourceTask("FileSource", []() {
            return std::make_unique<FileSourceTask>();
        });
    }
} fileSourceRegistrar;

}  // namespace

}  // namespace connect
}  // namespace kawasan
