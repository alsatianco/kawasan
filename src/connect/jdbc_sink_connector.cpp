#include "kawasan/connect/jdbc_sink_connector.h"
#include <spdlog/spdlog.h>
#include <nlohmann/json.hpp>
#include <sstream>
#include <thread>
#include <chrono>

namespace kawasan {
namespace connect {

// ============================================================================
// JdbcSinkTask Implementation
// ============================================================================

JdbcSinkTask::~JdbcSinkTask() {
    stop();
}

void JdbcSinkTask::start(const Properties& config) {
    connectionUrl_ = config.get("connection.url");
    username_ = config.get("connection.user", "");
    password_ = config.get("connection.password", "");
    tableName_ = config.get("table.name");

    // Parse insert mode
    std::string modeStr = config.get("insert.mode", "INSERT");
    if (modeStr == "UPSERT") {
        insertMode_ = JdbcInsertMode::UPSERT;
    } else if (modeStr == "UPDATE") {
        insertMode_ = JdbcInsertMode::UPDATE;
    } else {
        insertMode_ = JdbcInsertMode::INSERT;
    }

    // Parse primary key fields
    std::string pkStr = config.get("pk.fields", "");
    if (!pkStr.empty()) {
        std::stringstream ss(pkStr);
        std::string field;
        while (std::getline(ss, field, ',')) {
            size_t start = field.find_first_not_of(" \t");
            size_t end = field.find_last_not_of(" \t");
            if (start != std::string::npos) {
                pkFields_.push_back(field.substr(start, end - start + 1));
            }
        }
    }

    // Parse fields
    std::string fieldsStr = config.get("fields", "");
    if (!fieldsStr.empty()) {
        std::stringstream ss(fieldsStr);
        std::string field;
        while (std::getline(ss, field, ',')) {
            size_t start = field.find_first_not_of(" \t");
            size_t end = field.find_last_not_of(" \t");
            if (start != std::string::npos) {
                fields_.push_back(field.substr(start, end - start + 1));
            }
        }
    }

    // Other settings
    batchSize_ = config.getInt("batch.size", 500);
    autoCreateTable_ = config.getBool("auto.create", false);
    maxRetries_ = config.getInt("max.retries", 3);
    retryBackoffMs_ = config.getLong("retry.backoff.ms", 1000);

    running_ = true;

    // Connect to database
    if (!connect()) {
        spdlog::error("JdbcSinkTask failed to connect to database");
        throw std::runtime_error("Failed to connect to database");
    }

    spdlog::info("JdbcSinkTask started: table={}, mode={}, batch_size={}",
        tableName_, modeStr, batchSize_);
}

void JdbcSinkTask::stop() {
    if (!running_) return;

    running_ = false;

    // Flush any pending records
    {
        std::lock_guard<std::mutex> lock(batchMutex_);
        if (!pendingBatch_.empty()) {
            executeBatch();
        }
    }

    disconnect();

    spdlog::info("JdbcSinkTask stopped: {} records written in {} batches",
        recordsWritten_, batchesWritten_);
}

void JdbcSinkTask::put(const std::vector<SinkRecord>& records) {
    if (!running_) return;

    std::lock_guard<std::mutex> lock(batchMutex_);

    for (const auto& record : records) {
        auto fields = extractFields(record);
        if (!fields.empty()) {
            pendingBatch_.push_back(std::move(fields));
        }

        // Execute batch if full
        if (static_cast<int>(pendingBatch_.size()) >= batchSize_) {
            executeBatch();
        }
    }
}

void JdbcSinkTask::flush(const std::map<std::string, int64_t>& currentOffsets) {
    (void)currentOffsets;

    std::lock_guard<std::mutex> lock(batchMutex_);
    if (!pendingBatch_.empty()) {
        executeBatch();
    }

    spdlog::debug("JdbcSinkTask flushed");
}

void JdbcSinkTask::open(const std::vector<std::pair<std::string, int32_t>>& partitions) {
    spdlog::info("JdbcSinkTask opened {} partitions", partitions.size());
}

void JdbcSinkTask::close(const std::vector<std::pair<std::string, int32_t>>& partitions) {
    // Flush before closing
    std::lock_guard<std::mutex> lock(batchMutex_);
    if (!pendingBatch_.empty()) {
        executeBatch();
    }
    spdlog::info("JdbcSinkTask closed {} partitions", partitions.size());
}

bool JdbcSinkTask::connect() {
    // Stub implementation - would use actual database driver here
    // Example with PostgreSQL: PQconnectdb(connectionUrl_.c_str())
    // Example with MySQL: mysql_real_connect(...)

    spdlog::info("JdbcSinkTask connecting to: {}", connectionUrl_);

    // Simulate connection
    connected_ = true;

    // Auto-create table if configured
    if (autoCreateTable_) {
        // In real implementation, execute CREATE TABLE IF NOT EXISTS
        spdlog::info("JdbcSinkTask would auto-create table: {}", tableName_);
    }

    return true;
}

void JdbcSinkTask::disconnect() {
    if (connected_) {
        // Stub implementation - would close database connection
        spdlog::info("JdbcSinkTask disconnected");
        connected_ = false;
    }
}

bool JdbcSinkTask::isConnected() const {
    return connected_;
}

std::string JdbcSinkTask::generateInsertSql() const {
    std::ostringstream sql;
    sql << "INSERT INTO " << tableName_ << " (";

    bool first = true;
    for (const auto& field : fields_) {
        if (!first) sql << ", ";
        first = false;
        sql << field;
    }

    sql << ") VALUES (";

    first = true;
    for (size_t i = 0; i < fields_.size(); ++i) {
        if (!first) sql << ", ";
        first = false;
        sql << "$" << (i + 1);  // PostgreSQL-style parameters
    }

    sql << ")";

    return sql.str();
}

std::string JdbcSinkTask::generateUpsertSql() const {
    std::ostringstream sql;
    sql << generateInsertSql();

    // PostgreSQL ON CONFLICT syntax
    sql << " ON CONFLICT (";

    bool first = true;
    for (const auto& pk : pkFields_) {
        if (!first) sql << ", ";
        first = false;
        sql << pk;
    }

    sql << ") DO UPDATE SET ";

    first = true;
    for (const auto& field : fields_) {
        // Don't update primary key fields
        bool isPk = false;
        for (const auto& pk : pkFields_) {
            if (field == pk) {
                isPk = true;
                break;
            }
        }
        if (isPk) continue;

        if (!first) sql << ", ";
        first = false;
        sql << field << " = EXCLUDED." << field;
    }

    return sql.str();
}

std::string JdbcSinkTask::generateUpdateSql() const {
    std::ostringstream sql;
    sql << "UPDATE " << tableName_ << " SET ";

    bool first = true;
    int paramIdx = 1;
    for (const auto& field : fields_) {
        // Don't update primary key fields
        bool isPk = false;
        for (const auto& pk : pkFields_) {
            if (field == pk) {
                isPk = true;
                break;
            }
        }
        if (isPk) continue;

        if (!first) sql << ", ";
        first = false;
        sql << field << " = $" << paramIdx++;
    }

    sql << " WHERE ";

    first = true;
    for (const auto& pk : pkFields_) {
        if (!first) sql << " AND ";
        first = false;
        sql << pk << " = $" << paramIdx++;
    }

    return sql.str();
}

std::map<std::string, std::string> JdbcSinkTask::extractFields(const SinkRecord& record) const {
    std::map<std::string, std::string> result;

    try {
        auto j = nlohmann::json::parse(record.value);

        if (fields_.empty()) {
            // Extract all fields from JSON
            for (auto& [key, value] : j.items()) {
                if (value.is_string()) {
                    result[key] = value.get<std::string>();
                } else if (value.is_null()) {
                    result[key] = "";
                } else {
                    result[key] = value.dump();
                }
            }
        } else {
            // Extract only specified fields
            for (const auto& field : fields_) {
                if (field == "_key") {
                    result[field] = record.key.value_or("");
                } else if (field == "_timestamp") {
                    result[field] = std::to_string(record.timestamp);
                } else if (field == "_offset") {
                    result[field] = std::to_string(record.offset);
                } else if (field == "_partition") {
                    result[field] = std::to_string(record.partition);
                } else if (j.contains(field)) {
                    if (j[field].is_string()) {
                        result[field] = j[field].get<std::string>();
                    } else if (j[field].is_null()) {
                        result[field] = "";
                    } else {
                        result[field] = j[field].dump();
                    }
                }
            }
        }

        // Add key as field if configured
        if (std::find(fields_.begin(), fields_.end(), "_key") != fields_.end() ||
            fields_.empty()) {
            if (record.key.has_value()) {
                result["_key"] = record.key.value();
            }
        }

    } catch (const std::exception& e) {
        spdlog::warn("JdbcSinkTask failed to parse record value: {}", e.what());
        // For non-JSON values, use value as single field
        if (!fields_.empty()) {
            result[fields_[0]] = record.value;
        }
    }

    return result;
}

bool JdbcSinkTask::executeBatch() {
    if (pendingBatch_.empty()) return true;

    int attempts = 0;
    int64_t backoffMs = retryBackoffMs_;

    while (attempts < maxRetries_) {
        try {
            // Generate SQL based on insert mode
            std::string sql;
            switch (insertMode_) {
                case JdbcInsertMode::UPSERT:
                    sql = generateUpsertSql();
                    break;
                case JdbcInsertMode::UPDATE:
                    sql = generateUpdateSql();
                    break;
                case JdbcInsertMode::INSERT:
                default:
                    sql = generateInsertSql();
                    break;
            }

            // Stub: In real implementation, execute prepared statement for each row
            // or use batch insert if supported by driver
            spdlog::debug("JdbcSinkTask executing: {} ({} rows)", sql, pendingBatch_.size());

            // Simulate successful execution
            recordsWritten_ += pendingBatch_.size();
            batchesWritten_++;
            pendingBatch_.clear();

            return true;

        } catch (const std::exception& e) {
            attempts++;
            if (attempts < maxRetries_) {
                spdlog::warn("JdbcSinkTask batch failed, retrying in {}ms: {}",
                    backoffMs, e.what());
                std::this_thread::sleep_for(std::chrono::milliseconds(backoffMs));
                backoffMs = std::min(backoffMs * 2, static_cast<int64_t>(30000));

                // Reconnect if needed
                if (!isConnected()) {
                    connect();
                }
            } else {
                spdlog::error("JdbcSinkTask batch failed after {} attempts: {}",
                    attempts, e.what());
                throw;
            }
        }
    }

    return false;
}

// ============================================================================
// JdbcSinkConnector Implementation
// ============================================================================

void JdbcSinkConnector::start(const Properties& config) {
    config_ = config;
    spdlog::info("JdbcSinkConnector started");
}

std::vector<Properties> JdbcSinkConnector::taskConfigs(int maxTasks) {
    std::vector<Properties> configs;

    // For JDBC sink, we typically use fewer tasks than partitions
    // to avoid connection pool exhaustion
    for (int i = 0; i < maxTasks; ++i) {
        Properties taskConfig;
        taskConfig.setAll(config_.getAll());
        taskConfig.set("task.id", std::to_string(i));
        configs.push_back(taskConfig);
    }

    return configs;
}

void JdbcSinkConnector::stop() {
    spdlog::info("JdbcSinkConnector stopped");
}

std::string JdbcSinkConnector::validate(const Properties& config) {
    std::string baseError = SinkConnector::validate(config);
    if (!baseError.empty()) return baseError;

    if (!config.contains("connection.url")) {
        return "Missing required property: connection.url";
    }

    if (!config.contains("table.name")) {
        return "Missing required property: table.name";
    }

    // Validate insert mode
    std::string mode = config.get("insert.mode", "INSERT");
    if (mode != "INSERT" && mode != "UPSERT" && mode != "UPDATE") {
        return "Invalid insert.mode: " + mode +
            " (must be INSERT, UPSERT, or UPDATE)";
    }

    // UPSERT and UPDATE require pk.fields
    if ((mode == "UPSERT" || mode == "UPDATE") && !config.contains("pk.fields")) {
        return "insert.mode=" + mode + " requires pk.fields to be specified";
    }

    return "";
}

// ============================================================================
// Static registration
// ============================================================================

namespace {

struct JdbcSinkRegistrar {
    JdbcSinkRegistrar() {
        ConnectorFactory::registerConnector<JdbcSinkConnector>("JdbcSink");
        TaskFactory::registerSinkTask("JdbcSink", []() {
            return std::make_unique<JdbcSinkTask>();
        });
    }
} jdbcSinkRegistrar;

}  // namespace

}  // namespace connect
}  // namespace kawasan
