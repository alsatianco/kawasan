#pragma once

#include "kawasan/connect/connector.h"
#include <memory>
#include <queue>
#include <mutex>

namespace kawasan {
namespace connect {

/**
 * Insert mode for JDBC sink.
 */
enum class JdbcInsertMode {
    INSERT,     // Regular INSERT (fails on conflict)
    UPSERT,     // INSERT ON CONFLICT UPDATE
    UPDATE      // UPDATE only (requires pk.fields)
};

/**
 * JdbcSinkTask - Writes records to a database table via JDBC-like interface.
 *
 * This is a stub implementation that demonstrates the interface.
 * Real database connectivity would require a database driver (e.g., libpq, mysql client).
 *
 * Features:
 * - Batch inserts for performance
 * - Insert/Upsert/Update modes
 * - Table auto-creation (optional)
 * - Primary key handling
 */
class JdbcSinkTask : public SinkTask {
public:
    ~JdbcSinkTask() override;

    std::string version() const override { return "1.0.0"; }
    void start(const Properties& config) override;
    void stop() override;
    void put(const std::vector<SinkRecord>& records) override;
    void flush(const std::map<std::string, int64_t>& currentOffsets) override;
    void open(const std::vector<std::pair<std::string, int32_t>>& partitions) override;
    void close(const std::vector<std::pair<std::string, int32_t>>& partitions) override;

private:
    // Connection management
    bool connect();
    void disconnect();
    bool isConnected() const;

    // SQL generation
    std::string generateInsertSql() const;
    std::string generateUpsertSql() const;
    std::string generateUpdateSql() const;

    // Extract fields from record value (assumes JSON)
    std::map<std::string, std::string> extractFields(const SinkRecord& record) const;

    // Execute batch insert
    bool executeBatch();

    // Configuration
    std::string connectionUrl_;
    std::string username_;
    std::string password_;
    std::string tableName_;
    JdbcInsertMode insertMode_ = JdbcInsertMode::INSERT;
    std::vector<std::string> pkFields_;    // Primary key fields
    std::vector<std::string> fields_;      // Fields to insert
    int batchSize_ = 500;
    bool autoCreateTable_ = false;
    int maxRetries_ = 3;
    int64_t retryBackoffMs_ = 1000;

    // State
    bool running_ = false;
    bool connected_ = false;
    std::mutex batchMutex_;
    std::vector<std::map<std::string, std::string>> pendingBatch_;

    // Statistics
    int64_t recordsWritten_ = 0;
    int64_t batchesWritten_ = 0;
};

/**
 * JdbcSinkConnector - Connector for writing to databases.
 *
 * Configuration:
 * - connection.url: Database connection URL (required)
 * - connection.user: Database username (optional)
 * - connection.password: Database password (optional)
 * - table.name: Target table name (required)
 * - insert.mode: INSERT, UPSERT, or UPDATE (default: INSERT)
 * - pk.fields: Comma-separated primary key fields for upsert/update
 * - fields: Comma-separated field names to insert (empty = all from JSON)
 * - batch.size: Records per batch (default: 500)
 * - auto.create: Auto-create table if missing (default: false)
 * - max.retries: Max retry attempts (default: 3)
 * - retry.backoff.ms: Initial backoff between retries (default: 1000)
 */
class JdbcSinkConnector : public SinkConnector {
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
