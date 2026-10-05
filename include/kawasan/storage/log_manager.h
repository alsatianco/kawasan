#pragma once

#include <atomic>
#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "kawasan/common/types.h"
#include "kawasan/storage/log.h"

namespace kawasan::storage {

/// @brief Manages all logs (topic-partitions) for a broker
class LogManager {
public:
    explicit LogManager(const std::string& base_log_dir, LogConfig default_config = LogConfig());
    ~LogManager();

    /// @brief Updates both current and future logs under their locks.
    void setTopicConfig(const std::string& topic, const LogConfig& config);

    void configureTopic(const std::string& topic,
                        const std::map<std::string, std::string>& overrides) {
        setTopicConfig(topic, LogConfig::fromMap(overrides, default_config_));
    }
    LogConfig defaultConfig() const { return default_config_; }

    /// @brief 0A.4: returns the effective config for a topic (override or default).
    LogConfig getTopicConfig(const std::string& topic) const;

    /// @brief Phase 3.2: configure the cleanup-loop wake interval in ms.
    /// Default is 300000 (5 minutes — Kafka default). Tests / dev configs
    /// can set this much lower (e.g. 5000) to observe compaction quickly.
    void setCleanupIntervalMs(int64_t ms) { cleanup_interval_ms_ = ms; }

    /// @brief How often dirty high-watermark checkpoints are written (Kafka's
    /// `replica.high.watermark.checkpoint.interval.ms`, default 5000). Set
    /// before start().
    void setCheckpointIntervalMs(int64_t ms) { checkpoint_interval_ms_ = ms; }

    /// @brief When true, every log opened from now on has its high watermark
    /// recovered to its log-end offset. Correct when every partition is a sole
    /// replica (single-node): all appended records were committed, so a lagging
    /// on-disk checkpoint (written periodically) must not hide them.
    void setRecoverHighWatermarkToLogEnd(bool enabled) { recover_hw_to_log_end_ = enabled; }

    /// @brief Writes every dirty high-watermark checkpoint now.
    void flushCheckpoints();

    // Non-copyable/movable
    LogManager(const LogManager&) = delete;
    LogManager& operator=(const LogManager&) = delete;
    LogManager(LogManager&&) = delete;
    LogManager& operator=(LogManager&&) = delete;

    /// @brief Require existing authoritative storage for every partition of a topic.
    /// Set before opening any partition. The policy persists across closeAll(),
    /// cannot be disabled, and applies to every getOrCreateLog call: missing or
    /// corrupt sources propagate errors without quarantine or empty replacement.
    /// Fresh replicas must be initialized explicitly by the format/bootstrap path.
    void setAuthoritativeTopic(const std::string& topic);

    /// @brief Gets or creates a log for the given topic-partition.
    /// Authoritative topics only reopen existing, validated storage.
    /// @param topic Topic name
    /// @param partition Partition ID
    /// @return Pointer to the log
    Log* getOrCreateLog(const std::string& topic, PartitionId partition);

    /// @brief Explicit fresh-source initialization; requires authoritative policy
    /// and an absent directory. The format admission caller durably reserves it first.
    Log* initializeAuthoritativeLog(const std::string& topic, PartitionId partition);

    /// @brief Gets a log for the given topic-partition
    /// @param topic Topic name
    /// @param partition Partition ID
    /// @return Pointer to the log, or nullptr if not found
    Log* getLog(const std::string& topic, PartitionId partition);

    /// @brief Deletes a log for the given topic-partition
    /// @param topic Topic name
    /// @param partition Partition ID
    void deleteLog(const std::string& topic, PartitionId partition);

    /// @brief Returns all logs
    std::vector<Log*> allLogs();

    /// @brief Returns the number of currently-open partition logs. Each open log
    /// is one or more RocksDB instances (file descriptors), so this is a proxy
    /// for the broker's storage FD footprint — used by the FD-budget test and
    /// exposed as a metric.
    size_t openLogCount() const;

    /// @brief Flushes all logs
    void flushAll();

    /// @brief Closes all logs
    void closeAll();

    /// @brief Performs cleanup on all logs
    void cleanupAll();

    /// @brief Starts the log manager background tasks
    void start();

    /// @brief Stops the log manager
    void stop();

    /// @brief Phase EX-1 (§6.3): Prometheus metrics snapshot for LogCleaner.
    struct CleanerMetrics {
        bool running;                       // gauge (0 or 1)
        int64_t compactions_total;          // counter: compaction passes that dropped >0 batches
        int64_t dedupe_buffer_utilization;  // gauge: average OffsetMap size across last pass
        // Per-(topic, partition) dirty ratio. Range [0, 1].
        struct PartitionRatio {
            std::string topic;
            int32_t partition;
            double dirty_ratio;
        };
        std::vector<PartitionRatio> partition_dirty_ratios;
    };
    CleanerMetrics getCleanerMetrics() const;

    /// @brief Phase EX-1: bumped by Log::cleanup() when a compaction pass
    /// dropped >0 batches.
    void incrementCompactionsTotal();
    void recordDedupeBufferSize(int64_t size);

    /// @brief Installs a change listener on every current and future log (see
    /// Log::setChangeListener). Set once, before serving traffic.
    void setChangeListener(Log::ChangeListener listener);

private:
    std::string getLogDir(const std::string& topic, PartitionId partition) const;
    void cleanupThread();
    void checkpointThread();
    Log* registerLogLocked(const TopicPartition& tp, std::unique_ptr<Log> log);

    std::string base_log_dir_;
    LogConfig default_config_;
    std::map<TopicPartition, std::unique_ptr<Log>> logs_;
    // 0A.4: per-topic config overrides (cleanup.policy etc. from CreateTopics).
    std::unordered_map<std::string, LogConfig> topic_configs_;
    std::unordered_set<std::string> authoritative_topics_;
    mutable std::shared_mutex mutex_;  // Changed to shared_mutex for better concurrency
    Log::ChangeListener change_listener_;
    bool running_ = false;
    bool stop_requested_ = false;
    int64_t cleanup_interval_ms_ = 300000;  // 5 minutes default
    std::mutex cleanup_mutex_;
    std::condition_variable cleanup_cv_;
    std::thread cleanup_thread_;
    int64_t checkpoint_interval_ms_ = 5000;
    std::thread checkpoint_thread_;
    bool recover_hw_to_log_end_ = false;

    // Phase EX-1 metrics.
    std::atomic<bool> cleanup_running_{false};
    std::atomic<int64_t> compactions_total_{0};
    std::atomic<int64_t> dedupe_buffer_last_size_{0};
    // Per-(topic, partition) dirty-ratio snapshot updated on each
    // cleanup pass. Mutex-guarded; small (one entry per partition).
    mutable std::mutex dirty_ratio_mutex_;
    std::map<TopicPartition, double> dirty_ratios_;
};

}  // namespace kawasan::storage
