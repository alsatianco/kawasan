#include "kawasan/storage/log_manager.h"

#include <chrono>
#include <filesystem>
#include <thread>

#include "kawasan/common/logger.h"

namespace fs = std::filesystem;

namespace kawasan::storage {

LogManager::LogManager(const std::string& base_log_dir, LogConfig default_config)
    : base_log_dir_(base_log_dir), default_config_(default_config) {
    fs::create_directories(base_log_dir);
    Logger::info("Initialized LogManager with base directory: {}", base_log_dir_);
}

LogManager::~LogManager() {
    stop();
    closeAll();
}

namespace {

bool isEnvironmentalOpenError(const std::string& what) {
    static const char* const MARKERS[] = {
        "/LOCK",                  // RocksDB: "While lock file: <dir>/LOCK: ..."
        "lock hold by current process",
        "No locks available",     "Too many open files", "Resource temporarily unavailable",
        "Permission denied",      "No space left",       "Read-only file system",
        "Operation not permitted"};
    for (const char* marker : MARKERS) {
        if (what.find(marker) != std::string::npos) {
            return true;
        }
    }
    return false;
}

}  // namespace

Log* LogManager::getOrCreateLog(const std::string& topic, PartitionId partition) {
    TopicPartition tp{topic, partition};
    
    // Fast path: check if log exists without holding the global lock
    {
        std::shared_lock<std::shared_mutex> read_lock(mutex_);
        auto it = logs_.find(tp);
        if (it != logs_.end()) {
            return it->second.get();
        }
    }
    
    // Slow path: create new log with exclusive lock
    std::unique_lock<std::shared_mutex> write_lock(mutex_);
    
    // Double-check after acquiring write lock (another thread may have created it)
    auto it = logs_.find(tp);
    if (it != logs_.end()) {
        return it->second.get();
    }

    // Create new log. Opening may throw on corrupted on-disk state (handled
    // by quarantining the directory) or on environmental errors (rethrown).
    std::string log_dir = getLogDir(topic, partition);
    // 0A.4: prefer the per-topic config registered via setTopicConfig() so
    // that cleanup.policy and friends from CreateTopics are honored.
    LogConfig effective_config = default_config_;
    auto cfg_it = topic_configs_.find(topic);
    if (cfg_it != topic_configs_.end()) {
        effective_config = cfg_it->second;
    }
    try {
        Log* log_ptr = registerLogLocked(
            tp, std::make_unique<Log>(topic, partition, log_dir, effective_config));
        Logger::info("Created log for topic {} partition {} (cleanup.policy: {}{}{})",
                     topic, partition,
                     effective_config.cleanup_policy_delete ? "delete" : "",
                     (effective_config.cleanup_policy_delete &&
                      effective_config.cleanup_policy_compact) ? "," : "",
                     effective_config.cleanup_policy_compact ? "compact" : "");
        return log_ptr;
    } catch (const std::exception& ex) {
        // Never destroy data on open failure. Environmental errors (RocksDB
        // LOCK held by another process, fd exhaustion, permissions, full disk)
        // must surface: "recovering" them would either wipe a healthy log or
        // pull it out from under the process that holds it.
        if (isEnvironmentalOpenError(ex.what())) {
            Logger::error("Failed to open log for {}-{} at {}: {}", topic, partition, log_dir,
                          ex.what());
            throw;
        }
        // Anything else is treated as on-disk corruption: quarantine the
        // directory (kept for forensics / manual recovery) and start fresh.
        std::string quarantine =
            log_dir + ".corrupt-" +
            std::to_string(std::chrono::duration_cast<std::chrono::milliseconds>(
                               std::chrono::system_clock::now().time_since_epoch())
                               .count());
        Logger::error("Failed to open log for {}-{} at {}: {}. Quarantining it as {} and "
                      "recreating an empty log.",
                      topic, partition, log_dir, ex.what(), quarantine);
        try {
            std::error_code ec;
            std::filesystem::rename(log_dir, quarantine, ec);
            if (ec && std::filesystem::exists(log_dir)) {
                Logger::error("Could not quarantine {}: {}", log_dir, ec.message());
                throw;
            }
            Log* log_ptr = registerLogLocked(
                tp, std::make_unique<Log>(topic, partition, log_dir, effective_config));
            Logger::info("Recreated log for topic {} partition {}", topic, partition);
            return log_ptr;
        } catch (const std::exception& ex2) {
            // Surface the original context with the second failure reason.
            Logger::error(
                "Failed to recover log for {}-{} at {}: {}", topic, partition, log_dir,
                ex2.what());
            throw;
        }
    }
}

Log* LogManager::registerLogLocked(const TopicPartition& tp, std::unique_ptr<Log> log) {
    if (recover_hw_to_log_end_) {
        log->setHighWatermark(log->logEndOffset());
    }
    if (change_listener_) {
        log->setChangeListener(change_listener_);
    }
    Log* log_ptr = log.get();
    logs_[tp] = std::move(log);
    return log_ptr;
}

void LogManager::flushCheckpoints() {
    // Hold the map lock (like cleanupAll) so deleteLog can't free a log mid-flush.
    std::shared_lock<std::shared_mutex> read_lock(mutex_);
    for (auto& [_, log] : logs_) {
        try {
            log->flushCheckpoint();
        } catch (const std::exception& e) {
            Logger::error("Failed to checkpoint {}-{}: {}", log->topic(), log->partition(),
                          e.what());
        }
    }
}

void LogManager::setChangeListener(Log::ChangeListener listener) {
    std::unique_lock<std::shared_mutex> write_lock(mutex_);
    change_listener_ = std::move(listener);
    for (auto& [_, log] : logs_) {
        log->setChangeListener(change_listener_);
    }
}

Log* LogManager::getLog(const std::string& topic, PartitionId partition) {
    std::shared_lock<std::shared_mutex> read_lock(mutex_);

    TopicPartition tp{topic, partition};
    auto it = logs_.find(tp);

    if (it != logs_.end()) {
        return it->second.get();
    }

    return nullptr;
}

void LogManager::deleteLog(const std::string& topic, PartitionId partition) {
    std::unique_lock<std::shared_mutex> write_lock(mutex_);

    TopicPartition tp{topic, partition};
    auto it = logs_.find(tp);

    if (it != logs_.end()) {
        it->second->close();
        std::string log_dir = getLogDir(topic, partition);
        fs::remove_all(log_dir);
        logs_.erase(it);
        Logger::info("Deleted log for topic {} partition {}", topic, partition);
    }
}

std::vector<Log*> LogManager::allLogs() {
    std::shared_lock<std::shared_mutex> read_lock(mutex_);

    std::vector<Log*> result;
    result.reserve(logs_.size());
    for (auto& [tp, log] : logs_) {
        result.push_back(log.get());
    }
    return result;
}

size_t LogManager::openLogCount() const {
    std::shared_lock<std::shared_mutex> read_lock(mutex_);
    return logs_.size();
}

void LogManager::flushAll() {
    std::shared_lock<std::shared_mutex> read_lock(mutex_);

    for (auto& [tp, log] : logs_) {
        log->flush();
    }
}

void LogManager::closeAll() {
    std::unique_lock<std::shared_mutex> write_lock(mutex_);

    for (auto& [tp, log] : logs_) {
        log->close();
    }
    logs_.clear();
}

void LogManager::cleanupAll() {
    cleanup_running_.store(true, std::memory_order_relaxed);
    std::shared_lock<std::shared_mutex> read_lock(mutex_);

    for (auto& [tp, log] : logs_) {
        log->cleanup();
        // Phase EX-1: snapshot dirty ratio per partition. Defined as
        // (logEndOffset - logStartOffset) / logEndOffset — the fraction of
        // the log that has been compaction-eligible since last cleanup.
        // Approximate but matches the operator-intuition gauge Kafka exposes.
        const Offset end = log->logEndOffset();
        const Offset start = log->logStartOffset();
        const double ratio = (end > 0) ? (static_cast<double>(end - start) / end) : 0.0;
        {
            std::lock_guard<std::mutex> lock(dirty_ratio_mutex_);
            dirty_ratios_[tp] = ratio;
        }
    }
    cleanup_running_.store(false, std::memory_order_relaxed);
}

void LogManager::incrementCompactionsTotal() {
    compactions_total_.fetch_add(1, std::memory_order_relaxed);
}

void LogManager::recordDedupeBufferSize(int64_t size) {
    dedupe_buffer_last_size_.store(size, std::memory_order_relaxed);
}

LogManager::CleanerMetrics LogManager::getCleanerMetrics() const {
    CleanerMetrics m{};
    m.running = cleanup_running_.load(std::memory_order_relaxed);
    m.compactions_total = compactions_total_.load(std::memory_order_relaxed);
    m.dedupe_buffer_utilization = dedupe_buffer_last_size_.load(std::memory_order_relaxed);
    std::lock_guard<std::mutex> lock(dirty_ratio_mutex_);
    m.partition_dirty_ratios.reserve(dirty_ratios_.size());
    for (const auto& [tp, ratio] : dirty_ratios_) {
        m.partition_dirty_ratios.push_back({tp.topic, tp.partition, ratio});
    }
    return m;
}

void LogManager::start() {
    {
        std::lock_guard<std::mutex> lock(cleanup_mutex_);
        if (running_) {
            return;
        }
        running_ = true;
        stop_requested_ = false;
    }
    cleanup_thread_ = std::thread(&LogManager::cleanupThread, this);
    checkpoint_thread_ = std::thread(&LogManager::checkpointThread, this);
    Logger::info("Started LogManager");
}

void LogManager::stop() {
    {
        std::lock_guard<std::mutex> lock(cleanup_mutex_);
        if (!running_) {
            return;
        }
        stop_requested_ = true;
    }
    cleanup_cv_.notify_all();
    if (cleanup_thread_.joinable()) {
        cleanup_thread_.join();
    }
    if (checkpoint_thread_.joinable()) {
        checkpoint_thread_.join();
    }
    flushCheckpoints();
    {
        std::lock_guard<std::mutex> lock(cleanup_mutex_);
        running_ = false;
    }
    Logger::info("Stopped LogManager");
}

std::string LogManager::getLogDir(const std::string& topic, PartitionId partition) const {
    return base_log_dir_ + "/" + topic + "-" + std::to_string(partition);
}

void LogManager::setTopicConfig(const std::string& topic, const LogConfig& config) {
    std::unique_lock<std::shared_mutex> write_lock(mutex_);
    topic_configs_[topic] = config;
    Logger::info("Registered LogConfig for topic '{}' (compact={}, delete={}, retention.ms={})",
                 topic, config.cleanup_policy_compact, config.cleanup_policy_delete,
                 config.retention_ms);
}

LogConfig LogManager::getTopicConfig(const std::string& topic) const {
    std::shared_lock<std::shared_mutex> read_lock(mutex_);
    auto it = topic_configs_.find(topic);
    if (it != topic_configs_.end()) {
        return it->second;
    }
    return default_config_;
}

void LogManager::cleanupThread() {
    std::unique_lock<std::mutex> lock(cleanup_mutex_);
    while (!stop_requested_) {
        // Phase 3.2: interval is configurable so tests can observe compaction.
        if (cleanup_cv_.wait_for(lock, std::chrono::milliseconds(cleanup_interval_ms_),
                                 [this]() { return stop_requested_; })) {
            break;
        }
        lock.unlock();
        try {
            cleanupAll();
            flushAll();
            // Phase EX-1: every cleanup pass counts. The doc's
            // `kawasan_log_cleaner_compactions_total` represents
            // "compaction passes" — matches Kafka's metric naming.
            compactions_total_.fetch_add(1, std::memory_order_relaxed);
        } catch (const std::exception& e) {
            Logger::error("Error in cleanup thread: {}", e.what());
        }
        lock.lock();
    }
}

void LogManager::checkpointThread() {
    std::unique_lock<std::mutex> lock(cleanup_mutex_);
    while (!stop_requested_) {
        if (cleanup_cv_.wait_for(lock, std::chrono::milliseconds(checkpoint_interval_ms_),
                                 [this]() { return stop_requested_; })) {
            break;
        }
        lock.unlock();
        flushCheckpoints();
        lock.lock();
    }
}

}  // namespace kawasan::storage
