#include "kawasan/connect/worker.h"
#include "kawasan/connect/offset_storage.h"
#include <spdlog/spdlog.h>
#include <chrono>
#include <thread>

namespace kawasan {
namespace connect {

/**
 * SourceTaskRunner - Manages the lifecycle and execution of source tasks.
 *
 * Responsibilities:
 * - Poll task for records
 * - Produce records to Kawasan
 * - Track offsets and commit periodically
 * - Handle errors and retries
 */
class SourceTaskRunner {
public:
    struct Config {
        int64_t pollIntervalMs = 100;
        int pollBatchSize = 500;
        int64_t offsetCommitIntervalMs = 60000;
        int maxRetries = 3;
        int64_t retryBackoffMs = 1000;
        int64_t maxRetryBackoffMs = 30000;
    };

    SourceTaskRunner(
        SourceTask* task,
        const std::string& connectorName,
        int taskId,
        OffsetStorage* offsetStorage,
        const Config& config)
        : task_(task)
        , connectorName_(connectorName)
        , taskId_(taskId)
        , offsetStorage_(offsetStorage)
        , config_(config) {}

    /**
     * Run the source task loop.
     * Returns when stopped or on fatal error.
     */
    void run(std::atomic<bool>& running) {
        spdlog::info("SourceTaskRunner {} started for connector '{}'",
            taskId_, connectorName_);

        // Load last committed offset
        loadLastOffset();

        int64_t lastCommitTime = currentTimeMs();
        int consecutiveErrors = 0;

        while (running.load()) {
            try {
                // Poll for records
                auto records = task_->poll();

                if (!records.empty()) {
                    // Produce records with retry logic
                    if (produceWithRetry(records)) {
                        // Track offsets for commit
                        trackOffsets(records);

                        // Notify task of produced records
                        for (const auto& record : records) {
                            task_->commitRecord(record);
                        }

                        consecutiveErrors = 0;
                    } else {
                        consecutiveErrors++;
                        handleProduceFailure(consecutiveErrors);
                    }
                }

                // Commit offsets periodically
                int64_t now = currentTimeMs();
                if (now - lastCommitTime >= config_.offsetCommitIntervalMs) {
                    commitOffsets();
                    lastCommitTime = now;
                }

                // Sleep between polls
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(config_.pollIntervalMs));

            } catch (const std::exception& e) {
                spdlog::error("SourceTaskRunner {} error: {}", taskId_, e.what());
                consecutiveErrors++;

                if (!handleError(e, consecutiveErrors)) {
                    break;  // Fatal error, stop the task
                }
            }
        }

        // Final commit before stopping
        commitOffsets();

        spdlog::info("SourceTaskRunner {} stopped for connector '{}'",
            taskId_, connectorName_);
    }

private:
    int64_t currentTimeMs() const {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
    }

    void loadLastOffset() {
        if (!offsetStorage_) return;

        // Create partition key for this task
        std::map<std::string, std::string> partition;
        partition["connector"] = connectorName_;
        partition["task"] = std::to_string(taskId_);

        auto offset = offsetStorage_->load(connectorName_, partition);
        if (!offset.empty()) {
            std::string partKey = partitionToKey(partition);
            lastCommittedOffset_[partKey] = offset;
            spdlog::info("SourceTaskRunner {} loaded offset: {} keys",
                taskId_, offset.size());
        }
    }

    void trackOffsets(const std::vector<SourceRecord>& records) {
        std::lock_guard<std::mutex> lock(offsetMutex_);

        for (const auto& record : records) {
            // Key by source partition
            std::string partKey = partitionToKey(record.sourcePartition);
            pendingOffsets_[partKey] = record.sourceOffset;
        }
    }

    void commitOffsets() {
        if (!offsetStorage_) return;

        std::lock_guard<std::mutex> lock(offsetMutex_);

        if (pendingOffsets_.empty()) return;

        for (const auto& [partKey, offset] : pendingOffsets_) {
            auto partition = keyToPartition(partKey);
            offsetStorage_->store(connectorName_, partition, offset);
        }

        offsetStorage_->flush();
        task_->commit();

        lastCommittedOffset_ = pendingOffsets_;
        pendingOffsets_.clear();

        spdlog::debug("SourceTaskRunner {} committed {} offsets",
            taskId_, lastCommittedOffset_.size());
    }

    bool produceWithRetry(const std::vector<SourceRecord>& records) {
        int attempts = 0;
        int64_t backoffMs = config_.retryBackoffMs;

        while (attempts < config_.maxRetries) {
            if (produceRecords(records)) {
                return true;
            }

            attempts++;
            if (attempts < config_.maxRetries) {
                spdlog::warn("SourceTaskRunner {} produce failed, retrying in {}ms (attempt {})",
                    taskId_, backoffMs, attempts);

                std::this_thread::sleep_for(std::chrono::milliseconds(backoffMs));
                backoffMs = std::min(backoffMs * 2, config_.maxRetryBackoffMs);
            }
        }

        return false;
    }

    bool produceRecords(const std::vector<SourceRecord>& records) {
        // TODO: Implement actual Kawasan producer integration
        // For now, simulate successful production
        for (const auto& record : records) {
            spdlog::debug("SourceTaskRunner {} producing to {}: key={}, value={}",
                taskId_,
                record.topic,
                record.key.value_or("null"),
                record.value.substr(0, 100));
        }
        return true;
    }

    void handleProduceFailure(int consecutiveErrors) {
        if (consecutiveErrors >= config_.maxRetries) {
            spdlog::error("SourceTaskRunner {} too many consecutive produce failures",
                taskId_);
        }
    }

    bool handleError(const std::exception& e, int consecutiveErrors) {
        // Calculate backoff
        int64_t backoffMs = config_.retryBackoffMs * (1 << std::min(consecutiveErrors - 1, 6));
        backoffMs = std::min(backoffMs, config_.maxRetryBackoffMs);

        if (consecutiveErrors >= config_.maxRetries) {
            spdlog::error("SourceTaskRunner {} fatal error after {} retries: {}",
                taskId_, consecutiveErrors, e.what());
            return false;  // Stop the task
        }

        spdlog::warn("SourceTaskRunner {} error, backing off {}ms: {}",
            taskId_, backoffMs, e.what());

        std::this_thread::sleep_for(std::chrono::milliseconds(backoffMs));
        return true;  // Continue running
    }

    std::string partitionToKey(const std::map<std::string, std::string>& partition) const {
        nlohmann::json j = partition;
        return j.dump();
    }

    std::map<std::string, std::string> keyToPartition(const std::string& key) const {
        try {
            return nlohmann::json::parse(key).get<std::map<std::string, std::string>>();
        } catch (...) {
            return {};
        }
    }

    SourceTask* task_;
    std::string connectorName_;
    int taskId_;
    OffsetStorage* offsetStorage_;
    Config config_;

    std::mutex offsetMutex_;
    std::map<std::string, std::map<std::string, std::string>> pendingOffsets_;
    std::map<std::string, std::map<std::string, std::string>> lastCommittedOffset_;
};

}  // namespace connect
}  // namespace kawasan
