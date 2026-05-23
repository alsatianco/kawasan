#include "kawasan/connect/worker.h"
#include "kawasan/connect/offset_storage.h"
#include <spdlog/spdlog.h>
#include <chrono>
#include <thread>

namespace kawasan {
namespace connect {

/**
 * SinkTaskRunner - Manages the lifecycle and execution of sink tasks.
 *
 * Responsibilities:
 * - Consume records from Kawasan
 * - Batch records and call put()
 * - Call flush() before committing offsets
 * - Handle rebalancing (pause/resume)
 */
class SinkTaskRunner {
public:
    struct Config {
        int64_t fetchMaxWaitMs = 500;
        int fetchMinBytes = 1;
        int fetchMaxBytes = 1048576;  // 1MB
        int batchSize = 100;
        int64_t flushIntervalMs = 10000;  // 10 seconds
        int64_t offsetCommitIntervalMs = 60000;  // 60 seconds
        int maxRetries = 3;
        int64_t retryBackoffMs = 1000;
        int64_t maxRetryBackoffMs = 30000;
    };

    SinkTaskRunner(
        SinkTask* task,
        const std::string& connectorName,
        int taskId,
        const std::string& topic,
        OffsetStorage* offsetStorage,
        const Config& config)
        : task_(task)
        , connectorName_(connectorName)
        , taskId_(taskId)
        , topic_(topic)
        , offsetStorage_(offsetStorage)
        , config_(config) {}

    /**
     * Run the sink task loop.
     * Returns when stopped or on fatal error.
     */
    void run(std::atomic<bool>& running) {
        spdlog::info("SinkTaskRunner {} started for connector '{}', topic={}",
            taskId_, connectorName_, topic_);

        // Load last committed offset
        loadLastOffset();

        // Notify task of assigned partitions
        std::vector<std::pair<std::string, int32_t>> partitions;
        partitions.emplace_back(topic_, taskId_);
        task_->open(partitions);

        int64_t lastFlushTime = currentTimeMs();
        int64_t lastCommitTime = currentTimeMs();
        int consecutiveErrors = 0;
        std::vector<SinkRecord> batch;

        while (running.load()) {
            try {
                // Fetch records from Kawasan
                auto records = fetchRecords();

                if (!records.empty()) {
                    // Add to batch
                    for (auto& record : records) {
                        batch.push_back(std::move(record));

                        if (static_cast<int>(batch.size()) >= config_.batchSize) {
                            // Batch is full, call put()
                            if (putWithRetry(batch)) {
                                consecutiveErrors = 0;
                            } else {
                                consecutiveErrors++;
                            }
                            batch.clear();
                        }
                    }
                }

                int64_t now = currentTimeMs();

                // Flush on interval or if batch is not empty
                if (now - lastFlushTime >= config_.flushIntervalMs) {
                    if (!batch.empty()) {
                        if (putWithRetry(batch)) {
                            consecutiveErrors = 0;
                        }
                        batch.clear();
                    }

                    flushTask();
                    lastFlushTime = now;
                }

                // Commit offsets periodically
                if (now - lastCommitTime >= config_.offsetCommitIntervalMs) {
                    commitOffsets();
                    lastCommitTime = now;
                }

                // Sleep between fetches if no records
                if (records.empty()) {
                    std::this_thread::sleep_for(
                        std::chrono::milliseconds(config_.fetchMaxWaitMs));
                }

            } catch (const std::exception& e) {
                spdlog::error("SinkTaskRunner {} error: {}", taskId_, e.what());
                consecutiveErrors++;

                if (!handleError(e, consecutiveErrors)) {
                    break;  // Fatal error, stop the task
                }
            }
        }

        // Final flush and commit before stopping
        if (!batch.empty()) {
            putWithRetry(batch);
        }
        flushTask();
        commitOffsets();

        // Notify task of revoked partitions
        task_->close(partitions);

        spdlog::info("SinkTaskRunner {} stopped for connector '{}'",
            taskId_, connectorName_);
    }

private:
    int64_t currentTimeMs() const {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
    }

    void loadLastOffset() {
        if (!offsetStorage_) return;

        std::map<std::string, std::string> partition;
        partition["topic"] = topic_;
        partition["partition"] = std::to_string(taskId_);

        auto offset = offsetStorage_->load(connectorName_, partition);
        if (!offset.empty()) {
            auto it = offset.find("offset");
            if (it != offset.end()) {
                lastCommittedOffset_ = std::stoll(it->second);
                currentOffset_ = lastCommittedOffset_;
                spdlog::info("SinkTaskRunner {} loaded offset: {}",
                    taskId_, lastCommittedOffset_);
            }
        }
    }

    std::vector<SinkRecord> fetchRecords() {
        // TODO: Implement actual Kawasan consumer integration
        // For now, return empty - records would come from consumer
        return {};
    }

    bool putWithRetry(const std::vector<SinkRecord>& records) {
        int attempts = 0;
        int64_t backoffMs = config_.retryBackoffMs;

        while (attempts < config_.maxRetries) {
            try {
                task_->put(records);

                // Track the highest offset in this batch
                for (const auto& record : records) {
                    if (record.offset > currentOffset_) {
                        currentOffset_ = record.offset;
                    }
                }

                return true;

            } catch (const std::exception& e) {
                attempts++;
                if (attempts < config_.maxRetries) {
                    spdlog::warn("SinkTaskRunner {} put failed, retrying in {}ms (attempt {}): {}",
                        taskId_, backoffMs, attempts, e.what());

                    std::this_thread::sleep_for(std::chrono::milliseconds(backoffMs));
                    backoffMs = std::min(backoffMs * 2, config_.maxRetryBackoffMs);
                } else {
                    spdlog::error("SinkTaskRunner {} put failed after {} attempts: {}",
                        taskId_, attempts, e.what());
                    return false;
                }
            }
        }

        return false;
    }

    void flushTask() {
        std::map<std::string, int64_t> offsets;
        offsets[topic_ + ":" + std::to_string(taskId_)] = currentOffset_;

        try {
            task_->flush(offsets);
            spdlog::debug("SinkTaskRunner {} flushed at offset {}",
                taskId_, currentOffset_);
        } catch (const std::exception& e) {
            spdlog::error("SinkTaskRunner {} flush error: {}", taskId_, e.what());
        }
    }

    void commitOffsets() {
        if (!offsetStorage_) return;

        std::map<std::string, std::string> partition;
        partition["topic"] = topic_;
        partition["partition"] = std::to_string(taskId_);

        std::map<std::string, std::string> offset;
        offset["offset"] = std::to_string(currentOffset_);

        offsetStorage_->store(connectorName_, partition, offset);
        offsetStorage_->flush();

        lastCommittedOffset_ = currentOffset_;
        spdlog::debug("SinkTaskRunner {} committed offset {}",
            taskId_, currentOffset_);
    }

    bool handleError(const std::exception& e, int consecutiveErrors) {
        int64_t backoffMs = config_.retryBackoffMs * (1 << std::min(consecutiveErrors - 1, 6));
        backoffMs = std::min(backoffMs, config_.maxRetryBackoffMs);

        if (consecutiveErrors >= config_.maxRetries) {
            spdlog::error("SinkTaskRunner {} fatal error after {} retries: {}",
                taskId_, consecutiveErrors, e.what());
            return false;
        }

        spdlog::warn("SinkTaskRunner {} error, backing off {}ms: {}",
            taskId_, backoffMs, e.what());

        std::this_thread::sleep_for(std::chrono::milliseconds(backoffMs));
        return true;
    }

    SinkTask* task_;
    std::string connectorName_;
    int taskId_;
    std::string topic_;
    OffsetStorage* offsetStorage_;
    Config config_;

    int64_t lastCommittedOffset_ = 0;
    int64_t currentOffset_ = 0;
};

}  // namespace connect
}  // namespace kawasan
