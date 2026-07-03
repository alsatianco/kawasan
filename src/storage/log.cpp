#include "kawasan/storage/log.h"

#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <limits>
#include <sstream>
#include <tuple>
#include <unordered_set>
#include <utility>

#include "kawasan/common/error.h"
#include "kawasan/common/logger.h"

namespace fs = std::filesystem;

namespace kawasan::storage {

LogConfig LogConfig::fromMap(const std::map<std::string, std::string>& configs,
                             const LogConfig& base) {
    LogConfig result = base;
    auto get = [&](const char* key) -> const std::string* {
        auto it = configs.find(key);
        return it != configs.end() ? &it->second : nullptr;
    };

    if (const auto* policy = get("cleanup.policy")) {
        // Kafka allows "delete", "compact", or "compact,delete" (order-independent).
        result.cleanup_policy_delete = false;
        result.cleanup_policy_compact = false;
        std::stringstream ss(*policy);
        std::string token;
        while (std::getline(ss, token, ',')) {
            // trim whitespace
            auto first = token.find_first_not_of(" \t");
            auto last = token.find_last_not_of(" \t");
            if (first == std::string::npos)
                continue;
            token = token.substr(first, last - first + 1);
            if (token == "delete") {
                result.cleanup_policy_delete = true;
            } else if (token == "compact") {
                result.cleanup_policy_compact = true;
            } else {
                Logger::warn("Unknown cleanup.policy token '{}'; ignoring", token);
            }
        }
        if (!result.cleanup_policy_delete && !result.cleanup_policy_compact) {
            // Empty/all-unknown — restore Kafka default rather than silently disable cleanup.
            Logger::warn("cleanup.policy='{}' yielded no valid tokens; defaulting to 'delete'",
                         *policy);
            result.cleanup_policy_delete = true;
        }
    }

    auto parse_i64 = [&](const char* key, int64_t& out) {
        if (const auto* v = get(key)) {
            try {
                out = std::stoll(*v);
            } catch (const std::exception& ex) {
                Logger::warn("Invalid {} value '{}': {}", key, *v, ex.what());
            }
        }
    };
    auto parse_size = [&](const char* key, size_t& out) {
        if (const auto* v = get(key)) {
            try {
                int64_t parsed = std::stoll(*v);
                if (parsed > 0) {
                    out = static_cast<size_t>(parsed);
                }
            } catch (const std::exception& ex) {
                Logger::warn("Invalid {} value '{}': {}", key, *v, ex.what());
            }
        }
    };

    parse_i64("retention.ms", result.retention_ms);
    parse_i64("retention.bytes", result.retention_bytes);
    parse_size("segment.bytes", result.segment_size);
    parse_i64("segment.ms", result.segment_ms);

    return result;
}

LogConfig LogConfig::fromMap(const std::map<std::string, std::string>& configs) {
    return fromMap(configs, LogConfig{});
}

Log::Log(const std::string& topic, PartitionId partition, const std::string& log_dir,
         const LogConfig& config)
    : topic_(topic), partition_(partition), log_dir_(log_dir), config_(config) {
    // Create log directory if it doesn't exist
    fs::create_directories(log_dir);

    loadSegments();

    {
        std::lock_guard<std::mutex> lock(mutex_);
        // Create initial segment if none exist
        if (segments_.empty()) {
            rollNewSegment();
        }
    }

    loadCheckpoint();

    last_roll_time_ = now();
    last_roll_time_initialized_ = true;

    Logger::info("Opened log for topic {} partition {} at {}", topic_, partition_, log_dir_);
}

Log::~Log() {
    close();
}

Log::Log(Log&& other) noexcept {
    std::lock_guard<std::mutex> lock(other.mutex_);
    topic_ = std::move(other.topic_);
    partition_ = other.partition_;
    log_dir_ = std::move(other.log_dir_);
    config_ = other.config_;
    segments_ = std::move(other.segments_);
    high_watermark_ = other.high_watermark_;
    closed_ = other.closed_;
}

Log& Log::operator=(Log&& other) noexcept {
    if (this == &other) {
        return *this;
    }

    std::scoped_lock lock(mutex_, other.mutex_);
    topic_ = std::move(other.topic_);
    partition_ = other.partition_;
    log_dir_ = std::move(other.log_dir_);
    config_ = other.config_;
    segments_ = std::move(other.segments_);
    high_watermark_ = other.high_watermark_;
    closed_ = other.closed_;
    return *this;
}

Offset Log::append(const std::vector<Record>& records, bool force_sync) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (records.empty()) {
        return endOffsetUnlocked();
    }

    // Create record batch
    RecordBatch batch;
    batch.setBaseOffset(endOffsetUnlocked());
    batch.setFirstTimestamp(records[0].timestamp);

    for (const auto& record : records) {
        batch.addRecord(record);
    }

    // Check if we need to roll a new segment
    auto* segment = activeSegment();
    if (shouldRollForTime() || (segment && segment->size() >= config_.segment_size)) {
        rollNewSegment();
        segment = activeSegment();
    }

    // Append to active segment. force_sync overrides the log's flush mode so a
    // durability-critical caller (e.g. __transaction_state) fsyncs even when
    // the broker's data durability is async.
    const bool sync = force_sync || config_.flush_mode == FlushMode::kSync;
    Offset offset = segment->append(batch, sync);

    // Update high watermark (simplified - in reality this is managed by replication)
    high_watermark_ = segment->nextOffset();
    persistCheckpointLocked();

    return offset;
}

Offset Log::appendBatch(RecordBatch batch) {
    std::lock_guard<std::mutex> lock(mutex_);

    // Assign the next available offset to this pre-built batch.
    batch.setBaseOffset(endOffsetUnlocked());

    auto* segment = activeSegment();
    if (shouldRollForTime() || (segment && segment->size() >= config_.segment_size)) {
        rollNewSegment();
        segment = activeSegment();
    }

    Offset offset = segment->append(batch, config_.flush_mode == FlushMode::kSync);
    high_watermark_ = segment->nextOffset();
    persistCheckpointLocked();
    return offset;
}

std::vector<uint8_t> Log::readRaw(Offset start_offset, size_t max_bytes) {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<uint8_t> out;
    for (const auto& segment : segments_) {
        if (out.size() >= max_bytes)
            break;
        const Offset seg_base = segment->baseOffset();
        const Offset seg_next = segment->nextOffset();
        const Offset effective_start = std::max(start_offset, seg_base);
        if (effective_start >= seg_next)
            continue;

        const size_t remaining = max_bytes - out.size();
        auto bytes = segment->readRaw(effective_start, remaining);
        if (!bytes.empty()) {
            out.insert(out.end(), bytes.begin(), bytes.end());
        }
    }
    return out;
}

std::vector<RecordBatch> Log::read(Offset start_offset, size_t max_bytes) {
    std::lock_guard<std::mutex> lock(mutex_);

    std::vector<RecordBatch> result;
    size_t bytes_read = 0;

    // Find the segment containing start_offset
    for (const auto& segment : segments_) {
        if (segment->baseOffset() <= start_offset && start_offset < segment->nextOffset()) {
            // Read from this segment and subsequent ones
            auto batches = segment->read(start_offset, max_bytes - bytes_read);
            for (auto& batch : batches) {
                bytes_read += batch.size();
                result.push_back(std::move(batch));
            }
        } else if (segment->baseOffset() >= start_offset && bytes_read < max_bytes) {
            // Read from subsequent segments
            auto batches = segment->read(segment->baseOffset(), max_bytes - bytes_read);
            for (auto& batch : batches) {
                bytes_read += batch.size();
                result.push_back(std::move(batch));
            }
        }

        if (bytes_read >= max_bytes) {
            break;
        }
    }

    return result;
}

Offset Log::logStartOffset() const {
    std::lock_guard<std::mutex> lock(mutex_);
    if (segments_.empty()) {
        return 0;
    }
    return segments_.front()->baseOffset();
}

Offset Log::logEndOffset() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return endOffsetUnlocked();
}

void Log::flush() {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto& segment : segments_) {
        segment->flush();
    }
}

void Log::close() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (closed_) {
        return;
    }
    for (auto& segment : segments_) {
        segment->close();
    }
    persistCheckpointLocked();
    segments_.clear();
    closed_ = true;
}

std::optional<std::pair<Offset, int64_t>> Log::maxTimestampOffset() {
    std::lock_guard<std::mutex> lock(mutex_);
    std::optional<std::pair<Offset, int64_t>> best;
    for (const auto& segment : segments_) {
        std::vector<RecordBatch> batches;
        try {
            batches = segment->read(segment->baseOffset(), std::numeric_limits<size_t>::max());
        } catch (const std::exception& ex) {
            Logger::warn("maxTimestampOffset read failed for {}: {}", segment->path(), ex.what());
            continue;  // best-effort scan; a partial answer is OK
        }
        for (const auto& batch : batches) {
            if (batch.isControlBatch()) {
                continue;  // txn markers carry wall-clock timestamps; never the answer
            }
            if (!best || batch.maxTimestamp() > best->second) {
                best = std::make_pair(batch.baseOffset(), batch.maxTimestamp());
            }
        }
    }
    return best;
}

void Log::maybeRoll() {
    std::lock_guard<std::mutex> lock(mutex_);

    auto* segment = activeSegment();
    if (shouldRollForTime() || (segment && segment->size() >= config_.segment_size)) {
        rollNewSegment();
    }
}

Offset Log::truncatePrefix(Offset new_start_offset) {
    std::lock_guard<std::mutex> lock(mutex_);

    // Clamp upper bound to the next-to-be-written offset. Truncating past the
    // log end is a no-op (we never invent a non-existent start offset).
    const Offset end = endOffsetUnlocked();
    if (new_start_offset > end)
        new_start_offset = end;
    if (new_start_offset <= startOffsetUnlocked()) {
        return startOffsetUnlocked();
    }

    // Drop closed segments whose entire offset range is below new_start_offset.
    // The active (last) segment is preserved: Kafka semantics is that the
    // current writeable segment is never truncated by DeleteRecords; its data
    // is allowed to remain even if it falls below the requested watermark.
    bool modified = false;
    while (segments_.size() > 1) {
        auto& first = segments_.front();
        const Offset seg_end = first->baseOffset() + first->size();  // approximation
        const Offset seg_next = first->nextOffset();
        const Offset seg_last_inclusive = (seg_next > 0) ? (seg_next - 1) : first->baseOffset();
        if (seg_last_inclusive < new_start_offset) {
            auto path = first->path();
            Logger::info("DeleteRecords: dropping segment for {}-{} at {} (last={} < new_start={})",
                         topic_, partition_, path, seg_last_inclusive, new_start_offset);
            first->close();
            fs::remove_all(path);
            segments_.erase(segments_.begin());
            modified = true;
        } else {
            break;
        }
        (void)seg_end;
    }

    if (modified) {
        persistCheckpointLocked();
    }
    return startOffsetUnlocked();
}

void Log::cleanup() {
    std::lock_guard<std::mutex> lock(mutex_);

    bool modified = false;
    auto delete_segment = [&](size_t idx) {
        if (idx >= segments_.size()) {
            return;
        }
        auto path = segments_[idx]->path();
        Logger::info("Deleting segment for {}-{} at {}", topic_, partition_, path);
        segments_[idx]->close();
        fs::remove_all(path);
        segments_.erase(segments_.begin() + idx);
        modified = true;
    };

    if (config_.cleanup_policy_delete && segments_.size() > 1) {
        // Time-based retention
        if (config_.retention_ms > 0) {
            const auto retention_time = std::chrono::milliseconds(config_.retention_ms);
            std::vector<size_t> to_delete;
            for (size_t i = 0; i + 1 < segments_.size(); ++i) {
                const auto& segment = segments_[i];
                try {
                    auto segment_path = fs::path(segment->path());
                    auto last_write_time = fs::last_write_time(segment_path);
                    auto file_now = decltype(last_write_time)::clock::now();
                    auto segment_age = std::chrono::duration_cast<std::chrono::milliseconds>(
                        file_now - last_write_time);
                    if (segment_age > retention_time) {
                        to_delete.push_back(i);
                    }
                } catch (const std::exception& ex) {
                    Logger::warn("Failed to evaluate retention for {}: {}", segment->path(),
                                 ex.what());
                }
            }

            for (auto it = to_delete.rbegin(); it != to_delete.rend(); ++it) {
                delete_segment(*it);
            }
        }

        // Size-based retention
        if (config_.retention_bytes >= 0) {
            const size_t retention_limit = static_cast<size_t>(config_.retention_bytes);
            size_t total_size = 0;
            for (const auto& segment : segments_) {
                total_size += segment->size();
            }

            size_t idx = 0;
            while (segments_.size() > 1 && total_size > retention_limit &&
                   idx < segments_.size() - 1) {
                const size_t segment_size = segments_[idx]->size();
                delete_segment(idx);
                total_size = total_size > segment_size ? total_size - segment_size : 0;
            }
        }
    }

    if (config_.cleanup_policy_compact && segments_.size() > 0) {
        // Phase 3.2: production-grade streaming compaction.
        //
        // Two-pass algorithm, bounded by unique-key count (not log size):
        //   1. Build offset map: scan all non-active segments; for each
        //      record key, record the highest offset where it appeared.
        //   2. For each non-active segment, iterate batches; delete any
        //      batch whose every record is superseded by a later one for
        //      the same key, with tombstones preserved within
        //      delete.retention.ms (default 24h).
        //
        // Granularity is per-batch (not per-record): breaking up a batch
        // would require re-encoding CRCs. This matches the common case
        // where each batch is a single produce.
        const int64_t kDeleteRetentionMs = 24 * 60 * 60 * 1000;
        const int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                   std::chrono::system_clock::now().time_since_epoch())
                                   .count();

        // Pass 1: build offset map across all segments (active and non-active
        // both contribute the "latest" reference; we just don't delete from
        // the active one).
        std::unordered_map<std::string, Offset> latest_offset;
        for (size_t idx = 0; idx < segments_.size(); ++idx) {
            auto& segment = segments_[idx];
            std::vector<RecordBatch> batches;
            try {
                batches = segment->read(segment->baseOffset(), std::numeric_limits<size_t>::max());
            } catch (const std::exception& ex) {
                Logger::warn("Compaction pass1 read failed for {}: {}", segment->path(), ex.what());
                continue;
            }
            for (const auto& batch : batches) {
                const Offset base = batch.baseOffset();
                const auto& records = batch.records();
                for (size_t i = 0; i < records.size(); ++i) {
                    if (!records[i].key)
                        continue;
                    std::string key(records[i].key->begin(), records[i].key->end());
                    const Offset rec_offset = base + static_cast<Offset>(i);
                    auto it = latest_offset.find(key);
                    if (it == latest_offset.end() || rec_offset > it->second) {
                        latest_offset[key] = rec_offset;
                    }
                }
            }
        }

        // Pass 2: drop batches in non-active segments whose every record is
        // superseded. The loop bound already excludes the newest segment;
        // the isActive() check makes the invariant hold even if the bound or
        // the segment list shape changes (deleteBatchAt refuses as a final
        // backstop).
        size_t deleted_batches = 0;
        for (size_t idx = 0; idx + 1 < segments_.size(); ++idx) {
            auto& segment = segments_[idx];
            if (segment->isActive()) {
                continue;
            }
            std::vector<RecordBatch> batches;
            try {
                batches = segment->read(segment->baseOffset(), std::numeric_limits<size_t>::max());
            } catch (const std::exception& ex) {
                Logger::warn("Compaction pass2 read failed for {}: {}", segment->path(), ex.what());
                continue;
            }
            for (const auto& batch : batches) {
                const Offset base = batch.baseOffset();
                const auto& records = batch.records();
                bool keep = false;
                for (size_t i = 0; i < records.size(); ++i) {
                    if (!records[i].key) {
                        // Null key: not subject to compaction.
                        keep = true;
                        break;
                    }
                    std::string key(records[i].key->begin(), records[i].key->end());
                    const Offset rec_offset = base + static_cast<Offset>(i);
                    auto it = latest_offset.find(key);
                    if (it != latest_offset.end() && it->second == rec_offset) {
                        keep = true;
                        break;
                    }
                    // Tombstone (null/empty value): preserve within window.
                    if (records[i].value && records[i].value->empty()) {
                        if (now_ms - records[i].timestamp < kDeleteRetentionMs) {
                            keep = true;
                            break;
                        }
                    }
                }
                if (!keep) {
                    if (segment->deleteBatchAt(base)) {
                        ++deleted_batches;
                    }
                }
            }
        }

        if (deleted_batches > 0) {
            Logger::info("Compaction for {}-{}: dropped {} batch(es) across {} segment(s); "
                         "{} unique keys retained",
                         topic_, partition_, deleted_batches,
                         segments_.size() > 0 ? segments_.size() - 1 : 0, latest_offset.size());
            modified = true;
        }
    }

    if (modified) {
        persistCheckpointLocked();
    }
}

void Log::loadSegments() {
    if (!fs::exists(log_dir_)) {
        return;
    }

    // Find all segment directories
    std::vector<std::pair<Offset, std::string>> segment_paths;
    for (const auto& entry : fs::directory_iterator(log_dir_)) {
        if (entry.is_directory()) {
            std::string dirname = entry.path().filename().string();
            try {
                Offset base_offset = std::stoll(dirname);
                segment_paths.emplace_back(base_offset, entry.path().string());
            } catch (...) {
                // Skip non-numeric directories
            }
        }
    }

    // Sort by base offset
    std::sort(segment_paths.begin(), segment_paths.end());

    // Load segments
    for (const auto& [base_offset, path] : segment_paths) {
        segments_.push_back(std::make_unique<LogSegment>(base_offset, path));
    }
    if (!segments_.empty()) {
        segments_.back()->setActive(true);
    }
}

void Log::rollNewSegment() {
    Offset base_offset = endOffsetUnlocked();
    std::string segment_path = log_dir_ + "/" + std::to_string(base_offset);

    if (!segments_.empty()) {
        segments_.back()->setActive(false);
    }
    segments_.push_back(std::make_unique<LogSegment>(base_offset, segment_path));
    segments_.back()->setActive(true);
    last_roll_time_ = now();
    last_roll_time_initialized_ = true;
    persistCheckpointLocked();
    Logger::info("Rolled new segment for topic {} partition {} at offset {}", topic_, partition_,
                 base_offset);
}

LogSegment* Log::activeSegment() {
    if (segments_.empty()) {
        return nullptr;
    }
    return segments_.back().get();
}

const LogSegment* Log::activeSegment() const {
    if (segments_.empty()) {
        return nullptr;
    }
    return segments_.back().get();
}

bool Log::shouldRollForTime() const {
    if (config_.segment_ms <= 0 || !last_roll_time_initialized_) {
        return false;
    }
    const auto elapsed =
        std::chrono::duration_cast<std::chrono::milliseconds>(now() - last_roll_time_);
    return elapsed.count() >= config_.segment_ms;
}

std::chrono::steady_clock::time_point Log::now() const {
    return std::chrono::steady_clock::now();
}

Offset Log::endOffsetUnlocked() const {
    if (segments_.empty()) {
        return 0;
    }
    return segments_.back()->nextOffset();
}

Offset Log::startOffsetUnlocked() const {
    if (segments_.empty()) {
        return 0;
    }
    return segments_.front()->baseOffset();
}

void Log::loadCheckpoint() {
    std::lock_guard<std::mutex> lock(mutex_);
    auto checkpoint = readCheckpointFromDisk();
    const Offset start = startOffsetUnlocked();
    const Offset end = endOffsetUnlocked();

    if (checkpoint) {
        Offset stored_start;
        Offset stored_end;
        Offset stored_hw;
        std::tie(stored_start, stored_end, stored_hw) = *checkpoint;

        if (stored_start != start || stored_end != end) {
            Logger::warn("Checkpoint mismatch for {}-{} (stored start={}, end={}, actual "
                         "start={}, end={}) - updating checkpoint",
                         topic_, partition_, stored_start, stored_end, start, end);
        }

        const Offset clamped_hw = std::clamp(stored_hw, start, end);
        high_watermark_ = clamped_hw;
    } else {
        high_watermark_ = end;
    }

    persistCheckpointLocked();
}

void Log::persistCheckpointLocked() const {
    const Offset start = startOffsetUnlocked();
    const Offset end = endOffsetUnlocked();
    const std::string path = checkpointPath();
    Logger::debug("Persisting checkpoint for {}-{} start={} end={} hw={}", topic_, partition_,
                  start, end, high_watermark_);
    fs::create_directories(log_dir_);

    // Crash-safe write: render the payload, write to a temp file, fsync it, then
    // atomically rename over the real checkpoint. A plain ofstream left the
    // checkpoint exposed to truncation/torn writes on crash, so recovery could
    // read a stale or partial high-watermark. The temp+fsync+rename sequence
    // guarantees the checkpoint is either the old value or the fully-written new
    // one — never a torn intermediate.
    std::ostringstream payload;
    payload << "log_start_offset=" << start << "\n";
    payload << "log_end_offset=" << end << "\n";
    payload << "high_watermark=" << high_watermark_ << "\n";
    const std::string data = payload.str();

    const std::string tmp_path = path + ".tmp";
    const int fd = ::open(tmp_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        Logger::error("Failed to open checkpoint temp for {}-{} at {}", topic_, partition_,
                      tmp_path);
        return;
    }
    const char* buf = data.data();
    size_t remaining = data.size();
    bool write_ok = true;
    while (remaining > 0) {
        const ssize_t n = ::write(fd, buf, remaining);
        if (n < 0) {
            if (errno == EINTR)
                continue;
            Logger::error("Failed to write checkpoint for {}-{}: {}", topic_, partition_,
                          std::strerror(errno));
            write_ok = false;
            break;
        }
        buf += n;
        remaining -= static_cast<size_t>(n);
    }
    if (write_ok && ::fsync(fd) != 0) {
        Logger::error("Failed to fsync checkpoint for {}-{}: {}", topic_, partition_,
                      std::strerror(errno));
        write_ok = false;
    }
    ::close(fd);
    if (!write_ok) {
        ::unlink(tmp_path.c_str());
        return;
    }
    if (::rename(tmp_path.c_str(), path.c_str()) != 0) {
        Logger::error("Failed to rename checkpoint for {}-{}: {}", topic_, partition_,
                      std::strerror(errno));
        ::unlink(tmp_path.c_str());
    }
}

std::optional<std::tuple<Offset, Offset, Offset>> Log::readCheckpointFromDisk() const {
    const std::string path = checkpointPath();
    std::ifstream in(path);
    if (!in.is_open()) {
        return std::nullopt;
    }

    Offset start = 0;
    Offset end = 0;
    Offset high_watermark = 0;
    bool saw_start = false;
    bool saw_end = false;
    bool saw_hw = false;

    std::string line;
    while (std::getline(in, line)) {
        const auto pos = line.find('=');
        if (pos == std::string::npos) {
            continue;
        }
        const auto key = line.substr(0, pos);
        const auto value_str = line.substr(pos + 1);
        try {
            Offset value = std::stoll(value_str);
            if (key == "log_start_offset") {
                start = value;
                saw_start = true;
            } else if (key == "log_end_offset") {
                end = value;
                saw_end = true;
            } else if (key == "high_watermark") {
                high_watermark = value;
                saw_hw = true;
            }
        } catch (const std::exception& ex) {
            Logger::warn("Invalid checkpoint entry '{}' for {}-{}: {}", line, topic_, partition_,
                         ex.what());
        }
    }

    if (!saw_start || !saw_end || !saw_hw) {
        return std::nullopt;
    }

    return std::make_tuple(start, end, high_watermark);
}

std::string Log::checkpointPath() const {
    return log_dir_ + "/checkpoint.meta";
}

void Log::setHighWatermark(Offset offset) {
    std::lock_guard<std::mutex> lock(mutex_);
    const Offset clamped = std::clamp(offset, startOffsetUnlocked(), endOffsetUnlocked());
    high_watermark_ = clamped;
    persistCheckpointLocked();
}

}  // namespace kawasan::storage
