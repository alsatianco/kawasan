#include "kawasan/storage/leader_epoch_cache.h"

#include <fstream>
#include <sstream>

#include "kawasan/common/file_util.h"
#include "kawasan/common/logger.h"

namespace kawasan::storage {

namespace {
constexpr int kCheckpointVersion = 0;
}  // namespace

LeaderEpochCache::LeaderEpochCache(std::string checkpoint_path) : path_(std::move(checkpoint_path)) {
    std::ifstream in(path_);
    if (!in.is_open()) {
        return;  // no history yet (new partition, or a pre-M8 log)
    }
    int version = -1;
    size_t count = 0;
    std::vector<EpochEntry> loaded;
    bool ok = static_cast<bool>(in >> version >> count) && version == kCheckpointVersion;
    for (size_t i = 0; ok && i < count; ++i) {
        EpochEntry e{};
        ok = static_cast<bool>(in >> e.epoch >> e.start_offset) &&
             (loaded.empty() ||
              (e.epoch > loaded.back().epoch && e.start_offset >= loaded.back().start_offset));
        if (ok) {
            loaded.push_back(e);
        }
    }
    if (!ok) {
        Logger::warn("Ignoring unreadable leader-epoch checkpoint {}; starting with no epoch "
                     "history",
                     path_);
        return;
    }
    entries_ = std::move(loaded);
}

void LeaderEpochCache::assign(int32_t epoch, Offset start_offset) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (epoch < 0 || (!entries_.empty() && epoch <= entries_.back().epoch)) {
        return;
    }
    while (!entries_.empty() && entries_.back().start_offset >= start_offset) {
        entries_.pop_back();
    }
    entries_.push_back(EpochEntry{epoch, start_offset});
    persistLocked();
}

std::optional<int32_t> LeaderEpochCache::latestEpoch() const {
    std::lock_guard<std::mutex> lock(mutex_);
    if (entries_.empty()) {
        return std::nullopt;
    }
    return entries_.back().epoch;
}

std::pair<int32_t, Offset> LeaderEpochCache::endOffsetFor(int32_t requested,
                                                           Offset log_end_offset) const {
    std::lock_guard<std::mutex> lock(mutex_);
    if (requested < 0 || entries_.empty() || requested > entries_.back().epoch) {
        return {-1, -1};
    }
    if (requested == entries_.back().epoch) {
        return {requested, log_end_offset};
    }
    // First cached epoch strictly greater than requested (exists: requested < latest).
    size_t higher = 0;
    while (entries_[higher].epoch <= requested) {
        ++higher;
    }
    if (higher == 0) {
        return {requested, entries_.front().start_offset};
    }
    return {entries_[higher - 1].epoch, entries_[higher].start_offset};
}

void LeaderEpochCache::truncateFromEnd(Offset end_offset) {
    std::lock_guard<std::mutex> lock(mutex_);
    const size_t before = entries_.size();
    while (!entries_.empty() && entries_.back().start_offset >= end_offset) {
        entries_.pop_back();
    }
    if (entries_.size() != before) {
        persistLocked();
    }
}

void LeaderEpochCache::truncateFromStart(Offset start_offset) {
    std::lock_guard<std::mutex> lock(mutex_);
    size_t below = 0;  // entries starting before start_offset
    while (below < entries_.size() && entries_[below].start_offset < start_offset) {
        ++below;
    }
    if (below == 0) {
        return;
    }
    // Keep the last epoch that started below the new start: it covers it.
    entries_.erase(entries_.begin(), entries_.begin() + static_cast<long>(below - 1));
    entries_.front().start_offset = start_offset;
    persistLocked();
}

std::vector<EpochEntry> LeaderEpochCache::entries() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return entries_;
}

void LeaderEpochCache::persistLocked() const {
    std::ostringstream out;
    out << kCheckpointVersion << "\n" << entries_.size() << "\n";
    for (const auto& e : entries_) {
        out << e.epoch << " " << e.start_offset << "\n";
    }
    if (!writeFileAtomically(path_, out.str())) {
        Logger::error("Failed to persist leader-epoch checkpoint {}", path_);
    }
}

}  // namespace kawasan::storage
