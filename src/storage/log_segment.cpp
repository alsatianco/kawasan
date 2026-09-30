#include "kawasan/storage/log_segment.h"
#include "kawasan/common/rocksdb_compat.h"

#include <rocksdb/cache.h>
#include <rocksdb/db.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/options.h>
#include <rocksdb/table.h>

#include <cstring>
#include <memory>
#include <utility>

#include "kawasan/common/error.h"
#include "kawasan/common/logger.h"

namespace kawasan::storage {

// Helper functions to encode/decode offsets as binary keys for proper numerical ordering
static std::string encodeOffsetKey(Offset offset) {
    // Use big-endian 64-bit encoding to ensure numerical ordering in RocksDB
    uint64_t u_offset = static_cast<uint64_t>(offset);
    uint64_t be_offset =
        ((u_offset & 0xFF00000000000000ULL) >> 56) | ((u_offset & 0x00FF000000000000ULL) >> 40) |
        ((u_offset & 0x0000FF0000000000ULL) >> 24) | ((u_offset & 0x000000FF00000000ULL) >> 8) |
        ((u_offset & 0x00000000FF000000ULL) << 8) | ((u_offset & 0x0000000000FF0000ULL) << 24) |
        ((u_offset & 0x000000000000FF00ULL) << 40) | ((u_offset & 0x00000000000000FFULL) << 56);
    return std::string(reinterpret_cast<const char*>(&be_offset), sizeof(be_offset));
}

static Offset decodeOffsetKey(const std::string& key) {
    if (key.size() != sizeof(uint64_t)) {
        // Fallback for old string-based keys (for backward compatibility)
        try {
            return std::stoll(key);
        } catch (...) {
            return 0;
        }
    }
    uint64_t be_offset;
    std::memcpy(&be_offset, key.data(), sizeof(be_offset));
    uint64_t u_offset =
        ((be_offset & 0xFF00000000000000ULL) >> 56) | ((be_offset & 0x00FF000000000000ULL) >> 40) |
        ((be_offset & 0x0000FF0000000000ULL) >> 24) | ((be_offset & 0x000000FF00000000ULL) >> 8) |
        ((be_offset & 0x00000000FF000000ULL) << 8) | ((be_offset & 0x0000000000FF0000ULL) << 24) |
        ((be_offset & 0x000000000000FF00ULL) << 40) | ((be_offset & 0x00000000000000FFULL) << 56);
    return static_cast<Offset>(u_offset);
}

LogSegment::LogSegment(Offset base_offset, const std::string& path)
    : base_offset_(base_offset), path_(path), next_offset_(base_offset) {
    open();
}

LogSegment::LogSegment(LogSegment&& other) noexcept {
    std::lock_guard<std::mutex> lock(other.mutex_);
    base_offset_ = other.base_offset_;
    path_ = std::move(other.path_);
    db_ = std::move(other.db_);
    next_offset_ = other.next_offset_;
    size_bytes_ = other.size_bytes_;
    closed_ = other.closed_;
    active_ = other.active_;
    other.closed_ = true;
}

LogSegment& LogSegment::operator=(LogSegment&& other) noexcept {
    if (this == &other) {
        return *this;
    }

    std::scoped_lock lock(mutex_, other.mutex_);
    base_offset_ = other.base_offset_;
    path_ = std::move(other.path_);
    db_ = std::move(other.db_);
    next_offset_ = other.next_offset_;
    size_bytes_ = other.size_bytes_;
    closed_ = other.closed_;
    active_ = other.active_;
    other.closed_ = true;
    return *this;
}

LogSegment::~LogSegment() {
    close();
}

// Phase 5.1: process-wide RocksDB block cache shared across all segments.
// One block cache per LRU shard amortizes hot-data reads across partitions;
// without this each segment opens its own default 8 MiB cache, which adds
// up to gigabytes for a broker with hundreds of partitions and gives
// terrible hit rates. 256 MiB is a sane starting point for a single-node
// dev broker (Kafka's default is 32 MiB per partition; we share).
static std::shared_ptr<rocksdb::Cache>& sharedBlockCache() {
    static std::shared_ptr<rocksdb::Cache> cache =
        rocksdb::NewLRUCache(/*capacity=*/256 * 1024 * 1024,
                             /*num_shard_bits=*/6);
    return cache;
}

void LogSegment::open() {
    rocksdb::Options options;
    options.create_if_missing = true;
    // Phase 5.1: snappy block-level compression. Cheap on CPU, ~2-3× shrink
    // on typical record-batch payloads. We still pay zero CPU on the read
    // path when records are returned via the raw-bytes fetch path, since
    // RocksDB decompresses transparently before the Get returns.
    options.compression = rocksdb::kSnappyCompression;
    options.write_buffer_size = 64 * 1024 * 1024;  // 64MB
    options.max_write_buffer_number = 3;
    options.target_file_size_base = 64 * 1024 * 1024;  // 64MB

    // Phase 5.1: shared block cache + bloom filter. The bloom filter
    // accelerates point-Get hits/misses for sparse offset lookups (e.g.
    // OffsetForLeaderEpoch); the cache avoids hammering the OS page cache
    // when many segments compete for the same physical memory.
    rocksdb::BlockBasedTableOptions table_opts;
    table_opts.block_cache = sharedBlockCache();
    table_opts.filter_policy.reset(rocksdb::NewBloomFilterPolicy(/*bits_per_key=*/10,
                                                                 /*use_block_based=*/false));
    table_opts.cache_index_and_filter_blocks = true;
    options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_opts));

    std::unique_ptr<rocksdb::DB> db_ptr;
    rocksdb::Status status = openRocksDb(options, path_, db_ptr);
    if (!status.ok()) {
        throw StorageException(ErrorCode::KAFKA_STORAGE_ERROR,
                               "Failed to open log segment: " + status.ToString());
    }

    db_ = std::move(db_ptr);

    // Scan existing entries to compute sizes and next offset
    rocksdb::ReadOptions read_options;
    std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(read_options));
    size_bytes_ = 0;
    next_offset_ = base_offset_;
    Offset last_offset = base_offset_;
    size_t last_batch_records = 0;

    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        std::string key_str = it->key().ToString();
        std::string value = it->value().ToString();
        size_bytes_ += value.size();
        last_offset = decodeOffsetKey(key_str);
        std::vector<uint8_t> bytes(value.begin(), value.end());
        RecordBatch batch = RecordBatch::deserialize(bytes);
        last_batch_records = batch.records().size();
    }

    if (size_bytes_ > 0) {
        next_offset_ = last_offset + static_cast<Offset>(last_batch_records);
    }

    Logger::info("Opened log segment at {} with base offset {}", path_, base_offset_);
}

Offset LogSegment::append(const RecordBatch& batch, bool sync) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (closed_) {
        throw StorageException(ErrorCode::KAFKA_STORAGE_ERROR, "Cannot append to closed segment");
    }

    Offset offset = next_offset_;

    // Serialize the batch
    auto data = batch.serialize();

    // Store in RocksDB with binary-encoded offset as key for proper numerical ordering.
    // sync=true forces an fsync of the WAL before returning so an acknowledged
    // produce survives a power loss / OS crash (default WriteOptions leaves
    // sync=false, which only survives a process crash via the OS page cache).
    rocksdb::WriteOptions write_opts;
    write_opts.sync = sync;
    std::string key = encodeOffsetKey(offset);
    rocksdb::Status status = db_->Put(
        write_opts, key, rocksdb::Slice(reinterpret_cast<const char*>(data.data()), data.size()));

    if (!status.ok()) {
        throw StorageException(ErrorCode::KAFKA_STORAGE_ERROR,
                               "Failed to append record batch: " + status.ToString());
    }

    size_bytes_ += data.size();
    next_offset_ += batch.records().size();

    return offset;
}

std::optional<RecordBatch> LogSegment::read(Offset offset) {
    std::lock_guard<std::mutex> lock(mutex_);

    // Use SeekForPrev to find the batch containing this offset.
    // Exact Get() would miss offsets within multi-record batches.
    std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(rocksdb::ReadOptions()));
    std::string key = encodeOffsetKey(offset);
    it->SeekForPrev(key);

    if (!it->Valid()) {
        return std::nullopt;
    }

    std::string value = it->value().ToString();
    std::vector<uint8_t> data(value.begin(), value.end());
    auto batch = RecordBatch::deserialize(data);

    Offset batch_base = decodeOffsetKey(it->key().ToString());
    Offset batch_end = batch_base + static_cast<Offset>(batch.records().size());

    // Verify the requested offset actually falls within this batch
    if (offset < batch_base || offset >= batch_end) {
        return std::nullopt;
    }

    batch.setBaseOffset(batch_base);
    return batch;
}

std::vector<uint8_t> LogSegment::readRaw(Offset start_offset, size_t max_bytes) {
    std::lock_guard<std::mutex> lock(mutex_);

    std::vector<uint8_t> out;
    if (max_bytes == 0)
        return out;

    std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(rocksdb::ReadOptions()));
    std::string start_key = encodeOffsetKey(start_offset);

    it->SeekForPrev(start_key);
    if (!it->Valid()) {
        it->SeekToFirst();
    }

    while (it->Valid() && out.size() < max_bytes) {
        Offset batch_base = decodeOffsetKey(it->key().ToString());
        rocksdb::Slice value = it->value();

        // Need to deserialize just enough to know how many records this
        // batch contains so we can skip already-served prefixes. Cheap:
        // the header has fixed offsets.
        if (value.size() == 0) {
            it->Next();
            continue;
        }

        // For safety, parse the batch header to detect "this batch ends
        // before start_offset" without a full deserialize. Our record
        // batch format wraps Kafka's V2 batch — the records-count field
        // is at a fixed offset, but parsing it correctly requires care.
        // We fall back to a single deserialize per batch on this path
        // only for the boundary check; downstream gets the raw bytes.
        std::vector<uint8_t> bytes(value.data(), value.data() + value.size());
        auto batch_check = RecordBatch::deserialize(bytes);
        const Offset batch_end = batch_base + static_cast<Offset>(batch_check.records().size());
        if (batch_end <= start_offset) {
            it->Next();
            continue;
        }

        if (out.size() + bytes.size() > max_bytes && !out.empty()) {
            // Honor the cap once we've returned at least one batch.
            break;
        }
        out.insert(out.end(), bytes.begin(), bytes.end());
        it->Next();
    }
    return out;
}

std::vector<RecordBatch> LogSegment::read(Offset start_offset, size_t max_bytes) {
    std::lock_guard<std::mutex> lock(mutex_);

    std::vector<RecordBatch> batches;
    size_t bytes_read = 0;

    std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(rocksdb::ReadOptions()));
    std::string start_key = encodeOffsetKey(start_offset);

    // Use SeekForPrev to find the batch whose base offset is <= start_offset.
    // A batch at base offset 0 with 5 records covers offsets 0-4, so a request
    // for offset 2 must find the batch keyed at offset 0.
    it->SeekForPrev(start_key);

    // If SeekForPrev lands before any key, fall back to the first entry.
    if (!it->Valid()) {
        it->SeekToFirst();
    }

    while (it->Valid() && bytes_read < max_bytes) {
        Offset batch_base = decodeOffsetKey(it->key().ToString());
        std::string value = it->value().ToString();
        std::vector<uint8_t> data(value.begin(), value.end());

        auto batch = RecordBatch::deserialize(data);

        // Skip this batch if it ends entirely before the requested start_offset
        Offset batch_end = batch_base + static_cast<Offset>(batch.records().size());
        if (batch_end <= start_offset) {
            it->Next();
            continue;
        }

        batch.setBaseOffset(batch_base);
        bytes_read += data.size();
        batches.push_back(std::move(batch));

        it->Next();
    }

    return batches;
}

Offset LogSegment::nextOffset() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return next_offset_;
}

size_t LogSegment::size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return size_bytes_;
}

void LogSegment::flush() {
    std::lock_guard<std::mutex> lock(mutex_);

    if (db_) {
        rocksdb::FlushOptions options;
        options.wait = true;
        db_->Flush(options);
    }
}

void LogSegment::close() {
    std::lock_guard<std::mutex> lock(mutex_);

    if (!closed_) {
        if (db_) {
            db_->Close();
            db_.reset();
        }
        closed_ = true;
        Logger::info("Closed log segment at {}", path_);
    }
}

void LogSegment::setActive(bool active) {
    std::lock_guard<std::mutex> lock(mutex_);
    active_ = active;
}

bool LogSegment::isActive() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return active_;
}

bool LogSegment::deleteBatchAt(Offset base) {
    // Phase 3.2: drop the batch at the given offset. Used by Log::cleanup()
    // for cleanup.policy=compact. The deletion is per-batch (the granularity
    // of our RocksDB key scheme); compaction code is expected to only call
    // this for batches that have no records worth keeping.
    std::lock_guard<std::mutex> lock(mutex_);
    if (!db_ || closed_)
        return false;
    if (active_) {
        Logger::warn("Refusing deleteBatchAt({}) on the active segment at {}", base, path_);
        return false;
    }

    std::string key = encodeOffsetKey(base);
    std::string existing;
    auto get_status = db_->Get(rocksdb::ReadOptions(), key, &existing);
    if (!get_status.ok()) {
        return false;
    }
    auto status = db_->Delete(rocksdb::WriteOptions(), key);
    if (!status.ok()) {
        Logger::warn("Failed to delete batch at offset {}: {}", base, status.ToString());
        return false;
    }
    if (size_bytes_ >= existing.size()) {
        size_bytes_ -= existing.size();
    }
    return true;
}

Offset LogSegment::truncateTo(Offset target) {
    // M7: remove the tail of this segment. Unlike deleteBatchAt, this is allowed
    // on the active segment — a follower truncates its live tail to reconcile
    // with a new leader. Batches are atomic, so a batch straddling `target` is
    // removed whole (target should be a batch boundary in practice).
    std::lock_guard<std::mutex> lock(mutex_);
    if (!db_ || closed_) {
        return next_offset_;
    }

    std::vector<std::string> keys;
    size_t freed = 0;

    // A batch that straddles `target` (base < target < base + record_count).
    {
        std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(rocksdb::ReadOptions()));
        it->SeekForPrev(encodeOffsetKey(target));
        if (it->Valid()) {
            const Offset base = decodeOffsetKey(it->key().ToString());
            const std::string value = it->value().ToString();
            if (base < target) {
                auto batch =
                    RecordBatch::deserialize(std::vector<uint8_t>(value.begin(), value.end()));
                if (base + static_cast<Offset>(batch.records().size()) > target) {
                    keys.push_back(it->key().ToString());
                    freed += value.size();
                }
            }
        }
    }
    // Every batch keyed at or after `target`.
    {
        std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(rocksdb::ReadOptions()));
        for (it->Seek(encodeOffsetKey(target)); it->Valid(); it->Next()) {
            keys.push_back(it->key().ToString());
            freed += it->value().size();
        }
    }
    for (const auto& k : keys) {
        db_->Delete(rocksdb::WriteOptions(), k);
    }
    size_bytes_ = (size_bytes_ >= freed) ? (size_bytes_ - freed) : 0;

    // Reset the next-offset to the end of the highest remaining batch (or the
    // segment base if it is now empty).
    std::unique_ptr<rocksdb::Iterator> last(db_->NewIterator(rocksdb::ReadOptions()));
    last->SeekToLast();
    if (last->Valid()) {
        const Offset base = decodeOffsetKey(last->key().ToString());
        const std::string value = last->value().ToString();
        auto batch = RecordBatch::deserialize(std::vector<uint8_t>(value.begin(), value.end()));
        next_offset_ = base + static_cast<Offset>(batch.records().size());
    } else {
        next_offset_ = base_offset_;
    }
    return next_offset_;
}

}  // namespace kawasan::storage
