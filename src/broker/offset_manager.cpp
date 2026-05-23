#include "kawasan/broker/offset_manager.h"

#include <cstring>

#include <nlohmann/json.hpp>
#include <rocksdb/options.h>
#include <rocksdb/write_batch.h>

#include "kawasan/common/logger.h"

using json = nlohmann::json;

namespace kawasan::broker {

namespace {

// Default retention: 7 days in milliseconds
constexpr int64_t kDefaultRetentionMs = 7 * 24 * 60 * 60 * 1000LL;

// Key prefixes
constexpr const char* kOffsetPrefix = "offset:";
constexpr const char* kGroupPrefix = "group:";

}  // namespace

OffsetManager::OffsetManager(const std::string& db_path)
    : retention_ms_(kDefaultRetentionMs) {
    rocksdb::Options options;
    options.create_if_missing = true;
    options.error_if_exists = false;
    
    // Optimize for point lookups (offset fetches are typically by key)
    options.OptimizeForPointLookup(64);  // 64 MB block cache
    
    // Configure write buffer
    options.max_write_buffer_number = 2;
    options.write_buffer_size = 16 * 1024 * 1024;  // 16 MB
    
    // Disable compression for now (Snappy may not be available on all systems)
    options.compression = rocksdb::kNoCompression;
    
    rocksdb::DB* db_raw = nullptr;
    rocksdb::Status status = rocksdb::DB::Open(options, db_path, &db_raw);
    
    if (!status.ok()) {
        throw std::runtime_error("Failed to open offset RocksDB at " + db_path + 
                                ": " + status.ToString());
    }
    
    db_.reset(db_raw);
    Logger::info("OffsetManager initialized with db_path={}, retention={}ms", 
                 db_path, retention_ms_);
}

OffsetManager::~OffsetManager() {
    if (db_) {
        Logger::info("Closing OffsetManager RocksDB");
        db_.reset();
    }
}

void OffsetManager::commitOffset(
    const std::string& group_id,
    const std::string& topic,
    int32_t partition,
    int64_t offset,
    const std::string& metadata) {
    
    const std::string key = makeOffsetKey(group_id, topic, partition);
    
    // Build metadata with timestamps
    OffsetMetadata offset_meta;
    offset_meta.offset = offset;
    offset_meta.metadata = metadata;
    offset_meta.commit_timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    offset_meta.expiry_timestamp = offset_meta.commit_timestamp + retention_ms_;
    
    const std::string value = serializeOffsetMetadata(offset_meta);
    
    // Write to RocksDB with sync=true for durability
    rocksdb::WriteOptions write_opts;
    write_opts.sync = true;
    write_opts.disableWAL = false;
    
    rocksdb::Status status = db_->Put(write_opts, key, value);
    
    if (!status.ok()) {
        throw std::runtime_error("Failed to commit offset for group=" + group_id + 
                                " topic=" + topic + " partition=" + std::to_string(partition) +
                                ": " + status.ToString());
    }
    
    Logger::debug("Committed offset: group={}, topic={}, partition={}, offset={}",
                  group_id, topic, partition, offset);
}

std::optional<int64_t> OffsetManager::fetchOffset(
    const std::string& group_id,
    const std::string& topic,
    int32_t partition) const {
    
    auto metadata = fetchOffsetWithMetadata(group_id, topic, partition);
    if (metadata.has_value()) {
        return metadata->offset;
    }
    return std::nullopt;
}

std::optional<OffsetManager::OffsetMetadata> OffsetManager::fetchOffsetWithMetadata(
    const std::string& group_id,
    const std::string& topic,
    int32_t partition) const {
    
    const std::string key = makeOffsetKey(group_id, topic, partition);
    
    rocksdb::ReadOptions read_opts;
    read_opts.verify_checksums = true;
    
    std::string value;
    rocksdb::Status status = db_->Get(read_opts, key, &value);
    
    if (status.IsNotFound()) {
        Logger::debug("Offset not found: group={}, topic={}, partition={}",
                     group_id, topic, partition);
        return std::nullopt;
    }
    
    if (!status.ok()) {
        Logger::error("Failed to fetch offset for group={} topic={} partition={}: {}",
                     group_id, topic, partition, status.ToString());
        return std::nullopt;
    }
    
    auto metadata = deserializeOffsetMetadata(value);
    if (metadata.has_value()) {
        Logger::debug("Fetched offset: group={}, topic={}, partition={}, offset={}",
                     group_id, topic, partition, metadata->offset);
    }
    
    return metadata;
}

void OffsetManager::commitOffsetBatch(
    const std::string& group_id,
    const std::vector<OffsetCommitData>& offsets) {
    
    if (offsets.empty()) {
        return;
    }
    
    rocksdb::WriteBatch batch;
    
    const int64_t now = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    
    for (const auto& offset_data : offsets) {
        const std::string key = makeOffsetKey(group_id, offset_data.topic, offset_data.partition);
        
        OffsetMetadata offset_meta;
        offset_meta.offset = offset_data.offset;
        offset_meta.metadata = offset_data.metadata;
        offset_meta.commit_timestamp = now;
        offset_meta.expiry_timestamp = now + retention_ms_;
        
        const std::string value = serializeOffsetMetadata(offset_meta);
        batch.Put(key, value);
    }
    
    rocksdb::WriteOptions write_opts;
    write_opts.sync = true;
    write_opts.disableWAL = false;
    
    rocksdb::Status status = db_->Write(write_opts, &batch);
    
    if (!status.ok()) {
        throw std::runtime_error("Failed to commit offset batch for group=" + group_id +
                                ": " + status.ToString());
    }
    
    Logger::info("Committed offset batch: group={}, count={}", group_id, offsets.size());
}

void OffsetManager::deleteGroup(const std::string& group_id) {
    // Delete all offsets for this group
    const std::string prefix = makeGroupOffsetPrefix(group_id);
    
    rocksdb::ReadOptions read_opts;
    std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(read_opts));
    
    rocksdb::WriteBatch batch;
    size_t count = 0;
    
    for (it->Seek(prefix); it->Valid() && it->key().starts_with(prefix); it->Next()) {
        batch.Delete(it->key());
        ++count;
    }
    
    if (!it->status().ok()) {
        throw std::runtime_error("Failed to iterate offsets for group=" + group_id +
                                ": " + it->status().ToString());
    }
    
    // Also delete group metadata key (for future use)
    const std::string group_meta_key = makeGroupMetadataKey(group_id);
    batch.Delete(group_meta_key);
    
    if (count == 0) {
        Logger::info("No offsets found for group={}", group_id);
        return;
    }
    
    rocksdb::WriteOptions write_opts;
    write_opts.sync = true;
    
    rocksdb::Status status = db_->Write(write_opts, &batch);
    
    if (!status.ok()) {
        throw std::runtime_error("Failed to delete group=" + group_id +
                                ": " + status.ToString());
    }
    
    Logger::info("Deleted group: group={}, offsets_deleted={}", group_id, count);
}

bool OffsetManager::deleteOffset(const std::string& group_id,
                                 const std::string& topic,
                                 int32_t partition) {
    const std::string key = makeOffsetKey(group_id, topic, partition);
    // Probe first so we can report whether anything was deleted.
    std::string existing;
    auto get_status = db_->Get(rocksdb::ReadOptions(), key, &existing);
    if (get_status.IsNotFound()) {
        return false;
    }
    if (!get_status.ok()) {
        throw std::runtime_error("Failed to probe offset for delete: " +
                                 get_status.ToString());
    }

    rocksdb::WriteOptions write_opts;
    write_opts.sync = true;
    auto del_status = db_->Delete(write_opts, key);
    if (!del_status.ok()) {
        throw std::runtime_error("Failed to delete offset for " + group_id +
                                 "/" + topic + "-" + std::to_string(partition) +
                                 ": " + del_status.ToString());
    }
    Logger::info("OffsetDelete: removed offset for {}/{}-{}", group_id, topic, partition);
    return true;
}

std::vector<std::string> OffsetManager::listGroups() const {
    std::vector<std::string> groups;
    std::string last_group;
    
    rocksdb::ReadOptions read_opts;
    std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(read_opts));
    
    for (it->Seek(kOffsetPrefix); it->Valid() && it->key().starts_with(kOffsetPrefix); it->Next()) {
        auto offset_key = parseOffsetKey(it->key().ToString());
        if (offset_key.has_value() && offset_key->group_id != last_group) {
            groups.push_back(offset_key->group_id);
            last_group = offset_key->group_id;
        }
    }
    
    Logger::debug("Listed {} groups", groups.size());
    return groups;
}

std::vector<OffsetManager::OffsetKey> OffsetManager::listOffsetsForGroup(
    const std::string& group_id) const {
    
    std::vector<OffsetKey> offset_keys;
    const std::string prefix = makeGroupOffsetPrefix(group_id);
    
    rocksdb::ReadOptions read_opts;
    std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(read_opts));
    
    for (it->Seek(prefix); it->Valid() && it->key().starts_with(prefix); it->Next()) {
        auto offset_key = parseOffsetKey(it->key().ToString());
        if (offset_key.has_value()) {
            offset_keys.push_back(*offset_key);
        }
    }
    
    Logger::debug("Listed {} offsets for group={}", offset_keys.size(), group_id);
    return offset_keys;
}

std::map<std::pair<std::string, int32_t>, int64_t> OffsetManager::fetchAllOffsets(
    const std::string& group_id) const {
    
    std::map<std::pair<std::string, int32_t>, int64_t> result;
    const std::string prefix = makeGroupOffsetPrefix(group_id);
    
    rocksdb::ReadOptions read_opts;
    std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(read_opts));
    
    for (it->Seek(prefix); it->Valid() && it->key().starts_with(prefix); it->Next()) {
        auto offset_key = parseOffsetKey(it->key().ToString());
        if (offset_key.has_value()) {
            auto metadata = deserializeOffsetMetadata(it->value().ToString());
            if (metadata.has_value()) {
                result[{offset_key->topic, offset_key->partition}] = metadata->offset;
            }
        }
    }
    
    Logger::debug("Fetched {} offsets for group={}", result.size(), group_id);
    return result;
}

size_t OffsetManager::deleteExpiredOffsets(int64_t now_millis) {
    rocksdb::ReadOptions read_opts;
    std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(read_opts));
    
    rocksdb::WriteBatch batch;
    size_t count = 0;
    
    for (it->Seek(kOffsetPrefix); it->Valid() && it->key().starts_with(kOffsetPrefix); it->Next()) {
        auto metadata = deserializeOffsetMetadata(it->value().ToString());
        if (metadata.has_value() && metadata->expiry_timestamp < now_millis) {
            batch.Delete(it->key());
            ++count;
        }
    }
    
    if (!it->status().ok()) {
        Logger::error("Failed to iterate for expired offsets: {}", it->status().ToString());
        return 0;
    }
    
    if (count == 0) {
        return 0;
    }
    
    rocksdb::WriteOptions write_opts;
    write_opts.sync = true;
    
    rocksdb::Status status = db_->Write(write_opts, &batch);
    
    if (!status.ok()) {
        Logger::error("Failed to delete expired offsets: {}", status.ToString());
        return 0;
    }
    
    Logger::info("Deleted {} expired offsets", count);
    return count;
}

//
// Group Metadata Methods
//

void OffsetManager::saveGroupMetadata(const std::string& group_id, const GroupMetadata& metadata) {
    const std::string key = makeGroupMetadataKey(group_id);

    // Phase EX-7 fix: binary fields (member.metadata, member.assignment)
    // contain arbitrary bytes — including the Java consumer's binary
    // assignment protocol payload. nlohmann/json refuses non-UTF-8
    // strings, so previous code crashed with `invalid UTF-8 byte` and
    // the broker silently failed to persist the group state.
    // Base64-encode binary fields so JSON round-trips cleanly.
    auto b64 = [](const std::vector<uint8_t>& data) -> std::string {
        static constexpr char kTable[] =
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        std::string out;
        out.reserve((data.size() + 2) / 3 * 4);
        size_t i = 0;
        for (; i + 2 < data.size(); i += 3) {
            const uint32_t n = (static_cast<uint32_t>(data[i]) << 16) |
                               (static_cast<uint32_t>(data[i + 1]) << 8) |
                               static_cast<uint32_t>(data[i + 2]);
            out.push_back(kTable[(n >> 18) & 0x3F]);
            out.push_back(kTable[(n >> 12) & 0x3F]);
            out.push_back(kTable[(n >> 6) & 0x3F]);
            out.push_back(kTable[n & 0x3F]);
        }
        if (i < data.size()) {
            uint32_t n = static_cast<uint32_t>(data[i]) << 16;
            if (i + 1 < data.size()) n |= static_cast<uint32_t>(data[i + 1]) << 8;
            out.push_back(kTable[(n >> 18) & 0x3F]);
            out.push_back(kTable[(n >> 12) & 0x3F]);
            out.push_back(i + 1 < data.size() ? kTable[(n >> 6) & 0x3F] : '=');
            out.push_back('=');
        }
        return out;
    };

    // Serialize group metadata to JSON
    json j;
    j["state"] = metadata.state;
    j["protocol_type"] = metadata.protocol_type;
    j["protocol"] = metadata.protocol;
    j["generation"] = metadata.generation;
    j["last_update_timestamp"] = metadata.last_update_timestamp;

    // Serialize members array
    json members_array = json::array();
    for (const auto& member : metadata.members) {
        json member_obj;
        member_obj["member_id"] = member.member_id;
        member_obj["client_id"] = member.client_id;
        member_obj["client_host"] = member.client_host;

        // Base64-encode binary fields (Java client's consumer-protocol
        // metadata + binary partition assignment payload).
        if (!member.metadata.empty()) {
            member_obj["metadata_b64"] = b64(member.metadata);
        }
        if (!member.assignment.empty()) {
            member_obj["assignment_b64"] = b64(member.assignment);
        }

        members_array.push_back(member_obj);
    }
    j["members"] = members_array;
    
    const std::string value = j.dump();
    
    // Write to RocksDB asynchronously for better performance
    // WAL provides durability, sync on every write is too expensive
    rocksdb::WriteOptions write_opts;
    write_opts.sync = false;  // Async writes for better throughput
    write_opts.disableWAL = false;  // Keep WAL enabled for durability
    
    rocksdb::Status status = db_->Put(write_opts, key, value);
    
    if (!status.ok()) {
        throw std::runtime_error("Failed to save group metadata for group=" + group_id +
                                ": " + status.ToString());
    }
    
    Logger::debug("Saved group metadata: group={}, state={}, generation={}, members={}",
                  group_id, metadata.state, metadata.generation, metadata.members.size());
}

std::optional<OffsetManager::GroupMetadata> OffsetManager::loadGroupMetadata(
    const std::string& group_id) const {
    
    const std::string key = makeGroupMetadataKey(group_id);
    
    std::string value;
    rocksdb::Status status = db_->Get(rocksdb::ReadOptions(), key, &value);
    
    if (status.IsNotFound()) {
        return std::nullopt;
    }
    
    if (!status.ok()) {
        Logger::error("Failed to load group metadata for group={}: {}", 
                     group_id, status.ToString());
        return std::nullopt;
    }
    
    try {
        json j = json::parse(value);
        
        GroupMetadata metadata;
        metadata.state = j.value("state", "Empty");
        metadata.protocol_type = j.value("protocol_type", "");
        metadata.protocol = j.value("protocol", "");
        metadata.generation = j.value("generation", 0);
        metadata.last_update_timestamp = j.value("last_update_timestamp", 0LL);
        
        // Phase EX-7 fix: base64-decode binary fields. Accept both
        // the new `metadata_b64`/`assignment_b64` keys (Java client
        // assignment payloads are non-UTF-8) and the legacy raw-string
        // keys for backward compatibility with state persisted before
        // this fix.
        auto b64decode = [](const std::string& s) -> std::vector<uint8_t> {
            static int8_t inv[256];
            static bool inited = false;
            if (!inited) {
                std::memset(inv, -1, sizeof(inv));
                const char* tbl = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
                for (int i = 0; i < 64; ++i) inv[static_cast<uint8_t>(tbl[i])] = i;
                inited = true;
            }
            std::vector<uint8_t> out;
            out.reserve(s.size() * 3 / 4);
            uint32_t buf = 0; int bits = 0;
            for (char c : s) {
                if (c == '=') break;
                int v = inv[static_cast<uint8_t>(c)];
                if (v < 0) continue;
                buf = (buf << 6) | v;
                bits += 6;
                if (bits >= 8) {
                    bits -= 8;
                    out.push_back(static_cast<uint8_t>((buf >> bits) & 0xFF));
                }
            }
            return out;
        };

        if (j.contains("members") && j["members"].is_array()) {
            for (const auto& member_obj : j["members"]) {
                MemberMetadata member;
                member.member_id = member_obj.value("member_id", "");
                member.client_id = member_obj.value("client_id", "");
                member.client_host = member_obj.value("client_host", "");

                if (member_obj.contains("metadata_b64")) {
                    member.metadata = b64decode(member_obj["metadata_b64"].get<std::string>());
                } else if (member_obj.contains("metadata")) {
                    // Legacy path: stored as raw string (only worked for
                    // ASCII; broken for Java clients). Read as-is for
                    // backward compatibility with pre-fix persistence.
                    std::string metadata_str = member_obj["metadata"];
                    member.metadata = std::vector<uint8_t>(metadata_str.begin(), metadata_str.end());
                }
                if (member_obj.contains("assignment_b64")) {
                    member.assignment = b64decode(member_obj["assignment_b64"].get<std::string>());
                } else if (member_obj.contains("assignment")) {
                    std::string assignment_str = member_obj["assignment"];
                    member.assignment = std::vector<uint8_t>(assignment_str.begin(), assignment_str.end());
                }

                metadata.members.push_back(std::move(member));
            }
        }
        
        Logger::debug("Loaded group metadata: group={}, state={}, generation={}, members={}",
                     group_id, metadata.state, metadata.generation, metadata.members.size());
        
        return metadata;
    } catch (const std::exception& e) {
        Logger::error("Failed to parse group metadata for group={}: {}", 
                     group_id, e.what());
        return std::nullopt;
    }
}

//
// Private Helper Methods
//

std::string OffsetManager::makeOffsetKey(
    const std::string& group_id,
    const std::string& topic,
    int32_t partition) {
    return std::string(kOffsetPrefix) + group_id + ":" + topic + ":" + std::to_string(partition);
}

std::string OffsetManager::makeGroupOffsetPrefix(const std::string& group_id) {
    return std::string(kOffsetPrefix) + group_id + ":";
}

std::string OffsetManager::makeGroupMetadataKey(const std::string& group_id) {
    return std::string(kGroupPrefix) + group_id;
}

std::optional<OffsetManager::OffsetKey> OffsetManager::parseOffsetKey(const std::string& key) {
    // Expected format: "offset:<group_id>:<topic>:<partition>"
    if (!key.starts_with(kOffsetPrefix)) {
        return std::nullopt;
    }
    
    const std::string suffix = key.substr(std::strlen(kOffsetPrefix));
    
    // Find the first colon (separates group_id from topic)
    size_t first_colon = suffix.find(':');
    if (first_colon == std::string::npos) {
        return std::nullopt;
    }
    
    // Find the last colon (separates topic from partition)
    size_t last_colon = suffix.rfind(':');
    if (last_colon == std::string::npos || last_colon == first_colon) {
        return std::nullopt;
    }
    
    OffsetKey result;
    result.group_id = suffix.substr(0, first_colon);
    result.topic = suffix.substr(first_colon + 1, last_colon - first_colon - 1);
    
    try {
        result.partition = std::stoi(suffix.substr(last_colon + 1));
    } catch (...) {
        return std::nullopt;
    }
    
    return result;
}

// 0A.8: compact binary v1 format. JSON's `j.dump()` allocates ~200 bytes per
// commit and parses ~3x slower than direct byte copies. The new format is:
//   byte  0     : magic byte = 0x01 (binary v1)
//   bytes 1-8   : offset (big-endian int64)
//   bytes 9-16  : commit_timestamp (big-endian int64)
//   bytes 17-24 : expiry_timestamp (big-endian int64)
//   bytes 25-28 : metadata length (big-endian uint32)
//   bytes 29-N  : metadata bytes
// Total: 29 bytes + metadata.size() (vs. ~200 bytes for JSON with no metadata).
// Backward compatibility: if first byte is '{', fall back to the old JSON path.
namespace {
constexpr uint8_t kOffsetBinaryV1Magic = 0x01;

void appendInt64BE(std::string& out, int64_t value) {
    auto u = static_cast<uint64_t>(value);
    for (int i = 7; i >= 0; --i) {
        out.push_back(static_cast<char>((u >> (i * 8)) & 0xFF));
    }
}

void appendUInt32BE(std::string& out, uint32_t value) {
    for (int i = 3; i >= 0; --i) {
        out.push_back(static_cast<char>((value >> (i * 8)) & 0xFF));
    }
}

int64_t readInt64BE(const char* p) {
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i) {
        v = (v << 8) | static_cast<uint8_t>(p[i]);
    }
    return static_cast<int64_t>(v);
}

uint32_t readUInt32BE(const char* p) {
    uint32_t v = 0;
    for (int i = 0; i < 4; ++i) {
        v = (v << 8) | static_cast<uint8_t>(p[i]);
    }
    return v;
}
}  // namespace

std::string OffsetManager::serializeOffsetMetadata(const OffsetMetadata& metadata) {
    std::string out;
    out.reserve(29 + metadata.metadata.size());
    out.push_back(static_cast<char>(kOffsetBinaryV1Magic));
    appendInt64BE(out, metadata.offset);
    appendInt64BE(out, metadata.commit_timestamp);
    appendInt64BE(out, metadata.expiry_timestamp);
    appendUInt32BE(out, static_cast<uint32_t>(metadata.metadata.size()));
    out.append(metadata.metadata);
    return out;
}

std::optional<OffsetManager::OffsetMetadata> OffsetManager::deserializeOffsetMetadata(
    const std::string& data) {
    if (data.empty()) {
        return std::nullopt;
    }

    // Binary v1 path.
    if (static_cast<uint8_t>(data[0]) == kOffsetBinaryV1Magic) {
        if (data.size() < 29) {
            Logger::error("Binary offset record too short: {} bytes", data.size());
            return std::nullopt;
        }
        OffsetMetadata m;
        m.offset = readInt64BE(data.data() + 1);
        m.commit_timestamp = readInt64BE(data.data() + 9);
        m.expiry_timestamp = readInt64BE(data.data() + 17);
        const uint32_t meta_len = readUInt32BE(data.data() + 25);
        if (data.size() < static_cast<size_t>(29) + meta_len) {
            Logger::error("Binary offset record metadata length mismatch");
            return std::nullopt;
        }
        m.metadata.assign(data.data() + 29, meta_len);
        return m;
    }

    // Legacy JSON path — survives upgrade from pre-0A.8 stored values.
    try {
        json j = json::parse(data);
        OffsetMetadata m;
        m.offset = j.at("offset").get<int64_t>();
        m.metadata = j.value("metadata", "");
        m.commit_timestamp = j.value("commit_timestamp", 0LL);
        m.expiry_timestamp = j.value("expiry_timestamp", 0LL);
        return m;
    } catch (const std::exception& e) {
        Logger::error("Failed to deserialize offset metadata: {}", e.what());
        return std::nullopt;
    }
}

}  // namespace kawasan::broker
