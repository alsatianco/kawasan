#include "kawasan/broker/producer_state_snapshot.h"

#include <algorithm>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <vector>

#include "kawasan/common/buffer.h"
#include "kawasan/common/logger.h"

namespace fs = std::filesystem;

namespace kawasan::broker {

namespace {

constexpr int8_t kVersion = 1;
constexpr const char* kPrefix = "producer-";
constexpr const char* kSuffix = ".psnap";

// CRC-32C (Castagnoli, reflected 0x1EDC6F41) — self-contained so the snapshot
// helper has no cross-module dependency.
uint32_t crc32c(const uint8_t* data, size_t n) {
    static uint32_t table[256];
    static bool init = false;
    if (!init) {
        for (uint32_t i = 0; i < 256; ++i) {
            uint32_t c = i;
            for (int k = 0; k < 8; ++k)
                c = (c & 1) ? (0x82F63B78u ^ (c >> 1)) : (c >> 1);
            table[i] = c;
        }
        init = true;
    }
    uint32_t crc = 0xFFFFFFFFu;
    for (size_t i = 0; i < n; ++i)
        crc = table[(crc ^ data[i]) & 0xFF] ^ (crc >> 8);
    return crc ^ 0xFFFFFFFFu;
}

std::optional<Offset> parseOffset(const std::string& filename) {
    const std::string p = kPrefix, s = kSuffix;
    if (filename.size() <= p.size() + s.size())
        return std::nullopt;
    if (filename.compare(0, p.size(), p) != 0)
        return std::nullopt;
    if (filename.compare(filename.size() - s.size(), s.size(), s) != 0)
        return std::nullopt;
    const std::string num = filename.substr(p.size(), filename.size() - p.size() - s.size());
    try {
        return static_cast<Offset>(std::stoll(num));
    } catch (...) {
        return std::nullopt;
    }
}

}  // namespace

std::vector<uint8_t> ProducerStateSnapshot::serialize(
    Offset snapshot_offset, const std::vector<ProducerStateManager::SnapshotEntry>& entries) {
    Buffer body;
    body.writeInt8(kVersion);
    body.writeInt64(snapshot_offset);
    body.writeInt32(static_cast<int32_t>(entries.size()));
    for (const auto& e : entries) {
        body.writeInt64(e.producer_id);
        body.writeInt16(e.last_epoch);
        body.writeInt32(e.last_sequence);
        body.writeInt32(e.last_base_sequence);
        body.writeInt32(e.last_record_count);
        body.writeInt64(e.last_base_offset);
    }
    // Frame: CRC-32C(body) prefix, then body.
    const uint32_t crc = crc32c(body.data(), body.size());
    Buffer out;
    out.writeInt32(static_cast<int32_t>(crc));
    out.writeBytes(body.data(), body.size());
    return std::vector<uint8_t>(out.data(), out.data() + out.size());
}

std::optional<ProducerStateSnapshot::Loaded> ProducerStateSnapshot::deserialize(
    const std::vector<uint8_t>& bytes) {
    if (bytes.size() < 4)
        return std::nullopt;
    Buffer buf(bytes);
    const uint32_t stored_crc = static_cast<uint32_t>(buf.readInt32());
    const uint32_t computed = crc32c(bytes.data() + 4, bytes.size() - 4);
    if (stored_crc != computed)
        return std::nullopt;
    try {
        if (buf.readInt8() != kVersion)
            return std::nullopt;
        Loaded loaded;
        loaded.snapshot_offset = buf.readInt64();
        const int32_t count = buf.readInt32();
        if (count < 0)
            return std::nullopt;
        loaded.entries.reserve(static_cast<size_t>(count));
        for (int32_t i = 0; i < count; ++i) {
            ProducerStateManager::SnapshotEntry e;
            e.producer_id = buf.readInt64();
            e.last_epoch = buf.readInt16();
            e.last_sequence = buf.readInt32();
            e.last_base_sequence = buf.readInt32();
            e.last_record_count = buf.readInt32();
            e.last_base_offset = buf.readInt64();
            loaded.entries.push_back(e);
        }
        return loaded;
    } catch (const std::exception&) {
        return std::nullopt;  // truncated / malformed
    }
}

void ProducerStateSnapshot::write(const std::string& partition_dir, Offset snapshot_offset,
                                  const std::vector<ProducerStateManager::SnapshotEntry>& entries,
                                  int keep) {
    std::error_code ec;
    if (!fs::exists(partition_dir, ec))
        return;  // partition log dir must exist

    const auto bytes = serialize(snapshot_offset, entries);
    const std::string final_path =
        partition_dir + "/" + kPrefix + std::to_string(snapshot_offset) + kSuffix;
    const std::string tmp_path = final_path + ".tmp";
    {
        std::ofstream f(tmp_path, std::ios::binary | std::ios::trunc);
        if (!f) {
            Logger::warn("producer-snapshot: cannot open {}", tmp_path);
            return;
        }
        f.write(reinterpret_cast<const char*>(bytes.data()),
                static_cast<std::streamsize>(bytes.size()));
        f.flush();
    }
    fs::rename(tmp_path, final_path, ec);
    if (ec) {
        Logger::warn("producer-snapshot: rename {} -> {} failed: {}", tmp_path, final_path,
                     ec.message());
        fs::remove(tmp_path, ec);
        return;
    }

    // Prune: keep the `keep` newest by offset.
    std::vector<std::pair<Offset, std::string>> snaps;
    for (const auto& entry : fs::directory_iterator(partition_dir, ec)) {
        if (!entry.is_regular_file())
            continue;
        auto off = parseOffset(entry.path().filename().string());
        if (off)
            snaps.emplace_back(*off, entry.path().string());
    }
    if (static_cast<int>(snaps.size()) > keep) {
        std::sort(snaps.begin(), snaps.end(),
                  [](const auto& a, const auto& b) { return a.first > b.first; });
        for (size_t i = static_cast<size_t>(keep); i < snaps.size(); ++i) {
            fs::remove(snaps[i].second, ec);
        }
    }
}

std::optional<ProducerStateSnapshot::Loaded> ProducerStateSnapshot::loadNewest(
    const std::string& partition_dir) {
    std::error_code ec;
    if (!fs::exists(partition_dir, ec))
        return std::nullopt;
    std::vector<std::pair<Offset, std::string>> snaps;
    for (const auto& entry : fs::directory_iterator(partition_dir, ec)) {
        if (!entry.is_regular_file())
            continue;
        auto off = parseOffset(entry.path().filename().string());
        if (off)
            snaps.emplace_back(*off, entry.path().string());
    }
    // Newest offset first; skip corrupt ones.
    std::sort(snaps.begin(), snaps.end(),
              [](const auto& a, const auto& b) { return a.first > b.first; });
    for (const auto& [off, path] : snaps) {
        std::ifstream f(path, std::ios::binary);
        if (!f)
            continue;
        std::vector<uint8_t> bytes((std::istreambuf_iterator<char>(f)),
                                   std::istreambuf_iterator<char>());
        auto loaded = deserialize(bytes);
        if (loaded)
            return loaded;
        Logger::warn("producer-snapshot: {} is corrupt, skipping", path);
    }
    return std::nullopt;
}

}  // namespace kawasan::broker
