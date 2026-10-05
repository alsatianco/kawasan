#pragma once

#include <optional>
#include <set>
#include <string>

#include "kawasan/common/types.h"

namespace kawasan::storage {
class LogManager;
class Log;
}  // namespace kawasan::storage

namespace kawasan::broker {

// Immutable routing/storage contract carried by a committed metadata command.
struct CoordinatorFormat {
    int32_t version = 1;
    std::string cluster_id;
    std::string routing = "java-byte-hash-unsigned-mod-v1";
    int32_t offsets_partitions = 16;
    int32_t transaction_partitions = 16;
    bool operator==(const CoordinatorFormat&) const = default;

    void validate() const;
    std::string serialize() const;
    static CoordinatorFormat deserialize(const std::string& bytes);
    int32_t partitionCount(const std::string& topic) const;
};

// Local proof of format admission, separate from disposable coordinator caches.
// No storage is created until a matching committed declaration is supplied.
class CoordinatorFormatStorage {
public:
    CoordinatorFormatStorage(CoordinatorFormat expected, std::string log_dir,
                             std::string metadata_dir);
    void admit(const std::optional<CoordinatorFormat>& committed);
    storage::Log* openReplica(storage::LogManager& logs, const std::string& topic,
                              PartitionId partition);
    static void rejectFormattedLegacyRuntime(const std::string& metadata_dir);

private:
    void rejectLegacySources() const;
    void persist() const;
    CoordinatorFormat expected_;
    std::string log_dir_;
    std::string manifest_path_;
    bool admitted_ = false;
    // Reserved durably BEFORE creation. A crash during initialization therefore
    // fails closed on reopen; missing sources never get bootstrapped again.
    std::set<TopicPartition> initialized_;
};

}  // namespace kawasan::broker
