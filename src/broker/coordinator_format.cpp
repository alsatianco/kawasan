#include "kawasan/broker/coordinator_format.h"

#include <fcntl.h>
#include <unistd.h>

#include <filesystem>
#include <fstream>
#include <nlohmann/json.hpp>
#include <stdexcept>

#include "kawasan/common/file_util.h"
#include "kawasan/storage/log_manager.h"

namespace fs = std::filesystem;
namespace kawasan::broker {
namespace {
[[noreturn]] void reject(const std::string& reason) {
    throw std::runtime_error("Coordinator format: " + reason +
                             ". Retain all broker data; ADR 0001 requires a verified "
                             "fresh-cluster cutover for legacy clustered storage.");
}

nlohmann::json readJson(const fs::path& path) {
    std::ifstream input(path);
    if (!input)
        reject("cannot read " + path.string());
    nlohmann::json value;
    input >> value;
    return value;
}

// JSON's get<int32_t>() narrows large integers; format admission must not.
int32_t positiveInt(const nlohmann::json& value) {
    if (!value.is_number_integer() || value < 1 || value > INT32_MAX)
        reject("invalid version or partition count");
    return value.get<int32_t>();
}
}  // namespace

void CoordinatorFormat::validate() const {
    if (version != 1 || cluster_id.empty() || routing != "java-byte-hash-unsigned-mod-v1" ||
        offsets_partitions <= 0 || transaction_partitions <= 0)
        reject("unsupported or incomplete declaration");
}

std::string CoordinatorFormat::serialize() const {
    validate();
    return nlohmann::json{{"version", version},
                          {"cluster_id", cluster_id},
                          {"routing", routing},
                          {"offsets_partitions", offsets_partitions},
                          {"transaction_partitions", transaction_partitions}}
        .dump();
}

CoordinatorFormat CoordinatorFormat::deserialize(const std::string& bytes) {
    const auto json = nlohmann::json::parse(bytes);
    if (!json.is_object() || json.size() != 5)
        reject("incomplete or unsupported declaration");
    CoordinatorFormat format;
    format.version = positiveInt(json.at("version"));
    format.cluster_id = json.at("cluster_id").get<std::string>();
    format.routing = json.at("routing").get<std::string>();
    format.offsets_partitions = positiveInt(json.at("offsets_partitions"));
    format.transaction_partitions = positiveInt(json.at("transaction_partitions"));
    format.validate();
    return format;
}

int32_t CoordinatorFormat::partitionCount(const std::string& topic) const {
    if (topic == "__consumer_offsets")
        return offsets_partitions;
    if (topic == "__transaction_state")
        return transaction_partitions;
    reject("not a coordinator topic: " + topic);
}

CoordinatorFormatStorage::CoordinatorFormatStorage(CoordinatorFormat expected, std::string log_dir,
                                                   std::string metadata_dir)
    : expected_(std::move(expected)),
      log_dir_(std::move(log_dir)),
      manifest_path_(std::move(metadata_dir) + "/coordinator-format.json") {
    expected_.validate();
}

void CoordinatorFormatStorage::rejectLegacySources() const {
    if (!fs::exists(log_dir_))
        return;
    for (const auto& entry : fs::directory_iterator(log_dir_)) {
        const auto name = entry.path().filename().string();
        if (name == "consumer_offsets" || name.starts_with("__consumer_offsets-") ||
            name.starts_with("__transaction_state-"))
            reject("unformatted legacy/partial coordinator storage at " + entry.path().string());
    }
}

void CoordinatorFormatStorage::preflight() {
    const auto metadata = fs::path(manifest_path_).parent_path() / "topics.json";
    std::optional<CoordinatorFormat> declaration;
    if (fs::exists(metadata)) {
        const auto json = readJson(metadata);
        if (json.at("cluster_id").get<std::string>() != expected_.cluster_id)
            reject("cluster ID changed");
        if (json.contains("coordinator_format"))
            declaration = CoordinatorFormat::deserialize(json.at("coordinator_format").dump());
        for (const auto& topic : json.at("topics")) {
            const auto name = topic.at("name").get<std::string>();
            if (name != "__consumer_offsets" && name != "__transaction_state")
                continue;
            if (!declaration ||
                topic.at("partitions").size() !=
                    static_cast<size_t>(expected_.partitionCount(name)) ||
                topic.at("configs").value("cleanup.policy", "delete") != "compact")
                reject("legacy or changed coordinator metadata");
            int32_t expected_partition = 0;
            for (const auto& partition : topic.at("partitions"))
                if (partition.at("partition").get<int64_t>() != expected_partition++)
                    reject("coordinator partition identity changed");
        }
    }
    validateAdmission(declaration, false);
}

void CoordinatorFormatStorage::admit(const std::optional<CoordinatorFormat>& committed) {
    validateAdmission(committed, true);
}

void CoordinatorFormatStorage::validateAdmission(const std::optional<CoordinatorFormat>& committed,
                                                 bool write_manifest) {
    admitted_ = false;
    if (committed) {
        committed->validate();
        if (*committed != expected_)
            reject("committed declaration differs from configured contract");
    }
    if (fs::exists(manifest_path_ + ".tmp"))
        reject("partial local manifest");
    if (fs::exists(manifest_path_)) {
        const auto json = readJson(manifest_path_);
        if (!json.is_object() || json.size() != 2 || !json.at("initialized").is_array())
            reject("partial local manifest");
        const auto local = CoordinatorFormat::deserialize(json.at("format").dump());
        if (!committed || local != *committed)
            reject("local manifest lacks a matching committed declaration");
        std::set<TopicPartition> initialized;
        for (const auto& item : json.at("initialized")) {
            const auto topic = item.at("topic").get<std::string>();
            const auto& partition = item.at("partition");
            if (!partition.is_number_integer() || partition < 0 ||
                partition >= expected_.partitionCount(topic))
                reject("invalid initialized replica");
            if (!initialized.insert({topic, partition.get<int32_t>()}).second)
                reject("duplicate initialized replica");
        }
        initialized_ = std::move(initialized);
        admitted_ = true;
        return;
    }
    rejectLegacySources();
    if (!committed || !write_manifest)
        return;  // Preflight and empty joiners cannot invent a durable declaration.
    persist();
    admitted_ = true;
}

void CoordinatorFormatStorage::persist() const {
    nlohmann::json initialized = nlohmann::json::array();
    for (const auto& tp : initialized_)
        initialized.push_back({{"topic", tp.topic}, {"partition", tp.partition}});
    fs::create_directories(fs::path(manifest_path_).parent_path());
    const auto data = nlohmann::json{
        {"format", nlohmann::json::parse(expected_.serialize())},
        {"initialized",
         initialized}}.dump(2);
    if (!writeFileAtomically(manifest_path_, data))
        reject("cannot persist local manifest");
    // The manifest's directory entry is part of the durability boundary.
    const auto parent = fs::path(manifest_path_).parent_path().string();
    const int fd = ::open(parent.c_str(), O_RDONLY);
    if (fd < 0)
        reject("cannot open manifest directory for fsync");
    const int sync_result = ::fsync(fd);
    const int close_result = ::close(fd);
    if (sync_result != 0 || close_result != 0)
        reject("cannot fsync manifest directory");
}

storage::Log* CoordinatorFormatStorage::openReplica(storage::LogManager& logs,
                                                    const std::string& topic,
                                                    PartitionId partition) {
    if (!admitted_)
        reject("fresh replica requires a committed declaration");
    if (partition < 0 || partition >= expected_.partitionCount(topic))
        reject("invalid replica partition");
    logs.setAuthoritativeTopic(topic);
    const TopicPartition tp{topic, partition};
    if (initialized_.contains(tp))
        return logs.getOrCreateLog(topic, partition);
    if (fs::exists(fs::path(log_dir_) / (topic + "-" + std::to_string(partition))))
        reject("unreserved partial replica");
    initialized_.insert(tp);
    persist();  // A failure/crash after this point must never recreate a source.
    return logs.initializeAuthoritativeLog(topic, partition);
}

void CoordinatorFormatStorage::rejectFormattedLegacyRuntime(const std::string& metadata_dir) {
    const fs::path manifest = fs::path(metadata_dir) / "coordinator-format.json";
    if (fs::exists(manifest) || fs::exists(manifest.string() + ".tmp"))
        reject("this broker's legacy runtime cannot open a formatted coordinator store");
    const auto metadata = fs::path(metadata_dir) / "topics.json";
    if (fs::exists(metadata) && readJson(metadata).contains("coordinator_format"))
        reject("replicated coordinator runtime activation is not yet implemented");
}
}  // namespace kawasan::broker
