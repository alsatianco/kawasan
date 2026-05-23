#pragma once

#include <optional>
#include <string>
#include <vector>

#include "kawasan/common/buffer.h"
#include "kawasan/common/error.h"

namespace kawasan::protocol {

/// @brief OffsetFetch request
///
/// Phase 1.12: supports v0–v8.
///   v0:   group_id + topics[] (partitions required, null not allowed)
///   v2+:  topics may be null (fetch ALL committed offsets for the group)
///   v6:   flexible (compact strings + tagged fields)
///   v7:   + require_stable BOOL
///   v8:   shape change — groups[] of {group_id, topics?, member_id?, member_epoch}
class OffsetFetchRequest {
public:
    struct Partition {
        int32_t partition = 0;
    };

    struct Topic {
        std::string topic;
        std::vector<Partition> partitions;
    };

    // Phase 1.12: v8 multi-group form.
    struct Group {
        std::string group_id;
        std::string member_id;   // v9+, but kept here for forward-compat
        int32_t member_epoch = -1;  // v9+
        bool fetch_all_topics = false;
        std::vector<Topic> topics;
    };

    OffsetFetchRequest() = default;

    // Single-group accessors (v0–v7). For v8 these reflect the first
    // group in the multi-group form so existing handlers continue to work
    // for the common single-group case.
    const std::string& groupId() const { return group_id_; }
    bool fetchAllTopics() const { return fetch_all_topics_; }
    bool requireStable() const { return require_stable_; }
    const std::vector<Topic>& topics() const { return topics_; }

    // Phase 1.12 v8 multi-group accessors.
    const std::vector<Group>& groups() const { return groups_; }
    bool isMultiGroup() const { return !groups_.empty(); }

    void setGroupId(const std::string& id) { group_id_ = id; }
    void setFetchAllTopics(bool v) { fetch_all_topics_ = v; }
    void setRequireStable(bool v) { require_stable_ = v; }
    void setTopics(const std::vector<Topic>& topics) {
        topics_ = topics;
        fetch_all_topics_ = false;
    }
    void addGroup(Group g) { groups_.push_back(std::move(g)); }

    void encode(Buffer& buffer, int16_t api_version) const;
    void decode(Buffer& buffer, int16_t api_version);

private:
    std::string group_id_;
    bool fetch_all_topics_ = false;   // v2+ semantics: null topics array
    bool require_stable_ = false;     // v7+
    std::vector<Topic> topics_;
    std::vector<Group> groups_;       // v8+
};

/// @brief OffsetFetch response
///
///   v0:    topics[{ topic, partitions[{ partition, offset, metadata, error }] }], error_code (after)
///   v3+:   + throttle_time_ms (first), error_code at top level (after topics)
///   v5+:   + partitions.committed_leader_epoch INT32
///   v6:    flexible
///   v8:    shape change — groups[] of {group_id, topics, error_code}
class OffsetFetchResponse {
public:
    struct Partition {
        int32_t partition = 0;
        int64_t offset = -1;
        int32_t committed_leader_epoch = -1;  // v5+
        std::string metadata;
        ErrorCode error = ErrorCode::NONE;
    };

    struct Topic {
        std::string topic;
        std::vector<Partition> partitions;
    };

    // Phase 1.12: v8 multi-group form.
    struct Group {
        std::string group_id;
        std::vector<Topic> topics;
        ErrorCode error_code = ErrorCode::NONE;
    };

    OffsetFetchResponse() = default;

    void setErrorCode(ErrorCode code) { error_code_ = code; }
    void setThrottleTimeMs(int32_t v) { throttle_time_ms_ = v; }
    void setTopics(const std::vector<Topic>& topics) { topics_ = topics; }
    void addGroup(Group g) { groups_.push_back(std::move(g)); }

    ErrorCode errorCode() const { return error_code_; }
    int32_t throttleTimeMs() const { return throttle_time_ms_; }
    const std::vector<Topic>& topics() const { return topics_; }
    const std::vector<Group>& groups() const { return groups_; }

    void encode(Buffer& buffer, int16_t api_version) const;
    void decode(Buffer& buffer, int16_t api_version);

private:
    ErrorCode error_code_ = ErrorCode::NONE;
    int32_t throttle_time_ms_ = 0;
    std::vector<Topic> topics_;
    std::vector<Group> groups_;       // v8+
};

}  // namespace kawasan::protocol
