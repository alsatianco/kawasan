#pragma once

#include <array>
#include <string>
#include <vector>

#include "kawasan/common/buffer.h"
#include "kawasan/common/types.h"

namespace kawasan::protocol {

// Phase 1.16: v6 changes from a flat `topics: STRING[]` to
// `topics: {name?: NULLABLE_STRING, topic_id: UUID}[]`. The client may
// supply either by name (older form), by topic_id (new form), or both.
// We keep both representations available on the request so the handler
// can resolve either.
struct DeleteTopicsTopic {
    std::string name;
    std::array<uint8_t, 16> topic_id{};
    bool has_topic_id = false;
};

class DeleteTopicsRequest {
public:
    void setTimeoutMs(int32_t timeout) { timeout_ms_ = timeout; }
    int32_t timeoutMs() const { return timeout_ms_; }
    void addTopic(const std::string& topic) {
        DeleteTopicsTopic t;
        t.name = topic;
        topics_.push_back(std::move(t));
    }
    void addTopicWithId(const std::string& name,
                        const std::array<uint8_t, 16>& id) {
        DeleteTopicsTopic t;
        t.name = name;
        t.topic_id = id;
        t.has_topic_id = true;
        topics_.push_back(std::move(t));
    }
    const std::vector<DeleteTopicsTopic>& topics() const { return topics_; }
    // Legacy callers that only need names — synthesizes a vector of name
    // strings for backward compatibility with the existing handler code.
    std::vector<std::string> topicNames() const {
        std::vector<std::string> names;
        names.reserve(topics_.size());
        for (const auto& t : topics_) names.push_back(t.name);
        return names;
    }

    void encode(Buffer& buffer, int16_t api_version) const;
    void decode(Buffer& buffer, int16_t api_version);

private:
    std::vector<DeleteTopicsTopic> topics_;
    int32_t timeout_ms_ = 60000;
};

struct DeletableTopicResult {
    std::string name;
    // Phase 1.16: topic_id echoed in the response at v6+.
    std::array<uint8_t, 16> topic_id{};
    ErrorCode error_code = ErrorCode::NONE;
    std::string error_message;
};

class DeleteTopicsResponse {
public:
    void setThrottleTimeMs(int32_t throttle) { throttle_time_ms_ = throttle; }
    void addResult(const DeletableTopicResult& result) { results_.push_back(result); }
    const std::vector<DeletableTopicResult>& results() const { return results_; }

    void encode(Buffer& buffer, int16_t api_version) const;
    void decode(Buffer& buffer, int16_t api_version);

private:
    int32_t throttle_time_ms_ = 0;
    std::vector<DeletableTopicResult> results_;
};

}  // namespace kawasan::protocol

