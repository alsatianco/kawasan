#include "kawasan/protocol/delete_topics_request.h"

#include <optional>

namespace kawasan::protocol {

// Phase 1.16: v0–v6 ladder.
//   v0–v3: legacy strings; v3 adds throttle_time_ms (response)
//   v4:    flexible request/response (compact strings + tagged fields)
//   v5:    same shape, just enforces compact (no change for us)
//   v6:    request switches from `topics: STRING[]` to
//          `topics: {name?: COMPACT_NULLABLE_STRING, topic_id: UUID}[]`;
//          response gains `topic_id: UUID` per result.

void DeleteTopicsRequest::encode(Buffer& buffer, int16_t api_version) const {
    const bool flex = api_version >= 4;
    const bool with_topic_id = api_version >= 6;
    if (flex) {
        buffer.writeCompactArrayLen(static_cast<int32_t>(topics_.size()));
    } else {
        buffer.writeInt32(static_cast<int32_t>(topics_.size()));
    }
    for (const auto& topic : topics_) {
        if (with_topic_id) {
            const auto name_opt = topic.name.empty()
                                      ? std::optional<std::string>{}
                                      : std::optional<std::string>(topic.name);
            buffer.writeCompactNullableString(name_opt);
            buffer.writeBytes(std::vector<uint8_t>(topic.topic_id.begin(),
                                                  topic.topic_id.end()));
            buffer.writeEmptyTaggedFields();
        } else if (flex) {
            buffer.writeCompactString(topic.name);
        } else {
            buffer.writeString(topic.name);
        }
    }
    buffer.writeInt32(timeout_ms_);
    if (flex) buffer.writeEmptyTaggedFields();
}

void DeleteTopicsRequest::decode(Buffer& buffer, int16_t api_version) {
    const bool flex = api_version >= 4;
    const bool with_topic_id = api_version >= 6;
    int32_t topic_count = flex ? buffer.readCompactArrayLen() : buffer.readInt32();
    topics_.clear();
    topics_.resize(topic_count < 0 ? 0 : topic_count);
    for (int32_t i = 0; i < topic_count; ++i) {
        if (with_topic_id) {
            const auto name_opt = buffer.readCompactNullableString();
            topics_[i].name = name_opt.value_or("");
            const auto uuid_bytes = buffer.readBytes(16);
            std::copy(uuid_bytes.begin(), uuid_bytes.end(),
                      topics_[i].topic_id.begin());
            topics_[i].has_topic_id = true;
            buffer.skipTaggedFields();
        } else {
            topics_[i].name =
                flex ? buffer.readCompactString() : buffer.readString();
        }
    }
    timeout_ms_ = buffer.readInt32();
    if (flex) buffer.skipTaggedFields();
}

void DeleteTopicsResponse::encode(Buffer& buffer, int16_t api_version) const {
    const bool flex = api_version >= 4;
    const bool with_topic_id = api_version >= 6;
    if (api_version >= 1) {
        buffer.writeInt32(throttle_time_ms_);
    }
    if (flex) {
        buffer.writeCompactArrayLen(static_cast<int32_t>(results_.size()));
    } else {
        buffer.writeInt32(static_cast<int32_t>(results_.size()));
    }
    for (const auto& result : results_) {
        if (with_topic_id) {
            const auto name_opt = result.name.empty()
                                      ? std::optional<std::string>{}
                                      : std::optional<std::string>(result.name);
            buffer.writeCompactNullableString(name_opt);
            buffer.writeBytes(std::vector<uint8_t>(result.topic_id.begin(),
                                                  result.topic_id.end()));
        } else if (flex) {
            buffer.writeCompactString(result.name);
        } else {
            buffer.writeString(result.name);
        }
        buffer.writeInt16(static_cast<int16_t>(result.error_code));
        if (api_version >= 5) {
            const auto msg_opt = result.error_message.empty()
                                     ? std::optional<std::string>{}
                                     : std::optional<std::string>(result.error_message);
            buffer.writeCompactNullableString(msg_opt);
        }
        if (flex) buffer.writeEmptyTaggedFields();
    }
    if (flex) buffer.writeEmptyTaggedFields();
}

void DeleteTopicsResponse::decode(Buffer& buffer, int16_t api_version) {
    const bool flex = api_version >= 4;
    const bool with_topic_id = api_version >= 6;
    if (api_version >= 1) {
        throttle_time_ms_ = buffer.readInt32();
    }
    int32_t result_count = flex ? buffer.readCompactArrayLen() : buffer.readInt32();
    results_.clear();
    results_.resize(result_count < 0 ? 0 : result_count);
    for (int32_t i = 0; i < result_count; ++i) {
        if (with_topic_id) {
            const auto name_opt = buffer.readCompactNullableString();
            results_[i].name = name_opt.value_or("");
            const auto uuid_bytes = buffer.readBytes(16);
            std::copy(uuid_bytes.begin(), uuid_bytes.end(),
                      results_[i].topic_id.begin());
        } else {
            results_[i].name =
                flex ? buffer.readCompactString() : buffer.readString();
        }
        results_[i].error_code = static_cast<ErrorCode>(buffer.readInt16());
        if (api_version >= 5) {
            auto message = buffer.readCompactNullableString();
            results_[i].error_message = message.value_or("");
        }
        if (flex) buffer.skipTaggedFields();
    }
    if (flex) buffer.skipTaggedFields();
}

}  // namespace kawasan::protocol
