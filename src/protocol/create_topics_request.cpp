#include "kawasan/protocol/create_topics_request.h"

#include <optional>

#include "kawasan/common/error.h"

namespace kawasan::protocol {

namespace {

void encodeAssignments(Buffer& buffer,
                       const std::vector<CreatableReplicaAssignment>& assignments,
                       bool flex) {
    if (flex) {
        buffer.writeCompactArrayLen(static_cast<int32_t>(assignments.size()));
    } else {
        buffer.writeInt32(static_cast<int32_t>(assignments.size()));
    }
    for (const auto& assignment : assignments) {
        buffer.writeInt32(assignment.partition_index);
        if (flex) {
            buffer.writeCompactArrayLen(static_cast<int32_t>(assignment.broker_ids.size()));
        } else {
            buffer.writeInt32(static_cast<int32_t>(assignment.broker_ids.size()));
        }
        for (auto broker_id : assignment.broker_ids) {
            buffer.writeInt32(broker_id);
        }
        if (flex) buffer.writeEmptyTaggedFields();
    }
}

void decodeAssignments(Buffer& buffer,
                       std::vector<CreatableReplicaAssignment>& assignments,
                       bool flex) {
    int32_t count = flex ? buffer.readCompactArrayLen() : buffer.readInt32();
    assignments.clear();
    if (count <= 0) return;
    assignments.resize(count);
    for (int32_t i = 0; i < count; ++i) {
        assignments[i].partition_index = buffer.readInt32();
        int32_t broker_count =
            flex ? buffer.readCompactArrayLen() : buffer.readInt32();
        assignments[i].broker_ids.clear();
        if (broker_count > 0) {
            assignments[i].broker_ids.resize(broker_count);
            for (int32_t j = 0; j < broker_count; ++j) {
                assignments[i].broker_ids[j] = buffer.readInt32();
            }
        }
        if (flex) buffer.skipTaggedFields();
    }
}

void encodeConfigs(Buffer& buffer, const std::vector<CreatableTopicConfig>& configs,
                   int16_t api_version, bool flex) {
    if (flex) {
        buffer.writeCompactArrayLen(static_cast<int32_t>(configs.size()));
    } else {
        buffer.writeInt32(static_cast<int32_t>(configs.size()));
    }
    for (const auto& config : configs) {
        if (flex) buffer.writeCompactString(config.name);
        else buffer.writeString(config.name);

        // v5+ made value NULLABLE.
        const auto val_opt = (api_version >= 5 && config.value.empty())
                                 ? std::optional<std::string>{}
                                 : std::optional<std::string>(config.value);
        if (api_version >= 5) {
            if (flex) buffer.writeCompactNullableString(val_opt);
            else buffer.writeNullableString(val_opt);
        } else {
            if (flex) buffer.writeCompactString(config.value);
            else buffer.writeString(config.value);
        }
        if (flex) buffer.writeEmptyTaggedFields();
    }
}

void decodeConfigs(Buffer& buffer, std::vector<CreatableTopicConfig>& configs,
                   int16_t api_version, bool flex) {
    int32_t count = flex ? buffer.readCompactArrayLen() : buffer.readInt32();
    configs.clear();
    if (count <= 0) return;
    configs.resize(count);
    for (int32_t i = 0; i < count; ++i) {
        configs[i].name = flex ? buffer.readCompactString() : buffer.readString();
        if (api_version >= 5) {
            auto value = flex ? buffer.readCompactNullableString()
                              : buffer.readNullableString();
            configs[i].value = value.value_or("");
        } else {
            configs[i].value = flex ? buffer.readCompactString() : buffer.readString();
        }
        if (flex) buffer.skipTaggedFields();
    }
}

}  // namespace

void CreateTopicsRequest::encode(Buffer& buffer, int16_t api_version) const {
    const bool flex = api_version >= 5;
    if (flex) {
        buffer.writeCompactArrayLen(static_cast<int32_t>(topics_.size()));
    } else {
        buffer.writeInt32(static_cast<int32_t>(topics_.size()));
    }
    for (const auto& topic : topics_) {
        if (flex) buffer.writeCompactString(topic.name);
        else buffer.writeString(topic.name);
        buffer.writeInt32(topic.num_partitions);
        buffer.writeInt16(topic.replication_factor);
        encodeAssignments(buffer, topic.assignments, flex);
        encodeConfigs(buffer, topic.configs, api_version, flex);
        if (flex) buffer.writeEmptyTaggedFields();
    }
    buffer.writeInt32(timeout_ms_);
    if (api_version >= 1) {
        buffer.writeInt8(validate_only_ ? 1 : 0);
    }
    if (flex) buffer.writeEmptyTaggedFields();
}

void CreateTopicsRequest::decode(Buffer& buffer, int16_t api_version) {
    const bool flex = api_version >= 5;
    int32_t topic_count = flex ? buffer.readCompactArrayLen() : buffer.readInt32();
    topics_.clear();
    topics_.resize(topic_count < 0 ? 0 : topic_count);
    for (int32_t i = 0; i < topic_count; ++i) {
        topics_[i].name = flex ? buffer.readCompactString() : buffer.readString();
        topics_[i].num_partitions = buffer.readInt32();
        topics_[i].replication_factor = buffer.readInt16();
        decodeAssignments(buffer, topics_[i].assignments, flex);
        decodeConfigs(buffer, topics_[i].configs, api_version, flex);
        if (flex) buffer.skipTaggedFields();
    }
    timeout_ms_ = buffer.readInt32();
    if (api_version >= 1) {
        validate_only_ = (buffer.readInt8() != 0);
    }
    if (flex) buffer.skipTaggedFields();
}

void CreateTopicsResponse::encode(Buffer& buffer, int16_t api_version) const {
    const bool flex = api_version >= 5;
    if (api_version >= 2) {
        buffer.writeInt32(throttle_time_ms_);
    }
    if (flex) {
        buffer.writeCompactArrayLen(static_cast<int32_t>(topics_.size()));
    } else {
        buffer.writeInt32(static_cast<int32_t>(topics_.size()));
    }
    for (const auto& topic : topics_) {
        if (flex) buffer.writeCompactString(topic.name);
        else buffer.writeString(topic.name);
        // v7+: topic_id UUID. Place AFTER name, BEFORE error_code.
        if (api_version >= 7) {
            buffer.writeBytes(topic.topic_id.data(), topic.topic_id.size());
        }
        buffer.writeInt16(static_cast<int16_t>(topic.error_code));
        if (api_version >= 1) {
            const auto msg_opt = topic.error_message.empty()
                                     ? std::optional<std::string>{}
                                     : std::optional<std::string>(topic.error_message);
            if (flex) buffer.writeCompactNullableString(msg_opt);
            else buffer.writeNullableString(msg_opt);
        }
        // v5+ adds num_partitions/replication_factor + topic_config_error_code +
        // configs array; we write minimal placeholders.
        if (api_version >= 5) {
            buffer.writeInt32(topic.num_partitions);
            buffer.writeInt16(topic.replication_factor);
            buffer.writeInt16(0);                   // topic_config_error_code
            // Empty configs array.
            buffer.writeCompactArrayLen(0);
        }
        if (flex) buffer.writeEmptyTaggedFields();
    }
    if (flex) buffer.writeEmptyTaggedFields();
}

void CreateTopicsResponse::decode(Buffer& buffer, int16_t api_version) {
    const bool flex = api_version >= 5;
    if (api_version >= 2) {
        throttle_time_ms_ = buffer.readInt32();
    }
    int32_t count = flex ? buffer.readCompactArrayLen() : buffer.readInt32();
    topics_.clear();
    topics_.resize(count < 0 ? 0 : count);
    for (int32_t i = 0; i < count; ++i) {
        topics_[i].name = flex ? buffer.readCompactString() : buffer.readString();
        if (api_version >= 7) {
            auto uuid = buffer.readBytes(16);
            std::copy(uuid.begin(), uuid.end(), topics_[i].topic_id.begin());
        }
        topics_[i].error_code = static_cast<ErrorCode>(buffer.readInt16());
        if (api_version >= 1) {
            auto message = flex ? buffer.readCompactNullableString()
                                : buffer.readNullableString();
            topics_[i].error_message = message.value_or("");
        }
        if (api_version >= 5) {
            topics_[i].num_partitions = buffer.readInt32();
            topics_[i].replication_factor = buffer.readInt16();
            (void)buffer.readInt16();                            // topic_config_error_code
            (void)buffer.readCompactArrayLen();                  // configs[] (we skip body)
        }
        if (flex) buffer.skipTaggedFields();
    }
    if (flex) buffer.skipTaggedFields();
}

broker::TopicSpecification toTopicSpecification(const CreatableTopic& topic) {
    broker::TopicSpecification spec;
    spec.name = topic.name;
    spec.num_partitions = topic.num_partitions;
    spec.replication_factor = topic.replication_factor;
    spec.assignments.reserve(topic.assignments.size());
    for (const auto& assignment : topic.assignments) {
        spec.assignments.push_back(assignment.broker_ids);
    }
    for (const auto& config : topic.configs) {
        spec.configs[config.name] = config.value;
    }
    return spec;
}

}  // namespace kawasan::protocol
