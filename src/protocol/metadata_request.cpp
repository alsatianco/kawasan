#include "kawasan/protocol/metadata_request.h"

namespace kawasan::protocol {

// Phase 1.2: full v0–v12 support.

void MetadataRequest::encode(Buffer& buffer, int16_t api_version) const {
    const bool flex = api_version >= 9;

    // Topics
    if (topics_.empty()) {
        // v9+ uses compact array, others use int32.
        if (flex) {
            buffer.writeUnsignedVarInt(0);  // null compact array
        } else {
            buffer.writeInt32(-1);
        }
    } else {
        if (flex) {
            buffer.writeCompactArrayLen(static_cast<int32_t>(topics_.size()));
        } else {
            buffer.writeInt32(static_cast<int32_t>(topics_.size()));
        }
        for (const auto& topic : topics_) {
            if (api_version >= 10) {
                // v10+: each entry is { topic_id UUID, name COMPACT_NULLABLE_STRING }
                // Write all-zeros UUID since the client doesn't know IDs.
                std::array<uint8_t, 16> zero{};
                buffer.writeBytes(zero.data(), zero.size());
                buffer.writeCompactNullableString(topic);
                buffer.writeEmptyTaggedFields();
            } else {
                if (flex) buffer.writeCompactString(topic);
                else buffer.writeString(topic);
            }
        }
    }

    if (api_version >= 4) {
        buffer.writeInt8(allow_auto_topic_creation_ ? 1 : 0);
    }
    if (api_version >= 8 && api_version <= 10) {
        buffer.writeInt8(include_cluster_authorized_operations_ ? 1 : 0);
    }
    if (api_version >= 8) {
        buffer.writeInt8(include_topic_authorized_operations_ ? 1 : 0);
    }

    if (flex) buffer.writeEmptyTaggedFields();
}

void MetadataRequest::decode(Buffer& buffer, int16_t api_version) {
    const bool flex = api_version >= 9;

    topics_.clear();
    int32_t topic_count;
    if (flex) {
        const uint32_t raw = buffer.readUnsignedVarInt();
        topic_count = (raw == 0) ? -1 : static_cast<int32_t>(raw - 1);
    } else {
        topic_count = buffer.readInt32();
    }
    if (topic_count >= 0) {
        topics_.reserve(topic_count);
        for (int32_t i = 0; i < topic_count; ++i) {
            if (api_version >= 10) {
                (void)buffer.readBytes(16);  // topic_id UUID, ignored for now
                auto name = buffer.readCompactNullableString();
                topics_.push_back(name.value_or(""));
                buffer.skipTaggedFields();
            } else {
                topics_.push_back(flex ? buffer.readCompactString()
                                       : buffer.readString());
            }
        }
    }

    if (api_version >= 4) {
        allow_auto_topic_creation_ = (buffer.readInt8() != 0);
    }
    if (api_version >= 8 && api_version <= 10) {
        include_cluster_authorized_operations_ = (buffer.readInt8() != 0);
    }
    if (api_version >= 8) {
        include_topic_authorized_operations_ = (buffer.readInt8() != 0);
    }

    if (flex) buffer.skipTaggedFields();
}

size_t MetadataRequest::size(int16_t api_version) const {
    // Approximate — flexible encoding makes exact size hard.
    size_t result = sizeof(int32_t);
    for (const auto& topic : topics_) {
        result += sizeof(int16_t) + topic.size();
    }
    if (api_version >= 4) result += sizeof(int8_t);
    if (api_version >= 8) result += sizeof(int8_t) * 2;
    return result;
}

void MetadataResponse::encode(Buffer& buffer, int16_t api_version) const {
    const bool flex = api_version >= 9;

    if (api_version >= 3) {
        buffer.writeInt32(throttle_time_ms_);
    }

    // Brokers
    if (flex) {
        buffer.writeCompactArrayLen(static_cast<int32_t>(brokers_.size()));
    } else {
        buffer.writeInt32(static_cast<int32_t>(brokers_.size()));
    }
    for (const auto& broker : brokers_) {
        buffer.writeInt32(broker.id);
        if (flex) buffer.writeCompactString(broker.host);
        else buffer.writeString(broker.host);
        buffer.writeInt32(broker.port);
        if (api_version >= 1) {
            if (flex) buffer.writeCompactNullableString(broker.rack);
            else buffer.writeNullableString(broker.rack);
        }
        if (flex) buffer.writeEmptyTaggedFields();
    }

    if (api_version >= 2) {
        if (flex) {
            // cluster_id is NULLABLE on v9+
            buffer.writeCompactNullableString(
                cluster_id_.empty() ? std::optional<std::string>{}
                                    : std::optional<std::string>(cluster_id_));
        } else {
            buffer.writeString(cluster_id_);
        }
    }
    if (api_version >= 1) {
        buffer.writeInt32(controller_id_);
    }

    // Topics
    if (flex) {
        buffer.writeCompactArrayLen(static_cast<int32_t>(topics_.size()));
    } else {
        buffer.writeInt32(static_cast<int32_t>(topics_.size()));
    }
    for (const auto& topic : topics_) {
        buffer.writeInt16(static_cast<int16_t>(topic.error_code));
        if (flex) buffer.writeCompactString(topic.name);
        else buffer.writeString(topic.name);
        if (api_version >= 10) {
            buffer.writeBytes(topic.topic_id.data(), topic.topic_id.size());
        }
        if (api_version >= 1) {
            buffer.writeInt8(topic.is_internal ? 1 : 0);
        }

        if (flex) {
            buffer.writeCompactArrayLen(static_cast<int32_t>(topic.partitions.size()));
        } else {
            buffer.writeInt32(static_cast<int32_t>(topic.partitions.size()));
        }
        for (const auto& partition : topic.partitions) {
            buffer.writeInt16(static_cast<int16_t>(partition.error_code));
            buffer.writeInt32(partition.partition);
            buffer.writeInt32(partition.leader);
            if (api_version >= 7) buffer.writeInt32(partition.leader_epoch);

            if (flex) {
                buffer.writeCompactArrayLen(static_cast<int32_t>(partition.replicas.size()));
            } else {
                buffer.writeInt32(static_cast<int32_t>(partition.replicas.size()));
            }
            for (auto replica : partition.replicas) buffer.writeInt32(replica);

            if (flex) {
                buffer.writeCompactArrayLen(static_cast<int32_t>(partition.isr.size()));
            } else {
                buffer.writeInt32(static_cast<int32_t>(partition.isr.size()));
            }
            for (auto isr_broker : partition.isr) buffer.writeInt32(isr_broker);

            if (api_version >= 5) {
                if (flex) {
                    buffer.writeCompactArrayLen(static_cast<int32_t>(partition.offline_replicas.size()));
                } else {
                    buffer.writeInt32(static_cast<int32_t>(partition.offline_replicas.size()));
                }
                for (auto offline : partition.offline_replicas) buffer.writeInt32(offline);
            }
            if (flex) buffer.writeEmptyTaggedFields();
        }

        if (api_version >= 8) {
            buffer.writeInt32(topic.topic_authorized_operations);
        }
        if (flex) buffer.writeEmptyTaggedFields();
    }

    if (api_version >= 8 && api_version <= 10) {
        // cluster_authorized_operations — 0 instead of INT32_MIN sentinel
        // (see comment in TopicMetadata; same kafka-python bug).
        buffer.writeInt32(0);
    }

    if (flex) buffer.writeEmptyTaggedFields();
}

void MetadataResponse::decode(Buffer& buffer, int16_t api_version) {
    const bool flex = api_version >= 9;

    if (api_version >= 3) {
        throttle_time_ms_ = buffer.readInt32();
    }

    int32_t broker_count = flex ? buffer.readCompactArrayLen() : buffer.readInt32();
    brokers_.resize(broker_count < 0 ? 0 : broker_count);
    for (int32_t i = 0; i < broker_count; ++i) {
        brokers_[i].id = buffer.readInt32();
        brokers_[i].host = flex ? buffer.readCompactString() : buffer.readString();
        brokers_[i].port = buffer.readInt32();
        if (api_version >= 1) {
            brokers_[i].rack = flex ? buffer.readCompactNullableString()
                                    : buffer.readNullableString();
        }
        if (flex) buffer.skipTaggedFields();
    }

    if (api_version >= 2) {
        if (flex) {
            cluster_id_ = buffer.readCompactNullableString().value_or("");
        } else {
            auto cid = buffer.readNullableString();
            cluster_id_ = cid.value_or("");
        }
    }
    if (api_version >= 1) {
        controller_id_ = buffer.readInt32();
    }

    int32_t topic_count = flex ? buffer.readCompactArrayLen() : buffer.readInt32();
    topics_.resize(topic_count < 0 ? 0 : topic_count);
    for (int32_t i = 0; i < topic_count; ++i) {
        topics_[i].error_code = static_cast<ErrorCode>(buffer.readInt16());
        topics_[i].name = flex ? buffer.readCompactString() : buffer.readString();
        if (api_version >= 10) {
            auto uuid = buffer.readBytes(16);
            std::copy(uuid.begin(), uuid.end(), topics_[i].topic_id.begin());
        }
        if (api_version >= 1) {
            topics_[i].is_internal = (buffer.readInt8() != 0);
        }

        int32_t partition_count = flex ? buffer.readCompactArrayLen() : buffer.readInt32();
        topics_[i].partitions.resize(partition_count < 0 ? 0 : partition_count);
        for (int32_t j = 0; j < partition_count; ++j) {
            auto& partition = topics_[i].partitions[j];
            partition.error_code = static_cast<ErrorCode>(buffer.readInt16());
            partition.partition = buffer.readInt32();
            partition.leader = buffer.readInt32();
            if (api_version >= 7) partition.leader_epoch = buffer.readInt32();

            int32_t rc = flex ? buffer.readCompactArrayLen() : buffer.readInt32();
            partition.replicas.resize(rc < 0 ? 0 : rc);
            for (int32_t k = 0; k < rc; ++k) partition.replicas[k] = buffer.readInt32();

            int32_t isr = flex ? buffer.readCompactArrayLen() : buffer.readInt32();
            partition.isr.resize(isr < 0 ? 0 : isr);
            for (int32_t k = 0; k < isr; ++k) partition.isr[k] = buffer.readInt32();

            if (api_version >= 5) {
                int32_t oc = flex ? buffer.readCompactArrayLen() : buffer.readInt32();
                partition.offline_replicas.resize(oc < 0 ? 0 : oc);
                for (int32_t k = 0; k < oc; ++k) partition.offline_replicas[k] = buffer.readInt32();
            }
            if (flex) buffer.skipTaggedFields();
        }

        if (api_version >= 8) {
            topics_[i].topic_authorized_operations = buffer.readInt32();
        }
        if (flex) buffer.skipTaggedFields();
    }

    if (api_version >= 8 && api_version <= 10) {
        (void)buffer.readInt32();  // cluster_authorized_operations
    }

    if (flex) buffer.skipTaggedFields();
}

size_t MetadataResponse::size(int16_t api_version) const {
    // Approximate.
    size_t result = 0;
    if (api_version >= 3) result += sizeof(int32_t);
    result += sizeof(int32_t);
    for (const auto& broker : brokers_) {
        result += sizeof(int32_t) + sizeof(int16_t) + broker.host.size() + sizeof(int32_t);
        if (api_version >= 1) {
            result += sizeof(int16_t);
            if (broker.rack) result += broker.rack->size();
        }
    }
    if (api_version >= 2) result += sizeof(int16_t) + cluster_id_.size();
    if (api_version >= 1) result += sizeof(int32_t);
    result += sizeof(int32_t);
    for (const auto& topic : topics_) {
        result += sizeof(int16_t) + sizeof(int16_t) + topic.name.size();
        if (api_version >= 10) result += 16;
        if (api_version >= 1) result += sizeof(int8_t);
        result += sizeof(int32_t);
        for (const auto& partition : topic.partitions) {
            result += sizeof(int16_t) + sizeof(int32_t) * 2;
            if (api_version >= 7) result += sizeof(int32_t);
            result += sizeof(int32_t) + partition.replicas.size() * sizeof(int32_t);
            result += sizeof(int32_t) + partition.isr.size() * sizeof(int32_t);
            if (api_version >= 5) {
                result += sizeof(int32_t) + partition.offline_replicas.size() * sizeof(int32_t);
            }
        }
        if (api_version >= 8) result += sizeof(int32_t);
    }
    if (api_version >= 8 && api_version <= 10) result += sizeof(int32_t);
    return result;
}

}  // namespace kawasan::protocol
