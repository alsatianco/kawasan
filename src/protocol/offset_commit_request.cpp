#include "kawasan/protocol/offset_commit_request.h"

#include "kawasan/common/error.h"

namespace kawasan::protocol {

void OffsetCommitRequest::encode(Buffer& buffer, int16_t api_version) const {
    if (api_version < 0 || api_version > 8) {
        throw ProtocolException("Unsupported OffsetCommit request version");
    }
    const bool flex = api_version >= 8;

    if (flex) buffer.writeCompactString(group_id_);
    else buffer.writeString(group_id_);

    if (api_version >= 1) {
        buffer.writeInt32(generation_id_);
        if (flex) buffer.writeCompactString(member_id_);
        else buffer.writeString(member_id_);
    }

    if (api_version >= 7) {
        if (flex) buffer.writeCompactNullableString(group_instance_id_);
        else buffer.writeNullableString(group_instance_id_);
    }

    // retention_time_ms was added in v2 and removed in v5.
    if (api_version >= 2 && api_version <= 4) {
        buffer.writeInt64(retention_time_ms_);
    }

    if (flex) {
        buffer.writeCompactArrayLen(static_cast<int32_t>(topics_.size()));
    } else {
        buffer.writeInt32(static_cast<int32_t>(topics_.size()));
    }
    for (const auto& topic : topics_) {
        if (flex) buffer.writeCompactString(topic.topic);
        else buffer.writeString(topic.topic);

        if (flex) {
            buffer.writeCompactArrayLen(static_cast<int32_t>(topic.partitions.size()));
        } else {
            buffer.writeInt32(static_cast<int32_t>(topic.partitions.size()));
        }
        for (const auto& partition : topic.partitions) {
            buffer.writeInt32(partition.partition);
            buffer.writeInt64(partition.offset);
            if (api_version == 1) {
                buffer.writeInt64(partition.timestamp);
            }
            if (api_version >= 6) {
                buffer.writeInt32(partition.committed_leader_epoch);
            }
            if (flex) buffer.writeCompactNullableString(
                partition.metadata.empty() ? std::optional<std::string>{}
                                           : std::optional<std::string>(partition.metadata));
            else buffer.writeNullableString(
                partition.metadata.empty() ? std::optional<std::string>{}
                                           : std::optional<std::string>(partition.metadata));
            if (flex) buffer.writeEmptyTaggedFields();
        }
        if (flex) buffer.writeEmptyTaggedFields();
    }

    if (flex) buffer.writeEmptyTaggedFields();
}

void OffsetCommitRequest::decode(Buffer& buffer, int16_t api_version) {
    if (api_version < 0 || api_version > 8) {
        throw ProtocolException("Unsupported OffsetCommit request version");
    }
    const bool flex = api_version >= 8;

    group_id_ = flex ? buffer.readCompactString() : buffer.readString();

    if (api_version >= 1) {
        generation_id_ = buffer.readInt32();
        member_id_ = flex ? buffer.readCompactString() : buffer.readString();
    } else {
        generation_id_ = -1;
        member_id_.clear();
    }

    group_instance_id_.reset();
    if (api_version >= 7) {
        group_instance_id_ =
            flex ? buffer.readCompactNullableString() : buffer.readNullableString();
    }

    retention_time_ms_ = -1;
    if (api_version >= 2 && api_version <= 4) {
        retention_time_ms_ = buffer.readInt64();
    }

    const int32_t topic_count =
        flex ? buffer.readCompactArrayLen() : buffer.readInt32();
    topics_.clear();
    topics_.reserve(topic_count < 0 ? 0 : topic_count);
    for (int32_t i = 0; i < topic_count; ++i) {
        TopicData topic;
        topic.topic = flex ? buffer.readCompactString() : buffer.readString();

        const int32_t partition_count =
            flex ? buffer.readCompactArrayLen() : buffer.readInt32();
        topic.partitions.reserve(partition_count < 0 ? 0 : partition_count);
        for (int32_t j = 0; j < partition_count; ++j) {
            PartitionData p;
            p.partition = buffer.readInt32();
            p.offset = buffer.readInt64();
            if (api_version == 1) {
                p.timestamp = buffer.readInt64();
            }
            if (api_version >= 6) {
                p.committed_leader_epoch = buffer.readInt32();
            }
            auto meta = flex ? buffer.readCompactNullableString()
                             : buffer.readNullableString();
            p.metadata = meta.value_or("");
            if (flex) buffer.skipTaggedFields();
            topic.partitions.push_back(std::move(p));
        }
        if (flex) buffer.skipTaggedFields();
        topics_.push_back(std::move(topic));
    }

    if (flex) buffer.skipTaggedFields();
}

void OffsetCommitResponse::encode(Buffer& buffer, int16_t api_version) const {
    if (api_version < 0 || api_version > 8) {
        throw ProtocolException("Unsupported OffsetCommit response version");
    }
    const bool flex = api_version >= 8;

    if (api_version >= 3) {
        buffer.writeInt32(throttle_time_ms_);
    }
    if (flex) {
        buffer.writeCompactArrayLen(static_cast<int32_t>(topics_.size()));
    } else {
        buffer.writeInt32(static_cast<int32_t>(topics_.size()));
    }
    for (const auto& topic : topics_) {
        if (flex) buffer.writeCompactString(topic.topic);
        else buffer.writeString(topic.topic);
        if (flex) {
            buffer.writeCompactArrayLen(static_cast<int32_t>(topic.partitions.size()));
        } else {
            buffer.writeInt32(static_cast<int32_t>(topic.partitions.size()));
        }
        for (const auto& partition : topic.partitions) {
            buffer.writeInt32(partition.partition);
            buffer.writeInt16(static_cast<int16_t>(partition.error));
            if (flex) buffer.writeEmptyTaggedFields();
        }
        if (flex) buffer.writeEmptyTaggedFields();
    }
    if (flex) buffer.writeEmptyTaggedFields();
}

void OffsetCommitResponse::decode(Buffer& buffer, int16_t api_version) {
    if (api_version < 0 || api_version > 8) {
        throw ProtocolException("Unsupported OffsetCommit response version");
    }
    const bool flex = api_version >= 8;

    if (api_version >= 3) {
        throttle_time_ms_ = buffer.readInt32();
    }
    const int32_t topic_count =
        flex ? buffer.readCompactArrayLen() : buffer.readInt32();
    topics_.clear();
    topics_.reserve(topic_count < 0 ? 0 : topic_count);
    for (int32_t i = 0; i < topic_count; ++i) {
        Topic topic;
        topic.topic = flex ? buffer.readCompactString() : buffer.readString();
        const int32_t partition_count =
            flex ? buffer.readCompactArrayLen() : buffer.readInt32();
        topic.partitions.reserve(partition_count < 0 ? 0 : partition_count);
        for (int32_t j = 0; j < partition_count; ++j) {
            Partition p;
            p.partition = buffer.readInt32();
            p.error = static_cast<ErrorCode>(buffer.readInt16());
            if (flex) buffer.skipTaggedFields();
            topic.partitions.push_back(p);
        }
        if (flex) buffer.skipTaggedFields();
        topics_.push_back(std::move(topic));
    }
    if (flex) buffer.skipTaggedFields();
}

}  // namespace kawasan::protocol
