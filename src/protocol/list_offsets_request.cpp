#include "kawasan/protocol/list_offsets_request.h"

#include "kawasan/common/logger.h"

namespace kawasan::protocol {

// Phase 1.5: supports v0–v7.
//   v0:   includes max_num_offsets, response returns an array of offsets
//   v1+:  single (timestamp, offset) per partition; response shape changes
//   v2+:  + isolation_level (request)
//   v4+:  + current_leader_epoch (request), leader_epoch (response)
//   v6+:  flexible (compact strings + tagged fields)
//   v7+:  same wire format; adds MAX_TIMESTAMP (-3) sentinel semantics

void ListOffsetsRequest::encode(Buffer& buffer, int16_t api_version) const {
    const bool flex = api_version >= 6;

    buffer.writeInt32(replica_id_);
    if (api_version >= 2) {
        buffer.writeInt8(isolation_level_);
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
            if (api_version >= 4) {
                buffer.writeInt32(partition.current_leader_epoch);
            }
            buffer.writeInt64(partition.timestamp);
            if (api_version == 0) {
                buffer.writeInt32(partition.max_num_offsets);
            }
            if (flex) buffer.writeEmptyTaggedFields();
        }
        if (flex) buffer.writeEmptyTaggedFields();
    }
    if (flex) buffer.writeEmptyTaggedFields();
}

void ListOffsetsRequest::decode(Buffer& buffer, int16_t api_version) {
    const bool flex = api_version >= 6;

    replica_id_ = buffer.readInt32();
    if (api_version >= 2) {
        isolation_level_ = buffer.readInt8();
    }

    int32_t topic_count = flex ? buffer.readCompactArrayLen() : buffer.readInt32();
    topics_.clear();
    topics_.resize(topic_count < 0 ? 0 : topic_count);
    for (int32_t i = 0; i < topic_count; ++i) {
        topics_[i].topic = flex ? buffer.readCompactString() : buffer.readString();

        int32_t partition_count =
            flex ? buffer.readCompactArrayLen() : buffer.readInt32();
        topics_[i].partitions.resize(partition_count < 0 ? 0 : partition_count);
        for (int32_t j = 0; j < partition_count; ++j) {
            topics_[i].partitions[j].partition = buffer.readInt32();
            if (api_version >= 4) {
                topics_[i].partitions[j].current_leader_epoch = buffer.readInt32();
            }
            topics_[i].partitions[j].timestamp = buffer.readInt64();
            if (api_version == 0) {
                topics_[i].partitions[j].max_num_offsets = buffer.readInt32();
            }
            if (flex) buffer.skipTaggedFields();
        }
        if (flex) buffer.skipTaggedFields();
    }
    if (flex) buffer.skipTaggedFields();
}

void ListOffsetsResponse::encode(Buffer& buffer, int16_t api_version) const {
    const bool flex = api_version >= 6;

    if (api_version >= 2) {
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
            buffer.writeInt16(static_cast<int16_t>(partition.error_code));

            if (api_version == 0) {
                // v0 response: array of offsets per partition.
                buffer.writeInt32(static_cast<int32_t>(partition.old_style_offsets.size()));
                for (int64_t offset : partition.old_style_offsets) {
                    buffer.writeInt64(offset);
                }
            } else {
                buffer.writeInt64(partition.timestamp);
                buffer.writeInt64(partition.offset);
                if (api_version >= 4) {
                    buffer.writeInt32(partition.leader_epoch);
                }
            }
            if (flex) buffer.writeEmptyTaggedFields();
        }
        if (flex) buffer.writeEmptyTaggedFields();
    }
    if (flex) buffer.writeEmptyTaggedFields();
}

void ListOffsetsResponse::decode(Buffer& buffer, int16_t api_version) {
    const bool flex = api_version >= 6;

    if (api_version >= 2) {
        throttle_time_ms_ = buffer.readInt32();
    }
    int32_t topic_count = flex ? buffer.readCompactArrayLen() : buffer.readInt32();
    topics_.clear();
    topics_.resize(topic_count < 0 ? 0 : topic_count);
    for (int32_t i = 0; i < topic_count; ++i) {
        topics_[i].topic = flex ? buffer.readCompactString() : buffer.readString();

        int32_t partition_count =
            flex ? buffer.readCompactArrayLen() : buffer.readInt32();
        topics_[i].partitions.resize(partition_count < 0 ? 0 : partition_count);
        for (int32_t j = 0; j < partition_count; ++j) {
            auto& partition = topics_[i].partitions[j];
            partition.partition = buffer.readInt32();
            partition.error_code = static_cast<ErrorCode>(buffer.readInt16());

            if (api_version == 0) {
                int32_t offset_count = buffer.readInt32();
                partition.old_style_offsets.resize(offset_count);
                for (int32_t k = 0; k < offset_count; ++k) {
                    partition.old_style_offsets[k] = buffer.readInt64();
                }
                if (!partition.old_style_offsets.empty()) {
                    partition.offset = partition.old_style_offsets[0];
                }
            } else {
                partition.timestamp = buffer.readInt64();
                partition.offset = buffer.readInt64();
                if (api_version >= 4) {
                    partition.leader_epoch = buffer.readInt32();
                }
            }
            if (flex) buffer.skipTaggedFields();
        }
        if (flex) buffer.skipTaggedFields();
    }
    if (flex) buffer.skipTaggedFields();
}

}  // namespace kawasan::protocol
