#include "kawasan/protocol/offset_for_leader_epoch_request.h"

namespace kawasan::protocol {

void OffsetForLeaderEpochRequest::decode(Buffer& buffer, int16_t api_version) {
    const bool flex = api_version >= 4;
    if (api_version >= 3) {
        replica_id_ = buffer.readInt32();
    }

    int32_t topic_count = flex ? buffer.readCompactArrayLen() : buffer.readInt32();
    topics_.clear();
    topics_.reserve(topic_count < 0 ? 0 : topic_count);
    for (int32_t i = 0; i < topic_count; ++i) {
        TopicQuery t;
        t.name = flex ? buffer.readCompactString() : buffer.readString();

        int32_t partition_count =
            flex ? buffer.readCompactArrayLen() : buffer.readInt32();
        t.partitions.reserve(partition_count < 0 ? 0 : partition_count);
        for (int32_t j = 0; j < partition_count; ++j) {
            PartitionQuery p;
            p.partition = buffer.readInt32();
            p.current_leader_epoch = (api_version >= 2) ? buffer.readInt32() : -1;
            p.leader_epoch = buffer.readInt32();
            if (flex) buffer.skipTaggedFields();
            t.partitions.push_back(p);
        }
        if (flex) buffer.skipTaggedFields();
        topics_.push_back(std::move(t));
    }
    if (flex) buffer.skipTaggedFields();
}

void OffsetForLeaderEpochResponse::encode(Buffer& buffer, int16_t api_version) const {
    const bool flex = api_version >= 4;
    if (api_version >= 3) {
        buffer.writeInt32(throttle_time_ms_);
    }
    if (flex) {
        buffer.writeCompactArrayLen(static_cast<int32_t>(topics_.size()));
    } else {
        buffer.writeInt32(static_cast<int32_t>(topics_.size()));
    }
    for (const auto& t : topics_) {
        if (flex) {
            buffer.writeCompactString(t.name);
            buffer.writeCompactArrayLen(static_cast<int32_t>(t.partitions.size()));
        } else {
            buffer.writeString(t.name);
            buffer.writeInt32(static_cast<int32_t>(t.partitions.size()));
        }
        for (const auto& p : t.partitions) {
            buffer.writeInt16(static_cast<int16_t>(p.error_code));
            buffer.writeInt32(p.partition);
            if (api_version >= 1) {
                buffer.writeInt32(p.leader_epoch);
            }
            buffer.writeInt64(p.end_offset);
            if (flex) buffer.writeEmptyTaggedFields();
        }
        if (flex) buffer.writeEmptyTaggedFields();
    }
    if (flex) buffer.writeEmptyTaggedFields();
}

void OffsetForLeaderEpochRequest::encode(Buffer& buffer, int16_t api_version) const {
    const bool flex = api_version >= 4;
    if (api_version >= 3) {
        buffer.writeInt32(replica_id_);
    }
    if (flex) {
        buffer.writeCompactArrayLen(static_cast<int32_t>(topics_.size()));
    } else {
        buffer.writeInt32(static_cast<int32_t>(topics_.size()));
    }
    for (const auto& t : topics_) {
        if (flex) {
            buffer.writeCompactString(t.name);
            buffer.writeCompactArrayLen(static_cast<int32_t>(t.partitions.size()));
        } else {
            buffer.writeString(t.name);
            buffer.writeInt32(static_cast<int32_t>(t.partitions.size()));
        }
        for (const auto& p : t.partitions) {
            buffer.writeInt32(p.partition);
            if (api_version >= 2) {
                buffer.writeInt32(p.current_leader_epoch);
            }
            buffer.writeInt32(p.leader_epoch);
            if (flex) buffer.writeEmptyTaggedFields();
        }
        if (flex) buffer.writeEmptyTaggedFields();
    }
    if (flex) buffer.writeEmptyTaggedFields();
}

void OffsetForLeaderEpochResponse::decode(Buffer& buffer, int16_t api_version) {
    const bool flex = api_version >= 4;
    throttle_time_ms_ = (api_version >= 3) ? buffer.readInt32() : 0;
    const int32_t topic_count = flex ? buffer.readCompactArrayLen() : buffer.readInt32();
    topics_.clear();
    for (int32_t i = 0; i < topic_count; ++i) {
        TopicResult t;
        t.name = flex ? buffer.readCompactString() : buffer.readString();
        const int32_t partition_count = flex ? buffer.readCompactArrayLen() : buffer.readInt32();
        for (int32_t j = 0; j < partition_count; ++j) {
            PartitionResult p;
            p.error_code = static_cast<ErrorCode>(buffer.readInt16());
            p.partition = buffer.readInt32();
            p.leader_epoch = (api_version >= 1) ? buffer.readInt32() : -1;
            p.end_offset = buffer.readInt64();
            if (flex) buffer.skipTaggedFields();
            t.partitions.push_back(p);
        }
        if (flex) buffer.skipTaggedFields();
        topics_.push_back(std::move(t));
    }
    if (flex) buffer.skipTaggedFields();
}

}  // namespace kawasan::protocol
