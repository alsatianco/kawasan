#include "kawasan/protocol/produce_request.h"

#include "kawasan/common/logger.h"

namespace kawasan::protocol {

// Phase 1.3: full v0–v9.
//   v3:  + transactional_id NULLABLE_STRING (first field)
//   v5:  + log_start_offset in response
//   v8:  + record_errors[] + error_message in response
//   v9:  flexible

void ProduceRequest::encode(Buffer& buffer, int16_t api_version) const {
    const bool flex = api_version >= 9;

    if (api_version >= 3) {
        const auto txn_opt = transactional_id_.empty()
                                 ? std::optional<std::string>{}
                                 : std::optional<std::string>(transactional_id_);
        if (flex) buffer.writeCompactNullableString(txn_opt);
        else buffer.writeNullableString(txn_opt);
    }
    buffer.writeInt16(acks_);
    buffer.writeInt32(timeout_ms_);

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
            // record_batch is COMPACT_BYTES at v9, BYTES otherwise.
            if (flex) buffer.writeCompactBytes(partition.record_batch);
            else {
                buffer.writeInt32(static_cast<int32_t>(partition.record_batch.size()));
                buffer.writeBytes(partition.record_batch.data(),
                                  partition.record_batch.size());
            }
            if (flex) buffer.writeEmptyTaggedFields();
        }
        if (flex) buffer.writeEmptyTaggedFields();
    }
    if (flex) buffer.writeEmptyTaggedFields();
}

void ProduceRequest::decode(Buffer& buffer, int16_t api_version) {
    const bool flex = api_version >= 9;

    if (api_version >= 3) {
        auto txn = flex ? buffer.readCompactNullableString()
                        : buffer.readNullableString();
        transactional_id_ = txn.value_or("");
    }
    acks_ = buffer.readInt16();
    timeout_ms_ = buffer.readInt32();

    int32_t topic_count = flex ? buffer.readCompactArrayLen() : buffer.readInt32();
    topics_.clear();
    topics_.resize(topic_count < 0 ? 0 : topic_count);
    for (int32_t i = 0; i < topic_count; ++i) {
        topics_[i].topic = flex ? buffer.readCompactString() : buffer.readString();
        int32_t partition_count = flex ? buffer.readCompactArrayLen()
                                        : buffer.readInt32();
        topics_[i].partitions.resize(partition_count < 0 ? 0 : partition_count);
        for (int32_t j = 0; j < partition_count; ++j) {
            topics_[i].partitions[j].partition = buffer.readInt32();
            if (flex) {
                topics_[i].partitions[j].record_batch = buffer.readCompactBytes();
            } else {
                int32_t batch_size = buffer.readInt32();
                if (batch_size < 0) {
                    throw std::runtime_error("Negative batch size in produce request");
                }
                topics_[i].partitions[j].record_batch = buffer.readBytes(batch_size);
            }
            if (flex) buffer.skipTaggedFields();
        }
        if (flex) buffer.skipTaggedFields();
    }
    if (flex) buffer.skipTaggedFields();
}

size_t ProduceRequest::size(int16_t api_version) const {
    size_t result = 0;
    if (api_version >= 3) {
        result += sizeof(int16_t);
        if (!transactional_id_.empty()) result += transactional_id_.size();
    }
    result += sizeof(int16_t) + sizeof(int32_t) + sizeof(int32_t);
    for (const auto& topic : topics_) {
        result += sizeof(int16_t) + topic.topic.size() + sizeof(int32_t);
        for (const auto& partition : topic.partitions) {
            result += sizeof(int32_t) + sizeof(int32_t) + partition.record_batch.size();
        }
    }
    return result;
}

void ProduceResponse::encode(Buffer& buffer, int16_t api_version) const {
    const bool flex = api_version >= 9;

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
            buffer.writeInt64(partition.base_offset);
            if (api_version >= 2) buffer.writeInt64(partition.log_append_time);
            if (api_version >= 5) buffer.writeInt64(partition.log_start_offset);

            if (api_version >= 8) {
                if (flex) {
                    buffer.writeCompactArrayLen(
                        static_cast<int32_t>(partition.record_errors.size()));
                } else {
                    buffer.writeInt32(static_cast<int32_t>(partition.record_errors.size()));
                }
                for (const auto& re : partition.record_errors) {
                    buffer.writeInt32(re.batch_index);
                    const auto msg_opt = re.error_message.empty()
                                             ? std::optional<std::string>{}
                                             : std::optional<std::string>(re.error_message);
                    if (flex) buffer.writeCompactNullableString(msg_opt);
                    else buffer.writeNullableString(msg_opt);
                    if (flex) buffer.writeEmptyTaggedFields();
                }
                const auto emsg_opt = partition.error_message.empty()
                                          ? std::optional<std::string>{}
                                          : std::optional<std::string>(partition.error_message);
                if (flex) buffer.writeCompactNullableString(emsg_opt);
                else buffer.writeNullableString(emsg_opt);
            }
            if (flex) buffer.writeEmptyTaggedFields();
        }
        if (flex) buffer.writeEmptyTaggedFields();
    }

    if (api_version >= 1) {
        buffer.writeInt32(throttle_time_ms_);
    }
    if (flex) buffer.writeEmptyTaggedFields();
}

void ProduceResponse::decode(Buffer& buffer, int16_t api_version) {
    const bool flex = api_version >= 9;

    int32_t topic_count = flex ? buffer.readCompactArrayLen() : buffer.readInt32();
    topics_.resize(topic_count < 0 ? 0 : topic_count);
    for (int32_t i = 0; i < topic_count; ++i) {
        topics_[i].topic = flex ? buffer.readCompactString() : buffer.readString();
        int32_t partition_count = flex ? buffer.readCompactArrayLen()
                                        : buffer.readInt32();
        topics_[i].partitions.resize(partition_count < 0 ? 0 : partition_count);
        for (int32_t j = 0; j < partition_count; ++j) {
            auto& partition = topics_[i].partitions[j];
            partition.partition = buffer.readInt32();
            partition.error_code = static_cast<ErrorCode>(buffer.readInt16());
            partition.base_offset = buffer.readInt64();
            if (api_version >= 2) partition.log_append_time = buffer.readInt64();
            if (api_version >= 5) partition.log_start_offset = buffer.readInt64();

            if (api_version >= 8) {
                int32_t re_count = flex ? buffer.readCompactArrayLen()
                                        : buffer.readInt32();
                partition.record_errors.resize(re_count < 0 ? 0 : re_count);
                for (int32_t k = 0; k < re_count; ++k) {
                    partition.record_errors[k].batch_index = buffer.readInt32();
                    auto msg = flex ? buffer.readCompactNullableString()
                                    : buffer.readNullableString();
                    partition.record_errors[k].error_message = msg.value_or("");
                    if (flex) buffer.skipTaggedFields();
                }
                auto emsg = flex ? buffer.readCompactNullableString()
                                 : buffer.readNullableString();
                partition.error_message = emsg.value_or("");
            }
            if (flex) buffer.skipTaggedFields();
        }
        if (flex) buffer.skipTaggedFields();
    }

    if (api_version >= 1) throttle_time_ms_ = buffer.readInt32();
    if (flex) buffer.skipTaggedFields();
}

size_t ProduceResponse::size(int16_t api_version) const {
    size_t result = sizeof(int32_t);
    for (const auto& topic : topics_) {
        result += sizeof(int16_t) + topic.topic.size() + sizeof(int32_t);
        for ([[maybe_unused]] const auto& partition : topic.partitions) {
            result += sizeof(int32_t) + sizeof(int16_t) + sizeof(int64_t);
            if (api_version >= 2) result += sizeof(int64_t);
            if (api_version >= 5) result += sizeof(int64_t);
        }
    }
    if (api_version >= 1) result += sizeof(int32_t);
    return result;
}

}  // namespace kawasan::protocol
