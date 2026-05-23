#include "kawasan/protocol/fetch_request.h"

namespace kawasan::protocol {

// Phase 1.4: supports v0–v12. v13 topic_id refactor still deferred along
// with the full KIP-227 FetchSessionManager — at wire level we round-trip
// session_id / session_epoch but the broker only ever returns session_id=0
// (full fetch).

void FetchRequest::encode(Buffer& buffer, int16_t api_version) const {
    const bool flex = api_version >= 12;
    buffer.writeInt32(replica_id_);
    buffer.writeInt32(max_wait_ms_);
    buffer.writeInt32(min_bytes_);

    if (api_version >= 3) buffer.writeInt32(max_bytes_);
    if (api_version >= 4) buffer.writeInt8(isolation_level_);
    if (api_version >= 7) {
        buffer.writeInt32(session_id_);
        buffer.writeInt32(session_epoch_);
    }

    // Topics
    const bool use_topic_id = api_version >= 13;
    if (flex) buffer.writeCompactArrayLen(static_cast<int32_t>(topics_.size()));
    else buffer.writeInt32(static_cast<int32_t>(topics_.size()));
    for (const auto& topic : topics_) {
        // Phase 1.4: v13 replaces topic_name STRING with topic_id UUID.
        if (use_topic_id) {
            buffer.writeBytes(std::vector<uint8_t>(topic.topic_id.begin(),
                                                  topic.topic_id.end()));
        } else if (flex) {
            buffer.writeCompactString(topic.topic);
        } else {
            buffer.writeString(topic.topic);
        }
        if (flex) buffer.writeCompactArrayLen(static_cast<int32_t>(topic.partitions.size()));
        else buffer.writeInt32(static_cast<int32_t>(topic.partitions.size()));
        for (const auto& partition : topic.partitions) {
            buffer.writeInt32(partition.partition);
            if (api_version >= 9) buffer.writeInt32(partition.current_leader_epoch);
            buffer.writeInt64(partition.fetch_offset);
            // Phase 1.4: v12 added LastFetchedEpoch (INT32) here, between
            // FetchOffset and LogStartOffset. Decoder accepts and ignores
            // for now (full session manager will use this for KIP-595
            // epoch validation).
            if (api_version >= 12) buffer.writeInt32(partition.last_fetched_epoch);
            if (api_version >= 5) buffer.writeInt64(partition.log_start_offset);
            buffer.writeInt32(partition.partition_max_bytes);
            if (flex) buffer.writeEmptyTaggedFields();
        }
        if (flex) buffer.writeEmptyTaggedFields();
    }

    if (api_version >= 7) {
        // forgotten_topics_data
        if (flex) buffer.writeCompactArrayLen(static_cast<int32_t>(forgotten_topics_.size()));
        else buffer.writeInt32(static_cast<int32_t>(forgotten_topics_.size()));
        for (const auto& ft : forgotten_topics_) {
            if (flex) buffer.writeCompactString(ft.topic);
            else buffer.writeString(ft.topic);
            if (flex) buffer.writeCompactArrayLen(static_cast<int32_t>(ft.partitions.size()));
            else buffer.writeInt32(static_cast<int32_t>(ft.partitions.size()));
            for (int32_t p : ft.partitions) buffer.writeInt32(p);
            if (flex) buffer.writeEmptyTaggedFields();
        }
    }

    if (api_version >= 11) {
        if (flex) buffer.writeCompactString(rack_id_);
        else buffer.writeString(rack_id_);
    }
    if (flex) buffer.writeEmptyTaggedFields();
}

void FetchRequest::decode(Buffer& buffer, int16_t api_version) {
    const bool flex = api_version >= 12;
    replica_id_ = buffer.readInt32();
    max_wait_ms_ = buffer.readInt32();
    min_bytes_ = buffer.readInt32();
    if (api_version >= 3) max_bytes_ = buffer.readInt32();
    if (api_version >= 4) isolation_level_ = buffer.readInt8();
    if (api_version >= 7) {
        session_id_ = buffer.readInt32();
        session_epoch_ = buffer.readInt32();
    }

    const bool use_topic_id = api_version >= 13;
    int32_t topic_count = flex ? buffer.readCompactArrayLen() : buffer.readInt32();
    topics_.clear();
    topics_.resize(topic_count < 0 ? 0 : topic_count);
    for (int32_t i = 0; i < topic_count; ++i) {
        if (use_topic_id) {
            const auto uuid = buffer.readBytes(16);
            std::copy(uuid.begin(), uuid.end(), topics_[i].topic_id.begin());
            topics_[i].has_topic_id = true;
        } else {
            topics_[i].topic = flex ? buffer.readCompactString() : buffer.readString();
        }
        int32_t partition_count = flex ? buffer.readCompactArrayLen() : buffer.readInt32();
        topics_[i].partitions.resize(partition_count < 0 ? 0 : partition_count);
        for (int32_t j = 0; j < partition_count; ++j) {
            topics_[i].partitions[j].partition = buffer.readInt32();
            if (api_version >= 9) {
                topics_[i].partitions[j].current_leader_epoch = buffer.readInt32();
            }
            topics_[i].partitions[j].fetch_offset = buffer.readInt64();
            // Phase 1.4: v12 LastFetchedEpoch.
            if (api_version >= 12) {
                topics_[i].partitions[j].last_fetched_epoch = buffer.readInt32();
            }
            if (api_version >= 5) {
                topics_[i].partitions[j].log_start_offset = buffer.readInt64();
            }
            topics_[i].partitions[j].partition_max_bytes = buffer.readInt32();
            if (flex) buffer.skipTaggedFields();
        }
        if (flex) buffer.skipTaggedFields();
    }

    if (api_version >= 7) {
        int32_t fc = flex ? buffer.readCompactArrayLen() : buffer.readInt32();
        forgotten_topics_.clear();
        forgotten_topics_.resize(fc < 0 ? 0 : fc);
        for (int32_t i = 0; i < fc; ++i) {
            forgotten_topics_[i].topic = flex ? buffer.readCompactString() : buffer.readString();
            int32_t pc = flex ? buffer.readCompactArrayLen() : buffer.readInt32();
            forgotten_topics_[i].partitions.resize(pc < 0 ? 0 : pc);
            for (int32_t j = 0; j < pc; ++j) {
                forgotten_topics_[i].partitions[j] = buffer.readInt32();
            }
            if (flex) buffer.skipTaggedFields();
        }
    }

    if (api_version >= 11) {
        rack_id_ = flex ? buffer.readCompactString() : buffer.readString();
    }
    if (flex) buffer.skipTaggedFields();
}

size_t FetchRequest::size(int16_t api_version) const {
    size_t result = sizeof(int32_t) * 3;  // replica_id, max_wait_ms, min_bytes
    if (api_version >= 3) result += sizeof(int32_t);
    if (api_version >= 4) result += sizeof(int8_t);
    if (api_version >= 7) result += sizeof(int32_t) * 2;
    result += sizeof(int32_t);  // topic count
    for (const auto& topic : topics_) {
        result += sizeof(int16_t) + topic.topic.size() + sizeof(int32_t);
        for ([[maybe_unused]] const auto& partition : topic.partitions) {
            result += sizeof(int32_t);
            if (api_version >= 9) result += sizeof(int32_t);
            result += sizeof(int64_t);
            if (api_version >= 5) result += sizeof(int64_t);
            result += sizeof(int32_t);
        }
    }
    if (api_version >= 7) {
        result += sizeof(int32_t);
        for (const auto& ft : forgotten_topics_) {
            result += sizeof(int16_t) + ft.topic.size() + sizeof(int32_t);
            result += sizeof(int32_t) * ft.partitions.size();
        }
    }
    if (api_version >= 11) result += sizeof(int16_t) + rack_id_.size();
    return result;
}

void FetchResponse::encode(Buffer& buffer, int16_t api_version) const {
    const bool flex = api_version >= 12;
    if (api_version >= 1) buffer.writeInt32(throttle_time_ms_);
    if (api_version >= 7) {
        buffer.writeInt16(static_cast<int16_t>(error_code_));
        buffer.writeInt32(session_id_);
    }

    const bool use_topic_id = api_version >= 13;
    if (flex) buffer.writeCompactArrayLen(static_cast<int32_t>(topics_.size()));
    else buffer.writeInt32(static_cast<int32_t>(topics_.size()));
    for (const auto& topic : topics_) {
        // Phase 1.4: v13 response echoes topic_id instead of name.
        if (use_topic_id) {
            buffer.writeBytes(std::vector<uint8_t>(topic.topic_id.begin(),
                                                  topic.topic_id.end()));
        } else if (flex) {
            buffer.writeCompactString(topic.topic);
        } else {
            buffer.writeString(topic.topic);
        }
        if (flex) buffer.writeCompactArrayLen(static_cast<int32_t>(topic.partitions.size()));
        else buffer.writeInt32(static_cast<int32_t>(topic.partitions.size()));
        for (const auto& partition : topic.partitions) {
            buffer.writeInt32(partition.partition);
            buffer.writeInt16(static_cast<int16_t>(partition.error_code));
            buffer.writeInt64(partition.high_watermark);
            if (api_version >= 4) buffer.writeInt64(partition.last_stable_offset);
            if (api_version >= 5) buffer.writeInt64(partition.log_start_offset);
            if (api_version >= 4) {
                // aborted_transactions
                if (flex) {
                    if (partition.aborted_transactions.empty()) {
                        buffer.writeUnsignedVarInt(0);  // null compact array
                    } else {
                        buffer.writeCompactArrayLen(static_cast<int32_t>(partition.aborted_transactions.size()));
                        for (const auto& at : partition.aborted_transactions) {
                            buffer.writeInt64(at.producer_id);
                            buffer.writeInt64(at.first_offset);
                            buffer.writeEmptyTaggedFields();
                        }
                    }
                } else {
                    if (partition.aborted_transactions.empty()) {
                        buffer.writeInt32(-1);  // null array sentinel
                    } else {
                        buffer.writeInt32(static_cast<int32_t>(partition.aborted_transactions.size()));
                        for (const auto& at : partition.aborted_transactions) {
                            buffer.writeInt64(at.producer_id);
                            buffer.writeInt64(at.first_offset);
                        }
                    }
                }
            }
            if (api_version >= 11) buffer.writeInt32(partition.preferred_read_replica);

            if (flex) {
                buffer.writeCompactArrayLen(static_cast<int32_t>(partition.record_batches.size()));
            } else {
                buffer.writeInt32(static_cast<int32_t>(partition.record_batches.size()));
            }
            buffer.writeBytes(partition.record_batches.data(),
                              partition.record_batches.size());
            if (flex) buffer.writeEmptyTaggedFields();
        }
        if (flex) buffer.writeEmptyTaggedFields();
    }
    if (flex) buffer.writeEmptyTaggedFields();
}

void FetchResponse::decode(Buffer& buffer, int16_t api_version) {
    const bool flex = api_version >= 12;
    if (api_version >= 1) throttle_time_ms_ = buffer.readInt32();
    if (api_version >= 7) {
        error_code_ = static_cast<ErrorCode>(buffer.readInt16());
        session_id_ = buffer.readInt32();
    }

    const bool use_topic_id_resp = api_version >= 13;
    int32_t topic_count = flex ? buffer.readCompactArrayLen() : buffer.readInt32();
    topics_.resize(topic_count < 0 ? 0 : topic_count);
    for (int32_t i = 0; i < topic_count; ++i) {
        if (use_topic_id_resp) {
            const auto uuid = buffer.readBytes(16);
            std::copy(uuid.begin(), uuid.end(), topics_[i].topic_id.begin());
        } else {
            topics_[i].topic = flex ? buffer.readCompactString() : buffer.readString();
        }
        int32_t partition_count = flex ? buffer.readCompactArrayLen() : buffer.readInt32();
        topics_[i].partitions.resize(partition_count < 0 ? 0 : partition_count);
        for (int32_t j = 0; j < partition_count; ++j) {
            auto& partition = topics_[i].partitions[j];
            partition.partition = buffer.readInt32();
            partition.error_code = static_cast<ErrorCode>(buffer.readInt16());
            partition.high_watermark = buffer.readInt64();
            if (api_version >= 4) partition.last_stable_offset = buffer.readInt64();
            if (api_version >= 5) partition.log_start_offset = buffer.readInt64();
            if (api_version >= 4) {
                int32_t at_count = flex ? buffer.readCompactArrayLen() : buffer.readInt32();
                if (at_count > 0) {
                    partition.aborted_transactions.resize(at_count);
                    for (int32_t k = 0; k < at_count; ++k) {
                        partition.aborted_transactions[k].producer_id = buffer.readInt64();
                        partition.aborted_transactions[k].first_offset = buffer.readInt64();
                        if (flex) buffer.skipTaggedFields();
                    }
                }
            }
            if (api_version >= 11) partition.preferred_read_replica = buffer.readInt32();

            int32_t record_batch_size = flex ? buffer.readCompactArrayLen() : buffer.readInt32();
            partition.record_batches = buffer.readBytes(record_batch_size);
            if (flex) buffer.skipTaggedFields();
        }
        if (flex) buffer.skipTaggedFields();
    }
    if (flex) buffer.skipTaggedFields();
}

size_t FetchResponse::size(int16_t api_version) const {
    size_t result = sizeof(int32_t);
    if (api_version >= 1) result += sizeof(int32_t);
    if (api_version >= 7) result += sizeof(int16_t) + sizeof(int32_t);
    for (const auto& topic : topics_) {
        result += sizeof(int16_t) + topic.topic.size() + sizeof(int32_t);
        for (const auto& partition : topic.partitions) {
            result += sizeof(int32_t) + sizeof(int16_t) + sizeof(int64_t);
            if (api_version >= 4) result += sizeof(int64_t);
            if (api_version >= 5) result += sizeof(int64_t);
            if (api_version >= 4) {
                result += sizeof(int32_t);
                result += sizeof(int64_t) * 2 * partition.aborted_transactions.size();
            }
            if (api_version >= 11) result += sizeof(int32_t);
            result += sizeof(int32_t) + partition.record_batches.size();
        }
    }
    return result;
}

}  // namespace kawasan::protocol
