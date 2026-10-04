#include "kawasan/protocol/offset_fetch_request.h"

#include "kawasan/common/error.h"

namespace kawasan::protocol {

void OffsetFetchRequest::encode(Buffer& buffer, int16_t api_version) const {
    if (api_version < 0 || api_version > 9) {
        throw ProtocolException("Unsupported OffsetFetch request version");
    }
    const bool flex = api_version >= 6;

    // Phase 1.12: v8 multi-group form.
    if (api_version >= 8) {
        buffer.writeCompactArrayLen(static_cast<int32_t>(groups_.size()));
        for (const auto& g : groups_) {
            buffer.writeCompactString(g.group_id);
            if (api_version >= 9) {
                buffer.writeCompactNullableString(g.member_id);
                buffer.writeInt32(g.member_epoch);
            }
            if (g.fetch_all_topics) {
                buffer.writeUnsignedVarInt(0);  // null array
            } else {
                buffer.writeCompactArrayLen(static_cast<int32_t>(g.topics.size()));
                for (const auto& topic : g.topics) {
                    buffer.writeCompactString(topic.topic);
                    buffer.writeCompactArrayLen(static_cast<int32_t>(topic.partitions.size()));
                    for (const auto& p : topic.partitions) {
                        buffer.writeInt32(p.partition);
                    }
                    buffer.writeEmptyTaggedFields();
                }
            }
            buffer.writeEmptyTaggedFields();
        }
        buffer.writeInt8(require_stable_ ? 1 : 0);
        buffer.writeEmptyTaggedFields();
        return;
    }

    if (flex)
        buffer.writeCompactString(group_id_);
    else
        buffer.writeString(group_id_);

    // v2+ allows null topics to mean "all topics".
    if (fetch_all_topics_ && api_version >= 2) {
        if (flex) {
            buffer.writeUnsignedVarInt(0);  // compact null array
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
            if (flex)
                buffer.writeCompactString(topic.topic);
            else
                buffer.writeString(topic.topic);
            if (flex) {
                buffer.writeCompactArrayLen(static_cast<int32_t>(topic.partitions.size()));
            } else {
                buffer.writeInt32(static_cast<int32_t>(topic.partitions.size()));
            }
            for (const auto& p : topic.partitions) {
                buffer.writeInt32(p.partition);
            }
            if (flex)
                buffer.writeEmptyTaggedFields();
        }
    }

    if (api_version >= 7) {
        buffer.writeInt8(require_stable_ ? 1 : 0);
    }
    if (flex)
        buffer.writeEmptyTaggedFields();
}

void OffsetFetchRequest::decode(Buffer& buffer, int16_t api_version) {
    if (api_version < 0 || api_version > 9) {
        throw ProtocolException("Unsupported OffsetFetch request version");
    }
    const bool flex = api_version >= 6;

    // Phase 1.12: v8 multi-group form. The wire shape replaces the
    // top-level (group_id, topics) with a `groups` array whose entries
    // mirror the v0–v7 fields. We decode into `groups_` and also mirror
    // the first group into the legacy fields so the existing handler
    // logic continues to work for single-group requests.
    if (api_version >= 8) {
        const int32_t group_count = buffer.readArrayLength(true);
        groups_.clear();
        groups_.reserve(group_count < 0 ? 0 : group_count);
        for (int32_t gi = 0; gi < group_count; ++gi) {
            Group g;
            g.group_id = buffer.readCompactString();
            if (api_version >= 9) {
                g.member_id = buffer.readCompactNullableString();
                g.member_epoch = buffer.readInt32();
            }
            const uint32_t raw = buffer.readUnsignedVarInt();
            if (raw == 0) {
                g.fetch_all_topics = true;
            } else {
                if (raw - 1 > buffer.remaining()) {
                    throw ProtocolException("OffsetFetch topic count exceeds remaining bytes");
                }
                const int32_t tc = static_cast<int32_t>(raw - 1);
                for (int32_t i = 0; i < tc; ++i) {
                    Topic t;
                    t.topic = buffer.readCompactString();
                    int32_t pc = buffer.readArrayLength(true);
                    for (int32_t j = 0; j < pc; ++j) {
                        Partition p;
                        p.partition = buffer.readInt32();
                        t.partitions.push_back(p);
                    }
                    buffer.skipTaggedFields();
                    g.topics.push_back(std::move(t));
                }
            }
            buffer.skipTaggedFields();
            groups_.push_back(std::move(g));
        }
        require_stable_ = (buffer.readInt8() != 0);
        buffer.skipTaggedFields();
        // Mirror first group into single-group fields.
        if (!groups_.empty()) {
            group_id_ = groups_.front().group_id;
            fetch_all_topics_ = groups_.front().fetch_all_topics;
            topics_ = groups_.front().topics;
        }
        return;
    }

    group_id_ = flex ? buffer.readCompactString() : buffer.readString();

    topics_.clear();
    fetch_all_topics_ = false;
    if (flex) {
        const uint32_t raw = buffer.readUnsignedVarInt();
        if (raw == 0) {
            // null array — fetch all topics (v2+ only; pre-v2 this would be an error)
            fetch_all_topics_ = (api_version >= 2);
        } else {
            if (raw - 1 > buffer.remaining()) {
                throw ProtocolException("OffsetFetch topic count exceeds remaining bytes");
            }
            const int32_t topic_count = static_cast<int32_t>(raw - 1);
            topics_.reserve(topic_count);
            for (int32_t i = 0; i < topic_count; ++i) {
                Topic t;
                t.topic = buffer.readCompactString();
                int32_t pc = buffer.readArrayLength(true);
                t.partitions.reserve(pc < 0 ? 0 : pc);
                for (int32_t j = 0; j < pc; ++j) {
                    Partition p;
                    p.partition = buffer.readInt32();
                    t.partitions.push_back(p);
                }
                buffer.skipTaggedFields();
                topics_.push_back(std::move(t));
            }
        }
    } else {
        const int32_t topic_count = buffer.readInt32();
        if (topic_count < 0) {
            fetch_all_topics_ = (api_version >= 2);
        } else {
            if (static_cast<size_t>(topic_count) > buffer.remaining()) {
                throw ProtocolException("OffsetFetch topic count exceeds remaining bytes");
            }
            topics_.reserve(topic_count);
            for (int32_t i = 0; i < topic_count; ++i) {
                Topic t;
                t.topic = buffer.readString();
                int32_t pc = buffer.readArrayLength(false);
                t.partitions.reserve(pc < 0 ? 0 : pc);
                for (int32_t j = 0; j < pc; ++j) {
                    Partition p;
                    p.partition = buffer.readInt32();
                    t.partitions.push_back(p);
                }
                topics_.push_back(std::move(t));
            }
        }
    }

    require_stable_ = false;
    if (api_version >= 7) {
        require_stable_ = (buffer.readInt8() != 0);
    }
    if (flex)
        buffer.skipTaggedFields();
}

void OffsetFetchResponse::encode(Buffer& buffer, int16_t api_version) const {
    if (api_version < 0 || api_version > 9) {
        throw ProtocolException("Unsupported OffsetFetch response version");
    }
    const bool flex = api_version >= 6;

    // Phase 1.12: v8 multi-group response.
    if (api_version >= 8) {
        buffer.writeInt32(throttle_time_ms_);
        buffer.writeCompactArrayLen(static_cast<int32_t>(groups_.size()));
        for (const auto& g : groups_) {
            buffer.writeCompactString(g.group_id);
            buffer.writeCompactArrayLen(static_cast<int32_t>(g.topics.size()));
            for (const auto& topic : g.topics) {
                buffer.writeCompactString(topic.topic);
                buffer.writeCompactArrayLen(static_cast<int32_t>(topic.partitions.size()));
                for (const auto& p : topic.partitions) {
                    buffer.writeInt32(p.partition);
                    buffer.writeInt64(p.offset);
                    buffer.writeInt32(p.committed_leader_epoch);
                    const auto meta_opt = p.metadata.empty()
                                              ? std::optional<std::string>{}
                                              : std::optional<std::string>(p.metadata);
                    buffer.writeCompactNullableString(meta_opt);
                    buffer.writeInt16(static_cast<int16_t>(p.error));
                    buffer.writeEmptyTaggedFields();
                }
                buffer.writeEmptyTaggedFields();
            }
            buffer.writeInt16(static_cast<int16_t>(g.error_code));
            buffer.writeEmptyTaggedFields();
        }
        buffer.writeEmptyTaggedFields();
        return;
    }

    if (api_version >= 3) {
        buffer.writeInt32(throttle_time_ms_);
    }
    if (flex) {
        buffer.writeCompactArrayLen(static_cast<int32_t>(topics_.size()));
    } else {
        buffer.writeInt32(static_cast<int32_t>(topics_.size()));
    }
    for (const auto& topic : topics_) {
        if (flex)
            buffer.writeCompactString(topic.topic);
        else
            buffer.writeString(topic.topic);
        if (flex) {
            buffer.writeCompactArrayLen(static_cast<int32_t>(topic.partitions.size()));
        } else {
            buffer.writeInt32(static_cast<int32_t>(topic.partitions.size()));
        }
        for (const auto& p : topic.partitions) {
            buffer.writeInt32(p.partition);
            buffer.writeInt64(p.offset);
            if (api_version >= 5) {
                buffer.writeInt32(p.committed_leader_epoch);
            }
            const auto meta_opt = p.metadata.empty() ? std::optional<std::string>{}
                                                     : std::optional<std::string>(p.metadata);
            if (flex)
                buffer.writeCompactNullableString(meta_opt);
            else
                buffer.writeNullableString(meta_opt);
            buffer.writeInt16(static_cast<int16_t>(p.error));
            if (flex)
                buffer.writeEmptyTaggedFields();
        }
        if (flex)
            buffer.writeEmptyTaggedFields();
    }
    // v2+ moves top-level error_code to AFTER topics in the response.
    if (api_version >= 2) {
        buffer.writeInt16(static_cast<int16_t>(error_code_));
    }
    if (flex)
        buffer.writeEmptyTaggedFields();
}

void OffsetFetchResponse::decode(Buffer& buffer, int16_t api_version) {
    if (api_version < 0 || api_version > 9) {
        throw ProtocolException("Unsupported OffsetFetch response version");
    }
    const bool flex = api_version >= 6;

    // v8/v9 multi-group response.
    groups_.clear();
    if (api_version >= 8) {
        throttle_time_ms_ = buffer.readInt32();
        const int32_t gc = buffer.readArrayLength(true);
        for (int32_t gi = 0; gi < gc; ++gi) {
            Group g;
            g.group_id = buffer.readCompactString();
            int32_t tc = buffer.readArrayLength(true);
            for (int32_t i = 0; i < tc; ++i) {
                Topic t;
                t.topic = buffer.readCompactString();
                int32_t pc = buffer.readArrayLength(true);
                for (int32_t j = 0; j < pc; ++j) {
                    Partition p;
                    p.partition = buffer.readInt32();
                    p.offset = buffer.readInt64();
                    p.committed_leader_epoch = buffer.readInt32();
                    auto meta = buffer.readCompactNullableString();
                    p.metadata = meta.value_or("");
                    p.error = static_cast<ErrorCode>(buffer.readInt16());
                    buffer.skipTaggedFields();
                    t.partitions.push_back(std::move(p));
                }
                buffer.skipTaggedFields();
                g.topics.push_back(std::move(t));
            }
            g.error_code = static_cast<ErrorCode>(buffer.readInt16());
            buffer.skipTaggedFields();
            groups_.push_back(std::move(g));
        }
        buffer.skipTaggedFields();
        return;
    }

    if (api_version >= 3) {
        throttle_time_ms_ = buffer.readInt32();
    }
    const int32_t topic_count = buffer.readArrayLength(flex);
    topics_.clear();
    topics_.reserve(topic_count < 0 ? 0 : topic_count);
    for (int32_t i = 0; i < topic_count; ++i) {
        Topic t;
        t.topic = flex ? buffer.readCompactString() : buffer.readString();
        int32_t pc = buffer.readArrayLength(flex);
        t.partitions.reserve(pc < 0 ? 0 : pc);
        for (int32_t j = 0; j < pc; ++j) {
            Partition p;
            p.partition = buffer.readInt32();
            p.offset = buffer.readInt64();
            if (api_version >= 5) {
                p.committed_leader_epoch = buffer.readInt32();
            }
            auto meta = flex ? buffer.readCompactNullableString() : buffer.readNullableString();
            p.metadata = meta.value_or("");
            p.error = static_cast<ErrorCode>(buffer.readInt16());
            if (flex)
                buffer.skipTaggedFields();
            t.partitions.push_back(std::move(p));
        }
        if (flex)
            buffer.skipTaggedFields();
        topics_.push_back(std::move(t));
    }
    if (api_version >= 2) {
        error_code_ = static_cast<ErrorCode>(buffer.readInt16());
    }
    if (flex)
        buffer.skipTaggedFields();
}

}  // namespace kawasan::protocol
