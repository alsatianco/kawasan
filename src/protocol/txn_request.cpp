#include "kawasan/protocol/txn_request.h"

#include <optional>

namespace kawasan::protocol {

// Phase 3.3 scaffolding: wire-format for the transactional APIs.
// We support v0 (non-flexible) explicitly and v3+ flexible (compact
// strings + tagged fields) — the version distinction matters because
// Java 3.x clients negotiate based on the broker's advertised max.
// Bodies are deliberately minimal: enough to decode the producer
// identity and the topics/groups the request references.

// ---- AddPartitionsToTxn ----
void AddPartitionsToTxnRequest::encode(Buffer& buf, int16_t v) const {
    const bool flex = v >= 3;
    if (flex) buf.writeCompactString(transactional_id_);
    else buf.writeString(transactional_id_);
    buf.writeInt64(producer_id_);
    buf.writeInt16(producer_epoch_);
    if (flex) buf.writeCompactArrayLen(static_cast<int32_t>(topics_.size()));
    else buf.writeInt32(static_cast<int32_t>(topics_.size()));
    for (const auto& t : topics_) {
        if (flex) buf.writeCompactString(t.topic);
        else buf.writeString(t.topic);
        if (flex) buf.writeCompactArrayLen(static_cast<int32_t>(t.partitions.size()));
        else buf.writeInt32(static_cast<int32_t>(t.partitions.size()));
        for (int32_t p : t.partitions) buf.writeInt32(p);
        if (flex) buf.writeEmptyTaggedFields();
    }
    if (flex) buf.writeEmptyTaggedFields();
}

void AddPartitionsToTxnRequest::decode(Buffer& buf, int16_t v) {
    const bool flex = v >= 3;
    transactional_id_ = flex ? buf.readCompactString() : buf.readString();
    producer_id_ = buf.readInt64();
    producer_epoch_ = buf.readInt16();
    int32_t n = flex ? buf.readCompactArrayLen() : buf.readInt32();
    for (int32_t i = 0; i < n; ++i) {
        PartitionList t;
        t.topic = flex ? buf.readCompactString() : buf.readString();
        int32_t pc = flex ? buf.readCompactArrayLen() : buf.readInt32();
        for (int32_t j = 0; j < pc; ++j) t.partitions.push_back(buf.readInt32());
        if (flex) buf.skipTaggedFields();
        topics_.push_back(std::move(t));
    }
    if (flex) buf.skipTaggedFields();
}

void AddPartitionsToTxnResponse::encode(Buffer& buf, int16_t v) const {
    const bool flex = v >= 3;
    buf.writeInt32(throttle_time_ms_);
    if (flex) buf.writeCompactArrayLen(static_cast<int32_t>(topics_.size()));
    else buf.writeInt32(static_cast<int32_t>(topics_.size()));
    for (const auto& t : topics_) {
        if (flex) buf.writeCompactString(t.topic);
        else buf.writeString(t.topic);
        if (flex) buf.writeCompactArrayLen(static_cast<int32_t>(t.partitions.size()));
        else buf.writeInt32(static_cast<int32_t>(t.partitions.size()));
        for (const auto& p : t.partitions) {
            buf.writeInt32(p.partition);
            buf.writeInt16(static_cast<int16_t>(p.error_code));
            if (flex) buf.writeEmptyTaggedFields();
        }
        if (flex) buf.writeEmptyTaggedFields();
    }
    if (flex) buf.writeEmptyTaggedFields();
}

void AddPartitionsToTxnResponse::decode(Buffer& buf, int16_t v) {
    const bool flex = v >= 3;
    throttle_time_ms_ = buf.readInt32();
    int32_t n = flex ? buf.readCompactArrayLen() : buf.readInt32();
    for (int32_t i = 0; i < n; ++i) {
        TopicResult t;
        t.topic = flex ? buf.readCompactString() : buf.readString();
        int32_t pc = flex ? buf.readCompactArrayLen() : buf.readInt32();
        for (int32_t j = 0; j < pc; ++j) {
            PartitionResult p;
            p.partition = buf.readInt32();
            p.error_code = static_cast<ErrorCode>(buf.readInt16());
            if (flex) buf.skipTaggedFields();
            t.partitions.push_back(p);
        }
        if (flex) buf.skipTaggedFields();
        topics_.push_back(std::move(t));
    }
    if (flex) buf.skipTaggedFields();
}

// ---- AddOffsetsToTxn ----
void AddOffsetsToTxnRequest::encode(Buffer& buf, int16_t v) const {
    const bool flex = v >= 3;
    if (flex) buf.writeCompactString(transactional_id_);
    else buf.writeString(transactional_id_);
    buf.writeInt64(producer_id_);
    buf.writeInt16(producer_epoch_);
    if (flex) buf.writeCompactString(group_id_);
    else buf.writeString(group_id_);
    if (flex) buf.writeEmptyTaggedFields();
}

void AddOffsetsToTxnRequest::decode(Buffer& buf, int16_t v) {
    const bool flex = v >= 3;
    transactional_id_ = flex ? buf.readCompactString() : buf.readString();
    producer_id_ = buf.readInt64();
    producer_epoch_ = buf.readInt16();
    group_id_ = flex ? buf.readCompactString() : buf.readString();
    if (flex) buf.skipTaggedFields();
}

void AddOffsetsToTxnResponse::encode(Buffer& buf, int16_t v) const {
    const bool flex = v >= 3;
    buf.writeInt32(throttle_time_ms_);
    buf.writeInt16(static_cast<int16_t>(error_code_));
    if (flex) buf.writeEmptyTaggedFields();
}

void AddOffsetsToTxnResponse::decode(Buffer& buf, int16_t v) {
    const bool flex = v >= 3;
    throttle_time_ms_ = buf.readInt32();
    error_code_ = static_cast<ErrorCode>(buf.readInt16());
    if (flex) buf.skipTaggedFields();
}

// ---- EndTxn ----
void EndTxnRequest::encode(Buffer& buf, int16_t v) const {
    const bool flex = v >= 3;
    if (flex) buf.writeCompactString(transactional_id_);
    else buf.writeString(transactional_id_);
    buf.writeInt64(producer_id_);
    buf.writeInt16(producer_epoch_);
    buf.writeInt8(committed_ ? 1 : 0);
    if (flex) buf.writeEmptyTaggedFields();
}

void EndTxnRequest::decode(Buffer& buf, int16_t v) {
    const bool flex = v >= 3;
    transactional_id_ = flex ? buf.readCompactString() : buf.readString();
    producer_id_ = buf.readInt64();
    producer_epoch_ = buf.readInt16();
    committed_ = (buf.readInt8() != 0);
    if (flex) buf.skipTaggedFields();
}

void EndTxnResponse::encode(Buffer& buf, int16_t v) const {
    const bool flex = v >= 3;
    buf.writeInt32(throttle_time_ms_);
    buf.writeInt16(static_cast<int16_t>(error_code_));
    if (flex) buf.writeEmptyTaggedFields();
}

void EndTxnResponse::decode(Buffer& buf, int16_t v) {
    const bool flex = v >= 3;
    throttle_time_ms_ = buf.readInt32();
    error_code_ = static_cast<ErrorCode>(buf.readInt16());
    if (flex) buf.skipTaggedFields();
}

// ---- TxnOffsetCommit ----
void TxnOffsetCommitRequest::encode(Buffer& buf, int16_t v) const {
    const bool flex = v >= 3;
    if (flex) buf.writeCompactString(transactional_id_);
    else buf.writeString(transactional_id_);
    if (flex) buf.writeCompactString(group_id_);
    else buf.writeString(group_id_);
    buf.writeInt64(producer_id_);
    buf.writeInt16(producer_epoch_);
    if (flex) buf.writeCompactArrayLen(static_cast<int32_t>(topics_.size()));
    else buf.writeInt32(static_cast<int32_t>(topics_.size()));
    for (const auto& t : topics_) {
        if (flex) buf.writeCompactString(t.topic);
        else buf.writeString(t.topic);
        if (flex) buf.writeCompactArrayLen(static_cast<int32_t>(t.partitions.size()));
        else buf.writeInt32(static_cast<int32_t>(t.partitions.size()));
        for (const auto& p : t.partitions) {
            buf.writeInt32(p.partition);
            buf.writeInt64(p.offset);
            const auto meta_opt = p.metadata.empty()
                                      ? std::optional<std::string>{}
                                      : std::optional<std::string>(p.metadata);
            if (flex) buf.writeCompactNullableString(meta_opt);
            else buf.writeNullableString(meta_opt);
            if (flex) buf.writeEmptyTaggedFields();
        }
        if (flex) buf.writeEmptyTaggedFields();
    }
    if (flex) buf.writeEmptyTaggedFields();
}

void TxnOffsetCommitRequest::decode(Buffer& buf, int16_t v) {
    const bool flex = v >= 3;
    transactional_id_ = flex ? buf.readCompactString() : buf.readString();
    group_id_ = flex ? buf.readCompactString() : buf.readString();
    producer_id_ = buf.readInt64();
    producer_epoch_ = buf.readInt16();
    int32_t n = flex ? buf.readCompactArrayLen() : buf.readInt32();
    for (int32_t i = 0; i < n; ++i) {
        TopicOffsets t;
        t.topic = flex ? buf.readCompactString() : buf.readString();
        int32_t pc = flex ? buf.readCompactArrayLen() : buf.readInt32();
        for (int32_t j = 0; j < pc; ++j) {
            PartitionOffset p;
            p.partition = buf.readInt32();
            p.offset = buf.readInt64();
            auto meta = flex ? buf.readCompactNullableString() : buf.readNullableString();
            p.metadata = meta.value_or("");
            if (flex) buf.skipTaggedFields();
            t.partitions.push_back(p);
        }
        if (flex) buf.skipTaggedFields();
        topics_.push_back(std::move(t));
    }
    if (flex) buf.skipTaggedFields();
}

void TxnOffsetCommitResponse::encode(Buffer& buf, int16_t v) const {
    const bool flex = v >= 3;
    buf.writeInt32(throttle_time_ms_);
    if (flex) buf.writeCompactArrayLen(static_cast<int32_t>(topics_.size()));
    else buf.writeInt32(static_cast<int32_t>(topics_.size()));
    for (const auto& t : topics_) {
        if (flex) buf.writeCompactString(t.topic);
        else buf.writeString(t.topic);
        if (flex) buf.writeCompactArrayLen(static_cast<int32_t>(t.partitions.size()));
        else buf.writeInt32(static_cast<int32_t>(t.partitions.size()));
        for (const auto& p : t.partitions) {
            buf.writeInt32(p.partition);
            buf.writeInt16(static_cast<int16_t>(p.error_code));
            if (flex) buf.writeEmptyTaggedFields();
        }
        if (flex) buf.writeEmptyTaggedFields();
    }
    if (flex) buf.writeEmptyTaggedFields();
}

void TxnOffsetCommitResponse::decode(Buffer& buf, int16_t v) {
    const bool flex = v >= 3;
    throttle_time_ms_ = buf.readInt32();
    int32_t n = flex ? buf.readCompactArrayLen() : buf.readInt32();
    for (int32_t i = 0; i < n; ++i) {
        TopicResult t;
        t.topic = flex ? buf.readCompactString() : buf.readString();
        int32_t pc = flex ? buf.readCompactArrayLen() : buf.readInt32();
        for (int32_t j = 0; j < pc; ++j) {
            PartitionResult p;
            p.partition = buf.readInt32();
            p.error_code = static_cast<ErrorCode>(buf.readInt16());
            if (flex) buf.skipTaggedFields();
            t.partitions.push_back(p);
        }
        if (flex) buf.skipTaggedFields();
        topics_.push_back(std::move(t));
    }
    if (flex) buf.skipTaggedFields();
}

}  // namespace kawasan::protocol
