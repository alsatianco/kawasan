#include "kawasan/protocol/admin_stubs.h"

namespace kawasan::protocol {

// ---- DescribeProducers (61) ----
void DescribeProducersRequest::encode(Buffer& buf, int16_t /*v*/) const {
    buf.writeInt32(static_cast<int32_t>(topics_.size()));
    for (const auto& t : topics_) {
        buf.writeString(t.topic);
        buf.writeInt32(static_cast<int32_t>(t.partitions.size()));
        for (int32_t p : t.partitions) buf.writeInt32(p);
    }
}
void DescribeProducersRequest::decode(Buffer& buf, int16_t /*v*/) {
    int32_t tc = buf.readInt32();
    topics_.resize(tc < 0 ? 0 : tc);
    for (int32_t i = 0; i < tc; ++i) {
        topics_[i].topic = buf.readString();
        int32_t pc = buf.readInt32();
        topics_[i].partitions.resize(pc < 0 ? 0 : pc);
        for (int32_t j = 0; j < pc; ++j) topics_[i].partitions[j] = buf.readInt32();
    }
}
void DescribeProducersResponse::encode(Buffer& buf, int16_t /*v*/) const {
    buf.writeInt32(throttle_time_ms_);
    buf.writeInt32(static_cast<int32_t>(topics_.size()));
    for (const auto& t : topics_) {
        buf.writeString(t.topic);
        buf.writeInt32(static_cast<int32_t>(t.partitions.size()));
        for (const auto& p : t.partitions) {
            buf.writeInt32(p.partition);
            buf.writeInt16(static_cast<int16_t>(p.error_code));
            const auto msg = p.error_message.empty()
                                 ? std::optional<std::string>{}
                                 : std::optional<std::string>(p.error_message);
            buf.writeNullableString(msg);
            buf.writeInt32(static_cast<int32_t>(p.active_producers.size()));
            for (const auto& ap : p.active_producers) {
                buf.writeInt64(ap.producer_id);
                buf.writeInt32(ap.producer_epoch);
                buf.writeInt32(ap.last_sequence);
                buf.writeInt64(ap.last_timestamp);
                buf.writeInt32(ap.coordinator_epoch);
                buf.writeInt64(ap.current_txn_start_offset);
            }
        }
    }
}
void DescribeProducersResponse::decode(Buffer& buf, int16_t /*v*/) {
    throttle_time_ms_ = buf.readInt32();
    int32_t tc = buf.readInt32();
    topics_.resize(tc < 0 ? 0 : tc);
    for (int32_t i = 0; i < tc; ++i) {
        topics_[i].topic = buf.readString();
        int32_t pc = buf.readInt32();
        topics_[i].partitions.resize(pc < 0 ? 0 : pc);
        for (int32_t j = 0; j < pc; ++j) {
            topics_[i].partitions[j].partition = buf.readInt32();
            topics_[i].partitions[j].error_code =
                static_cast<ErrorCode>(buf.readInt16());
            auto msg = buf.readNullableString();
            topics_[i].partitions[j].error_message = msg.value_or("");
            int32_t ac = buf.readInt32();
            topics_[i].partitions[j].active_producers.resize(ac < 0 ? 0 : ac);
            for (int32_t k = 0; k < ac; ++k) {
                auto& ap = topics_[i].partitions[j].active_producers[k];
                ap.producer_id = buf.readInt64();
                ap.producer_epoch = buf.readInt32();
                ap.last_sequence = buf.readInt32();
                ap.last_timestamp = buf.readInt64();
                ap.coordinator_epoch = buf.readInt32();
                ap.current_txn_start_offset = buf.readInt64();
            }
        }
    }
}

// ---- ListTransactions (66) ----
void ListTransactionsRequest::encode(Buffer& buf, int16_t /*v*/) const {
    buf.writeInt32(static_cast<int32_t>(state_filters_.size()));
    for (const auto& s : state_filters_) buf.writeString(s);
    buf.writeInt32(static_cast<int32_t>(producer_id_filters_.size()));
    for (int64_t pid : producer_id_filters_) buf.writeInt64(pid);
}
void ListTransactionsRequest::decode(Buffer& buf, int16_t /*v*/) {
    int32_t sc = buf.readInt32();
    state_filters_.resize(sc < 0 ? 0 : sc);
    for (int32_t i = 0; i < sc; ++i) state_filters_[i] = buf.readString();
    int32_t pc = buf.readInt32();
    producer_id_filters_.resize(pc < 0 ? 0 : pc);
    for (int32_t i = 0; i < pc; ++i) producer_id_filters_[i] = buf.readInt64();
}
void ListTransactionsResponse::encode(Buffer& buf, int16_t /*v*/) const {
    buf.writeInt32(throttle_time_ms_);
    buf.writeInt16(static_cast<int16_t>(error_code_));
    buf.writeInt32(static_cast<int32_t>(unknown_state_filters_.size()));
    for (const auto& s : unknown_state_filters_) buf.writeString(s);
    buf.writeInt32(static_cast<int32_t>(states_.size()));
    for (const auto& s : states_) {
        buf.writeString(s.transactional_id);
        buf.writeInt64(s.producer_id);
        buf.writeString(s.state);
    }
}
void ListTransactionsResponse::decode(Buffer& buf, int16_t /*v*/) {
    throttle_time_ms_ = buf.readInt32();
    error_code_ = static_cast<ErrorCode>(buf.readInt16());
    int32_t uc = buf.readInt32();
    unknown_state_filters_.resize(uc < 0 ? 0 : uc);
    for (int32_t i = 0; i < uc; ++i) unknown_state_filters_[i] = buf.readString();
    int32_t sc = buf.readInt32();
    states_.resize(sc < 0 ? 0 : sc);
    for (int32_t i = 0; i < sc; ++i) {
        states_[i].transactional_id = buf.readString();
        states_[i].producer_id = buf.readInt64();
        states_[i].state = buf.readString();
    }
}

// ---- DescribeTransactions (65) ----
void DescribeTransactionsRequest::encode(Buffer& buf, int16_t /*v*/) const {
    buf.writeInt32(static_cast<int32_t>(ids_.size()));
    for (const auto& s : ids_) buf.writeString(s);
}
void DescribeTransactionsRequest::decode(Buffer& buf, int16_t /*v*/) {
    int32_t n = buf.readInt32();
    ids_.resize(n < 0 ? 0 : n);
    for (int32_t i = 0; i < n; ++i) ids_[i] = buf.readString();
}
void DescribeTransactionsResponse::encode(Buffer& buf, int16_t /*v*/) const {
    buf.writeInt32(throttle_time_ms_);
    buf.writeInt32(static_cast<int32_t>(states_.size()));
    for (const auto& s : states_) {
        buf.writeInt16(static_cast<int16_t>(s.error_code));
        buf.writeString(s.transactional_id);
        buf.writeString(s.state);
        buf.writeInt32(s.transaction_timeout_ms);
        buf.writeInt64(s.producer_id);
        buf.writeInt32(s.producer_epoch);
    }
}
void DescribeTransactionsResponse::decode(Buffer& buf, int16_t /*v*/) {
    throttle_time_ms_ = buf.readInt32();
    int32_t n = buf.readInt32();
    states_.resize(n < 0 ? 0 : n);
    for (int32_t i = 0; i < n; ++i) {
        states_[i].error_code = static_cast<ErrorCode>(buf.readInt16());
        states_[i].transactional_id = buf.readString();
        states_[i].state = buf.readString();
        states_[i].transaction_timeout_ms = buf.readInt32();
        states_[i].producer_id = buf.readInt64();
        states_[i].producer_epoch = buf.readInt32();
    }
}

// ---- AlterPartition (56) ----
void AlterPartitionRequest::encode(Buffer& /*buf*/, int16_t /*v*/) const {}
void AlterPartitionRequest::decode(Buffer& /*buf*/, int16_t /*v*/) {}
void AlterPartitionResponse::encode(Buffer& buf, int16_t /*v*/) const {
    buf.writeInt32(throttle_time_ms_);
    buf.writeInt16(static_cast<int16_t>(error_code_));
}
void AlterPartitionResponse::decode(Buffer& buf, int16_t /*v*/) {
    throttle_time_ms_ = buf.readInt32();
    error_code_ = static_cast<ErrorCode>(buf.readInt16());
}

// ---- ACL (Phase 4.2c) ----
// Wire format follows Kafka's DescribeAcls/CreateAcls/DeleteAcls schemas.
// v2+ is flexible (compact strings + tagged fields); v1 added pattern_type.
//
// For simplicity (and because the harness doesn't exercise SASL yet),
// only v0 is fully decoded for incoming requests — that's the version
// kafka-python and most CLIs send when SASL isn't enabled. The wire
// shape at v0:
//   DescribeAclsRequest: resource_type INT8, resource_name NULLABLE_STRING,
//                        principal NULLABLE_STRING, host NULLABLE_STRING,
//                        operation INT8, permission_type INT8
//   v1+: + pattern_type INT8 (after resource_name)

void DescribeAclsRequest::encode(Buffer& buf, int16_t v) const {
    const bool flex = v >= 2;
    buf.writeInt8(resource_type);
    if (flex) {
        buf.writeCompactNullableString(resource_name_filter.empty()
                                           ? std::optional<std::string>{}
                                           : std::optional<std::string>(resource_name_filter));
    } else {
        buf.writeNullableString(resource_name_filter.empty()
                                    ? std::optional<std::string>{}
                                    : std::optional<std::string>(resource_name_filter));
    }
    if (v >= 1) buf.writeInt8(pattern_type);
    if (flex) {
        buf.writeCompactNullableString(principal_filter.empty()
                                            ? std::optional<std::string>{}
                                            : std::optional<std::string>(principal_filter));
        buf.writeCompactNullableString(host_filter.empty()
                                            ? std::optional<std::string>{}
                                            : std::optional<std::string>(host_filter));
    } else {
        buf.writeNullableString(principal_filter.empty()
                                    ? std::optional<std::string>{}
                                    : std::optional<std::string>(principal_filter));
        buf.writeNullableString(host_filter.empty()
                                    ? std::optional<std::string>{}
                                    : std::optional<std::string>(host_filter));
    }
    buf.writeInt8(operation);
    buf.writeInt8(permission_type);
    if (flex) buf.writeEmptyTaggedFields();
}

void DescribeAclsRequest::decode(Buffer& buf, int16_t v) {
    const bool flex = v >= 2;
    resource_type = buf.readInt8();
    resource_name_filter = (flex ? buf.readCompactNullableString()
                                 : buf.readNullableString()).value_or("");
    if (v >= 1) pattern_type = buf.readInt8();
    principal_filter = (flex ? buf.readCompactNullableString()
                             : buf.readNullableString()).value_or("");
    host_filter = (flex ? buf.readCompactNullableString()
                        : buf.readNullableString()).value_or("");
    operation = buf.readInt8();
    permission_type = buf.readInt8();
    if (flex) buf.skipTaggedFields();
}

void DescribeAclsResponse::encode(Buffer& buf, int16_t v) const {
    const bool flex = v >= 2;
    buf.writeInt32(throttle_time_ms_);
    buf.writeInt16(static_cast<int16_t>(error_code_));
    const auto msg_opt = error_message_.empty() ? std::optional<std::string>{}
                                                : std::optional<std::string>(error_message_);
    if (flex) buf.writeCompactNullableString(msg_opt);
    else buf.writeNullableString(msg_opt);
    if (flex) buf.writeCompactArrayLen(static_cast<int32_t>(resources_.size()));
    else buf.writeInt32(static_cast<int32_t>(resources_.size()));
    for (const auto& r : resources_) {
        buf.writeInt8(r.resource_type);
        if (flex) buf.writeCompactString(r.resource_name);
        else buf.writeString(r.resource_name);
        if (v >= 1) buf.writeInt8(r.pattern_type);
        if (flex) buf.writeCompactArrayLen(static_cast<int32_t>(r.acls.size()));
        else buf.writeInt32(static_cast<int32_t>(r.acls.size()));
        for (const auto& a : r.acls) {
            if (flex) buf.writeCompactString(a.principal);
            else buf.writeString(a.principal);
            if (flex) buf.writeCompactString(a.host);
            else buf.writeString(a.host);
            buf.writeInt8(a.operation);
            buf.writeInt8(a.permission_type);
            if (flex) buf.writeEmptyTaggedFields();
        }
        if (flex) buf.writeEmptyTaggedFields();
    }
    if (flex) buf.writeEmptyTaggedFields();
}

void DescribeAclsResponse::decode(Buffer& buf, int16_t v) {
    const bool flex = v >= 2;
    throttle_time_ms_ = buf.readInt32();
    error_code_ = static_cast<ErrorCode>(buf.readInt16());
    error_message_ = (flex ? buf.readCompactNullableString()
                            : buf.readNullableString()).value_or("");
    int32_t n = flex ? buf.readCompactArrayLen() : buf.readInt32();
    for (int32_t i = 0; i < n; ++i) {
        Resource r;
        r.resource_type = buf.readInt8();
        r.resource_name = flex ? buf.readCompactString() : buf.readString();
        if (v >= 1) r.pattern_type = buf.readInt8();
        int32_t na = flex ? buf.readCompactArrayLen() : buf.readInt32();
        for (int32_t j = 0; j < na; ++j) {
            Resource::Acl a;
            a.principal = flex ? buf.readCompactString() : buf.readString();
            a.host = flex ? buf.readCompactString() : buf.readString();
            a.operation = buf.readInt8();
            a.permission_type = buf.readInt8();
            if (flex) buf.skipTaggedFields();
            r.acls.push_back(std::move(a));
        }
        if (flex) buf.skipTaggedFields();
        resources_.push_back(std::move(r));
    }
    if (flex) buf.skipTaggedFields();
}

void CreateAclsRequest::encode(Buffer& buf, int16_t v) const {
    const bool flex = v >= 2;
    if (flex) buf.writeCompactArrayLen(static_cast<int32_t>(creations.size()));
    else buf.writeInt32(static_cast<int32_t>(creations.size()));
    for (const auto& b : creations) {
        buf.writeInt8(b.resource_type);
        if (flex) buf.writeCompactString(b.resource_name);
        else buf.writeString(b.resource_name);
        if (v >= 1) buf.writeInt8(b.pattern_type);
        if (flex) buf.writeCompactString(b.principal);
        else buf.writeString(b.principal);
        if (flex) buf.writeCompactString(b.host);
        else buf.writeString(b.host);
        buf.writeInt8(b.operation);
        buf.writeInt8(b.permission_type);
        if (flex) buf.writeEmptyTaggedFields();
    }
    if (flex) buf.writeEmptyTaggedFields();
}

void CreateAclsRequest::decode(Buffer& buf, int16_t v) {
    const bool flex = v >= 2;
    int32_t n = flex ? buf.readCompactArrayLen() : buf.readInt32();
    for (int32_t i = 0; i < n; ++i) {
        AclBinding b;
        b.resource_type = buf.readInt8();
        b.resource_name = flex ? buf.readCompactString() : buf.readString();
        if (v >= 1) b.pattern_type = buf.readInt8();
        b.principal = flex ? buf.readCompactString() : buf.readString();
        b.host = flex ? buf.readCompactString() : buf.readString();
        b.operation = buf.readInt8();
        b.permission_type = buf.readInt8();
        if (flex) buf.skipTaggedFields();
        creations.push_back(std::move(b));
    }
    if (flex) buf.skipTaggedFields();
}

void CreateAclsResponse::encode(Buffer& buf, int16_t v) const {
    const bool flex = v >= 2;
    buf.writeInt32(throttle_time_ms_);
    if (flex) buf.writeCompactArrayLen(static_cast<int32_t>(results_.size()));
    else buf.writeInt32(static_cast<int32_t>(results_.size()));
    for (const auto& r : results_) {
        buf.writeInt16(static_cast<int16_t>(r.error_code));
        const auto msg_opt = r.error_message.empty() ? std::optional<std::string>{}
                                                     : std::optional<std::string>(r.error_message);
        if (flex) buf.writeCompactNullableString(msg_opt);
        else buf.writeNullableString(msg_opt);
        if (flex) buf.writeEmptyTaggedFields();
    }
    if (flex) buf.writeEmptyTaggedFields();
}

void CreateAclsResponse::decode(Buffer& buf, int16_t v) {
    const bool flex = v >= 2;
    throttle_time_ms_ = buf.readInt32();
    int32_t n = flex ? buf.readCompactArrayLen() : buf.readInt32();
    for (int32_t i = 0; i < n; ++i) {
        Result r;
        r.error_code = static_cast<ErrorCode>(buf.readInt16());
        r.error_message = (flex ? buf.readCompactNullableString()
                                 : buf.readNullableString()).value_or("");
        if (flex) buf.skipTaggedFields();
        results_.push_back(std::move(r));
    }
    if (flex) buf.skipTaggedFields();
}

void DeleteAclsRequest::encode(Buffer& buf, int16_t v) const {
    const bool flex = v >= 2;
    if (flex) buf.writeCompactArrayLen(static_cast<int32_t>(filters.size()));
    else buf.writeInt32(static_cast<int32_t>(filters.size()));
    for (const auto& f : filters) {
        buf.writeInt8(f.resource_type);
        const auto rn = f.resource_name_filter.empty()
                            ? std::optional<std::string>{}
                            : std::optional<std::string>(f.resource_name_filter);
        if (flex) buf.writeCompactNullableString(rn);
        else buf.writeNullableString(rn);
        if (v >= 1) buf.writeInt8(f.pattern_type);
        const auto pp = f.principal_filter.empty()
                            ? std::optional<std::string>{}
                            : std::optional<std::string>(f.principal_filter);
        if (flex) buf.writeCompactNullableString(pp);
        else buf.writeNullableString(pp);
        const auto hp = f.host_filter.empty()
                            ? std::optional<std::string>{}
                            : std::optional<std::string>(f.host_filter);
        if (flex) buf.writeCompactNullableString(hp);
        else buf.writeNullableString(hp);
        buf.writeInt8(f.operation);
        buf.writeInt8(f.permission_type);
        if (flex) buf.writeEmptyTaggedFields();
    }
    if (flex) buf.writeEmptyTaggedFields();
}

void DeleteAclsRequest::decode(Buffer& buf, int16_t v) {
    const bool flex = v >= 2;
    int32_t n = flex ? buf.readCompactArrayLen() : buf.readInt32();
    for (int32_t i = 0; i < n; ++i) {
        Filter f;
        f.resource_type = buf.readInt8();
        f.resource_name_filter = (flex ? buf.readCompactNullableString()
                                       : buf.readNullableString()).value_or("");
        if (v >= 1) f.pattern_type = buf.readInt8();
        f.principal_filter = (flex ? buf.readCompactNullableString()
                                    : buf.readNullableString()).value_or("");
        f.host_filter = (flex ? buf.readCompactNullableString()
                               : buf.readNullableString()).value_or("");
        f.operation = buf.readInt8();
        f.permission_type = buf.readInt8();
        if (flex) buf.skipTaggedFields();
        filters.push_back(std::move(f));
    }
    if (flex) buf.skipTaggedFields();
}

void DeleteAclsResponse::encode(Buffer& buf, int16_t v) const {
    const bool flex = v >= 2;
    buf.writeInt32(throttle_time_ms_);
    if (flex) buf.writeCompactArrayLen(static_cast<int32_t>(filter_results_.size()));
    else buf.writeInt32(static_cast<int32_t>(filter_results_.size()));
    for (const auto& fr : filter_results_) {
        buf.writeInt16(static_cast<int16_t>(fr.error_code));
        const auto msg = fr.error_message.empty() ? std::optional<std::string>{}
                                                  : std::optional<std::string>(fr.error_message);
        if (flex) buf.writeCompactNullableString(msg);
        else buf.writeNullableString(msg);
        if (flex) buf.writeCompactArrayLen(static_cast<int32_t>(fr.matches.size()));
        else buf.writeInt32(static_cast<int32_t>(fr.matches.size()));
        for (const auto& m : fr.matches) {
            buf.writeInt16(static_cast<int16_t>(m.error_code));
            const auto mmsg = m.error_message.empty()
                                  ? std::optional<std::string>{}
                                  : std::optional<std::string>(m.error_message);
            if (flex) buf.writeCompactNullableString(mmsg);
            else buf.writeNullableString(mmsg);
            buf.writeInt8(m.binding.resource_type);
            if (flex) buf.writeCompactString(m.binding.resource_name);
            else buf.writeString(m.binding.resource_name);
            if (v >= 1) buf.writeInt8(m.binding.pattern_type);
            if (flex) buf.writeCompactString(m.binding.principal);
            else buf.writeString(m.binding.principal);
            if (flex) buf.writeCompactString(m.binding.host);
            else buf.writeString(m.binding.host);
            buf.writeInt8(m.binding.operation);
            buf.writeInt8(m.binding.permission_type);
            if (flex) buf.writeEmptyTaggedFields();
        }
        if (flex) buf.writeEmptyTaggedFields();
    }
    if (flex) buf.writeEmptyTaggedFields();
}

void DeleteAclsResponse::decode(Buffer& buf, int16_t v) {
    const bool flex = v >= 2;
    throttle_time_ms_ = buf.readInt32();
    int32_t n = flex ? buf.readCompactArrayLen() : buf.readInt32();
    for (int32_t i = 0; i < n; ++i) {
        FilterResult fr;
        fr.error_code = static_cast<ErrorCode>(buf.readInt16());
        fr.error_message = (flex ? buf.readCompactNullableString()
                                  : buf.readNullableString()).value_or("");
        int32_t nm = flex ? buf.readCompactArrayLen() : buf.readInt32();
        for (int32_t j = 0; j < nm; ++j) {
            MatchingAcl m;
            m.error_code = static_cast<ErrorCode>(buf.readInt16());
            m.error_message = (flex ? buf.readCompactNullableString()
                                     : buf.readNullableString()).value_or("");
            m.binding.resource_type = buf.readInt8();
            m.binding.resource_name = flex ? buf.readCompactString() : buf.readString();
            if (v >= 1) m.binding.pattern_type = buf.readInt8();
            m.binding.principal = flex ? buf.readCompactString() : buf.readString();
            m.binding.host = flex ? buf.readCompactString() : buf.readString();
            m.binding.operation = buf.readInt8();
            m.binding.permission_type = buf.readInt8();
            if (flex) buf.skipTaggedFields();
            fr.matches.push_back(std::move(m));
        }
        if (flex) buf.skipTaggedFields();
        filter_results_.push_back(std::move(fr));
    }
    if (flex) buf.skipTaggedFields();
}

}  // namespace kawasan::protocol
