// Phase 4.1 — minimal-but-correct encoders/decoders for the admin-misc batch.
// Each API supports v0 only (the schema is stable across versions for our
// purposes; clients negotiate down). Where the spec went flexible we keep the
// non-flexible path; later revisits can add compact strings.

#include "kawasan/protocol/admin_misc_requests.h"

namespace kawasan::protocol {

// ---------- DescribeLogDirs ----------

void DescribeLogDirsRequest::encode(Buffer& buf, int16_t /*v*/) const {
    if (fetch_all_) {
        buf.writeInt32(-1);
    } else {
        buf.writeInt32(static_cast<int32_t>(topics_.size()));
        for (const auto& t : topics_) {
            buf.writeString(t.topic);
            buf.writeInt32(static_cast<int32_t>(t.partitions.size()));
            for (int32_t p : t.partitions) buf.writeInt32(p);
        }
    }
}

void DescribeLogDirsRequest::decode(Buffer& buf, int16_t /*v*/) {
    int32_t tc = buf.readInt32();
    if (tc < 0) {
        fetch_all_ = true;
        topics_.clear();
        return;
    }
    fetch_all_ = false;
    topics_.resize(tc);
    for (int32_t i = 0; i < tc; ++i) {
        topics_[i].topic = buf.readString();
        int32_t pc = buf.readInt32();
        topics_[i].partitions.resize(pc < 0 ? 0 : pc);
        for (int32_t j = 0; j < pc; ++j) topics_[i].partitions[j] = buf.readInt32();
    }
}

void DescribeLogDirsResponse::encode(Buffer& buf, int16_t /*v*/) const {
    buf.writeInt32(throttle_time_ms_);
    buf.writeInt32(static_cast<int32_t>(log_dirs_.size()));
    for (const auto& d : log_dirs_) {
        buf.writeInt16(static_cast<int16_t>(d.error_code));
        buf.writeString(d.log_dir);
        buf.writeInt32(static_cast<int32_t>(d.topics.size()));
        for (const auto& t : d.topics) {
            buf.writeString(t.topic);
            buf.writeInt32(static_cast<int32_t>(t.partitions.size()));
            for (const auto& p : t.partitions) {
                buf.writeInt32(p.partition);
                buf.writeInt64(p.size_bytes);
                buf.writeInt64(p.offset_lag);
                buf.writeInt8(p.is_future ? 1 : 0);
            }
        }
    }
}

void DescribeLogDirsResponse::decode(Buffer& buf, int16_t /*v*/) {
    throttle_time_ms_ = buf.readInt32();
    int32_t dc = buf.readInt32();
    log_dirs_.resize(dc < 0 ? 0 : dc);
    for (int32_t i = 0; i < dc; ++i) {
        log_dirs_[i].error_code = static_cast<ErrorCode>(buf.readInt16());
        log_dirs_[i].log_dir = buf.readString();
        int32_t tc = buf.readInt32();
        log_dirs_[i].topics.resize(tc < 0 ? 0 : tc);
        for (int32_t j = 0; j < tc; ++j) {
            log_dirs_[i].topics[j].topic = buf.readString();
            int32_t pc = buf.readInt32();
            log_dirs_[i].topics[j].partitions.resize(pc < 0 ? 0 : pc);
            for (int32_t k = 0; k < pc; ++k) {
                log_dirs_[i].topics[j].partitions[k].partition = buf.readInt32();
                log_dirs_[i].topics[j].partitions[k].size_bytes = buf.readInt64();
                log_dirs_[i].topics[j].partitions[k].offset_lag = buf.readInt64();
                log_dirs_[i].topics[j].partitions[k].is_future = (buf.readInt8() != 0);
            }
        }
    }
}

// ---------- AlterReplicaLogDirs ----------

void AlterReplicaLogDirsRequest::encode(Buffer& buf, int16_t /*v*/) const {
    buf.writeInt32(static_cast<int32_t>(dirs_.size()));
    for (const auto& d : dirs_) {
        buf.writeString(d.log_dir);
        buf.writeInt32(static_cast<int32_t>(d.partitions.size()));
        for (const auto& p : d.partitions) {
            buf.writeString(p.topic);
            buf.writeInt32(p.partition);
        }
    }
}

void AlterReplicaLogDirsRequest::decode(Buffer& buf, int16_t /*v*/) {
    int32_t dc = buf.readInt32();
    dirs_.resize(dc < 0 ? 0 : dc);
    for (int32_t i = 0; i < dc; ++i) {
        dirs_[i].log_dir = buf.readString();
        int32_t pc = buf.readInt32();
        dirs_[i].partitions.resize(pc < 0 ? 0 : pc);
        for (int32_t j = 0; j < pc; ++j) {
            dirs_[i].partitions[j].topic = buf.readString();
            dirs_[i].partitions[j].partition = buf.readInt32();
        }
    }
}

void AlterReplicaLogDirsResponse::encode(Buffer& buf, int16_t /*v*/) const {
    buf.writeInt32(throttle_time_ms_);
    buf.writeInt32(static_cast<int32_t>(results_.size()));
    for (const auto& r : results_) {
        buf.writeString(r.topic);
        buf.writeInt32(r.partition);
        buf.writeInt16(static_cast<int16_t>(r.error_code));
    }
}

void AlterReplicaLogDirsResponse::decode(Buffer& buf, int16_t /*v*/) {
    throttle_time_ms_ = buf.readInt32();
    int32_t rc = buf.readInt32();
    results_.resize(rc < 0 ? 0 : rc);
    for (int32_t i = 0; i < rc; ++i) {
        results_[i].topic = buf.readString();
        results_[i].partition = buf.readInt32();
        results_[i].error_code = static_cast<ErrorCode>(buf.readInt16());
    }
}

// ---------- ElectLeaders ----------

void ElectLeadersRequest::encode(Buffer& buf, int16_t v) const {
    if (v >= 1) buf.writeInt8(election_type_);
    buf.writeInt32(static_cast<int32_t>(topics_.size()));
    for (const auto& t : topics_) {
        buf.writeString(t.topic);
        buf.writeInt32(static_cast<int32_t>(t.partitions.size()));
        for (int32_t p : t.partitions) buf.writeInt32(p);
    }
    buf.writeInt32(timeout_ms_);
}

void ElectLeadersRequest::decode(Buffer& buf, int16_t v) {
    election_type_ = (v >= 1) ? buf.readInt8() : 0;
    int32_t tc = buf.readInt32();
    topics_.resize(tc < 0 ? 0 : tc);
    for (int32_t i = 0; i < tc; ++i) {
        topics_[i].topic = buf.readString();
        int32_t pc = buf.readInt32();
        topics_[i].partitions.resize(pc < 0 ? 0 : pc);
        for (int32_t j = 0; j < pc; ++j) topics_[i].partitions[j] = buf.readInt32();
    }
    timeout_ms_ = buf.readInt32();
}

void ElectLeadersResponse::encode(Buffer& buf, int16_t v) const {
    buf.writeInt32(throttle_time_ms_);
    if (v >= 1) buf.writeInt16(static_cast<int16_t>(error_code_));
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
        }
    }
}

void ElectLeadersResponse::decode(Buffer& buf, int16_t v) {
    throttle_time_ms_ = buf.readInt32();
    error_code_ = (v >= 1) ? static_cast<ErrorCode>(buf.readInt16()) : ErrorCode::NONE;
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
        }
    }
}

// ---------- DeleteRecords ----------

void DeleteRecordsRequest::encode(Buffer& buf, int16_t /*v*/) const {
    buf.writeInt32(static_cast<int32_t>(topics_.size()));
    for (const auto& t : topics_) {
        buf.writeString(t.topic);
        buf.writeInt32(static_cast<int32_t>(t.partitions.size()));
        for (const auto& p : t.partitions) {
            buf.writeInt32(p.partition);
            buf.writeInt64(p.offset);
        }
    }
    buf.writeInt32(timeout_ms_);
}

void DeleteRecordsRequest::decode(Buffer& buf, int16_t /*v*/) {
    int32_t tc = buf.readInt32();
    topics_.resize(tc < 0 ? 0 : tc);
    for (int32_t i = 0; i < tc; ++i) {
        topics_[i].topic = buf.readString();
        int32_t pc = buf.readInt32();
        topics_[i].partitions.resize(pc < 0 ? 0 : pc);
        for (int32_t j = 0; j < pc; ++j) {
            topics_[i].partitions[j].partition = buf.readInt32();
            topics_[i].partitions[j].offset = buf.readInt64();
        }
    }
    timeout_ms_ = buf.readInt32();
}

void DeleteRecordsResponse::encode(Buffer& buf, int16_t /*v*/) const {
    buf.writeInt32(throttle_time_ms_);
    buf.writeInt32(static_cast<int32_t>(topics_.size()));
    for (const auto& t : topics_) {
        buf.writeString(t.topic);
        buf.writeInt32(static_cast<int32_t>(t.partitions.size()));
        for (const auto& p : t.partitions) {
            buf.writeInt32(p.partition);
            buf.writeInt64(p.low_watermark);
            buf.writeInt16(static_cast<int16_t>(p.error_code));
        }
    }
}

void DeleteRecordsResponse::decode(Buffer& buf, int16_t /*v*/) {
    throttle_time_ms_ = buf.readInt32();
    int32_t tc = buf.readInt32();
    topics_.resize(tc < 0 ? 0 : tc);
    for (int32_t i = 0; i < tc; ++i) {
        topics_[i].topic = buf.readString();
        int32_t pc = buf.readInt32();
        topics_[i].partitions.resize(pc < 0 ? 0 : pc);
        for (int32_t j = 0; j < pc; ++j) {
            topics_[i].partitions[j].partition = buf.readInt32();
            topics_[i].partitions[j].low_watermark = buf.readInt64();
            topics_[i].partitions[j].error_code =
                static_cast<ErrorCode>(buf.readInt16());
        }
    }
}

// ---------- DeleteGroups ----------

void DeleteGroupsRequest::encode(Buffer& buf, int16_t /*v*/) const {
    buf.writeInt32(static_cast<int32_t>(groups_.size()));
    for (const auto& g : groups_) buf.writeString(g);
}

void DeleteGroupsRequest::decode(Buffer& buf, int16_t /*v*/) {
    int32_t n = buf.readInt32();
    groups_.resize(n < 0 ? 0 : n);
    for (int32_t i = 0; i < n; ++i) groups_[i] = buf.readString();
}

void DeleteGroupsResponse::encode(Buffer& buf, int16_t /*v*/) const {
    buf.writeInt32(throttle_time_ms_);
    buf.writeInt32(static_cast<int32_t>(results_.size()));
    for (const auto& r : results_) {
        buf.writeString(r.group_id);
        buf.writeInt16(static_cast<int16_t>(r.error_code));
    }
}

void DeleteGroupsResponse::decode(Buffer& buf, int16_t /*v*/) {
    throttle_time_ms_ = buf.readInt32();
    int32_t n = buf.readInt32();
    results_.resize(n < 0 ? 0 : n);
    for (int32_t i = 0; i < n; ++i) {
        results_[i].group_id = buf.readString();
        results_[i].error_code = static_cast<ErrorCode>(buf.readInt16());
    }
}

// ---------- OffsetDelete ----------

void OffsetDeleteRequest::encode(Buffer& buf, int16_t /*v*/) const {
    buf.writeString(group_id_);
    buf.writeInt32(static_cast<int32_t>(topics_.size()));
    for (const auto& t : topics_) {
        buf.writeString(t.topic);
        buf.writeInt32(static_cast<int32_t>(t.partitions.size()));
        for (const auto& p : t.partitions) buf.writeInt32(p.partition);
    }
}

void OffsetDeleteRequest::decode(Buffer& buf, int16_t /*v*/) {
    group_id_ = buf.readString();
    int32_t tc = buf.readInt32();
    topics_.resize(tc < 0 ? 0 : tc);
    for (int32_t i = 0; i < tc; ++i) {
        topics_[i].topic = buf.readString();
        int32_t pc = buf.readInt32();
        topics_[i].partitions.resize(pc < 0 ? 0 : pc);
        for (int32_t j = 0; j < pc; ++j) {
            topics_[i].partitions[j].partition = buf.readInt32();
        }
    }
}

void OffsetDeleteResponse::encode(Buffer& buf, int16_t /*v*/) const {
    buf.writeInt16(static_cast<int16_t>(error_code_));
    buf.writeInt32(throttle_time_ms_);
    buf.writeInt32(static_cast<int32_t>(topics_.size()));
    for (const auto& t : topics_) {
        buf.writeString(t.topic);
        buf.writeInt32(static_cast<int32_t>(t.partitions.size()));
        for (const auto& p : t.partitions) {
            buf.writeInt32(p.partition);
            buf.writeInt16(static_cast<int16_t>(p.error_code));
        }
    }
}

void OffsetDeleteResponse::decode(Buffer& buf, int16_t /*v*/) {
    error_code_ = static_cast<ErrorCode>(buf.readInt16());
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
        }
    }
}

// ---------- CreatePartitions ----------

void CreatePartitionsRequest::encode(Buffer& buf, int16_t /*v*/) const {
    buf.writeInt32(static_cast<int32_t>(topics_.size()));
    for (const auto& t : topics_) {
        buf.writeString(t.topic);
        buf.writeInt32(t.count);
        // Assignment array (nullable).
        if (t.assignments.empty()) {
            buf.writeInt32(-1);
        } else {
            buf.writeInt32(static_cast<int32_t>(t.assignments.size()));
            for (const auto& a : t.assignments) {
                buf.writeInt32(static_cast<int32_t>(a.size()));
                for (int32_t b : a) buf.writeInt32(b);
            }
        }
    }
    buf.writeInt32(timeout_ms_);
    buf.writeInt8(validate_only_ ? 1 : 0);
}

void CreatePartitionsRequest::decode(Buffer& buf, int16_t /*v*/) {
    int32_t tc = buf.readInt32();
    topics_.resize(tc < 0 ? 0 : tc);
    for (int32_t i = 0; i < tc; ++i) {
        topics_[i].topic = buf.readString();
        topics_[i].count = buf.readInt32();
        int32_t ac = buf.readInt32();
        if (ac > 0) {
            topics_[i].assignments.resize(ac);
            for (int32_t j = 0; j < ac; ++j) {
                int32_t rc = buf.readInt32();
                topics_[i].assignments[j].resize(rc < 0 ? 0 : rc);
                for (int32_t k = 0; k < rc; ++k) {
                    topics_[i].assignments[j][k] = buf.readInt32();
                }
            }
        }
    }
    timeout_ms_ = buf.readInt32();
    validate_only_ = (buf.readInt8() != 0);
}

void CreatePartitionsResponse::encode(Buffer& buf, int16_t /*v*/) const {
    buf.writeInt32(throttle_time_ms_);
    buf.writeInt32(static_cast<int32_t>(results_.size()));
    for (const auto& r : results_) {
        buf.writeString(r.topic);
        buf.writeInt16(static_cast<int16_t>(r.error_code));
        const auto msg = r.error_message.empty()
                             ? std::optional<std::string>{}
                             : std::optional<std::string>(r.error_message);
        buf.writeNullableString(msg);
    }
}

void CreatePartitionsResponse::decode(Buffer& buf, int16_t /*v*/) {
    throttle_time_ms_ = buf.readInt32();
    int32_t n = buf.readInt32();
    results_.resize(n < 0 ? 0 : n);
    for (int32_t i = 0; i < n; ++i) {
        results_[i].topic = buf.readString();
        results_[i].error_code = static_cast<ErrorCode>(buf.readInt16());
        auto msg = buf.readNullableString();
        results_[i].error_message = msg.value_or("");
    }
}

}  // namespace kawasan::protocol
