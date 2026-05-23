#pragma once

#include <vector>

#include "kawasan/common/buffer.h"
#include "kawasan/common/types.h"

namespace kawasan::storage {

/// @brief Record batch - the unit of storage in Kafka
class RecordBatch {
public:
    RecordBatch() = default;

    // Getters
    Offset baseOffset() const { return base_offset_; }
    int32_t batchLength() const { return batch_length_; }
    int32_t partitionLeaderEpoch() const { return partition_leader_epoch_; }
    int8_t magic() const { return magic_; }
    uint32_t crc() const { return crc_; }
    int16_t attributes() const { return attributes_; }
    int32_t lastOffsetDelta() const { return last_offset_delta_; }
    Timestamp firstTimestamp() const { return first_timestamp_; }
    Timestamp maxTimestamp() const { return max_timestamp_; }
    int64_t producerId() const { return producer_id_; }
    int16_t producerEpoch() const { return producer_epoch_; }
    int32_t baseSequence() const { return base_sequence_; }
    const std::vector<Record>& records() const { return records_; }

    // Setters
    void setBaseOffset(Offset offset) { base_offset_ = offset; }
    void setPartitionLeaderEpoch(int32_t epoch) { partition_leader_epoch_ = epoch; }
    void setMagic(int8_t magic) { magic_ = magic; }
    void setAttributes(int16_t attributes) { attributes_ = attributes; }
    void setFirstTimestamp(Timestamp ts) { first_timestamp_ = ts; }
    void setMaxTimestamp(Timestamp ts) { max_timestamp_ = ts; }
    void setProducerId(int64_t id) { producer_id_ = id; }
    void setProducerEpoch(int16_t epoch) { producer_epoch_ = epoch; }
    void setBaseSequence(int32_t seq) { base_sequence_ = seq; }
    void addRecord(const Record& record);

    // Compression
    CompressionType compressionType() const;
    bool isTransactional() const;
    bool isControlBatch() const;

    // Serialization
    void encode(Buffer& buffer) const;
    void decode(Buffer& buffer);
    std::vector<uint8_t> serialize() const;
    static RecordBatch deserialize(const std::vector<uint8_t>& data);
    static RecordBatch deserialize(Buffer& buffer);
    
    // Deserialize from produce request format (without base_offset/batch_length prefix)
    static RecordBatch deserializeFromProduceRequest(const std::vector<uint8_t>& data);

    // Size calculation
    size_t size() const;

    // Validation
    bool isValid() const;
    uint32_t computeCrc() const;

    /// @brief Phase EX-10: build a control batch carrying a COMMIT or
    /// ABORT marker for a transaction. The batch has 1 record with a
    /// key encoding `(version=0, type)` where type=0 is ABORT and
    /// type=1 is COMMIT. The batch's `isControl` attribute bit is set
    /// so consumers know to interpret the record specially.
    ///
    /// @param producer_id   The transaction's producer_id (echoed in batch).
    /// @param producer_epoch The transaction's producer_epoch.
    /// @param base_offset    The offset this control batch will be appended at.
    /// @param committed      true for COMMIT marker, false for ABORT.
    /// @param timestamp_ms   Wall-clock timestamp (typically now()).
    static RecordBatch makeControlBatch(int64_t producer_id,
                                        int16_t producer_epoch,
                                        Offset base_offset, bool committed,
                                        Timestamp timestamp_ms);

private:
    void updateDerivedFields();
    std::vector<uint8_t> encodeRecords() const;

    Offset base_offset_ = 0;
    mutable int32_t batch_length_ = 0;
    int32_t partition_leader_epoch_ = -1;
    int8_t magic_ = 2;  // Magic byte version (2 for Kafka 0.11+)
    mutable uint32_t crc_ = 0;
    int16_t attributes_ = 0;
    int32_t last_offset_delta_ = 0;
    Timestamp first_timestamp_ = 0;
    Timestamp max_timestamp_ = 0;
    int64_t producer_id_ = -1;
    int16_t producer_epoch_ = -1;
    int32_t base_sequence_ = -1;
    std::vector<Record> records_;
};

}  // namespace kawasan::storage
