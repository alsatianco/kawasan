#pragma once

#include <chrono>
#include <cstdint>
#include <stdexcept>
#include <string>

namespace kawasan {
namespace streams {

/**
 * Represents a record with timestamp information.
 *
 * This is used by TimestampExtractor to extract timestamps from records.
 */
struct ConsumerRecord {
    std::string topic;
    int32_t partition;
    int64_t offset;
    int64_t timestamp;  // Record timestamp from Kafka (-1 if not available)
    std::string key;
    std::string value;

    ConsumerRecord() = default;

    ConsumerRecord(const std::string& topic, int32_t partition, int64_t offset,
                   int64_t timestamp, const std::string& key, const std::string& value)
        : topic(topic), partition(partition), offset(offset),
          timestamp(timestamp), key(key), value(value) {}
};

/**
 * TimestampExtractor extracts the timestamp from a record.
 *
 * The extracted timestamp is used for:
 * - Windowed aggregations (determining which window a record belongs to)
 * - Stream time advancement
 * - Time-based operations like punctuations
 *
 * Kafka Streams supports three time semantics:
 * - Event time: The time when the event was created (embedded in the record)
 * - Ingestion time: The time when the event was received by Kafka
 * - Processing time: The current wall clock time when processing the record
 */
class TimestampExtractor {
public:
    virtual ~TimestampExtractor() = default;

    /**
     * Extract the timestamp from a record.
     *
     * @param record The consumer record
     * @param partition_time The highest timestamp seen so far for this partition
     *                       (-1 if no record has been seen yet)
     * @return The timestamp to use for this record in milliseconds
     */
    virtual int64_t extract(const ConsumerRecord& record, int64_t partition_time) = 0;
};

/**
 * Extracts the embedded timestamp from the record.
 *
 * This extractor uses the timestamp stored in the Kafka record itself.
 * If the record doesn't have a valid timestamp (timestamp < 0), an exception is thrown.
 *
 * Use this when events have meaningful timestamps embedded by the producer.
 */
class FailOnInvalidTimestamp : public TimestampExtractor {
public:
    int64_t extract(const ConsumerRecord& record, int64_t /*partition_time*/) override {
        if (record.timestamp < 0) {
            throw std::runtime_error(
                "Invalid timestamp in record from topic " + record.topic +
                ", partition " + std::to_string(record.partition) +
                ", offset " + std::to_string(record.offset) +
                ". Use a different TimestampExtractor to handle records without timestamps.");
        }
        return record.timestamp;
    }
};

/**
 * Extracts the embedded timestamp, returning a fallback for invalid timestamps.
 *
 * If the record has a valid timestamp (>= 0), that timestamp is returned.
 * Otherwise, the partition time (highest timestamp seen so far) is returned.
 * If no valid timestamp has been seen yet, returns 0.
 *
 * Use this for fault-tolerant processing where some records may lack timestamps.
 */
class LogAndSkipOnInvalidTimestamp : public TimestampExtractor {
public:
    int64_t extract(const ConsumerRecord& record, int64_t partition_time) override {
        if (record.timestamp >= 0) {
            return record.timestamp;
        }
        // Return the partition time or 0 if no valid timestamp has been seen
        return partition_time >= 0 ? partition_time : 0;
    }
};

/**
 * Uses the current wall clock time as the timestamp.
 *
 * This extractor ignores the record's embedded timestamp and uses
 * the current system time when the record is processed.
 *
 * Note: Processing time semantics can lead to non-deterministic results
 * since the same input can produce different outputs based on when it's processed.
 * Use event time semantics (FailOnInvalidTimestamp or LogAndSkipOnInvalidTimestamp)
 * when deterministic replay is required.
 */
class WallclockTimestampExtractor : public TimestampExtractor {
public:
    int64_t extract(const ConsumerRecord& /*record*/, int64_t /*partition_time*/) override {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()
        ).count();
    }
};

/**
 * Uses the timestamp stored in the record.
 *
 * Alias for FailOnInvalidTimestamp - this is the default extractor.
 */
using ExtractRecordMetadataTimestamp = FailOnInvalidTimestamp;

/**
 * Custom timestamp extractor that extracts timestamp from record value.
 *
 * Use this when the timestamp is embedded in the record payload rather than
 * in the Kafka record metadata.
 *
 * Example:
 *   class JsonTimestampExtractor : public ValueTimestampExtractor {
 *       int64_t extractFromValue(const std::string& value) override {
 *           auto json = parse(value);
 *           return json["timestamp"].get<int64_t>();
 *       }
 *   };
 */
class ValueTimestampExtractor : public TimestampExtractor {
public:
    int64_t extract(const ConsumerRecord& record, int64_t /*partition_time*/) override {
        return extractFromValue(record.value);
    }

protected:
    /**
     * Extract timestamp from the record value.
     *
     * @param value The record value (typically JSON or other structured format)
     * @return The extracted timestamp in milliseconds
     */
    virtual int64_t extractFromValue(const std::string& value) = 0;
};

/**
 * Punctuation type for scheduled callbacks.
 *
 * STREAM_TIME: Triggered when stream time advances past the scheduled time.
 *              Stream time only advances when new records with higher timestamps arrive.
 *              Use this for time-based processing that should be deterministic
 *              regardless of processing speed.
 *
 * WALL_CLOCK_TIME: Triggered based on actual wall clock time.
 *                  Use this for time-based processing that should happen at
 *                  regular real-world intervals regardless of input data rate.
 */
enum class PunctuationType {
    STREAM_TIME,
    WALL_CLOCK_TIME
};

/**
 * Callback interface for punctuation.
 *
 * Punctuators are scheduled callbacks that execute periodically based on
 * stream time or wall clock time.
 */
class Punctuator {
public:
    virtual ~Punctuator() = default;

    /**
     * Called when the punctuation triggers.
     *
     * @param timestamp The timestamp that triggered the punctuation.
     *                  For STREAM_TIME, this is the stream time that exceeded the schedule.
     *                  For WALL_CLOCK_TIME, this is the current wall clock time.
     */
    virtual void punctuate(int64_t timestamp) = 0;
};

/**
 * Cancellable interface for scheduled punctuations.
 *
 * When scheduling a punctuation, the returned Cancellable allows the
 * punctuation to be cancelled before it triggers.
 */
class Cancellable {
public:
    virtual ~Cancellable() = default;

    /**
     * Cancel this scheduled punctuation.
     * After cancellation, the punctuator will not be called again.
     */
    virtual void cancel() = 0;
};

} // namespace streams
} // namespace kawasan
