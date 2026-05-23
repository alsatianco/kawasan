#pragma once

#include "serde.h"
#include <nlohmann/json.hpp>
#include <string>
#include <vector>

namespace kawasan {
namespace streams {

/**
 * JSON Serde for generic types.
 *
 * Uses nlohmann::json for serialization. Types must be compatible
 * with nlohmann::json (provide to_json/from_json via ADL or be
 * basic types like string, int, double, etc.).
 *
 * @tparam T The type to serialize (must be JSON-compatible)
 */
template<typename T>
class JsonSerde : public Serde<T> {
public:
    std::vector<uint8_t> serialize(const T& data) override {
        nlohmann::json j = data;
        std::string str = j.dump();
        return std::vector<uint8_t>(str.begin(), str.end());
    }

    T deserialize(const std::vector<uint8_t>& bytes) override {
        std::string str(bytes.begin(), bytes.end());
        nlohmann::json j = nlohmann::json::parse(str);
        return j.get<T>();
    }
};

/**
 * JSON Serde that serializes directly to nlohmann::json objects.
 *
 * This is useful when you want to work with JSON objects directly
 * without converting to a specific type.
 */
class RawJsonSerde : public Serde<nlohmann::json> {
public:
    std::vector<uint8_t> serialize(const nlohmann::json& data) override {
        std::string str = data.dump();
        return std::vector<uint8_t>(str.begin(), str.end());
    }

    nlohmann::json deserialize(const std::vector<uint8_t>& bytes) override {
        std::string str(bytes.begin(), bytes.end());
        return nlohmann::json::parse(str);
    }
};

/**
 * Pretty-printing JSON Serde.
 *
 * Like JsonSerde but produces human-readable output with indentation.
 * Not recommended for production use due to larger output size.
 *
 * @tparam T The type to serialize
 */
template<typename T>
class PrettyJsonSerde : public Serde<T> {
public:
    explicit PrettyJsonSerde(int indent = 2) : indent_(indent) {}

    std::vector<uint8_t> serialize(const T& data) override {
        nlohmann::json j = data;
        std::string str = j.dump(indent_);
        return std::vector<uint8_t>(str.begin(), str.end());
    }

    T deserialize(const std::vector<uint8_t>& bytes) override {
        std::string str(bytes.begin(), bytes.end());
        nlohmann::json j = nlohmann::json::parse(str);
        return j.get<T>();
    }

private:
    int indent_;
};

/**
 * Lenient JSON Serde that handles malformed input.
 *
 * Returns a default value if parsing fails instead of throwing.
 *
 * @tparam T The type to serialize
 */
template<typename T>
class LenientJsonSerde : public Serde<T> {
public:
    explicit LenientJsonSerde(T default_value = T{})
        : default_value_(std::move(default_value)) {}

    std::vector<uint8_t> serialize(const T& data) override {
        nlohmann::json j = data;
        std::string str = j.dump();
        return std::vector<uint8_t>(str.begin(), str.end());
    }

    T deserialize(const std::vector<uint8_t>& bytes) override {
        try {
            std::string str(bytes.begin(), bytes.end());
            nlohmann::json j = nlohmann::json::parse(str);
            return j.get<T>();
        } catch (const nlohmann::json::exception&) {
            return default_value_;
        }
    }

private:
    T default_value_;
};

/**
 * Factory for creating JSON Serdes.
 */
class JsonSerdes {
public:
    /**
     * Get a JSON Serde for the specified type.
     *
     * @tparam T The type to serialize (must be JSON-compatible)
     */
    template<typename T>
    static std::shared_ptr<Serde<T>> Json() {
        return std::make_shared<JsonSerde<T>>();
    }

    /**
     * Get a raw JSON Serde for nlohmann::json objects.
     */
    static std::shared_ptr<Serde<nlohmann::json>> RawJson() {
        static auto instance = std::make_shared<RawJsonSerde>();
        return instance;
    }

    /**
     * Get a pretty-printing JSON Serde.
     *
     * @tparam T The type to serialize
     * @param indent Indentation level (default: 2 spaces)
     */
    template<typename T>
    static std::shared_ptr<Serde<T>> PrettyJson(int indent = 2) {
        return std::make_shared<PrettyJsonSerde<T>>(indent);
    }

    /**
     * Get a lenient JSON Serde that handles parse errors.
     *
     * @tparam T The type to serialize
     * @param default_value Value to return on parse failure
     */
    template<typename T>
    static std::shared_ptr<Serde<T>> LenientJson(T default_value = T{}) {
        return std::make_shared<LenientJsonSerde<T>>(std::move(default_value));
    }
};

/**
 * Common JSON-serializable types for streaming.
 */
namespace json_types {

/**
 * A key-value pair in JSON format.
 */
struct KeyValue {
    std::string key;
    nlohmann::json value;

    NLOHMANN_DEFINE_TYPE_INTRUSIVE(KeyValue, key, value)
};

/**
 * A timestamped record.
 */
struct TimestampedRecord {
    int64_t timestamp;
    std::string key;
    nlohmann::json value;

    NLOHMANN_DEFINE_TYPE_INTRUSIVE(TimestampedRecord, timestamp, key, value)
};

/**
 * A windowed key.
 */
struct WindowedKey {
    std::string key;
    int64_t window_start;
    int64_t window_end;

    NLOHMANN_DEFINE_TYPE_INTRUSIVE(WindowedKey, key, window_start, window_end)
};

/**
 * Aggregation result with count.
 */
struct CountResult {
    std::string key;
    int64_t count;

    NLOHMANN_DEFINE_TYPE_INTRUSIVE(CountResult, key, count)
};

/**
 * Aggregation result with sum.
 */
struct SumResult {
    std::string key;
    double sum;

    NLOHMANN_DEFINE_TYPE_INTRUSIVE(SumResult, key, sum)
};

/**
 * Aggregation result with average.
 */
struct AverageResult {
    std::string key;
    double average;
    int64_t count;

    NLOHMANN_DEFINE_TYPE_INTRUSIVE(AverageResult, key, average, count)
};

/**
 * Join result.
 */
template<typename L, typename R>
struct JoinResult {
    std::string key;
    std::optional<L> left;
    std::optional<R> right;
};

template<typename L, typename R>
void to_json(nlohmann::json& j, const JoinResult<L, R>& r) {
    j = nlohmann::json{{"key", r.key}};
    if (r.left) {
        j["left"] = *r.left;
    }
    if (r.right) {
        j["right"] = *r.right;
    }
}

template<typename L, typename R>
void from_json(const nlohmann::json& j, JoinResult<L, R>& r) {
    j.at("key").get_to(r.key);
    if (j.contains("left") && !j["left"].is_null()) {
        r.left = j["left"].get<L>();
    }
    if (j.contains("right") && !j["right"].is_null()) {
        r.right = j["right"].get<R>();
    }
}

} // namespace json_types

} // namespace streams
} // namespace kawasan
