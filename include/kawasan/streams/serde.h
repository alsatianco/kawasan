#pragma once

#include <cstdint>
#include <cstring>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <vector>

namespace kawasan {
namespace streams {

/**
 * Serialization/Deserialization interface.
 *
 * Serde provides bidirectional conversion between typed data and bytes.
 * This is used for:
 * - Kafka record key/value serialization
 * - State store key/value serialization
 * - Changelog topic serialization
 *
 * @tparam T The type to serialize/deserialize
 */
template<typename T>
class Serde {
public:
    virtual ~Serde() = default;

    /**
     * Serialize data to bytes.
     *
     * @param data The data to serialize
     * @return Serialized bytes
     */
    virtual std::vector<uint8_t> serialize(const T& data) = 0;

    /**
     * Deserialize bytes to data.
     *
     * @param bytes The bytes to deserialize
     * @return Deserialized data
     * @throws std::runtime_error if deserialization fails
     */
    virtual T deserialize(const std::vector<uint8_t>& bytes) = 0;
};

/**
 * String serializer.
 *
 * Serializes strings as UTF-8 bytes.
 */
class StringSerde : public Serde<std::string> {
public:
    std::vector<uint8_t> serialize(const std::string& data) override {
        return std::vector<uint8_t>(data.begin(), data.end());
    }

    std::string deserialize(const std::vector<uint8_t>& bytes) override {
        return std::string(bytes.begin(), bytes.end());
    }
};

/**
 * 64-bit signed integer serializer.
 *
 * Uses big-endian byte order for consistent cross-platform serialization.
 */
class LongSerde : public Serde<int64_t> {
public:
    std::vector<uint8_t> serialize(const int64_t& data) override {
        std::vector<uint8_t> bytes(8);
        // Big-endian encoding
        bytes[0] = static_cast<uint8_t>((data >> 56) & 0xFF);
        bytes[1] = static_cast<uint8_t>((data >> 48) & 0xFF);
        bytes[2] = static_cast<uint8_t>((data >> 40) & 0xFF);
        bytes[3] = static_cast<uint8_t>((data >> 32) & 0xFF);
        bytes[4] = static_cast<uint8_t>((data >> 24) & 0xFF);
        bytes[5] = static_cast<uint8_t>((data >> 16) & 0xFF);
        bytes[6] = static_cast<uint8_t>((data >> 8) & 0xFF);
        bytes[7] = static_cast<uint8_t>(data & 0xFF);
        return bytes;
    }

    int64_t deserialize(const std::vector<uint8_t>& bytes) override {
        if (bytes.size() < 8) {
            throw std::runtime_error("LongSerde: insufficient bytes for int64_t");
        }
        return (static_cast<int64_t>(bytes[0]) << 56) |
               (static_cast<int64_t>(bytes[1]) << 48) |
               (static_cast<int64_t>(bytes[2]) << 40) |
               (static_cast<int64_t>(bytes[3]) << 32) |
               (static_cast<int64_t>(bytes[4]) << 24) |
               (static_cast<int64_t>(bytes[5]) << 16) |
               (static_cast<int64_t>(bytes[6]) << 8) |
               static_cast<int64_t>(bytes[7]);
    }
};

/**
 * 32-bit signed integer serializer.
 *
 * Uses big-endian byte order.
 */
class IntegerSerde : public Serde<int32_t> {
public:
    std::vector<uint8_t> serialize(const int32_t& data) override {
        std::vector<uint8_t> bytes(4);
        // Big-endian encoding
        bytes[0] = static_cast<uint8_t>((data >> 24) & 0xFF);
        bytes[1] = static_cast<uint8_t>((data >> 16) & 0xFF);
        bytes[2] = static_cast<uint8_t>((data >> 8) & 0xFF);
        bytes[3] = static_cast<uint8_t>(data & 0xFF);
        return bytes;
    }

    int32_t deserialize(const std::vector<uint8_t>& bytes) override {
        if (bytes.size() < 4) {
            throw std::runtime_error("IntegerSerde: insufficient bytes for int32_t");
        }
        return (static_cast<int32_t>(bytes[0]) << 24) |
               (static_cast<int32_t>(bytes[1]) << 16) |
               (static_cast<int32_t>(bytes[2]) << 8) |
               static_cast<int32_t>(bytes[3]);
    }
};

// Alias for backwards compatibility
using IntSerde = IntegerSerde;

/**
 * Double-precision floating point serializer.
 *
 * Uses IEEE 754 representation in big-endian byte order.
 */
class DoubleSerde : public Serde<double> {
public:
    std::vector<uint8_t> serialize(const double& data) override {
        static_assert(sizeof(double) == 8, "double must be 8 bytes");
        std::vector<uint8_t> bytes(8);
        uint64_t bits;
        std::memcpy(&bits, &data, sizeof(bits));
        // Big-endian encoding
        bytes[0] = static_cast<uint8_t>((bits >> 56) & 0xFF);
        bytes[1] = static_cast<uint8_t>((bits >> 48) & 0xFF);
        bytes[2] = static_cast<uint8_t>((bits >> 40) & 0xFF);
        bytes[3] = static_cast<uint8_t>((bits >> 32) & 0xFF);
        bytes[4] = static_cast<uint8_t>((bits >> 24) & 0xFF);
        bytes[5] = static_cast<uint8_t>((bits >> 16) & 0xFF);
        bytes[6] = static_cast<uint8_t>((bits >> 8) & 0xFF);
        bytes[7] = static_cast<uint8_t>(bits & 0xFF);
        return bytes;
    }

    double deserialize(const std::vector<uint8_t>& bytes) override {
        if (bytes.size() < 8) {
            throw std::runtime_error("DoubleSerde: insufficient bytes for double");
        }
        uint64_t bits = (static_cast<uint64_t>(bytes[0]) << 56) |
                        (static_cast<uint64_t>(bytes[1]) << 48) |
                        (static_cast<uint64_t>(bytes[2]) << 40) |
                        (static_cast<uint64_t>(bytes[3]) << 32) |
                        (static_cast<uint64_t>(bytes[4]) << 24) |
                        (static_cast<uint64_t>(bytes[5]) << 16) |
                        (static_cast<uint64_t>(bytes[6]) << 8) |
                        static_cast<uint64_t>(bytes[7]);
        double result;
        std::memcpy(&result, &bits, sizeof(result));
        return result;
    }
};

/**
 * Float serializer.
 *
 * Uses IEEE 754 representation in big-endian byte order.
 */
class FloatSerde : public Serde<float> {
public:
    std::vector<uint8_t> serialize(const float& data) override {
        static_assert(sizeof(float) == 4, "float must be 4 bytes");
        std::vector<uint8_t> bytes(4);
        uint32_t bits;
        std::memcpy(&bits, &data, sizeof(bits));
        // Big-endian encoding
        bytes[0] = static_cast<uint8_t>((bits >> 24) & 0xFF);
        bytes[1] = static_cast<uint8_t>((bits >> 16) & 0xFF);
        bytes[2] = static_cast<uint8_t>((bits >> 8) & 0xFF);
        bytes[3] = static_cast<uint8_t>(bits & 0xFF);
        return bytes;
    }

    float deserialize(const std::vector<uint8_t>& bytes) override {
        if (bytes.size() < 4) {
            throw std::runtime_error("FloatSerde: insufficient bytes for float");
        }
        uint32_t bits = (static_cast<uint32_t>(bytes[0]) << 24) |
                        (static_cast<uint32_t>(bytes[1]) << 16) |
                        (static_cast<uint32_t>(bytes[2]) << 8) |
                        static_cast<uint32_t>(bytes[3]);
        float result;
        std::memcpy(&result, &bits, sizeof(result));
        return result;
    }
};

/**
 * Byte array serializer.
 *
 * Pass-through serializer that returns bytes unchanged.
 */
class ByteArraySerde : public Serde<std::vector<uint8_t>> {
public:
    std::vector<uint8_t> serialize(const std::vector<uint8_t>& data) override {
        return data;
    }

    std::vector<uint8_t> deserialize(const std::vector<uint8_t>& bytes) override {
        return bytes;
    }
};

/**
 * Boolean serializer.
 *
 * Serializes true as 1, false as 0.
 */
class BooleanSerde : public Serde<bool> {
public:
    std::vector<uint8_t> serialize(const bool& data) override {
        return {static_cast<uint8_t>(data ? 1 : 0)};
    }

    bool deserialize(const std::vector<uint8_t>& bytes) override {
        if (bytes.empty()) {
            throw std::runtime_error("BooleanSerde: empty bytes");
        }
        return bytes[0] != 0;
    }
};

/**
 * Short serializer (16-bit signed integer).
 */
class ShortSerde : public Serde<int16_t> {
public:
    std::vector<uint8_t> serialize(const int16_t& data) override {
        std::vector<uint8_t> bytes(2);
        bytes[0] = static_cast<uint8_t>((data >> 8) & 0xFF);
        bytes[1] = static_cast<uint8_t>(data & 0xFF);
        return bytes;
    }

    int16_t deserialize(const std::vector<uint8_t>& bytes) override {
        if (bytes.size() < 2) {
            throw std::runtime_error("ShortSerde: insufficient bytes");
        }
        return static_cast<int16_t>((bytes[0] << 8) | bytes[1]);
    }
};

/**
 * Factory class for creating common Serde instances.
 *
 * Usage:
 *   auto stringSerde = Serdes::String();
 *   auto longSerde = Serdes::Long();
 */
class Serdes {
public:
    /**
     * Get a String Serde (UTF-8 encoding).
     */
    static std::shared_ptr<Serde<std::string>> String() {
        static auto instance = std::make_shared<StringSerde>();
        return instance;
    }

    /**
     * Get a Long (int64_t) Serde.
     */
    static std::shared_ptr<Serde<int64_t>> Long() {
        static auto instance = std::make_shared<LongSerde>();
        return instance;
    }

    /**
     * Get an Integer (int32_t) Serde.
     */
    static std::shared_ptr<Serde<int32_t>> Integer() {
        static auto instance = std::make_shared<IntegerSerde>();
        return instance;
    }

    /**
     * Get a Double Serde.
     */
    static std::shared_ptr<Serde<double>> Double() {
        static auto instance = std::make_shared<DoubleSerde>();
        return instance;
    }

    /**
     * Get a Float Serde.
     */
    static std::shared_ptr<Serde<float>> Float() {
        static auto instance = std::make_shared<FloatSerde>();
        return instance;
    }

    /**
     * Get a ByteArray Serde.
     */
    static std::shared_ptr<Serde<std::vector<uint8_t>>> ByteArray() {
        static auto instance = std::make_shared<ByteArraySerde>();
        return instance;
    }

    /**
     * Get a Boolean Serde.
     */
    static std::shared_ptr<Serde<bool>> Boolean() {
        static auto instance = std::make_shared<BooleanSerde>();
        return instance;
    }

    /**
     * Get a Short (int16_t) Serde.
     */
    static std::shared_ptr<Serde<int16_t>> Short() {
        static auto instance = std::make_shared<ShortSerde>();
        return instance;
    }
};

/**
 * Void Serde for null/absent values.
 */
class VoidSerde : public Serde<std::nullptr_t> {
public:
    std::vector<uint8_t> serialize(const std::nullptr_t&) override {
        return {};
    }

    std::nullptr_t deserialize(const std::vector<uint8_t>&) override {
        return nullptr;
    }
};

/**
 * Wrapped Serde that handles null values.
 *
 * Wraps another Serde to handle null/absent values by prefixing
 * with a null indicator byte.
 *
 * @tparam T The type to serialize
 */
template<typename T>
class NullableSerde : public Serde<std::optional<T>> {
public:
    explicit NullableSerde(std::shared_ptr<Serde<T>> inner)
        : inner_(std::move(inner)) {}

    std::vector<uint8_t> serialize(const std::optional<T>& data) override {
        if (!data.has_value()) {
            return {0};  // Null indicator
        }
        auto bytes = inner_->serialize(data.value());
        bytes.insert(bytes.begin(), 1);  // Non-null indicator
        return bytes;
    }

    std::optional<T> deserialize(const std::vector<uint8_t>& bytes) override {
        if (bytes.empty()) {
            throw std::runtime_error("NullableSerde: empty bytes");
        }
        if (bytes[0] == 0) {
            return std::nullopt;
        }
        return inner_->deserialize(
            std::vector<uint8_t>(bytes.begin() + 1, bytes.end()));
    }

private:
    std::shared_ptr<Serde<T>> inner_;
};

} // namespace streams
} // namespace kawasan
