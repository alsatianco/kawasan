#pragma once

#include <optional>
#include <string>

#include "kawasan/common/buffer.h"
#include "kawasan/common/types.h"
#include "kawasan/protocol/api_keys.h"

namespace kawasan::protocol {

/// @brief Request header for all Kafka protocol requests
class RequestHeader {
public:
    RequestHeader() = default;
    RequestHeader(ApiKey api_key, int16_t api_version, CorrelationId correlation_id,
                  const std::string& client_id)
        : api_key_(api_key),
          api_version_(api_version),
          correlation_id_(correlation_id),
          client_id_(client_id) {}

    // Getters
    ApiKey apiKey() const { return api_key_; }
    int16_t apiVersion() const { return api_version_; }
    CorrelationId correlationId() const { return correlation_id_; }
    const std::string& clientId() const { return client_id_; }

    // Setters
    void setApiKey(ApiKey key) { api_key_ = key; }
    void setApiVersion(int16_t version) { api_version_ = version; }
    void setCorrelationId(CorrelationId id) { correlation_id_ = id; }
    void setClientId(const std::string& id) { client_id_ = id; }

    // Serialization
    void encode(Buffer& buffer) const;
    void decode(Buffer& buffer);
    bool isFlexibleVersion() const;

    /// @brief Check if the response header should use flexible format (v1 with tagged fields)
    /// Note: Response header version is often different from request header version
    bool isFlexibleResponseHeader() const;

    // Size calculation
    size_t size() const;

private:
    ApiKey api_key_ = ApiKey::PRODUCE;
    int16_t api_version_ = 0;
    CorrelationId correlation_id_ = 0;
    std::string client_id_;
};

/// @brief Response header for all Kafka protocol responses
class ResponseHeader {
public:
    ResponseHeader() = default;
    explicit ResponseHeader(CorrelationId correlation_id, bool flexible = false)
        : correlation_id_(correlation_id), flexible_(flexible) {}

    // Getters
    CorrelationId correlationId() const { return correlation_id_; }
    bool isFlexible() const { return flexible_; }

    // Setters
    void setCorrelationId(CorrelationId id) { correlation_id_ = id; }
    void setFlexible(bool flexible) { flexible_ = flexible; }

    // Serialization
    void encode(Buffer& buffer) const;
    void decode(Buffer& buffer);

    // Size calculation
    size_t size() const;

private:
    CorrelationId correlation_id_ = 0;
    bool flexible_ = false;
};

}  // namespace kawasan::protocol
