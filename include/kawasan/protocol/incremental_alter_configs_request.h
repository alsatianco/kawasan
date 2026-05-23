#pragma once

#include <optional>
#include <string>
#include <vector>

#include "kawasan/common/buffer.h"
#include "kawasan/common/error.h"
#include "kawasan/protocol/describe_configs_request.h"

namespace kawasan::protocol {

/// @brief IncrementalAlterConfigs (API 44) — per-key SET/DELETE/APPEND/SUBTRACT.
///
/// Phase 4.1b: supports v0–v1.
///   v0:  resources[{ type, name, configs[{ name, op INT8, value NULLABLE_STRING }] }], validate_only
///   v1:  flexible (compact strings + tagged fields)
///
/// op values: 0=SET, 1=DELETE, 2=APPEND, 3=SUBTRACT.
class IncrementalAlterConfigsRequest {
public:
    enum class Op : int8_t { SET = 0, DELETE = 1, APPEND = 2, SUBTRACT = 3 };

    struct ConfigEntry {
        std::string name;
        Op op = Op::SET;
        std::optional<std::string> value;
    };

    struct Resource {
        ConfigResourceType resource_type = ConfigResourceType::UNKNOWN;
        std::string resource_name;
        std::vector<ConfigEntry> configs;
    };

    const std::vector<Resource>& resources() const { return resources_; }
    bool validateOnly() const { return validate_only_; }

    void addResource(Resource r) { resources_.push_back(std::move(r)); }
    void setValidateOnly(bool v) { validate_only_ = v; }

    void encode(Buffer& buffer, int16_t api_version) const;
    void decode(Buffer& buffer, int16_t api_version);

private:
    std::vector<Resource> resources_;
    bool validate_only_ = false;
};

class IncrementalAlterConfigsResponse {
public:
    struct ResourceResult {
        ErrorCode error_code = ErrorCode::NONE;
        std::string error_message;
        ConfigResourceType resource_type = ConfigResourceType::UNKNOWN;
        std::string resource_name;
    };

    void setThrottleTimeMs(int32_t v) { throttle_time_ms_ = v; }
    void addResult(ResourceResult r) { results_.push_back(std::move(r)); }

    int32_t throttleTimeMs() const { return throttle_time_ms_; }
    const std::vector<ResourceResult>& results() const { return results_; }

    void encode(Buffer& buffer, int16_t api_version) const;
    void decode(Buffer& buffer, int16_t api_version);

private:
    int32_t throttle_time_ms_ = 0;
    std::vector<ResourceResult> results_;
};

}  // namespace kawasan::protocol
