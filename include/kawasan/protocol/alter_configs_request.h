#pragma once

#include <optional>
#include <string>
#include <vector>

#include "kawasan/common/buffer.h"
#include "kawasan/common/error.h"
#include "kawasan/protocol/describe_configs_request.h"  // ConfigResourceType

namespace kawasan::protocol {

/// @brief AlterConfigs (API 33) — replace-all semantics for a resource.
///
/// Phase 4.1a: supports v0–v2.
///   v0:  resources[{ type, name, configs[{ name, value }] }], validate_only
///   v1:  same body (v1 just bumps for the response throttle position semantics)
///   v2:  flexible (compact strings + tagged fields)
///
/// Note: AlterConfigs replaces the FULL config set on the resource. Use
/// IncrementalAlterConfigs (API 44) for per-key SET/DELETE/APPEND/SUBTRACT.
class AlterConfigsRequest {
public:
    struct ConfigEntry {
        std::string name;
        std::optional<std::string> value;
    };

    struct Resource {
        ConfigResourceType resource_type = ConfigResourceType::UNKNOWN;
        std::string resource_name;
        std::vector<ConfigEntry> configs;
    };

    AlterConfigsRequest() = default;

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

class AlterConfigsResponse {
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
