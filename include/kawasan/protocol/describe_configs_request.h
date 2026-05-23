#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "kawasan/common/buffer.h"
#include "kawasan/common/error.h"

namespace kawasan::protocol {

/// @brief Resource type for DescribeConfigs request
enum class ConfigResourceType : int8_t {
    UNKNOWN = 0,
    TOPIC = 2,
    BROKER = 4,
    BROKER_LOGGER = 8
};

/// @brief Resource specification for DescribeConfigs request
struct ConfigResource {
    ConfigResourceType resource_type;
    std::string resource_name;
    std::vector<std::string> config_names;  // Empty means all configs

    ConfigResource() : resource_type(ConfigResourceType::UNKNOWN) {}
    ConfigResource(ConfigResourceType type, const std::string& name)
        : resource_type(type), resource_name(name) {}
};

/// @brief DescribeConfigs request for retrieving broker/topic configurations
class DescribeConfigsRequest {
public:
    DescribeConfigsRequest() = default;

    // Getters
    const std::vector<ConfigResource>& resources() const { return resources_; }
    bool includeSynonyms() const { return include_synonyms_; }
    bool includeDocumentation() const { return include_documentation_; }

    // Setters
    void setResources(const std::vector<ConfigResource>& resources) { 
        resources_ = resources; 
    }
    void addResource(const ConfigResource& resource) { 
        resources_.push_back(resource); 
    }
    void setIncludeSynonyms(bool include) { include_synonyms_ = include; }
    void setIncludeDocumentation(bool include) { include_documentation_ = include; }

    // Serialization
    void encode(Buffer& buffer, int16_t api_version) const;
    void decode(Buffer& buffer, int16_t api_version);

    // Size calculation
    size_t size(int16_t api_version) const;

private:
    std::vector<ConfigResource> resources_;
    bool include_synonyms_ = false;
    bool include_documentation_ = false;
};

/// @brief Configuration entry in DescribeConfigs response
struct ConfigEntry {
    std::string name;
    std::string value;
    bool read_only = false;
    bool is_default = false;
    bool is_sensitive = false;
};

/// @brief Resource result in DescribeConfigs response
struct DescribeConfigsResourceResult {
    ErrorCode error_code = ErrorCode::NONE;
    std::string error_message;
    ConfigResourceType resource_type = ConfigResourceType::UNKNOWN;
    std::string resource_name;
    std::vector<ConfigEntry> configs;
};

/// @brief DescribeConfigs response containing configuration information
class DescribeConfigsResponse {
public:
    DescribeConfigsResponse() = default;

    // Getters
    int32_t throttleTimeMs() const { return throttle_time_ms_; }
    const std::vector<DescribeConfigsResourceResult>& results() const { 
        return results_; 
    }

    // Setters
    void setThrottleTimeMs(int32_t time) { throttle_time_ms_ = time; }
    void setResults(const std::vector<DescribeConfigsResourceResult>& results) { 
        results_ = results; 
    }
    void addResult(const DescribeConfigsResourceResult& result) { 
        results_.push_back(result); 
    }

    // Serialization
    void encode(Buffer& buffer, int16_t api_version) const;
    void decode(Buffer& buffer, int16_t api_version);

    // Size calculation
    size_t size(int16_t api_version) const;

private:
    int32_t throttle_time_ms_ = 0;
    std::vector<DescribeConfigsResourceResult> results_;
};

}  // namespace kawasan::protocol
