#pragma once

#include <string>
#include <vector>

#include "kawasan/common/buffer.h"
#include "kawasan/common/types.h"
#include "kawasan/protocol/api_keys.h"

namespace kawasan::protocol {

struct ApiVersionInfo {
    ApiKey api_key;
    int16_t min_version;
    int16_t max_version;
};

// Validate client software name/version strings according to KIP-511
// Only letters, digits, hyphens, underscores, and periods are allowed
bool isValidClientSoftwareString(const std::string& str);

class ApiVersionsRequest {
public:
    void encode(Buffer& buffer, int16_t api_version) const;
    void decode(Buffer& buffer, int16_t api_version);

    const std::string& clientSoftwareName() const { return client_software_name_; }
    const std::string& clientSoftwareVersion() const { return client_software_version_; }

    void setClientSoftwareName(std::string name) {
        client_software_name_ = std::move(name);
    }
    void setClientSoftwareVersion(std::string version) {
        client_software_version_ = std::move(version);
    }

private:
    std::string client_software_name_ = "kawasan";
    std::string client_software_version_ = "dev";
};

class ApiVersionsResponse {
public:
    ApiVersionsResponse() = default;

    void setErrorCode(ErrorCode code) { error_code_ = code; }
    void setThrottleTimeMs(int32_t throttle) { throttle_time_ms_ = throttle; }
    void setApiVersions(const std::vector<ApiVersionInfo>& versions) {
        api_versions_ = versions;
    }

    const std::vector<ApiVersionInfo>& apiVersions() const { return api_versions_; }
    ErrorCode errorCode() const { return error_code_; }
    int32_t throttleTimeMs() const { return throttle_time_ms_; }

    void addApiVersion(const ApiVersionInfo& info) { api_versions_.push_back(info); }

    void encode(Buffer& buffer, int16_t api_version) const;
    void decode(Buffer& buffer, int16_t api_version);

private:
    ErrorCode error_code_ = ErrorCode::NONE;
    int32_t throttle_time_ms_ = 0;
    std::vector<ApiVersionInfo> api_versions_;
};

}  // namespace kawasan::protocol
