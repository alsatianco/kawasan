#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "kawasan/common/buffer.h"
#include "kawasan/common/error.h"
#include "kawasan/common/types.h"

namespace kawasan::protocol {

/// @brief DescribeCluster request for retrieving cluster metadata
class DescribeClusterRequest {
public:
    DescribeClusterRequest() = default;

    // Getters
    bool includeClusterAuthorizedOperations() const {
        return include_cluster_authorized_operations_;
    }

    // Setters
    void setIncludeClusterAuthorizedOperations(bool include) {
        include_cluster_authorized_operations_ = include;
    }

    // Serialization
    void encode(Buffer& buffer, int16_t api_version) const;
    void decode(Buffer& buffer, int16_t api_version);

    // Size calculation
    size_t size(int16_t api_version) const;

    // Phase EX-7: v1 adds endpoint_type to the request. We
    // accept-and-ignore (default is 1 = BROKER, which is what we are).
    int8_t endpointTypeRequest() const { return endpoint_type_request_; }
    void setEndpointTypeRequest(int8_t v) { endpoint_type_request_ = v; }

private:
    bool include_cluster_authorized_operations_ = false;
    int8_t endpoint_type_request_ = 1;  // BROKER
};

/// @brief DescribeCluster response containing cluster information
class DescribeClusterResponse {
public:
    DescribeClusterResponse() = default;

    // Getters
    int32_t throttleTimeMs() const { return throttle_time_ms_; }
    ErrorCode errorCode() const { return error_code_; }
    const std::string& errorMessage() const { return error_message_; }
    int8_t endpointType() const { return endpoint_type_; }
    const std::string& clusterId() const { return cluster_id_; }
    BrokerId controllerId() const { return controller_id_; }
    const std::vector<BrokerMetadata>& brokers() const { return brokers_; }
    int32_t clusterAuthorizedOperations() const {
        return cluster_authorized_operations_;
    }

    // Setters
    void setThrottleTimeMs(int32_t time) { throttle_time_ms_ = time; }
    void setErrorCode(ErrorCode code) { error_code_ = code; }
    void setErrorMessage(const std::string& message) { error_message_ = message; }
    void setEndpointType(int8_t type) { endpoint_type_ = type; }
    void setClusterId(const std::string& id) { cluster_id_ = id; }
    void setControllerId(BrokerId id) { controller_id_ = id; }
    void setBrokers(const std::vector<BrokerMetadata>& brokers) { brokers_ = brokers; }
    void addBroker(const BrokerMetadata& broker) { brokers_.push_back(broker); }
    void setClusterAuthorizedOperations(int32_t ops) {
        cluster_authorized_operations_ = ops;
    }

    // Serialization
    void encode(Buffer& buffer, int16_t api_version) const;
    void decode(Buffer& buffer, int16_t api_version);

    // Size calculation
    size_t size(int16_t api_version) const;

private:
    int32_t throttle_time_ms_ = 0;
    ErrorCode error_code_ = ErrorCode::NONE;
    std::string error_message_;
    int8_t endpoint_type_ = 1;  // 1 = BROKER, 2 = CONTROLLER
    std::string cluster_id_;
    BrokerId controller_id_ = -1;
    std::vector<BrokerMetadata> brokers_;
    int32_t cluster_authorized_operations_ = -2147483648;  // INT32_MIN means not included
};

}  // namespace kawasan::protocol
