#include "kawasan/protocol/describe_cluster_request.h"
#include "kawasan/common/logger.h"

namespace kawasan::protocol {

// Helper to write compact string (length+1 as unsigned varint, then bytes)
static void writeCompactString(Buffer& buffer, const std::string& value) {
    uint32_t length_plus_one = static_cast<uint32_t>(value.size() + 1);
    Logger::info("writeCompactString: value='{}' size={} writing length_plus_one={}", 
                 value, value.size(), length_plus_one);
    buffer.writeUnsignedVarInt(length_plus_one);
    buffer.writeBytes(reinterpret_cast<const uint8_t*>(value.data()), value.size());
}

// Helper to write compact nullable string
static void writeCompactNullableString(Buffer& buffer, const std::optional<std::string>& value) {
    if (value) {
        buffer.writeUnsignedVarInt(static_cast<uint32_t>(value->size() + 1));
        buffer.writeBytes(reinterpret_cast<const uint8_t*>(value->data()), value->size());
    } else {
        buffer.writeUnsignedVarInt(0);  // 0 means null in compact encoding
    }
}

// DescribeClusterRequest encode
void DescribeClusterRequest::encode(Buffer& buffer, int16_t api_version) const {
    // DESCRIBE_CLUSTER v0+ uses flexible versions
    // Include cluster authorized operations (v0+)
    buffer.writeInt8(include_cluster_authorized_operations_ ? 1 : 0);
    // EndpointType (v1+, KIP-919)
    if (api_version >= 1) {
        buffer.writeInt8(endpoint_type_request_);
    }
    // Tagged fields (empty)
    buffer.writeUnsignedVarInt(0);
}

// DescribeClusterRequest decode
void DescribeClusterRequest::decode(Buffer& buffer, int16_t api_version) {
    // DESCRIBE_CLUSTER v0+ uses flexible versions.
    // Include cluster authorized operations (v0+)
    include_cluster_authorized_operations_ = buffer.readInt8() != 0;
    // Phase EX-7 fix: v1 REQUEST adds an `endpoint_type` INT8 between
    // include_cluster_authorized_operations and the tagged_fields. We
    // accept-and-ignore it (default 1 = BROKER per spec).
    if (api_version >= 1) {
        endpoint_type_request_ = buffer.readInt8();
    }
    // Skip tagged fields
    uint32_t num_tagged_fields = buffer.readUnsignedVarInt();
    for (uint32_t i = 0; i < num_tagged_fields; ++i) {
        uint32_t tag = buffer.readUnsignedVarInt();
        uint32_t size = buffer.readUnsignedVarInt();
        if (size > 0) {
            (void)buffer.readBytes(size);
        }
        (void)tag;  // Unused
    }
}

// DescribeClusterRequest size
size_t DescribeClusterRequest::size(int16_t api_version) const {
    (void)api_version;  // Unused
    return 1 + 1;  // include_cluster_authorized_operations + empty tagged fields
}

// DescribeClusterResponse encode
void DescribeClusterResponse::encode(Buffer& buffer, int16_t api_version) const {
    // DESCRIBE_CLUSTER v0+ uses flexible versions (compact strings + tagged fields)

    // Throttle time (v0+)
    buffer.writeInt32(throttle_time_ms_);

    // Error code (v0+)
    buffer.writeInt16(static_cast<int16_t>(error_code_));

    // Error message (compact nullable string)
    writeCompactNullableString(buffer,
        error_message_.empty() ? std::nullopt : std::optional<std::string>(error_message_));

    // Endpoint type (v1+ only) - 1 = BROKER, 2 = CONTROLLER
    if (api_version >= 1) {
        buffer.writeInt8(endpoint_type_);
    }

    // Cluster ID (compact string, non-nullable)
    Logger::info("DescribeClusterResponse::encode: cluster_id='{}' length={}", cluster_id_, cluster_id_.size());
    writeCompactString(buffer, cluster_id_);

    // Controller ID (v0+)
    buffer.writeInt32(controller_id_);

    // Brokers array (compact array: length+1 as unsigned varint)
    buffer.writeUnsignedVarInt(static_cast<uint32_t>(brokers_.size() + 1));
    for (const auto& broker : brokers_) {
        buffer.writeInt32(broker.id);
        writeCompactString(buffer, broker.host);
        buffer.writeInt32(broker.port);
        // Rack (compact nullable string)
        writeCompactNullableString(buffer, broker.rack);
        // Tagged fields for broker (empty)
        buffer.writeUnsignedVarInt(0);
    }

    // Cluster authorized operations (v0+)
    buffer.writeInt32(cluster_authorized_operations_);
    
    // Tagged fields (empty)
    buffer.writeUnsignedVarInt(0);
}

// DescribeClusterResponse decode
void DescribeClusterResponse::decode(Buffer& buffer, int16_t api_version) {
    // DESCRIBE_CLUSTER v0+ uses flexible versions
    (void)api_version;
    
    // Throttle time
    throttle_time_ms_ = buffer.readInt32();

    // Error code
    error_code_ = static_cast<ErrorCode>(buffer.readInt16());
    
    // Error message (compact nullable string)
    uint32_t error_msg_len_plus1 = buffer.readUnsignedVarInt();
    if (error_msg_len_plus1 > 0) {
        error_message_ = buffer.readString();  // Will read error_msg_len_plus1 - 1 bytes
    } else {
        error_message_ = "";
    }

    // Endpoint type (v1+) — KIP-919. Bug fix (Phase 1.17): used to read
    // unconditionally even on v0 which would consume one byte of the cluster_id
    // varint.
    if (api_version >= 1) {
        endpoint_type_ = buffer.readInt8();
    }

    // Cluster ID (compact string, non-nullable)
    uint32_t cluster_id_len_plus1 = buffer.readUnsignedVarInt();
    if (cluster_id_len_plus1 == 0) {
        throw std::runtime_error("DESCRIBE_CLUSTER cluster_id cannot be null");
    }
    std::vector<uint8_t> cluster_id_bytes(cluster_id_len_plus1 - 1);
    for (size_t i = 0; i < cluster_id_len_plus1 - 1; ++i) {
        cluster_id_bytes[i] = buffer.readInt8();
    }
    cluster_id_ = std::string(cluster_id_bytes.begin(), cluster_id_bytes.end());

    // Controller ID
    controller_id_ = buffer.readInt32();

    // Brokers array (compact array)
    uint32_t broker_count_plus1 = buffer.readUnsignedVarInt();
    if (broker_count_plus1 == 0) {
        brokers_.clear();
        return;
    }
    
    int32_t broker_count = static_cast<int32_t>(broker_count_plus1 - 1);
    brokers_.clear();
    brokers_.reserve(broker_count);

    for (int32_t i = 0; i < broker_count; ++i) {
        BrokerMetadata broker;
        broker.id = buffer.readInt32();
        
        // Host (compact string)
        uint32_t host_len_plus1 = buffer.readUnsignedVarInt();
        std::vector<uint8_t> host_bytes(host_len_plus1 - 1);
        for (size_t j = 0; j < host_len_plus1 - 1; ++j) {
            host_bytes[j] = buffer.readInt8();
        }
        broker.host = std::string(host_bytes.begin(), host_bytes.end());
        
        broker.port = buffer.readInt32();
        
        // Rack (compact nullable string)
        uint32_t rack_len_plus1 = buffer.readUnsignedVarInt();
        if (rack_len_plus1 > 0) {
            std::vector<uint8_t> rack_bytes(rack_len_plus1 - 1);
            for (size_t j = 0; j < rack_len_plus1 - 1; ++j) {
                rack_bytes[j] = buffer.readInt8();
            }
            broker.rack = std::string(rack_bytes.begin(), rack_bytes.end());
        } else {
            broker.rack = std::nullopt;
        }
        
        // Skip broker tagged fields
        uint32_t num_tagged_fields = buffer.readUnsignedVarInt();
        for (uint32_t j = 0; j < num_tagged_fields; ++j) {
            uint32_t tag = buffer.readUnsignedVarInt();
            uint32_t size = buffer.readUnsignedVarInt();
            if (size > 0) {
                (void)buffer.readBytes(size);
            }
            (void)tag;
        }

        brokers_.push_back(std::move(broker));
    }

    // Cluster authorized operations
    cluster_authorized_operations_ = buffer.readInt32();
    
    // Skip tagged fields
    uint32_t num_tagged_fields = buffer.readUnsignedVarInt();
    for (uint32_t i = 0; i < num_tagged_fields; ++i) {
        uint32_t tag = buffer.readUnsignedVarInt();
        uint32_t size = buffer.readUnsignedVarInt();
        if (size > 0) {
            (void)buffer.readBytes(size);
        }
        (void)tag;
    }
}

// DescribeClusterResponse size
size_t DescribeClusterResponse::size(int16_t api_version) const {
    // This is approximate - flexible versions make exact calculation complex
    size_t total = 4;  // throttle_time_ms
    total += 2;        // error_code
    // Error message (compact nullable)
    total += 1 + (error_message_.empty() ? 0 : error_message_.size());
    total += 1;        // endpoint_type
    // Cluster ID (compact)
    total += 1 + cluster_id_.size();
    total += 4;        // controller_id
    // Brokers (compact array)
    total += 1;  // array length varint
    for (const auto& broker : brokers_) {
        total += 4;  // id
        total += 1 + broker.host.size();  // host (compact)
        total += 4;  // port
        total += 1 + (broker.rack.has_value() ? broker.rack->size() : 0);  // rack (compact nullable)
        total += 1;  // broker tagged fields
    }
    total += 4;  // cluster_authorized_operations
    total += 1;  // response tagged fields
    (void)api_version;
    return total;
}

}  // namespace kawasan::protocol
