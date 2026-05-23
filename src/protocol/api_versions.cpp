#include "kawasan/protocol/api_versions.h"

#include <spdlog/fmt/fmt.h>

#include "kawasan/common/error.h"

namespace kawasan::protocol {
namespace {

std::string readCompactString(Buffer& buffer, bool allow_null = false) {
    // Check if we have enough data to read the varint length
    if (buffer.remaining() < 1) {
        throw ProtocolException("Insufficient data for compact string length");
    }
    
    const uint32_t length_plus_one = buffer.readUnsignedVarInt();
    if (length_plus_one == 0) {
        if (!allow_null) {
            throw ProtocolException("Compact string cannot be null");
        }
        // Treat null values as an empty string so legacy clients can omit fields.
        return {};
    }
    const uint32_t length = length_plus_one - 1;
    
    // Check if we have enough data for the string content
    if (buffer.remaining() < length) {
        throw ProtocolException("Insufficient data for compact string content");
    }
    
    const auto bytes = buffer.readBytes(length);
    return std::string(bytes.begin(), bytes.end());
}

void writeCompactString(Buffer& buffer, const std::string& value) {
    const uint32_t length = static_cast<uint32_t>(value.size());
    buffer.writeUnsignedVarInt(length + 1);
    if (!value.empty()) {
        buffer.writeBytes(reinterpret_cast<const uint8_t*>(value.data()), length);
    }
}

void skipTaggedFields(Buffer& buffer) {
    // Check if we have enough data to read the tagged fields count
    if (buffer.remaining() < 1) {
        throw ProtocolException("Insufficient data for tagged fields count");
    }
    
    const uint32_t tagged_fields = buffer.readUnsignedVarInt();
    for (uint32_t i = 0; i < tagged_fields; ++i) {
        // Check if we have enough data for tag id
        if (buffer.remaining() < 1) {
            throw ProtocolException(fmt::format(
                "Insufficient data for tagged field {} tag id (need 1, have {})",
                i, buffer.remaining()));
        }
        const uint32_t tag_id = buffer.readUnsignedVarInt();
        
        // Check if we have enough data for size
        if (buffer.remaining() < 1) {
            throw ProtocolException(fmt::format(
                "Insufficient data for tagged field {} size (need 1, have {})",
                i, buffer.remaining()));
        }
        const uint32_t size = buffer.readUnsignedVarInt();
        if (size > 0) {
            // Check if we have enough data for the tagged field content
            if (buffer.remaining() < size) {
                throw ProtocolException(fmt::format(
                    "Insufficient data for tagged field {} content (tag_id={}, need {}, have {})",
                    i, tag_id, size, buffer.remaining()));
            }
            (void)buffer.readBytes(size);
        }
    }
}

void writeEmptyTaggedFields(Buffer& buffer) {
    buffer.writeUnsignedVarInt(0);
}

int32_t readArrayLength(Buffer& buffer, int16_t api_version) {
    if (api_version >= 3) {
        const uint32_t length_plus_one = buffer.readUnsignedVarInt();
        if (length_plus_one == 0) {
            throw ProtocolException("Compact array cannot be null");
        }
        return static_cast<int32_t>(length_plus_one - 1);
    }
    return buffer.readInt32();
}

void writeArrayLength(Buffer& buffer, int16_t api_version, size_t size) {
    if (api_version >= 3) {
        buffer.writeUnsignedVarInt(static_cast<uint32_t>(size + 1));
    } else {
        buffer.writeInt32(static_cast<int32_t>(size));
    }
}

}  // namespace

bool isValidClientSoftwareString(const std::string& str) {
    // According to KIP-511, client.software.name and client.software.version
    // should only contain letters, digits, hyphens, underscores, and periods.
    // Other special characters are NOT allowed.
    if (str.empty()) {
        return true;  // Empty strings are allowed
    }
    for (char c : str) {
        if (!std::isalnum(static_cast<unsigned char>(c)) && 
            c != '-' && c != '_' && c != '.') {
            return false;
        }
    }
    return true;
}

void ApiVersionsRequest::encode(Buffer& buffer, int16_t api_version) const {
    if (api_version < 0 || api_version > 4) {
        throw ProtocolException("Unsupported ApiVersions request version");
    }
    if (api_version >= 3) {
        writeCompactString(buffer, client_software_name_);
        writeCompactString(buffer, client_software_version_);
        writeEmptyTaggedFields(buffer);
    }
}

void ApiVersionsRequest::decode(Buffer& buffer, int16_t api_version) {
    if (api_version < 0 || api_version > 4) {
        throw ProtocolException("Unsupported ApiVersions request version");
    }
    if (api_version >= 3) {
        // Validate we have enough buffer data before attempting to decode
        // Each compact string needs at least 1 byte for length, and tagged fields need at least 1 byte
        if (buffer.remaining() < 3) {
            throw ProtocolException("Insufficient data for API versions request v3+");
        }
        
        try {
            client_software_name_ = readCompactString(buffer, /*allow_null=*/true);
            client_software_version_ =
                readCompactString(buffer, /*allow_null=*/true);
            skipTaggedFields(buffer);
        } catch (const ProtocolException&) {
            // Re-throw protocol exceptions as-is
            throw;
        } catch (const std::exception& ex) {
            // Wrap other exceptions as protocol exceptions for consistent error handling
            throw ProtocolException(std::string("Failed to decode API versions request: ") + ex.what());
        }
    } else {
        client_software_name_.clear();
        client_software_version_.clear();
    }
}

void ApiVersionsResponse::encode(Buffer& buffer, int16_t api_version) const {
    if (api_version < 0 || api_version > 4) {
        throw ProtocolException("Unsupported ApiVersions response version");
    }

    buffer.writeInt16(static_cast<int16_t>(error_code_));

    writeArrayLength(buffer, api_version, api_versions_.size());
    for (const auto& version : api_versions_) {
        buffer.writeInt16(static_cast<int16_t>(version.api_key));
        buffer.writeInt16(version.min_version);
        buffer.writeInt16(version.max_version);
        // Tagged fields for each API version entry (v3+)
        if (api_version >= 3) {
            writeEmptyTaggedFields(buffer);
        }
    }

    if (api_version >= 1) {
        buffer.writeInt32(throttle_time_ms_);
    }

    // Tagged fields for the response (v3+)
    if (api_version >= 3) {
        writeEmptyTaggedFields(buffer);
    }
}

void ApiVersionsResponse::decode(Buffer& buffer, int16_t api_version) {
    if (api_version < 0 || api_version > 3) {
        throw ProtocolException("Unsupported ApiVersions response version");
    }

    error_code_ = static_cast<ErrorCode>(buffer.readInt16());

    const int32_t count = readArrayLength(buffer, api_version);
    api_versions_.resize(count);
    for (int32_t i = 0; i < count; ++i) {
        api_versions_[i].api_key = static_cast<ApiKey>(buffer.readInt16());
        api_versions_[i].min_version = buffer.readInt16();
        api_versions_[i].max_version = buffer.readInt16();
    }

    if (api_version >= 1) {
        throttle_time_ms_ = buffer.readInt32();
    }

    if (api_version >= 3) {
        skipTaggedFields(buffer);
    }
}

}  // namespace kawasan::protocol
