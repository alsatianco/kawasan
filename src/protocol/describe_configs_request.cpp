#include "kawasan/protocol/describe_configs_request.h"

#include <optional>

namespace {

std::string readCompactString(kawasan::Buffer& buffer) {
    const uint32_t length_plus_one = buffer.readUnsignedVarInt();
    if (length_plus_one == 0) {
        throw kawasan::ProtocolException("Compact string cannot be null");
    }
    const auto bytes = buffer.readBytes(length_plus_one - 1);
    return std::string(bytes.begin(), bytes.end());
}

std::optional<std::string> readCompactNullableString(kawasan::Buffer& buffer) {
    const uint32_t length_plus_one = buffer.readUnsignedVarInt();
    if (length_plus_one == 0) {
        return std::nullopt;
    }
    const auto bytes = buffer.readBytes(length_plus_one - 1);
    return std::string(bytes.begin(), bytes.end());
}

void writeCompactString(kawasan::Buffer& buffer, const std::string& value) {
    buffer.writeUnsignedVarInt(static_cast<uint32_t>(value.size() + 1));
    if (!value.empty()) {
        buffer.writeBytes(reinterpret_cast<const uint8_t*>(value.data()),
                          value.size());
    }
}

void writeCompactNullableString(kawasan::Buffer& buffer,
                                const std::optional<std::string>& value) {
    if (value) {
        writeCompactString(buffer, *value);
    } else {
        buffer.writeUnsignedVarInt(0);
    }
}

int32_t readCompactArrayLength(kawasan::Buffer& buffer,
                               const char* field_name) {
    const uint32_t length_plus_one = buffer.readUnsignedVarInt();
    if (length_plus_one == 0) {
        throw kawasan::ProtocolException(
            std::string(field_name) + " compact array cannot be null");
    }
    return static_cast<int32_t>(length_plus_one - 1);
}

void skipTaggedFields(kawasan::Buffer& buffer) {
    const uint32_t tagged_fields = buffer.readUnsignedVarInt();
    for (uint32_t i = 0; i < tagged_fields; ++i) {
        const uint32_t tag_id = buffer.readUnsignedVarInt();
        const uint32_t size = buffer.readUnsignedVarInt();
        if (size > 0) {
            (void)buffer.readBytes(size);
        }
        (void)tag_id;
    }
}

void writeEmptyTaggedFields(kawasan::Buffer& buffer) {
    buffer.writeUnsignedVarInt(0);
}

}  // namespace

namespace kawasan::protocol {

// DescribeConfigsRequest encode
void DescribeConfigsRequest::encode(Buffer& buffer, int16_t api_version) const {
    const bool flexible = api_version >= 4;

    auto write_resource_name = [&](const std::string& name) {
        if (flexible) {
            writeCompactString(buffer, name);
        } else {
            buffer.writeString(name);
        }
    };

    auto write_config_names = [&](const std::vector<std::string>& names) {
        if (flexible) {
            if (names.empty()) {
                // Null array means return all configs
                buffer.writeUnsignedVarInt(0);
            } else {
                buffer.writeUnsignedVarInt(static_cast<uint32_t>(names.size() + 1));
                for (const auto& name : names) {
                    writeCompactString(buffer, name);
                }
            }
        } else {
            if (names.empty()) {
                buffer.writeInt32(-1);
            } else {
                buffer.writeInt32(static_cast<int32_t>(names.size()));
                for (const auto& name : names) {
                    buffer.writeString(name);
                }
            }
        }
    };

    if (flexible) {
        buffer.writeUnsignedVarInt(static_cast<uint32_t>(resources_.size() + 1));
    } else {
        buffer.writeInt32(static_cast<int32_t>(resources_.size()));
    }

    for (const auto& resource : resources_) {
        buffer.writeInt8(static_cast<int8_t>(resource.resource_type));
        write_resource_name(resource.resource_name);
        write_config_names(resource.config_names);
        if (flexible) {
            writeEmptyTaggedFields(buffer);  // Resource tagged fields
        }
    }

    if (api_version >= 1) {
        buffer.writeInt8(include_synonyms_ ? 1 : 0);
    }

    if (api_version >= 3) {
        buffer.writeInt8(include_documentation_ ? 1 : 0);
    }

    if (flexible) {
        writeEmptyTaggedFields(buffer);  // Request tagged fields
    }
}

// DescribeConfigsRequest decode
void DescribeConfigsRequest::decode(Buffer& buffer, int16_t api_version) {
    const bool flexible_version = api_version >= 4;
    bool use_flexible = flexible_version;

    auto read_resource_name = [&]() -> std::string {
        return use_flexible ? readCompactString(buffer) : buffer.readString();
    };

    auto read_config_names = [&](ConfigResource& resource) {
        if (use_flexible) {
            const uint32_t count_plus_one = buffer.readUnsignedVarInt();
            if (count_plus_one > 0) {
                const int32_t config_count =
                    static_cast<int32_t>(count_plus_one - 1);
                resource.config_names.reserve(config_count);
                for (int32_t j = 0; j < config_count; ++j) {
                    resource.config_names.push_back(readCompactString(buffer));
                }
            } else {
                resource.config_names.clear();
            }
        } else {
            const int32_t config_count = buffer.readInt32();
            if (config_count > 0) {
                resource.config_names.reserve(config_count);
                for (int32_t j = 0; j < config_count; ++j) {
                    resource.config_names.push_back(buffer.readString());
                }
            } else {
                resource.config_names.clear();
            }
        }
    };

    int32_t resource_count = 0;
    if (use_flexible) {
        const size_t start_pos = buffer.position();
        const uint32_t count_plus_one = buffer.readUnsignedVarInt();
        if (count_plus_one == 0) {
            // Some clients still send legacy-encoded bodies for v4; fall back.
            use_flexible = false;
            buffer.setPosition(start_pos);
        } else {
            resource_count = static_cast<int32_t>(count_plus_one - 1);
        }
    }

    if (!use_flexible) {
        resource_count = buffer.readInt32();
    }

    resources_.clear();
    resources_.reserve(resource_count);

    for (int32_t i = 0; i < resource_count; ++i) {
        ConfigResource resource;
        resource.resource_type =
            static_cast<ConfigResourceType>(buffer.readInt8());
        resource.resource_name = read_resource_name();
        read_config_names(resource);
        resources_.push_back(std::move(resource));

        if (use_flexible) {
            skipTaggedFields(buffer);  // Resource tagged fields
        }
    }

    if (api_version >= 1) {
        include_synonyms_ = buffer.readInt8() != 0;
    } else {
        include_synonyms_ = false;
    }

    if (api_version >= 3) {
        include_documentation_ = buffer.readInt8() != 0;
    } else {
        include_documentation_ = false;
    }

    if (use_flexible) {
        skipTaggedFields(buffer);  // Request tagged fields
    }
}

// DescribeConfigsRequest size
size_t DescribeConfigsRequest::size(int16_t api_version) const {
    const bool flexible = api_version >= 4;
    size_t total = flexible ? 1 : 4;  // resources array length (varint vs int32)

    for (const auto& resource : resources_) {
        total += 1;  // resource_type
        total += (flexible ? 1 : 2) + resource.resource_name.size();

        if (flexible) {
            total += 1;  // config_names array length varint
            for (const auto& name : resource.config_names) {
                total += 1 + name.size();
            }
            total += 1;  // resource tagged fields
        } else {
            total += 4;  // config_names array length
            for (const auto& name : resource.config_names) {
                total += 2 + name.size();
            }
        }
    }

    if (api_version >= 1) {
        total += 1;  // include_synonyms
    }

    if (api_version >= 3) {
        total += 1;  // include_documentation
    }

    if (flexible) {
        total += 1;  // tagged fields (empty)
    }

    return total;
}

// DescribeConfigsResponse encode
void DescribeConfigsResponse::encode(Buffer& buffer, int16_t api_version) const {
    const bool flexible = api_version >= 4;

    // Throttle time (v0+)
    buffer.writeInt32(throttle_time_ms_);
    
    // Results array
    if (flexible) {
        buffer.writeUnsignedVarInt(static_cast<uint32_t>(results_.size() + 1));
    } else {
        buffer.writeInt32(static_cast<int32_t>(results_.size()));
    }
    for (const auto& result : results_) {
        buffer.writeInt16(static_cast<int16_t>(result.error_code));
        if (flexible) {
            auto error_msg = result.error_message.empty()
                                 ? std::optional<std::string>{}
                                 : std::optional<std::string>(result.error_message);
            writeCompactNullableString(buffer, error_msg);
        } else {
            buffer.writeNullableString(
                result.error_message.empty()
                    ? std::optional<std::string>{}
                    : std::optional<std::string>(result.error_message));
        }
        buffer.writeInt8(static_cast<int8_t>(result.resource_type));
        if (flexible) {
            writeCompactString(buffer, result.resource_name);
        } else {
            buffer.writeString(result.resource_name);
        }
        
        // Config entries
        if (flexible) {
            buffer.writeUnsignedVarInt(
                static_cast<uint32_t>(result.configs.size() + 1));
        } else {
            buffer.writeInt32(static_cast<int32_t>(result.configs.size()));
        }
        for (const auto& config : result.configs) {
            if (flexible) {
                writeCompactString(buffer, config.name);
                writeCompactNullableString(
                    buffer,
                    config.value.empty()
                        ? std::optional<std::string>{}
                        : std::optional<std::string>(config.value));
            } else {
                buffer.writeString(config.name);
                buffer.writeNullableString(
                    config.value.empty()
                        ? std::optional<std::string>{}
                        : std::optional<std::string>(config.value));
            }
            buffer.writeInt8(config.read_only ? 1 : 0);
            
            if (api_version >= 1) {
                // Config source (we'll use DEFAULT_CONFIG = 4)
                buffer.writeInt8(config.is_default ? 4 : 5);
            } else {
                buffer.writeInt8(config.is_default ? 1 : 0);
            }
            
            buffer.writeInt8(config.is_sensitive ? 1 : 0);
            
            // Synonyms (v1+) - empty array
            if (api_version >= 1) {
                if (flexible) {
                    buffer.writeUnsignedVarInt(1);
                } else {
                    buffer.writeInt32(0);
                }
            }
            
            // Config type (v3+) - UNKNOWN = 0
            if (api_version >= 3) {
                buffer.writeInt8(0);
            }
            
            // Documentation (v3+) - nullable string
            if (api_version >= 3) {
                if (flexible) {
                    writeCompactNullableString(buffer, std::nullopt);
                } else {
                    buffer.writeInt16(-1);  // null
                }
            }

            if (flexible) {
                writeEmptyTaggedFields(buffer);  // Config tagged fields
            }
        }

        if (flexible) {
            writeEmptyTaggedFields(buffer);  // Result tagged fields
        }
    }

    if (flexible) {
        writeEmptyTaggedFields(buffer);  // Response tagged fields
    }
}

// DescribeConfigsResponse decode
void DescribeConfigsResponse::decode(Buffer& buffer, int16_t api_version) {
    const bool flexible = api_version >= 4;

    // Throttle time
    throttle_time_ms_ = buffer.readInt32();
    
    // Results array
    int32_t result_count = 0;
    if (flexible) {
        result_count = readCompactArrayLength(buffer, "DescribeConfigs results");
    } else {
        result_count = buffer.readInt32();
        if (result_count < 0) {
            result_count = 0;
        }
    }
    results_.clear();
    results_.reserve(result_count);
    
    for (int32_t i = 0; i < result_count; ++i) {
        DescribeConfigsResourceResult result;
        result.error_code = static_cast<ErrorCode>(buffer.readInt16());
        if (flexible) {
            auto error_msg = readCompactNullableString(buffer);
            result.error_message = error_msg.value_or("");
        } else {
            auto error_msg = buffer.readNullableString();
            result.error_message = error_msg.value_or("");
        }
        result.resource_type = static_cast<ConfigResourceType>(buffer.readInt8());
        result.resource_name = flexible ? readCompactString(buffer)
                                        : buffer.readString();
        
        // Config entries
        int32_t config_count = 0;
        if (flexible) {
            config_count =
                readCompactArrayLength(buffer, "DescribeConfigs configs");
        } else {
            config_count = buffer.readInt32();
            if (config_count < 0) {
                config_count = 0;
            }
        }
        result.configs.reserve(config_count);
        
        for (int32_t j = 0; j < config_count; ++j) {
            ConfigEntry config;
            config.name =
                flexible ? readCompactString(buffer) : buffer.readString();
            if (flexible) {
                auto value = readCompactNullableString(buffer);
                config.value = value.value_or("");
            } else {
                auto value = buffer.readNullableString();
                config.value = value.value_or("");
            }
            config.read_only = buffer.readInt8() != 0;
            
            if (api_version >= 1) {
                int8_t source = buffer.readInt8();
                config.is_default = (source == 4);
            } else {
                config.is_default = buffer.readInt8() != 0;
            }
            
            config.is_sensitive = buffer.readInt8() != 0;
            
            // Synonyms (v1+)
            if (api_version >= 1) {
                if (flexible) {
                    const uint32_t synonym_count_plus_one =
                        buffer.readUnsignedVarInt();
                    if (synonym_count_plus_one > 0) {
                        const int32_t synonym_count =
                            static_cast<int32_t>(synonym_count_plus_one - 1);
                        for (int32_t k = 0; k < synonym_count; ++k) {
                            (void)readCompactString(buffer);      // name
                            (void)readCompactNullableString(buffer);  // value
                            (void)buffer.readInt8();              // source
                            skipTaggedFields(buffer);             // synonym tags
                        }
                    }
                } else {
                    int32_t synonym_count = buffer.readInt32();
                    // Skip synonyms for now
                    for (int32_t k = 0; k < synonym_count; ++k) {
                        buffer.readString();         // name
                        buffer.readNullableString(); // value
                        buffer.readInt8();           // source
                    }
                }
            }
            
            // Config type (v3+)
            if (api_version >= 3) {
                buffer.readInt8();
            }
            
            // Documentation (v3+)
            if (api_version >= 3) {
                if (flexible) {
                    (void)readCompactNullableString(buffer);
                } else {
                    int16_t doc_len = buffer.readInt16();
                    if (doc_len > 0) {
                        buffer.readBytes(doc_len);
                    }
                }
            }

            if (flexible) {
                skipTaggedFields(buffer);  // Config tagged fields
            }
            
            result.configs.push_back(std::move(config));
        }
        
        if (flexible) {
            skipTaggedFields(buffer);  // Result tagged fields
        }

        results_.push_back(std::move(result));
    }

    if (flexible) {
        skipTaggedFields(buffer);  // Response tagged fields
    }
}

// DescribeConfigsResponse size
size_t DescribeConfigsResponse::size(int16_t api_version) const {
    const bool flexible = api_version >= 4;
    size_t total = 4;  // throttle_time_ms
    total += flexible ? 1 : 4;        // results array length
    
    for (const auto& result : results_) {
        total += 2;  // error_code
        total += (flexible ? 1 : 2) +
                 result.error_message.size();  // error_message
        total += 1;  // resource_type
        total += (flexible ? 1 : 2) + result.resource_name.size();
        total += flexible ? 1 : 4;  // configs array length
        
        for (const auto& config : result.configs) {
            total += (flexible ? 1 : 2) + config.name.size();   // name
            total += (flexible ? 1 : 2) + config.value.size();  // value
            total += 1;  // read_only
            
            if (api_version >= 1) {
                total += 1;  // config source
                total += flexible ? 1 : 4;  // synonyms array (empty)
            } else {
                total += 1;  // is_default
            }
            
            total += 1;  // is_sensitive
            
            if (api_version >= 3) {
                total += 1;  // config type
                total += (flexible ? 1 : 2);  // documentation (null)
            }

            if (flexible) {
                total += 1;  // Config tagged fields
            }
        }

        if (flexible) {
            total += 1;  // Result tagged fields
        }
    }
    
    if (flexible) {
        total += 1;  // Response tagged fields
    }

    return total;
}

}  // namespace kawasan::protocol
