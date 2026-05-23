#include "kawasan/protocol/alter_configs_request.h"

namespace kawasan::protocol {

void AlterConfigsRequest::encode(Buffer& buffer, int16_t api_version) const {
    const bool flex = api_version >= 2;

    if (flex) buffer.writeCompactArrayLen(static_cast<int32_t>(resources_.size()));
    else buffer.writeInt32(static_cast<int32_t>(resources_.size()));
    for (const auto& r : resources_) {
        buffer.writeInt8(static_cast<int8_t>(r.resource_type));
        if (flex) buffer.writeCompactString(r.resource_name);
        else buffer.writeString(r.resource_name);

        if (flex) buffer.writeCompactArrayLen(static_cast<int32_t>(r.configs.size()));
        else buffer.writeInt32(static_cast<int32_t>(r.configs.size()));
        for (const auto& c : r.configs) {
            if (flex) buffer.writeCompactString(c.name);
            else buffer.writeString(c.name);
            if (flex) buffer.writeCompactNullableString(c.value);
            else buffer.writeNullableString(c.value);
            if (flex) buffer.writeEmptyTaggedFields();
        }
        if (flex) buffer.writeEmptyTaggedFields();
    }
    buffer.writeInt8(validate_only_ ? 1 : 0);
    if (flex) buffer.writeEmptyTaggedFields();
}

void AlterConfigsRequest::decode(Buffer& buffer, int16_t api_version) {
    const bool flex = api_version >= 2;

    int32_t rc = flex ? buffer.readCompactArrayLen() : buffer.readInt32();
    resources_.clear();
    resources_.resize(rc < 0 ? 0 : rc);
    for (int32_t i = 0; i < rc; ++i) {
        resources_[i].resource_type = static_cast<ConfigResourceType>(buffer.readInt8());
        resources_[i].resource_name =
            flex ? buffer.readCompactString() : buffer.readString();

        int32_t cc = flex ? buffer.readCompactArrayLen() : buffer.readInt32();
        resources_[i].configs.resize(cc < 0 ? 0 : cc);
        for (int32_t j = 0; j < cc; ++j) {
            resources_[i].configs[j].name =
                flex ? buffer.readCompactString() : buffer.readString();
            resources_[i].configs[j].value =
                flex ? buffer.readCompactNullableString() : buffer.readNullableString();
            if (flex) buffer.skipTaggedFields();
        }
        if (flex) buffer.skipTaggedFields();
    }
    validate_only_ = (buffer.readInt8() != 0);
    if (flex) buffer.skipTaggedFields();
}

void AlterConfigsResponse::encode(Buffer& buffer, int16_t api_version) const {
    const bool flex = api_version >= 2;
    buffer.writeInt32(throttle_time_ms_);
    if (flex) buffer.writeCompactArrayLen(static_cast<int32_t>(results_.size()));
    else buffer.writeInt32(static_cast<int32_t>(results_.size()));
    for (const auto& r : results_) {
        buffer.writeInt16(static_cast<int16_t>(r.error_code));
        const auto msg = r.error_message.empty()
                             ? std::optional<std::string>{}
                             : std::optional<std::string>(r.error_message);
        if (flex) buffer.writeCompactNullableString(msg);
        else buffer.writeNullableString(msg);
        buffer.writeInt8(static_cast<int8_t>(r.resource_type));
        if (flex) buffer.writeCompactString(r.resource_name);
        else buffer.writeString(r.resource_name);
        if (flex) buffer.writeEmptyTaggedFields();
    }
    if (flex) buffer.writeEmptyTaggedFields();
}

void AlterConfigsResponse::decode(Buffer& buffer, int16_t api_version) {
    const bool flex = api_version >= 2;
    throttle_time_ms_ = buffer.readInt32();
    int32_t rc = flex ? buffer.readCompactArrayLen() : buffer.readInt32();
    results_.clear();
    results_.resize(rc < 0 ? 0 : rc);
    for (int32_t i = 0; i < rc; ++i) {
        results_[i].error_code = static_cast<ErrorCode>(buffer.readInt16());
        auto msg = flex ? buffer.readCompactNullableString() : buffer.readNullableString();
        results_[i].error_message = msg.value_or("");
        results_[i].resource_type = static_cast<ConfigResourceType>(buffer.readInt8());
        results_[i].resource_name = flex ? buffer.readCompactString() : buffer.readString();
        if (flex) buffer.skipTaggedFields();
    }
    if (flex) buffer.skipTaggedFields();
}

}  // namespace kawasan::protocol
