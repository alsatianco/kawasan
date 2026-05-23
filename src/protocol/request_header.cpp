#include "kawasan/protocol/request_header.h"

namespace kawasan::protocol {

namespace {

void skipTaggedFields(Buffer& buffer) {
    buffer.skipTaggedFields();
}

void writeEmptyTaggedFields(Buffer& buffer) {
    buffer.writeEmptyTaggedFields();
}

// Determine if this API version uses flexible request header
// According to Kafka protocol: Request headers have separate flex version from request bodies
// For ApiVersionsRequest, the HEADER is NEVER flexible (even for v3+) because
// the client doesn't know the broker's capabilities yet. Only the request BODY uses flexible types.
bool isFlexibleRequestHeader(ApiKey api_key, int16_t api_version) {
    switch (api_key) {
        case ApiKey::API_VERSIONS:
            // ApiVersions keeps the legacy header (with tagged fields in v3+)
            // even though the body switches to flexible encoding.
            return false;
        case ApiKey::METADATA:
            return api_version >= 9;
        case ApiKey::PRODUCE:
            return api_version >= 9;
        case ApiKey::FETCH:
            return api_version >= 12;
        case ApiKey::LIST_OFFSETS:
            // Phase 1.5: ListOffsets v6+ is flexible — header carries
            // tagged_fields, otherwise the body decoder reads one byte too
            // early. Missing this case caused librdkafka's v7 requests to
            // fail with Buffer underflow.
            return api_version >= 6;
        case ApiKey::OFFSET_FETCH:
            return api_version >= 6;
        case ApiKey::OFFSET_COMMIT:
            return api_version >= 8;
        case ApiKey::FIND_COORDINATOR:
            return api_version >= 3;
        case ApiKey::JOIN_GROUP:
            return api_version >= 6;
        case ApiKey::HEARTBEAT:
            return api_version >= 4;
        case ApiKey::LEAVE_GROUP:
            return api_version >= 4;
        case ApiKey::SYNC_GROUP:
            return api_version >= 4;
        case ApiKey::DESCRIBE_GROUPS:
            return api_version >= 5;
        case ApiKey::LIST_GROUPS:
            return api_version >= 3;
        case ApiKey::SASL_HANDSHAKE:
            // Phase EX-8 fix: SaslHandshake stays non-flexible at every
            // version. Its request header has no tagged_fields trailer
            // even at v1 — KIP-152 introduced v1 specifically without
            // flexible versioning (clients pre-negotiate SASL before
            // they know the broker's flexible-versions support).
            return false;
        case ApiKey::SASL_AUTHENTICATE:
            // Phase 4.2: SaslAuthenticate v2+ uses Request Header v2.
            return api_version >= 2;
        case ApiKey::CREATE_TOPICS:
            return api_version >= 5;
        case ApiKey::DELETE_TOPICS:
            return api_version >= 4;
        case ApiKey::INIT_PRODUCER_ID:
            // Phase 1.18: v2+ flexible
            return api_version >= 2;
        case ApiKey::OFFSET_FOR_LEADER_EPOCH:
            // Phase 1.19: v4+ flexible
            return api_version >= 4;
        case ApiKey::ALTER_CONFIGS:
            // Phase 4.1a: v2+ flexible
            return api_version >= 2;
        case ApiKey::INCREMENTAL_ALTER_CONFIGS:
            // Phase 4.1b: v1+ flexible
            return api_version >= 1;
        case ApiKey::DESCRIBE_LOG_DIRS:
            // Phase 4.1e: v2+ flexible
            return api_version >= 2;
        case ApiKey::ALTER_REPLICA_LOG_DIRS:
            // v2+ flexible
            return api_version >= 2;
        case ApiKey::ELECT_LEADERS:
            // v2+ flexible
            return api_version >= 2;
        case ApiKey::DELETE_RECORDS:
            // Phase 4.1d: v2+ flexible
            return api_version >= 2;
        case ApiKey::DELETE_GROUPS:
            // Phase 4.1: v2+ flexible
            return api_version >= 2;
        case ApiKey::CREATE_PARTITIONS:
            // Phase 4.1c: v2+ flexible
            return api_version >= 2;
        case ApiKey::DESCRIBE_PRODUCERS:
            // Phase 4.1j: v0+ flexible
            return true;
        case ApiKey::LIST_TRANSACTIONS:
            // Phase 4.1l: v0+ flexible
            return true;
        case ApiKey::DESCRIBE_TRANSACTIONS:
            // Phase 4.1k: v0+ flexible
            return true;
        case ApiKey::ALTER_PARTITION:
            // v0+ flexible
            return true;
        case ApiKey::DESCRIBE_ACLS:
            // Phase 4.2c: v2+ flexible
            return api_version >= 2;
        case ApiKey::CREATE_ACLS:
            // Phase 4.2c: v2+ flexible
            return api_version >= 2;
        case ApiKey::DELETE_ACLS:
            // Phase 4.2c: v2+ flexible
            return api_version >= 2;
        case ApiKey::DESCRIBE_CONFIGS:
            // v4 uses flexible body but clients send non-flexible header (v1)
            return false;
        case ApiKey::DESCRIBE_CLUSTER:
            return false;  // Uses legacy header (client sends non-flexible header)
        default:
            return false;
    }
}

// Check if request header has tagged fields (Request Header v1+)
// Request header v1 uses regular strings but has tagged fields
// Request header v2 uses compact strings and has tagged fields
bool hasTaggedFieldsInHeader(ApiKey api_key, int16_t api_version) {
    // For ApiVersions, header v1+ is used starting from API version 3
    // Header v1 uses regular (non-compact) strings but has tagged fields
    if (api_key == ApiKey::API_VERSIONS) {
        return api_version >= 3;
    }
    // DESCRIBE_CLUSTER uses Request Header v1 (non-compact string + tagged fields)
    if (api_key == ApiKey::DESCRIBE_CLUSTER) {
        return true;  // v0+ has tagged fields in header
    }
    // DESCRIBE_CONFIGS v4 uses Request Header v1 (non-compact string + tagged fields)
    if (api_key == ApiKey::DESCRIBE_CONFIGS && api_version >= 4) {
        return true;
    }
    // For other APIs, if the header is flexible, it has tagged fields
    return isFlexibleRequestHeader(api_key, api_version);
}

}  // namespace

void RequestHeader::encode(Buffer& buffer) const {
    buffer.writeInt16(static_cast<int16_t>(api_key_));
    buffer.writeInt16(api_version_);
    buffer.writeInt32(correlation_id_);
    
    if (isFlexibleRequestHeader(api_key_, api_version_)) {
        // Request Header v2: regular STRING for client_id + tagged_fields.
        // See decode() for the corresponding spec note.
        buffer.writeString(client_id_);
        writeEmptyTaggedFields(buffer);
    } else {
        // Legacy version: use regular string for client_id
        buffer.writeString(client_id_);
        // Header v2+ has tagged fields even though it's not flexible
        if (hasTaggedFieldsInHeader(api_key_, api_version_)) {
            writeEmptyTaggedFields(buffer);
        }
    }
}

void RequestHeader::decode(Buffer& buffer) {
    // First, always read the fixed fields (api_key, api_version, correlation_id)
    // These are always in the same format regardless of API version
    api_key_ = static_cast<ApiKey>(buffer.readInt16());
    api_version_ = buffer.readInt16();
    correlation_id_ = buffer.readInt32();
    
    // Now we know the api_key and api_version, so we can determine the header format.
    //
    // Per the Kafka wire spec, Request Header v2 ("flexible" header) uses a
    // REGULAR STRING for client_id (NOT a compact string) plus a trailing
    // tagged_fields varint. This is one of Kafka's wire-protocol traps:
    // even though "flexible" usually means "compact strings everywhere,"
    // the header keeps client_id non-compact. librdkafka and the official
    // Kafka Java client both send the regular string form here.
    if (isFlexibleRequestHeader(api_key_, api_version_)) {
        client_id_ = buffer.readString();
        skipTaggedFields(buffer);
    } else {
        // Legacy version: read regular string for client_id
        client_id_ = buffer.readString();
        // Header v2+ has tagged fields even though it's not flexible
        if (hasTaggedFieldsInHeader(api_key_, api_version_)) {
            skipTaggedFields(buffer);
        }
    }
}


size_t RequestHeader::size() const {
    size_t base_size = sizeof(int16_t) +  // api_key
                       sizeof(int16_t) +  // api_version
                       sizeof(int32_t);   // correlation_id
    
    if (isFlexibleRequestHeader(api_key_, api_version_)) {
        // Flexible version: compact string + tagged fields
        // This is an approximation; actual size depends on varint encoding
        base_size += 1 + client_id_.size() + 1;  // varint length + data + varint for tagged fields
    } else {
        // Legacy version: regular string
        base_size += sizeof(int16_t) + client_id_.size();
    }
    
    return base_size;
}

bool RequestHeader::isFlexibleVersion() const {
    return isFlexibleRequestHeader(api_key_, api_version_);
}

bool RequestHeader::isFlexibleResponseHeader() const {
    // Response header v1 has tagged fields
    // This is determined by the API and version, independent of request header format
    switch (api_key_) {
        case ApiKey::API_VERSIONS:
            // API_VERSIONS is special: response ALWAYS uses header v0 (no tagged fields)
            // because client doesn't know broker capabilities yet
            return false;
        case ApiKey::DESCRIBE_CONFIGS:
            // DESCRIBE_CONFIGS v4+ uses response header v1
            return api_version_ >= 4;
        case ApiKey::DESCRIBE_CLUSTER:
            // DESCRIBE_CLUSTER v0+ uses response header v1
            return true;
        case ApiKey::METADATA:
            return api_version_ >= 9;
        case ApiKey::PRODUCE:
            return api_version_ >= 9;
        case ApiKey::FETCH:
            return api_version_ >= 12;
        case ApiKey::LIST_OFFSETS:
            // Phase 1.5: ListOffsets v6+ is flexible
            return api_version_ >= 6;
        case ApiKey::OFFSET_FETCH:
            return api_version_ >= 6;
        case ApiKey::OFFSET_COMMIT:
            return api_version_ >= 8;
        case ApiKey::FIND_COORDINATOR:
            return api_version_ >= 3;
        case ApiKey::JOIN_GROUP:
            return api_version_ >= 6;
        case ApiKey::HEARTBEAT:
            return api_version_ >= 4;
        case ApiKey::LEAVE_GROUP:
            return api_version_ >= 4;
        case ApiKey::SYNC_GROUP:
            return api_version_ >= 4;
        case ApiKey::DESCRIBE_GROUPS:
            return api_version_ >= 5;
        case ApiKey::LIST_GROUPS:
            return api_version_ >= 3;
        case ApiKey::CREATE_TOPICS:
            return api_version_ >= 5;
        case ApiKey::DELETE_TOPICS:
            return api_version_ >= 4;
        case ApiKey::SASL_AUTHENTICATE:
            return api_version_ >= 2;
        case ApiKey::SASL_HANDSHAKE:
            // Response header for SASL_HANDSHAKE remains v0 (no tagged fields)
            return false;
        case ApiKey::INIT_PRODUCER_ID:
            return api_version_ >= 2;
        case ApiKey::OFFSET_FOR_LEADER_EPOCH:
            return api_version_ >= 4;
        case ApiKey::ALTER_CONFIGS:
            return api_version_ >= 2;
        case ApiKey::INCREMENTAL_ALTER_CONFIGS:
            return api_version_ >= 1;
        case ApiKey::DESCRIBE_LOG_DIRS:
            return api_version_ >= 2;
        case ApiKey::ALTER_REPLICA_LOG_DIRS:
            return api_version_ >= 2;
        case ApiKey::ELECT_LEADERS:
            return api_version_ >= 2;
        case ApiKey::DELETE_RECORDS:
            return api_version_ >= 2;
        case ApiKey::DELETE_GROUPS:
            return api_version_ >= 2;
        case ApiKey::CREATE_PARTITIONS:
            return api_version_ >= 2;
        case ApiKey::DESCRIBE_PRODUCERS:
            return true;
        case ApiKey::LIST_TRANSACTIONS:
            return true;
        case ApiKey::DESCRIBE_TRANSACTIONS:
            return true;
        case ApiKey::ALTER_PARTITION:
            return true;
        case ApiKey::DESCRIBE_ACLS:
            return api_version_ >= 2;
        case ApiKey::CREATE_ACLS:
            return api_version_ >= 2;
        case ApiKey::DELETE_ACLS:
            return api_version_ >= 2;
        default:
            return false;
    }
}

void ResponseHeader::encode(Buffer& buffer) const {
    buffer.writeInt32(correlation_id_);
    if (flexible_) {
        // Tagged fields (response header v1)
        buffer.writeUnsignedVarInt(0);
    }
}

void ResponseHeader::decode(Buffer& buffer) {
    correlation_id_ = buffer.readInt32();
    if (flexible_) {
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
}

size_t ResponseHeader::size() const {
    return sizeof(int32_t) + (flexible_ ? 1 : 0);  // correlation_id + optional tagged fields varint
}

}  // namespace kawasan::protocol
