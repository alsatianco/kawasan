#include "kawasan/broker/request_dispatcher.h"

#include <arpa/inet.h>

#include <cstring>

#include <fmt/format.h>

#include "kawasan/common/logger.h"

namespace kawasan::broker {

RequestDispatcher::RequestDispatcher(std::shared_ptr<metrics::RequestMetrics> metrics)
    : metrics_(std::move(metrics)) {}

void RequestDispatcher::registerHandler(protocol::ApiKey api_key, int16_t min_version,
                                        int16_t max_version, HandlerFunc handler,
                                        ErrorBuilder error_builder) {
    handlers_[static_cast<int16_t>(api_key)].push_back(
        HandlerRegistration{min_version, max_version, std::move(handler),
                            std::move(error_builder)});
}

RequestDispatcher::DispatchResult RequestDispatcher::dispatch(
    RequestContext context) {
    context.payload_start = context.payload.position();
    const auto key_value = static_cast<int16_t>(context.header.apiKey());
    const auto handlers = findHandlers(context.header.apiKey());

    auto build_and_record = [&](const Buffer& payload, bool close_connection,
                                bool suppress_response) {
        return finalize(context.header.apiKey(), context.frame_size_bytes,
                        HandlerResult{payload, close_connection, suppress_response, false});
    };

    if (!handlers) {
        Logger::warn("Peer {} sent unsupported API key {}",
                     context.peer_identity, key_value);
        auto payload =
            buildLegacyErrorPayload(context, ErrorCode::UNSUPPORTED_VERSION);
        return build_and_record(payload, false, false);
    }

    const auto version = context.header.apiVersion();
    const auto registration = selectHandler(*handlers, version);
    if (!registration) {
        Logger::warn("Peer {} used unsupported version {} for API key {}",
                     context.peer_identity, version, key_value);
        auto payload =
            buildErrorPayload(&handlers->front(), context,
                              ErrorCode::UNSUPPORTED_VERSION, handlers->front().max_version);
        return build_and_record(payload, false, false);
    }

    if (context.deferred_sink) {
        context.complete = [this, sink = context.deferred_sink, api_key = context.header.apiKey(),
                            request_bytes = context.frame_size_bytes](HandlerResult result) {
            sink(finalize(api_key, request_bytes, result));
        };
    }

    try {
        auto handler_result = registration->handler(context);
        if (handler_result.deferred) {
            DispatchResult deferred;
            deferred.deferred = true;
            return deferred;
        }
        return build_and_record(handler_result.payload, handler_result.close_connection,
                                handler_result.suppress_response);
    } catch (const ProtocolException& ex) {
        Logger::warn("Protocol error while handling API {} from {}: {}",
                     key_value, context.peer_identity, ex.what());
        // Protocol exceptions indicate malformed requests - close connection
        // only if it's a critical protocol violation
        auto payload = buildErrorPayload(registration, context,
                                         ErrorCode::INVALID_REQUEST,
                                         context.header.apiVersion());
        return build_and_record(payload, true, false);
    } catch (const KawasanException& ex) {
        Logger::error("Kawasan error while handling API {} from {}: {}",
                      key_value, context.peer_identity, ex.what());
        // Kawasan exceptions are typically recoverable - return error response
        auto payload =
            buildErrorPayload(registration, context, ex.code(),
                              context.header.apiVersion());
        return build_and_record(payload, false, false);
    } catch (const std::runtime_error& ex) {
        // Runtime errors (like buffer underflow) are often recoverable
        // Don't close connection unless it's clearly a protocol violation
        Logger::warn("Runtime error while handling API {} from {}: {}",
                     key_value, context.peer_identity, ex.what());
        auto payload = buildErrorPayload(registration, context,
                                         ErrorCode::INVALID_REQUEST,
                                         context.header.apiVersion());
        // Only close connection for buffer underflow or similar critical errors
        const bool is_critical = std::string(ex.what()).find("underflow") != std::string::npos ||
                                 std::string(ex.what()).find("overflow") != std::string::npos;
        return build_and_record(payload, is_critical, false);
    } catch (const std::exception& ex) {
        // Other exceptions - be conservative and don't close connection
        // unless we're certain it's a fatal protocol error
        Logger::error("Unexpected error while handling API {} from {}: {}",
                      key_value, context.peer_identity, ex.what());
        auto payload = buildErrorPayload(registration, context,
                                         ErrorCode::BROKER_NOT_AVAILABLE,
                                         context.header.apiVersion());
        // Don't close connection for unexpected exceptions - let client retry
        return build_and_record(payload, false, false);
    }
}

const RequestDispatcher::HandlerList* RequestDispatcher::findHandlers(
    protocol::ApiKey api_key) const {
    auto it = handlers_.find(static_cast<int16_t>(api_key));
    if (it == handlers_.end() || it->second.empty()) {
        return nullptr;
    }
    return &it->second;
}

const RequestDispatcher::HandlerRegistration* RequestDispatcher::selectHandler(
    const HandlerList& list, int16_t version) const {
    for (const auto& entry : list) {
        if (version >= entry.min_version && version <= entry.max_version) {
            return &entry;
        }
    }
    return nullptr;
}

Buffer RequestDispatcher::buildLegacyErrorPayload(const RequestContext& context,
                                                  ErrorCode code) const {
    Buffer buffer;
    protocol::ResponseHeader header(
        context.header.correlationId(),
        context.header.isFlexibleVersion());
    header.encode(buffer);
    buffer.writeInt16(static_cast<int16_t>(code));
    return buffer;
}

Buffer RequestDispatcher::buildErrorPayload(
    const HandlerRegistration* registration, const RequestContext& context,
    ErrorCode code, int16_t response_version) const {
    if (registration && registration->error_builder) {
        return registration->error_builder(context, code, response_version);
    }
    return buildLegacyErrorPayload(context, code);
}

RequestDispatcher::DispatchResult RequestDispatcher::finalize(protocol::ApiKey api_key,
                                                             size_t request_bytes,
                                                             const HandlerResult& result) const {
    if (result.suppress_response) {
        if (metrics_) {
            metrics_->record(api_key, request_bytes, 0);
        }
        return DispatchResult{{}, result.close_connection, true, false};
    }
    auto frame = wrapFrame(result.payload);
    if (metrics_) {
        metrics_->record(api_key, request_bytes, frame.size());
    }
    return DispatchResult{std::move(frame), result.close_connection, false, false};
}

std::vector<uint8_t> RequestDispatcher::wrapFrame(const Buffer& payload) const {
    const auto payload_size = static_cast<int32_t>(payload.size());
    std::vector<uint8_t> frame(sizeof(int32_t) + payload_size);
    uint32_t net_length = htonl(payload_size);
    std::memcpy(frame.data(), &net_length, sizeof(net_length));
    if (payload_size > 0) {
        std::memcpy(frame.data() + sizeof(int32_t), payload.data(), payload.size());
    }
    return frame;
}

}  // namespace kawasan::broker
