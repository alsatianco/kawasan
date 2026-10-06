#include "kawasan/broker/request_dispatcher.h"

#include <arpa/inet.h>
#include <fmt/format.h>

#include <atomic>
#include <cstring>

#include "kawasan/common/logger.h"

namespace kawasan::broker {

RequestDispatcher::RequestDispatcher(std::shared_ptr<metrics::RequestMetrics> metrics)
    : metrics_(std::move(metrics)) {}

void RequestDispatcher::registerHandler(protocol::ApiKey api_key, int16_t min_version,
                                        int16_t max_version, HandlerFunc handler,
                                        ErrorBuilder error_builder) {
    handlers_[static_cast<int16_t>(api_key)].push_back(HandlerRegistration{
        min_version, max_version, std::move(handler), std::move(error_builder)});
}

RequestDispatcher::DispatchResult RequestDispatcher::dispatch(RequestContext context) {
    context.payload_start = context.payload.position();
    const auto key_value = static_cast<int16_t>(context.header.apiKey());
    const auto handlers = findHandlers(context.header.apiKey());

    auto build_and_record = [&](const Buffer& payload, bool close_connection,
                                bool suppress_response) {
        HandlerResult result;
        result.payload = payload;
        result.close_connection = close_connection;
        result.suppress_response = suppress_response;
        return finalize(metrics_, context.header.apiKey(), context.frame_size_bytes, result);
    };

    if (!handlers) {
        Logger::warn("Peer {} sent unsupported API key {}", context.peer_identity, key_value);
        auto payload = buildLegacyErrorPayload(context, ErrorCode::UNSUPPORTED_VERSION);
        return build_and_record(payload, false, false);
    }

    const auto version = context.header.apiVersion();
    const auto registration = selectHandler(*handlers, version);
    if (!registration) {
        Logger::warn("Peer {} used unsupported version {} for API key {}", context.peer_identity,
                     version, key_value);
        auto payload =
            buildErrorPayload(&handlers->front(), context, ErrorCode::UNSUPPORTED_VERSION,
                              handlers->front().max_version);
        return build_and_record(payload, false, false);
    }

    if (context.deferred_sink) {
        context.complete = [metrics = metrics_, sink = context.deferred_sink,
                            completed = std::make_shared<std::atomic<bool>>(false),
                            api_key = context.header.apiKey(),
                            request_bytes = context.frame_size_bytes](HandlerResult result) {
            if (!completed->exchange(true))
                sink(finalize(metrics, api_key, request_bytes, result));
        };
    }

    try {
        auto handler_result = registration->handler(context);
        if (handler_result.deferred) {
            DispatchResult deferred;
            deferred.deferred = true;
            return deferred;
        }
        return finalize(metrics_, context.header.apiKey(), context.frame_size_bytes,
                        handler_result);
    } catch (const ProtocolException& ex) {
        Logger::warn("Protocol error while handling API {} from {}: {}", key_value,
                     context.peer_identity, ex.what());
        // Protocol exceptions indicate malformed requests - close connection
        // only if it's a critical protocol violation
        auto payload = buildErrorPayload(registration, context, ErrorCode::INVALID_REQUEST,
                                         context.header.apiVersion());
        return build_and_record(payload, true, false);
    } catch (const KawasanException& ex) {
        Logger::error("Kawasan error while handling API {} from {}: {}", key_value,
                      context.peer_identity, ex.what());
        // Kawasan exceptions are typically recoverable - return error response
        auto payload =
            buildErrorPayload(registration, context, ex.code(), context.header.apiVersion());
        return build_and_record(payload, false, false);
    } catch (const std::runtime_error& ex) {
        // Runtime errors (like buffer underflow) are often recoverable
        // Don't close connection unless it's clearly a protocol violation
        Logger::warn("Runtime error while handling API {} from {}: {}", key_value,
                     context.peer_identity, ex.what());
        auto payload = buildErrorPayload(registration, context, ErrorCode::INVALID_REQUEST,
                                         context.header.apiVersion());
        // Only close connection for buffer underflow or similar critical errors
        const bool is_critical = std::string(ex.what()).find("underflow") != std::string::npos ||
                                 std::string(ex.what()).find("overflow") != std::string::npos;
        return build_and_record(payload, is_critical, false);
    } catch (const std::exception& ex) {
        // Other exceptions - be conservative and don't close connection
        // unless we're certain it's a fatal protocol error
        Logger::error("Unexpected error while handling API {} from {}: {}", key_value,
                      context.peer_identity, ex.what());
        auto payload = buildErrorPayload(registration, context, ErrorCode::BROKER_NOT_AVAILABLE,
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
    protocol::ResponseHeader header(context.header.correlationId(),
                                    context.header.isFlexibleVersion());
    header.encode(buffer);
    buffer.writeInt16(static_cast<int16_t>(code));
    return buffer;
}

Buffer RequestDispatcher::buildErrorPayload(const HandlerRegistration* registration,
                                            const RequestContext& context, ErrorCode code,
                                            int16_t response_version) const {
    if (registration && registration->error_builder) {
        return registration->error_builder(context, code, response_version);
    }
    return buildLegacyErrorPayload(context, code);
}

RequestDispatcher::DispatchResult RequestDispatcher::finalize(
    const std::shared_ptr<metrics::RequestMetrics>& metrics, protocol::ApiKey api_key,
    size_t request_bytes, const HandlerResult& result) {
    DispatchResult dispatch;
    dispatch.close_connection = result.close_connection;
    dispatch.suppress_response = result.suppress_response;
    dispatch.lifetime = result.lifetime;
    dispatch.publication_guard = result.publication_guard;
    if (result.publication_error) {
        dispatch.publication_error = [errors = result.publication_error](ErrorCode status) {
            return wrapFrame(errors(status));
        };
    }
    if (!result.suppress_response)
        dispatch.frame = wrapFrame(result.payload);
    if (metrics && !dispatch.publication_guard)
        metrics->record(api_key, request_bytes, dispatch.frame.size());
    else if (metrics) {
        // Record the actual fenced response (success may become an ownership error).
        dispatch.publication_guard =
            [guard = dispatch.publication_guard, errors = dispatch.publication_error, metrics,
             api_key, request_bytes, bytes = dispatch.frame.size()](const auto& publish) {
                guard([&](ErrorCode status) {
                    metrics->record(
                        api_key, request_bytes,
                        status == ErrorCode::NONE || !errors ? bytes : errors(status).size());
                    publish(status);
                });
            };
    }
    return dispatch;
}

std::vector<uint8_t> RequestDispatcher::wrapFrame(const Buffer& payload) {
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
