#pragma once

#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "kawasan/broker/metrics/request_metrics.h"
#include "kawasan/common/buffer.h"
#include "kawasan/common/error.h"
#include "kawasan/protocol/api_keys.h"
#include "kawasan/protocol/request_header.h"

namespace kawasan::broker {

class RequestDispatcher {
public:
    /// @brief Per-connection state that persists across requests on the same TCP
    /// connection. Owned by the network session (TcpSession), so it lives and
    /// dies with the connection — no broker-side map to leak or to let a reused
    /// ip:port inherit a prior connection's identity. Carries the authenticated
    /// SASL principal so authorization can be enforced on subsequent requests.
    struct ConnectionContext {
        std::optional<std::string> authenticated_principal;  // e.g. "User:alice"
    };

    struct DispatchResult {
        std::vector<uint8_t> frame;
        bool close_connection = false;
        bool suppress_response = false;
        // The handler parked the request; the response arrives later through
        // RequestContext::deferred_sink. The transport must not read the next
        // request until then (preserves per-connection response ordering).
        bool deferred = false;
    };

    struct HandlerResult {
        Buffer payload;
        bool close_connection = false;
        bool suppress_response = false;
        // See DispatchResult::deferred. A handler may only return deferred=true
        // when RequestContext::complete is set, and must then call it exactly once.
        bool deferred = false;
    };

    struct RequestContext {
        protocol::RequestHeader header;
        Buffer payload;
        // Snapshot taken by dispatch before decoding, so error builders can
        // recover requested group/topic identities after a decoder throws.
        size_t payload_start = 0;
        size_t frame_size_bytes = 0;
        std::string peer_identity;
        // Points to the owning connection's state (nullptr only in unit tests
        // that build a context directly). Valid for the duration of dispatch,
        // and — when deferred_sink is set — for as long as the sink is alive.
        ConnectionContext* connection = nullptr;
        // Set by a transport that supports deferred responses (may be invoked
        // from any thread). Empty when unsupported, e.g. direct handler calls.
        std::function<void(DispatchResult)> deferred_sink;
        // Set by dispatch() when deferred_sink is present: framing + metrics
        // wrapper a deferring handler uses to deliver its eventual response.
        std::function<void(HandlerResult)> complete;
    };

    using HandlerFunc = std::function<HandlerResult(RequestContext&)>;
    using ErrorBuilder =
        std::function<Buffer(const RequestContext&, ErrorCode error, int16_t response_version)>;

    explicit RequestDispatcher(std::shared_ptr<metrics::RequestMetrics> metrics);

    void registerHandler(protocol::ApiKey api_key, int16_t min_version, int16_t max_version,
                         HandlerFunc handler, ErrorBuilder error_builder = nullptr);

    DispatchResult dispatch(RequestContext context);

private:
    struct HandlerRegistration {
        int16_t min_version;
        int16_t max_version;
        HandlerFunc handler;
        ErrorBuilder error_builder;
    };

    using HandlerList = std::vector<HandlerRegistration>;

    const HandlerList* findHandlers(protocol::ApiKey api_key) const;
    const HandlerRegistration* selectHandler(const HandlerList& list, int16_t version) const;
    Buffer buildLegacyErrorPayload(const RequestContext& context, ErrorCode code) const;
    Buffer buildErrorPayload(const HandlerRegistration* registration, const RequestContext& context,
                             ErrorCode code, int16_t response_version) const;
    std::vector<uint8_t> wrapFrame(const Buffer& payload) const;
    DispatchResult finalize(protocol::ApiKey api_key, size_t request_bytes,
                            const HandlerResult& result) const;

    std::unordered_map<int16_t, HandlerList> handlers_;
    std::shared_ptr<metrics::RequestMetrics> metrics_;
};

}  // namespace kawasan::broker
