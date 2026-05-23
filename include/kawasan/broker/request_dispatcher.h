#pragma once

#include <functional>
#include <memory>
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
    struct RequestContext {
        protocol::RequestHeader header;
        Buffer payload;
        size_t frame_size_bytes = 0;
        std::string peer_identity;
    };

    struct DispatchResult {
        std::vector<uint8_t> frame;
        bool close_connection = false;
        bool suppress_response = false;
    };

    struct HandlerResult {
        Buffer payload;
        bool close_connection = false;
        bool suppress_response = false;
    };

    using HandlerFunc = std::function<HandlerResult(RequestContext&)>;
    using ErrorBuilder = std::function<Buffer(const RequestContext&, ErrorCode error,
                                              int16_t response_version)>;

    explicit RequestDispatcher(std::shared_ptr<metrics::RequestMetrics> metrics);

    void registerHandler(protocol::ApiKey api_key, int16_t min_version,
                         int16_t max_version, HandlerFunc handler,
                         ErrorBuilder error_builder = nullptr);

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
    const HandlerRegistration* selectHandler(const HandlerList& list,
                                             int16_t version) const;
    Buffer buildLegacyErrorPayload(const RequestContext& context,
                                   ErrorCode code) const;
    Buffer buildErrorPayload(const HandlerRegistration* registration,
                             const RequestContext& context, ErrorCode code,
                             int16_t response_version) const;
    std::vector<uint8_t> wrapFrame(const Buffer& payload) const;

    std::unordered_map<int16_t, HandlerList> handlers_;
    std::shared_ptr<metrics::RequestMetrics> metrics_;
};

}  // namespace kawasan::broker
