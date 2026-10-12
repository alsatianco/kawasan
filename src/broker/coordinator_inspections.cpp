#include "kawasan/broker/kawasan_broker.h"
#include "kawasan/protocol/admin_stubs.h"
#include "kawasan/protocol/request_header.h"

namespace kawasan::broker {

RequestDispatcher::HandlerResult KawasanBroker::deferCoordinatorDescriptions(
    RequestDispatcher::RequestContext context, RequestDispatcher::HandlerFunc work,
    RequestDispatcher::ErrorBuilder errors) {
    const auto version = context.header.apiVersion();
    const bool groups = context.header.apiKey() == protocol::ApiKey::DESCRIBE_GROUPS;
    std::vector<std::string> keys;
    bool include_operations = false;
    Buffer input = context.payload;
    if (groups) {
        protocol::DescribeGroupsRequest request;
        request.decode(input, version);
        keys = request.groups();
        include_operations = request.includeAuthorizedOperations();
    } else {
        protocol::DescribeTransactionsRequest request;
        request.decode(input, version);
        keys = request.transactionalIds();
    }
    std::vector<RequestDispatcher::RequestContext> parts;
    for (const auto& key : keys) {
        auto part = context;
        part.payload = Buffer{};
        part.payload_start = 0;
        if (groups) {
            protocol::DescribeGroupsRequest single;
            single.setGroups({key});
            single.setIncludeAuthorizedOperations(include_operations);
            single.encode(part.payload, version);
        } else {
            part.payload.writeCompactArrayLen(1);
            part.payload.writeCompactString(key);
            part.payload.writeEmptyTaggedFields();
        }
        parts.push_back(std::move(part));
    }
    const auto admission = context;
    context.connection = nullptr;
    context.complete = {};
    context.deferred_sink = {};
    auto assemble = [this, context, version, groups](const auto& payloads) {
        protocol::DescribeGroupsResponse group_response;
        protocol::DescribeTransactionsResponse txn_response;
        std::vector<protocol::DescribeGroupsResponse::Group> described_groups;
        for (auto input : payloads) {
            protocol::ResponseHeader header;
            header.setFlexible(context.header.isFlexibleResponseHeader());
            header.decode(input);
            if (groups) {
                protocol::DescribeGroupsResponse part;
                part.decode(input, version);
                described_groups.insert(described_groups.end(), part.groups().begin(),
                                        part.groups().end());
            } else {
                protocol::DescribeTransactionsResponse part;
                part.decode(input, version);
                for (const auto& state : part.states())
                    txn_response.addState(state);
            }
        }
        return encodeResponse(context, [&](Buffer& output) {
            if (groups) {
                group_response.setGroups(described_groups);
                group_response.encode(output, version);
            } else {
                txn_response.encode(output, version);
            }
        });
    };
    return deferCoordinatorBatch(
        admission, std::move(parts), keys, std::move(work), std::move(errors), std::move(assemble),
        true, groups ? protocol::CoordinatorType::GROUP : protocol::CoordinatorType::TRANSACTION);
}

Buffer KawasanBroker::buildDescribeTransactionsError(
    const RequestDispatcher::RequestContext& context, ErrorCode code, int16_t version) const {
    protocol::DescribeTransactionsResponse response;
    try {
        Buffer input = context.payload;
        input.setPosition(context.payload_start);
        protocol::DescribeTransactionsRequest request;
        request.decode(input, version);
        for (const auto& id : request.transactionalIds()) {
            protocol::DescribeTransactionsResponse::State state;
            state.transactional_id = id;
            state.error_code = code;
            response.addState(std::move(state));
        }
    } catch (const std::exception&) {
        // Undecodable requests have no trustworthy key identities.
    }
    return encodeResponse(context, [&](Buffer& output) { response.encode(output, version); });
}

}  // namespace kawasan::broker
