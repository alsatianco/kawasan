#include <algorithm>
#include <map>
#include <set>

#include "kawasan/broker/coordinator_routing.h"
#include "kawasan/broker/kawasan_broker.h"
#include "kawasan/common/logger.h"
#include "kawasan/protocol/admin_stubs.h"
#include "kawasan/protocol/list_groups_request.h"
#include "kawasan/protocol/request_header.h"

namespace kawasan::broker {

void KawasanBroker::withCoordinatorEnumerationFence(const std::string& topic,
                                                    const std::vector<RequestOwnership>& sources,
                                                    const std::function<void(ErrorCode)>& publish) {
    if (coordinator_mutations_stopping_.load() || !running_.load() || !dataPlaneCurrent()) {
        publish(ErrorCode::NOT_COORDINATOR);
        return;
    }
    // Same lock order as acquisition/writers; socket publication never waits
    // behind an ISR writer. The metadata lock also fences newly owned sources.
    std::map<TopicPartition, std::shared_ptr<std::mutex>> mutexes;
    for (const auto& source : sources)
        mutexes.try_emplace(source.tp, coordinatorMutationMutex(source.tp));
    std::vector<std::unique_lock<std::mutex>> locks;
    for (const auto& [tp, mutex] : mutexes) {
        (void)tp;
        locks.emplace_back(*mutex, std::try_to_lock);
    }
    std::unique_lock<std::mutex> acquisition(coordinator_acquisition_mutex_, std::try_to_lock);
    std::vector<std::pair<TopicPartition, int32_t>> partitions;
    for (const auto& source : sources)
        partitions.emplace_back(source.tp, source.epoch);
    metadata_controller_->withOwnedPartitionLeaderships(
        topic, broker_id_, partitions, [&](auto status) {
            if (coordinator_mutations_stopping_.load() || !dataPlaneCurrent())
                status = ErrorCode::NOT_COORDINATOR;
            if (status == ErrorCode::NONE) {
                if (!acquisition.owns_lock() ||
                    std::any_of(locks.begin(), locks.end(),
                                [](const auto& lock) { return !lock.owns_lock(); })) {
                    status = ErrorCode::COORDINATOR_LOAD_IN_PROGRESS;
                } else {
                    for (const auto& source : sources) {
                        const auto acquired = acquired_coordinator_epochs_.find(source.tp);
                        if (acquired == acquired_coordinator_epochs_.end() ||
                            acquired->second != source.epoch ||
                            !replica_manager_->readableHighWatermark(source.tp, source.epoch) ||
                            failed_coordinator_appends_.contains(source.tp) ||
                            coordinator_commit_barriers_.contains(source.tp)) {
                            status = ErrorCode::COORDINATOR_LOAD_IN_PROGRESS;
                            break;
                        }
                    }
                }
            }
            publish(status);  // Cache reads/builders must not reenter metadata.
        });
}

RequestDispatcher::HandlerResult KawasanBroker::deferCoordinatorListings(
    RequestDispatcher::RequestContext context, RequestDispatcher::ErrorBuilder errors) {
    auto error_context = context;
    error_context.connection = nullptr;
    error_context.complete = {};
    error_context.deferred_sink = {};
    auto error_payload = [errors, error_context](ErrorCode code) {
        return errors(error_context, code, error_context.header.apiVersion());
    };
    auto refusal = [&](ErrorCode code) {
        RequestDispatcher::HandlerResult result;
        result.payload = error_payload(code);
        return result;
    };
    if (!coordinator_executor_ || !context.complete)
        return refusal(ErrorCode::INVALID_REQUEST);
    const bool groups = context.header.apiKey() == protocol::ApiKey::LIST_GROUPS;
    Buffer input = context.payload;
    if (groups) {
        protocol::ListGroupsRequest request;
        request.decode(input, context.header.apiVersion());
    } else {
        protocol::ListTransactionsRequest request;
        request.decode(input, context.header.apiVersion());
    }
    const std::string topic = groups ? "__consumer_offsets" : "__transaction_state";
    const auto metadata = metadata_controller_->describeTopics({topic});
    if (metadata.size() != 1 || metadata[0].error_code != ErrorCode::NONE ||
        metadata[0].partitions.size() !=
            static_cast<size_t>(staged_coordinator_format_->partitionCount(topic)))
        return refusal(ErrorCode::COORDINATOR_LOAD_IN_PROGRESS);
    std::vector<RequestOwnership> sources;
    for (const auto& partition : metadata[0].partitions)
        if (partition.leader == broker_id_)
            sources.push_back({this, {topic, partition.partition}, partition.leader_epoch});
    ErrorCode status = ErrorCode::NOT_COORDINATOR;
    withCoordinatorEnumerationFence(topic, sources, [&](auto error) { status = error; });
    if (status != ErrorCode::NONE)
        return refusal(status);
    auto complete = context.complete;
    // These cache-only APIs do not use connection identity. Drop the raw pointer
    // before parking, while retaining exactly one queue/work/response ticket.
    context.connection = nullptr;
    context.complete = {};
    context.deferred_sink = {};
    if (!coordinator_executor_->submit([this, context = std::move(context), complete, topic,
                                        sources,
                                        error_payload](bool admitted, auto ticket) mutable {
            RequestDispatcher::HandlerResult result;
            auto status = ErrorCode::NOT_COORDINATOR;
            try {
                if (admitted)
                    withCoordinatorEnumerationFence(topic, sources, [&](auto error) {
                        status = error;
                        if (status == ErrorCode::NONE)
                            result.payload = readCoordinatorListing(context, sources);
                    });
            } catch (const ProtocolException&) {
                status = ErrorCode::INVALID_REQUEST;
            } catch (const KawasanException& ex) {
                status = ex.code();
            } catch (const std::exception& ex) {
                Logger::error("Coordinator enumeration failed: {}", ex.what());
                status = ErrorCode::KAFKA_STORAGE_ERROR;
            } catch (...) {
                status = ErrorCode::KAFKA_STORAGE_ERROR;
            }
            if (status != ErrorCode::NONE)
                result.payload = error_payload(status);
            result.lifetime = std::move(ticket);
            result.publication_error = [error_payload, status](ErrorCode error) {
                return error_payload(error == ErrorCode::COORDINATOR_LOAD_IN_PROGRESS &&
                                             status != ErrorCode::NONE
                                         ? status
                                         : error);
            };
            result.publication_guard = [this, topic, sources,
                                        ticket = result.lifetime](const auto& publish) {
                (void)ticket;
                withCoordinatorEnumerationFence(topic, sources, publish);
            };
            complete(std::move(result));
        }))
        return refusal(coordinator_executor_->stopping() ? ErrorCode::NOT_COORDINATOR
                                                         : ErrorCode::COORDINATOR_LOAD_IN_PROGRESS);
    RequestDispatcher::HandlerResult parked;
    parked.deferred = true;
    return parked;
}

Buffer KawasanBroker::readCoordinatorListing(RequestDispatcher::RequestContext& context,
                                             const std::vector<RequestOwnership>& sources) {
    const bool groups = context.header.apiKey() == protocol::ApiKey::LIST_GROUPS;
    std::function<bool(const std::string&)> include;
    if (staged_coordinator_format_) {
        std::set<int32_t> partitions;
        for (const auto& source : sources)
            partitions.insert(source.tp.partition);
        const auto count = staged_coordinator_format_->partitionCount(
            groups ? "__consumer_offsets" : "__transaction_state");
        include = [partitions, count](const auto& id) {
            return partitions.contains(coordinatorPartitionFor(id, count));
        };
    }
    if (groups) {
        protocol::ListGroupsRequest request;
        request.decode(context.payload, context.header.apiVersion());
        auto listed = group_coordinator_->listGroups(include);
        const auto& filters = request.statesFilter();
        std::erase_if(listed, [&](const auto& group) {
            return !filters.empty() &&
                   std::find(filters.begin(), filters.end(), group.group_state) == filters.end();
        });
        protocol::ListGroupsResponse response;
        response.setGroups(listed);
        return encodeResponse(
            context, [&](Buffer& output) { response.encode(output, context.header.apiVersion()); });
    }
    protocol::ListTransactionsRequest request;
    request.decode(context.payload, context.header.apiVersion());
    protocol::ListTransactionsResponse response;
    std::set<std::string> known;
    for (const auto state :
         {TransactionCoordinator::State::Empty, TransactionCoordinator::State::Ongoing,
          TransactionCoordinator::State::PrepareCommit, TransactionCoordinator::State::PrepareAbort,
          TransactionCoordinator::State::CompleteCommit,
          TransactionCoordinator::State::CompleteAbort, TransactionCoordinator::State::Dead,
          TransactionCoordinator::State::PrepareEpochFence})
        known.insert(TransactionCoordinator::stateName(state));
    std::vector<std::string> unknown;
    std::set<std::string> seen;
    for (const auto& filter : request.stateFilters())
        if (!known.contains(filter) && seen.insert(filter).second)
            unknown.push_back(filter);
    response.setUnknownStateFilters(std::move(unknown));
    for (const auto& snapshot : transaction_coordinator_->list(
             request.stateFilters(), request.producerIdFilters(), include))
        response.addState({snapshot.transactional_id, snapshot.producer_id,
                           TransactionCoordinator::stateName(snapshot.state)});
    return encodeResponse(
        context, [&](Buffer& output) { response.encode(output, context.header.apiVersion()); });
}

Buffer KawasanBroker::buildListTransactionsError(const RequestDispatcher::RequestContext& context,
                                                 ErrorCode code, int16_t version) const {
    protocol::ListTransactionsResponse response;
    response.setErrorCode(code);
    return encodeResponse(context, [&](Buffer& output) { response.encode(output, version); });
}

Buffer KawasanBroker::buildDescribeProducersError(const RequestDispatcher::RequestContext& context,
                                                  ErrorCode code, int16_t version) const {
    protocol::DescribeProducersResponse response;
    try {
        Buffer input = context.payload;
        input.setPosition(context.payload_start);
        protocol::DescribeProducersRequest request;
        request.decode(input, version);
        for (const auto& topic : request.topics()) {
            protocol::DescribeProducersResponse::TopicResult result;
            result.topic = topic.topic;
            for (const auto partition : topic.partitions) {
                protocol::DescribeProducersResponse::PartitionResult entry;
                entry.partition = partition;
                entry.error_code = code;
                result.partitions.push_back(std::move(entry));
            }
            response.addTopic(std::move(result));
        }
    } catch (const std::exception&) {
        // Undecodable requests have no trustworthy partition identities.
    }
    return encodeResponse(context, [&](Buffer& output) { response.encode(output, version); });
}

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
