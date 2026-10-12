#include <algorithm>
#include <limits>
#include <map>

#include "kawasan/broker/coordinator_routing.h"
#include "kawasan/broker/kawasan_broker.h"
#include "kawasan/common/logger.h"
#include "kawasan/protocol/admin_misc_requests.h"
#include "kawasan/protocol/request_header.h"

namespace kawasan::broker {

void KawasanBroker::withCoordinatorRequestFences(
    const std::vector<RequestOwnership>& sources,
    const std::function<void(const std::vector<ErrorCode>&)>& publish) {
    if (coordinator_mutations_stopping_.load() || !running_.load() || !dataPlaneCurrent()) {
        publish(std::vector<ErrorCode>(sources.size(), ErrorCode::NOT_COORDINATOR));
        return;
    }
    // Sorted, deduplicated partition locks avoid deadlock when groups share a
    // source. Socket threads never wait behind an ISR writer or acquisition.
    std::map<TopicPartition, std::shared_ptr<std::mutex>> mutexes;
    for (const auto& source : sources)
        mutexes.try_emplace(source.tp, coordinatorMutationMutex(source.tp));
    std::map<TopicPartition, std::unique_lock<std::mutex>> locks;
    for (const auto& [tp, mutex] : mutexes)
        locks.emplace(tp, std::unique_lock<std::mutex>(*mutex, std::try_to_lock));
    std::unique_lock<std::mutex> acquisition(coordinator_acquisition_mutex_, std::try_to_lock);
    std::vector<std::pair<TopicPartition, int32_t>> partitions;
    for (const auto& source : sources)
        partitions.emplace_back(source.tp, source.epoch);
    metadata_controller_->withPartitionLeaderships(partitions, broker_id_, [&](auto statuses) {
        for (size_t i = 0; i < sources.size(); ++i) {
            const auto& source = sources[i];
            // Owner loss wins even if another thread has the acquisition lock.
            if (coordinator_mutations_stopping_.load() || !dataPlaneCurrent())
                statuses[i] = ErrorCode::NOT_COORDINATOR;
            if (statuses[i] != ErrorCode::NONE)
                continue;
            if (!locks.at(source.tp).owns_lock() || !acquisition.owns_lock()) {
                statuses[i] = ErrorCode::COORDINATOR_LOAD_IN_PROGRESS;
                continue;
            }
            const auto acquired = acquired_coordinator_epochs_.find(source.tp);
            if (acquired == acquired_coordinator_epochs_.end() ||
                acquired->second != source.epoch ||
                !replica_manager_->readableHighWatermark(source.tp, source.epoch) ||
                failed_coordinator_appends_.contains(source.tp) ||
                coordinator_commit_barriers_.contains(source.tp))
                statuses[i] = ErrorCode::COORDINATOR_LOAD_IN_PROGRESS;
        }
        publish(statuses);  // All source epochs stay fenced through write initiation.
    });
}

RequestDispatcher::HandlerResult KawasanBroker::deferCoordinatorGroupBatch(
    RequestDispatcher::RequestContext context, std::vector<RequestDispatcher::RequestContext> parts,
    const std::vector<std::string>& keys, RequestDispatcher::HandlerFunc work,
    RequestDispatcher::ErrorBuilder errors, CoordinatorBatchAssembler assemble, bool read_only) {
    if (parts.size() != keys.size() || !work || !errors || !assemble)
        throw std::invalid_argument("Invalid coordinator batch");
    struct Batch {
        std::vector<RequestDispatcher::RequestContext> parts;
        std::vector<RequestOwnership> sources;
        std::vector<ErrorCode> status;
        std::vector<Buffer> payloads;
        std::shared_ptr<RequestDispatcher::ConnectionContext> connection;
    };
    auto batch = std::make_shared<Batch>();
    batch->parts = std::move(parts);
    if (context.connection)
        batch->connection =
            std::make_shared<RequestDispatcher::ConnectionContext>(*context.connection);
    for (size_t i = 0; i < keys.size(); ++i) {
        auto& part = batch->parts[i];
        part.connection = batch->connection.get();
        part.complete = {};
        part.deferred_sink = {};
        const TopicPartition tp{"__consumer_offsets",
                                coordinatorPartitionFor(keys[i], offsets_topic_num_partitions_)};
        const auto metadata = currentPartitionMetadata(tp);
        batch->sources.push_back({this, tp, metadata ? metadata->leader_epoch : -1});
        ErrorCode status =
            keys[i].empty() ? ErrorCode::INVALID_GROUP_ID : ErrorCode::COORDINATOR_LOAD_IN_PROGRESS;
        if (!coordinator_executor_ || !context.complete)
            status = ErrorCode::INVALID_REQUEST;
        else if (!keys[i].empty() && metadata)
            withCoordinatorRequestFence(tp, metadata->leader_epoch,
                                        [&](auto error) { status = error; });
        batch->status.push_back(status);
        batch->payloads.push_back(errors(part, status, part.header.apiVersion()));
    }
    auto refusal = [&](ErrorCode code) {
        RequestDispatcher::HandlerResult result;
        std::vector<Buffer> payloads;
        for (const auto& part : batch->parts)
            payloads.push_back(errors(part, code, part.header.apiVersion()));
        result.payload = assemble(payloads);
        return result;
    };
    if (!coordinator_executor_ || !context.complete)
        return refusal(ErrorCode::INVALID_REQUEST);
    if (keys.empty()) {
        RequestDispatcher::HandlerResult result;
        result.payload = assemble({});
        return result;
    }
    auto complete = context.complete;
    if (!coordinator_executor_->submit([this, batch, work = std::move(work), errors, assemble,
                                        complete, read_only](bool admitted, auto ticket) {
            // One bounded job/ticket, but a separate original-source admission
            // for every group. Mutation work cannot cross that epoch pin.
            std::vector<size_t> admitted_indices;
            std::vector<RequestOwnership> sources;
            for (size_t i = 0; i < batch->parts.size(); ++i) {
                if (batch->status[i] != ErrorCode::NONE)
                    continue;
                admitted_indices.push_back(i);
                sources.push_back(batch->sources[i]);
                auto& status = batch->status[i];
                const auto previous = coordinator_request_owner_;
                coordinator_request_owner_ = batch->sources[i];
                try {
                    status = ErrorCode::NOT_COORDINATOR;
                    RequestDispatcher::HandlerResult result;
                    if (admitted) {
                        const auto& source = batch->sources[i];
                        withCoordinatorRequestFence(source.tp, source.epoch, [&](auto error) {
                            status = error;
                            if (read_only && status == ErrorCode::NONE)
                                result = work(batch->parts[i]);
                        });
                        if (!read_only && status == ErrorCode::NONE)
                            result = work(batch->parts[i]);
                        if (result.deferred)
                            status = ErrorCode::INVALID_REQUEST;
                    }
                    if (status == ErrorCode::NONE)
                        batch->payloads[i] = std::move(result.payload);
                } catch (const ProtocolException&) {
                    status = ErrorCode::INVALID_REQUEST;
                } catch (const KawasanException& ex) {
                    status = ex.code();
                } catch (const std::exception& ex) {
                    Logger::error("Coordinator batch failed: {}", ex.what());
                    status = ErrorCode::KAFKA_STORAGE_ERROR;
                } catch (...) {
                    status = ErrorCode::KAFKA_STORAGE_ERROR;
                }
                coordinator_request_owner_ = previous;
                if (status != ErrorCode::NONE)
                    batch->payloads[i] =
                        errors(batch->parts[i], status, batch->parts[i].header.apiVersion());
            }
            RequestDispatcher::HandlerResult result;
            result.payload = assemble(batch->payloads);
            result.lifetime = std::move(ticket);
            auto fenced_payload = std::make_shared<Buffer>(result.payload);
            result.publication_error = [fenced_payload](ErrorCode) { return *fenced_payload; };
            result.publication_guard = [this, batch, errors, assemble, admitted_indices, sources,
                                        fenced_payload,
                                        ticket = result.lifetime](const auto& publish) {
                (void)ticket;
                withCoordinatorRequestFences(sources, [&](const auto& statuses) {
                    auto payloads = batch->payloads;
                    bool changed = false;
                    for (size_t n = 0; n < statuses.size(); ++n) {
                        const auto i = admitted_indices[n];
                        auto error = statuses[n];
                        if (error == ErrorCode::COORDINATOR_LOAD_IN_PROGRESS &&
                            batch->status[i] != ErrorCode::NONE)
                            error = batch->status[i];
                        if (error != ErrorCode::NONE) {
                            changed = true;
                            payloads[i] =
                                errors(batch->parts[i], error, batch->parts[i].header.apiVersion());
                        }
                    }
                    if (changed)
                        *fenced_payload = assemble(payloads);
                    // A nonzero guard status selects the per-group replacement
                    // factory; it is not a wire-wide error.
                    publish(changed ? ErrorCode::COORDINATOR_LOAD_IN_PROGRESS : ErrorCode::NONE);
                });
            };
            complete(std::move(result));
        }))
        return refusal(coordinator_executor_->stopping() ? ErrorCode::NOT_COORDINATOR
                                                         : ErrorCode::COORDINATOR_LOAD_IN_PROGRESS);
    RequestDispatcher::HandlerResult parked;
    parked.deferred = true;
    return parked;
}

RequestDispatcher::HandlerResult KawasanBroker::deferCoordinatorOffsetFetch(
    RequestDispatcher::RequestContext context, RequestDispatcher::HandlerFunc work,
    RequestDispatcher::ErrorBuilder errors) {
    Buffer input = context.payload;
    protocol::OffsetFetchRequest request;
    const auto version = context.header.apiVersion();
    request.decode(input, version);
    if (version < 8)
        return deferCoordinatorRequest(context, request.groupId(), protocol::CoordinatorType::GROUP,
                                       std::move(work), std::move(errors), true);
    std::vector<RequestDispatcher::RequestContext> parts;
    std::vector<std::string> keys;
    for (const auto& group : request.groups()) {
        auto part = context;
        part.payload = Buffer{};
        part.payload_start = 0;
        protocol::OffsetFetchRequest single;
        single.setRequireStable(request.requireStable());
        single.addGroup(group);
        single.encode(part.payload, version);
        keys.push_back(group.group_id);
        parts.push_back(std::move(part));
    }
    context.connection = nullptr;
    context.complete = {};  // Assembler never retains a transport/connection.
    context.deferred_sink = {};
    auto assemble = [this, context, version](const auto& payloads) {
        protocol::OffsetFetchResponse response;
        for (auto input : payloads) {
            protocol::ResponseHeader header;
            header.setFlexible(true);
            header.decode(input);
            protocol::OffsetFetchResponse part;
            part.decode(input, version);
            for (const auto& group : part.groups())
                response.addGroup(group);
        }
        return encodeResponse(context, [&](Buffer& buffer) { response.encode(buffer, version); });
    };
    // Retain completion/identity only in the admission context, not the assembler.
    auto admission = parts.empty() ? context : parts.front();
    if (parts.empty()) {
        RequestDispatcher::HandlerResult result;
        result.payload = assemble(std::vector<Buffer>{});
        return result;
    }
    return deferCoordinatorGroupBatch(admission, std::move(parts), keys, std::move(work),
                                      std::move(errors), std::move(assemble), true);
}

RequestDispatcher::HandlerResult KawasanBroker::deferCoordinatorDeleteGroups(
    RequestDispatcher::RequestContext context, RequestDispatcher::HandlerFunc work,
    RequestDispatcher::ErrorBuilder errors) {
    Buffer input = context.payload;
    protocol::DeleteGroupsRequest request;
    const auto version = context.header.apiVersion();
    request.decode(input, version);
    std::vector<RequestDispatcher::RequestContext> parts;
    for (const auto& key : request.groups()) {
        auto part = context;
        part.payload = Buffer{};
        part.payload_start = 0;
        part.payload.writeInt32(1);
        part.payload.writeString(key);  // Only advertised DeleteGroups v0.
        parts.push_back(std::move(part));
    }
    auto admission = context;
    context.connection = nullptr;
    context.complete = {};
    context.deferred_sink = {};
    auto assemble = [this, context, version](const auto& payloads) {
        protocol::DeleteGroupsResponse response;
        for (auto input : payloads) {
            protocol::ResponseHeader header;
            header.decode(input);
            protocol::DeleteGroupsResponse part;
            part.decode(input, version);
            for (const auto& group : part.results())
                response.addResult(group);
        }
        return encodeResponse(context, [&](Buffer& buffer) { response.encode(buffer, version); });
    };
    return deferCoordinatorGroupBatch(admission, std::move(parts), request.groups(),
                                      std::move(work), std::move(errors), std::move(assemble),
                                      false);
}

void KawasanBroker::scheduleCoordinatorRetention() {
    if (!staged_coordinator_persistence_ || coordinator_mutations_stopping_.load() ||
        !running_.load() || !internal_topics_ready_.load())
        return;
    for (const auto& id : group_coordinator_->retentionCandidates()) {
        const TopicPartition tp{"__consumer_offsets",
                                coordinatorPartitionFor(id, offsets_topic_num_partitions_)};
        const auto metadata = currentPartitionMetadata(tp);
        if (!metadata)
            continue;
        const auto epoch = metadata->leader_epoch;
        ErrorCode status = ErrorCode::NOT_COORDINATOR;
        withCoordinatorRequestFence(tp, epoch, [&](auto error) { status = error; });
        if (status != ErrorCode::NONE)
            continue;
        {
            std::lock_guard<std::mutex> lock(coordinator_timeout_mutex_);
            if (!coordinator_retention_pending_.insert(id).second)
                continue;
        }
        const bool queued = coordinator_executor_->submit([this, id, tp, epoch](bool admitted,
                                                                                auto ticket) {
            (void)ticket;
            const auto previous = coordinator_request_owner_;
            coordinator_request_owner_ = RequestOwnership{this, tp, epoch};
            try {
                if (admitted) {
                    const auto error = mutateCoordinatorGroup(id, [&](const auto& current,
                                                                      auto& changes) {
                        const auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                                             std::chrono::system_clock::now().time_since_epoch())
                                             .count();
                        const GroupRecord* group = nullptr;
                        std::set<std::pair<std::string, int32_t>> pending;
                        for (const auto& record : current) {
                            if (record.key.kind == GroupRecordKey::Kind::Group)
                                group = &record;
                            if (record.key.kind == GroupRecordKey::Kind::PendingOffset)
                                pending.emplace(record.key.topic, record.key.partition);
                        }
                        if (group && !group->group.members.empty())
                            return ErrorCode::NONE;
                        bool retained = !pending.empty();
                        for (const auto& record : current) {
                            if (record.key.kind != GroupRecordKey::Kind::Offset)
                                continue;
                            const bool expired =
                                record.offset.expiry_timestamp >= 0
                                    ? now >= record.offset.expiry_timestamp
                                    : now - std::max(record.offset.commit_timestamp,
                                                     group && !group->group.protocol_type.empty()
                                                         ? group->group.last_update_timestamp
                                                         : int64_t{0}) >
                                          offset_manager_->getRetentionMs();
                            if (!expired ||
                                pending.contains({record.key.topic, record.key.partition})) {
                                retained = true;
                                continue;
                            }
                            auto tombstone = record;
                            tombstone.tombstone = true;
                            changes.push_back(std::move(tombstone));
                        }
                        if (group && !retained &&
                            now - group->group.last_update_timestamp >
                                group_coordinator_->getGroupRetentionMs()) {
                            auto tombstone = *group;
                            tombstone.tombstone = true;
                            changes.push_back(std::move(tombstone));
                        }
                        return ErrorCode::NONE;
                    });
                    if (error != ErrorCode::NONE)
                        Logger::debug("Coordinator retention '{}' refused: {}", id,
                                      static_cast<int>(error));
                }
            } catch (const std::exception& ex) {
                Logger::error("Coordinator retention '{}' failed: {}", id, ex.what());
            }
            coordinator_request_owner_ = previous;
            std::lock_guard<std::mutex> lock(coordinator_timeout_mutex_);
            coordinator_retention_pending_.erase(id);
        });
        if (!queued) {
            std::lock_guard<std::mutex> lock(coordinator_timeout_mutex_);
            coordinator_retention_pending_.erase(id);
        }
    }
}
}  // namespace kawasan::broker
