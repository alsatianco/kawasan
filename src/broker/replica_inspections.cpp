#include <algorithm>
#include <map>

#include "kawasan/broker/kawasan_broker.h"
#include "kawasan/common/logger.h"
#include "kawasan/protocol/admin_stubs.h"

namespace kawasan::broker {

void KawasanBroker::withProducerInspectionFences(
    const std::vector<ReplicaAssignment>& sources,
    const std::vector<std::shared_ptr<storage::Log>>& logs,
    const std::function<void(const std::vector<ErrorCode>&)>& publish) {
    metadata_controller_->withReplicaAssignments(sources, broker_id_, [&](auto statuses) {
        if (coordinator_mutations_stopping_.load() || !running_.load() || !dataPlaneCurrent()) {
            publish(std::vector<ErrorCode>(sources.size(), ErrorCode::NOT_LEADER_FOR_PARTITION));
            return;
        }
        // No opening/creation and no scans under metadata. A registry replacement
        // invalidates the original log instance even when its leader epoch repeats.
        for (size_t i = 0; i < sources.size(); ++i)
            if (statuses[i] == ErrorCode::NONE &&
                log_manager_->getLog(sources[i].tp.topic, sources[i].tp.partition) != logs[i].get())
                statuses[i] = ErrorCode::OFFSET_NOT_AVAILABLE;
        replica_manager_->withProducerInspectionReplicas(sources, logs, [&](const auto& ready) {
            for (size_t i = 0; i < sources.size(); ++i)
                if (statuses[i] == ErrorCode::NONE)
                    statuses[i] = ready[i];
            publish(
                statuses);  // All metadata/replica sources remain held through write initiation.
        });
    });
}

RequestDispatcher::HandlerResult KawasanBroker::deferProducerInspection(
    RequestDispatcher::RequestContext context) {
    using Response = protocol::DescribeProducersResponse;
    protocol::DescribeProducersRequest request;
    request.decode(context.payload, context.header.apiVersion());
    // Inspection uses no connection identity; builders must not reenter metadata.
    context.connection = nullptr;
    auto complete = context.complete;
    context.complete = {};
    context.deferred_sink = {};
    struct Batch {
        std::vector<Response::TopicResult> topics;
        std::vector<ReplicaAssignment> sources;
        std::vector<std::shared_ptr<storage::Log>> logs;
        std::vector<ErrorCode> status;
    };
    auto batch = std::make_shared<Batch>();
    for (const auto& topic : request.topics()) {
        Response::TopicResult result;
        result.topic = topic.topic;
        const auto metadata = metadata_controller_->describeTopics({topic.topic});
        for (const auto partition : topic.partitions) {
            result.partitions.push_back({partition, ErrorCode::NONE, {}, {}});
            ReplicaAssignment source;
            source.tp = {topic.topic, partition};
            if (metadata.size() == 1 && metadata[0].error_code == ErrorCode::NONE) {
                source.topic_id = metadata[0].topic_id;
                for (const auto& entry : metadata[0].partitions)
                    if (entry.partition == partition) {
                        source.leader = entry.leader;
                        source.epoch = entry.leader_epoch;
                        break;
                    }
            }
            batch->sources.push_back(source);
            batch->logs.push_back(log_manager_->retainLog(topic.topic, partition));
        }
        batch->topics.push_back(std::move(result));
    }
    auto encode = [this, context](const auto& topics) {
        Response response;
        for (const auto& topic : topics)
            response.addTopic(topic);
        return encodeResponse(
            context, [&](Buffer& output) { response.encode(output, context.header.apiVersion()); });
    };
    auto refuse = [&](ErrorCode error) {
        auto topics = batch->topics;
        for (auto& topic : topics)
            for (auto& partition : topic.partitions)
                partition.error_code = error;
        RequestDispatcher::HandlerResult result;
        result.payload = encode(topics);
        return result;
    };
    if (!coordinator_executor_ || !complete)
        return refuse(ErrorCode::INVALID_REQUEST);
    withProducerInspectionFences(batch->sources, batch->logs,
                                 [&](const auto& statuses) { batch->status = statuses; });
    if (!coordinator_executor_->submit([this, batch, encode, complete](bool admitted, auto ticket) {
            std::vector<ErrorCode> current(batch->sources.size(),
                                           ErrorCode::NOT_LEADER_FOR_PARTITION);
            if (admitted)
                withProducerInspectionFences(batch->sources, batch->logs,
                                             [&](const auto& statuses) { current = statuses; });
            // Scan consistent local LEO images off IO threads and outside metadata
            // and replica locks. Repeated partitions share one image per request.
            std::map<TopicPartition, std::pair<ErrorCode, storage::Log::ProducerInspection>> images;
            size_t i = 0;
            for (auto& topic : batch->topics) {
                for (auto& partition : topic.partitions) {
                    auto status = batch->status[i];
                    if (status == ErrorCode::NONE)
                        status = current[i];
                    if (status == ErrorCode::NONE) {
                        auto [image, fresh] = images.try_emplace(batch->sources[i].tp);
                        if (fresh) {
                            try {
                                image->second.second = batch->logs[i]->inspectProducerState();
                                image->second.first = ErrorCode::NONE;
                            } catch (const KawasanException& ex) {
                                image->second.first = ex.code();
                            } catch (const std::exception& ex) {
                                Logger::error("Replica producer inspection failed: {}", ex.what());
                                image->second.first = ErrorCode::KAFKA_STORAGE_ERROR;
                            } catch (...) {
                                image->second.first = ErrorCode::KAFKA_STORAGE_ERROR;
                            }
                        }
                        status = image->second.first;
                        if (status == ErrorCode::NONE)
                            for (const auto& producer : image->second.second.producers)
                                partition.active_producers.push_back(
                                    {producer.producer_id, producer.producer_epoch,
                                     producer.last_sequence, producer.last_timestamp,
                                     producer.coordinator_epoch,
                                     producer.current_txn_start_offset});
                    }
                    partition.error_code = status;
                    ++i;
                }
            }
            RequestDispatcher::HandlerResult result;
            result.payload = encode(batch->topics);
            result.lifetime = std::move(ticket);
            auto fenced_payload = std::make_shared<Buffer>(result.payload);
            result.publication_error = [fenced_payload](ErrorCode) { return *fenced_payload; };
            result.publication_guard = [this, batch, encode, fenced_payload,
                                        ticket = result.lifetime](const auto& publish) {
                (void)ticket;
                withProducerInspectionFences(
                    batch->sources, batch->logs, [&](const auto& statuses) {
                        auto topics = batch->topics;
                        size_t i = 0;
                        bool changed = false;
                        for (auto& topic : topics) {
                            for (auto& partition : topic.partitions) {
                                auto error = statuses[i];
                                // Retain admission/protocol/storage errors when a busy
                                // replica cannot be checked; known identity loss wins.
                                if (batch->status[i] != ErrorCode::NONE)
                                    error = ErrorCode::NONE;
                                else if (error == ErrorCode::OFFSET_NOT_AVAILABLE &&
                                         partition.error_code != ErrorCode::NONE)
                                    error = ErrorCode::NONE;
                                if (error != ErrorCode::NONE) {
                                    partition.error_code = error;
                                    partition.active_producers.clear();
                                    changed = true;
                                }
                                ++i;
                            }
                        }
                        if (changed)
                            *fenced_payload = encode(topics);
                        publish(changed ? ErrorCode::OFFSET_NOT_AVAILABLE : ErrorCode::NONE);
                    });
            };
            complete(std::move(result));
        }))
        return refuse(coordinator_executor_->stopping() ? ErrorCode::NOT_LEADER_FOR_PARTITION
                                                        : ErrorCode::OFFSET_NOT_AVAILABLE);
    RequestDispatcher::HandlerResult parked;
    parked.deferred = true;
    return parked;
}

}  // namespace kawasan::broker
