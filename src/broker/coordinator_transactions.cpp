#include <algorithm>

#include "kawasan/broker/coordinator_routing.h"
#include "kawasan/broker/kawasan_broker.h"
#include "kawasan/common/error.h"
#include "kawasan/common/logger.h"

namespace kawasan::broker {
namespace {
ErrorCode registrationError(ErrorCode code, int16_t version) {
    if (code == ErrorCode::INVALID_PRODUCER_EPOCH || code == ErrorCode::PRODUCER_FENCED)
        return version < 2 ? ErrorCode::INVALID_PRODUCER_EPOCH : ErrorCode::PRODUCER_FENCED;
    return code;
}
}  // namespace

Buffer KawasanBroker::buildTransactionRegistrationError(
    const RequestDispatcher::RequestContext& context, ErrorCode code, int16_t version) const {
    code = registrationError(code, version);
    if (context.header.apiKey() == protocol::ApiKey::ADD_OFFSETS_TO_TXN) {
        protocol::AddOffsetsToTxnResponse response;
        response.setErrorCode(code);
        return encodeResponse(context, [&](Buffer& output) { response.encode(output, version); });
    }
    protocol::AddPartitionsToTxnResponse response;
    try {
        Buffer payload = context.payload;
        payload.setPosition(context.payload_start);
        protocol::AddPartitionsToTxnRequest request;
        request.decode(payload, version);
        for (const auto& topic : request.topics()) {
            protocol::AddPartitionsToTxnResponse::TopicResult result;
            result.topic = topic.topic;
            for (const auto partition : topic.partitions)
                result.partitions.push_back({partition, code});
            response.addTopic(std::move(result));
        }
    } catch (const std::exception&) {
        // An undecodable request has no trustworthy partition identities.
    }
    return encodeResponse(context, [&](Buffer& output) { response.encode(output, version); });
}

ErrorCode KawasanBroker::registerCoordinatorTransactionPartitions(
    const std::string& id, int64_t producer_id, int16_t producer_epoch,
    const std::vector<TopicPartition>& partitions, bool capture_first_offsets) {
    const auto observed = transaction_coordinator_->describe(id);
    ErrorCode protocol_error = ErrorCode::NONE;
    const auto durable = mutateCoordinatorTransaction(id, observed, [&](auto& proposed) {
        if (producer_id < 0 || producer_epoch < 0) {
            protocol_error = ErrorCode::INVALID_REQUEST;
            return ErrorCode::NONE;
        }
        if (!proposed || proposed->producer_id != producer_id) {
            protocol_error = ErrorCode::INVALID_PRODUCER_ID_MAPPING;
            return ErrorCode::NONE;
        }
        if (proposed->producer_epoch != producer_epoch) {
            protocol_error = ErrorCode::PRODUCER_FENCED;
            return ErrorCode::NONE;
        }
        using State = TransactionCoordinator::State;
        if (proposed->state == State::PrepareCommit || proposed->state == State::PrepareAbort ||
            proposed->state == State::PrepareEpochFence) {
            protocol_error = ErrorCode::CONCURRENT_TRANSACTIONS;
            return ErrorCode::NONE;
        }
        if ((proposed->state != State::Empty && proposed->state != State::Ongoing &&
             proposed->state != State::CompleteCommit && proposed->state != State::CompleteAbort) ||
            (proposed->state != State::Ongoing &&
             (!proposed->partitions.empty() || !proposed->pending_offsets.empty()))) {
            protocol_error = ErrorCode::INVALID_TXN_STATE;
            return ErrorCode::NONE;
        }
        auto next = *proposed;
        for (const auto& tp : partitions) {
            const auto metadata = currentPartitionMetadata(tp);
            if (!metadata) {
                protocol_error = ErrorCode::UNKNOWN_TOPIC_OR_PARTITION;
                return ErrorCode::NONE;
            }
            if (std::any_of(next.partitions.begin(), next.partitions.end(), [&](const auto& p) {
                    return p.topic == tp.topic && p.partition == tp.partition;
                }))
                continue;  // A retry must never move an existing first offset.
            int64_t first_offset = -1;
            if (capture_first_offsets && metadata->leader == broker_id_) {
                auto write_lock = lockPartitionWrites(tp);
                auto* log = log_manager_->getLog(tp.topic, tp.partition);
                if (!log)
                    throw KawasanException(ErrorCode::KAFKA_STORAGE_ERROR);
                const auto captured = metadata_controller_->withPartitionLeadership(
                    tp, broker_id_, metadata->leader_epoch, [&](const auto&) {
                        first_offset = log->logEndOffset();
                        return ErrorCode::NONE;
                    });
                if (captured != ErrorCode::NONE) {
                    protocol_error = ErrorCode::CONCURRENT_TRANSACTIONS;
                    return ErrorCode::NONE;
                }
            }
            // Remote LSO state belongs to the partition leader. A follower's
            // LEO is not a valid transaction start. Marker/LSO work is gated.
            next.partitions.push_back({tp.topic, tp.partition, first_offset});
        }
        if (next.state != State::Ongoing) {
            next.state = State::Ongoing;
            next.state_start_time_ms = transaction_coordinator_->nowMs();
        }
        proposed = std::move(next);
        return ErrorCode::NONE;
    });
    if (durable != ErrorCode::NONE) {
        // A changed full snapshot within this epoch is retryable. Keep storage
        // failures in the executor status so a source barrier cannot hide them.
        throw KawasanException(
            durable == ErrorCode::INVALID_TXN_STATE ? ErrorCode::CONCURRENT_TRANSACTIONS : durable);
    }
    return protocol_error;
}

Buffer KawasanBroker::handleCoordinatorAddPartitions(
    RequestDispatcher::RequestContext& context,
    const protocol::AddPartitionsToTxnRequest& request) {
    std::vector<TopicPartition> partitions;
    protocol::AddPartitionsToTxnResponse invalid;
    bool rejected = false;
    for (const auto& topic : request.topics()) {
        protocol::AddPartitionsToTxnResponse::TopicResult result;
        result.topic = topic.topic;
        for (const auto partition : topic.partitions) {
            const TopicPartition tp{topic.topic, partition};
            auto error = ErrorCode::OPERATION_NOT_ATTEMPTED;
            if (topic.topic == "__consumer_offsets" || topic.topic == "__transaction_state" ||
                topic.topic == "__share_group_state")
                error = ErrorCode::TOPIC_AUTHORIZATION_FAILED;
            else if (!currentPartitionMetadata(tp))
                error = ErrorCode::UNKNOWN_TOPIC_OR_PARTITION;
            if (error != ErrorCode::OPERATION_NOT_ATTEMPTED)
                rejected = true;
            result.partitions.push_back({partition, error});
            partitions.push_back(tp);
        }
        invalid.addTopic(std::move(result));
    }
    if (rejected)
        return encodeResponse(
            context, [&](Buffer& output) { invalid.encode(output, context.header.apiVersion()); });
    const auto error = registerCoordinatorTransactionPartitions(
        request.transactionalId(), request.producerId(), request.producerEpoch(), partitions, true);
    // Cache publication is the only side effect. Never call legacy addPartitions,
    // persistence, IsolationTracker or marker completion from this handler.
    return buildTransactionRegistrationError(context, error, context.header.apiVersion());
}

Buffer KawasanBroker::handleCoordinatorAddOffsets(RequestDispatcher::RequestContext& context,
                                                  const protocol::AddOffsetsToTxnRequest& request) {
    const TopicPartition source{
        "__consumer_offsets",
        coordinatorPartitionFor(request.groupId(), offsets_topic_num_partitions_)};
    const auto error = registerCoordinatorTransactionPartitions(
        request.transactionalId(), request.producerId(), request.producerEpoch(), {source}, false);
    // Participation is transaction-owned; pending offset staging/completion
    // remains group-owned work. No group cache or offset source is mutated here.
    return buildTransactionRegistrationError(context, error, context.header.apiVersion());
}

ErrorCode KawasanBroker::prepareCoordinatorTransactionTimeout(
    const TransactionCoordinator::TxnSnapshot& expected, std::chrono::milliseconds timeout) {
    return mutateCoordinatorTransaction(
        expected.transactional_id, expected,
        [&](auto& proposed) {
            const auto now = transaction_coordinator_->nowMs();
            if (!proposed || !TransactionCoordinator::hasTimedOut(*proposed, now))
                return ErrorCode::NONE;  // Clock rollback or an unexpired candidate: fenced no-op.
            proposed->state = TransactionCoordinator::State::PrepareAbort;
            proposed->state_start_time_ms = now;
            // Preserve every identity, retry, participation and pending offset field.
            // This is a durable decision only: no legacy completion/LSO/marker effects.
            return ErrorCode::NONE;
        },
        timeout);
}

void KawasanBroker::scheduleCoordinatorTransactionTimeouts() {
    if (!staged_coordinator_persistence_ || coordinator_mutations_stopping_.load() ||
        !running_.load() || !internal_topics_ready_.load())
        return;
    const auto topics = metadata_controller_->describeTopics({"__transaction_state"});
    if (topics.empty())
        return;
    for (const auto& partition : topics.front().partitions) {
        if (partition.leader != broker_id_)
            continue;
        const TopicPartition tp{"__transaction_state", partition.partition};
        const auto epoch = partition.leader_epoch;
        std::vector<TransactionCoordinator::TxnSnapshot> candidates;
        // Capture snapshots inside the original readable source/epoch fence.
        // Select by routing before inspecting fields in potentially nonowned caches.
        withCoordinatorRequestFence(tp, epoch, [&](auto status) {
            if (status != ErrorCode::NONE)
                return;
            candidates = transaction_coordinator_->list({}, {}, [&](const auto& id) {
                return coordinatorPartitionFor(id, txn_state_num_partitions_) == tp.partition;
            });
            const auto now = transaction_coordinator_->nowMs();
            std::erase_if(candidates, [&](const auto& s) {
                return !TransactionCoordinator::hasTimedOut(s, now);
            });
        });
        for (const auto& snapshot : candidates) {
            const auto id = snapshot.transactional_id;
            {
                std::lock_guard<std::mutex> lock(coordinator_timeout_mutex_);
                if (!coordinator_transaction_timeout_pending_.insert(id).second)
                    continue;
            }
            const bool queued = coordinator_executor_->submit(
                [this, snapshot, tp, epoch](bool admitted, auto ticket) {
                    (void)ticket;  // Retains the broker through queued/work shutdown draining.
                    const auto previous = coordinator_request_owner_;
                    coordinator_request_owner_ = RequestOwnership{this, tp, epoch};
                    try {
                        if (admitted) {
                            const auto error = prepareCoordinatorTransactionTimeout(snapshot);
                            if (error != ErrorCode::NONE)
                                Logger::warn("Coordinator transaction timeout '{}' refused: {}",
                                             snapshot.transactional_id, static_cast<int>(error));
                        }
                    } catch (const std::exception& ex) {
                        Logger::error("Coordinator transaction timeout '{}' failed: {}",
                                      snapshot.transactional_id, ex.what());
                    }
                    coordinator_request_owner_ = previous;
                    std::lock_guard<std::mutex> lock(coordinator_timeout_mutex_);
                    coordinator_transaction_timeout_pending_.erase(snapshot.transactional_id);
                });
            if (!queued) {
                std::lock_guard<std::mutex> lock(coordinator_timeout_mutex_);
                coordinator_transaction_timeout_pending_.erase(id);
            }
        }
    }
}

}  // namespace kawasan::broker
