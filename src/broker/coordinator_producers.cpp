#include <limits>

#include "kawasan/broker/kawasan_broker.h"
#include "kawasan/common/error.h"
#include "kawasan/common/logger.h"
#include "kawasan/protocol/init_producer_id_request.h"

namespace kawasan::broker {

void KawasanBroker::withBrokerProducerFence(const std::function<void(ErrorCode)>& publish) {
    std::shared_lock<std::shared_mutex> lifecycle(coordinator_mutation_lifecycle_mutex_,
                                                  std::try_to_lock);
    if (!lifecycle.owns_lock() || coordinator_mutations_stopping_.load() || !running_.load() ||
        !dataPlaneCurrent()) {
        publish(ErrorCode::NOT_COORDINATOR);
        return;
    }
    if (!staged_coordinator_persistence_ || !staged_coordinator_format_ ||
        !internal_topics_ready_.load() || !metadata_controller_) {
        publish(ErrorCode::COORDINATOR_LOAD_IN_PROGRESS);
        return;
    }
    metadata_controller_->withCoordinatorFormat(*staged_coordinator_format_, [&](auto error) {
        if (coordinator_mutations_stopping_.load() || !running_.load() || !dataPlaneCurrent())
            error = ErrorCode::NOT_COORDINATOR;
        else if (!internal_topics_ready_.load())
            error = ErrorCode::COORDINATOR_LOAD_IN_PROGRESS;
        // No source/cache access or metadata reentry. Covers socket write initiation.
        publish(error);
    });
}

RequestDispatcher::HandlerResult KawasanBroker::deferBrokerProducerInitialization(
    RequestDispatcher::RequestContext context, RequestDispatcher::HandlerFunc work,
    RequestDispatcher::ErrorBuilder errors) {
    if (!errors)
        throw std::invalid_argument("deferred broker producer request needs an error builder");
    auto error_context = context;
    error_context.connection = nullptr;
    error_context.complete = {};
    error_context.deferred_sink = {};
    auto error_payload = [errors, error_context](ErrorCode error) {
        return errors(error_context, error, error_context.header.apiVersion());
    };
    auto refuse = [&](ErrorCode error) {
        RequestDispatcher::HandlerResult result;
        result.payload = error_payload(error);
        return result;
    };
    if (!coordinator_executor_ || !context.complete || !work)
        return refuse(ErrorCode::INVALID_REQUEST);
    ErrorCode admission = ErrorCode::NOT_COORDINATOR;
    withBrokerProducerFence([&](auto status) { admission = status; });
    if (admission != ErrorCode::NONE)
        return refuse(admission);
    auto complete = context.complete;
    auto connection =
        context.connection
            ? std::make_shared<RequestDispatcher::ConnectionContext>(*context.connection)
            : nullptr;
    context.connection = connection.get();
    context.complete = {};
    context.deferred_sink = {};
    const bool queued = coordinator_executor_->submit(
        [this, context = std::move(context), connection, complete, work = std::move(work),
         error_payload](bool admitted, auto ticket) mutable {
            (void)connection;
            RequestDispatcher::HandlerResult result;
            auto status = ErrorCode::NOT_COORDINATOR;
            try {
                if (admitted) {
                    withBrokerProducerFence([&](auto error) { status = error; });
                    if (status == ErrorCode::NONE)
                        result = work(context);  // All counter I/O runs on this worker.
                    if (result.deferred)
                        status = ErrorCode::INVALID_REQUEST;
                }
            } catch (const ProtocolException&) {
                status = ErrorCode::INVALID_REQUEST;
            } catch (const KawasanException& ex) {
                status = ex.code();
            } catch (const std::exception& ex) {
                Logger::error("Deferred broker producer initialization failed: {}", ex.what());
                status = ErrorCode::KAFKA_STORAGE_ERROR;
            } catch (...) {
                status = ErrorCode::KAFKA_STORAGE_ERROR;
            }
            if (status != ErrorCode::NONE)
                result.payload = error_payload(status);
            result.deferred = false;
            result.lifetime = std::move(ticket);
            result.publication_error = [error_payload, status](auto error) {
                return error_payload(error == ErrorCode::COORDINATOR_LOAD_IN_PROGRESS &&
                                             status != ErrorCode::NONE
                                         ? status
                                         : error);
            };
            result.publication_guard = [this, ticket = result.lifetime](const auto& publish) {
                (void)ticket;
                withBrokerProducerFence(publish);
            };
            complete(std::move(result));
        });
    if (!queued)
        return refuse(coordinator_executor_->stopping() ? ErrorCode::NOT_COORDINATOR
                                                        : ErrorCode::COORDINATOR_LOAD_IN_PROGRESS);
    RequestDispatcher::HandlerResult parked;
    parked.deferred = true;
    return parked;
}

ErrorCode KawasanBroker::validateProducerInitialization(
    const protocol::InitProducerIdRequest& request) const {
    if (request.producerId() < -1 || request.producerEpoch() < -1 ||
        ((request.producerId() == -1) != (request.producerEpoch() == -1)) ||
        (request.transactionalId() && request.transactionalId()->empty()))
        return ErrorCode::INVALID_REQUEST;
    if (request.transactionalId() &&
        (request.transactionTimeoutMs() <= 0 ||
         request.transactionTimeoutMs() >
             config_.get<int64_t>("transaction.max.timeout.ms", 900000)))
        return ErrorCode::INVALID_TRANSACTION_TIMEOUT;
    return ErrorCode::NONE;
}

Buffer KawasanBroker::handleCoordinatorProducerInitialization(
    RequestDispatcher::RequestContext& context, const protocol::InitProducerIdRequest& request) {
    if (const auto error = validateProducerInitialization(request); error != ErrorCode::NONE)
        return buildInitProducerIdError(context, error, context.header.apiVersion());
    if (!request.transactionalId()) {
        // Kafka always allocates a fresh identity here. Valid expected fields
        // and the transaction timeout do not establish a transaction context.
        protocol::InitProducerIdResponse response;
        response.setProducerId(allocateNextProducerId(true));
        response.setProducerEpoch(0);
        return encodeResponse(
            context, [&](Buffer& output) { response.encode(output, context.header.apiVersion()); });
    }
    const auto& id = *request.transactionalId();
    const auto observed = transaction_coordinator_->describe(id);
    ErrorCode protocol_error = ErrorCode::NONE;
    int64_t producer_id = -1;
    int16_t producer_epoch = -1;
    const auto durable = mutateCoordinatorTransaction(id, observed, [&](auto& proposed) {
        const bool expected = request.producerId() != -1;
        bool retry = false;
        if (proposed) {
            const bool current_identity = request.producerId() == proposed->producer_id &&
                                          request.producerEpoch() == proposed->producer_epoch;
            retry = expected && request.producerId() == proposed->previous_producer_id &&
                    request.producerEpoch() == proposed->previous_producer_epoch;
            if (expected && !current_identity && !retry) {
                protocol_error = ErrorCode::PRODUCER_FENCED;
                return ErrorCode::NONE;
            }
            using State = TransactionCoordinator::State;
            if (proposed->state == State::Ongoing || proposed->state == State::PrepareCommit ||
                proposed->state == State::PrepareAbort ||
                proposed->state == State::PrepareEpochFence) {
                // Marker completion is a separate M10 dependency. Never abort
                // using the legacy local path or replace a pending identity.
                protocol_error = ErrorCode::CONCURRENT_TRANSACTIONS;
                return ErrorCode::NONE;
            }
            if ((proposed->state != State::Empty && proposed->state != State::CompleteCommit &&
                 proposed->state != State::CompleteAbort) ||
                !proposed->partitions.empty() || !proposed->pending_offsets.empty()) {
                protocol_error = ErrorCode::INVALID_TXN_STATE;
                return ErrorCode::NONE;
            }
        }

        if (retry) {
            // An unchanged retry needs neither an allocation nor a source
            // append. The mutation API still checks CAS and the owner epoch.
            producer_id = proposed->producer_id;
            producer_epoch = proposed->producer_epoch;
            if (proposed->transaction_timeout_ms != request.transactionTimeoutMs() ||
                proposed->state != TransactionCoordinator::State::Empty) {
                proposed->transaction_timeout_ms = request.transactionTimeoutMs();
                proposed->state = TransactionCoordinator::State::Empty;
                proposed->state_start_time_ms = transaction_coordinator_->nowMs();
            }
            return ErrorCode::NONE;
        }
        auto next = proposed.value_or(TransactionCoordinator::TxnSnapshot{});
        next.transactional_id = id;
        next.previous_producer_id = expected && proposed ? proposed->producer_id : -1;
        next.previous_producer_epoch = expected && proposed ? proposed->producer_epoch : -1;
        if (!proposed || proposed->producer_epoch >= std::numeric_limits<int16_t>::max() - 1) {
            // Reserve the final epoch for coordinator fencing, as Kafka does.
            // Durable allocation happens inside the original-epoch CAS lock;
            // failed source writes can leave gaps, but never reissue an ID.
            next.producer_id = allocateNextProducerId();
            next.producer_epoch = 0;
        } else {
            next.producer_epoch = static_cast<int16_t>(proposed->producer_epoch + 1);
        }
        next.transaction_timeout_ms = request.transactionTimeoutMs();
        next.state = TransactionCoordinator::State::Empty;
        next.state_start_time_ms = transaction_coordinator_->nowMs();
        producer_id = next.producer_id;
        producer_epoch = next.producer_epoch;
        proposed = std::move(next);
        return ErrorCode::NONE;
    });
    if (durable != ErrorCode::NONE) {
        // Only the full-snapshot comparison can return these statuses: the
        // proposal callback keeps protocol outcomes separate from persistence.
        // A concurrent initializer is retryable; expected identities fence.
        if (durable == ErrorCode::INVALID_TXN_STATE)
            throw KawasanException(ErrorCode::CONCURRENT_TRANSACTIONS);
        if (durable == ErrorCode::INVALID_PRODUCER_ID_MAPPING ||
            durable == ErrorCode::INVALID_PRODUCER_EPOCH)
            throw KawasanException(request.producerId() == -1 ? ErrorCode::CONCURRENT_TRANSACTIONS
                                                              : ErrorCode::PRODUCER_FENCED);
        throw KawasanException(durable);
    }
    if (protocol_error != ErrorCode::NONE)
        return buildInitProducerIdError(context, protocol_error, context.header.apiVersion());
    protocol::InitProducerIdResponse response;
    response.setProducerId(producer_id);
    response.setProducerEpoch(producer_epoch);
    return encodeResponse(
        context, [&](Buffer& output) { response.encode(output, context.header.apiVersion()); });
}

}  // namespace kawasan::broker
