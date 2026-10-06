#include <algorithm>
#include <set>
#include <thread>

#include "kawasan/broker/coordinator_routing.h"
#include "kawasan/broker/kawasan_broker.h"
#include "kawasan/broker/transaction_state_manager.h"
#include "kawasan/common/error.h"
#include "kawasan/common/logger.h"
#include "kawasan/storage/record_batch.h"

namespace kawasan::broker {

thread_local std::optional<KawasanBroker::RequestOwnership>
    KawasanBroker::coordinator_request_owner_;

// Lock order matches acquisition: mutation -> acquisition -> metadata. Try the
// first two locks because response publication runs on a socket IO thread.
void KawasanBroker::withCoordinatorRequestFence(const TopicPartition& tp, int32_t epoch,
                                                const std::function<void(ErrorCode)>& publish) {
    if (coordinator_mutations_stopping_.load() || !running_.load() || !dataPlaneCurrent()) {
        publish(ErrorCode::NOT_COORDINATOR);
        return;
    }
    // Known owner/epoch loss takes priority over a busy acquisition lock. The
    // atomic metadata fence below still rechecks changes after this snapshot.
    const auto metadata = currentPartitionMetadata(tp);
    if (!metadata) {
        publish(ErrorCode::COORDINATOR_LOAD_IN_PROGRESS);
        return;
    }
    if (metadata->leader != broker_id_ || metadata->leader_epoch != epoch) {
        publish(ErrorCode::NOT_COORDINATOR);
        return;
    }
    auto mutex = coordinatorMutationMutex(tp);
    std::unique_lock<std::mutex> mutation(*mutex, std::try_to_lock);
    std::unique_lock<std::mutex> acquisition(coordinator_acquisition_mutex_, std::try_to_lock);
    if (!mutation.owns_lock() || !acquisition.owns_lock()) {
        publish(ErrorCode::COORDINATOR_LOAD_IN_PROGRESS);
        return;
    }
    bool called = false;
    const auto status =
        metadata_controller_->withPartitionLeadership(tp, broker_id_, epoch, [&](const auto&) {
            const auto loaded = acquired_coordinator_epochs_.find(tp);
            auto error = ErrorCode::NONE;
            if (coordinator_mutations_stopping_.load() || !dataPlaneCurrent())
                error = ErrorCode::NOT_COORDINATOR;
            else if (loaded == acquired_coordinator_epochs_.end() || loaded->second != epoch ||
                     !replica_manager_->readableHighWatermark(tp, epoch) ||
                     failed_coordinator_appends_.contains(tp) ||
                     coordinator_commit_barriers_.contains(tp))
                error = ErrorCode::COORDINATOR_LOAD_IN_PROGRESS;
            called = true;
            publish(error);  // Includes actual transport write initiation.
            return error;
        });
    if (!called)
        publish(status);
}

RequestDispatcher::HandlerResult KawasanBroker::deferCoordinatorRequest(
    RequestDispatcher::RequestContext context, const std::string& key,
    protocol::CoordinatorType type, RequestDispatcher::HandlerFunc work,
    RequestDispatcher::ErrorBuilder errors, bool read_only) {
    if (!errors)
        throw std::invalid_argument("deferred coordinator request needs an error builder");
    auto error_context = context;
    error_context.connection = nullptr;
    error_context.complete = {};
    error_context.deferred_sink = {};
    auto error_payload = [errors, error_context](ErrorCode error) {
        return errors(error_context, error, error_context.header.apiVersion());
    };
    auto refusal = [&](ErrorCode error) {
        RequestDispatcher::HandlerResult result;
        result.payload = error_payload(error);
        return result;
    };
    if (!coordinator_executor_ || !context.complete || !work || key.empty() ||
        (type != protocol::CoordinatorType::GROUP &&
         type != protocol::CoordinatorType::TRANSACTION))
        return refusal(ErrorCode::INVALID_REQUEST);
    const std::string topic =
        type == protocol::CoordinatorType::GROUP ? "__consumer_offsets" : "__transaction_state";
    const TopicPartition tp{
        topic, coordinatorPartitionFor(key, staged_coordinator_format_->partitionCount(topic))};
    const auto metadata = currentPartitionMetadata(tp);
    if (!metadata)
        return refusal(ErrorCode::COORDINATOR_LOAD_IN_PROGRESS);
    const auto epoch = metadata->leader_epoch;
    ErrorCode admission = ErrorCode::NOT_COORDINATOR;
    withCoordinatorRequestFence(tp, epoch, [&](auto status) { admission = status; });
    if (admission != ErrorCode::NONE)
        return refusal(admission);
    auto complete = context.complete;
    // A deferred transport sink is weak; snapshot the authenticated identity so
    // an idle disconnect cannot leave a worker with a dangling connection pointer.
    auto connection =
        context.connection
            ? std::make_shared<RequestDispatcher::ConnectionContext>(*context.connection)
            : nullptr;
    context.connection = connection.get();
    context.complete = {};
    context.deferred_sink = {};
    const bool queued = coordinator_executor_->submit(
        [this, context = std::move(context), connection, complete, work = std::move(work),
         error_payload, tp, epoch, read_only](bool admitted, auto ticket) mutable {
            (void)connection;
            RequestDispatcher::HandlerResult result;
            ErrorCode status = ErrorCode::NOT_COORDINATOR;
            const auto previous = coordinator_request_owner_;
            coordinator_request_owner_ = RequestOwnership{this, tp, epoch};
            try {
                if (admitted) {
                    withCoordinatorRequestFence(tp, epoch, [&](auto error) {
                        status = error;
                        if (read_only && status == ErrorCode::NONE)
                            result = work(context);
                    });
                    if (!read_only && status == ErrorCode::NONE)
                        result = work(context);  // Mutations retain the admitted epoch below.
                    if (result.deferred)
                        status = ErrorCode::INVALID_REQUEST;  // No nested deferrals.
                }
            } catch (const ProtocolException&) {
                status = ErrorCode::INVALID_REQUEST;
            } catch (const KawasanException& ex) {
                status = ex.code();
            } catch (const std::exception& ex) {
                Logger::error("Deferred coordinator handler failed: {}", ex.what());
                status = ErrorCode::KAFKA_STORAGE_ERROR;
            } catch (...) {
                status = ErrorCode::KAFKA_STORAGE_ERROR;
            }
            coordinator_request_owner_ = previous;
            if (status != ErrorCode::NONE)
                result.payload = error_payload(status);
            result.deferred = false;
            result.lifetime = std::move(ticket);
            result.publication_error = error_payload;
            result.publication_guard = [this, tp, epoch,
                                        ticket = result.lifetime](const auto& publish) {
                (void)ticket;
                withCoordinatorRequestFence(tp, epoch, publish);
            };
            complete(std::move(result));
        });
    if (!queued)
        return refusal(coordinator_executor_->stopping() ? ErrorCode::NOT_COORDINATOR
                                                         : ErrorCode::COORDINATOR_LOAD_IN_PROGRESS);
    RequestDispatcher::HandlerResult parked;
    parked.deferred = true;
    return parked;
}

std::shared_ptr<std::mutex> KawasanBroker::coordinatorMutationMutex(const TopicPartition& tp) {
    std::lock_guard<std::mutex> lock(coordinator_mutation_map_mutex_);
    auto& mutex = coordinator_mutation_mutexes_[tp];
    if (!mutex)
        mutex = std::make_shared<std::mutex>();
    return mutex;
}

// Caller retains the coordinator mutation lock through admission, proposal,
// persistence and publication. Partition writes and metadata locks are held
// only during append/publication; follower Fetch and ISR changes must run while
// waiting. Acquisition tries the mutation lock and skips busy partitions.
ErrorCode KawasanBroker::commitCoordinatorRecords(const TopicPartition& tp, int32_t epoch,
                                                  const std::vector<Record>& records,
                                                  const std::function<void()>& publish,
                                                  std::chrono::milliseconds timeout) {
    if (records.empty() || timeout.count() < 0)
        return ErrorCode::INVALID_REQUEST;
    auto* log = log_manager_->getLog(tp.topic, tp.partition);
    if (!log)
        return ErrorCode::KAFKA_STORAGE_ERROR;
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    Offset required = log->logEndOffset();
    bool attempted = false;
    bool appended = false;
    auto invalidate = [&] {
        if (!attempted)
            return;
        std::lock_guard<std::mutex> lock(coordinator_acquisition_mutex_);
        acquired_coordinator_epochs_.erase(tp);
        coordinator_commit_barriers_[tp] = {epoch, std::max(required, log->logEndOffset())};
        if (!appended)
            failed_coordinator_appends_.insert(tp);
    };
    auto fence = [&](const PartitionMetadata& metadata, bool appended) {
        if (coordinator_mutations_stopping_.load() || !running_.load() || !dataPlaneCurrent())
            return ErrorCode::NOT_COORDINATOR;
        if (metadata.replicas.size() != std::min<size_t>(3, cluster_brokers_.size() + 1))
            return ErrorCode::INVALID_REQUEST;  // Never reuse staged RF=1 as replicated state.
        if (!replica_manager_->isLeader(tp) || replica_manager_->getLeaderEpoch(tp) != epoch ||
            !replica_manager_->readableHighWatermark(tp, epoch))
            return ErrorCode::COORDINATOR_LOAD_IN_PROGRESS;
        const size_t minimum_isr = std::min<size_t>(2, metadata.replicas.size());
        if (metadata.isr.size() < minimum_isr)
            return appended ? ErrorCode::NOT_ENOUGH_REPLICAS_AFTER_APPEND
                            : ErrorCode::NOT_ENOUGH_REPLICAS;
        replica_manager_->updateISR(tp, metadata.isr);
        if (replica_manager_->getISR(tp).size() < minimum_isr)
            return appended ? ErrorCode::NOT_ENOUGH_REPLICAS_AFTER_APPEND
                            : ErrorCode::NOT_ENOUGH_REPLICAS;
        return ErrorCode::NONE;
    };
    try {
        ErrorCode error;
        {
            auto write_lock = lockPartitionWrites(tp);
            error = metadata_controller_->withPartitionLeadership(
                tp, broker_id_, epoch, [&](const auto& metadata) {
                    const auto status = fence(metadata, false);
                    if (status != ErrorCode::NONE)
                        return status;
                    // A timed-out tail must resolve before another proposal is
                    // based on a committed cache. Acquisition alone is insufficient.
                    if (log->highWatermark() != log->logEndOffset())
                        return ErrorCode::COORDINATOR_LOAD_IN_PROGRESS;
                    storage::RecordBatch batch;
                    for (const auto& record : records)
                        batch.addRecord(record);
                    attempted = true;
                    const auto base = log->appendBatch(std::move(batch), metadata.isr.size() == 1,
                                                       /*force_sync=*/true);
                    appended = true;
                    required = base + static_cast<Offset>(records.size());
                    replica_manager_->maybeAdvanceHighWatermark(tp);
                    return ErrorCode::NONE;
                });
        }
        if (error != ErrorCode::NONE) {
            invalidate();
            return error;
        }
        for (;;) {
            {
                auto write_lock = lockPartitionWrites(tp);
                error = metadata_controller_->withPartitionLeadership(
                    tp, broker_id_, epoch, [&](const auto& metadata) {
                        const auto status = fence(metadata, true);
                        if (status != ErrorCode::NONE)
                            return status;
                        const auto committed = replica_manager_->isrCommittedOffset(tp);
                        if (!committed || *committed < required || log->highWatermark() < required)
                            return ErrorCode::REQUEST_TIMED_OUT;
                        // Strict checkpoint errors must abort publication, even
                        // after replication succeeds. The source remains authoritative.
                        log->flushCheckpoint(/*strict=*/true);
                        if (coordinator_mutations_stopping_.load() || !dataPlaneCurrent())
                            return ErrorCode::NOT_COORDINATOR;
                        publish();
                        return !coordinator_mutations_stopping_.load() && dataPlaneCurrent()
                                   ? ErrorCode::NONE
                                   : ErrorCode::NOT_COORDINATOR;
                    });
            }
            if (error != ErrorCode::REQUEST_TIMED_OUT ||
                std::chrono::steady_clock::now() >= deadline) {
                if (error != ErrorCode::NONE)
                    invalidate();
                return error;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
    } catch (const std::exception& ex) {
        invalidate();
        Logger::error("Coordinator durable mutation {}-{} failed: {}", tp.topic, tp.partition,
                      ex.what());
        return ErrorCode::KAFKA_STORAGE_ERROR;
    }
}

ErrorCode KawasanBroker::mutateCoordinatorGroup(const std::string& group_id,
                                                const GroupMutation& mutation,
                                                std::chrono::milliseconds timeout) {
    return mutateCoordinatorGroupImpl(group_id, mutation, timeout);
}

ErrorCode KawasanBroker::mutateCoordinatorGroupImpl(
    const std::string& group_id, const GroupMutation& mutation, std::chrono::milliseconds timeout,
    const std::unique_ptr<GroupCoordinator::GroupProposal>* proposal) {
    if (!staged_coordinator_persistence_ || group_id.empty() || !mutation || timeout.count() < 0)
        return ErrorCode::INVALID_REQUEST;
    if (coordinator_mutations_stopping_.load())
        return ErrorCode::NOT_COORDINATOR;
    std::shared_lock<std::shared_mutex> lifecycle_lock(coordinator_mutation_lifecycle_mutex_);
    if (coordinator_mutations_stopping_.load())
        return ErrorCode::NOT_COORDINATOR;
    const TopicPartition tp{"__consumer_offsets",
                            coordinatorPartitionFor(group_id, offsets_topic_num_partitions_)};
    auto mutex = coordinatorMutationMutex(tp);
    std::lock_guard<std::mutex> mutation_lock(*mutex);
    if (coordinator_mutations_stopping_.load())
        return ErrorCode::NOT_COORDINATOR;
    const auto status = coordinatorLoadStatus(group_id, protocol::CoordinatorType::GROUP);
    if (status != ErrorCode::NONE)
        return status;
    const auto metadata = currentPartitionMetadata(tp);
    if (!metadata)
        return ErrorCode::NOT_COORDINATOR;
    if (coordinator_request_owner_ && coordinator_request_owner_->broker == this) {
        if (coordinator_request_owner_->tp != tp)
            return ErrorCode::INVALID_REQUEST;
        if (coordinator_request_owner_->epoch != metadata->leader_epoch)
            return ErrorCode::NOT_COORDINATOR;
    }
    try {
        GroupStateManager source(log_manager_.get(), offsets_topic_num_partitions_);
        auto image = source.loadCommittedPartition(tp.partition);
        std::vector<GroupRecord> current;
        for (const auto& [key, record] : image)
            if (key.group_id == group_id)
                current.push_back(record);
        std::vector<GroupRecord> changes;
        const auto admitted = mutation(current, changes);
        if (admitted != ErrorCode::NONE)
            return admitted;
        std::set<GroupRecordKey> keys;
        std::vector<Record> records;
        for (const auto& change : changes) {
            if (change.key.group_id != group_id || !keys.insert(change.key).second)
                return ErrorCode::INVALID_REQUEST;
            auto record = GroupStateManager::encode(change);
            // Encoder/decoder validation precedes the first storage mutation.
            const auto validated = GroupStateManager::decode(record);
            if (validated.tombstone)
                image.erase(validated.key);
            else
                image[validated.key] = validated;
            records.push_back(std::move(record));
        }
        std::vector<GroupRecord> proposed;
        for (const auto& [key, record] : image) {
            (void)key;
            proposed.push_back(record);
        }
        return commitCoordinatorRecords(
            tp, metadata->leader_epoch, records,
            [&] {
                group_coordinator_->publishCoordinatorPartition(
                    tp.partition, offsets_topic_num_partitions_, proposed, offset_manager_,
                    proposal ? proposal->get() : nullptr);
            },
            timeout);
    } catch (const std::exception& ex) {
        Logger::error("Coordinator group proposal '{}' failed: {}", group_id, ex.what());
        return ErrorCode::KAFKA_STORAGE_ERROR;
    }
}

ErrorCode KawasanBroker::mutateCoordinatorGroupState(const std::string& group_id,
                                                     const GroupStateMutation& mutation,
                                                     std::chrono::milliseconds timeout) {
    if (!mutation)
        return ErrorCode::INVALID_REQUEST;
    std::unique_ptr<GroupCoordinator::GroupProposal> proposal;
    return mutateCoordinatorGroupImpl(
        group_id,
        [&](const auto& current, auto& changes) {
            std::optional<GroupRecord> committed;
            for (const auto& record : current)
                if (record.key.kind == GroupRecordKey::Kind::Group)
                    committed = record;
            proposal = group_coordinator_->proposeGroup(group_id, committed);
            mutation(*proposal);
            if (const auto record = proposal->record()) {
                changes.push_back(*record);
            } else {
                // An unknown-group protocol error must not invent group state.
                // An exact identity tombstone leaves any offsets/pending keys
                // intact and still fences admission through durable publication.
                GroupRecord tombstone;
                tombstone.key.group_id = group_id;
                tombstone.tombstone = true;
                changes.push_back(std::move(tombstone));
            }
            return ErrorCode::NONE;
        },
        timeout, &proposal);
}

ErrorCode KawasanBroker::deleteCoordinatorGroup(const std::string& group_id,
                                                std::chrono::milliseconds timeout) {
    return mutateCoordinatorGroup(
        group_id,
        [group_id](const auto& current, auto& changes) {
            for (const auto& record : current) {
                auto tombstone = record;
                tombstone.tombstone = true;
                changes.push_back(std::move(tombstone));
            }
            // Persisting an empty group's identity tombstone makes deletion
            // idempotent without inventing wildcard tombstone semantics.
            if (changes.empty()) {
                GroupRecord tombstone;
                tombstone.key.group_id = group_id;
                tombstone.tombstone = true;
                changes.push_back(std::move(tombstone));
            }
            return ErrorCode::NONE;
        },
        timeout);
}

ErrorCode KawasanBroker::mutateCoordinatorTransaction(
    const std::string& transactional_id,
    const std::optional<TransactionCoordinator::TxnSnapshot>& expected,
    const TransactionMutation& mutation, std::chrono::milliseconds timeout) {
    if (!staged_coordinator_persistence_ || transactional_id.empty() || !mutation ||
        timeout.count() < 0)
        return ErrorCode::INVALID_REQUEST;
    if (coordinator_mutations_stopping_.load())
        return ErrorCode::NOT_COORDINATOR;
    std::shared_lock<std::shared_mutex> lifecycle_lock(coordinator_mutation_lifecycle_mutex_);
    if (coordinator_mutations_stopping_.load())
        return ErrorCode::NOT_COORDINATOR;
    const TopicPartition tp{"__transaction_state",
                            coordinatorPartitionFor(transactional_id, txn_state_num_partitions_)};
    auto mutex = coordinatorMutationMutex(tp);
    std::lock_guard<std::mutex> mutation_lock(*mutex);
    if (coordinator_mutations_stopping_.load())
        return ErrorCode::NOT_COORDINATOR;
    const auto status =
        coordinatorLoadStatus(transactional_id, protocol::CoordinatorType::TRANSACTION);
    if (status != ErrorCode::NONE)
        return status;
    const auto metadata = currentPartitionMetadata(tp);
    if (!metadata)
        return ErrorCode::NOT_COORDINATOR;
    if (coordinator_request_owner_ && coordinator_request_owner_->broker == this) {
        if (coordinator_request_owner_->tp != tp)
            return ErrorCode::INVALID_REQUEST;
        if (coordinator_request_owner_->epoch != metadata->leader_epoch)
            return ErrorCode::NOT_COORDINATOR;
    }
    auto proposed = transaction_coordinator_->describe(transactional_id);
    if (expected != proposed) {
        if (expected && proposed && expected->producer_id != proposed->producer_id)
            return ErrorCode::INVALID_PRODUCER_ID_MAPPING;
        if (expected && proposed && expected->producer_epoch != proposed->producer_epoch)
            return ErrorCode::INVALID_PRODUCER_EPOCH;
        return ErrorCode::INVALID_TXN_STATE;
    }
    try {
        const auto admitted = mutation(proposed);
        if (admitted != ErrorCode::NONE)
            return admitted;
        Record record;
        record.key = std::vector<uint8_t>(transactional_id.begin(), transactional_id.end());
        if (proposed) {
            if (proposed->transactional_id != transactional_id || proposed->producer_id < 0 ||
                proposed->producer_epoch < 0)
                return ErrorCode::INVALID_REQUEST;
            record.value = TransactionStateManager::serialize(*proposed);
            (void)TransactionStateManager::deserialize(*record.value);
        }
        // Build the complete replacement without changing the live cache.
        auto image = transaction_state_manager_->loadCommittedPartition(tp.partition);
        std::erase_if(image, [&](const auto& snapshot) {
            return snapshot.transactional_id == transactional_id;
        });
        if (proposed)
            image.push_back(*proposed);
        return commitCoordinatorRecords(
            tp, metadata->leader_epoch, {record},
            [&] {
                transaction_coordinator_->replaceCoordinatorPartition(
                    tp.partition, txn_state_num_partitions_, image);
            },
            timeout);
    } catch (const std::exception& ex) {
        Logger::error("Coordinator transaction proposal '{}' failed: {}", transactional_id,
                      ex.what());
        return ErrorCode::KAFKA_STORAGE_ERROR;
    }
}

}  // namespace kawasan::broker
