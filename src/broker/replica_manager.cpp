#include "kawasan/broker/replica_manager.h"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <chrono>

#include "kawasan/broker/kawasan_broker.h"
#include "kawasan/broker/peer_client.h"
#include "kawasan/common/buffer.h"
#include "kawasan/storage/record_batch.h"

namespace kawasan::broker {

ReplicaManager::ReplicaManager() {
    spdlog::info("ReplicaManager initialized (single-node mode)");
}

ReplicaManager::~ReplicaManager() {
    stop();
    spdlog::info("ReplicaManager shutting down");
}

void ReplicaManager::addReplica(const TopicPartition& tp, std::shared_ptr<storage::Log> log) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (replicas_.find(tp) != replicas_.end()) {
        spdlog::warn("Replica for {}-{} already exists, updating", tp.topic, tp.partition);
    }

    ReplicaInfo info;
    info.log = log;
    info.leader = local_broker_id_;           // In single-node mode, we're always the leader
    info.isr = {local_broker_id_};            // ISR contains only this broker
    info.fetch_offset = log->logEndOffset();  // Start fetching from current end offset

    replicas_[tp] = info;

    spdlog::debug("Added replica for {}-{}, leader={}, ISR size={}, fetch_offset={}", tp.topic,
                  tp.partition, info.leader, info.isr.size(), info.fetch_offset);
}

void ReplicaManager::removeReplica(const TopicPartition& tp) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = replicas_.find(tp);
    if (it == replicas_.end()) {
        spdlog::warn("Attempted to remove non-existent replica for {}-{}", tp.topic, tp.partition);
        return;
    }

    replicas_.erase(it);
    spdlog::debug("Removed replica for {}-{}", tp.topic, tp.partition);
}

bool ReplicaManager::isLeader(const TopicPartition& tp) const {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = replicas_.find(tp);
    if (it == replicas_.end()) {
        return false;
    }

    // In single-node mode, we're always the leader if we have the replica
    return it->second.leader == local_broker_id_;
}

std::optional<BrokerId> ReplicaManager::getLeader(const TopicPartition& tp) const {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = replicas_.find(tp);
    if (it == replicas_.end()) {
        return std::nullopt;
    }

    return it->second.leader;
}

std::optional<Offset> ReplicaManager::getHighWatermark(const TopicPartition& tp) const {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = replicas_.find(tp);
    if (it == replicas_.end()) {
        return std::nullopt;
    }

    return it->second.log->highWatermark();
}

void ReplicaManager::updateHighWatermark(const TopicPartition& tp, Offset hw) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = replicas_.find(tp);
    if (it == replicas_.end()) {
        spdlog::warn("Attempted to update high watermark for non-existent replica {}-{}", tp.topic,
                     tp.partition);
        return;
    }

    it->second.log->setHighWatermark(hw);
    spdlog::trace("Updated high watermark for {}-{} to {}", tp.topic, tp.partition, hw);
}

Offset ReplicaManager::computeHighWatermarkLocked(const ReplicaInfo& info) const {
    // HW starts at the leader's log-end-offset and is pulled back to the slowest
    // in-sync follower. With only the leader in the ISR (single-node) the loop
    // body never runs, so HW == leader LEO and behavior is unchanged.
    Offset hw = info.log->logEndOffset();
    for (BrokerId id : info.isr) {
        if (id == local_broker_id_) {
            continue;  // leader contributes its LEO, already the starting value
        }
        auto fit = info.follower_states.find(id);
        const Offset follower_offset = (fit != info.follower_states.end())
                                           ? fit->second.last_fetched_offset
                                           : info.log->logStartOffset();
        hw = std::min(hw, follower_offset);
    }
    return hw;
}

Offset ReplicaManager::maybeAdvanceHighWatermark(const TopicPartition& tp) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = replicas_.find(tp);
    if (it == replicas_.end()) {
        return 0;
    }
    const Offset new_hw = computeHighWatermarkLocked(it->second);
    const Offset current_hw = it->second.log->highWatermark();
    if (new_hw > current_hw) {
        it->second.log->setHighWatermark(new_hw);
        spdlog::trace("Advanced high watermark for {}-{} to {} (from {})", tp.topic, tp.partition,
                      new_hw, current_hw);
        return new_hw;
    }
    return current_hw;  // HW never moves backward
}

std::optional<Offset> ReplicaManager::isrCommittedOffset(const TopicPartition& tp) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = replicas_.find(tp);
    if (it == replicas_.end()) {
        return std::nullopt;
    }
    return computeHighWatermarkLocked(it->second);
}

std::optional<int32_t> ReplicaManager::getLeaderEpoch(const TopicPartition& tp) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = replicas_.find(tp);
    if (it == replicas_.end()) {
        return std::nullopt;
    }
    return it->second.leader_epoch;
}

std::optional<int32_t> ReplicaManager::bumpLeaderEpoch(const TopicPartition& tp) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = replicas_.find(tp);
    if (it == replicas_.end()) {
        return std::nullopt;
    }
    it->second.leader_epoch += 1;
    spdlog::info("Leader epoch for {}-{} bumped to {}", tp.topic, tp.partition,
                 it->second.leader_epoch);
    return it->second.leader_epoch;
}

std::vector<BrokerId> ReplicaManager::getISR(const TopicPartition& tp) const {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = replicas_.find(tp);
    if (it == replicas_.end()) {
        return {};
    }

    return it->second.isr;
}

void ReplicaManager::updateISR(const TopicPartition& tp, const std::vector<BrokerId>& isr) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = replicas_.find(tp);
    if (it == replicas_.end()) {
        spdlog::warn("Attempted to update ISR for non-existent replica {}-{}", tp.topic,
                     tp.partition);
        return;
    }

    it->second.isr = isr;
    spdlog::debug("Updated ISR for {}-{}, new ISR size={}", tp.topic, tp.partition, isr.size());
}

std::shared_ptr<storage::Log> ReplicaManager::getLog(const TopicPartition& tp) const {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = replicas_.find(tp);
    if (it == replicas_.end()) {
        return nullptr;
    }

    return it->second.log;
}

std::vector<TopicPartition> ReplicaManager::getAllReplicas() const {
    std::lock_guard<std::mutex> lock(mutex_);

    std::vector<TopicPartition> result;
    result.reserve(replicas_.size());

    for (const auto& [tp, _] : replicas_) {
        result.push_back(tp);
    }

    return result;
}

void ReplicaManager::setBroker(KawasanBroker* broker) {
    broker_ = broker;
    spdlog::debug("ReplicaManager: broker reference set");
}

void ReplicaManager::start() {
    if (running_) {
        spdlog::warn("ReplicaManager already running");
        return;
    }

    running_ = true;
    fetcher_thread_ = std::thread(&ReplicaManager::fetcherThreadLoop, this);
    spdlog::info("ReplicaManager: follower fetch thread started");
}

void ReplicaManager::stop() {
    if (!running_) {
        return;
    }

    running_ = false;
    if (fetcher_thread_.joinable()) {
        fetcher_thread_.join();
    }
    spdlog::info("ReplicaManager: follower fetch thread stopped");
}

void ReplicaManager::fetcherThreadLoop() {
    spdlog::info("Follower fetch thread started");

    while (running_) {
        try {
            // M5: reconcile ReplicaManager against the latest committed metadata
            // (register/update leader + follower replicas). Done OUTSIDE mutex_.
            if (broker_) {
                broker_->reconcileReplicas();
            }

            // Snapshot the follower fetch work under the lock, so the blocking
            // network fetch below never holds mutex_. fetchPartitionFromLeader
            // writes results back into replicas_ (fixing the old by-value bug).
            std::vector<FetchTask> tasks;
            {
                std::lock_guard<std::mutex> lock(mutex_);
                for (auto& [tp, info] : replicas_) {
                    if (info.leader != local_broker_id_ && info.log) {
                        tasks.push_back({tp, info.leader, info.fetch_offset, info.log});
                    }
                }
            }
            for (const auto& task : tasks) {
                if (!running_) {
                    break;
                }
                fetchPartitionFromLeader(task);
            }
        } catch (const std::exception& e) {
            spdlog::error("Error in follower fetch thread: {}", e.what());
        }

        // Sleep in small steps so stop() stays responsive.
        for (int64_t slept = 0; slept < fetcher_interval_ms_ && running_; slept += 20) {
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
    }

    spdlog::info("Follower fetch thread stopped");
}

PeerClient* ReplicaManager::peerClientFor(BrokerId leader, const std::string& host, int32_t port) {
    auto it = peer_clients_.find(leader);
    if (it != peer_clients_.end()) {
        // Recreate if the leader's advertised endpoint changed.
        if (it->second->host() != host || it->second->port() != port) {
            peer_clients_.erase(it);
        } else {
            return it->second.get();
        }
    }
    auto client = std::make_unique<PeerClient>(host, port, local_broker_id_);
    PeerClient* raw = client.get();
    peer_clients_[leader] = std::move(client);
    return raw;
}

void ReplicaManager::fetchPartitionFromLeader(const FetchTask& task) {
    if (!broker_) {
        return;
    }
    // Resolve the leader's Kafka listener address from cluster metadata.
    auto endpoint = broker_->peerEndpoint(task.leader);
    if (!endpoint) {
        spdlog::trace("No endpoint for leader {} of {}-{}; skipping", task.leader, task.tp.topic,
                      task.tp.partition);
        return;
    }

    PeerClient* client = peerClientFor(task.leader, endpoint->first, endpoint->second);
    auto result = client->fetch(task.tp.topic, task.tp.partition, task.fetch_offset);
    if (!result) {
        // Connection/protocol error: drop the cached client so next cycle
        // reconnects; the loop sleep provides the backoff.
        peer_clients_.erase(task.leader);
        return;
    }
    if (result->error != ErrorCode::NONE) {
        // Leadership may have moved; the next reconcile fixes our view.
        spdlog::debug("Fetch from leader {} for {}-{} returned error {}", task.leader,
                      task.tp.topic, task.tp.partition, static_cast<int16_t>(result->error));
        return;
    }

    // Ingest the leader's raw batches, offset-preserved. Stop on a gap (the
    // follower diverged; leader-epoch truncation is M7).
    if (!result->record_batches.empty()) {
        Buffer buf(result->record_batches);
        while (buf.remaining() >= 12) {
            storage::RecordBatch batch;
            try {
                batch = storage::RecordBatch::deserialize(buf);
            } catch (const std::exception& e) {
                spdlog::warn("Malformed replicated batch for {}-{}: {}", task.tp.topic,
                             task.tp.partition, e.what());
                break;
            }
            const auto r = task.log->appendReplicatedBatch(batch);
            if (r == storage::Log::ReplicaAppendResult::kGap) {
                break;  // need truncation before continuing (M7)
            }
            // kDuplicate: already had it; kAppended: continue.
        }
    }

    // Adopt the leader's high watermark, clamped to what we actually hold, so a
    // follower never exposes records the leader has not committed.
    const Offset new_leo = task.log->logEndOffset();
    task.log->setHighWatermark(std::min(result->high_watermark, new_leo));

    // Write the advanced fetch offset back into the live map entry.
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = replicas_.find(task.tp);
        if (it != replicas_.end()) {
            it->second.fetch_offset = new_leo;
        }
    }
}

void ReplicaManager::updateFollowerFetchOffset(const TopicPartition& tp, BrokerId broker_id,
                                               Offset offset) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = replicas_.find(tp);
    if (it == replicas_.end()) {
        spdlog::warn("Attempted to update follower offset for non-existent replica {}-{}", tp.topic,
                     tp.partition);
        return;
    }

    // Only track follower state if we're the leader
    if (it->second.leader != local_broker_id_) {
        return;
    }

    auto& follower_state = it->second.follower_states[broker_id];
    follower_state.last_fetched_offset = offset;
    follower_state.last_update_time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                             std::chrono::system_clock::now().time_since_epoch())
                                             .count();

    spdlog::trace("Updated follower offset for broker {} on {}-{} to {}", broker_id, tp.topic,
                  tp.partition, offset);
}

void ReplicaManager::reconcileReplica(const TopicPartition& tp, std::shared_ptr<storage::Log> log,
                                      BrokerId leader, const std::vector<BrokerId>& isr,
                                      int32_t leader_epoch) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = replicas_.find(tp);
    if (it == replicas_.end()) {
        ReplicaInfo info;
        info.log = std::move(log);
        info.leader = leader;
        info.isr = isr;
        info.leader_epoch = leader_epoch;
        // A follower starts fetching from its current log-end; a leader's
        // fetch_offset is unused.
        info.fetch_offset = (leader != local_broker_id_) ? info.log->logEndOffset() : 0;
        replicas_[tp] = std::move(info);
        spdlog::debug("Reconciled NEW replica {}-{} leader={} isr={} (follower={})", tp.topic,
                      tp.partition, leader, isr.size(), leader != local_broker_id_);
        return;
    }
    // Existing: update role/ISR/epoch but preserve fetch progress + follower_states.
    auto& info = it->second;
    if (!info.log && log) {
        info.log = std::move(log);
    }
    info.leader = leader;
    info.isr = isr;
    info.leader_epoch = leader_epoch;
}

bool ReplicaManager::checkAndUpdateISR(const TopicPartition& tp) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = replicas_.find(tp);
    if (it == replicas_.end()) {
        return false;
    }

    // Only check ISR if we're the leader
    if (it->second.leader != local_broker_id_) {
        return false;
    }

    auto& replica_info = it->second;
    auto log_end_offset = replica_info.log->logEndOffset();

    std::vector<BrokerId> new_isr;
    bool isr_changed = false;

    // Leader is always in ISR
    new_isr.push_back(local_broker_id_);

    // Check each follower's lag
    for (BrokerId follower_id : replica_info.isr) {
        if (follower_id == local_broker_id_) {
            continue;  // Skip leader
        }

        auto follower_it = replica_info.follower_states.find(follower_id);
        if (follower_it == replica_info.follower_states.end()) {
            // No state for this follower, remove from ISR
            spdlog::warn("Removing broker {} from ISR for {}-{}: no follower state", follower_id,
                         tp.topic, tp.partition);
            isr_changed = true;
            continue;
        }

        auto& follower_state = follower_it->second;
        int64_t lag = log_end_offset - follower_state.last_fetched_offset;

        if (lag > max_replica_lag_messages_) {
            // Follower is lagging too far, remove from ISR
            spdlog::warn("Removing broker {} from ISR for {}-{}: lag {} exceeds max {}",
                         follower_id, tp.topic, tp.partition, lag, max_replica_lag_messages_);
            isr_changed = true;
        } else {
            // Follower is keeping up, keep in ISR
            new_isr.push_back(follower_id);
        }
    }

    // Check if any followers that were out of ISR have caught up
    for (const auto& [follower_id, state] : replica_info.follower_states) {
        if (follower_id == local_broker_id_) {
            continue;
        }

        // Skip if already in ISR
        if (std::find(new_isr.begin(), new_isr.end(), follower_id) != new_isr.end()) {
            continue;
        }

        int64_t lag = log_end_offset - state.last_fetched_offset;

        if (lag <= max_replica_lag_messages_) {
            // Follower has caught up, add to ISR
            spdlog::info("Adding broker {} to ISR for {}-{}: lag {} is within limit {}",
                         follower_id, tp.topic, tp.partition, lag, max_replica_lag_messages_);
            new_isr.push_back(follower_id);
            isr_changed = true;
        }
    }

    if (isr_changed) {
        replica_info.isr = new_isr;
        spdlog::info("ISR updated for {}-{}, new ISR size: {}", tp.topic, tp.partition,
                     new_isr.size());
    }

    return isr_changed;
}

std::optional<int64_t> ReplicaManager::getFollowerLag(const TopicPartition& tp,
                                                      BrokerId broker_id) const {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = replicas_.find(tp);
    if (it == replicas_.end()) {
        return std::nullopt;
    }

    // Only track lag if we're the leader
    if (it->second.leader != local_broker_id_) {
        return std::nullopt;
    }

    auto follower_it = it->second.follower_states.find(broker_id);
    if (follower_it == it->second.follower_states.end()) {
        return std::nullopt;
    }

    auto log_end_offset = it->second.log->logEndOffset();
    int64_t lag = log_end_offset - follower_it->second.last_fetched_offset;

    return lag;
}

}  // namespace kawasan::broker
