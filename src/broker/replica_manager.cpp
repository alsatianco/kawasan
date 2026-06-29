#include "kawasan/broker/replica_manager.h"

#include <algorithm>
#include <chrono>
#include <spdlog/spdlog.h>

#include "kawasan/broker/kawasan_broker.h"

namespace kawasan::broker {

ReplicaManager::ReplicaManager() {
    spdlog::info("ReplicaManager initialized (single-node mode)");
}

ReplicaManager::~ReplicaManager() {
    stop();
    spdlog::info("ReplicaManager shutting down");
}

void ReplicaManager::addReplica(const TopicPartition& tp,
                                std::shared_ptr<storage::Log> log) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (replicas_.find(tp) != replicas_.end()) {
        spdlog::warn("Replica for {}-{} already exists, updating", tp.topic, tp.partition);
    }
    
    ReplicaInfo info;
    info.log = log;
    info.leader = local_broker_id_;  // In single-node mode, we're always the leader
    info.isr = {local_broker_id_};   // ISR contains only this broker
    info.fetch_offset = log->logEndOffset();  // Start fetching from current end offset
    
    replicas_[tp] = info;
    
    spdlog::debug("Added replica for {}-{}, leader={}, ISR size={}, fetch_offset={}",
                  tp.topic, tp.partition, info.leader, info.isr.size(), info.fetch_offset);
}

void ReplicaManager::removeReplica(const TopicPartition& tp) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto it = replicas_.find(tp);
    if (it == replicas_.end()) {
        spdlog::warn("Attempted to remove non-existent replica for {}-{}",
                     tp.topic, tp.partition);
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
        spdlog::warn("Attempted to update high watermark for non-existent replica {}-{}",
                     tp.topic, tp.partition);
        return;
    }
    
    it->second.log->setHighWatermark(hw);
    spdlog::trace("Updated high watermark for {}-{} to {}",
                  tp.topic, tp.partition, hw);
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
        spdlog::trace("Advanced high watermark for {}-{} to {} (from {})",
                      tp.topic, tp.partition, new_hw, current_hw);
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

void ReplicaManager::updateISR(const TopicPartition& tp,
                               const std::vector<BrokerId>& isr) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto it = replicas_.find(tp);
    if (it == replicas_.end()) {
        spdlog::warn("Attempted to update ISR for non-existent replica {}-{}",
                     tp.topic, tp.partition);
        return;
    }
    
    it->second.isr = isr;
    spdlog::debug("Updated ISR for {}-{}, new ISR size={}",
                  tp.topic, tp.partition, isr.size());
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
            // Get all replicas and check which ones we're followers for
            std::vector<std::pair<TopicPartition, ReplicaInfo>> replicas_to_fetch;
            {
                std::lock_guard<std::mutex> lock(mutex_);
                for (auto& [tp, info] : replicas_) {
                    // If we're not the leader, we need to fetch from the leader
                    if (info.leader != local_broker_id_) {
                        replicas_to_fetch.emplace_back(tp, info);
                    }
                }
            }
            
            // Fetch from leaders for each follower replica
            for (auto& [tp, info] : replicas_to_fetch) {
                fetchFromLeader(tp, info);
            }
            
            // Sleep for 100ms before next fetch cycle
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            
        } catch (const std::exception& e) {
            spdlog::error("Error in follower fetch thread: {}", e.what());
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }
    
    spdlog::info("Follower fetch thread stopped");
}

void ReplicaManager::fetchFromLeader(const TopicPartition& tp, ReplicaInfo& info) {
    // In single-node mode, this should never be called since we're always the leader
    // In multi-broker mode (future), this would:
    // 1. Send Fetch request to the leader broker
    // 2. Append fetched batches to local log
    // 3. Update local high watermark
    // 4. Update fetch_offset
    
    if (!broker_) {
        spdlog::warn("Cannot fetch from leader: broker reference not set");
        return;
    }
    
    // TODO: Implement actual fetch logic when multi-broker support is added
    // For now, just log a debug message
    spdlog::trace("Would fetch from leader {} for {}-{} at offset {}",
                  info.leader, tp.topic, tp.partition, info.fetch_offset);
    
    // Placeholder: In real implementation, we would:
    // 1. Construct FetchRequest to leader broker
    // 2. Send request and wait for response
    // 3. Parse FetchResponse and extract record batches
    // 4. Append batches to local log:
    //    info.log->append(batches);
    // 5. Update high watermark:
    //    info.log->setHighWatermark(response.high_watermark);
    // 6. Update fetch_offset:
    //    info.fetch_offset = last_fetched_offset + 1;
}

void ReplicaManager::updateFollowerFetchOffset(const TopicPartition& tp,
                                               BrokerId broker_id,
                                               Offset offset) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto it = replicas_.find(tp);
    if (it == replicas_.end()) {
        spdlog::warn("Attempted to update follower offset for non-existent replica {}-{}",
                     tp.topic, tp.partition);
        return;
    }
    
    // Only track follower state if we're the leader
    if (it->second.leader != local_broker_id_) {
        return;
    }
    
    auto& follower_state = it->second.follower_states[broker_id];
    follower_state.last_fetched_offset = offset;
    follower_state.last_update_time_ms = 
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
    
    spdlog::trace("Updated follower offset for broker {} on {}-{} to {}",
                  broker_id, tp.topic, tp.partition, offset);
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
            spdlog::warn("Removing broker {} from ISR for {}-{}: no follower state",
                        follower_id, tp.topic, tp.partition);
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
        spdlog::info("ISR updated for {}-{}, new ISR size: {}",
                     tp.topic, tp.partition, new_isr.size());
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
