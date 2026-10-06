#pragma once

#include <atomic>
#include <boost/asio.hpp>
#include <condition_variable>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "kawasan/broker/delayed_operation_purgatory.h"
#include "kawasan/broker/group_coordinator.h"
#include "kawasan/broker/group_state_manager.h"
#include "kawasan/broker/metadata_controller.h"
#include "kawasan/broker/metrics/request_metrics.h"
#include "kawasan/broker/monitoring/metrics_collector.h"
#include "kawasan/broker/monitoring/monitoring_manager.h"
#include "kawasan/broker/network/tcp_server.h"
#include "kawasan/broker/offset_manager.h"
#include "kawasan/broker/replica_manager.h"
#include "kawasan/broker/request_dispatcher.h"
#include "kawasan/broker/transaction_coordinator.h"
#include "kawasan/common/buffer.h"
#include "kawasan/common/config.h"
#include "kawasan/common/types.h"
#include "kawasan/protocol/api_versions.h"
#include "kawasan/protocol/find_coordinator_request.h"
#include "kawasan/raft/raft_node.h"
#include "kawasan/storage/log_manager.h"

namespace kawasan::broker {

/// @brief TLS configuration for Kafka protocol
struct TlsConfig {
    bool enabled = false;
    std::string cert_file;
    std::string key_file;
    std::string key_password;
    std::string ca_file;
    std::string client_auth = "none";  // none, requested, required

    bool isValid() const {
        if (!enabled)
            return true;
        return !cert_file.empty() && !key_file.empty();
    }

    bool requiresClientAuth() const { return client_auth == "required"; }
};

/// @brief Main Kafka broker class
class KawasanBroker {
public:
    explicit KawasanBroker(const Config& config);
    // Internal M10 integration seam. Coordinator requests remain refused until
    // handler migration lands; no configuration enables this mode.
    struct CoordinatorAcquisitionOnly {
        CoordinatorFormat format;
    };
    KawasanBroker(const Config& config, CoordinatorAcquisitionOnly staging);
    // Internal persistence checkpoint: RF=min(3, cluster size), committed
    // mutations, but coordinator wire handlers/background jobs remain gated.
    struct CoordinatorPersistenceOnly {
        CoordinatorFormat format;
    };
    KawasanBroker(const Config& config, CoordinatorPersistenceOnly staging);
    using GroupMutation =
        std::function<ErrorCode(const std::vector<GroupRecord>&, std::vector<GroupRecord>&)>;
    // Proposal callbacks run under the mutation fence and must not reenter a
    // coordinator mutation or stop(). Publication preserves live clocks and
    // counters; acquisition rebuilds them. Shutdown drains admitted proposals.
    ErrorCode mutateCoordinatorGroup(const std::string& group_id, const GroupMutation& mutation,
                                     std::chrono::milliseconds timeout = std::chrono::seconds(5));
    using GroupStateMutation = std::function<void(GroupCoordinator::GroupProposal&)>;
    // Returns persistence/admission status; capture the protocol result in the
    // callback and return it only after success. Some protocol errors themselves
    // change state (e.g. SyncGroup rejecting obsolete assignments).
    ErrorCode mutateCoordinatorGroupState(
        const std::string& group_id, const GroupStateMutation& mutation,
        std::chrono::milliseconds timeout = std::chrono::seconds(5));
    ErrorCode deleteCoordinatorGroup(const std::string& group_id,
                                     std::chrono::milliseconds timeout = std::chrono::seconds(5));
    using TransactionMutation =
        std::function<ErrorCode(std::optional<TransactionCoordinator::TxnSnapshot>&)>;
    // null expected admits only a new ID. Existing mutations (including timeout
    // sweep/cleanup) must compare the full observed snapshot under this fence.
    ErrorCode mutateCoordinatorTransaction(
        const std::string& transactional_id,
        const std::optional<TransactionCoordinator::TxnSnapshot>& expected,
        const TransactionMutation& mutation,
        std::chrono::milliseconds timeout = std::chrono::seconds(5));
    ErrorCode coordinatorLoadStatus(const std::string& key, protocol::CoordinatorType type) const;
    void acquireCoordinatorPartitions();
    ~KawasanBroker();

    // Non-copyable/movable
    KawasanBroker(const KawasanBroker&) = delete;
    KawasanBroker& operator=(const KawasanBroker&) = delete;
    KawasanBroker(KawasanBroker&&) = delete;
    KawasanBroker& operator=(KawasanBroker&&) = delete;

    /// @brief Starts the broker
    void start();

    /// @brief Stops the broker
    void stop();

    /// @brief Returns whether the broker is running
    bool isRunning() const { return running_; }

    /// @brief Returns the broker ID
    BrokerId brokerId() const { return broker_id_; }

    /// @brief Returns the log manager
    storage::LogManager* logManager() { return log_manager_.get(); }

    /// @brief Returns the Raft node
    raft::RaftNode* raftNode() { return raft_node_.get(); }

    /// @brief Returns the replica manager (leader/ISR/high-watermark state).
    ReplicaManager* replicaManager() { return replica_manager_.get(); }
    MetadataController* metadataController() { return metadata_controller_.get(); }

    /// @brief M5: resolve a peer broker's Kafka listener address (host, port)
    /// from cluster metadata. Used by the replica fetcher to reach a partition
    /// leader. Returns nullopt if the broker id is unknown.
    std::optional<std::pair<std::string, int32_t>> peerEndpoint(BrokerId broker_id) const;

    /// @brief Serialize clustered produce, replica ingestion and role transitions
    /// for one partition. Returns an unlocked guard in single-node mode.
    std::unique_lock<std::mutex> lockPartitionWrites(const TopicPartition& tp);

    /// @brief M5: reconcile ReplicaManager against the Raft-committed metadata —
    /// for every partition this broker replicates, register it as leader (ISR =
    /// assigned replicas) or as a follower (leader = the assigned leader) so the
    /// fetcher thread replicates it. Called periodically by the fetcher thread
    /// and once at startup. A no-op-equivalent for single-node RF=1 (registers
    /// leader=self / ISR={self}).
    void reconcileReplicas();

    /// @brief M6: for every partition this broker LEADS, propose an ISR
    /// shrink/expand (from tracked follower lag) and commit it through the
    /// controller — directly via `updatePartitionISR` when this broker is the
    /// controller, otherwise via an AlterPartition RPC to the controller. Called
    /// periodically by the replica-fetcher thread (multi-broker only).
    void maintainLeaderIsr();

    /// @brief M8-C: controller-only failover sweep. Brokers whose Raft ack age
    /// exceeds `broker.liveness.timeout.ms` are dead; partitions they lead get a
    /// new leader from the live ISR (or go offline, leader -1, unless
    /// `unclean.leader.election.enable`), and they are dropped from ISRs. See
    /// computeLeadershipChanges. Called by the replica-fetcher thread
    /// (multi-broker only); a no-op unless this broker is the Raft leader.
    void maintainPartitionLeaders();

    /// @brief M8-E1: whether this broker may serve produce/fetch from its
    /// metadata view. Always true single-node. Multi-broker: the Raft view must
    /// be provably current (RaftNode::hasCurrentMetadata) within a lease of half
    /// `broker.liveness.timeout.ms` — so a restarted or cut-off broker answers
    /// NOT_LEADER_FOR_PARTITION instead of acting on stale leadership.
    bool dataPlaneCurrent() const;

    /// @brief Whether the committed metadata names `leader` at `leader_epoch` as
    /// the leader of `tp` (the replica fetcher re-checks before ingesting).
    bool isPartitionLeadership(const TopicPartition& tp, BrokerId leader,
                               int32_t leader_epoch) const;

    /// @brief M8-E1: sets the /ready probe from dataPlaneCurrent(). Called by
    /// the replica-fetcher thread (multi-broker only).
    void refreshReadiness();

    /// @brief Returns the port the broker is bound to.
    int32_t port() const {
        if (tcp_server_) {
            return tcp_server_->listeningPort();
        }
        return port_;
    }

    /// @brief Number of currently-open partition logs (a proxy for the storage
    /// file-descriptor footprint, since each log is backed by RocksDB). Exposed
    /// for the FD-budget test and operational observability.
    size_t openLogCount() const { return log_manager_ ? log_manager_->openLogCount() : 0; }

private:
    friend struct CoordinatorAcquisitionProbe;
    friend struct CoordinatorPersistenceProbe;
    KawasanBroker(const Config& config, std::optional<CoordinatorFormat> staging,
                  bool persistence = false);
    std::optional<CoordinatorFormat> staged_coordinator_format_;
    bool staged_coordinator_persistence_ = false;
    mutable std::mutex coordinator_acquisition_mutex_;
    std::map<TopicPartition, int32_t> acquired_coordinator_epochs_;
    // An ambiguous append blocks reacquisition in the same epoch until its full
    // span is committed. A new epoch uses ReplicaManager's safe-prefix barrier.
    std::map<TopicPartition, std::pair<int32_t, Offset>> coordinator_commit_barriers_;
    // An append/fsync exception may have written bytes without advancing LEO.
    // Reopening the source is required; never retry over an unknown WAL result.
    std::set<TopicPartition> failed_coordinator_appends_;
    // A shared lease covers proposal/source reads through publication. Stop
    // seals admission, cancels ISR waits and drains leases before closing logs.
    std::shared_mutex coordinator_mutation_lifecycle_mutex_;
    std::atomic<bool> coordinator_mutations_stopping_{false};
    std::mutex coordinator_mutation_map_mutex_;
    std::map<TopicPartition, std::shared_ptr<std::mutex>> coordinator_mutation_mutexes_;
    std::shared_ptr<std::mutex> coordinatorMutationMutex(const TopicPartition& tp);
    ErrorCode mutateCoordinatorGroupImpl(
        const std::string& group_id, const GroupMutation& mutation,
        std::chrono::milliseconds timeout,
        const std::unique_ptr<GroupCoordinator::GroupProposal>* proposal = nullptr);
    ErrorCode commitCoordinatorRecords(const TopicPartition& tp, int32_t epoch,
                                       const std::vector<Record>& records,
                                       const std::function<void()>& publish,
                                       std::chrono::milliseconds timeout);
    void initializeRaft();
    void initializeMetadata();
    void startServices();
    void stopServices();
    /// @brief Idempotently ensures the internal topics (__consumer_offsets,
    /// __transaction_state) exist. If they already exist (created by whichever
    /// broker is the controller and replicated via Raft) it records the offsets
    /// partition count and marks bootstrap complete. Otherwise, only the active
    /// controller (Raft leader) creates them; non-leaders return quietly so the
    /// bootstrap loop can retry. This decouples internal-topic creation from the
    /// startup race where, in a multi-broker cluster, no controller exists yet.
    void ensureInternalTopics();
    std::optional<PartitionMetadata> currentPartitionMetadata(const TopicPartition& tp) const;

    /// @brief Rebuilds idempotent-producer state for one partition by replaying
    /// its persisted record-batch headers into producer_state_manager_ at
    /// startup (B3 — survives restart without a separate snapshot file).
    void replayProducerStateFromLog(const std::string& topic, PartitionId partition,
                                    Offset start_offset = 0);
    /// @brief M3: write a producer-state snapshot for every open partition
    /// (called periodically by the snapshot loop and once on graceful stop),
    /// so restart only replays the log tail after the snapshot offset.
    void writeAllProducerSnapshots();
    /// @brief M3: background loop that periodically calls writeAllProducerSnapshots().
    void producerSnapshotLoop();
    /// @brief M1: rebuilds TransactionCoordinator + IsolationTracker state at
    /// startup by replaying persisted snapshots from __transaction_state, and
    /// re-drives any transaction left in a Prepare* state to completion.
    void replayTransactionStateFromLog();
    /// @brief M1: persist the current snapshot of `transactional_id` to
    /// __transaction_state (no-op if the state manager isn't ready).
    void persistTxnState(const std::string& transactional_id);
    /// @brief M1: shared completion path for EndTxn and crash-recovery replay.
    /// Emits control markers on participating partitions (idempotently when
    /// is_replay=true), applies/discards staged offsets, updates the isolation
    /// tracker, transitions the coordinator to Complete*, and persists it.
    void finishTxnCompletion(
        const std::string& transactional_id, int64_t producer_id, int16_t producer_epoch,
        bool committed, const std::vector<TransactionCoordinator::TxnPartition>& participating,
        const std::vector<TransactionCoordinator::PendingOffset>& pending_offsets, bool is_replay);
    /// @brief M1: true if the partition log already holds a control batch for
    /// `producer_id` at/after `from_offset` — the idempotency guard that stops
    /// crash-recovery re-drive from writing a duplicate marker.
    bool logHasControlBatchForProducer(storage::Log* log, int64_t producer_id, Offset from_offset);
    /// @brief Require an initialized transaction's exact producer ID and epoch.
    ErrorCode validateTxnProducer(const std::string& transactional_id, int64_t producer_id,
                                  int16_t producer_epoch);
    /// @brief M2: background loop that auto-aborts Ongoing transactions past
    /// their transaction.timeout.ms so a hung producer never blocks
    /// read_committed consumers forever.
    void transactionSweepLoop();
    /// @brief Background loop that calls ensureInternalTopics() until the
    /// internal topics exist, then exits. Handles the multi-broker case where
    /// leadership is established after startup.
    void controllerBootstrapLoop();
    /// @brief When deployment.mode=production (or KAWASAN_DEPLOYMENT_MODE env),
    /// throws std::runtime_error on settings the broker cannot honor in
    /// production (TLS, RF>1, minISR>1) rather than silently degrading them.
    void validateProductionConfig() const;

    /// @brief Authorization check for a request. Returns true (allowed) when the
    /// authorizer is disabled (default). When enabled, resolves the principal
    /// from the connection (or "User:ANONYMOUS"), honors super.users, and
    /// consults the ACL store. operation/resource_type use Kafka's ACL enum
    /// values (e.g. READ=3, WRITE=4, CREATE=5, DELETE=6; TOPIC=2, GROUP=3).
    bool authorize(const RequestDispatcher::RequestContext& context, int8_t operation,
                   int8_t resource_type, const std::string& resource_name) const;
    void registerProtocolHandlers();
    Buffer encodeResponse(const RequestDispatcher::RequestContext& context,
                          const std::function<void(Buffer&)>& writer) const;
    Buffer handleApiVersions(RequestDispatcher::RequestContext& context);
    Buffer buildApiVersionsError(const RequestDispatcher::RequestContext& context, ErrorCode code,
                                 int16_t response_version) const;
    Buffer handleMetadata(RequestDispatcher::RequestContext& context);
    Buffer buildMetadataError(const RequestDispatcher::RequestContext& context, ErrorCode code,
                              int16_t response_version) const;
    Buffer handleCreateTopics(RequestDispatcher::RequestContext& context);
    Buffer buildCreateTopicsError(const RequestDispatcher::RequestContext& context, ErrorCode code,
                                  int16_t response_version) const;
    Buffer handleDeleteTopics(RequestDispatcher::RequestContext& context);
    Buffer buildDeleteTopicsError(const RequestDispatcher::RequestContext& context, ErrorCode code,
                                  int16_t response_version) const;
    RequestDispatcher::HandlerResult handleProduce(RequestDispatcher::RequestContext& context);
    Buffer buildProduceError(const RequestDispatcher::RequestContext& context, ErrorCode code,
                             int16_t response_version) const;
    RequestDispatcher::HandlerResult handleFetch(RequestDispatcher::RequestContext& context);
    Buffer buildFetchError(const RequestDispatcher::RequestContext& context, ErrorCode code,
                           int16_t response_version) const;
    RequestDispatcher::HandlerResult handleListOffsets(RequestDispatcher::RequestContext& context);
    Buffer buildListOffsetsError(const RequestDispatcher::RequestContext& context, ErrorCode code,
                                 int16_t response_version) const;
    bool isCoordinatorFor(const std::string& key, protocol::CoordinatorType type) const;
    protocol::FindCoordinatorResponse::Coordinator resolveCoordinator(
        const std::string& key, protocol::CoordinatorType type) const;

    RequestDispatcher::HandlerResult handleFindCoordinator(
        RequestDispatcher::RequestContext& context);
    Buffer buildFindCoordinatorError(const RequestDispatcher::RequestContext& context,
                                     ErrorCode code, int16_t response_version) const;
    Buffer handleJoinGroup(RequestDispatcher::RequestContext& context);
    Buffer buildJoinGroupError(const RequestDispatcher::RequestContext& context, ErrorCode code,
                               int16_t response_version) const;
    Buffer handleSyncGroup(RequestDispatcher::RequestContext& context);
    Buffer buildSyncGroupError(const RequestDispatcher::RequestContext& context, ErrorCode code,
                               int16_t response_version) const;
    Buffer handleHeartbeat(RequestDispatcher::RequestContext& context);
    Buffer buildHeartbeatError(const RequestDispatcher::RequestContext& context, ErrorCode code,
                               int16_t response_version) const;
    Buffer handleLeaveGroup(RequestDispatcher::RequestContext& context);
    Buffer buildLeaveGroupError(const RequestDispatcher::RequestContext& context, ErrorCode code,
                                int16_t response_version) const;
    Buffer handleOffsetCommit(RequestDispatcher::RequestContext& context);
    Buffer buildOffsetCommitError(const RequestDispatcher::RequestContext& context, ErrorCode code,
                                  int16_t response_version) const;
    Buffer handleOffsetFetch(RequestDispatcher::RequestContext& context);
    Buffer buildOffsetFetchError(const RequestDispatcher::RequestContext& context, ErrorCode code,
                                 int16_t response_version) const;
    Buffer handleDescribeGroups(RequestDispatcher::RequestContext& context);
    Buffer buildDescribeGroupsError(const RequestDispatcher::RequestContext& context,
                                    ErrorCode code, int16_t response_version) const;
    Buffer handleListGroups(RequestDispatcher::RequestContext& context);
    Buffer buildListGroupsError(const RequestDispatcher::RequestContext& context, ErrorCode code,
                                int16_t response_version) const;
    Buffer handleDescribeConfigs(RequestDispatcher::RequestContext& context);
    Buffer buildDescribeConfigsError(const RequestDispatcher::RequestContext& context,
                                     ErrorCode code, int16_t response_version) const;
    Buffer handleDescribeCluster(RequestDispatcher::RequestContext& context);
    Buffer buildDescribeClusterError(const RequestDispatcher::RequestContext& context,
                                     ErrorCode code, int16_t response_version) const;
    // Phase 1.18: minimal InitProducerId — allocates from a monotonic durable
    // counter and returns epoch=0. Full ProducerStateManager + transactional
    // semantics arrive in Phase 2.1 / Phase 3.3.
    Buffer handleInitProducerId(RequestDispatcher::RequestContext& context);
    Buffer buildInitProducerIdError(const RequestDispatcher::RequestContext& context,
                                    ErrorCode code, int16_t response_version) const;
    int64_t allocateNextProducerId();
    // Phase 1.19: OffsetForLeaderEpoch (API 23) — single-broker stub returning
    // (epoch=0, end_offset=log_end_offset) for known partitions.
    Buffer handleOffsetForLeaderEpoch(RequestDispatcher::RequestContext& context);
    Buffer buildOffsetForLeaderEpochError(const RequestDispatcher::RequestContext& context,
                                          ErrorCode code, int16_t response_version) const;
    // Phase 4.1a: AlterConfigs (replace-all topic/broker config).
    Buffer handleAlterConfigs(RequestDispatcher::RequestContext& context);
    Buffer buildAlterConfigsError(const RequestDispatcher::RequestContext& context, ErrorCode code,
                                  int16_t response_version) const;
    // Phase 4.1b: IncrementalAlterConfigs (per-key SET/DELETE/APPEND/SUBTRACT).
    Buffer handleIncrementalAlterConfigs(RequestDispatcher::RequestContext& context);
    Buffer buildIncrementalAlterConfigsError(const RequestDispatcher::RequestContext& context,
                                             ErrorCode code, int16_t response_version) const;
    // Phase 4.1c–m: admin batch.
    Buffer handleDescribeLogDirs(RequestDispatcher::RequestContext& context);
    Buffer handleAlterReplicaLogDirs(RequestDispatcher::RequestContext& context);
    Buffer handleElectLeaders(RequestDispatcher::RequestContext& context);
    Buffer handleDeleteRecords(RequestDispatcher::RequestContext& context);
    Buffer handleDeleteGroups(RequestDispatcher::RequestContext& context);
    Buffer handleOffsetDelete(RequestDispatcher::RequestContext& context);
    Buffer handleCreatePartitions(RequestDispatcher::RequestContext& context);
    // Phase 4.1 stubs that will fill in once their backends land.
    Buffer handleDescribeProducers(RequestDispatcher::RequestContext& context);
    Buffer handleListTransactions(RequestDispatcher::RequestContext& context);
    Buffer handleDescribeTransactions(RequestDispatcher::RequestContext& context);
    Buffer handleAlterPartition(RequestDispatcher::RequestContext& context);
    Buffer handleDescribeAcls(RequestDispatcher::RequestContext& context);
    Buffer handleCreateAcls(RequestDispatcher::RequestContext& context);
    Buffer handleDeleteAcls(RequestDispatcher::RequestContext& context);
    // Phase 4.2a: SASL PLAIN.
    Buffer handleSaslHandshake(RequestDispatcher::RequestContext& context);
    Buffer handleSaslAuthenticate(RequestDispatcher::RequestContext& context);
    // Phase 3.3 scaffolding: transactional API handlers. Bodies decode
    // the request enough to extract producer/transactional identity and
    // return NONE (no real transaction state is maintained yet — the
    // produce path doesn't honor LSO or write control markers).
    Buffer handleAddPartitionsToTxn(RequestDispatcher::RequestContext& context);
    Buffer handleAddOffsetsToTxn(RequestDispatcher::RequestContext& context);
    Buffer handleEndTxn(RequestDispatcher::RequestContext& context);
    Buffer handleTxnOffsetCommit(RequestDispatcher::RequestContext& context);
    // Shared trivial error builder for all the misc handlers above.
    Buffer buildEmptyErrorResponse(const RequestDispatcher::RequestContext& context) const;
    std::vector<BrokerMetadata> buildBrokerMetadata() const;
    /// @brief CM-5: the id of the active controller — the current Raft leader —
    /// for Metadata/DescribeCluster responses. Falls back to this broker's own id
    /// when no leader is known yet (single-node becomes leader immediately, so it
    /// reports itself; a multi-broker follower reports the real controller).
    BrokerId controllerId() const;
    TopicMetadata buildDefaultTopicMetadata() const;
    BrokerMetadata localBrokerMetadata() const;
    std::pair<std::optional<TopicMetadata>, ErrorCode> getTopicMetadata(
        const std::string& topic_name, bool allow_auto_create);

    Config config_;
    // Must outlive Raft sockets: members are destroyed in reverse order.
    boost::asio::io_context io_context_;
    BrokerId broker_id_;
    // M4: min.insync.replicas — an acks=all produce is rejected with
    // NOT_ENOUGH_REPLICAS when the partition's ISR has fewer members than this.
    // Default 1 keeps single-node produce working unchanged.
    int32_t min_insync_replicas_ = 1;
    // M6: replica.lag.time.max.ms — a follower whose last fetch is older than
    // this is dropped from the ISR (shrink), unblocking acks=all.
    int64_t replica_lag_time_max_ms_ = 30000;
    // M8-C: a peer whose last Raft ack is older than this is considered dead by
    // the controller's failover sweep.
    int64_t broker_liveness_timeout_ms_ = 9000;
    int64_t metadataLeaseMs() const;
    // M8-C: allow electing an out-of-sync replica when no ISR member is alive
    // (may lose acknowledged records). Off by default.
    bool unclean_leader_election_enabled_ = false;
    // M8-C: last dead set seen by the sweep (log transitions only).
    std::set<BrokerId> dead_brokers_;
    std::string host_;
    std::string advertised_host_;
    int32_t port_;
    std::string cluster_id_;
    std::string log_dir_;
    std::string metadata_dir_;
    storage::LogConfig log_config_;

    std::unique_ptr<storage::LogManager> log_manager_;
    // Declaration order matters for destruction: metadata_controller_ is the
    // target of the Raft commit callback invoked by raft_node_'s apply thread,
    // so raft_node_ MUST be destroyed first (its destructor joins the apply
    // thread) while metadata_controller_ is still alive. Members destruct in
    // reverse declaration order, hence metadata_controller_ is declared BEFORE
    // raft_node_.
    std::unique_ptr<MetadataController> metadata_controller_;
    std::unique_ptr<raft::RaftNode> raft_node_;
    std::unique_ptr<ReplicaManager> replica_manager_;
    std::shared_ptr<OffsetManager> offset_manager_;
    std::shared_ptr<GroupCoordinator> group_coordinator_;
    // Phase 2.1: idempotent producer dedup state.
    std::unique_ptr<class ProducerStateManager> producer_state_manager_;
    // Phase 4.2c: in-memory ACL store backing DescribeAcls/CreateAcls/DeleteAcls.
    std::unique_ptr<class AclStore> acl_store_;
    std::unique_ptr<class QuotaManager> quota_manager_;
    // Phase 4.1k/4.1l: minimal in-memory transactional_id registry. Full
    // TransactionCoordinator (LSO, control records, EndTxn, …) arrives
    // with Phase 3.3 — this scaffolding exists so DescribeTransactions
    // and ListTransactions can return real state for txn IDs the broker
    // has seen via InitProducerId.
    std::unique_ptr<class TransactionCoordinator> transaction_coordinator_;
    // M1: durable persistence of transaction state to __transaction_state and
    // replay on startup. Constructed once the internal-topic partition count
    // is known (see ensureInternalTopics).
    std::unique_ptr<class TransactionStateManager> transaction_state_manager_;
    int32_t txn_state_num_partitions_ = 16;
    // Phase 1.4: KIP-227 FetchSessionManager. Allocates and tracks
    // fetch session_ids; the Fetch response uses these to maintain a
    // stable handle across polls.
    std::unique_ptr<class FetchSessionManager> fetch_session_manager_;
    // P4: parks long-poll fetches off the network IO threads; woken by log
    // changes (see LogManager::setChangeListener). Shared so the log listener
    // can hold it safely for the logs' whole lifetime.
    std::shared_ptr<DelayedOperationPurgatory> delayed_fetch_purgatory_;
    // P4: parks acks=all produces waiting for the ISR off the IO threads; woken
    // by high-watermark changes and by leader/ISR reconciliation.
    std::shared_ptr<DelayedOperationPurgatory> delayed_produce_purgatory_;
    // Phase EX-10: read_committed isolation tracker. Holds per-partition
    // in-flight transactional state so Fetch can report LSO + aborted
    // transactions correctly, and so read_committed consumers filter
    // out aborted records.
    std::unique_ptr<class IsolationTracker> isolation_tracker_;
    std::unique_ptr<network::TcpServer> tcp_server_;
    std::shared_ptr<metrics::RequestMetrics> request_metrics_;
    std::shared_ptr<RequestDispatcher> request_dispatcher_;
    std::vector<protocol::ApiVersionInfo> supported_api_versions_;

    // Monitoring and metrics
    std::shared_ptr<monitoring::MetricsCollector> metrics_collector_;
    std::unique_ptr<monitoring::MonitoringManager> monitoring_manager_;

    bool auto_create_topics_enabled_ = true;
    int32_t default_num_partitions_ = 1;
    int16_t default_replication_factor_ = 1;

    // True when deployment.mode=production (or KAWASAN_DEPLOYMENT_MODE=production).
    // Tightens runtime behavior (e.g. refuses SASL/PLAIN "accept-any" when no
    // credentials are configured) in addition to the startup validator.
    bool production_mode_ = false;

    // Authorization (ACL enforcement). Off by default so existing deployments
    // with no ACLs are unaffected; set authorizer.enabled=true to enforce the
    // bindings in acl_store_. When enabled, requests with no matching ACL are
    // denied unless allow.everyone.if.no.acl.found=true. Principals listed in
    // super.users (Kafka format, ';'-separated, e.g. "User:admin;User:svc")
    // always pass.
    bool authorizer_enabled_ = false;
    bool allow_everyone_if_no_acl_ = false;
    std::unordered_set<std::string> super_users_;

    // Cluster membership derived from raft.peers at startup (self + peers).
    // Empty in single-node mode. Seeded into the metadata store so replica
    // assignment can spread partitions across brokers and Metadata responses
    // advertise the whole cluster.
    std::vector<BrokerMetadata> cluster_brokers_;

    // Number of brokers in raft.peers (1 in single-node mode). Replication
    // factor is capped at this — a topic can't have more replicas than brokers.
    int cluster_size_ = 1;

    // Controller-bootstrap state: a background thread ensures the internal topics
    // exist once a controller is elected, fixing the multi-broker startup race.
    std::atomic<bool> internal_topics_ready_{false};
    std::atomic<bool> bootstrap_stop_{false};
    std::thread bootstrap_thread_;
    std::mutex bootstrap_mutex_;
    std::condition_variable bootstrap_cv_;

    // M2: transaction-timeout sweep thread — auto-aborts expired Ongoing txns.
    std::atomic<bool> txn_sweep_stop_{false};
    std::thread txn_sweep_thread_;
    std::mutex txn_sweep_mutex_;
    std::condition_variable txn_sweep_cv_;
    int64_t txn_sweep_interval_ms_ = 10000;

    // M3: producer-state snapshot writer thread.
    std::atomic<bool> producer_snapshot_stop_{false};
    std::thread producer_snapshot_thread_;
    std::mutex producer_snapshot_mutex_;
    std::mutex partition_write_mutex_map_mutex_;
    std::map<TopicPartition, std::unique_ptr<std::mutex>> partition_write_mutexes_;
    std::condition_variable producer_snapshot_cv_;
    int64_t producer_snapshot_interval_ms_ = 60000;

    // Actual partition count of the `__consumer_offsets` topic, captured from
    // metadata after the topic is created/confirmed at startup. Used to route
    // offset-commit mirror records (group_id hash % count). Reading the real
    // count from metadata (rather than a hardcoded 50) keeps routing correct
    // when offsets.topic.num.partitions is tuned or the topic pre-exists with a
    // different count.
    int32_t offsets_topic_num_partitions_ = 50;

    // Phase 1.18: durable monotonic producer-id counter. Reads from
    // {metadata.dir}/producer_id.counter on start; writes atomically on
    // every allocation. The cap follows Kafka's allocation-block pattern
    // but with block size 1 for simplicity (Phase 2 will batch).
    std::atomic<int64_t> next_producer_id_{1};
    mutable std::mutex producer_id_mutex_;

    // TLS configuration
    TlsConfig tls_config_;
    TlsConfig raft_tls_config_;

    // Phase 4.2a: SASL/PLAIN credentials map. Populated from
    // `sasl.plain.users` config (JSON object: user → password) or from the
    // file at `sasl.plain.credentials.file` (one `user:password` per line).
    // If empty, the SASL handler accepts any non-empty credentials (dev
    // mode). If non-empty, only listed users are accepted.
    std::unordered_map<std::string, std::string> sasl_plain_creds_;

    // Phase 4.2b: SASL/SCRAM-SHA-256 credentials. Populated from
    // `sasl.scram.users` config (JSON object: user → password — broker
    // generates salt + stored_key + server_key at startup) or from a file
    // at `sasl.scram.credentials.file` (one `user:password` per line).
    // Per-connection SCRAM state is held by the protocol session layer,
    // not stored here.
    std::unordered_map<std::string, class ScramCredentials> sasl_scram_creds_;

    // Per-connection SCRAM state, keyed by peer_identity. Phase 4.2b uses
    // peer_identity (the TCP "ip:port" string) as the session key; a
    // future polish should add a proper SaslSession abstraction so the
    // state lives next to the underlying socket instead of in a global
    // map.
    mutable std::mutex sasl_session_mutex_;
    std::unordered_map<std::string, std::unique_ptr<class ScramAuthenticator>> sasl_sessions_;

    std::atomic<bool> running_{false};

    // 0A.13: Work guard keeps io_context_.run() alive even when there is no
    // outstanding work (e.g. single-node mode where Raft transport has no
    // sessions). Reset in stop() before io_context_.stop() to let run() return.
    std::optional<boost::asio::executor_work_guard<boost::asio::io_context::executor_type>>
        io_work_guard_;
    std::unique_ptr<std::thread> io_thread_;

    // Consumer lag computation
    std::atomic<bool> lag_computation_running_{false};
    std::thread lag_computation_thread_;
};

}  // namespace kawasan::broker
