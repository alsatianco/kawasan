#pragma once

#include <atomic>
#include <functional>
#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <boost/asio.hpp>

#include "kawasan/broker/group_coordinator.h"
#include "kawasan/broker/metadata_controller.h"
#include "kawasan/broker/metrics/request_metrics.h"
#include "kawasan/broker/monitoring/metrics_collector.h"
#include "kawasan/broker/monitoring/monitoring_manager.h"
#include "kawasan/broker/network/tcp_server.h"
#include "kawasan/broker/offset_manager.h"
#include "kawasan/broker/replica_manager.h"
#include "kawasan/broker/request_dispatcher.h"
#include "kawasan/common/buffer.h"
#include "kawasan/common/config.h"
#include "kawasan/common/types.h"
#include "kawasan/protocol/api_versions.h"
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
        if (!enabled) return true;
        return !cert_file.empty() && !key_file.empty();
    }
    
    bool requiresClientAuth() const {
        return client_auth == "required";
    }
};

/// @brief Main Kafka broker class
class KawasanBroker {
public:
    explicit KawasanBroker(const Config& config);
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
    size_t openLogCount() const {
        return log_manager_ ? log_manager_->openLogCount() : 0;
    }

private:
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
    Buffer encodeResponse(
        const RequestDispatcher::RequestContext& context,
        const std::function<void(Buffer&)>& writer) const;
    Buffer handleApiVersions(RequestDispatcher::RequestContext& context);
    Buffer buildApiVersionsError(const RequestDispatcher::RequestContext& context,
                                 ErrorCode code, int16_t response_version) const;
    Buffer handleMetadata(RequestDispatcher::RequestContext& context);
    Buffer buildMetadataError(const RequestDispatcher::RequestContext& context,
                              ErrorCode code, int16_t response_version) const;
    Buffer handleCreateTopics(RequestDispatcher::RequestContext& context);
    Buffer buildCreateTopicsError(const RequestDispatcher::RequestContext& context,
                                  ErrorCode code, int16_t response_version) const;
    Buffer handleDeleteTopics(RequestDispatcher::RequestContext& context);
    Buffer buildDeleteTopicsError(const RequestDispatcher::RequestContext& context,
                                  ErrorCode code, int16_t response_version) const;
    RequestDispatcher::HandlerResult handleProduce(RequestDispatcher::RequestContext& context);
    Buffer buildProduceError(const RequestDispatcher::RequestContext& context,
                             ErrorCode code, int16_t response_version) const;
    RequestDispatcher::HandlerResult handleFetch(RequestDispatcher::RequestContext& context);
    Buffer buildFetchError(const RequestDispatcher::RequestContext& context,
                           ErrorCode code, int16_t response_version) const;
    RequestDispatcher::HandlerResult handleListOffsets(RequestDispatcher::RequestContext& context);
    Buffer buildListOffsetsError(const RequestDispatcher::RequestContext& context,
                                 ErrorCode code, int16_t response_version) const;
    RequestDispatcher::HandlerResult handleFindCoordinator(RequestDispatcher::RequestContext& context);
    Buffer buildFindCoordinatorError(const RequestDispatcher::RequestContext& context,
                                     ErrorCode code, int16_t response_version) const;
    Buffer handleJoinGroup(RequestDispatcher::RequestContext& context);
    Buffer buildJoinGroupError(const RequestDispatcher::RequestContext& context,
                               ErrorCode code, int16_t response_version) const;
    Buffer handleSyncGroup(RequestDispatcher::RequestContext& context);
    Buffer buildSyncGroupError(const RequestDispatcher::RequestContext& context,
                               ErrorCode code, int16_t response_version) const;
    Buffer handleHeartbeat(RequestDispatcher::RequestContext& context);
    Buffer buildHeartbeatError(const RequestDispatcher::RequestContext& context,
                               ErrorCode code, int16_t response_version) const;
    Buffer handleLeaveGroup(RequestDispatcher::RequestContext& context);
    Buffer buildLeaveGroupError(const RequestDispatcher::RequestContext& context,
                                ErrorCode code, int16_t response_version) const;
    Buffer handleOffsetCommit(RequestDispatcher::RequestContext& context);
    Buffer buildOffsetCommitError(const RequestDispatcher::RequestContext& context,
                                  ErrorCode code, int16_t response_version) const;
    Buffer handleOffsetFetch(RequestDispatcher::RequestContext& context);
    Buffer buildOffsetFetchError(const RequestDispatcher::RequestContext& context,
                                 ErrorCode code, int16_t response_version) const;
    Buffer handleDescribeGroups(RequestDispatcher::RequestContext& context);
    Buffer buildDescribeGroupsError(const RequestDispatcher::RequestContext& context,
                                    ErrorCode code, int16_t response_version) const;
    Buffer handleListGroups(RequestDispatcher::RequestContext& context);
    Buffer buildListGroupsError(const RequestDispatcher::RequestContext& context,
                                ErrorCode code, int16_t response_version) const;
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
    Buffer buildAlterConfigsError(const RequestDispatcher::RequestContext& context,
                                  ErrorCode code, int16_t response_version) const;
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
    TopicMetadata buildDefaultTopicMetadata() const;
    BrokerMetadata localBrokerMetadata() const;
    std::pair<std::optional<TopicMetadata>, ErrorCode> getTopicMetadata(
        const std::string& topic_name, bool allow_auto_create);

    Config config_;
    BrokerId broker_id_;
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
    // Phase 1.4: KIP-227 FetchSessionManager. Allocates and tracks
    // fetch session_ids; the Fetch response uses these to maintain a
    // stable handle across polls.
    std::unique_ptr<class FetchSessionManager> fetch_session_manager_;
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
    std::unordered_map<std::string, std::unique_ptr<class ScramAuthenticator>>
        sasl_sessions_;

    std::atomic<bool> running_{false};
    
    // IO context for Raft transport
    boost::asio::io_context io_context_;
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
