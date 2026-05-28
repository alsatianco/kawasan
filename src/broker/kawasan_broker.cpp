#include "kawasan/broker/kawasan_broker.h"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <limits>
#include <optional>
#include <thread>
#include <utility>
#include <zlib.h>

#include "kawasan/broker/replica_manager.h"
#include "kawasan/common/error.h"
#include "kawasan/common/logger.h"
#include "kawasan/protocol/api_versions.h"
#include "kawasan/protocol/create_topics_request.h"
#include "kawasan/protocol/delete_topics_request.h"
#include "kawasan/protocol/describe_cluster_request.h"
#include "kawasan/broker/acl_store.h"
#include "kawasan/broker/fetch_session_manager.h"
#include "kawasan/broker/isolation_tracker.h"
#include "kawasan/broker/producer_state_manager.h"
#include "kawasan/broker/scram_auth.h"
#include "kawasan/broker/transaction_coordinator.h"
#include "kawasan/protocol/txn_request.h"
#include "kawasan/protocol/describe_configs_request.h"
#include "kawasan/protocol/describe_groups_request.h"
#include "kawasan/protocol/fetch_request.h"
#include "kawasan/protocol/find_coordinator_request.h"
#include "kawasan/protocol/heartbeat_request.h"
#include "kawasan/protocol/join_group_request.h"
#include "kawasan/protocol/leave_group_request.h"
#include "kawasan/protocol/list_groups_request.h"
#include "kawasan/protocol/list_offsets_request.h"
#include "kawasan/protocol/metadata_request.h"
#include "kawasan/protocol/offset_commit_request.h"
#include "kawasan/protocol/offset_fetch_request.h"
#include "kawasan/protocol/produce_request.h"
#include "kawasan/protocol/sync_group_request.h"
#include "kawasan/protocol/init_producer_id_request.h"
#include "kawasan/protocol/offset_for_leader_epoch_request.h"
#include "kawasan/protocol/alter_configs_request.h"
#include "kawasan/protocol/incremental_alter_configs_request.h"
#include "kawasan/protocol/admin_misc_requests.h"
#include "kawasan/protocol/admin_stubs.h"
#include "kawasan/protocol/sasl_request.h"
#include "kawasan/storage/record_batch.h"

#include <algorithm>
#include <sstream>

#include <filesystem>
#include <fstream>
#include <map>

#include <nlohmann/json.hpp>

namespace kawasan::broker {

namespace {
constexpr int16_t kMetadataMaxVersion = 12;  // Phase 1.2
constexpr int16_t kProduceMaxVersion = 9;  // Phase 1.3
constexpr int16_t kFetchMaxVersion = 12;  // Phase 1.4 (v13 wire-format ready; advertise stays at v12 — librdkafka still rejects)
constexpr int16_t kListOffsetsMaxVersion = 7;  // Phase 1.5
constexpr int16_t kFindCoordinatorMaxVersion = 4;  // Phase 1.6
constexpr int16_t kJoinGroupMaxVersion = 9;  // Phase 1.7
constexpr int16_t kSyncGroupMaxVersion = 5;  // Phase 1.8
constexpr int16_t kHeartbeatMaxVersion = 4;  // Phase 1.9
constexpr int16_t kLeaveGroupMaxVersion = 5;  // Phase 1.10
constexpr int16_t kOffsetCommitMaxVersion = 8;  // Phase 1.11
constexpr int16_t kOffsetFetchMaxVersion = 8;  // Phase 1.12 (v8 multi-group supported)
constexpr int16_t kDescribeGroupsMaxVersion = 5;  // Phase 1.13
constexpr int16_t kListGroupsMaxVersion = 4;  // Phase 1.14
}  // namespace

KawasanBroker::KawasanBroker(const Config& config) : config_(config) {
    broker_id_ = config_.get<BrokerId>("broker.id");
    host_ = config_.get<std::string>("host", "localhost");
    advertised_host_ = config_.get<std::string>("advertised.host", host_);
    port_ = config_.get<int32_t>("port", 9092);
    cluster_id_ = config_.get<std::string>("cluster.id", "kawasan-cluster");

    auto_create_topics_enabled_ =
        config_.get<bool>("auto.create.topics.enable", true);

    const int32_t configured_partitions =
        config_.get<int32_t>("num.partitions", 1);
    if (configured_partitions <= 0) {
        Logger::warn(
            "Configured num.partitions={} is invalid; using 1",
            configured_partitions);
        default_num_partitions_ = 1;
    } else {
        default_num_partitions_ = configured_partitions;
    }

    int16_t configured_replication_factor =
        config_.get<int16_t>("default.replication.factor", 1);
    if (configured_replication_factor < 1) {
        Logger::warn(
            "Configured default.replication.factor={} is invalid; using 1",
            configured_replication_factor);
        configured_replication_factor = 1;
    } else if (configured_replication_factor > 1) {
        Logger::warn(
            "Replication factor {} is not supported; forcing to 1",
            configured_replication_factor);
        configured_replication_factor = 1;
    }
    default_replication_factor_ = configured_replication_factor;

    log_dir_ = config_.get<std::string>("log.dirs", "/tmp/kawasan-logs");
    metadata_dir_ = config_.get<std::string>("metadata.dir", log_dir_ + "/meta");

    storage::LogConfig configured_log;
    const auto default_segment_bytes =
        static_cast<int64_t>(configured_log.segment_size);
    const int64_t configured_segment_bytes =
        config_.get<int64_t>("log.segment.bytes", default_segment_bytes);
    if (configured_segment_bytes > 0) {
        configured_log.segment_size = static_cast<size_t>(configured_segment_bytes);
    }

    constexpr int64_t kMillisPerSecond = 1000;
    constexpr int64_t kSecondsPerHour = 60 * 60;
    int64_t roll_ms = config_.get<int64_t>("log.roll.ms", configured_log.segment_ms);
    if (roll_ms <= 0) {
        const int64_t roll_hours = config_.get<int64_t>("log.roll.hours", -1);
        if (roll_hours > 0) {
            roll_ms = roll_hours * kSecondsPerHour * kMillisPerSecond;
        }
    }
    configured_log.segment_ms = roll_ms;

    int64_t retention_ms =
        config_.get<int64_t>("log.retention.ms", configured_log.retention_ms);
    if (retention_ms <= 0) {
        const int64_t retention_hours = config_.get<int64_t>("log.retention.hours", -1);
        if (retention_hours > 0) {
            retention_ms = retention_hours * kSecondsPerHour * kMillisPerSecond;
        }
    }
    if (retention_ms > 0) {
        configured_log.retention_ms = retention_ms;
    }

    configured_log.retention_bytes =
        config_.get<int64_t>("log.retention.bytes", configured_log.retention_bytes);

    log_config_ = configured_log;
    
    // Parse TLS configuration for Kafka protocol
    std::string security_protocol = config_.get<std::string>("security.protocol", "PLAINTEXT");
    tls_config_.enabled = (security_protocol == "SSL") || 
                          config_.get<bool>("ssl.enabled", false);
    
    if (tls_config_.enabled) {
        // 0A.12 (refuse-loud, per Anti-pattern A5 in improve-opus.md §5):
        // until the Kafka TCP session class actually wraps its socket in
        // boost::asio::ssl::stream and performs async_handshake(), accepting
        // an `ssl.enabled=true` config produces a broker that *thinks* it
        // serves TLS but listens as plain TCP. That's worse than refusing —
        // it generates "WrongVersionNumber" alerts on the client when the
        // ClientHello is read as Kafka request framing. Refuse loudly so the
        // operator knows TLS is not in this build, and rejects the config.
        throw std::runtime_error(
            "ssl.enabled / security.protocol=SSL is configured, but TLS is not "
            "implemented in this build (TcpSession uses a plain socket). Either "
            "set security.protocol=PLAINTEXT or wait for the TLS-reality task. "
            "See improve-opus.md §2.10(b) and §5 Anti-pattern A5.");
    } else {
        Logger::info("TLS disabled for Kafka protocol (using PLAINTEXT)");
    }
    
    // Parse TLS configuration for Raft inter-broker communication
    raft_tls_config_.enabled = config_.get<bool>("raft.ssl.enabled", false);
    
    if (raft_tls_config_.enabled) {
        raft_tls_config_.cert_file = config_.get<std::string>("raft.ssl.cert.file", "");
        raft_tls_config_.key_file = config_.get<std::string>("raft.ssl.key.file", "");
        raft_tls_config_.key_password = config_.get<std::string>("raft.ssl.key.password", "");
        raft_tls_config_.ca_file = config_.get<std::string>("raft.ssl.ca.file", "");
        
        if (!raft_tls_config_.isValid()) {
            throw std::runtime_error(
                "Raft TLS is enabled but certificate or key file paths are not specified");
        }
        
        Logger::info("TLS enabled for Raft protocol (cert: {})",
                    raft_tls_config_.cert_file);
    } else {
        Logger::info("TLS disabled for Raft protocol (using PLAINTEXT)");
    }
    
    // Phase 4.2a: Load SASL/PLAIN credentials. Two sources, checked in
    // order:
    //   1. `sasl.plain.credentials.file` — path to a text file with one
    //      `user:password` per line. Lines starting with `#` are comments.
    //   2. `sasl.plain.users` — inline JSON object {user: password, ...}.
    // Both populate `sasl_plain_creds_`. If neither is set, the map stays
    // empty and the handler falls back to "accept any non-empty creds"
    // (dev mode).
    {
        const auto creds_file =
            config_.getString("sasl.plain.credentials.file").value_or("");
        if (!creds_file.empty()) {
            std::ifstream ifs(creds_file);
            if (!ifs) {
                throw std::runtime_error(
                    "sasl.plain.credentials.file configured but cannot be opened: " +
                    creds_file);
            }
            std::string line;
            while (std::getline(ifs, line)) {
                if (line.empty() || line[0] == '#') continue;
                const auto colon = line.find(':');
                if (colon == std::string::npos) continue;
                auto user = line.substr(0, colon);
                auto pass = line.substr(colon + 1);
                if (!user.empty() && !pass.empty()) {
                    sasl_plain_creds_.emplace(std::move(user), std::move(pass));
                }
            }
            Logger::info("SASL PLAIN: loaded {} credential(s) from {}",
                         sasl_plain_creds_.size(), creds_file);
        }
        const auto inline_creds =
            config_.getString("sasl.plain.users").value_or("");
        if (!inline_creds.empty()) {
            try {
                auto j = nlohmann::json::parse(inline_creds);
                if (j.is_object()) {
                    for (auto it = j.begin(); it != j.end(); ++it) {
                        if (it.value().is_string()) {
                            sasl_plain_creds_.emplace(it.key(),
                                                      it.value().get<std::string>());
                        }
                    }
                    Logger::info("SASL PLAIN: total {} credential(s) after inline merge",
                                 sasl_plain_creds_.size());
                }
            } catch (const std::exception& e) {
                throw std::runtime_error(
                    std::string("sasl.plain.users JSON parse error: ") + e.what());
            }
        }
        if (sasl_plain_creds_.empty()) {
            Logger::warn(
                "SASL PLAIN: no credentials configured — accepting any non-empty "
                "user/password (dev mode). Set sasl.plain.credentials.file or "
                "sasl.plain.users for production.");
        }
    }

    // Phase 4.2b: Load SASL/SCRAM-SHA-256 credentials. Same two sources
    // as PLAIN. Broker generates salt + stored_key + server_key from the
    // plaintext password at startup; a production deployment should ship
    // pre-computed credentials in the file instead of plaintexts.
    {
        const auto scram_file =
            config_.getString("sasl.scram.credentials.file").value_or("");
        if (!scram_file.empty()) {
            std::ifstream ifs(scram_file);
            if (!ifs) {
                throw std::runtime_error(
                    "sasl.scram.credentials.file configured but cannot be opened: " +
                    scram_file);
            }
            std::string line;
            while (std::getline(ifs, line)) {
                if (line.empty() || line[0] == '#') continue;
                const auto colon = line.find(':');
                if (colon == std::string::npos) continue;
                auto user = line.substr(0, colon);
                auto pass = line.substr(colon + 1);
                if (!user.empty() && !pass.empty()) {
                    sasl_scram_creds_.emplace(
                        std::move(user), ScramCredentials::fromPassword(pass));
                }
            }
        }
        const auto inline_scram =
            config_.getString("sasl.scram.users").value_or("");
        if (!inline_scram.empty()) {
            try {
                auto j = nlohmann::json::parse(inline_scram);
                if (j.is_object()) {
                    for (auto it = j.begin(); it != j.end(); ++it) {
                        if (it.value().is_string()) {
                            sasl_scram_creds_.emplace(
                                it.key(),
                                ScramCredentials::fromPassword(
                                    it.value().get<std::string>()));
                        }
                    }
                }
            } catch (const std::exception& e) {
                throw std::runtime_error(
                    std::string("sasl.scram.users JSON parse error: ") + e.what());
            }
        }
        if (!sasl_scram_creds_.empty()) {
            Logger::info("SASL SCRAM-SHA-256: loaded {} credential(s)",
                         sasl_scram_creds_.size());
        }
    }

    log_manager_ = std::make_unique<storage::LogManager>(log_dir_, log_config_);
    // Phase 3.2: cleanup interval (compaction + retention sweep). Kafka default
    // is 5 minutes; expose via `log.cleaner.interval.ms` so dev/test configs
    // can observe compaction within a short window.
    {
        const int64_t cleanup_ms =
            config_.get<int64_t>("log.cleaner.interval.ms", 300000);
        log_manager_->setCleanupIntervalMs(cleanup_ms);
    }
    replica_manager_ = std::make_unique<ReplicaManager>();
    
    // Initialize OffsetManager with persistent storage
    const std::string offset_db_path = log_dir_ + "/consumer_offsets";
    offset_manager_ = std::make_shared<OffsetManager>(offset_db_path);

    // Phase 2.1: ProducerStateManager (in-memory dedup state).
    producer_state_manager_ = std::make_unique<ProducerStateManager>();

    // Phase 4.2c: ACL binding store. Bindings are persisted only in
    // memory; restart clears them (matches the behavior of Kafka's
    // built-in `kafka.security.authorizer.AclAuthorizer` when no
    // persistent store is configured).
    acl_store_ = std::make_unique<AclStore>();

    // Phase 4.1k/4.1l: TransactionCoordinator scaffolding.
    transaction_coordinator_ = std::make_unique<TransactionCoordinator>();

    // Phase 1.4: FetchSessionManager (KIP-227).
    fetch_session_manager_ = std::make_unique<FetchSessionManager>();

    // Phase EX-10: IsolationTracker for read_committed support.
    isolation_tracker_ = std::make_unique<IsolationTracker>();
    
    // Initialize monitoring and metrics
    std::string monitoring_host = config_.get<std::string>("monitoring.host", "0.0.0.0");
    int monitoring_port = config_.get<int>("monitoring.port", 9094);
    monitoring_manager_ = std::make_unique<monitoring::MonitoringManager>(
        monitoring_host,
        monitoring_port
    );
    // Use raw pointer from monitoring_manager for metrics_collector_
    // The monitoring_manager owns the MetricsCollector lifecycle
    auto* raw_metrics_collector = monitoring_manager_->metricsCollector();

    // Phase EX-1 (§6.3): wire per-subsystem metric providers. These are
    // queried at every Prometheus scrape; the lambdas hold raw pointers
    // to the broker's owned subsystems (broker outlives MetricsCollector).
    raw_metrics_collector->setProducerStateProvider([this]() {
        std::ostringstream oss;
        if (!producer_state_manager_) return std::string{};
        auto m = producer_state_manager_->getMetrics();
        oss << "# HELP kawasan_producer_state_entries Tracked (topic,partition,producer_id) entries\n"
            << "# TYPE kawasan_producer_state_entries gauge\n"
            << "kawasan_producer_state_entries " << m.entries << "\n\n"
            << "# HELP kawasan_producer_state_evictions_total Entries evicted from the producer state map\n"
            << "# TYPE kawasan_producer_state_evictions_total counter\n"
            << "kawasan_producer_state_evictions_total " << m.evictions_total << "\n\n"
            << "# HELP kawasan_producer_id_count Distinct active producer_ids\n"
            << "# TYPE kawasan_producer_id_count gauge\n"
            << "kawasan_producer_id_count " << m.producer_id_count << "\n\n";
        return oss.str();
    });
    raw_metrics_collector->setFetchSessionProvider([this]() {
        std::ostringstream oss;
        if (!fetch_session_manager_) return std::string{};
        auto m = fetch_session_manager_->getMetrics();
        oss << "# HELP kawasan_fetch_session_count Active fetch sessions\n"
            << "# TYPE kawasan_fetch_session_count gauge\n"
            << "kawasan_fetch_session_count " << m.session_count << "\n\n"
            << "# HELP kawasan_fetch_session_evictions_total Sessions evicted due to idle timeout\n"
            << "# TYPE kawasan_fetch_session_evictions_total counter\n"
            << "kawasan_fetch_session_evictions_total " << m.evictions_total << "\n\n"
            << "# HELP kawasan_incremental_fetch_session_hit_ratio Fraction of fetches served by an existing session\n"
            << "# TYPE kawasan_incremental_fetch_session_hit_ratio gauge\n"
            << "kawasan_incremental_fetch_session_hit_ratio " << m.incremental_hit_ratio << "\n\n";
        return oss.str();
    });
    raw_metrics_collector->setTransactionProvider([this]() {
        std::ostringstream oss;
        if (!transaction_coordinator_) return std::string{};
        auto m = transaction_coordinator_->getMetrics();
        oss << "# HELP kawasan_transactions_in_progress Transactions currently in Ongoing state\n"
            << "# TYPE kawasan_transactions_in_progress gauge\n"
            << "kawasan_transactions_in_progress " << m.in_progress << "\n\n"
            << "# HELP kawasan_transaction_commits_total Committed transactions\n"
            << "# TYPE kawasan_transaction_commits_total counter\n"
            << "kawasan_transaction_commits_total " << m.commits_total << "\n\n"
            << "# HELP kawasan_transaction_aborts_total Aborted transactions\n"
            << "# TYPE kawasan_transaction_aborts_total counter\n"
            << "kawasan_transaction_aborts_total " << m.aborts_total << "\n\n"
            << "# HELP kawasan_transaction_state_loads_total InitProducerId invocations (state machine loads)\n"
            << "# TYPE kawasan_transaction_state_loads_total counter\n"
            << "kawasan_transaction_state_loads_total " << m.state_loads_total << "\n\n";
        return oss.str();
    });
    // group_provider_ and log_cleaner_provider_ are wired after the
    // respective subsystems exist; see below.

    // Initialize GroupCoordinator with OffsetManager, LogManager, and MetricsCollector
    group_coordinator_ = std::make_shared<GroupCoordinator>(
        offset_manager_,
        log_manager_.get(),
        std::shared_ptr<monitoring::MetricsCollector>(raw_metrics_collector, [](monitoring::MetricsCollector*){})
    );

    // Phase EX-1: wire GroupCoordinator + LogCleaner providers (must
    // come after group_coordinator_ + log_manager_ are constructed).
    raw_metrics_collector->setGroupProvider([this]() {
        std::ostringstream oss;
        if (!group_coordinator_) return std::string{};
        auto m = group_coordinator_->getMetrics();
        oss << "# HELP kawasan_group_member_timeout_total Members evicted for missed heartbeats\n"
            << "# TYPE kawasan_group_member_timeout_total counter\n"
            << "kawasan_group_member_timeout_total " << m.member_timeout_total << "\n\n";
        // Always emit HELP/TYPE for label-family metrics even when the
        // family is empty — operators want to know the metric exists.
        oss << "# HELP kawasan_group_rebalances_total Per-group rebalance count\n"
            << "# TYPE kawasan_group_rebalances_total counter\n";
        for (const auto& g : m.groups) {
            oss << "kawasan_group_rebalances_total{group=\"" << g.group_id
                << "\"} " << g.rebalances_total << "\n";
        }
        oss << "\n# HELP kawasan_group_state Group state machine kind (1 = currently in this state)\n"
            << "# TYPE kawasan_group_state gauge\n";
        for (const auto& g : m.groups) {
            oss << "kawasan_group_state{group=\"" << g.group_id
                << "\",state=\"" << g.state << "\"} 1\n";
        }
        oss << "\n";
        return oss.str();
    });
    raw_metrics_collector->setLogCleanerProvider([this]() {
        std::ostringstream oss;
        if (!log_manager_) return std::string{};
        auto m = log_manager_->getCleanerMetrics();
        oss << "# HELP kawasan_log_cleaner_running 1 if a cleanup pass is currently active\n"
            << "# TYPE kawasan_log_cleaner_running gauge\n"
            << "kawasan_log_cleaner_running " << (m.running ? 1 : 0) << "\n\n"
            << "# HELP kawasan_log_cleaner_compactions_total Cleanup passes executed\n"
            << "# TYPE kawasan_log_cleaner_compactions_total counter\n"
            << "kawasan_log_cleaner_compactions_total " << m.compactions_total << "\n\n"
            << "# HELP kawasan_log_cleaner_dedupe_buffer_utilization Last cleanup pass's OffsetMap size (unique keys)\n"
            << "# TYPE kawasan_log_cleaner_dedupe_buffer_utilization gauge\n"
            << "kawasan_log_cleaner_dedupe_buffer_utilization " << m.dedupe_buffer_utilization << "\n\n";
        if (!m.partition_dirty_ratios.empty()) {
            oss << "# HELP kawasan_log_cleaner_dirty_ratio Per-partition fraction of log eligible for compaction\n"
                << "# TYPE kawasan_log_cleaner_dirty_ratio gauge\n";
            for (const auto& r : m.partition_dirty_ratios) {
                oss << "kawasan_log_cleaner_dirty_ratio{topic=\"" << r.topic
                    << "\",partition=\"" << r.partition
                    << "\"} " << r.dirty_ratio << "\n";
            }
            oss << "\n";
        }
        return oss.str();
    });
    
    // Load persisted group state from storage
    group_coordinator_->loadGroupsFromStorage();
    
    // Start background cleanup thread for expired groups and timed-out members
    group_coordinator_->startCleanupThread();
    
    request_metrics_ = std::make_shared<metrics::RequestMetrics>();
    request_dispatcher_ = std::make_shared<RequestDispatcher>(request_metrics_);
    supported_api_versions_ = {
        {protocol::ApiKey::API_VERSIONS, 0, 4},
        {protocol::ApiKey::LIST_OFFSETS, 0, kListOffsetsMaxVersion},
        {protocol::ApiKey::METADATA, 0, kMetadataMaxVersion},
        {protocol::ApiKey::FIND_COORDINATOR, 0, kFindCoordinatorMaxVersion},
        // Phase 1.11/1.12: range is now 0..max (previously pinned at the
        // single constant, which silently broke any Java 3.x client that
        // negotiated a different version).
        {protocol::ApiKey::OFFSET_COMMIT, 0, kOffsetCommitMaxVersion},
        {protocol::ApiKey::OFFSET_FETCH, 0, kOffsetFetchMaxVersion},
        {protocol::ApiKey::JOIN_GROUP, 0, kJoinGroupMaxVersion},
        {protocol::ApiKey::SYNC_GROUP, 0, kSyncGroupMaxVersion},
        {protocol::ApiKey::HEARTBEAT, 0, kHeartbeatMaxVersion},
    {protocol::ApiKey::LEAVE_GROUP, 0, kLeaveGroupMaxVersion},
        {protocol::ApiKey::CREATE_TOPICS, 0, 7},  // Phase 1.15
        {protocol::ApiKey::DELETE_TOPICS, 0, 6},  // Phase 1.16: v6 topic-id form supported
        {protocol::ApiKey::PRODUCE, 0, kProduceMaxVersion},
        {protocol::ApiKey::FETCH, 0, kFetchMaxVersion},
        {protocol::ApiKey::DESCRIBE_GROUPS, 0, kDescribeGroupsMaxVersion},
        {protocol::ApiKey::LIST_GROUPS, 0, kListGroupsMaxVersion},
        {protocol::ApiKey::DESCRIBE_CONFIGS, 0, 4},
        {protocol::ApiKey::DESCRIBE_CLUSTER, 0, 1},  // Phase 1.17
        // Phase 1.18: minimal InitProducerId. We advertise v0–v4 so Java 3.x
        // (which negotiates the highest version it supports) gets a working
        // handshake even though the body returned is identical at all versions.
        {protocol::ApiKey::INIT_PRODUCER_ID, 0, 4},
        // Phase 1.19: OffsetForLeaderEpoch. Single-broker only ever has one
        // leader epoch (=0), so the answer is always (epoch=0, end_offset).
        {protocol::ApiKey::OFFSET_FOR_LEADER_EPOCH, 0, 4},
        // Phase 4.1a: AlterConfigs (legacy replace-all configs API).
        {protocol::ApiKey::ALTER_CONFIGS, 0, 2},
        // Phase 4.1b: IncrementalAlterConfigs (modern per-key SET/DELETE/...).
        {protocol::ApiKey::INCREMENTAL_ALTER_CONFIGS, 0, 1},
        // Phase 4.1c-m: admin batch.
        {protocol::ApiKey::DESCRIBE_LOG_DIRS, 0, 0},
        {protocol::ApiKey::ALTER_REPLICA_LOG_DIRS, 0, 0},
        {protocol::ApiKey::ELECT_LEADERS, 0, 1},
        {protocol::ApiKey::DELETE_RECORDS, 0, 0},
        {protocol::ApiKey::DELETE_GROUPS, 0, 0},
        {protocol::ApiKey::OFFSET_DELETE, 0, 0},
        {protocol::ApiKey::CREATE_PARTITIONS, 0, 0},
        {protocol::ApiKey::DESCRIBE_PRODUCERS, 0, 0},
        {protocol::ApiKey::LIST_TRANSACTIONS, 0, 0},
        {protocol::ApiKey::DESCRIBE_TRANSACTIONS, 0, 0},
        {protocol::ApiKey::ALTER_PARTITION, 0, 0},
        {protocol::ApiKey::DESCRIBE_ACLS, 0, 0},
        {protocol::ApiKey::CREATE_ACLS, 0, 0},
        {protocol::ApiKey::DELETE_ACLS, 0, 0},
        // Phase 4.2a: SASL PLAIN handshake + authenticate.
        {protocol::ApiKey::SASL_HANDSHAKE, 0, 1},
        {protocol::ApiKey::SASL_AUTHENTICATE, 0, 1},
        // Phase 3.3 scaffolding: transactional APIs (v0 only — covers
        // kafka-python + librdkafka negotiation; full semantics deferred).
        {protocol::ApiKey::ADD_PARTITIONS_TO_TXN, 0, 0},
        {protocol::ApiKey::ADD_OFFSETS_TO_TXN, 0, 0},
        {protocol::ApiKey::END_TXN, 0, 0},
        {protocol::ApiKey::TXN_OFFSET_COMMIT, 0, 0},
    };
    registerProtocolHandlers();

    Logger::info("Initialized KawasanBroker with ID {} on {}:{}", broker_id_, host_, port_);
}

KawasanBroker::~KawasanBroker() {
    stop();
}

void KawasanBroker::start() {
    if (running_) {
        Logger::warn("Broker already running");
        return;
    }

    Logger::info("Starting KawasanBroker...");

    // Start log manager
    log_manager_->start();

    // 0A.13: Hold a work guard so io_context_.run() does not return when the
    // task queue transiently empties. Without it, single-node deployments with
    // no Raft transport sessions could see the IO thread exit immediately.
    io_work_guard_.emplace(boost::asio::make_work_guard(io_context_));

    // Start IO context thread for Raft transport
    io_thread_ = std::make_unique<std::thread>([this]() {
        Logger::debug("IO context thread started");
        io_context_.run();
        Logger::debug("IO context thread stopped");
    });

    // Initialize Raft for metadata management
    initializeRaft();

    // Load metadata and install commit hooks
    initializeMetadata();

    // Start services
    startServices();
    if (metadata_controller_) {
        metadata_controller_->updateLocalBroker(localBrokerMetadata());
    }

    running_ = true;

    // 0A.11: Mark the broker as healthy and ready for serving traffic.
    // Probes against /readiness and /liveness previously returned 503 forever
    // because these flags defaulted to false and were never flipped.
    if (monitoring_manager_) {
        monitoring_manager_->setBrokerHealthy(true);
        monitoring_manager_->setBrokerReady(true);
    }

    Logger::info("KawasanBroker started successfully on {}:{}", host_, port_);
}

void KawasanBroker::stop() {
    if (!running_) {
        return;
    }

    Logger::info("Stopping KawasanBroker...");

    // 0A.11: Mark unhealthy/not-ready at the start of shutdown so probes
    // immediately reflect "draining"; orchestrators can stop sending traffic
    // while the broker completes graceful shutdown.
    if (monitoring_manager_) {
        monitoring_manager_->setBrokerReady(false);
        monitoring_manager_->setBrokerHealthy(false);
    }

    stopServices();

    if (metadata_controller_) {
        metadata_controller_->stop();
    }

    if (raft_node_) {
        raft_node_->stop();
    }

    // Stop IO context
    // 0A.13: Reset the work guard first so io_context_.run() can return
    // naturally once the remaining handlers finish.
    io_work_guard_.reset();
    io_context_.stop();
    if (io_thread_ && io_thread_->joinable()) {
        io_thread_->join();
        io_thread_.reset();
    }

    if (log_manager_) {
        log_manager_->stop();
        log_manager_->flushAll();
        log_manager_->closeAll();
    }

    // Release OffsetManager and GroupCoordinator so their RocksDB handles
    // are closed before another broker instance opens the same directory.
    group_coordinator_.reset();
    offset_manager_.reset();

    running_ = false;
    Logger::info("KawasanBroker stopped");
}

void KawasanBroker::initializeRaft() {
    // Parse Raft peers from configuration
    // Format: "raft.peers=0:localhost:9093,1:localhost:9094,2:localhost:9095"
    std::vector<raft::PeerInfo> peers;
    bool found_self_in_peers = false;
    
    const std::string peers_str = config_.get<std::string>("raft.peers", "");
    if (!peers_str.empty()) {
        // Split by comma
        size_t start = 0;
        size_t end = peers_str.find(',');
        
        while (start < peers_str.length()) {
            std::string peer_str = (end == std::string::npos) 
                ? peers_str.substr(start)
                : peers_str.substr(start, end - start);
            
            // Parse "broker_id:host:port"
            size_t first_colon = peer_str.find(':');
            size_t second_colon = peer_str.find(':', first_colon + 1);
            
            if (first_colon != std::string::npos && second_colon != std::string::npos) {
                try {
                    int peer_id = std::stoi(peer_str.substr(0, first_colon));
                    std::string host = peer_str.substr(first_colon + 1, 
                                                       second_colon - first_colon - 1);
                    int port = std::stoi(peer_str.substr(second_colon + 1));
                    
                    // Check if this is the current broker
                    if (peer_id == broker_id_) {
                        found_self_in_peers = true;
                        Logger::info("Found current broker (id={}) in peers list: {}:{}",
                                   peer_id, host, port);
                    } else {
                        // Add other peers for Raft communication
                        raft::PeerInfo info;
                        info.id = peer_id;
                        info.host = host;
                        info.port = port;
                        peers.push_back(info);
                        Logger::info("Added Raft peer: id={}, host={}, port={}", 
                                   peer_id, host, port);
                    }
                } catch (const std::exception& e) {
                    Logger::warn("Failed to parse Raft peer '{}': {}", peer_str, e.what());
                }
            }
            
            if (end == std::string::npos) break;
            start = end + 1;
            end = peers_str.find(',', start);
        }
        
        // Validate that broker.id is in the peers list
        if (!found_self_in_peers) {
            Logger::error("broker.id={} is not present in raft.peers list: {}",
                        broker_id_, peers_str);
            throw std::runtime_error(
                "Configuration error: broker.id must be present in raft.peers list when raft.peers is configured");
        }
    }
    
    // Get Raft port from config (default: 9093)
    int raft_port = config_.get<int>("raft.port", 9093);
    
    // Create RaftNode with io_context
    // 0A.7: persist Raft state under {metadata_dir}/raft so current_term,
    // voted_for, and log entries survive process restart (required for
    // multi-node Raft safety; harmless in single-node mode).
    const std::string raft_data_dir = metadata_dir_ + "/raft";
    raft_node_ = std::make_unique<raft::RaftNode>(broker_id_, peers, io_context_,
                                                  raft_port, raft_data_dir);
    raft_node_->start();

    if (peers.empty()) {
        Logger::info("Initialized Raft node for broker {} in single-node mode (no peers configured)", 
                    broker_id_);
    } else {
        Logger::info("Initialized Raft node for broker {} on port {} with {} peer(s) in multi-broker mode", 
                    broker_id_, raft_port, peers.size());
    }
}

void KawasanBroker::initializeMetadata() {
    if (metadata_controller_) {
        return;
    }
    BrokerMetadata broker = localBrokerMetadata();
    metadata_controller_ = std::make_unique<MetadataController>(
        metadata_dir_, cluster_id_, broker, log_manager_.get(), raft_node_.get());
    metadata_controller_->start();
    cluster_id_ = metadata_controller_->clusterId();

    // Phase EX-9 (durability): on restart, pre-create Log objects for
    // every (topic, partition) that exists in the metadata store. This
    // closes a durability bug where post-restart ListOffsets / Fetch
    // calls into log_manager_->getLog() which returns nullptr because
    // the Log wasn't yet instantiated in memory — even though the data
    // is durably on disk. Without this loop, every acked record would
    // be invisible to clients until something else (Produce, etc.)
    // forced lazy log creation. **An acknowledged record that's
    // invisible after restart is a critical correctness failure.**
    if (metadata_controller_ && log_manager_) {
        const auto topics = metadata_controller_->describeTopics({});
        size_t restored = 0;
        for (const auto& tm : topics) {
            // Per-topic config (cleanup.policy, etc.) is already
            // registered by applyCreate during the metadata load path.
            for (const auto& pm : tm.partitions) {
                // getOrCreateLog opens the existing log dir and loads
                // its segments via Log::loadSegments(); this is what
                // restores prior records.
                (void)log_manager_->getOrCreateLog(tm.name, pm.partition);
                ++restored;
            }
        }
        if (restored > 0) {
            Logger::info("Restored {} partitions from metadata on startup", restored);
        }
    }

    // Phase 3.1: create `__consumer_offsets` as a real Kafka topic. Now
    // using the Kafka-default 50 partitions; OffsetCommit hashes the
    // group_id (FNV-1a) to pick the target partition. This matches the
    // Kafka convention so `kafka-consumer-groups.sh --describe` can find
    // commits by reading a deterministic partition rather than scanning.
    if (metadata_controller_) {
        constexpr int32_t kConsumerOffsetsPartitions = 50;
        TopicSpecification spec;
        spec.name = "__consumer_offsets";
        spec.num_partitions = kConsumerOffsetsPartitions;
        spec.replication_factor = 1;
        spec.configs["cleanup.policy"] = "compact";
        spec.configs["segment.bytes"] = "104857600";
        auto result = metadata_controller_->createTopic(spec);
        if (result.error_code == ErrorCode::NONE) {
            Logger::info("Created internal topic __consumer_offsets with {} partitions",
                         kConsumerOffsetsPartitions);
        } else if (result.error_code == ErrorCode::TOPIC_ALREADY_EXISTS) {
            Logger::debug("Internal topic __consumer_offsets already exists");
        } else {
            Logger::warn("Failed to create __consumer_offsets: {}",
                         result.error_message);
        }

        // Phase 3.3: auto-create `__transaction_state`. Kafka uses 50
        // partitions by default (same as `__consumer_offsets`) with
        // compact policy. TransactionCoordinator (when fully
        // implemented) writes commit/abort markers and PrepareCommit
        // entries here. For now the topic exists so Java AdminClient
        // probes and transactional producers don't see
        // UNKNOWN_TOPIC_OR_PARTITION on initial setup.
        constexpr int32_t kTxnStatePartitions = 50;
        TopicSpecification txn_spec;
        txn_spec.name = "__transaction_state";
        txn_spec.num_partitions = kTxnStatePartitions;
        txn_spec.replication_factor = 1;
        txn_spec.configs["cleanup.policy"] = "compact";
        txn_spec.configs["segment.bytes"] = "104857600";
        txn_spec.configs["min.compaction.lag.ms"] = "0";
        auto txn_result = metadata_controller_->createTopic(txn_spec);
        if (txn_result.error_code == ErrorCode::NONE) {
            Logger::info("Created internal topic __transaction_state with {} partitions",
                         kTxnStatePartitions);
        } else if (txn_result.error_code == ErrorCode::TOPIC_ALREADY_EXISTS) {
            Logger::debug("Internal topic __transaction_state already exists");
        } else {
            Logger::warn("Failed to create __transaction_state: {}",
                         txn_result.error_message);
        }
    }
}

void KawasanBroker::startServices() {
    const uint32_t hw_threads = std::thread::hardware_concurrency();
    size_t default_threads = hw_threads == 0 ? 1 : hw_threads;
    int32_t configured_threads =
        config_.get<int32_t>("network.io_threads", static_cast<int32_t>(default_threads));
    if (configured_threads <= 0) {
        configured_threads = static_cast<int32_t>(default_threads);
    }

    const size_t max_frame_bytes = static_cast<size_t>(
        config_.get<int64_t>("network.max_frame_bytes", 16 * 1024 * 1024));

    // Prepare TLS config for TCP server
    network::TlsConfig server_tls_config;
    server_tls_config.enabled = tls_config_.enabled;
    server_tls_config.cert_file = tls_config_.cert_file;
    server_tls_config.key_file = tls_config_.key_file;
    server_tls_config.ca_file = tls_config_.ca_file;
    server_tls_config.key_password = tls_config_.key_password;
    server_tls_config.verify_client = tls_config_.requiresClientAuth();

    tcp_server_ = std::make_unique<network::TcpServer>(
        host_, port_, static_cast<size_t>(configured_threads), max_frame_bytes,
        request_dispatcher_, nullptr, std::chrono::seconds(600), server_tls_config);
    tcp_server_->start();
    port_ = tcp_server_->listeningPort();

    Logger::info("Broker TCP listener active on {}:{}", host_, port_);

    // Start monitoring HTTP server
    if (monitoring_manager_) {
        monitoring_manager_->start();
        Logger::info("Monitoring server started on port 9094");
    }

    // Consumer lag computation thread is DISABLED by default due to performance impact
    // TODO: Make this optional via config (consumer.lag.metrics.enabled=false by default)
    // Even with 30s interval, the lag computation causes:
    // - Producer throughput: -12.4% (34,623 -> 30,326 msg/s)
    // - Consumer throughput: -15.6% (21,574 -> 18,217 msg/s)
    // Leaving code commented for future optional enablement
    /*
    lag_computation_running_ = true;
    lag_computation_thread_ = std::thread([this]() {
        Logger::info("Starting consumer lag computation thread (interval: 30s)");
        while (lag_computation_running_) {
            if (group_coordinator_) {
                group_coordinator_->computeAndRecordConsumerLag();
            }
            std::this_thread::sleep_for(std::chrono::seconds(30));
        }
        Logger::info("Consumer lag computation thread stopped");
    });
    */
    Logger::info("Consumer lag metrics DISABLED (enable via consumer.lag.metrics.enabled=true)");
}

void KawasanBroker::stopServices() {
    // Stop lag computation thread
    if (lag_computation_running_) {
        lag_computation_running_ = false;
        if (lag_computation_thread_.joinable()) {
            lag_computation_thread_.join();
        }
    }

    // Stop monitoring server
    if (monitoring_manager_) {
        monitoring_manager_->stop();
        Logger::info("Monitoring server stopped");
    }

    if (tcp_server_) {
        tcp_server_->stop();
        tcp_server_.reset();
    }
    Logger::info("Stopped broker services");
}

void KawasanBroker::registerProtocolHandlers() {
    if (!request_dispatcher_) {
        return;
    }
    using Context = RequestDispatcher::RequestContext;

    request_dispatcher_->registerHandler(
        protocol::ApiKey::API_VERSIONS, 0, 4,
        [this](Context& context) {
            RequestDispatcher::HandlerResult result;
            result.payload = handleApiVersions(context);
            return result;
        },
        [this](const Context& context, ErrorCode code, int16_t version) {
            return buildApiVersionsError(context, code, version);
        });

    request_dispatcher_->registerHandler(
        protocol::ApiKey::METADATA, 0, kMetadataMaxVersion,
        [this](Context& context) {
            RequestDispatcher::HandlerResult result;
            result.payload = handleMetadata(context);
            return result;
        },
        [this](const Context& context, ErrorCode code, int16_t version) {
            return buildMetadataError(context, code, version);
        });

    request_dispatcher_->registerHandler(
        protocol::ApiKey::CREATE_TOPICS, 0, 7,  // Phase 1.15
        [this](Context& context) {
            RequestDispatcher::HandlerResult result;
            result.payload = handleCreateTopics(context);
            return result;
        },
        [this](const Context& context, ErrorCode code, int16_t version) {
            return buildCreateTopicsError(context, code, version);
        });

    request_dispatcher_->registerHandler(
        protocol::ApiKey::DELETE_TOPICS, 0, 6,  // Phase 1.16 (v6: topic_id supported)
        [this](Context& context) {
            RequestDispatcher::HandlerResult result;
            result.payload = handleDeleteTopics(context);
            return result;
        },
        [this](const Context& context, ErrorCode code, int16_t version) {
            return buildDeleteTopicsError(context, code, version);
        });

    request_dispatcher_->registerHandler(
        protocol::ApiKey::PRODUCE, 0, kProduceMaxVersion,
        [this](Context& context) { return handleProduce(context); },
        [this](const Context& context, ErrorCode code, int16_t version) {
            return buildProduceError(context, code, version);
        });

    request_dispatcher_->registerHandler(
        protocol::ApiKey::FETCH, 0, kFetchMaxVersion,
        [this](Context& context) { return handleFetch(context); },
        [this](const Context& context, ErrorCode code, int16_t version) {
            return buildFetchError(context, code, version);
        });

    request_dispatcher_->registerHandler(
        protocol::ApiKey::LIST_OFFSETS, 0, kListOffsetsMaxVersion,
        [this](Context& context) { return handleListOffsets(context); },
        [this](const Context& context, ErrorCode code, int16_t version) {
            return buildListOffsetsError(context, code, version);
        });

    request_dispatcher_->registerHandler(
        protocol::ApiKey::FIND_COORDINATOR, 0, kFindCoordinatorMaxVersion,
        [this](Context& context) { return handleFindCoordinator(context); },
        [this](const Context& context, ErrorCode code, int16_t version) {
            return buildFindCoordinatorError(context, code, version);
        });

    request_dispatcher_->registerHandler(
        protocol::ApiKey::JOIN_GROUP, 0, kJoinGroupMaxVersion,
        [this](Context& context) {
            RequestDispatcher::HandlerResult result;
            result.payload = handleJoinGroup(context);
            return result;
        },
        [this](const Context& context, ErrorCode code, int16_t version) {
            return buildJoinGroupError(context, code, version);
        });

    request_dispatcher_->registerHandler(
        protocol::ApiKey::SYNC_GROUP, 0, kSyncGroupMaxVersion,
        [this](Context& context) {
            RequestDispatcher::HandlerResult result;
            result.payload = handleSyncGroup(context);
            return result;
        },
        [this](const Context& context, ErrorCode code, int16_t version) {
            return buildSyncGroupError(context, code, version);
        });

    request_dispatcher_->registerHandler(
        protocol::ApiKey::HEARTBEAT, 0, kHeartbeatMaxVersion,
        [this](Context& context) {
            RequestDispatcher::HandlerResult result;
            result.payload = handleHeartbeat(context);
            return result;
        },
        [this](const Context& context, ErrorCode code, int16_t version) {
            return buildHeartbeatError(context, code, version);
        });

    request_dispatcher_->registerHandler(
        protocol::ApiKey::LEAVE_GROUP, 0, kLeaveGroupMaxVersion,
        [this](Context& context) {
            RequestDispatcher::HandlerResult result;
            result.payload = handleLeaveGroup(context);
            return result;
        },
        [this](const Context& context, ErrorCode code, int16_t version) {
            return buildLeaveGroupError(context, code, version);
        });

    request_dispatcher_->registerHandler(
        protocol::ApiKey::OFFSET_COMMIT, 0, kOffsetCommitMaxVersion,
        [this](Context& context) {
            RequestDispatcher::HandlerResult result;
            result.payload = handleOffsetCommit(context);
            return result;
        },
        [this](const Context& context, ErrorCode code, int16_t version) {
            return buildOffsetCommitError(context, code, version);
        });

    request_dispatcher_->registerHandler(
        protocol::ApiKey::OFFSET_FETCH, 0, kOffsetFetchMaxVersion,
        [this](Context& context) {
            RequestDispatcher::HandlerResult result;
            result.payload = handleOffsetFetch(context);
            return result;
        },
        [this](const Context& context, ErrorCode code, int16_t version) {
            return buildOffsetFetchError(context, code, version);
        });

    request_dispatcher_->registerHandler(
        protocol::ApiKey::DESCRIBE_GROUPS, 0, kDescribeGroupsMaxVersion,
        [this](Context& context) {
            RequestDispatcher::HandlerResult result;
            result.payload = handleDescribeGroups(context);
            return result;
        },
        [this](const Context& context, ErrorCode code, int16_t version) {
            return buildDescribeGroupsError(context, code, version);
        });

    request_dispatcher_->registerHandler(
        protocol::ApiKey::LIST_GROUPS, 0, kListGroupsMaxVersion,
        [this](Context& context) {
            RequestDispatcher::HandlerResult result;
            result.payload = handleListGroups(context);
            return result;
        },
        [this](const Context& context, ErrorCode code, int16_t version) {
            return buildListGroupsError(context, code, version);
        });

    request_dispatcher_->registerHandler(
        protocol::ApiKey::DESCRIBE_CONFIGS, 0, 4,
        [this](Context& context) {
            RequestDispatcher::HandlerResult result;
            result.payload = handleDescribeConfigs(context);
            return result;
        },
        [this](const Context& context, ErrorCode code, int16_t version) {
            return buildDescribeConfigsError(context, code, version);
        });

    request_dispatcher_->registerHandler(
        protocol::ApiKey::DESCRIBE_CLUSTER, 0, 1,  // Phase 1.17
        [this](Context& context) {
            RequestDispatcher::HandlerResult result;
            result.payload = handleDescribeCluster(context);
            return result;
        },
        [this](const Context& context, ErrorCode code, int16_t version) {
            return buildDescribeClusterError(context, code, version);
        });

    // Phase 1.18: InitProducerId (API 22). Minimal viable — see header note.
    request_dispatcher_->registerHandler(
        protocol::ApiKey::INIT_PRODUCER_ID, 0, 4,
        [this](Context& context) {
            RequestDispatcher::HandlerResult result;
            result.payload = handleInitProducerId(context);
            return result;
        },
        [this](const Context& context, ErrorCode code, int16_t version) {
            return buildInitProducerIdError(context, code, version);
        });

    // Phase 1.19: OffsetForLeaderEpoch (API 23). Single-broker stub.
    request_dispatcher_->registerHandler(
        protocol::ApiKey::OFFSET_FOR_LEADER_EPOCH, 0, 4,
        [this](Context& context) {
            RequestDispatcher::HandlerResult result;
            result.payload = handleOffsetForLeaderEpoch(context);
            return result;
        },
        [this](const Context& context, ErrorCode code, int16_t version) {
            return buildOffsetForLeaderEpochError(context, code, version);
        });

    // Phase 4.1a: AlterConfigs.
    request_dispatcher_->registerHandler(
        protocol::ApiKey::ALTER_CONFIGS, 0, 2,
        [this](Context& context) {
            RequestDispatcher::HandlerResult result;
            result.payload = handleAlterConfigs(context);
            return result;
        },
        [this](const Context& context, ErrorCode code, int16_t version) {
            return buildAlterConfigsError(context, code, version);
        });

    // Phase 4.1b: IncrementalAlterConfigs.
    request_dispatcher_->registerHandler(
        protocol::ApiKey::INCREMENTAL_ALTER_CONFIGS, 0, 1,
        [this](Context& context) {
            RequestDispatcher::HandlerResult result;
            result.payload = handleIncrementalAlterConfigs(context);
            return result;
        },
        [this](const Context& context, ErrorCode code, int16_t version) {
            return buildIncrementalAlterConfigsError(context, code, version);
        });

    // Phase 4.1c-m: admin batch (each shares a trivial error builder).
    auto reg_admin = [&](protocol::ApiKey k, int16_t mn, int16_t mx,
                         Buffer (KawasanBroker::*fn)(RequestDispatcher::RequestContext&)) {
        request_dispatcher_->registerHandler(
            k, mn, mx,
            [this, fn](Context& ctx) {
                RequestDispatcher::HandlerResult r;
                r.payload = (this->*fn)(ctx);
                return r;
            },
            [this](const Context& ctx, ErrorCode /*code*/, int16_t /*ver*/) {
                return buildEmptyErrorResponse(ctx);
            });
    };
    reg_admin(protocol::ApiKey::DESCRIBE_LOG_DIRS, 0, 0,
              &KawasanBroker::handleDescribeLogDirs);
    reg_admin(protocol::ApiKey::ALTER_REPLICA_LOG_DIRS, 0, 0,
              &KawasanBroker::handleAlterReplicaLogDirs);
    reg_admin(protocol::ApiKey::ELECT_LEADERS, 0, 1,
              &KawasanBroker::handleElectLeaders);
    reg_admin(protocol::ApiKey::DELETE_RECORDS, 0, 0,
              &KawasanBroker::handleDeleteRecords);
    reg_admin(protocol::ApiKey::DELETE_GROUPS, 0, 0,
              &KawasanBroker::handleDeleteGroups);
    reg_admin(protocol::ApiKey::OFFSET_DELETE, 0, 0,
              &KawasanBroker::handleOffsetDelete);
    reg_admin(protocol::ApiKey::CREATE_PARTITIONS, 0, 0,
              &KawasanBroker::handleCreatePartitions);
    reg_admin(protocol::ApiKey::DESCRIBE_PRODUCERS, 0, 0,
              &KawasanBroker::handleDescribeProducers);
    reg_admin(protocol::ApiKey::LIST_TRANSACTIONS, 0, 0,
              &KawasanBroker::handleListTransactions);
    reg_admin(protocol::ApiKey::DESCRIBE_TRANSACTIONS, 0, 0,
              &KawasanBroker::handleDescribeTransactions);
    reg_admin(protocol::ApiKey::ALTER_PARTITION, 0, 0,
              &KawasanBroker::handleAlterPartition);
    reg_admin(protocol::ApiKey::DESCRIBE_ACLS, 0, 0,
              &KawasanBroker::handleDescribeAcls);
    reg_admin(protocol::ApiKey::CREATE_ACLS, 0, 0,
              &KawasanBroker::handleCreateAcls);
    reg_admin(protocol::ApiKey::DELETE_ACLS, 0, 0,
              &KawasanBroker::handleDeleteAcls);

    // Phase 4.2a: SASL PLAIN.
    reg_admin(protocol::ApiKey::SASL_HANDSHAKE, 0, 1,
              &KawasanBroker::handleSaslHandshake);
    reg_admin(protocol::ApiKey::SASL_AUTHENTICATE, 0, 1,
              &KawasanBroker::handleSaslAuthenticate);

    // Phase 3.3 scaffolding: transactional APIs. Advertised v0 only —
    // sufficient for kafka-python and librdkafka to negotiate; higher
    // versions just add tagged_fields which our v0 encoders don't emit
    // but our flex-header path handles correctly.
    reg_admin(protocol::ApiKey::ADD_PARTITIONS_TO_TXN, 0, 0,
              &KawasanBroker::handleAddPartitionsToTxn);
    reg_admin(protocol::ApiKey::ADD_OFFSETS_TO_TXN, 0, 0,
              &KawasanBroker::handleAddOffsetsToTxn);
    reg_admin(protocol::ApiKey::END_TXN, 0, 0,
              &KawasanBroker::handleEndTxn);
    reg_admin(protocol::ApiKey::TXN_OFFSET_COMMIT, 0, 0,
              &KawasanBroker::handleTxnOffsetCommit);
}

Buffer KawasanBroker::encodeResponse(
    const RequestDispatcher::RequestContext& context,
    const std::function<void(Buffer&)>& writer) const {
    Buffer buffer;
    // Use isFlexibleResponseHeader() for response header, which may differ from request header
    protocol::ResponseHeader header(context.header.correlationId(),
                                    context.header.isFlexibleResponseHeader());
    header.encode(buffer);
    writer(buffer);
    return buffer;
}

Buffer KawasanBroker::handleApiVersions(RequestDispatcher::RequestContext& context) {
    protocol::ApiVersionsRequest request;
    
    // Defensively decode the request - if decoding fails, return error response
    // instead of letting exception propagate and close connection
    try {
        request.decode(context.payload, context.header.apiVersion());
        
        // Validate client software fields for API version 3+ (KIP-511)
        if (context.header.apiVersion() >= 3) {
            const auto& name = request.clientSoftwareName();
            const auto& version = request.clientSoftwareVersion();
            
            // Validate that the fields only contain allowed characters
            if (!protocol::isValidClientSoftwareString(name)) {
                Logger::warn("Invalid client.software.name from {}: '{}' contains invalid characters",
                             context.peer_identity, name);
                return buildApiVersionsError(context, ErrorCode::INVALID_REQUEST,
                                             context.header.apiVersion());
            }
            
            if (!protocol::isValidClientSoftwareString(version)) {
                Logger::warn("Invalid client.software.version from {}: '{}' contains invalid characters",
                             context.peer_identity, version);
                return buildApiVersionsError(context, ErrorCode::INVALID_REQUEST,
                                             context.header.apiVersion());
            }
            
            // Log client software info for debugging
            Logger::info("API versions request from {}: client='{}' version='{}'",
                         context.peer_identity, name, version);
        }
    } catch (const ProtocolException& ex) {
        Logger::warn("Failed to decode API versions request from {}: {}",
                     context.peer_identity, ex.what());
        // Return error response but don't close connection
        return buildApiVersionsError(context, ErrorCode::INVALID_REQUEST,
                                     context.header.apiVersion());
    } catch (const std::exception& ex) {
        Logger::warn("Unexpected error decoding API versions request from {}: {}",
                     context.peer_identity, ex.what());
        // Return error response but don't close connection
        return buildApiVersionsError(context, ErrorCode::INVALID_REQUEST,
                                     context.header.apiVersion());
    }
    
    protocol::ApiVersionsResponse response;
    response.setErrorCode(ErrorCode::NONE);
    response.setThrottleTimeMs(0);
    response.setApiVersions(supported_api_versions_);
    const int16_t response_version =
        std::clamp<int16_t>(context.header.apiVersion(), 0, 4);
    return encodeResponse(context, [&](Buffer& buffer) {
        response.encode(buffer, response_version);
    });
}

Buffer KawasanBroker::buildApiVersionsError(
    const RequestDispatcher::RequestContext& context, ErrorCode code,
    int16_t response_version) const {
    protocol::ApiVersionsResponse response;
    response.setErrorCode(code);
    response.setThrottleTimeMs(0);
    response.setApiVersions(supported_api_versions_);
    const int16_t version = std::clamp<int16_t>(response_version, 0, 4);
    return encodeResponse(context, [&](Buffer& buffer) {
        response.encode(buffer, version);
    });
}

Buffer KawasanBroker::handleMetadata(RequestDispatcher::RequestContext& context) {
    protocol::MetadataRequest request;
    request.decode(context.payload, context.header.apiVersion());

    Logger::info("Metadata request (API version {})", context.header.apiVersion());

    protocol::MetadataResponse response;
    response.setThrottleTimeMs(0);
    const std::string cluster_id =
        metadata_controller_ ? metadata_controller_->clusterId() : cluster_id_;
    Logger::info("Metadata: cluster_id='{}' (length={})", cluster_id, cluster_id.size());
    response.setClusterId(cluster_id);
    response.setControllerId(broker_id_);
    response.setBrokers(buildBrokerMetadata());

    std::vector<TopicMetadata> topics;
    if (metadata_controller_) {
        if (request.topics().empty()) {
            topics = metadata_controller_->describeTopics(request.topics());
        } else {
            const bool allow_auto_create =
                auto_create_topics_enabled_ && request.allowAutoTopicCreation();
            topics.reserve(request.topics().size());
            for (const auto& topic_name : request.topics()) {
                auto [metadata_opt, error_code] =
                    getTopicMetadata(topic_name, allow_auto_create);
                if (metadata_opt.has_value()) {
                    topics.push_back(*metadata_opt);
                } else {
                    TopicMetadata missing;
                    missing.error_code = error_code;
                    missing.name = topic_name;
                    missing.is_internal = false;
                    topics.push_back(std::move(missing));
                }
            }
        }
    } else {
        const auto fallback = buildDefaultTopicMetadata();
        const auto& requested = request.topics();
        if (requested.empty() || std::find(requested.begin(), requested.end(),
                                           fallback.name) != requested.end()) {
            topics.push_back(fallback);
        }
    }
    response.setTopics(topics);

    const int16_t version =
        std::clamp<int16_t>(context.header.apiVersion(), 0, kMetadataMaxVersion);
    return encodeResponse(context, [&](Buffer& buffer) {
        response.encode(buffer, version);
    });
}

Buffer KawasanBroker::buildMetadataError(
    const RequestDispatcher::RequestContext& context, ErrorCode code,
    int16_t response_version) const {
    protocol::MetadataResponse response;
    response.setThrottleTimeMs(0);
    const std::string cluster_id =
        metadata_controller_ ? metadata_controller_->clusterId() : cluster_id_;
    response.setClusterId(cluster_id);
    response.setControllerId(broker_id_);
    response.setBrokers(buildBrokerMetadata());
    TopicMetadata error_topic;
    error_topic.error_code = code;
    error_topic.name = "__kawasan_error__";
    error_topic.is_internal = true;
    response.setTopics({error_topic});

    const int16_t version =
        std::clamp<int16_t>(response_version, 0, kMetadataMaxVersion);
    return encodeResponse(context, [&](Buffer& buffer) {
        response.encode(buffer, version);
    });
}

Buffer KawasanBroker::handleCreateTopics(RequestDispatcher::RequestContext& context) {
    protocol::CreateTopicsRequest request;
    request.decode(context.payload, context.header.apiVersion());

    protocol::CreateTopicsResponse response;
    response.setThrottleTimeMs(0);

    for (const auto& topic : request.topics()) {
        protocol::CreatableTopicResult result;
        result.name = topic.name;

        if (!metadata_controller_) {
            result.error_code = ErrorCode::BROKER_NOT_AVAILABLE;
            result.error_message = "Metadata controller unavailable";
            response.addTopicResult(result);
            continue;
        }

        if (request.validateOnly()) {
            result.error_code = ErrorCode::INVALID_REQUEST;
            result.error_message = "validateOnly is not supported yet";
            response.addTopicResult(result);
            continue;
        }

        auto spec = protocol::toTopicSpecification(topic);
        auto operation = metadata_controller_->createTopic(spec);
        result.error_code = operation.error_code;
        if (!operation.error_message.empty()) {
            result.error_message = operation.error_message;
        }
        response.addTopicResult(result);
    }

    const int16_t version =
        std::clamp<int16_t>(context.header.apiVersion(), 0, static_cast<int16_t>(7));  // Phase 1.15
    return encodeResponse(context, [&](Buffer& buffer) {
        response.encode(buffer, version);
    });
}

Buffer KawasanBroker::buildCreateTopicsError(
    const RequestDispatcher::RequestContext& context, ErrorCode code,
    int16_t response_version) const {
    protocol::CreateTopicsResponse response;
    response.setThrottleTimeMs(0);
    protocol::CreatableTopicResult result;
    result.name = "__kawasan_topic__";
    result.error_code = code;
    result.error_message = KawasanException::toString(code);
    response.addTopicResult(result);

    const int16_t version =
        std::clamp<int16_t>(response_version, 0, static_cast<int16_t>(7));  // Phase 1.15
    return encodeResponse(context, [&](Buffer& buffer) {
        response.encode(buffer, version);
    });
}

Buffer KawasanBroker::handleDeleteTopics(RequestDispatcher::RequestContext& context) {
    protocol::DeleteTopicsRequest request;
    request.decode(context.payload, context.header.apiVersion());

    protocol::DeleteTopicsResponse response;
    response.setThrottleTimeMs(0);

    // Phase 1.16: in v6+, callers may supply topic_id instead of (or in
    // addition to) topic name. When name is empty, resolve by topic_id
    // against the metadata store. UNKNOWN_TOPIC_ID is the appropriate
    // error if neither name nor topic_id matches an existing topic.
    for (const auto& spec : request.topics()) {
        protocol::DeletableTopicResult result;
        result.name = spec.name;
        result.topic_id = spec.topic_id;

        if (!metadata_controller_) {
            result.error_code = ErrorCode::BROKER_NOT_AVAILABLE;
            result.error_message = "Metadata controller unavailable";
            response.addResult(result);
            continue;
        }

        std::string resolved_name = spec.name;
        if (resolved_name.empty() && spec.has_topic_id) {
            // Resolve topic_id → name by scanning the metadata store.
            for (const auto& t : metadata_controller_->describeTopics({})) {
                if (t.topic_id == spec.topic_id) {
                    resolved_name = t.name;
                    result.name = t.name;
                    break;
                }
            }
            if (resolved_name.empty()) {
                // Kafka error code 100 = UNKNOWN_TOPIC_ID. Not in our
                // ErrorCode enum yet; cast directly.
                result.error_code = static_cast<ErrorCode>(100);
                response.addResult(result);
                continue;
            }
        }

        auto operation = metadata_controller_->deleteTopic(resolved_name);
        result.error_code = operation.error_code;
        if (!operation.error_message.empty()) {
            result.error_message = operation.error_message;
        }
        response.addResult(result);
    }

    const int16_t version =
        std::clamp<int16_t>(context.header.apiVersion(), 0, static_cast<int16_t>(6));  // Phase 1.16
    return encodeResponse(context, [&](Buffer& buffer) {
        response.encode(buffer, version);
    });
}

Buffer KawasanBroker::buildDeleteTopicsError(
    const RequestDispatcher::RequestContext& context, ErrorCode code,
    int16_t response_version) const {
    protocol::DeleteTopicsResponse response;
    response.setThrottleTimeMs(0);
    protocol::DeletableTopicResult result;
    result.name = "__kawasan_topic__";
    result.error_code = code;
    result.error_message = KawasanException::toString(code);
    response.addResult(result);

    const int16_t version =
        std::clamp<int16_t>(response_version, 0, static_cast<int16_t>(6));  // Phase 1.16
    return encodeResponse(context, [&](Buffer& buffer) {
        response.encode(buffer, version);
    });
}

RequestDispatcher::HandlerResult KawasanBroker::handleProduce(
    RequestDispatcher::RequestContext& context) {
    auto start_time = std::chrono::steady_clock::now();
    
    protocol::ProduceRequest request;
    request.decode(context.payload, context.header.apiVersion());

    const int16_t acks = request.acks();
    if (acks != -1 && acks != 0 && acks != 1) {
        RequestDispatcher::HandlerResult invalid_acks;
        invalid_acks.payload = buildProduceError(
            context, ErrorCode::INVALID_REQUIRED_ACKS, context.header.apiVersion());
        invalid_acks.close_connection = false;
        return invalid_acks;
    }

    protocol::ProduceResponse response;
    response.setThrottleTimeMs(0);

    const auto now_ms = []() -> Timestamp {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::system_clock::now().time_since_epoch())
            .count();
    };

    bool has_error = false;

    for (const auto& topic_data : request.topics()) {
        protocol::ProduceTopicResponse topic_response;
        topic_response.topic = topic_data.topic;

        auto [topic_metadata_opt, topic_error] =
            getTopicMetadata(topic_data.topic, auto_create_topics_enabled_);

        for (const auto& partition_data : topic_data.partitions) {
            protocol::ProducePartitionResponse partition_response;
            partition_response.partition = partition_data.partition;
            partition_response.base_offset = 0;
            partition_response.log_start_offset = 0;
            partition_response.log_append_time = -1;
            partition_response.error_code = ErrorCode::NONE;

            if (!topic_metadata_opt.has_value()) {
                partition_response.error_code = topic_error;
                topic_response.partitions.push_back(partition_response);
                has_error = true;
                continue;
            }

            const auto& topic_metadata = topic_metadata_opt.value();
            auto partition_it = std::find_if(
                topic_metadata.partitions.begin(), topic_metadata.partitions.end(),
                [&](const PartitionMetadata& metadata) {
                    return metadata.partition == partition_data.partition;
                });

            if (partition_it == topic_metadata.partitions.end()) {
                partition_response.error_code = ErrorCode::UNKNOWN_TOPIC_OR_PARTITION;
                topic_response.partitions.push_back(partition_response);
                has_error = true;
                continue;
            }

            if (partition_it->leader != broker_id_) {
                partition_response.error_code = ErrorCode::NOT_LEADER_FOR_PARTITION;
                topic_response.partitions.push_back(partition_response);
                has_error = true;
                continue;
            }

            if (!log_manager_) {
                partition_response.error_code = ErrorCode::KAFKA_STORAGE_ERROR;
                topic_response.partitions.push_back(partition_response);
                has_error = true;
                continue;
            }

            try {
                storage::RecordBatch batch =
                    storage::RecordBatch::deserializeFromProduceRequest(partition_data.record_batch);
                auto* log =
                    log_manager_->getOrCreateLog(topic_data.topic, partition_data.partition);
                if (!log) {
                    throw StorageException(ErrorCode::KAFKA_STORAGE_ERROR,
                                           "Failed to create log");
                }

                // Register replica with ReplicaManager if not already registered
                TopicPartition tp{topic_data.topic, partition_data.partition};
                if (!replica_manager_->isLeader(tp)) {
                    // Create a shared_ptr wrapper for the log (LogManager owns the actual log)
                    // We use a non-owning shared_ptr to track it in ReplicaManager
                    std::shared_ptr<storage::Log> log_ptr(log, [](storage::Log*){});
                    replica_manager_->addReplica(tp, log_ptr);
                }

                // Phase 2.1: idempotent producer dedup. For non-idempotent
                // producers (producer_id < 0), check() returns NONE.
                if (producer_state_manager_ && batch.producerId() >= 0) {
                    auto chk = producer_state_manager_->check(
                        topic_data.topic, partition_data.partition,
                        batch.producerId(), batch.producerEpoch(),
                        batch.baseSequence(),
                        static_cast<int32_t>(batch.records().size()));
                    if (chk.error == ErrorCode::DUPLICATE_SEQUENCE_NUMBER) {
                        // Duplicate retry: return the original base_offset
                        // (or -1 if unknown) so the client treats it as
                        // a successful idempotent replay.
                        partition_response.base_offset = chk.duplicate_offset;
                        partition_response.log_start_offset = log->logStartOffset();
                        partition_response.log_append_time = now_ms();
                        partition_response.error_code = ErrorCode::NONE;
                        Logger::info("Produce dedup: {}-{} pid={} epoch={} seq={} → DUPLICATE returning offset={}",
                                     topic_data.topic, partition_data.partition,
                                     batch.producerId(), batch.producerEpoch(),
                                     batch.baseSequence(), chk.duplicate_offset);
                        topic_response.partitions.push_back(partition_response);
                        continue;
                    }
                    if (chk.error != ErrorCode::NONE) {
                        partition_response.error_code = chk.error;
                        Logger::warn("Produce rejected: {}-{} pid={} epoch={} seq={} → error={}",
                                     topic_data.topic, partition_data.partition,
                                     batch.producerId(), batch.producerEpoch(),
                                     batch.baseSequence(),
                                     static_cast<int16_t>(chk.error));
                        topic_response.partitions.push_back(partition_response);
                        has_error = true;
                        continue;
                    }
                }

                // Phase EX-10: preserve the entire V2 batch header
                // (producer_id, producer_epoch, baseSequence, isTransactional
                // bit, isControl bit) when appending. Stripping these via
                // batch.records() + log->append(records) breaks read_committed
                // isolation because the consumer needs the original
                // producer_id to match against the aborted_transactions
                // list returned by Fetch.
                const size_t record_count = batch.records().size();
                const int64_t saved_pid = batch.producerId();
                const int16_t saved_epoch = batch.producerEpoch();
                const int32_t saved_base_seq = batch.baseSequence();
                const Offset base_offset = log->appendBatch(std::move(batch));

                // Phase 2.1: record successful append for dedup.
                if (producer_state_manager_ && saved_pid >= 0) {
                    producer_state_manager_->recordAppend(
                        topic_data.topic, partition_data.partition,
                        saved_pid, saved_epoch, saved_base_seq,
                        static_cast<int32_t>(record_count),
                        base_offset);
                }
                
                // Update metrics: track messages produced and bytes
                if (monitoring_manager_ && monitoring_manager_->metricsCollector()) {
                    monitoring_manager_->metricsCollector()->incrementMessagesProduced(record_count);
                    size_t batch_bytes = partition_data.record_batch.size();
                    monitoring_manager_->metricsCollector()->incrementBytesIn(batch_bytes);
                }
                
                // Update high watermark
                replica_manager_->updateHighWatermark(tp, log->logEndOffset());
                
                // Handle acks=-1: wait for ISR replicas to acknowledge
                if (acks == -1) {
                    // Get current ISR for this partition
                    auto isr = replica_manager_->getISR(tp);
                    
                    // In single-node mode, ISR only contains this broker, so no waiting needed
                    // In multi-broker mode, we would:
                    // 1. Track which replicas in ISR have acknowledged
                    // 2. Wait for all ISR replicas to fetch up to base_offset + record_count
                    // 3. Timeout if not all acknowledge within request.timeout.ms (default 30s)
                    // 4. Return REQUEST_TIMED_OUT if timeout occurs
                    
                    // For now (single-node or leader-only), acks=-1 behaves like acks=1
                    if (isr.size() > 1) {
                        // TODO: Implement ISR wait logic for multi-broker
                        // This would involve:
                        // - Tracking follower fetch offsets
                        // - Waiting with timeout
                        // - Checking ISR membership
                        Logger::warn("acks=-1 requested but ISR wait not yet implemented for multi-broker");
                    }
                }
                
                partition_response.base_offset = base_offset;
                partition_response.log_start_offset = log->logStartOffset();
                partition_response.log_append_time = now_ms();
                partition_response.error_code = ErrorCode::NONE;
            } catch (const StorageException& ex) {
                Logger::error("Storage error while appending to {}-{}: {}", topic_data.topic,
                              partition_data.partition, ex.what());
                partition_response.error_code = ex.code();
                has_error = true;
            } catch (const std::exception& ex) {
                const auto& raw = partition_data.record_batch;
                std::string hex_dump;
                const size_t dump_len = std::min<size_t>(raw.size(), 24);
                hex_dump.reserve(dump_len * 3);
                for (size_t i = 0; i < dump_len; ++i) {
                    if (i > 0) {
                        hex_dump.push_back(' ');
                    }
                    char buf[4];
                    std::snprintf(buf, sizeof(buf), "%02x",
                                  static_cast<unsigned int>(raw[i]));
                    hex_dump.append(buf);
                }
                Logger::warn(
                    "Failed to append produce data for {}-{}: {} ({} bytes, first {} bytes: {})",
                    topic_data.topic, partition_data.partition, ex.what(), raw.size(), dump_len,
                    hex_dump.empty() ? "<none>" : hex_dump.c_str());
                partition_response.error_code = ErrorCode::CORRUPT_MESSAGE;
                has_error = true;
            }

            topic_response.partitions.push_back(partition_response);
        }

        response.addTopic(topic_response);
    }

    const int16_t version =
        std::clamp<int16_t>(context.header.apiVersion(), 0, kProduceMaxVersion);
    RequestDispatcher::HandlerResult result;
    result.suppress_response = (acks == 0 && !has_error);
    if (!result.suppress_response) {
        result.payload = encodeResponse(context, [&](Buffer& buffer) {
            response.encode(buffer, version);
        });
    }
    
    // Record produce latency
    if (monitoring_manager_ && monitoring_manager_->metricsCollector()) {
        auto end_time = std::chrono::steady_clock::now();
        auto duration_ms = std::chrono::duration<double, std::milli>(end_time - start_time).count();
        monitoring_manager_->metricsCollector()->recordProduceLatency(duration_ms);
        monitoring_manager_->metricsCollector()->incrementRequestsTotal("Produce");
        if (has_error) {
            monitoring_manager_->metricsCollector()->incrementRequestErrors("Produce");
        }
    }
    
    return result;
}

Buffer KawasanBroker::buildProduceError(
    const RequestDispatcher::RequestContext& context, ErrorCode code,
    int16_t response_version) const {
    protocol::ProduceResponse response;
    response.setThrottleTimeMs(0);
    protocol::ProduceTopicResponse topic_response;
    topic_response.topic = "__kawasan_error__";
    protocol::ProducePartitionResponse partition_response;
    partition_response.partition = -1;
    partition_response.error_code = code;
    partition_response.base_offset = 0;
    partition_response.log_append_time = -1;
    partition_response.log_start_offset = 0;
    topic_response.partitions.push_back(partition_response);
    response.addTopic(topic_response);

    const int16_t version =
        std::clamp<int16_t>(response_version, 0, kProduceMaxVersion);
    return encodeResponse(context, [&](Buffer& buffer) {
        response.encode(buffer, version);
    });
}

RequestDispatcher::HandlerResult KawasanBroker::handleFetch(
    RequestDispatcher::RequestContext& context) {
    const auto start_time = std::chrono::steady_clock::now();
    
    protocol::FetchRequest request;
    request.decode(context.payload, context.header.apiVersion());

    const int32_t max_wait_ms = std::max<int32_t>(0, request.maxWaitMs());
    const size_t min_bytes = static_cast<size_t>(std::max<int32_t>(0, request.minBytes()));
    const int32_t request_max_bytes_raw =
        request.maxBytes() > 0 ? request.maxBytes() : 50 * 1024 * 1024;
    const size_t request_max_bytes = static_cast<size_t>(request_max_bytes_raw);

    const auto deadline = start_time + std::chrono::milliseconds(max_wait_ms);

    const auto make_legacy_messageset =
        [](const std::vector<storage::RecordBatch>& batches) -> std::vector<uint8_t> {
        Buffer message_set;
        for (const auto& batch : batches) {
            const Offset base = batch.baseOffset();
            const auto& records = batch.records();
            for (size_t idx = 0; idx < records.size(); ++idx) {
                const auto& record = records[idx];

                Buffer message_body;
                message_body.writeInt8(1);  // magic v1 to retain timestamps
                message_body.writeInt8(0);  // attributes (no compression)
                message_body.writeInt64(record.timestamp);

                if (record.key && !record.key->empty()) {
                    message_body.writeInt32(static_cast<int32_t>(record.key->size()));
                    message_body.writeBytes(record.key->data(), record.key->size());
                } else {
                    message_body.writeInt32(-1);
                }

                if (record.value && !record.value->empty()) {
                    message_body.writeInt32(static_cast<int32_t>(record.value->size()));
                    message_body.writeBytes(record.value->data(), record.value->size());
                } else {
                    message_body.writeInt32(-1);
                }

                auto payload = message_body.takeVector();
                const uint32_t crc = crc32(0L, payload.data(), payload.size());

                Buffer entry;
                entry.writeInt64(base + static_cast<Offset>(idx));
                entry.writeInt32(static_cast<int32_t>(sizeof(int32_t) + payload.size()));
                entry.writeInt32(static_cast<int32_t>(crc));
                if (!payload.empty()) {
                    entry.writeBytes(payload.data(), payload.size());
                }

                auto entry_bytes = entry.takeVector();
                if (!entry_bytes.empty()) {
                    message_set.writeBytes(entry_bytes.data(), entry_bytes.size());
                }
            }
        }
        return message_set.takeVector();
    };

    const auto serialize_batches =
        [&](const std::vector<storage::RecordBatch>& batches) -> std::vector<uint8_t> {
        if (context.header.apiVersion() <= 3) {
            return make_legacy_messageset(batches);
        }

        std::vector<uint8_t> payload;
        for (const auto& batch : batches) {
            auto bytes = batch.serialize();
            payload.insert(payload.end(), bytes.begin(), bytes.end());
        }
        return payload;
    };

    auto build_response = [&](protocol::FetchResponse& response) -> size_t {
        size_t total_bytes = 0;
        size_t remaining_request_bytes = request_max_bytes;

        for (const auto& topic : request.topics()) {
            protocol::FetchTopicResponse topic_response;
            topic_response.topic = topic.topic;
            topic_response.topic_id = topic.topic_id;

            // Phase 1.4: v13 sends topic_id instead of name. Resolve here
            // so the rest of the handler can use the name uniformly.
            std::string lookup_name = topic.topic;
            if (topic.has_topic_id && metadata_controller_) {
                for (const auto& m : metadata_controller_->describeTopics({})) {
                    if (m.topic_id == topic.topic_id) {
                        lookup_name = m.name;
                        topic_response.topic = m.name;
                        break;
                    }
                }
            }

            auto [topic_metadata_opt, topic_error] =
                getTopicMetadata(lookup_name, auto_create_topics_enabled_);

            for (const auto& partition : topic.partitions) {
                protocol::FetchPartitionResponse partition_response;
                partition_response.partition = partition.partition;
                partition_response.error_code = ErrorCode::NONE;
                partition_response.high_watermark = 0;
                partition_response.last_stable_offset = 0;
                partition_response.log_start_offset = 0;

                auto finalize_partition = [&](size_t bytes) {
                    total_bytes += bytes;
                    topic_response.partitions.push_back(std::move(partition_response));
                };

                if (!topic_metadata_opt) {
                    partition_response.error_code = topic_error;
                    finalize_partition(0);
                    continue;
                }

                const auto& topic_metadata = *topic_metadata_opt;
                auto partition_it = std::find_if(
                    topic_metadata.partitions.begin(), topic_metadata.partitions.end(),
                    [&](const PartitionMetadata& metadata) {
                        return metadata.partition == partition.partition;
                    });

                if (partition_it == topic_metadata.partitions.end()) {
                    partition_response.error_code = ErrorCode::UNKNOWN_TOPIC_OR_PARTITION;
                    finalize_partition(0);
                    continue;
                }

                if (partition_it->leader != broker_id_) {
                    partition_response.error_code = ErrorCode::NOT_LEADER_FOR_PARTITION;
                    finalize_partition(0);
                    continue;
                }

                if (!log_manager_) {
                    partition_response.error_code = ErrorCode::KAFKA_STORAGE_ERROR;
                    finalize_partition(0);
                    continue;
                }

                storage::Log* log = nullptr;
                try {
                    log = log_manager_->getOrCreateLog(lookup_name, partition.partition);
                } catch (const StorageException& ex) {
                    Logger::error("Storage error fetching {}-{}: {}", lookup_name,
                                  partition.partition, ex.what());
                    partition_response.error_code = ex.code();
                    finalize_partition(0);
                    continue;
                } catch (const std::exception& ex) {
                    Logger::error("Unexpected error fetching {}-{}: {}", lookup_name,
                                  partition.partition, ex.what());
                    partition_response.error_code = ErrorCode::KAFKA_STORAGE_ERROR;
                    finalize_partition(0);
                    continue;
                }

                if (!log) {
                    partition_response.error_code = ErrorCode::KAFKA_STORAGE_ERROR;
                    finalize_partition(0);
                    continue;
                }

                // Register replica with ReplicaManager if not already registered
                TopicPartition tp{lookup_name, partition.partition};
                if (!replica_manager_->isLeader(tp)) {
                    // Create a non-owning shared_ptr wrapper
                    std::shared_ptr<storage::Log> log_ptr(log, [](storage::Log*){});
                    replica_manager_->addReplica(tp, log_ptr);
                }

                const Offset log_start = log->logStartOffset();
                const Offset log_end = log->logEndOffset();
                partition_response.log_start_offset = log_start;
                
                // Get high watermark from ReplicaManager (falls back to log's HW if not found)
                auto hw_opt = replica_manager_->getHighWatermark(tp);
                const Offset high_watermark = hw_opt.value_or(log->highWatermark());
                partition_response.high_watermark = high_watermark;

                // Phase EX-10: LSO = min(first_offset across in-flight
                // transactional batches) or HWM if none. read_committed
                // consumers will only see records up to this offset.
                const Offset lso = isolation_tracker_
                    ? isolation_tracker_->lastStableOffset(
                        lookup_name, partition.partition, high_watermark)
                    : high_watermark;
                partition_response.last_stable_offset = lso;

                // Phase EX-10: populate aborted_transactions list. The
                // consumer uses this with isolation_level=READ_COMMITTED
                // to filter out records from aborted producers in the
                // (fetch_offset, last_stable_offset] window.
                if (isolation_tracker_ && context.header.apiVersion() >= 4) {
                    auto aborts = isolation_tracker_->abortedTransactions(
                        lookup_name, partition.partition, partition.fetch_offset);
                    for (const auto& a : aborts) {
                        protocol::FetchAbortedTransaction at;
                        at.producer_id = a.producer_id;
                        at.first_offset = a.first_offset;
                        partition_response.aborted_transactions.push_back(at);
                    }
                }

                // Allow fetch_offset == log_end (returns empty data), reject only if strictly beyond
                if (partition.fetch_offset < log_start || partition.fetch_offset > log_end) {
                    Logger::warn("Fetch offset {} out of range for {}-{} (log_start={}, log_end={})",
                                 partition.fetch_offset, lookup_name, partition.partition,
                                 log_start, log_end);
                    partition_response.error_code = ErrorCode::OFFSET_OUT_OF_RANGE;
                    finalize_partition(0);
                    continue;
                }

                size_t effective_cap = remaining_request_bytes;
                if (partition.partition_max_bytes > 0) {
                    effective_cap = std::min(
                        effective_cap, static_cast<size_t>(partition.partition_max_bytes));
                }

                if (effective_cap == 0) {
                    finalize_partition(0);
                    continue;
                }

                try {
                    // Phase 5.1: raw-bytes fetch path for v4+ (RecordBatch
                    // format). For older v0–v3 requests we still need the
                    // legacy MessageSet conversion which requires
                    // deserialized records. The check sidesteps
                    // double-CRC because RocksDB stores already-CRC'd
                    // batches from the produce path.
                    //
                    // Phase EX-10: for read_committed isolation, take
                    // the deserialized path so we can filter batches
                    // whose base_offset is past LSO (those are in-flight
                    // transactional records the consumer must not see).
                    const bool read_committed = request.isolationLevel() == 1;
                    const bool use_raw =
                        context.header.apiVersion() >= 4 && !read_committed;
                    std::vector<uint8_t> serialized;
                    size_t total_messages_for_metrics = 0;
                    if (use_raw) {
                        serialized = log->readRaw(partition.fetch_offset, effective_cap);
                        // We don't have the record count without a parse;
                        // omit the per-batch breakdown for metrics. A
                        // future polish should track record counts in
                        // the raw path via a header-only parse.
                    } else {
                        auto batches = log->read(partition.fetch_offset, effective_cap);

                        // Phase EX-10: read_committed filtering. Drop
                        // batches whose base_offset >= LSO (those records
                        // belong to in-flight or aborted transactions).
                        // We also surface aborted-transactions via the
                        // response field — kafka-clients then drops
                        // records by producer_id, but we additionally
                        // filter aborted *batches* by producer_id +
                        // first_offset for an extra safety net.
                        if (read_committed) {
                            std::vector<storage::RecordBatch> kept;
                            kept.reserve(batches.size());
                            for (auto& b : batches) {
                                if (b.baseOffset() >= lso) continue;
                                kept.push_back(std::move(b));
                            }
                            batches = std::move(kept);
                        }

                        serialized = serialize_batches(batches);
                        for (const auto& batch : batches) {
                            total_messages_for_metrics += batch.records().size();
                        }
                    }
                    if (!serialized.empty()) {
                        const size_t batch_bytes = serialized.size();
                        if (remaining_request_bytes <= batch_bytes) {
                            remaining_request_bytes = 0;
                        } else {
                            remaining_request_bytes -= batch_bytes;
                        }

                        if (monitoring_manager_ && monitoring_manager_->metricsCollector()) {
                            monitoring_manager_->metricsCollector()->incrementMessagesConsumed(total_messages_for_metrics);
                            monitoring_manager_->metricsCollector()->incrementBytesOut(batch_bytes);
                        }

                        partition_response.record_batches = std::move(serialized);
                        finalize_partition(batch_bytes);
                        continue;
                    }
                } catch (const StorageException& ex) {
                    Logger::error("Storage error reading {}-{}: {}", lookup_name,
                                  partition.partition, ex.what());
                    partition_response.error_code = ex.code();
                    finalize_partition(0);
                    continue;
                } catch (const std::exception& ex) {
                    Logger::error("Unexpected error reading {}-{}: {}", lookup_name,
                                  partition.partition, ex.what());
                    partition_response.error_code = ErrorCode::KAFKA_STORAGE_ERROR;
                    finalize_partition(0);
                    continue;
                }

                finalize_partition(0);
            }

            response.addTopic(topic_response);
        }

        return total_bytes;
    };

    // Phase 1.4: validate the fetch session reference. For v7+ the
    // client supplies (session_id, session_epoch); we either allocate a
    // new session or validate the existing one. The response echoes
    // the (potentially new) session_id and epoch.
    int32_t resp_session_id = 0;
    int16_t session_error = 0;
    if (context.header.apiVersion() >= 7 && fetch_session_manager_) {
        auto v = fetch_session_manager_->validate(request.sessionId(),
                                                  request.sessionEpoch());
        if (v.is_error) {
            session_error = v.error;
        } else {
            resp_session_id = v.session_id;
            if (v.is_new_session) {
                Logger::info("Fetch: allocated session_id={} for client {}",
                             v.session_id, context.peer_identity);
            }
        }
    }

    protocol::FetchResponse response;
    size_t bytes_returned = 0;
    if (session_error != 0) {
        response.setErrorCode(static_cast<ErrorCode>(session_error));
        response.setThrottleTimeMs(0);
        const int16_t version =
            std::clamp<int16_t>(context.header.apiVersion(), 0, kFetchMaxVersion);
        RequestDispatcher::HandlerResult result;
        result.payload = encodeResponse(context, [&](Buffer& buffer) {
            response.encode(buffer, version);
        });
        result.close_connection = false;
        return result;
    }
    while (true) {
        response = protocol::FetchResponse();
        response.setThrottleTimeMs(0);
        response.setSessionId(resp_session_id);
        bytes_returned = build_response(response);

        if (bytes_returned >= min_bytes || min_bytes == 0) {
            break;
        }
        if (max_wait_ms == 0) {
            break;
        }
        if (std::chrono::steady_clock::now() >= deadline) {
            break;
        }
        // Sleep 1ms for better balance between CPU usage and latency
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    const auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                std::chrono::steady_clock::now() - start_time)
                                .count();
    response.setThrottleTimeMs(static_cast<int32_t>(
        std::min<int64_t>(elapsed_ms, std::numeric_limits<int32_t>::max())));

    const int16_t version =
        std::clamp<int16_t>(context.header.apiVersion(), 0, kFetchMaxVersion);
    RequestDispatcher::HandlerResult result;
    result.payload = encodeResponse(context, [&](Buffer& buffer) {
        response.encode(buffer, version);
    });
    
    // Record fetch latency and request metrics
    if (monitoring_manager_ && monitoring_manager_->metricsCollector()) {
        auto end_time = std::chrono::steady_clock::now();
        auto duration_ms = std::chrono::duration<double, std::milli>(end_time - start_time).count();
        monitoring_manager_->metricsCollector()->recordFetchLatency(duration_ms);
        monitoring_manager_->metricsCollector()->incrementRequestsTotal("Fetch");
    }
    
    return result;
}

Buffer KawasanBroker::buildFetchError(
    const RequestDispatcher::RequestContext& context, ErrorCode code,
    int16_t response_version) const {
    protocol::FetchResponse response;
    response.setThrottleTimeMs(0);
    protocol::FetchTopicResponse topic_response;
    topic_response.topic = "__kawasan_error__";
    protocol::FetchPartitionResponse partition_response;
    partition_response.partition = -1;
    partition_response.error_code = code;
    partition_response.high_watermark = 0;
    partition_response.last_stable_offset = 0;
    partition_response.log_start_offset = 0;
    partition_response.record_batches.clear();
    topic_response.partitions.push_back(partition_response);
    response.addTopic(topic_response);

    const int16_t version =
        std::clamp<int16_t>(response_version, 0, kFetchMaxVersion);
    return encodeResponse(context, [&](Buffer& buffer) {
        response.encode(buffer, version);
    });
}

RequestDispatcher::HandlerResult KawasanBroker::handleListOffsets(
    RequestDispatcher::RequestContext& context) {
    protocol::ListOffsetsRequest request;
    request.decode(context.payload, context.header.apiVersion());

    Logger::debug("ListOffsets v{}: replica_id={}, {} topics",
                  context.header.apiVersion(), request.replicaId(),
                  request.topics().size());

    protocol::ListOffsetsResponse response;
    response.setThrottleTimeMs(0);

    for (const auto& topic : request.topics()) {
        protocol::ListOffsetsTopicResponse topic_response;
        topic_response.topic = topic.topic;

        for (const auto& partition : topic.partitions) {
            protocol::ListOffsetsPartitionResponse partition_response;
            partition_response.partition = partition.partition;
            partition_response.error_code = ErrorCode::NONE;
            partition_response.timestamp = -1;
            partition_response.offset = -1;
            partition_response.leader_epoch = -1;

            // Get the log for this partition
            auto* log = log_manager_->getLog(topic.topic, partition.partition);
            if (!log) {
                // Partition doesn't exist
                partition_response.error_code = ErrorCode::UNKNOWN_TOPIC_OR_PARTITION;
                topic_response.partitions.push_back(partition_response);
                continue;
            }

            try {
                // Handle special timestamp values per Kafka protocol:
                // -1 = LATEST (end of log, high watermark)
                // -2 = EARLIEST (start of log)
                if (partition.timestamp == -1) {
                    // Latest offset (high watermark / end of log)
                    partition_response.offset = log->highWatermark();
                    partition_response.timestamp = partition.timestamp;
                } else if (partition.timestamp == -2) {
                    // Earliest offset (log start offset)
                    partition_response.offset = log->logStartOffset();
                    partition_response.timestamp = partition.timestamp;
                } else if (partition.timestamp == -3) {
                    // Phase 1.5 / v7: MAX_TIMESTAMP — offset of the record
                    // with the highest timestamp. Scan all batches and
                    // pick the one whose max_timestamp is greatest. We
                    // return its base_offset since a per-record offset
                    // would require per-record timestamp tracking.
                    Offset best_offset = log->logStartOffset();
                    int64_t best_ts = INT64_MIN;
                    try {
                        Offset cur = log->logStartOffset();
                        const Offset end = log->logEndOffset();
                        while (cur < end) {
                            auto batches = log->read(cur, /*max_bytes=*/64 * 1024);
                            if (batches.empty()) break;
                            Offset next = cur;
                            for (const auto& batch : batches) {
                                if (batch.maxTimestamp() > best_ts) {
                                    best_ts = batch.maxTimestamp();
                                    best_offset = batch.baseOffset();
                                }
                                next = batch.baseOffset() +
                                       static_cast<Offset>(batch.records().size());
                            }
                            if (next <= cur) break;
                            cur = next;
                        }
                    } catch (...) {
                        // Best-effort scan; partial answer is OK.
                    }
                    partition_response.offset = best_offset;
                    partition_response.timestamp = best_ts == INT64_MIN ? -1 : best_ts;
                } else {
                    // Phase 1.5: timestamp-based offset lookup via batch
                    // scan. Returns the first batch whose first_timestamp
                    // is >= the requested timestamp, or log end if none
                    // qualifies. For small logs this is O(n) over batches
                    // — a real timestamp index (.timeindex file) is a
                    // future storage task.
                    Offset found_offset = log->logEndOffset();
                    int64_t found_ts = -1;
                    try {
                        Offset cur = log->logStartOffset();
                        const Offset end = log->logEndOffset();
                        while (cur < end) {
                            auto batches = log->read(cur, /*max_bytes=*/64 * 1024);
                            if (batches.empty()) break;
                            Offset next = cur;
                            bool done = false;
                            for (const auto& batch : batches) {
                                if (batch.firstTimestamp() >= partition.timestamp) {
                                    found_offset = batch.baseOffset();
                                    found_ts = batch.firstTimestamp();
                                    done = true;
                                    break;
                                }
                                next = batch.baseOffset() +
                                       static_cast<Offset>(batch.records().size());
                            }
                            if (done) break;
                            if (next <= cur) break;
                            cur = next;
                        }
                    } catch (...) {
                        // Best-effort scan.
                    }
                    partition_response.offset = found_offset;
                    partition_response.timestamp = found_ts;
                }

                // For v0, populate old_style_offsets array
                if (context.header.apiVersion() == 0) {
                    partition_response.old_style_offsets.push_back(partition_response.offset);
                }

                partition_response.error_code = ErrorCode::NONE;
            } catch (const std::exception& ex) {
                Logger::error("Error getting offsets for {}-{}: {}", 
                              topic.topic, partition.partition, ex.what());
                partition_response.error_code = ErrorCode::KAFKA_STORAGE_ERROR;
            }

            topic_response.partitions.push_back(partition_response);
        }

        response.addTopic(topic_response);
    }

    const int16_t version =
        std::clamp<int16_t>(context.header.apiVersion(), 0, kListOffsetsMaxVersion);

    Logger::debug("ListOffsets response: {} topics",
                  response.topics().size());
    for (const auto& t : response.topics()) {
        for (const auto& p : t.partitions) {
            Logger::debug("  {}-{}: offset={}, error={}",
                          t.topic, p.partition, p.offset,
                          static_cast<int16_t>(p.error_code));
        }
    }

    RequestDispatcher::HandlerResult result;
    result.payload = encodeResponse(context, [&](Buffer& buffer) {
        response.encode(buffer, version);
    });
    return result;
}

Buffer KawasanBroker::buildListOffsetsError(
    const RequestDispatcher::RequestContext& context, ErrorCode code,
    int16_t response_version) const {
    protocol::ListOffsetsResponse response;
    response.setThrottleTimeMs(0);
    protocol::ListOffsetsTopicResponse topic_response;
    topic_response.topic = "__kawasan_error__";
    protocol::ListOffsetsPartitionResponse partition_response;
    partition_response.partition = -1;
    partition_response.error_code = code;
    partition_response.timestamp = -1;
    partition_response.offset = -1;
    partition_response.leader_epoch = -1;
    topic_response.partitions.push_back(partition_response);
    response.addTopic(topic_response);

    const int16_t version =
        std::clamp<int16_t>(response_version, 0, kListOffsetsMaxVersion);
    return encodeResponse(context, [&](Buffer& buffer) {
        response.encode(buffer, version);
    });
}

RequestDispatcher::HandlerResult KawasanBroker::handleFindCoordinator(
    RequestDispatcher::RequestContext& context) {
    protocol::FindCoordinatorRequest request;
    request.decode(context.payload, context.header.apiVersion());

    Logger::info("FindCoordinator request for {} key(s) (first: {})",
                 request.keys().size(), request.key());

    // In a single-broker setup, this broker is always the coordinator.
    protocol::FindCoordinatorResponse response;
    response.setThrottleTimeMs(0);
    response.setErrorCode(ErrorCode::NONE);
    response.setNodeId(broker_id_);
    response.setHost(advertised_host_);
    response.setPort(port_);

    // Phase 1.6: v4 returns one Coordinator entry per requested key.
    if (context.header.apiVersion() >= 4) {
        for (const auto& key : request.keys()) {
            protocol::FindCoordinatorResponse::Coordinator c;
            c.key = key;
            c.node_id = broker_id_;
            c.host = advertised_host_;
            c.port = port_;
            c.error_code = ErrorCode::NONE;
            response.addCoordinator(std::move(c));
        }
    }

    const int16_t version =
        std::clamp<int16_t>(context.header.apiVersion(), 0, kFindCoordinatorMaxVersion);
    RequestDispatcher::HandlerResult result;
    result.payload = encodeResponse(context, [&](Buffer& buffer) {
        response.encode(buffer, version);
    });
    return result;
}

Buffer KawasanBroker::buildFindCoordinatorError(
    const RequestDispatcher::RequestContext& context, ErrorCode code,
    int16_t response_version) const {
    protocol::FindCoordinatorResponse response;
    response.setThrottleTimeMs(0);
    response.setErrorCode(code);
    response.setErrorMessage(KawasanException::toString(code));
    response.setNodeId(-1);
    response.setHost("");
    response.setPort(0);

    const int16_t version =
        std::clamp<int16_t>(response_version, 0, kFindCoordinatorMaxVersion);
    return encodeResponse(context, [&](Buffer& buffer) {
        response.encode(buffer, version);
    });
}

Buffer KawasanBroker::handleJoinGroup(RequestDispatcher::RequestContext& context) {
    protocol::JoinGroupRequest request;
    request.decode(context.payload, context.header.apiVersion());

    // 0A.10: forward the real client identity so DescribeGroups returns
    // something useful instead of "unknown".
    const auto result = group_coordinator_->handleJoinGroup(
        request, context.header.clientId(), context.peer_identity);

    protocol::JoinGroupResponse response;
    response.setErrorCode(result.error);
    response.setGenerationId(result.generation_id);
    response.setGroupProtocol(result.protocol_name);  // v0–v6 single field
    // EX-12: v7+ split group_protocol into protocol_type + protocol_name
    // (both NULLABLE). Kafka Connect's leader-side performAssignment()
    // calls ConnectProtocolCompatibility.fromProtocol(protocol_name),
    // which throws on null — so a Connect worker that receives a null
    // protocol_name never advances to SyncGroup. Echo the real values;
    // leave them null only on error paths (handled by buildJoinGroupError).
    if (!result.protocol_type.empty()) {
        response.setProtocolType(result.protocol_type);
    }
    if (!result.protocol_name.empty()) {
        response.setProtocolName(result.protocol_name);
    }
    response.setLeaderId(result.leader_id);
    response.setMemberId(result.member_id);
    response.setMembers(result.members);

    const int16_t version =
        std::clamp<int16_t>(context.header.apiVersion(), 0, kJoinGroupMaxVersion);
    return encodeResponse(context, [&](Buffer& buffer) {
        response.encode(buffer, version);
    });
}

Buffer KawasanBroker::buildJoinGroupError(
    const RequestDispatcher::RequestContext& context, ErrorCode code,
    int16_t response_version) const {
    protocol::JoinGroupResponse response;
    response.setErrorCode(code);
    response.setGenerationId(0);
    response.setGroupProtocol("");
    response.setLeaderId("");
    response.setMemberId("");
    response.setMembers({});

    const int16_t version =
        std::clamp<int16_t>(response_version, 0, kJoinGroupMaxVersion);
    return encodeResponse(context, [&](Buffer& buffer) {
        response.encode(buffer, version);
    });
}

Buffer KawasanBroker::handleSyncGroup(RequestDispatcher::RequestContext& context) {
    protocol::SyncGroupRequest request;
    request.decode(context.payload, context.header.apiVersion());

    Logger::info("SyncGroup request: group='{}' generation={} member='{}' {} assignments",
                 request.groupId(), request.generationId(), request.memberId(),
                 request.assignments().size());

    const auto result = group_coordinator_->handleSyncGroup(request);

    Logger::info("SyncGroup response: error={} assignment_size={}",
                 static_cast<int>(result.error), result.assignment.size());

    protocol::SyncGroupResponse response;
    response.setErrorCode(result.error);
    response.setAssignment(result.assignment);
    // EX-12: SyncGroup v5+ echoes protocol_type/protocol_name (KIP-559).
    if (!result.protocol_type.empty()) {
        response.setProtocolType(result.protocol_type);
    }
    if (!result.protocol_name.empty()) {
        response.setProtocolName(result.protocol_name);
    }

    const int16_t version =
        std::clamp<int16_t>(context.header.apiVersion(), 0, kSyncGroupMaxVersion);
    return encodeResponse(context, [&](Buffer& buffer) {
        response.encode(buffer, version);
    });
}

Buffer KawasanBroker::buildSyncGroupError(
    const RequestDispatcher::RequestContext& context, ErrorCode code,
    int16_t response_version) const {
    protocol::SyncGroupResponse response;
    response.setErrorCode(code);
    response.setAssignment({});

    const int16_t version =
        std::clamp<int16_t>(response_version, 0, kSyncGroupMaxVersion);
    return encodeResponse(context, [&](Buffer& buffer) {
        response.encode(buffer, version);
    });
}

Buffer KawasanBroker::handleHeartbeat(RequestDispatcher::RequestContext& context) {
    protocol::HeartbeatRequest request;
    request.decode(context.payload, context.header.apiVersion());

    const ErrorCode error = group_coordinator_->handleHeartbeat(request);

    protocol::HeartbeatResponse response;
    response.setErrorCode(error);

    const int16_t version =
        std::clamp<int16_t>(context.header.apiVersion(), 0, kHeartbeatMaxVersion);
    return encodeResponse(context, [&](Buffer& buffer) {
        response.encode(buffer, version);
    });
}

Buffer KawasanBroker::buildHeartbeatError(
    const RequestDispatcher::RequestContext& context, ErrorCode code,
    int16_t response_version) const {
    protocol::HeartbeatResponse response;
    response.setErrorCode(code);

    const int16_t version =
        std::clamp<int16_t>(response_version, 0, kHeartbeatMaxVersion);
    return encodeResponse(context, [&](Buffer& buffer) {
        response.encode(buffer, version);
    });
}

Buffer KawasanBroker::handleLeaveGroup(RequestDispatcher::RequestContext& context) {
    protocol::LeaveGroupRequest request;
    request.decode(context.payload, context.header.apiVersion());

    // Phase 1.10: v3+ accepts a batch of members. Call coordinator once per
    // member; aggregate per-member results into the v3+ response shape. For
    // v0–v2 there is exactly one member and the top-level error_code holds it.
    protocol::LeaveGroupResponse response;
    ErrorCode top_level_error = ErrorCode::NONE;
    for (const auto& member : request.members()) {
        const ErrorCode err =
            group_coordinator_->handleLeaveGroup(request.groupId(), member.member_id);
        if (err != ErrorCode::NONE && top_level_error == ErrorCode::NONE) {
            top_level_error = err;
        }
        protocol::LeaveGroupResponse::MemberResult mr;
        mr.member_id = member.member_id;
        mr.group_instance_id = member.group_instance_id;
        mr.error_code = err;
        response.addMember(std::move(mr));
    }
    response.setErrorCode(top_level_error);

    const int16_t version =
        std::clamp<int16_t>(context.header.apiVersion(), 0, kLeaveGroupMaxVersion);
    return encodeResponse(context, [&](Buffer& buffer) {
        response.encode(buffer, version);
    });
}

Buffer KawasanBroker::buildLeaveGroupError(
    const RequestDispatcher::RequestContext& context, ErrorCode code,
    int16_t response_version) const {
    protocol::LeaveGroupResponse response;
    response.setErrorCode(code);

    const int16_t version =
        std::clamp<int16_t>(response_version, 0, kLeaveGroupMaxVersion);
    return encodeResponse(context, [&](Buffer& buffer) {
        response.encode(buffer, version);
    });
}

Buffer KawasanBroker::handleOffsetCommit(RequestDispatcher::RequestContext& context) {
    protocol::OffsetCommitRequest request;
    request.decode(context.payload, context.header.apiVersion());

    Logger::info("OffsetCommit request: group='{}' generation={} member='{}' {} topics",
                 request.groupId(), request.generationId(), request.memberId(), request.topics().size());

    ErrorCode overall_error = ErrorCode::NONE;
    const auto topics = group_coordinator_->handleOffsetCommit(request, overall_error);

    Logger::info("OffsetCommit response: overall_error={} {} topics",
                 static_cast<int>(overall_error), topics.size());

    // Phase 3.1: also append a record to the __consumer_offsets topic so
    // `kcat -t __consumer_offsets -C` and similar tooling see the commit
    // stream. **Hash-route** the partition by group_id using the same
    // algorithm Kafka uses internally (Java's `String.hashCode()` mod 50).
    // That matches what `kafka-consumer-groups.sh --describe` expects, so
    // a Kawasan deployment is observably identical to Kafka for offset
    // browsing.
    if (overall_error == ErrorCode::NONE && log_manager_ != nullptr) {
        // Java String.hashCode():  for each char c, h = 31*h + c. We treat
        // the group_id as Latin-1 / ASCII bytes (matching kafka-clients).
        int32_t h = 0;
        for (unsigned char c : request.groupId()) {
            h = 31 * h + static_cast<int32_t>(c);
        }
        const int32_t target_partition =
            static_cast<int32_t>(static_cast<uint32_t>(h) % 50u);
        auto* offsets_log = log_manager_->getLog("__consumer_offsets", target_partition);
        if (offsets_log != nullptr) {
            std::vector<Record> commit_records;
            const auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                    std::chrono::system_clock::now().time_since_epoch())
                                    .count();
            for (const auto& t : request.topics()) {
                for (const auto& p : t.partitions) {
                    Record r;
                    r.timestamp = now_ms;
                    const std::string key = request.groupId() + "|" + t.topic +
                                            "|" + std::to_string(p.partition);
                    r.key = std::vector<uint8_t>(key.begin(), key.end());
                    const std::string value = std::to_string(p.offset) + "|" + p.metadata;
                    r.value = std::vector<uint8_t>(value.begin(), value.end());
                    commit_records.push_back(std::move(r));
                }
            }
            if (!commit_records.empty()) {
                try {
                    offsets_log->append(commit_records);
                } catch (const std::exception& ex) {
                    Logger::warn("Failed to append to __consumer_offsets: {}", ex.what());
                }
            }
        }
    }

    protocol::OffsetCommitResponse response;
    response.setTopics(topics);

    // Phase 1.11: respect the negotiated request version (was hardcoded to
    // the max constant which made every response encode at v8 regardless of
    // what the client requested).
    const int16_t version =
        std::clamp<int16_t>(context.header.apiVersion(), 0, kOffsetCommitMaxVersion);
    return encodeResponse(context, [&](Buffer& buffer) {
        response.encode(buffer, version);
    });
}

Buffer KawasanBroker::buildOffsetCommitError(
    const RequestDispatcher::RequestContext& context, ErrorCode code,
    int16_t response_version) const {
    protocol::OffsetCommitResponse response;
    protocol::OffsetCommitResponse::Partition partition;
    partition.error = code;
    protocol::OffsetCommitResponse::Topic topic;
    topic.partitions.push_back(partition);
    response.setTopics({topic});

    const int16_t version =
        std::clamp<int16_t>(response_version, 0, kOffsetCommitMaxVersion);
    return encodeResponse(context, [&](Buffer& buffer) {
        response.encode(buffer, version);
    });
}

Buffer KawasanBroker::handleOffsetFetch(RequestDispatcher::RequestContext& context) {
    protocol::OffsetFetchRequest request;
    request.decode(context.payload, context.header.apiVersion());

    const int16_t api_v = context.header.apiVersion();
    protocol::OffsetFetchResponse response;

    // Phase 1.12: v8+ multi-group form. Iterate over each group and
    // call into the GroupCoordinator with a per-group view, then emit
    // a per-group entry in the response.
    if (api_v >= 8 && request.isMultiGroup()) {
        for (const auto& g : request.groups()) {
            protocol::OffsetFetchRequest single;
            single.setGroupId(g.group_id);
            if (g.fetch_all_topics) {
                single.setFetchAllTopics(true);
            } else {
                single.setTopics(g.topics);
            }
            single.setRequireStable(request.requireStable());
            ErrorCode group_error = ErrorCode::NONE;
            const auto topics = group_coordinator_->handleOffsetFetch(single, group_error);
            protocol::OffsetFetchResponse::Group rg;
            rg.group_id = g.group_id;
            rg.error_code = group_error;
            rg.topics = topics;
            response.addGroup(std::move(rg));
            Logger::info("OffsetFetch v{}: group='{}' returned {} topics (error={})",
                         api_v, g.group_id, topics.size(),
                         static_cast<int>(group_error));
        }
        const int16_t version =
            std::clamp<int16_t>(api_v, 0, kOffsetFetchMaxVersion);
        return encodeResponse(context, [&](Buffer& buffer) {
            response.encode(buffer, version);
        });
    }

    // Legacy v0–v7 single-group form.
    Logger::info("OffsetFetch request for group '{}', {} topics",
                  request.groupId(), request.topics().size());

    ErrorCode overall_error = ErrorCode::NONE;
    const auto topics = group_coordinator_->handleOffsetFetch(request, overall_error);

    Logger::info("OffsetFetch response: overall_error={} {} topics",
                  static_cast<int>(overall_error), topics.size());

    response.setErrorCode(overall_error);
    response.setTopics(topics);

    const int16_t version =
        std::clamp<int16_t>(api_v, 0, kOffsetFetchMaxVersion);
    return encodeResponse(context, [&](Buffer& buffer) {
        response.encode(buffer, version);
    });
}

Buffer KawasanBroker::buildOffsetFetchError(
    const RequestDispatcher::RequestContext& context, ErrorCode code,
    int16_t response_version) const {
    protocol::OffsetFetchResponse response;
    response.setErrorCode(code);
    response.setTopics({});

    const int16_t version =
        std::clamp<int16_t>(response_version, 0, kOffsetFetchMaxVersion);
    return encodeResponse(context, [&](Buffer& buffer) {
        response.encode(buffer, version);
    });
}

std::vector<BrokerMetadata> KawasanBroker::buildBrokerMetadata() const {
    if (metadata_controller_) {
        auto brokers = metadata_controller_->brokers();
        if (!brokers.empty()) {
            return brokers;
        }
    }
    return {localBrokerMetadata()};
}

TopicMetadata KawasanBroker::buildDefaultTopicMetadata() const {
    TopicMetadata topic;
    topic.error_code = ErrorCode::NONE;
    topic.name = "kawasan-default";
    topic.is_internal = false;
    PartitionMetadata partition;
    partition.error_code = ErrorCode::NONE;
    partition.partition = 0;
    partition.leader = broker_id_;
    partition.leader_epoch = 0;
    partition.replicas = {broker_id_};
    partition.isr = {broker_id_};
    partition.offline_replicas.clear();
    topic.partitions.push_back(partition);
    return topic;
}

BrokerMetadata KawasanBroker::localBrokerMetadata() const {
    BrokerMetadata broker;
    broker.id = broker_id_;
    broker.host = advertised_host_;
    broker.port = port();
    broker.rack = std::nullopt;
    return broker;
}

std::pair<std::optional<TopicMetadata>, ErrorCode> KawasanBroker::getTopicMetadata(
    const std::string& topic_name, bool allow_auto_create) {
    if (topic_name.empty()) {
        return {std::nullopt, ErrorCode::INVALID_TOPIC_EXCEPTION};
    }
    if (!metadata_controller_) {
        return {std::nullopt, ErrorCode::BROKER_NOT_AVAILABLE};
    }

    auto topics = metadata_controller_->describeTopics({topic_name});
    if (!topics.empty() && topics.front().error_code == ErrorCode::NONE) {
        return {topics.front(), ErrorCode::NONE};
    }

    const ErrorCode lookup_error =
        topics.empty() ? ErrorCode::UNKNOWN_TOPIC_OR_PARTITION
                       : topics.front().error_code;

    if (!allow_auto_create || lookup_error != ErrorCode::UNKNOWN_TOPIC_OR_PARTITION) {
        return {std::nullopt, lookup_error};
    }

    TopicSpecification spec;
    spec.name = topic_name;
    spec.num_partitions = std::max<int32_t>(1, default_num_partitions_);
    spec.replication_factor = std::max<int16_t>(1, default_replication_factor_);

    Logger::info("Auto-creating topic '{}' with {} partition(s)", spec.name,
                 spec.num_partitions);
    auto create_result = metadata_controller_->createTopic(spec);
    if (create_result.error_code != ErrorCode::NONE &&
        create_result.error_code != ErrorCode::TOPIC_ALREADY_EXISTS) {
        Logger::warn("Auto-creation of topic '{}' failed: {}", topic_name,
                     KawasanException::toString(create_result.error_code));
        return {std::nullopt, create_result.error_code};
    }

    if (create_result.has_metadata) {
        return {create_result.topic_metadata, ErrorCode::NONE};
    }

    auto retry = metadata_controller_->describeTopics({topic_name});
    if (!retry.empty() && retry.front().error_code == ErrorCode::NONE) {
        return {retry.front(), ErrorCode::NONE};
    }

    if (create_result.error_code == ErrorCode::TOPIC_ALREADY_EXISTS) {
        return {std::nullopt, create_result.error_code};
    }

    return {std::nullopt, ErrorCode::UNKNOWN_TOPIC_OR_PARTITION};
}

Buffer KawasanBroker::handleDescribeGroups(RequestDispatcher::RequestContext& context) {
    protocol::DescribeGroupsRequest request;
    request.decode(context.payload, context.header.apiVersion());

    Logger::info("DescribeGroups request for {} group(s)", request.groups().size());

    auto groups = group_coordinator_->describeGroups(request.groups());

    protocol::DescribeGroupsResponse response;
    response.setThrottleTimeMs(0);
    response.setGroups(groups);

    const int16_t version =
        std::clamp<int16_t>(context.header.apiVersion(), 0, kDescribeGroupsMaxVersion);
    return encodeResponse(context, [&](Buffer& buffer) {
        response.encode(buffer, version);
    });
}

Buffer KawasanBroker::buildDescribeGroupsError(
    const RequestDispatcher::RequestContext& context, ErrorCode code,
    int16_t response_version) const {
    protocol::DescribeGroupsResponse response;
    response.setThrottleTimeMs(0);
    response.setGroups({});
    (void)code;  // Error code not used in error response for this API

    const int16_t version =
        std::clamp<int16_t>(response_version, 0, kDescribeGroupsMaxVersion);
    return encodeResponse(context, [&](Buffer& buffer) {
        response.encode(buffer, version);
    });
}

Buffer KawasanBroker::handleListGroups(RequestDispatcher::RequestContext& context) {
    protocol::ListGroupsRequest request;
    request.decode(context.payload, context.header.apiVersion());

    Logger::info("ListGroups request");

    auto groups = group_coordinator_->listGroups();

    protocol::ListGroupsResponse response;
    response.setErrorCode(ErrorCode::NONE);
    response.setThrottleTimeMs(0);
    response.setGroups(groups);

    const int16_t version =
        std::clamp<int16_t>(context.header.apiVersion(), 0, kListGroupsMaxVersion);
    return encodeResponse(context, [&](Buffer& buffer) {
        response.encode(buffer, version);
    });
}

Buffer KawasanBroker::buildListGroupsError(
    const RequestDispatcher::RequestContext& context, ErrorCode code,
    int16_t response_version) const {
    protocol::ListGroupsResponse response;
    response.setErrorCode(code);
    response.setThrottleTimeMs(0);
    response.setGroups({});

    const int16_t version =
        std::clamp<int16_t>(response_version, 0, kListGroupsMaxVersion);
    return encodeResponse(context, [&](Buffer& buffer) {
        response.encode(buffer, version);
    });
}

Buffer KawasanBroker::handleDescribeConfigs(RequestDispatcher::RequestContext& context) {
    protocol::DescribeConfigsRequest request;
    request.decode(context.payload, context.header.apiVersion());

    Logger::info("DescribeConfigs request for {} resource(s)", request.resources().size());

    protocol::DescribeConfigsResponse response;
    response.setThrottleTimeMs(0);

    // Process each requested resource
    for (const auto& resource : request.resources()) {
        protocol::DescribeConfigsResourceResult result;
        result.error_code = ErrorCode::NONE;
        result.resource_type = resource.resource_type;
        result.resource_name = resource.resource_name;

        // Return basic broker/topic configurations
        if (resource.resource_type == protocol::ConfigResourceType::BROKER) {
            // Broker configs
            std::vector<protocol::ConfigEntry> configs;
            
            protocol::ConfigEntry log_dir_config;
            log_dir_config.name = "log.dirs";
            log_dir_config.value = log_dir_;
            log_dir_config.read_only = false;
            log_dir_config.is_default = false;
            log_dir_config.is_sensitive = false;
            configs.push_back(log_dir_config);

            protocol::ConfigEntry num_partitions_config;
            num_partitions_config.name = "num.partitions";
            num_partitions_config.value = std::to_string(default_num_partitions_);
            num_partitions_config.read_only = false;
            num_partitions_config.is_default = true;
            num_partitions_config.is_sensitive = false;
            configs.push_back(num_partitions_config);

            protocol::ConfigEntry replication_factor_config;
            replication_factor_config.name = "default.replication.factor";
            replication_factor_config.value = std::to_string(default_replication_factor_);
            replication_factor_config.read_only = false;
            replication_factor_config.is_default = true;
            replication_factor_config.is_sensitive = false;
            configs.push_back(replication_factor_config);

            protocol::ConfigEntry auto_create_config;
            auto_create_config.name = "auto.create.topics.enable";
            auto_create_config.value = auto_create_topics_enabled_ ? "true" : "false";
            auto_create_config.read_only = false;
            auto_create_config.is_default = false;
            auto_create_config.is_sensitive = false;
            configs.push_back(auto_create_config);

            result.configs = configs;
        } else if (resource.resource_type == protocol::ConfigResourceType::TOPIC) {
            // Topic configs - return basic defaults
            std::vector<protocol::ConfigEntry> configs;
            
            protocol::ConfigEntry retention_config;
            retention_config.name = "retention.ms";
            retention_config.value = std::to_string(log_config_.retention_ms);
            retention_config.read_only = false;
            retention_config.is_default = true;
            retention_config.is_sensitive = false;
            configs.push_back(retention_config);

            protocol::ConfigEntry segment_size_config;
            segment_size_config.name = "segment.bytes";
            segment_size_config.value = std::to_string(log_config_.segment_size);
            segment_size_config.read_only = false;
            segment_size_config.is_default = true;
            segment_size_config.is_sensitive = false;
            configs.push_back(segment_size_config);

            result.configs = configs;
        }

        response.addResult(result);
    }

    const int16_t version = std::clamp<int16_t>(context.header.apiVersion(), 0, 4);
    return encodeResponse(context, [&](Buffer& buffer) {
        response.encode(buffer, version);
    });
}

Buffer KawasanBroker::buildDescribeConfigsError(
    const RequestDispatcher::RequestContext& context, ErrorCode code,
    int16_t response_version) const {
    protocol::DescribeConfigsResponse response;
    response.setThrottleTimeMs(0);

    protocol::DescribeConfigsResourceResult error_result;
    error_result.error_code = code;
    error_result.error_message = "Error processing DescribeConfigs request";
    response.addResult(error_result);

    const int16_t version = std::clamp<int16_t>(response_version, 0, 4);
    return encodeResponse(context, [&](Buffer& buffer) {
        response.encode(buffer, version);
    });
}

Buffer KawasanBroker::handleDescribeCluster(RequestDispatcher::RequestContext& context) {
    protocol::DescribeClusterRequest request;
    request.decode(context.payload, context.header.apiVersion());

    Logger::info("DescribeCluster request (API version {})", context.header.apiVersion());

    protocol::DescribeClusterResponse response;
    response.setThrottleTimeMs(0);
    response.setErrorCode(ErrorCode::NONE);
    
    const std::string cluster_id =
        metadata_controller_ ? metadata_controller_->clusterId() : cluster_id_;
    Logger::info("DescribeCluster: cluster_id='{}' (length={})", cluster_id, cluster_id.size());
    response.setClusterId(cluster_id);
    response.setControllerId(broker_id_);
    response.setBrokers(buildBrokerMetadata());

    // Set cluster authorized operations if requested
    if (request.includeClusterAuthorizedOperations()) {
        // Return all operations allowed (simple implementation)
        response.setClusterAuthorizedOperations(0x7FFFFFFF);
    }

    // Phase 1.17: DescribeCluster now supports v0..v1; clamp accordingly.
    const int16_t version = std::clamp<int16_t>(context.header.apiVersion(), 0, 1);
    return encodeResponse(context, [&](Buffer& buffer) {
        response.encode(buffer, version);
    });
}

Buffer KawasanBroker::buildDescribeClusterError(
    const RequestDispatcher::RequestContext& context, ErrorCode code,
    int16_t response_version) const {
    protocol::DescribeClusterResponse response;
    response.setThrottleTimeMs(0);
    response.setErrorCode(code);
    response.setErrorMessage("Error processing DescribeCluster request");

    const std::string cluster_id =
        metadata_controller_ ? metadata_controller_->clusterId() : cluster_id_;
    response.setClusterId(cluster_id);
    response.setControllerId(broker_id_);
    response.setBrokers(buildBrokerMetadata());

    const int16_t version = std::clamp<int16_t>(response_version, 0, 1);  // Phase 1.17
    return encodeResponse(context, [&](Buffer& buffer) {
        response.encode(buffer, version);
    });
}

int64_t KawasanBroker::allocateNextProducerId() {
    // Phase 1.18: synchronous load → increment → persist. Simple and correct.
    // Phase 2.1 will replace with a block-allocator (Kafka allocates 1000 IDs
    // at a time to amortize the fsync cost).
    std::lock_guard<std::mutex> lock(producer_id_mutex_);
    const std::string counter_path = metadata_dir_ + "/producer_id.counter";
    int64_t current = next_producer_id_.load();
    // If counter file exists and we haven't loaded it yet, load now.
    std::ifstream in(counter_path);
    if (in.is_open()) {
        int64_t stored = 0;
        if (in >> stored && stored >= current) {
            current = stored;
        }
        in.close();
    }
    const int64_t allocated = current;
    const int64_t next = current + 1;
    next_producer_id_.store(next);
    std::filesystem::create_directories(metadata_dir_);
    std::ofstream out(counter_path, std::ios::trunc);
    if (out.is_open()) {
        out << next;
        out.close();
    } else {
        Logger::error("Failed to persist producer_id counter to {}", counter_path);
    }
    return allocated;
}

Buffer KawasanBroker::handleInitProducerId(RequestDispatcher::RequestContext& context) {
    protocol::InitProducerIdRequest request;
    request.decode(context.payload, context.header.apiVersion());

    Logger::info("InitProducerId request from {}: transactional_id='{}' timeout={}ms",
                 context.peer_identity,
                 request.transactionalId().value_or("<null>"),
                 request.transactionTimeoutMs());

    const int64_t producer_id = allocateNextProducerId();

    protocol::InitProducerIdResponse response;
    response.setThrottleTimeMs(0);
    response.setErrorCode(ErrorCode::NONE);
    response.setProducerId(producer_id);
    response.setProducerEpoch(0);

    // Phase 4.1k/4.1l: register the transactional_id with the
    // TransactionCoordinator so subsequent DescribeTransactions /
    // ListTransactions can see it. Non-transactional InitProducerId
    // (transactional_id null) skips this — no txn ID to track.
    if (transaction_coordinator_ && request.transactionalId().has_value()
        && !request.transactionalId()->empty()) {
        transaction_coordinator_->recordInitProducerId(
            *request.transactionalId(), producer_id, /*epoch=*/0,
            request.transactionTimeoutMs());
    }

    const int16_t version = std::clamp<int16_t>(context.header.apiVersion(), 0, 4);
    return encodeResponse(context, [&](Buffer& buffer) {
        response.encode(buffer, version);
    });
}

Buffer KawasanBroker::buildInitProducerIdError(
    const RequestDispatcher::RequestContext& context, ErrorCode code,
    int16_t response_version) const {
    protocol::InitProducerIdResponse response;
    response.setThrottleTimeMs(0);
    response.setErrorCode(code);
    response.setProducerId(-1);
    response.setProducerEpoch(-1);
    const int16_t version = std::clamp<int16_t>(response_version, 0, 4);
    return encodeResponse(context, [&](Buffer& buffer) {
        response.encode(buffer, version);
    });
}

Buffer KawasanBroker::handleOffsetForLeaderEpoch(
    RequestDispatcher::RequestContext& context) {
    protocol::OffsetForLeaderEpochRequest request;
    request.decode(context.payload, context.header.apiVersion());

    protocol::OffsetForLeaderEpochResponse response;
    response.setThrottleTimeMs(0);

    for (const auto& topic : request.topics()) {
        protocol::OffsetForLeaderEpochResponse::TopicResult tres;
        tres.name = topic.name;
        for (const auto& pq : topic.partitions) {
            protocol::OffsetForLeaderEpochResponse::PartitionResult pres;
            pres.partition = pq.partition;
            // Phase 1.19: single-broker — always leader_epoch=0. If the consumer
            // requested an epoch > 0 we still answer with 0 (truncation never
            // happens for a persistent single-leader log). Return UNKNOWN
            // if the partition doesn't exist.
            auto* log =
                log_manager_ ? log_manager_->getLog(topic.name, pq.partition) : nullptr;
            if (log == nullptr) {
                pres.error_code = ErrorCode::UNKNOWN_TOPIC_OR_PARTITION;
                pres.leader_epoch = -1;
                pres.end_offset = -1;
            } else {
                pres.error_code = ErrorCode::NONE;
                pres.leader_epoch = 0;
                pres.end_offset = log->logEndOffset();
            }
            tres.partitions.push_back(pres);
        }
        response.addTopic(std::move(tres));
    }

    const int16_t version = std::clamp<int16_t>(context.header.apiVersion(), 0, 4);
    return encodeResponse(context, [&](Buffer& buffer) {
        response.encode(buffer, version);
    });
}

Buffer KawasanBroker::buildOffsetForLeaderEpochError(
    const RequestDispatcher::RequestContext& context, ErrorCode code,
    int16_t response_version) const {
    protocol::OffsetForLeaderEpochResponse response;
    response.setThrottleTimeMs(0);
    // Synthesize an error topic so the client's decoder doesn't crash.
    protocol::OffsetForLeaderEpochResponse::TopicResult tres;
    tres.name = "__kawasan_error__";
    protocol::OffsetForLeaderEpochResponse::PartitionResult pres;
    pres.partition = -1;
    pres.error_code = code;
    tres.partitions.push_back(pres);
    response.addTopic(std::move(tres));
    const int16_t version = std::clamp<int16_t>(response_version, 0, 4);
    return encodeResponse(context, [&](Buffer& buffer) {
        response.encode(buffer, version);
    });
}

Buffer KawasanBroker::handleAlterConfigs(RequestDispatcher::RequestContext& context) {
    protocol::AlterConfigsRequest request;
    request.decode(context.payload, context.header.apiVersion());

    protocol::AlterConfigsResponse response;
    response.setThrottleTimeMs(0);

    for (const auto& res : request.resources()) {
        protocol::AlterConfigsResponse::ResourceResult rr;
        rr.resource_type = res.resource_type;
        rr.resource_name = res.resource_name;
        rr.error_code = ErrorCode::NONE;

        if (res.resource_type == protocol::ConfigResourceType::TOPIC) {
            // Phase 4.1a: build the new config map from the request and push it
            // through LogManager::setTopicConfig. validate_only is honored —
            // we never persist when set.
            std::map<std::string, std::string> cfg;
            for (const auto& e : res.configs) {
                if (e.value.has_value()) cfg[e.name] = *e.value;
            }
            if (!request.validateOnly() && log_manager_) {
                log_manager_->setTopicConfig(
                    res.resource_name, storage::LogConfig::fromMap(cfg));
                Logger::info("AlterConfigs: topic '{}' updated with {} configs",
                             res.resource_name, cfg.size());
            }
        } else if (res.resource_type == protocol::ConfigResourceType::BROKER) {
            // Broker-resource alteration is accepted but not persisted to the
            // running broker — most broker configs are read-only at runtime.
            Logger::info("AlterConfigs: broker config alteration ignored (read-only)");
        } else {
            rr.error_code = ErrorCode::INVALID_REQUEST;
            rr.error_message = "Unsupported resource type for AlterConfigs";
        }
        response.addResult(std::move(rr));
    }

    const int16_t version = std::clamp<int16_t>(context.header.apiVersion(), 0, 2);
    return encodeResponse(context, [&](Buffer& buffer) {
        response.encode(buffer, version);
    });
}

Buffer KawasanBroker::buildAlterConfigsError(
    const RequestDispatcher::RequestContext& context, ErrorCode code,
    int16_t response_version) const {
    protocol::AlterConfigsResponse response;
    response.setThrottleTimeMs(0);
    protocol::AlterConfigsResponse::ResourceResult rr;
    rr.error_code = code;
    rr.resource_type = protocol::ConfigResourceType::UNKNOWN;
    response.addResult(std::move(rr));
    const int16_t version = std::clamp<int16_t>(response_version, 0, 2);
    return encodeResponse(context, [&](Buffer& buffer) {
        response.encode(buffer, version);
    });
}

Buffer KawasanBroker::handleIncrementalAlterConfigs(
    RequestDispatcher::RequestContext& context) {
    protocol::IncrementalAlterConfigsRequest request;
    request.decode(context.payload, context.header.apiVersion());

    protocol::IncrementalAlterConfigsResponse response;
    response.setThrottleTimeMs(0);

    using Op = protocol::IncrementalAlterConfigsRequest::Op;

    for (const auto& res : request.resources()) {
        protocol::IncrementalAlterConfigsResponse::ResourceResult rr;
        rr.resource_type = res.resource_type;
        rr.resource_name = res.resource_name;
        rr.error_code = ErrorCode::NONE;

        if (res.resource_type == protocol::ConfigResourceType::TOPIC && log_manager_) {
            // Phase 4.1b: start from the current effective config, then apply
            // per-key operations. APPEND/SUBTRACT honor comma-list semantics
            // (cleanup.policy can be "delete,compact").
            auto current = log_manager_->getTopicConfig(res.resource_name);
            std::map<std::string, std::string> cfg;
            std::string policy;
            if (current.cleanup_policy_delete && current.cleanup_policy_compact) {
                policy = "delete,compact";
            } else if (current.cleanup_policy_compact) {
                policy = "compact";
            } else {
                policy = "delete";
            }
            cfg["cleanup.policy"] = policy;
            cfg["retention.ms"] = std::to_string(current.retention_ms);
            cfg["retention.bytes"] = std::to_string(current.retention_bytes);
            cfg["segment.bytes"] = std::to_string(current.segment_size);
            cfg["segment.ms"] = std::to_string(current.segment_ms);

            for (const auto& c : res.configs) {
                switch (c.op) {
                    case Op::SET:
                        if (c.value.has_value()) cfg[c.name] = *c.value;
                        else cfg.erase(c.name);
                        break;
                    case Op::DELETE:
                        cfg.erase(c.name);
                        break;
                    case Op::APPEND:
                    case Op::SUBTRACT: {
                        auto it = cfg.find(c.name);
                        std::vector<std::string> parts;
                        if (it != cfg.end()) {
                            std::stringstream ss(it->second);
                            std::string tok;
                            while (std::getline(ss, tok, ',')) parts.push_back(tok);
                        }
                        if (c.value.has_value()) {
                            if (c.op == Op::APPEND) {
                                parts.push_back(*c.value);
                            } else {
                                parts.erase(std::remove(parts.begin(), parts.end(), *c.value),
                                            parts.end());
                            }
                        }
                        std::string joined;
                        for (size_t i = 0; i < parts.size(); ++i) {
                            if (i > 0) joined.push_back(',');
                            joined += parts[i];
                        }
                        cfg[c.name] = joined;
                        break;
                    }
                }
            }
            if (!request.validateOnly()) {
                log_manager_->setTopicConfig(
                    res.resource_name, storage::LogConfig::fromMap(cfg));
                Logger::info("IncrementalAlterConfigs: topic '{}' applied {} ops",
                             res.resource_name, res.configs.size());
            }
        } else if (res.resource_type == protocol::ConfigResourceType::BROKER) {
            Logger::info("IncrementalAlterConfigs: broker config no-op (read-only)");
        } else {
            rr.error_code = ErrorCode::INVALID_REQUEST;
            rr.error_message = "Unsupported resource type";
        }
        response.addResult(std::move(rr));
    }

    const int16_t version = std::clamp<int16_t>(context.header.apiVersion(), 0, 1);
    return encodeResponse(context, [&](Buffer& buffer) {
        response.encode(buffer, version);
    });
}

Buffer KawasanBroker::buildIncrementalAlterConfigsError(
    const RequestDispatcher::RequestContext& context, ErrorCode code,
    int16_t response_version) const {
    protocol::IncrementalAlterConfigsResponse response;
    response.setThrottleTimeMs(0);
    protocol::IncrementalAlterConfigsResponse::ResourceResult rr;
    rr.error_code = code;
    rr.resource_type = protocol::ConfigResourceType::UNKNOWN;
    response.addResult(std::move(rr));
    const int16_t version = std::clamp<int16_t>(response_version, 0, 1);
    return encodeResponse(context, [&](Buffer& buffer) {
        response.encode(buffer, version);
    });
}

// Phase 4.1: shared trivial error builder.
Buffer KawasanBroker::buildEmptyErrorResponse(
    const RequestDispatcher::RequestContext& context) const {
    return encodeResponse(context, [&](Buffer& /*buffer*/) {});
}

Buffer KawasanBroker::handleDescribeLogDirs(
    RequestDispatcher::RequestContext& context) {
    protocol::DescribeLogDirsRequest request;
    request.decode(context.payload, context.header.apiVersion());

    protocol::DescribeLogDirsResponse response;
    response.setThrottleTimeMs(0);

    protocol::DescribeLogDirsResponse::LogDirInfo info;
    info.error_code = ErrorCode::NONE;
    info.log_dir = log_dir_;
    if (log_manager_) {
        for (auto* log : log_manager_->allLogs()) {
            protocol::DescribeLogDirsResponse::TopicInfo ti;
            ti.topic = log->topic();
            protocol::DescribeLogDirsResponse::PartitionInfo pi;
            pi.partition = log->partition();
            pi.size_bytes = 0;
            pi.offset_lag = 0;
            pi.is_future = false;
            ti.partitions.push_back(pi);
            info.topics.push_back(std::move(ti));
        }
    }
    response.addLogDir(std::move(info));

    return encodeResponse(context, [&](Buffer& buffer) {
        response.encode(buffer, context.header.apiVersion());
    });
}

Buffer KawasanBroker::handleAlterReplicaLogDirs(
    RequestDispatcher::RequestContext& context) {
    protocol::AlterReplicaLogDirsRequest request;
    request.decode(context.payload, context.header.apiVersion());

    protocol::AlterReplicaLogDirsResponse response;
    response.setThrottleTimeMs(0);
    for (const auto& d : request.dirs()) {
        for (const auto& p : d.partitions) {
            protocol::AlterReplicaLogDirsResponse::PartitionResult r;
            r.topic = p.topic;
            r.partition = p.partition;
            r.error_code = ErrorCode::NONE;
            response.addResult(std::move(r));
        }
    }
    return encodeResponse(context, [&](Buffer& buffer) {
        response.encode(buffer, context.header.apiVersion());
    });
}

Buffer KawasanBroker::handleElectLeaders(
    RequestDispatcher::RequestContext& context) {
    protocol::ElectLeadersRequest request;
    request.decode(context.payload, context.header.apiVersion());

    protocol::ElectLeadersResponse response;
    response.setThrottleTimeMs(0);
    response.setErrorCode(ErrorCode::NONE);
    for (const auto& t : request.topics()) {
        protocol::ElectLeadersResponse::TopicResult tr;
        tr.topic = t.topic;
        for (int32_t p : t.partitions) {
            protocol::ElectLeadersResponse::PartitionResult pr;
            pr.partition = p;
            pr.error_code = ErrorCode::NONE;
            tr.partitions.push_back(pr);
        }
        response.addTopic(std::move(tr));
    }
    return encodeResponse(context, [&](Buffer& buffer) {
        response.encode(buffer, context.header.apiVersion());
    });
}

Buffer KawasanBroker::handleDeleteRecords(
    RequestDispatcher::RequestContext& context) {
    protocol::DeleteRecordsRequest request;
    request.decode(context.payload, context.header.apiVersion());

    protocol::DeleteRecordsResponse response;
    response.setThrottleTimeMs(0);
    // Phase 4.1d: actually truncate the prefix of each partition log.
    // Special offset -1 means "delete up to high_watermark". Returns the
    // resulting low_watermark so clients can see the effective deletion.
    for (const auto& t : request.topics()) {
        protocol::DeleteRecordsResponse::TopicResult tr;
        tr.topic = t.topic;
        for (const auto& p : t.partitions) {
            protocol::DeleteRecordsResponse::PartitionResult pr;
            pr.partition = p.partition;

            auto* log = log_manager_ ? log_manager_->getLog(t.topic, p.partition) : nullptr;
            if (!log) {
                pr.error_code = ErrorCode::UNKNOWN_TOPIC_OR_PARTITION;
                pr.low_watermark = -1;
            } else {
                Offset target = p.offset;
                if (target < 0) {
                    target = log->highWatermark();
                }
                pr.low_watermark = log->truncatePrefix(target);
                pr.error_code = ErrorCode::NONE;
                Logger::info("DeleteRecords: {}-{} truncated to low_watermark={}",
                             t.topic, p.partition, pr.low_watermark);
            }
            tr.partitions.push_back(pr);
        }
        response.addTopic(std::move(tr));
    }
    return encodeResponse(context, [&](Buffer& buffer) {
        response.encode(buffer, context.header.apiVersion());
    });
}

Buffer KawasanBroker::handleDeleteGroups(
    RequestDispatcher::RequestContext& context) {
    protocol::DeleteGroupsRequest request;
    request.decode(context.payload, context.header.apiVersion());

    protocol::DeleteGroupsResponse response;
    response.setThrottleTimeMs(0);
    for (const auto& g : request.groups()) {
        protocol::DeleteGroupsResponse::Result r;
        r.group_id = g;
        try {
            if (offset_manager_) {
                offset_manager_->deleteGroup(g);
            }
            r.error_code = ErrorCode::NONE;
            Logger::info("DeleteGroups: removed group '{}'", g);
        } catch (const std::exception& ex) {
            Logger::warn("DeleteGroups: failed to delete '{}': {}", g, ex.what());
            r.error_code = ErrorCode::COORDINATOR_NOT_AVAILABLE;
        }
        response.addResult(std::move(r));
    }
    return encodeResponse(context, [&](Buffer& buffer) {
        response.encode(buffer, context.header.apiVersion());
    });
}

Buffer KawasanBroker::handleOffsetDelete(
    RequestDispatcher::RequestContext& context) {
    protocol::OffsetDeleteRequest request;
    request.decode(context.payload, context.header.apiVersion());

    protocol::OffsetDeleteResponse response;
    response.setErrorCode(ErrorCode::NONE);
    response.setThrottleTimeMs(0);
    // Phase 4.1f: actually drop per-(group, topic, partition) offsets from
    // the OffsetManager. Per-partition errors are reported so callers can
    // distinguish "wasn't there" from "succeeded".
    for (const auto& t : request.topics()) {
        protocol::OffsetDeleteResponse::TopicResult tr;
        tr.topic = t.topic;
        for (const auto& p : t.partitions) {
            protocol::OffsetDeleteResponse::PartitionResult pr;
            pr.partition = p.partition;
            try {
                if (!offset_manager_) {
                    pr.error_code = ErrorCode::COORDINATOR_NOT_AVAILABLE;
                } else {
                    offset_manager_->deleteOffset(request.groupId(), t.topic, p.partition);
                    pr.error_code = ErrorCode::NONE;
                }
            } catch (const std::exception& ex) {
                pr.error_code = ErrorCode::KAFKA_STORAGE_ERROR;
                Logger::error("OffsetDelete failed for {}/{}-{}: {}",
                              request.groupId(), t.topic, p.partition, ex.what());
            }
            tr.partitions.push_back(pr);
        }
        response.addTopic(std::move(tr));
    }
    Logger::info("OffsetDelete: group='{}' processed {} topic(s)",
                 request.groupId(), request.topics().size());
    return encodeResponse(context, [&](Buffer& buffer) {
        response.encode(buffer, context.header.apiVersion());
    });
}

Buffer KawasanBroker::handleCreatePartitions(
    RequestDispatcher::RequestContext& context) {
    protocol::CreatePartitionsRequest request;
    request.decode(context.payload, context.header.apiVersion());

    protocol::CreatePartitionsResponse response;
    response.setThrottleTimeMs(0);
    for (const auto& t : request.topics()) {
        protocol::CreatePartitionsResponse::Result r;
        r.topic = t.topic;

        // Phase 4.1c: real implementation. Forwards the partition increase
        // through the Raft-replicated metadata controller. New partitions
        // are appended with round-robin replica assignment continuing from
        // the existing partitions; logs are created lazily on first produce
        // for each new partition.
        if (!metadata_controller_) {
            r.error_code = ErrorCode::COORDINATOR_NOT_AVAILABLE;
            r.error_message = "Metadata controller not initialized";
        } else {
            auto result = metadata_controller_->increasePartitions(t.topic, t.count);
            r.error_code = result.error_code;
            if (result.error_code != ErrorCode::NONE && !result.error_message.empty()) {
                r.error_message = result.error_message;
            }
        }

        Logger::info("CreatePartitions: topic='{}' new_count={} -> error={}",
                     t.topic, t.count, static_cast<int16_t>(r.error_code));
        response.addResult(std::move(r));
    }
    return encodeResponse(context, [&](Buffer& buffer) {
        response.encode(buffer, context.header.apiVersion());
    });
}

// ---------- Phase 4.1 stubs (each populates an empty-but-valid response) ----------

Buffer KawasanBroker::handleDescribeProducers(
    RequestDispatcher::RequestContext& context) {
    protocol::DescribeProducersRequest req;
    req.decode(context.payload, context.header.apiVersion());
    protocol::DescribeProducersResponse resp;
    resp.setThrottleTimeMs(0);
    // Phase 4.1j: populate active producers from ProducerStateManager
    // (now that Phase 2.1 maintains the state). Each partition entry
    // surfaces the per-producer last_sequence and last_offset, which is
    // what `kafka-describe-producers.sh` and Kafka UI display.
    for (const auto& t : req.topics()) {
        protocol::DescribeProducersResponse::TopicResult tr;
        tr.topic = t.topic;
        for (int32_t p : t.partitions) {
            protocol::DescribeProducersResponse::PartitionResult pr;
            pr.partition = p;
            pr.error_code = ErrorCode::NONE;
            if (producer_state_manager_) {
                for (const auto& ap :
                     producer_state_manager_->listProducers(t.topic, p)) {
                    protocol::DescribeProducersResponse::ActiveProducer entry;
                    entry.producer_id = ap.producer_id;
                    entry.producer_epoch = ap.producer_epoch;
                    entry.last_sequence = ap.last_sequence;
                    entry.last_timestamp = -1;  // Not tracked yet
                    entry.coordinator_epoch = -1;
                    entry.current_txn_start_offset = -1;  // No txn support
                    pr.active_producers.push_back(entry);
                }
            }
            tr.partitions.push_back(std::move(pr));
        }
        resp.addTopic(std::move(tr));
    }
    return encodeResponse(context, [&](Buffer& buf) {
        resp.encode(buf, context.header.apiVersion());
    });
}

Buffer KawasanBroker::handleListTransactions(
    RequestDispatcher::RequestContext& context) {
    protocol::ListTransactionsRequest req;
    req.decode(context.payload, context.header.apiVersion());
    protocol::ListTransactionsResponse resp;
    resp.setThrottleTimeMs(0);
    resp.setErrorCode(ErrorCode::NONE);
    // Phase 4.1l: enumerate known transactional IDs from the
    // TransactionCoordinator with optional state/producer_id filters.
    if (transaction_coordinator_) {
        const auto txns = transaction_coordinator_->list(req.stateFilters(),
                                                         req.producerIdFilters());
        for (const auto& t : txns) {
            protocol::ListTransactionsResponse::TxnState s;
            s.transactional_id = t.transactional_id;
            s.producer_id = t.producer_id;
            s.state = TransactionCoordinator::stateName(t.state);
            resp.addState(std::move(s));
        }
        Logger::info("ListTransactions: returned {} txn(s)", txns.size());
    }
    return encodeResponse(context, [&](Buffer& buf) {
        resp.encode(buf, context.header.apiVersion());
    });
}

Buffer KawasanBroker::handleDescribeTransactions(
    RequestDispatcher::RequestContext& context) {
    protocol::DescribeTransactionsRequest req;
    req.decode(context.payload, context.header.apiVersion());
    protocol::DescribeTransactionsResponse resp;
    resp.setThrottleTimeMs(0);
    // Phase 4.1k: return per-txn state from the TransactionCoordinator.
    // Unknown transactional_ids get TRANSACTIONAL_ID_NOT_FOUND (52) or
    // NONE-with-Empty depending on Kafka semantics — we follow the
    // "NONE + Empty" path so kafka-consumer-groups.sh and Kafka UI
    // don't error on probing.
    for (const auto& tid : req.transactionalIds()) {
        protocol::DescribeTransactionsResponse::State s;
        s.transactional_id = tid;
        if (transaction_coordinator_) {
            const auto opt = transaction_coordinator_->describe(tid);
            if (opt.has_value()) {
                s.error_code = ErrorCode::NONE;
                s.state = TransactionCoordinator::stateName(opt->state);
            } else {
                s.error_code = ErrorCode::NONE;
                s.state = "Empty";
            }
        } else {
            s.error_code = ErrorCode::NONE;
            s.state = "Empty";
        }
        resp.addState(std::move(s));
    }
    return encodeResponse(context, [&](Buffer& buf) {
        resp.encode(buf, context.header.apiVersion());
    });
}

Buffer KawasanBroker::handleAlterPartition(
    RequestDispatcher::RequestContext& context) {
    protocol::AlterPartitionRequest req;
    req.decode(context.payload, context.header.apiVersion());
    protocol::AlterPartitionResponse resp;
    resp.setThrottleTimeMs(0);
    resp.setErrorCode(ErrorCode::NONE);
    return encodeResponse(context, [&](Buffer& buf) {
        resp.encode(buf, context.header.apiVersion());
    });
}

Buffer KawasanBroker::handleDescribeAcls(
    RequestDispatcher::RequestContext& context) {
    protocol::DescribeAclsRequest req;
    req.decode(context.payload, context.header.apiVersion());
    protocol::DescribeAclsResponse resp;
    resp.setThrottleTimeMs(0);
    // Phase 4.2c: query the store with the request's filter and group
    // matching bindings by (resource_type, resource_name, pattern_type).
    if (acl_store_) {
        AclStore::Filter f;
        f.resource_type = req.resource_type;
        f.resource_name_filter = req.resource_name_filter;
        f.pattern_type = req.pattern_type;
        f.principal_filter = req.principal_filter;
        f.host_filter = req.host_filter;
        f.operation = req.operation;
        f.permission_type = req.permission_type;

        const auto bindings = acl_store_->describe(f);
        // Group by resource so the wire response matches the Kafka shape.
        struct ResKey {
            int8_t rt;
            std::string name;
            int8_t pt;
            bool operator<(const ResKey& o) const {
                if (rt != o.rt) return rt < o.rt;
                if (name != o.name) return name < o.name;
                return pt < o.pt;
            }
        };
        std::map<ResKey, std::vector<AclStore::Binding>> grouped;
        for (const auto& b : bindings) {
            grouped[{b.resource_type, b.resource_name, b.pattern_type}].push_back(b);
        }
        for (const auto& [k, list] : grouped) {
            protocol::DescribeAclsResponse::Resource r;
            r.resource_type = k.rt;
            r.resource_name = k.name;
            r.pattern_type = k.pt;
            for (const auto& b : list) {
                protocol::DescribeAclsResponse::Resource::Acl a;
                a.principal = b.principal;
                a.host = b.host;
                a.operation = b.operation;
                a.permission_type = b.permission_type;
                r.acls.push_back(std::move(a));
            }
            resp.addResource(std::move(r));
        }
        Logger::info("DescribeAcls: returned {} resource(s)", grouped.size());
    }
    return encodeResponse(context, [&](Buffer& buf) {
        resp.encode(buf, context.header.apiVersion());
    });
}

Buffer KawasanBroker::handleCreateAcls(
    RequestDispatcher::RequestContext& context) {
    protocol::CreateAclsRequest req;
    req.decode(context.payload, context.header.apiVersion());
    protocol::CreateAclsResponse resp;
    resp.setThrottleTimeMs(0);
    // Phase 4.2c: persist each binding in the in-memory store and emit
    // a per-binding NONE result. Validation (e.g. operation in known
    // enum) is intentionally permissive — Kafka's authorizer rejects
    // some combinations but our store accepts everything, matching the
    // "store-only, no enforcement" posture.
    for (const auto& b : req.creations) {
        protocol::CreateAclsResponse::Result r;
        if (acl_store_) {
            AclStore::Binding sb;
            sb.resource_type = b.resource_type;
            sb.resource_name = b.resource_name;
            sb.pattern_type = b.pattern_type;
            sb.principal = b.principal;
            sb.host = b.host;
            sb.operation = b.operation;
            sb.permission_type = b.permission_type;
            acl_store_->add(sb);
            r.error_code = ErrorCode::NONE;
        } else {
            r.error_code = ErrorCode::BROKER_NOT_AVAILABLE;
        }
        resp.addResult(std::move(r));
    }
    Logger::info("CreateAcls: stored {} binding(s); total={}",
                 req.creations.size(),
                 acl_store_ ? acl_store_->size() : 0);
    return encodeResponse(context, [&](Buffer& buf) {
        resp.encode(buf, context.header.apiVersion());
    });
}

Buffer KawasanBroker::handleDeleteAcls(
    RequestDispatcher::RequestContext& context) {
    protocol::DeleteAclsRequest req;
    req.decode(context.payload, context.header.apiVersion());
    protocol::DeleteAclsResponse resp;
    resp.setThrottleTimeMs(0);
    // Phase 4.2c: process each filter independently. For each filter,
    // delete all matching bindings from the store and echo them back
    // in the response so the client can confirm what was removed.
    for (const auto& f : req.filters) {
        protocol::DeleteAclsResponse::FilterResult fr;
        if (!acl_store_) {
            fr.error_code = ErrorCode::BROKER_NOT_AVAILABLE;
            resp.addFilterResult(std::move(fr));
            continue;
        }
        AclStore::Filter sf;
        sf.resource_type = f.resource_type;
        sf.resource_name_filter = f.resource_name_filter;
        sf.pattern_type = f.pattern_type;
        sf.principal_filter = f.principal_filter;
        sf.host_filter = f.host_filter;
        sf.operation = f.operation;
        sf.permission_type = f.permission_type;
        const auto removed = acl_store_->remove(sf);
        for (const auto& b : removed) {
            protocol::DeleteAclsResponse::MatchingAcl m;
            m.error_code = ErrorCode::NONE;
            m.binding.resource_type = b.resource_type;
            m.binding.resource_name = b.resource_name;
            m.binding.pattern_type = b.pattern_type;
            m.binding.principal = b.principal;
            m.binding.host = b.host;
            m.binding.operation = b.operation;
            m.binding.permission_type = b.permission_type;
            fr.matches.push_back(std::move(m));
        }
        resp.addFilterResult(std::move(fr));
    }
    Logger::info("DeleteAcls: processed {} filter(s); total remaining={}",
                 req.filters.size(),
                 acl_store_ ? acl_store_->size() : 0);
    return encodeResponse(context, [&](Buffer& buf) {
        resp.encode(buf, context.header.apiVersion());
    });
}

// ---------- Phase 4.2a: SASL PLAIN ----------

Buffer KawasanBroker::handleSaslHandshake(
    RequestDispatcher::RequestContext& context) {
    protocol::SaslHandshakeRequest req;
    req.decode(context.payload, context.header.apiVersion());

    protocol::SaslHandshakeResponse resp;
    // Phase 4.2b: advertise PLAIN, SCRAM-SHA-256, and SCRAM-SHA-512.
    std::vector<std::string> kEnabled = {"PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"};
    resp.setEnabledMechanisms(kEnabled);

    const auto& mech = req.mechanism();
    if (mech == "PLAIN" || mech == "SCRAM-SHA-256" || mech == "SCRAM-SHA-512") {
        resp.setErrorCode(ErrorCode::NONE);
        Logger::info("SaslHandshake: accepted mechanism '{}' for {}",
                     mech, context.peer_identity);
        // Phase 4.2b: stash a fresh SCRAM session for this peer so the
        // subsequent SaslAuthenticate messages have somewhere to track
        // state. PLAIN doesn't need session state.
        if (mech == "SCRAM-SHA-256" || mech == "SCRAM-SHA-512") {
            std::lock_guard<std::mutex> lock(sasl_session_mutex_);
            const auto algo = (mech == "SCRAM-SHA-512") ? ScramAlgorithm::kSha512
                                                        : ScramAlgorithm::kSha256;
            sasl_sessions_[context.peer_identity] =
                std::make_unique<ScramAuthenticator>(sasl_scram_creds_, algo);
        }
    } else {
        // Kafka error code 33 = UNSUPPORTED_SASL_MECHANISM.
        resp.setErrorCode(static_cast<ErrorCode>(33));
        Logger::warn("SaslHandshake: rejected mechanism '{}' (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512 supported)",
                     mech);
    }

    return encodeResponse(context, [&](Buffer& buf) {
        resp.encode(buf, context.header.apiVersion());
    });
}

Buffer KawasanBroker::handleSaslAuthenticate(
    RequestDispatcher::RequestContext& context) {
    protocol::SaslAuthenticateRequest req;
    req.decode(context.payload, context.header.apiVersion());

    protocol::SaslAuthenticateResponse resp;

    // Phase 4.2b: dispatch to a SCRAM session if one is active for this
    // peer (set up during the SaslHandshake). Otherwise fall through to
    // PLAIN handling.
    {
        std::unique_lock<std::mutex> lock(sasl_session_mutex_);
        auto it = sasl_sessions_.find(context.peer_identity);
        if (it != sasl_sessions_.end() && it->second) {
            auto* auth = it->second.get();
            const auto out = auth->step(req.authBytes());
            if (auth->failed()) {
                resp.setErrorCode(static_cast<ErrorCode>(58));  // SASL_AUTHENTICATION_FAILED
                resp.setErrorMessage("SCRAM authentication failed");
                Logger::warn("SaslAuthenticate: SCRAM failed for {}", context.peer_identity);
                sasl_sessions_.erase(it);
            } else {
                resp.setErrorCode(ErrorCode::NONE);
                resp.setAuthBytes(out);
                if (auth->authenticated()) {
                    resp.setSessionLifetimeMs(0);
                    Logger::info("SaslAuthenticate: SCRAM completed for user '{}' from {}",
                                 auth->username(), context.peer_identity);
                    sasl_sessions_.erase(it);
                }
            }
            lock.unlock();
            return encodeResponse(context, [&](Buffer& buf) {
                resp.encode(buf, context.header.apiVersion());
            });
        }
    }

    // SASL PLAIN wire format (RFC 4616): "\0username\0password".
    // Phase 4.2a accepts any non-empty username/password — backing with a
    // credentials file is the Phase 4.2b follow-up. The handshake still
    // round-trips so SASL-configured clients connect without
    // UnsupportedSaslMechanism / IllegalSaslState.
    const auto& bytes = req.authBytes();
    if (bytes.empty()) {
        resp.setErrorCode(static_cast<ErrorCode>(58));  // SASL_AUTHENTICATION_FAILED
        resp.setErrorMessage("Empty auth bytes");
        Logger::warn("SaslAuthenticate: empty auth bytes from {}", context.peer_identity);
    } else {
        std::vector<std::string> parts;
        std::string current;
        for (uint8_t b : bytes) {
            if (b == 0) { parts.push_back(std::move(current)); current.clear(); }
            else { current.push_back(static_cast<char>(b)); }
        }
        if (!current.empty()) parts.push_back(std::move(current));
        std::string username, password;
        if (parts.size() >= 3) {
            username = parts[1];
            password = parts[2];
        } else if (parts.size() == 2) {
            username = parts[0];
            password = parts[1];
        }
        if (username.empty() || password.empty()) {
            resp.setErrorCode(static_cast<ErrorCode>(58));
            resp.setErrorMessage("Invalid SASL/PLAIN payload");
        } else if (!sasl_plain_creds_.empty()) {
            // Production path: strict credential check against configured map.
            auto it = sasl_plain_creds_.find(username);
            if (it != sasl_plain_creds_.end() && it->second == password) {
                resp.setErrorCode(ErrorCode::NONE);
                resp.setSessionLifetimeMs(0);
                Logger::info("SaslAuthenticate: PLAIN user='{}' authenticated", username);
            } else {
                resp.setErrorCode(static_cast<ErrorCode>(58));  // SASL_AUTHENTICATION_FAILED
                resp.setErrorMessage("Invalid credentials");
                Logger::warn("SaslAuthenticate: PLAIN user='{}' rejected (unknown user or bad password)",
                             username);
            }
        } else {
            // Dev mode: no credentials configured → accept any non-empty pair.
            resp.setErrorCode(ErrorCode::NONE);
            resp.setSessionLifetimeMs(0);
            Logger::info("SaslAuthenticate: PLAIN user='{}' accepted (dev mode — no sasl.plain.users)",
                         username);
        }
    }

    return encodeResponse(context, [&](Buffer& buf) {
        resp.encode(buf, context.header.apiVersion());
    });
}

// ---------- Phase 3.3 scaffolding: transactional APIs ----------

Buffer KawasanBroker::handleAddPartitionsToTxn(
    RequestDispatcher::RequestContext& context) {
    protocol::AddPartitionsToTxnRequest req;
    req.decode(context.payload, context.header.apiVersion());
    Logger::info("AddPartitionsToTxn: transactional_id='{}' pid={} epoch={} {} topics",
                 req.transactionalId(), req.producerId(), req.producerEpoch(),
                 req.topics().size());
    // Phase 3.3: register the txn in the coordinator (idempotent — if
    // already registered, it stays) and record the participating
    // partitions so EndTxn can later emit control records to them.
    if (transaction_coordinator_ && !req.transactionalId().empty()) {
        // Don't reset state if the txn is already known.
        if (!transaction_coordinator_->describe(req.transactionalId()).has_value()) {
            transaction_coordinator_->recordInitProducerId(
                req.transactionalId(), req.producerId(), req.producerEpoch(),
                /*timeout=*/60000);
        }
        std::vector<std::pair<std::string, int32_t>> partitions;
        for (const auto& t : req.topics()) {
            for (int32_t p : t.partitions) {
                partitions.emplace_back(t.topic, p);
            }
        }
        transaction_coordinator_->addPartitions(req.transactionalId(), partitions);

        // Phase EX-10: register each partition in the isolation tracker
        // with the current log_end_offset as the txn's first_offset.
        // This is what LSO will hold consumers behind until EndTxn lands.
        if (isolation_tracker_ && log_manager_) {
            for (const auto& [topic, p] : partitions) {
                auto* log = log_manager_->getOrCreateLog(topic, p);
                if (log) {
                    isolation_tracker_->recordInFlightTxn(
                        req.producerId(), topic, p, log->logEndOffset());
                }
            }
        }
    }
    protocol::AddPartitionsToTxnResponse resp;
    resp.setThrottleTimeMs(0);
    for (const auto& t : req.topics()) {
        protocol::AddPartitionsToTxnResponse::TopicResult tr;
        tr.topic = t.topic;
        for (int32_t p : t.partitions) {
            protocol::AddPartitionsToTxnResponse::PartitionResult pr;
            pr.partition = p;
            pr.error_code = ErrorCode::NONE;
            tr.partitions.push_back(pr);
        }
        resp.addTopic(std::move(tr));
    }
    return encodeResponse(context, [&](Buffer& buf) {
        resp.encode(buf, context.header.apiVersion());
    });
}

Buffer KawasanBroker::handleAddOffsetsToTxn(
    RequestDispatcher::RequestContext& context) {
    protocol::AddOffsetsToTxnRequest req;
    req.decode(context.payload, context.header.apiVersion());
    Logger::info("AddOffsetsToTxn: txn='{}' group='{}' pid={} epoch={}",
                 req.transactionalId(), req.groupId(),
                 req.producerId(), req.producerEpoch());
    protocol::AddOffsetsToTxnResponse resp;
    resp.setThrottleTimeMs(0);
    resp.setErrorCode(ErrorCode::NONE);
    return encodeResponse(context, [&](Buffer& buf) {
        resp.encode(buf, context.header.apiVersion());
    });
}

Buffer KawasanBroker::handleEndTxn(
    RequestDispatcher::RequestContext& context) {
    protocol::EndTxnRequest req;
    req.decode(context.payload, context.header.apiVersion());
    Logger::info("EndTxn: txn='{}' pid={} epoch={} committed={}",
                 req.transactionalId(), req.producerId(),
                 req.producerEpoch(), req.committed());
    // Phase 3.3 + EX-10: drive the txn state machine and emit control
    // records to participating partitions.
    if (transaction_coordinator_ && !req.transactionalId().empty()) {
        std::vector<std::pair<std::string, int32_t>> participating;
        // Phase EX-6: drain pending offsets BEFORE the txn state
        // transition so the snapshot still has them. On commit we apply
        // them; on abort drainPendingOffsets returns an empty vector
        // (abortTxn already discarded them) so this is a no-op.
        auto staged_offsets = transaction_coordinator_->drainPendingOffsets(
            req.transactionalId());
        if (req.committed()) {
            participating = transaction_coordinator_->commitTxn(req.transactionalId());
            Logger::info("EndTxn(commit): txn='{}' committed across {} partitions, "
                         "applying {} staged offsets",
                         req.transactionalId(), participating.size(),
                         staged_offsets.size());
            // Phase EX-6: apply staged offsets to OffsetManager only
            // on commit. Grouped by group_id for the OffsetManager
            // batch API. Abort path is a no-op.
            if (offset_manager_ && !staged_offsets.empty()) {
                std::unordered_map<std::string, std::vector<OffsetManager::OffsetCommitData>>
                    by_group;
                for (auto& po : staged_offsets) {
                    OffsetManager::OffsetCommitData d;
                    d.topic = po.topic;
                    d.partition = po.partition;
                    d.offset = po.offset;
                    d.metadata = po.metadata;
                    by_group[po.group_id].push_back(std::move(d));
                }
                for (auto& [group_id, data] : by_group) {
                    try {
                        offset_manager_->commitOffsetBatch(group_id, data);
                    } catch (const std::exception& ex) {
                        Logger::warn(
                            "EndTxn(commit) failed to apply staged offsets "
                            "for group '{}': {}", group_id, ex.what());
                    }
                }
            }
        } else {
            participating = transaction_coordinator_->abortTxn(req.transactionalId());
            Logger::info("EndTxn(abort): txn='{}' aborted across {} partitions, "
                         "discarding {} staged offsets",
                         req.transactionalId(), participating.size(),
                         staged_offsets.size());
        }

        // Phase EX-10: emit a control RecordBatch (COMMIT or ABORT
        // marker) on each participating partition so consumers with
        // read_committed isolation can detect the boundary.
        if (log_manager_) {
            const Timestamp now_ms =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch())
                    .count();
            for (const auto& [topic, partition] : participating) {
                auto* log = log_manager_->getOrCreateLog(topic, partition);
                if (!log) continue;
                const Offset base_offset = log->logEndOffset();
                auto control_batch = storage::RecordBatch::makeControlBatch(
                    req.producerId(), req.producerEpoch(),
                    base_offset, req.committed(), now_ms);
                try {
                    // Append the full batch — preserving the isControl
                    // and isTransactional attribute bits, producer_id,
                    // and producer_epoch. A read_committed consumer uses
                    // these to identify and skip transaction boundary
                    // markers and aborted records (KIP-98).
                    log->appendBatch(std::move(control_batch));
                } catch (const std::exception& ex) {
                    Logger::warn(
                        "Failed to emit control record on {}-{}: {}",
                        topic, partition, ex.what());
                }
            }
        }

        // Phase EX-10: update the isolation tracker. Commit releases
        // the LSO hold; abort moves the txn's first_offset into the
        // aborted-transactions ring.
        if (isolation_tracker_) {
            if (req.committed()) {
                isolation_tracker_->commitInFlightTxns(participating,
                                                       req.producerId());
            } else {
                isolation_tracker_->abortInFlightTxns(participating,
                                                      req.producerId());
            }
        }
    }
    protocol::EndTxnResponse resp;
    resp.setThrottleTimeMs(0);
    resp.setErrorCode(ErrorCode::NONE);
    return encodeResponse(context, [&](Buffer& buf) {
        resp.encode(buf, context.header.apiVersion());
    });
}

Buffer KawasanBroker::handleTxnOffsetCommit(
    RequestDispatcher::RequestContext& context) {
    protocol::TxnOffsetCommitRequest req;
    req.decode(context.payload, context.header.apiVersion());
    Logger::info("TxnOffsetCommit: txn='{}' group='{}' pid={} epoch={} {} topics",
                 req.transactionalId(), req.groupId(),
                 req.producerId(), req.producerEpoch(),
                 req.topics().size());
    // Phase EX-6: stage the offsets in the transactional context. They
    // are applied to OffsetManager only on EndTxn(commit=true) and
    // discarded on EndTxn(commit=false). This makes consumer-group
    // offset commits truly transactional (KIP-447) — required for
    // Streams EOS v2 semantics.
    if (transaction_coordinator_ && !req.transactionalId().empty()
        && !req.groupId().empty()) {
        std::vector<TransactionCoordinator::PendingOffset> staged;
        for (const auto& t : req.topics()) {
            for (const auto& p : t.partitions) {
                TransactionCoordinator::PendingOffset po;
                po.group_id = req.groupId();
                po.topic = t.topic;
                po.partition = p.partition;
                po.offset = p.offset;
                po.metadata = p.metadata;
                staged.push_back(std::move(po));
            }
        }
        transaction_coordinator_->stagePendingOffsets(req.transactionalId(),
                                                      std::move(staged));
    } else if (offset_manager_ && !req.groupId().empty()) {
        // Fallback: if no transactional context is provided, treat
        // this like a plain OffsetCommit. This preserves the old
        // scaffolding behavior for clients that send TxnOffsetCommit
        // without a real txn.
        std::vector<OffsetManager::OffsetCommitData> data;
        for (const auto& t : req.topics()) {
            for (const auto& p : t.partitions) {
                OffsetManager::OffsetCommitData d;
                d.topic = t.topic;
                d.partition = p.partition;
                d.offset = p.offset;
                d.metadata = p.metadata;
                data.push_back(std::move(d));
            }
        }
        try {
            offset_manager_->commitOffsetBatch(req.groupId(), data);
        } catch (const std::exception& ex) {
            Logger::warn("TxnOffsetCommit batch commit failed: {}", ex.what());
        }
    }
    protocol::TxnOffsetCommitResponse resp;
    resp.setThrottleTimeMs(0);
    for (const auto& t : req.topics()) {
        protocol::TxnOffsetCommitResponse::TopicResult tr;
        tr.topic = t.topic;
        for (const auto& p : t.partitions) {
            protocol::TxnOffsetCommitResponse::PartitionResult pr;
            pr.partition = p.partition;
            pr.error_code = ErrorCode::NONE;
            tr.partitions.push_back(pr);
        }
        resp.addTopic(std::move(tr));
    }
    return encodeResponse(context, [&](Buffer& buf) {
        resp.encode(buf, context.header.apiVersion());
    });
}

}  // namespace kawasan::broker
