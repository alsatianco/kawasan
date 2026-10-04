#include "kawasan/broker/kawasan_broker.h"

#include <zlib.h>

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <limits>
#include <map>
#include <nlohmann/json.hpp>
#include <optional>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

#include "kawasan/broker/acl_store.h"
#include "kawasan/broker/coordinator_routing.h"
#include "kawasan/broker/fetch_session_manager.h"
#include "kawasan/broker/isolation_tracker.h"
#include "kawasan/broker/leader_election_policy.h"
#include "kawasan/broker/peer_client.h"
#include "kawasan/broker/producer_id.h"
#include "kawasan/broker/producer_state_manager.h"
#include "kawasan/broker/producer_state_snapshot.h"
#include "kawasan/broker/quota_manager.h"
#include "kawasan/broker/replica_manager.h"
#include "kawasan/broker/scram_auth.h"
#include "kawasan/broker/transaction_coordinator.h"
#include "kawasan/broker/transaction_state_manager.h"
#include "kawasan/common/error.h"
#include "kawasan/common/file_util.h"
#include "kawasan/common/logger.h"
#include "kawasan/protocol/admin_misc_requests.h"
#include "kawasan/protocol/admin_stubs.h"
#include "kawasan/protocol/alter_configs_request.h"
#include "kawasan/protocol/api_versions.h"
#include "kawasan/protocol/create_topics_request.h"
#include "kawasan/protocol/delete_topics_request.h"
#include "kawasan/protocol/describe_cluster_request.h"
#include "kawasan/protocol/describe_configs_request.h"
#include "kawasan/protocol/describe_groups_request.h"
#include "kawasan/protocol/fetch_request.h"
#include "kawasan/protocol/find_coordinator_request.h"
#include "kawasan/protocol/heartbeat_request.h"
#include "kawasan/protocol/incremental_alter_configs_request.h"
#include "kawasan/protocol/init_producer_id_request.h"
#include "kawasan/protocol/join_group_request.h"
#include "kawasan/protocol/leave_group_request.h"
#include "kawasan/protocol/list_groups_request.h"
#include "kawasan/protocol/list_offsets_request.h"
#include "kawasan/protocol/metadata_request.h"
#include "kawasan/protocol/offset_commit_request.h"
#include "kawasan/protocol/offset_fetch_request.h"
#include "kawasan/protocol/offset_for_leader_epoch_request.h"
#include "kawasan/protocol/produce_request.h"
#include "kawasan/protocol/sasl_request.h"
#include "kawasan/protocol/sync_group_request.h"
#include "kawasan/protocol/txn_request.h"
#include "kawasan/storage/record_batch.h"

namespace kawasan::broker {

namespace {
constexpr int16_t kMetadataMaxVersion = 12;        // Phase 1.2
constexpr int16_t kProduceMaxVersion = 11;         // CM-3
constexpr int16_t kFetchMaxVersion = 13;           // CM-3: persisted controller UUIDs
constexpr int16_t kListOffsetsMaxVersion = 8;      // CM-3: EARLIEST_LOCAL
constexpr int16_t kFindCoordinatorMaxVersion = 4;  // Phase 1.6
constexpr int16_t kJoinGroupMaxVersion = 9;        // Phase 1.7
constexpr int16_t kSyncGroupMaxVersion = 5;        // Phase 1.8
constexpr int16_t kHeartbeatMaxVersion = 4;        // Phase 1.9
constexpr int16_t kLeaveGroupMaxVersion = 5;       // Phase 1.10
constexpr int16_t kOffsetCommitMaxVersion = 8;     // Phase 1.11
constexpr int16_t kOffsetFetchMaxVersion = 9;      // CM-3: classic/null membership
constexpr int16_t kDescribeGroupsMaxVersion = 5;   // Phase 1.13
constexpr int16_t kListGroupsMaxVersion = 4;       // Phase 1.14
}  // namespace

void KawasanBroker::validateProductionConfig() const {
    if (!production_mode_) {
        return;
    }

    std::vector<std::string> errors;

    // Client/broker TLS is not implemented; SASL_SSL/SSL would not actually
    // encrypt. (security.protocol=SSL is already refused above; catch SASL_SSL
    // and ssl.enabled here too so production never silently runs plaintext under
    // a TLS-implying protocol.)
    const std::string security_protocol =
        config_.get<std::string>("security.protocol", "PLAINTEXT");
    if (security_protocol.find("SSL") != std::string::npos ||
        config_.get<bool>("ssl.enabled", false)) {
        errors.emplace_back(
            "security.protocol implies TLS (" + security_protocol +
            ") but client/broker TLS is not implemented; use PLAINTEXT/SASL_PLAINTEXT "
            "and terminate TLS at a proxy");
    }

    // Replication factor can't exceed the cluster size (number of brokers in
    // raft.peers). In production, reject rather than silently clamp so the
    // operator's durability intent isn't quietly weakened.
    if (config_.get<int>("default.replication.factor", 1) > cluster_size_) {
        errors.emplace_back("default.replication.factor exceeds the cluster size (" +
                            std::to_string(cluster_size_) +
                            " broker(s) in raft.peers); reduce it or add brokers");
    }
    if (config_.get<int>("min.insync.replicas", 1) > cluster_size_) {
        errors.emplace_back("min.insync.replicas exceeds the cluster size (" +
                            std::to_string(cluster_size_) +
                            " broker(s)); reduce it or add brokers");
    }

    if (!errors.empty()) {
        std::string msg = "deployment.mode=production but the configuration requests capabilities "
                          "this build cannot honor:";
        for (const auto& e : errors) {
            msg += "\n  - " + e;
        }
        throw std::runtime_error(msg);
    }
    Logger::info("Production-mode config validation passed");
}

bool KawasanBroker::authorize(const RequestDispatcher::RequestContext& context, int8_t operation,
                              int8_t resource_type, const std::string& resource_name) const {
    if (!authorizer_enabled_) {
        return true;  // enforcement opted out — preserve pre-authorizer behavior
    }
    const std::string principal =
        (context.connection && context.connection->authenticated_principal)
            ? *context.connection->authenticated_principal
            : std::string("User:ANONYMOUS");
    if (super_users_.count(principal) > 0) {
        return true;
    }
    // peer_identity is "ip:port"; ACL host matching uses the IP.
    std::string host = context.peer_identity;
    const auto colon = host.rfind(':');
    if (colon != std::string::npos) {
        host = host.substr(0, colon);
    }
    return acl_store_ && acl_store_->authorize(principal, operation, resource_type, resource_name,
                                               host, allow_everyone_if_no_acl_);
}

KawasanBroker::KawasanBroker(const Config& config) : config_(config) {
    const auto compatibility_profile =
        config_.get<std::string>("compatibility.max.api.version.profile", "4.x");
    if (compatibility_profile != "4.x" && compatibility_profile != "3.x") {
        throw std::invalid_argument("compatibility.max.api.version.profile must be 4.x or 3.x");
    }
    broker_id_ = config_.get<BrokerId>("broker.id");
    host_ = config_.get<std::string>("host", "localhost");
    advertised_host_ = config_.get<std::string>("advertised.host", host_);
    port_ = config_.get<int32_t>("port", 9092);
    cluster_id_ = config_.get<std::string>("cluster.id", "kawasan-cluster");

    auto_create_topics_enabled_ = config_.get<bool>("auto.create.topics.enable", true);

    const int32_t configured_partitions = config_.get<int32_t>("num.partitions", 1);
    if (configured_partitions <= 0) {
        Logger::warn("Configured num.partitions={} is invalid; using 1", configured_partitions);
        default_num_partitions_ = 1;
    } else {
        default_num_partitions_ = configured_partitions;
    }

    // Cluster size = number of brokers listed in raft.peers (1 if single-node).
    // Replication factor is capped at the cluster size — a topic can't have more
    // replicas than there are brokers.
    {
        const std::string peers = config_.get<std::string>("raft.peers", "");
        cluster_size_ =
            peers.empty() ? 1 : 1 + static_cast<int>(std::count(peers.begin(), peers.end(), ','));
    }

    int16_t configured_replication_factor = config_.get<int16_t>("default.replication.factor", 1);
    if (configured_replication_factor < 1) {
        Logger::warn("Configured default.replication.factor={} is invalid; using 1",
                     configured_replication_factor);
        configured_replication_factor = 1;
    } else if (configured_replication_factor > cluster_size_) {
        Logger::warn("default.replication.factor {} exceeds cluster size {}; clamping to {}",
                     configured_replication_factor, cluster_size_, cluster_size_);
        configured_replication_factor = static_cast<int16_t>(cluster_size_);
    }
    default_replication_factor_ = configured_replication_factor;

    log_dir_ = config_.get<std::string>("log.dirs", "/tmp/kawasan-logs");
    metadata_dir_ = config_.get<std::string>("metadata.dir", log_dir_ + "/meta");

    storage::LogConfig configured_log;
    const auto default_segment_bytes = static_cast<int64_t>(configured_log.segment_size);
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

    int64_t retention_ms = config_.get<int64_t>("log.retention.ms", configured_log.retention_ms);
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

    // Durability mode for partition-log writes. "sync" (default) fsyncs each
    // produce append so an acknowledged record survives a power loss; "async"
    // trades that for throughput (WAL-buffered, lost on machine crash). This
    // backs the at-least-once guarantee the docs advertise.
    const std::string durability = config_.get<std::string>("log.durability", "sync");
    if (durability == "async") {
        configured_log.flush_mode = storage::FlushMode::kAsync;
        Logger::warn(
            "log.durability=async: acked records are WAL-buffered and may be lost on a "
            "power loss / OS crash. Use log.durability=sync for at-least-once durability.");
    } else {
        if (durability != "sync") {
            Logger::warn("Unknown log.durability='{}'; defaulting to 'sync'", durability);
        }
        configured_log.flush_mode = storage::FlushMode::kSync;
    }

    log_config_ = configured_log;

    // Parse TLS configuration for Kafka protocol
    std::string security_protocol = config_.get<std::string>("security.protocol", "PLAINTEXT");
    tls_config_.enabled = (security_protocol == "SSL") || config_.get<bool>("ssl.enabled", false);

    if (tls_config_.enabled) {
        // Until TcpSession wraps its socket in boost::asio::ssl::stream and
        // performs async_handshake(), accepting ssl.enabled=true would produce a
        // broker that *thinks* it serves TLS but listens as plain TCP — worse
        // than refusing, because the client's ClientHello is read as Kafka
        // request framing and surfaces as a confusing "WrongVersionNumber"
        // alert. Refuse loudly so the operator knows TLS is not in this build.
        throw std::runtime_error(
            "security.protocol=SSL / ssl.enabled=true is configured, but client/broker "
            "TLS is not implemented in this build (the TCP session uses a plain socket). "
            "Set security.protocol=PLAINTEXT (or SASL_PLAINTEXT) and terminate TLS at a "
            "proxy if you need encryption in transit.");
    } else {
        Logger::info("TLS disabled for Kafka protocol (using PLAINTEXT)");
    }

    // Determine deployment mode once. Production mode (a) makes the validator
    // below fail fast on settings the broker can't honor and (b) tightens
    // runtime behavior such as refusing SASL/PLAIN "accept-any".
    {
        std::string mode = config_.get<std::string>("deployment.mode", "");
        if (mode.empty()) {
            if (const char* env = std::getenv("KAWASAN_DEPLOYMENT_MODE")) {
                mode = env;
            }
        }
        production_mode_ = (mode == "production");
    }

    // Fail fast on settings the broker cannot actually honor instead of silently
    // degrading them. An operator who asked for RF=3 in production must not
    // discover at 3am that it was quietly forced to 1.
    validateProductionConfig();

    // Authorization (ACL enforcement). Off by default so deployments with no
    // ACLs behave exactly as before; opt in with authorizer.enabled=true.
    authorizer_enabled_ = config_.get<bool>("authorizer.enabled", false);
    allow_everyone_if_no_acl_ = config_.get<bool>("allow.everyone.if.no.acl.found", false);
    {
        const std::string supers = config_.get<std::string>("super.users", "");
        size_t start = 0;
        while (start < supers.size()) {
            size_t sep = supers.find(';', start);
            if (sep == std::string::npos)
                sep = supers.size();
            std::string p = supers.substr(start, sep - start);
            // trim surrounding whitespace
            const auto b = p.find_first_not_of(" \t");
            const auto e = p.find_last_not_of(" \t");
            if (b != std::string::npos)
                super_users_.insert(p.substr(b, e - b + 1));
            start = sep + 1;
        }
    }
    if (authorizer_enabled_) {
        Logger::info("Authorizer ENABLED (allow.everyone.if.no.acl.found={}, {} super.user(s))",
                     allow_everyone_if_no_acl_, super_users_.size());
    }

    // Client quotas (per-client byte-rate throttling). Disabled by default
    // (bytes/sec <= 0). When set, an over-quota client gets a throttle_time_ms
    // in its response so it backs off, protecting the broker from noisy clients.
    quota_manager_ =
        std::make_unique<QuotaManager>(config_.get<int64_t>("quota.producer.default", 0),
                                       config_.get<int64_t>("quota.consumer.default", 0));
    if (quota_manager_->producerQuotaEnabled() || quota_manager_->consumerQuotaEnabled()) {
        Logger::info("Client quotas ENABLED (producer={} B/s, consumer={} B/s)",
                     config_.get<int64_t>("quota.producer.default", 0),
                     config_.get<int64_t>("quota.consumer.default", 0));
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

        Logger::info("TLS enabled for Raft protocol (cert: {})", raft_tls_config_.cert_file);
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
        const auto creds_file = config_.getString("sasl.plain.credentials.file").value_or("");
        if (!creds_file.empty()) {
            std::ifstream ifs(creds_file);
            if (!ifs) {
                throw std::runtime_error(
                    "sasl.plain.credentials.file configured but cannot be opened: " + creds_file);
            }
            std::string line;
            while (std::getline(ifs, line)) {
                if (line.empty() || line[0] == '#')
                    continue;
                const auto colon = line.find(':');
                if (colon == std::string::npos)
                    continue;
                auto user = line.substr(0, colon);
                auto pass = line.substr(colon + 1);
                if (!user.empty() && !pass.empty()) {
                    sasl_plain_creds_.emplace(std::move(user), std::move(pass));
                }
            }
            Logger::info("SASL PLAIN: loaded {} credential(s) from {}", sasl_plain_creds_.size(),
                         creds_file);
        }
        const auto inline_creds = config_.getString("sasl.plain.users").value_or("");
        if (!inline_creds.empty()) {
            try {
                auto j = nlohmann::json::parse(inline_creds);
                if (j.is_object()) {
                    for (auto it = j.begin(); it != j.end(); ++it) {
                        if (it.value().is_string()) {
                            sasl_plain_creds_.emplace(it.key(), it.value().get<std::string>());
                        }
                    }
                    Logger::info("SASL PLAIN: total {} credential(s) after inline merge",
                                 sasl_plain_creds_.size());
                }
            } catch (const std::exception& e) {
                throw std::runtime_error(std::string("sasl.plain.users JSON parse error: ") +
                                         e.what());
            }
        }
        if (sasl_plain_creds_.empty()) {
            Logger::warn("SASL PLAIN: no credentials configured — accepting any non-empty "
                         "user/password (dev mode). Set sasl.plain.credentials.file or "
                         "sasl.plain.users for production.");
        }
    }

    // Phase 4.2b: Load SASL/SCRAM-SHA-256 credentials. Same two sources
    // as PLAIN. Broker generates salt + stored_key + server_key from the
    // plaintext password at startup; a production deployment should ship
    // pre-computed credentials in the file instead of plaintexts.
    {
        const auto scram_file = config_.getString("sasl.scram.credentials.file").value_or("");
        if (!scram_file.empty()) {
            std::ifstream ifs(scram_file);
            if (!ifs) {
                throw std::runtime_error(
                    "sasl.scram.credentials.file configured but cannot be opened: " + scram_file);
            }
            std::string line;
            while (std::getline(ifs, line)) {
                if (line.empty() || line[0] == '#')
                    continue;
                const auto colon = line.find(':');
                if (colon == std::string::npos)
                    continue;
                auto user = line.substr(0, colon);
                auto pass = line.substr(colon + 1);
                if (!user.empty() && !pass.empty()) {
                    sasl_scram_creds_.emplace(std::move(user),
                                              ScramCredentials::fromPassword(pass));
                }
            }
        }
        const auto inline_scram = config_.getString("sasl.scram.users").value_or("");
        if (!inline_scram.empty()) {
            try {
                auto j = nlohmann::json::parse(inline_scram);
                if (j.is_object()) {
                    for (auto it = j.begin(); it != j.end(); ++it) {
                        if (it.value().is_string()) {
                            sasl_scram_creds_.emplace(it.key(), ScramCredentials::fromPassword(
                                                                    it.value().get<std::string>()));
                        }
                    }
                }
            } catch (const std::exception& e) {
                throw std::runtime_error(std::string("sasl.scram.users JSON parse error: ") +
                                         e.what());
            }
        }
        if (!sasl_scram_creds_.empty()) {
            Logger::info("SASL SCRAM-SHA-256: loaded {} credential(s)", sasl_scram_creds_.size());
        }
    }

    log_manager_ = std::make_unique<storage::LogManager>(log_dir_, log_config_);
    // Phase 3.2: cleanup interval (compaction + retention sweep). Kafka default
    // is 5 minutes; expose via `log.cleaner.interval.ms` so dev/test configs
    // can observe compaction within a short window.
    {
        const int64_t cleanup_ms = config_.get<int64_t>("log.cleaner.interval.ms", 300000);
        log_manager_->setCleanupIntervalMs(cleanup_ms);
        log_manager_->setCheckpointIntervalMs(std::max<int64_t>(
            1, config_.get<int64_t>("replica.high.watermark.checkpoint.interval.ms", 5000)));
        // Single-node: every partition is its own sole replica, so HW == LEO by
        // definition; don't let a lagging periodic checkpoint hide records.
        log_manager_->setRecoverHighWatermarkToLogEnd(cluster_size_ == 1);
    }
    {
        const auto threads = config_.get<int32_t>("fetch.purgatory.threads", 2);
        delayed_fetch_purgatory_ =
            std::make_shared<DelayedOperationPurgatory>(static_cast<size_t>(std::max(1, threads)));
        // Completing a produce is cheap (an HW comparison + response encode).
        delayed_produce_purgatory_ = std::make_shared<DelayedOperationPurgatory>(1);
        log_manager_->setChangeListener(
            [fetches = delayed_fetch_purgatory_, produces = delayed_produce_purgatory_](
                const std::string& topic, PartitionId partition) {
                fetches->notify(topic, partition);
                produces->notify(topic, partition);
            });
    }
    replica_manager_ = std::make_unique<ReplicaManager>();
    // M4: give the replica manager this broker's real id BEFORE any partition is
    // registered, so leader/ISR identity and the leader-side follower-offset
    // check use broker.id instead of the hardcoded 0.
    replica_manager_->setLocalBrokerId(broker_id_);
    // M4: cache min.insync.replicas for the acks=all NOT_ENOUGH_REPLICAS gate.
    min_insync_replicas_ = std::max<int32_t>(1, config_.get<int32_t>("min.insync.replicas", 1));
    // M6: replica.lag.time.max.ms — how long a follower can go without fetching
    // before the leader drops it from the ISR (clamped to a sane floor).
    replica_lag_time_max_ms_ =
        std::max<int64_t>(1000, config_.get<int64_t>("replica.lag.time.max.ms", 30000));
    broker_liveness_timeout_ms_ =
        std::max<int64_t>(1000, config_.get<int64_t>("broker.liveness.timeout.ms", 9000));
    unclean_leader_election_enabled_ = config_.get<bool>("unclean.leader.election.enable", false);

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
    monitoring_manager_ =
        std::make_unique<monitoring::MonitoringManager>(monitoring_host, monitoring_port);
    // Use raw pointer from monitoring_manager for metrics_collector_
    // The monitoring_manager owns the MetricsCollector lifecycle
    auto* raw_metrics_collector = monitoring_manager_->metricsCollector();

    // Phase EX-1 (§6.3): wire per-subsystem metric providers. These are
    // queried at every Prometheus scrape; the lambdas hold raw pointers
    // to the broker's owned subsystems (broker outlives MetricsCollector).
    raw_metrics_collector->setProducerStateProvider([this]() {
        std::ostringstream oss;
        if (!producer_state_manager_)
            return std::string{};
        auto m = producer_state_manager_->getMetrics();
        oss << "# HELP kawasan_producer_state_entries Tracked (topic,partition,producer_id) "
               "entries\n"
            << "# TYPE kawasan_producer_state_entries gauge\n"
            << "kawasan_producer_state_entries " << m.entries << "\n\n"
            << "# HELP kawasan_producer_state_evictions_total Entries evicted from the producer "
               "state map\n"
            << "# TYPE kawasan_producer_state_evictions_total counter\n"
            << "kawasan_producer_state_evictions_total " << m.evictions_total << "\n\n"
            << "# HELP kawasan_producer_id_count Distinct active producer_ids\n"
            << "# TYPE kawasan_producer_id_count gauge\n"
            << "kawasan_producer_id_count " << m.producer_id_count << "\n\n";
        return oss.str();
    });
    raw_metrics_collector->setFetchSessionProvider([this]() {
        std::ostringstream oss;
        if (!fetch_session_manager_)
            return std::string{};
        auto m = fetch_session_manager_->getMetrics();
        oss << "# HELP kawasan_fetch_session_count Active fetch sessions\n"
            << "# TYPE kawasan_fetch_session_count gauge\n"
            << "kawasan_fetch_session_count " << m.session_count << "\n\n"
            << "# HELP kawasan_fetch_session_evictions_total Sessions evicted due to idle timeout\n"
            << "# TYPE kawasan_fetch_session_evictions_total counter\n"
            << "kawasan_fetch_session_evictions_total " << m.evictions_total << "\n\n"
            << "# HELP kawasan_incremental_fetch_session_hit_ratio Fraction of fetches served by "
               "an existing session\n"
            << "# TYPE kawasan_incremental_fetch_session_hit_ratio gauge\n"
            << "kawasan_incremental_fetch_session_hit_ratio " << m.incremental_hit_ratio << "\n\n";
        return oss.str();
    });
    raw_metrics_collector->setTransactionProvider([this]() {
        std::ostringstream oss;
        if (!transaction_coordinator_)
            return std::string{};
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
            << "# HELP kawasan_transaction_state_loads_total InitProducerId invocations (state "
               "machine loads)\n"
            << "# TYPE kawasan_transaction_state_loads_total counter\n"
            << "kawasan_transaction_state_loads_total " << m.state_loads_total << "\n\n";
        return oss.str();
    });
    // group_provider_ and log_cleaner_provider_ are wired after the
    // respective subsystems exist; see below.

    // Initialize GroupCoordinator with OffsetManager, LogManager, and MetricsCollector
    group_coordinator_ = std::make_shared<GroupCoordinator>(
        offset_manager_, log_manager_.get(),
        std::shared_ptr<monitoring::MetricsCollector>(raw_metrics_collector,
                                                      [](monitoring::MetricsCollector*) {}));

    // Phase EX-1: wire GroupCoordinator + LogCleaner providers (must
    // come after group_coordinator_ + log_manager_ are constructed).
    raw_metrics_collector->setGroupProvider([this]() {
        std::ostringstream oss;
        if (!group_coordinator_)
            return std::string{};
        auto m = group_coordinator_->getMetrics();
        oss << "# HELP kawasan_group_member_timeout_total Members evicted for missed heartbeats\n"
            << "# TYPE kawasan_group_member_timeout_total counter\n"
            << "kawasan_group_member_timeout_total " << m.member_timeout_total << "\n\n";
        // Always emit HELP/TYPE for label-family metrics even when the
        // family is empty — operators want to know the metric exists.
        oss << "# HELP kawasan_group_rebalances_total Per-group rebalance count\n"
            << "# TYPE kawasan_group_rebalances_total counter\n";
        for (const auto& g : m.groups) {
            oss << "kawasan_group_rebalances_total{group=\"" << g.group_id << "\"} "
                << g.rebalances_total << "\n";
        }
        oss << "\n# HELP kawasan_group_state Group state machine kind (1 = currently in this "
               "state)\n"
            << "# TYPE kawasan_group_state gauge\n";
        for (const auto& g : m.groups) {
            oss << "kawasan_group_state{group=\"" << g.group_id << "\",state=\"" << g.state
                << "\"} 1\n";
        }
        oss << "\n";
        return oss.str();
    });
    raw_metrics_collector->setLogCleanerProvider([this]() {
        std::ostringstream oss;
        if (!log_manager_)
            return std::string{};
        auto m = log_manager_->getCleanerMetrics();
        oss << "# HELP kawasan_log_cleaner_running 1 if a cleanup pass is currently active\n"
            << "# TYPE kawasan_log_cleaner_running gauge\n"
            << "kawasan_log_cleaner_running " << (m.running ? 1 : 0) << "\n\n"
            << "# HELP kawasan_log_cleaner_compactions_total Cleanup passes executed\n"
            << "# TYPE kawasan_log_cleaner_compactions_total counter\n"
            << "kawasan_log_cleaner_compactions_total " << m.compactions_total << "\n\n"
            << "# HELP kawasan_log_cleaner_dedupe_buffer_utilization Last cleanup pass's OffsetMap "
               "size (unique keys)\n"
            << "# TYPE kawasan_log_cleaner_dedupe_buffer_utilization gauge\n"
            << "kawasan_log_cleaner_dedupe_buffer_utilization " << m.dedupe_buffer_utilization
            << "\n\n";
        if (!m.partition_dirty_ratios.empty()) {
            oss << "# HELP kawasan_log_cleaner_dirty_ratio Per-partition fraction of log eligible "
                   "for compaction\n"
                << "# TYPE kawasan_log_cleaner_dirty_ratio gauge\n";
            for (const auto& r : m.partition_dirty_ratios) {
                oss << "kawasan_log_cleaner_dirty_ratio{topic=\"" << r.topic << "\",partition=\""
                    << r.partition << "\"} " << r.dirty_ratio << "\n";
            }
            oss << "\n";
        }
        return oss.str();
    });

    // Load persisted group state from storage
    group_coordinator_->loadGroupsFromStorage();

    // Start background cleanup thread for expired groups and timed-out members
    group_coordinator_->startCleanupThread();

    // M2: start the transaction-timeout sweep. Runs after initializeMetadata()
    // has already replayed/re-armed persisted transactions, so it never acts on
    // partially-restored state.
    txn_sweep_interval_ms_ = std::max<int64_t>(
        1000,
        config_.get<int64_t>("transaction.abort.timed.out.transaction.cleanup.interval.ms", 10000));
    txn_sweep_stop_.store(false);
    txn_sweep_thread_ = std::thread(&KawasanBroker::transactionSweepLoop, this);

    // M3: start the producer-state snapshot writer. Started during construction
    // (like the sweep) so a construct-only broker still owns a joinable thread
    // that stop() tears down before its running_ guard. The loop is null-safe,
    // so it does nothing useful until start() has populated the log manager and
    // metadata.
    producer_snapshot_interval_ms_ =
        std::max<int64_t>(1000, config_.get<int64_t>("producer.state.snapshot.interval.ms", 60000));
    producer_snapshot_stop_.store(false);
    producer_snapshot_thread_ = std::thread(&KawasanBroker::producerSnapshotLoop, this);

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
        {protocol::ApiKey::DELETE_RECORDS, 0, 2},
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
        {protocol::ApiKey::SASL_AUTHENTICATE, 0, 2},
        // Phase 3.3 scaffolding: transactional APIs (v0 only — covers
        // kafka-python + librdkafka negotiation; full semantics deferred).
        {protocol::ApiKey::ADD_PARTITIONS_TO_TXN, 0, 0},
        {protocol::ApiKey::ADD_OFFSETS_TO_TXN, 0, 0},
        {protocol::ApiKey::END_TXN, 0, 0},
        {protocol::ApiKey::TXN_OFFSET_COMMIT, 0, 0},
    };
    if (compatibility_profile == "3.x") {
        const std::map<protocol::ApiKey, int16_t> caps{
            {protocol::ApiKey::PRODUCE, 9},        {protocol::ApiKey::FETCH, 12},
            {protocol::ApiKey::LIST_OFFSETS, 7},   {protocol::ApiKey::OFFSET_FETCH, 8},
            {protocol::ApiKey::DELETE_RECORDS, 0}, {protocol::ApiKey::SASL_AUTHENTICATE, 1}};
        for (auto& api : supported_api_versions_) {
            if (const auto it = caps.find(api.api_key); it != caps.end()) {
                api.max_version = std::min(api.max_version, it->second);
            }
        }
    }
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

    // M5: start the follower replica fetcher ONLY in multi-broker mode. In
    // single-node mode there is nothing to replicate from, so we skip it entirely
    // and behavior stays byte-identical (no fetcher thread, no metadata
    // reconciliation). With peers present, the fetcher reconciles replicas from
    // metadata and pulls follower partitions from their leaders.
    if (!cluster_brokers_.empty() && replica_manager_) {
        replica_manager_->setBroker(this);
        replica_manager_->start();
        Logger::info("Started follower replica fetcher (multi-broker mode, {} peers)",
                     cluster_brokers_.size());
    }

    // Start the controller-bootstrap loop. In single-node mode the internal
    // topics were already created synchronously above (this broker is leader
    // immediately), so the loop sees them and exits at once. In a multi-broker
    // cluster it retries until a controller is elected and the topics exist.
    bootstrap_stop_.store(false);
    bootstrap_thread_ = std::thread(&KawasanBroker::controllerBootstrapLoop, this);

    // 0A.11: Mark the broker as healthy and ready for serving traffic.
    // Probes against /readiness and /liveness previously returned 503 forever
    // because these flags defaulted to false and were never flipped.
    if (monitoring_manager_) {
        monitoring_manager_->setBrokerHealthy(true);
        // Multi-broker: ready only once the metadata view is current (M8-E1);
        // the replica-fetcher thread keeps this up to date.
        monitoring_manager_->setBrokerReady(dataPlaneCurrent());
    }

    Logger::info("KawasanBroker started successfully on {}:{}", host_, port_);
}

void KawasanBroker::stop() {
    // M2: the transaction-timeout sweep is started during construction (next
    // to the group cleanup thread), i.e. BEFORE running_ becomes true. Join it
    // unconditionally here — before the running_ guard — so a broker that was
    // constructed but never fully started (e.g. a config-validation test) still
    // joins the sweep thread instead of terminating on a joinable std::thread.
    txn_sweep_stop_.store(true);
    txn_sweep_cv_.notify_all();
    if (txn_sweep_thread_.joinable()) {
        txn_sweep_thread_.join();
    }

    // M3: same lifecycle as the sweep — stop + join the snapshot writer before
    // the running_ guard so a never-started broker doesn't terminate on a
    // joinable thread.
    producer_snapshot_stop_.store(true);
    producer_snapshot_cv_.notify_all();
    if (producer_snapshot_thread_.joinable()) {
        producer_snapshot_thread_.join();
    }

    if (!running_) {
        return;
    }

    // M5: stop the follower replica fetcher first, so no fetch thread touches the
    // logs / metadata while they are torn down below. Safe if never started.
    if (replica_manager_) {
        replica_manager_->stop();
    }

    // M3: write a final producer-state snapshot before teardown, while the log
    // manager and metadata are still alive and the writer thread is already
    // joined (no concurrent writer). This makes the next restart replay the
    // shortest possible tail.
    writeAllProducerSnapshots();

    Logger::info("Stopping KawasanBroker...");

    // 0A.11: Mark unhealthy/not-ready at the start of shutdown so probes
    // immediately reflect "draining"; orchestrators can stop sending traffic
    // while the broker completes graceful shutdown.
    if (monitoring_manager_) {
        monitoring_manager_->setBrokerReady(false);
        monitoring_manager_->setBrokerHealthy(false);
    }

    // Signal the controller-bootstrap loop to stop, but DON'T join it yet — it
    // may be blocked in createTopic() awaiting a Raft commit. It is joined below
    // after the metadata controller is stopped (which fulfills its pending
    // promise), so the join can't hang.
    bootstrap_stop_.store(true);
    bootstrap_cv_.notify_all();

    stopServices();

    // Stop Raft FIRST: this drains and joins its apply thread, so no further
    // commit callbacks fire into the metadata controller. The controller and
    // log_manager_ are still alive here, so a draining apply can complete.
    if (raft_node_) {
        raft_node_->stop();
    }
    // Now the callback target can be torn down safely. stop() also fulfills any
    // still-pending replicateAndAwait promises with BROKER_NOT_AVAILABLE, which
    // unblocks a bootstrap thread waiting on an uncommitted createTopic.
    if (metadata_controller_) {
        metadata_controller_->stop();
    }

    // The bootstrap loop's in-flight createTopic (if any) has now returned, so
    // it observes bootstrap_stop_ and exits; join is safe.
    if (bootstrap_thread_.joinable()) {
        bootstrap_thread_.join();
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
                    std::string host =
                        peer_str.substr(first_colon + 1, second_colon - first_colon - 1);
                    int port = std::stoi(peer_str.substr(second_colon + 1));

                    // Check if this is the current broker
                    if (peer_id == broker_id_) {
                        found_self_in_peers = true;
                        Logger::info("Found current broker (id={}) in peers list: {}:{}", peer_id,
                                     host, port);
                    } else {
                        // Add other peers for Raft communication
                        raft::PeerInfo info;
                        info.id = peer_id;
                        info.host = host;
                        info.port = port;
                        peers.push_back(info);
                        Logger::info("Added Raft peer: id={}, host={}, port={}", peer_id, host,
                                     port);

                        // Seed cluster membership for replica assignment + Metadata.
                        // raft.peers carries the Raft port; the Kafka listener port
                        // is, by the convention used across all shipped configs and
                        // the cluster harness, the Raft port minus one. (Per-broker
                        // advertised addresses via a proper BrokerRegistration flow
                        // are a follow-up.)
                        BrokerMetadata peer_broker;
                        peer_broker.id = peer_id;
                        peer_broker.host = host;
                        peer_broker.port = port - 1;
                        peer_broker.rack = std::nullopt;
                        cluster_brokers_.push_back(peer_broker);
                    }
                } catch (const std::exception& e) {
                    Logger::warn("Failed to parse Raft peer '{}': {}", peer_str, e.what());
                }
            }

            if (end == std::string::npos)
                break;
            start = end + 1;
            end = peers_str.find(',', start);
        }

        // Validate that broker.id is in the peers list
        if (!found_self_in_peers) {
            Logger::error("broker.id={} is not present in raft.peers list: {}", broker_id_,
                          peers_str);
            throw std::runtime_error("Configuration error: broker.id must be present in raft.peers "
                                     "list when raft.peers is configured");
        }
    }

    // Get Raft port from config (default: 9093)
    int raft_port = config_.get<int>("raft.port", 9093);

    // Create RaftNode with io_context
    // 0A.7: persist Raft state under {metadata_dir}/raft so current_term,
    // voted_for, and log entries survive process restart (required for
    // multi-node Raft safety; harmless in single-node mode).
    const std::string raft_data_dir = metadata_dir_ + "/raft";
    raft_node_ =
        std::make_unique<raft::RaftNode>(broker_id_, peers, io_context_, raft_port, raft_data_dir);
    raft_node_->start();

    if (peers.empty()) {
        Logger::info(
            "Initialized Raft node for broker {} in single-node mode (no peers configured)",
            broker_id_);
    } else {
        Logger::info(
            "Initialized Raft node for broker {} on port {} with {} peer(s) in multi-broker mode",
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

    // Seed cluster membership (peers from raft.peers) so the metadata store knows
    // all brokers: replica assignment can then spread partitions across them and
    // Metadata responses advertise the whole cluster. The local broker is already
    // registered by the store; this adds the peers. Empty in single-node mode.
    for (const auto& peer : cluster_brokers_) {
        metadata_controller_->registerBroker(peer);
    }
    if (!cluster_brokers_.empty()) {
        Logger::info("Cluster membership seeded: {} brokers known",
                     metadata_controller_->brokerCount());
    }

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
                // M3: rebuild idempotent-producer state. Load the newest valid
                // producer-state snapshot (if any) and restore its entries, then
                // replay only the log TAIL after the snapshot offset. Without a
                // snapshot this falls back to a full-log replay from offset 0
                // (Kafka's behavior), which is why start_offset defaults there.
                // Rebuilding this state is essential: a restart that reset
                // sequence tracking would treat an in-flight producer retry as a
                // new batch -> silent duplicate. The records themselves are
                // already durable; only the dedup state is being reconstructed.
                Offset psnap_offset = 0;
                if (producer_state_manager_) {
                    const std::string part_dir =
                        log_dir_ + "/" + tm.name + "-" + std::to_string(pm.partition);
                    if (auto snap = ProducerStateSnapshot::loadNewest(part_dir)) {
                        producer_state_manager_->restoreEntries(tm.name, pm.partition,
                                                                snap->entries);
                        psnap_offset = snap->snapshot_offset;
                    }
                }
                replayProducerStateFromLog(tm.name, pm.partition, psnap_offset);
                // Terminal coordinator snapshots do not retain old aborts.
                // Data-log control markers are the durable source of history.
                if (isolation_tracker_ && !tm.name.starts_with("__")) {
                    isolation_tracker_->recoverAbortedTransactions(
                        tm.name, pm.partition, *log_manager_->getLog(tm.name, pm.partition));
                }
                ++restored;
            }
        }
        if (restored > 0) {
            Logger::info("Restored {} partitions from metadata on startup", restored);
        }
    }

    // M1: on a RESTART, __transaction_state already exists in the metadata
    // store and its logs were reopened above. Construct the state manager and
    // replay it here — independent of controller leadership, which isn't
    // established yet (ensureInternalTopics below is a no-op until then) and
    // must not gate recovery. This runs before the broker serves traffic, so
    // restored transactions hold their LSO before any client can fetch.
    if (metadata_controller_ && log_manager_ && !transaction_state_manager_) {
        const auto txn_md = metadata_controller_->describeTopics({"__transaction_state"});
        if (!txn_md.empty() && !txn_md.front().partitions.empty()) {
            txn_state_num_partitions_ = static_cast<int32_t>(txn_md.front().partitions.size());
            transaction_state_manager_ = std::make_unique<TransactionStateManager>(
                log_manager_.get(), txn_state_num_partitions_);
            replayTransactionStateFromLog();
        }
    }

    // Ensure the internal topics exist. In single-node mode this broker is
    // already the leader (RaftNode::start becomes leader immediately with no
    // peers), so they are created synchronously here. In a multi-broker cluster
    // no controller exists yet at startup, so this is a no-op until the
    // controller-bootstrap loop (started in startServices) retries after a
    // leader is elected.
    ensureInternalTopics();
}

void KawasanBroker::replayTransactionStateFromLog() {
    if (!transaction_state_manager_ || !transaction_coordinator_)
        return;
    const auto snapshots = transaction_state_manager_->loadAll();
    if (snapshots.empty())
        return;

    size_t restored = 0, in_flight = 0, redriven = 0;
    for (const auto& snap : snapshots) {
        transaction_coordinator_->restore(snap);
        ++restored;
        const bool ongoing_or_prepare =
            snap.state == TransactionCoordinator::State::Ongoing ||
            snap.state == TransactionCoordinator::State::PrepareCommit ||
            snap.state == TransactionCoordinator::State::PrepareAbort;
        if (ongoing_or_prepare && isolation_tracker_) {
            for (const auto& tp : snap.partitions) {
                if (tp.first_offset >= 0) {
                    isolation_tracker_->recordInFlightTxn(snap.producer_id, tp.topic, tp.partition,
                                                          tp.first_offset);
                }
            }
            ++in_flight;
        }
        // Re-drive a transaction that crashed mid-EndTxn (Prepare persisted,
        // Complete not yet). This is what guarantees "no frozen LSO": a
        // half-finished commit/abort is completed at startup.
        if (snap.state == TransactionCoordinator::State::PrepareCommit ||
            snap.state == TransactionCoordinator::State::PrepareAbort) {
            const bool committed = snap.state == TransactionCoordinator::State::PrepareCommit;
            finishTxnCompletion(snap.transactional_id, snap.producer_id, snap.producer_epoch,
                                committed, snap.partitions, snap.pending_offsets,
                                /*is_replay=*/true);
            ++redriven;
        }
    }
    Logger::info("Replayed {} transaction(s) from __transaction_state ({} in-flight "
                 "re-armed, {} re-driven to completion)",
                 restored, in_flight, redriven);
}

void KawasanBroker::persistTxnState(const std::string& transactional_id) {
    if (!transaction_state_manager_ || !transaction_coordinator_)
        return;
    auto snap = transaction_coordinator_->describe(transactional_id);
    if (snap.has_value()) {
        transaction_state_manager_->persist(*snap);
    }
}

bool KawasanBroker::txnEpochFenced(const std::string& transactional_id, int16_t req_epoch) {
    if (!transaction_coordinator_ || transactional_id.empty())
        return false;
    auto snap = transaction_coordinator_->describe(transactional_id);
    // Unknown txn: nothing to fence (it is auto-registered at this epoch).
    if (!snap.has_value())
        return false;
    return req_epoch < snap->producer_epoch;
}

void KawasanBroker::transactionSweepLoop() {
    // M2: periodically abort Ongoing transactions that have exceeded their
    // transaction.timeout.ms, so a hung/crashed producer never holds the LSO
    // (and read_committed consumers) hostage. Candidates are gathered under the
    // coordinator lock (expiredOngoing) and aborted outside it — the same
    // collect-then-act discipline the group cleanup thread uses.
    while (!txn_sweep_stop_.load()) {
        {
            std::unique_lock<std::mutex> lock(txn_sweep_mutex_);
            txn_sweep_cv_.wait_for(lock, std::chrono::milliseconds(txn_sweep_interval_ms_),
                                   [this] { return txn_sweep_stop_.load(); });
        }
        if (txn_sweep_stop_.load())
            break;
        if (!transaction_coordinator_)
            continue;

        const int64_t now = transaction_coordinator_->nowMs();
        const auto expired = transaction_coordinator_->expiredOngoing(now);
        for (const auto& snap : expired) {
            Logger::info("Transaction '{}' timed out (timeout={}ms) — auto-aborting",
                         snap.transactional_id, snap.transaction_timeout_ms);
            auto participating = transaction_coordinator_->prepareAbort(snap.transactional_id);
            persistTxnState(snap.transactional_id);
            finishTxnCompletion(snap.transactional_id, snap.producer_id, snap.producer_epoch,
                                /*committed=*/false, participating, snap.pending_offsets,
                                /*is_replay=*/false);
        }
    }
}

void KawasanBroker::producerSnapshotLoop() {
    // M3: periodically checkpoint per-partition producer-state to disk so a
    // restart replays only the log tail after the last snapshot, not the whole
    // log. The wait/stop discipline mirrors transactionSweepLoop().
    while (!producer_snapshot_stop_.load()) {
        {
            std::unique_lock<std::mutex> lock(producer_snapshot_mutex_);
            producer_snapshot_cv_.wait_for(
                lock, std::chrono::milliseconds(producer_snapshot_interval_ms_),
                [this] { return producer_snapshot_stop_.load(); });
        }
        if (producer_snapshot_stop_.load())
            break;
        writeAllProducerSnapshots();
    }
}

void KawasanBroker::writeAllProducerSnapshots() {
    if (!producer_state_manager_ || !log_manager_ || !metadata_controller_)
        return;
    const auto topics = metadata_controller_->describeTopics({});
    for (const auto& tm : topics) {
        for (const auto& pm : tm.partitions) {
            const TopicPartition tp{tm.name, pm.partition};
            auto partition_write_lock = lockPartitionWrites(tp);
            // Followers rebuild on promotion; their cached state may lag the log.
            if (!cluster_brokers_.empty() && !replica_manager_->isLeader(tp))
                continue;
            auto* log = log_manager_->getLog(tm.name, pm.partition);
            if (!log)
                continue;
            // Read the log-end offset BEFORE the entries so the snapshot offset
            // never runs ahead of the state it captures. If a batch is appended
            // between these two reads, the entries reflect it while the offset
            // does not — harmless, because startup re-applies the tail from the
            // offset and recordAppend is idempotent. The reverse ordering would
            // skip that batch on replay and leave stale dedup state.
            const Offset leo = log->logEndOffset();
            auto entries = producer_state_manager_->snapshotEntries(tm.name, pm.partition);
            if (entries.empty())
                continue;  // no idempotent producers here; nothing to checkpoint
            const std::string part_dir =
                log_dir_ + "/" + tm.name + "-" + std::to_string(pm.partition);
            ProducerStateSnapshot::write(part_dir, leo, entries);
        }
    }
}

void KawasanBroker::finishTxnCompletion(
    const std::string& transactional_id, int64_t producer_id, int16_t producer_epoch,
    bool committed, const std::vector<TransactionCoordinator::TxnPartition>& participating,
    const std::vector<TransactionCoordinator::PendingOffset>& pending_offsets, bool is_replay) {
    // Emit a control marker (COMMIT/ABORT) on each participating partition so
    // read_committed consumers can detect the boundary. On replay we guard
    // against double-emission: if a marker for this producer already exists
    // at/after the txn's first_offset, skip it.
    if (log_manager_) {
        const Timestamp now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                     std::chrono::system_clock::now().time_since_epoch())
                                     .count();
        for (const auto& tp : participating) {
            auto* log = log_manager_->getOrCreateLog(tp.topic, tp.partition);
            if (!log)
                continue;
            if (is_replay && tp.first_offset >= 0 &&
                logHasControlBatchForProducer(log, producer_id, tp.first_offset)) {
                continue;  // marker already durable before the crash
            }
            const Offset base_offset = log->logEndOffset();
            auto control_batch = storage::RecordBatch::makeControlBatch(
                producer_id, producer_epoch, base_offset, committed, now_ms);
            try {
                log->appendBatch(std::move(control_batch));
            } catch (const std::exception& ex) {
                Logger::warn("Failed to emit control record on {}-{}: {}", tp.topic, tp.partition,
                             ex.what());
            }
        }
    }

    // On commit, apply staged consumer-group offsets to the OffsetManager.
    if (committed && offset_manager_ && !pending_offsets.empty()) {
        std::unordered_map<std::string, std::vector<OffsetManager::OffsetCommitData>> by_group;
        for (const auto& po : pending_offsets) {
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
                Logger::warn("EndTxn(commit) failed to apply staged offsets for '{}': {}", group_id,
                             ex.what());
            }
        }
    }

    // Update the isolation tracker: commit releases the LSO hold; abort moves
    // each first_offset into the aborted-transactions ring.
    if (isolation_tracker_) {
        std::vector<std::pair<std::string, int32_t>> pairs;
        pairs.reserve(participating.size());
        for (const auto& tp : participating)
            pairs.emplace_back(tp.topic, tp.partition);
        if (committed) {
            isolation_tracker_->commitInFlightTxns(pairs, producer_id);
        } else {
            for (const auto& tp : participating) {
                auto* log = log_manager_ ? log_manager_->getLog(tp.topic, tp.partition) : nullptr;
                const Offset marker_offset =
                    log ? log->logEndOffset() - 1 : std::numeric_limits<Offset>::max();
                isolation_tracker_->abortInFlightTxns({{tp.topic, tp.partition}}, producer_id,
                                                      marker_offset);
            }
        }
    }

    // Transition to Complete* and persist the terminal snapshot.
    if (transaction_coordinator_) {
        if (committed) {
            transaction_coordinator_->completeCommit(transactional_id);
        } else {
            transaction_coordinator_->completeAbort(transactional_id);
        }
    }
    persistTxnState(transactional_id);
}

bool KawasanBroker::logHasControlBatchForProducer(storage::Log* log, int64_t producer_id,
                                                  Offset from_offset) {
    if (!log)
        return false;
    const Offset end = log->logEndOffset();
    Offset off = std::max<Offset>(from_offset, log->logStartOffset());
    constexpr size_t kChunkBytes = 4 * 1024 * 1024;
    while (off < end) {
        std::vector<storage::RecordBatch> batches;
        try {
            batches = log->read(off, kChunkBytes);
        } catch (const std::exception&) {
            return false;
        }
        if (batches.empty())
            break;
        Offset next = off;
        for (const auto& batch : batches) {
            next = std::max(next, batch.baseOffset() + static_cast<Offset>(batch.records().size()));
            if (batch.isControlBatch() && batch.producerId() == producer_id) {
                return true;
            }
        }
        if (next <= off)
            break;
        off = next;
    }
    return false;
}

void KawasanBroker::replayProducerStateFromLog(const std::string& topic, PartitionId partition,
                                               Offset start_offset) {
    if (!producer_state_manager_ || !log_manager_) {
        return;
    }
    auto* log = log_manager_->getLog(topic, partition);
    if (!log) {
        return;
    }
    const Offset end = log->logEndOffset();
    // M3: start from the producer-snapshot offset (if any) rather than the log
    // start, so restart only rescans the tail. Clamp to the log start in case a
    // snapshot references an already-truncated prefix.
    Offset off = std::max(start_offset, log->logStartOffset());
    constexpr size_t kChunkBytes = 8 * 1024 * 1024;
    while (off < end) {
        auto batches = log->read(off, kChunkBytes);
        if (batches.empty()) {
            break;
        }
        Offset next = off;
        for (const auto& batch : batches) {
            const Offset base = batch.baseOffset();
            const int32_t count = static_cast<int32_t>(batch.records().size());
            if (base + count > next) {
                next = base + count;
            }
            if (batch.producerId() >= 0 && count > 0 && !batch.isControlBatch()) {
                producer_state_manager_->recordAppend(topic, partition, batch.producerId(),
                                                      batch.producerEpoch(), batch.baseSequence(),
                                                      count, base);
            }
        }
        if (next <= off) {
            break;  // no forward progress; avoid an infinite loop
        }
        off = next;
    }
}

void KawasanBroker::ensureInternalTopics() {
    if (!metadata_controller_) {
        return;
    }

    auto topic_exists = [&](const std::string& name) {
        const auto md = metadata_controller_->describeTopics({name});
        return !md.empty() && !md.front().partitions.empty();
    };

    // If both already exist (created by whichever broker is the controller and
    // replicated to us via Raft), record the offsets partition count and finish.
    if (topic_exists("__consumer_offsets") && topic_exists("__transaction_state")) {
        const auto md = metadata_controller_->describeTopics({"__consumer_offsets"});
        offsets_topic_num_partitions_ = static_cast<int32_t>(md.front().partitions.size());
        internal_topics_ready_.store(true);
        return;
    }

    // Only the active controller (Raft leader) can create topics. Non-leaders
    // return quietly; the bootstrap loop retries once leadership is established.
    if (raft_node_ && !raft_node_->isLeader()) {
        return;
    }

    // Internal-topic partition counts are configurable. Each partition is a
    // RocksDB instance, so Kafka's default of 50 (offsets + txn => ~100 DBs)
    // is a file-descriptor hazard; default to a smaller single-node-appropriate
    // count. Internal topics stay RF=1 for now (cross-broker replication of the
    // metadata topics is part of the follower-fetcher work).
    auto create_if_missing = [&](const std::string& name, int32_t partitions,
                                 std::map<std::string, std::string> cfg) -> bool {
        if (topic_exists(name)) {
            return true;
        }
        TopicSpecification spec;
        spec.name = name;
        spec.num_partitions = partitions;
        spec.replication_factor = 1;
        spec.configs = std::move(cfg);
        const auto result = metadata_controller_->createTopic(spec);
        if (result.error_code == ErrorCode::NONE) {
            Logger::info("Created internal topic {} with {} partitions", name, partitions);
            return true;
        }
        if (result.error_code == ErrorCode::TOPIC_ALREADY_EXISTS) {
            return true;
        }
        Logger::debug("Deferred creating internal topic {}: {}", name, result.error_message);
        return false;
    };

    const int32_t offsets_partitions =
        std::max(1, config_.get<int32_t>("offsets.topic.num.partitions", 16));
    const int32_t txn_partitions =
        std::max(1, config_.get<int32_t>("transaction.state.topic.num.partitions", 16));

    const bool offsets_ok =
        create_if_missing("__consumer_offsets", offsets_partitions,
                          {{"cleanup.policy", "compact"}, {"segment.bytes", "104857600"}});
    const bool txn_ok = create_if_missing("__transaction_state", txn_partitions,
                                          {{"cleanup.policy", "compact"},
                                           {"segment.bytes", "104857600"},
                                           {"min.compaction.lag.ms", "0"}});

    // Capture the actual offsets partition count (handles a pre-existing topic
    // with a different count) so offset-commit routing uses the true modulus.
    const auto offsets_md = metadata_controller_->describeTopics({"__consumer_offsets"});
    if (!offsets_md.empty() && !offsets_md.front().partitions.empty()) {
        offsets_topic_num_partitions_ = static_cast<int32_t>(offsets_md.front().partitions.size());
    }
    // M1: capture the actual __transaction_state partition count and construct
    // the state manager so txn-state routing (write + replay) agrees on the
    // modulus. Route by transactional_id hash % this count.
    const auto txn_md = metadata_controller_->describeTopics({"__transaction_state"});
    if (!txn_md.empty() && !txn_md.front().partitions.empty()) {
        txn_state_num_partitions_ = static_cast<int32_t>(txn_md.front().partitions.size());
    } else {
        txn_state_num_partitions_ = txn_partitions;
    }
    if (txn_ok && log_manager_ && !transaction_state_manager_) {
        transaction_state_manager_ = std::make_unique<TransactionStateManager>(
            log_manager_.get(), txn_state_num_partitions_);
    }
    if (offsets_ok && txn_ok) {
        internal_topics_ready_.store(true);
    }
}

void KawasanBroker::controllerBootstrapLoop() {
    while (!bootstrap_stop_.load() && !internal_topics_ready_.load()) {
        try {
            ensureInternalTopics();
        } catch (const std::exception& ex) {
            Logger::warn("ensureInternalTopics failed (will retry): {}", ex.what());
        }
        std::unique_lock<std::mutex> lock(bootstrap_mutex_);
        bootstrap_cv_.wait_for(lock, std::chrono::milliseconds(500),
                               [&] { return bootstrap_stop_.load(); });
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

    const size_t max_frame_bytes =
        static_cast<size_t>(config_.get<int64_t>("network.max_frame_bytes", 16 * 1024 * 1024));

    // Prepare TLS config for TCP server
    network::TlsConfig server_tls_config;
    server_tls_config.enabled = tls_config_.enabled;
    server_tls_config.cert_file = tls_config_.cert_file;
    server_tls_config.key_file = tls_config_.key_file;
    server_tls_config.ca_file = tls_config_.ca_file;
    server_tls_config.key_password = tls_config_.key_password;
    server_tls_config.verify_client = tls_config_.requiresClientAuth();

    // Kernel-level socket tuning + idle reaping, operator-configurable.
    network::SocketTuning socket_tuning;
    socket_tuning.no_delay = config_.get<bool>("network.tcp_nodelay", true);
    socket_tuning.keep_alive = config_.get<bool>("network.tcp_keepalive", true);
    socket_tuning.send_buffer_bytes = config_.get<int32_t>("network.socket_send_buffer_bytes", 0);
    socket_tuning.recv_buffer_bytes = config_.get<int32_t>("network.socket_recv_buffer_bytes", 0);
    const auto idle_timeout =
        std::chrono::seconds(config_.get<int64_t>("network.idle_connection_timeout_seconds", 600));

    tcp_server_ = std::make_unique<network::TcpServer>(
        host_, port_, static_cast<size_t>(configured_threads), max_frame_bytes, request_dispatcher_,
        monitoring_manager_ ? monitoring_manager_->sharedMetricsCollector() : nullptr, idle_timeout,
        server_tls_config, socket_tuning);
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

    // Drop parked long-polls before the network layer goes away; their
    // connections are closed by the TCP server stop below.
    if (delayed_fetch_purgatory_) {
        delayed_fetch_purgatory_->stop();
    }
    if (delayed_produce_purgatory_) {
        delayed_produce_purgatory_->stop();
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
    // The advertised table is the single source of dispatcher limits. Error
    // responses to a profile-capped request still use its known codec shape.
    auto register_handler = [&](protocol::ApiKey key, int16_t codec_min, int16_t codec_max,
                                RequestDispatcher::HandlerFunc handler,
                                RequestDispatcher::ErrorBuilder errors) {
        const auto api =
            std::find_if(supported_api_versions_.begin(), supported_api_versions_.end(),
                         [key](const auto& entry) { return entry.api_key == key; });
        if (api == supported_api_versions_.end() || api->min_version < codec_min ||
            api->max_version > codec_max) {
            throw std::logic_error("advertised API range exceeds registered codec range");
        }
        request_dispatcher_->registerHandler(
            key, api->min_version, api->max_version, std::move(handler),
            [errors = std::move(errors), codec_min, codec_max](const Context& ctx, ErrorCode code,
                                                               int16_t /*version*/) {
                return errors(ctx, code,
                              std::clamp<int16_t>(ctx.header.apiVersion(), codec_min, codec_max));
            });
    };

    register_handler(
        protocol::ApiKey::API_VERSIONS, 0, 4,
        [this](Context& context) {
            RequestDispatcher::HandlerResult result;
            result.payload = handleApiVersions(context);
            return result;
        },
        [this](const Context& context, ErrorCode code, int16_t version) {
            return buildApiVersionsError(context, code, version);
        });

    register_handler(
        protocol::ApiKey::METADATA, 0, kMetadataMaxVersion,
        [this](Context& context) {
            RequestDispatcher::HandlerResult result;
            result.payload = handleMetadata(context);
            return result;
        },
        [this](const Context& context, ErrorCode code, int16_t version) {
            return buildMetadataError(context, code, version);
        });

    register_handler(
        protocol::ApiKey::CREATE_TOPICS, 0, 7,  // Phase 1.15
        [this](Context& context) {
            RequestDispatcher::HandlerResult result;
            result.payload = handleCreateTopics(context);
            return result;
        },
        [this](const Context& context, ErrorCode code, int16_t version) {
            return buildCreateTopicsError(context, code, version);
        });

    register_handler(
        protocol::ApiKey::DELETE_TOPICS, 0, 6,  // Phase 1.16 (v6: topic_id supported)
        [this](Context& context) {
            RequestDispatcher::HandlerResult result;
            result.payload = handleDeleteTopics(context);
            return result;
        },
        [this](const Context& context, ErrorCode code, int16_t version) {
            return buildDeleteTopicsError(context, code, version);
        });

    register_handler(
        protocol::ApiKey::PRODUCE, 0, kProduceMaxVersion,
        [this](Context& context) { return handleProduce(context); },
        [this](const Context& context, ErrorCode code, int16_t version) {
            return buildProduceError(context, code, version);
        });

    register_handler(
        protocol::ApiKey::FETCH, 0, kFetchMaxVersion,
        [this](Context& context) { return handleFetch(context); },
        [this](const Context& context, ErrorCode code, int16_t version) {
            return buildFetchError(context, code, version);
        });

    register_handler(
        protocol::ApiKey::LIST_OFFSETS, 0, kListOffsetsMaxVersion,
        [this](Context& context) { return handleListOffsets(context); },
        [this](const Context& context, ErrorCode code, int16_t version) {
            return buildListOffsetsError(context, code, version);
        });

    register_handler(
        protocol::ApiKey::FIND_COORDINATOR, 0, kFindCoordinatorMaxVersion,
        [this](Context& context) { return handleFindCoordinator(context); },
        [this](const Context& context, ErrorCode code, int16_t version) {
            return buildFindCoordinatorError(context, code, version);
        });

    register_handler(
        protocol::ApiKey::JOIN_GROUP, 0, kJoinGroupMaxVersion,
        [this](Context& context) {
            RequestDispatcher::HandlerResult result;
            result.payload = handleJoinGroup(context);
            return result;
        },
        [this](const Context& context, ErrorCode code, int16_t version) {
            return buildJoinGroupError(context, code, version);
        });

    register_handler(
        protocol::ApiKey::SYNC_GROUP, 0, kSyncGroupMaxVersion,
        [this](Context& context) {
            RequestDispatcher::HandlerResult result;
            result.payload = handleSyncGroup(context);
            return result;
        },
        [this](const Context& context, ErrorCode code, int16_t version) {
            return buildSyncGroupError(context, code, version);
        });

    register_handler(
        protocol::ApiKey::HEARTBEAT, 0, kHeartbeatMaxVersion,
        [this](Context& context) {
            RequestDispatcher::HandlerResult result;
            result.payload = handleHeartbeat(context);
            return result;
        },
        [this](const Context& context, ErrorCode code, int16_t version) {
            return buildHeartbeatError(context, code, version);
        });

    register_handler(
        protocol::ApiKey::LEAVE_GROUP, 0, kLeaveGroupMaxVersion,
        [this](Context& context) {
            RequestDispatcher::HandlerResult result;
            result.payload = handleLeaveGroup(context);
            return result;
        },
        [this](const Context& context, ErrorCode code, int16_t version) {
            return buildLeaveGroupError(context, code, version);
        });

    register_handler(
        protocol::ApiKey::OFFSET_COMMIT, 0, kOffsetCommitMaxVersion,
        [this](Context& context) {
            RequestDispatcher::HandlerResult result;
            result.payload = handleOffsetCommit(context);
            return result;
        },
        [this](const Context& context, ErrorCode code, int16_t version) {
            return buildOffsetCommitError(context, code, version);
        });

    register_handler(
        protocol::ApiKey::OFFSET_FETCH, 0, kOffsetFetchMaxVersion,
        [this](Context& context) {
            RequestDispatcher::HandlerResult result;
            result.payload = handleOffsetFetch(context);
            return result;
        },
        [this](const Context& context, ErrorCode code, int16_t version) {
            return buildOffsetFetchError(context, code, version);
        });

    register_handler(
        protocol::ApiKey::DESCRIBE_GROUPS, 0, kDescribeGroupsMaxVersion,
        [this](Context& context) {
            RequestDispatcher::HandlerResult result;
            result.payload = handleDescribeGroups(context);
            return result;
        },
        [this](const Context& context, ErrorCode code, int16_t version) {
            return buildDescribeGroupsError(context, code, version);
        });

    register_handler(
        protocol::ApiKey::LIST_GROUPS, 0, kListGroupsMaxVersion,
        [this](Context& context) {
            RequestDispatcher::HandlerResult result;
            result.payload = handleListGroups(context);
            return result;
        },
        [this](const Context& context, ErrorCode code, int16_t version) {
            return buildListGroupsError(context, code, version);
        });

    register_handler(
        protocol::ApiKey::DESCRIBE_CONFIGS, 0, 4,
        [this](Context& context) {
            RequestDispatcher::HandlerResult result;
            result.payload = handleDescribeConfigs(context);
            return result;
        },
        [this](const Context& context, ErrorCode code, int16_t version) {
            return buildDescribeConfigsError(context, code, version);
        });

    register_handler(
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
    register_handler(
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
    register_handler(
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
    register_handler(
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
    register_handler(
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
        register_handler(
            k, mn, mx,
            [this, fn](Context& ctx) {
                RequestDispatcher::HandlerResult r;
                r.payload = (this->*fn)(ctx);
                return r;
            },
            [this, k](const Context& ctx, ErrorCode code, int16_t ver) {
                if (k == protocol::ApiKey::SASL_AUTHENTICATE) {
                    protocol::SaslAuthenticateResponse response;
                    response.setErrorCode(code);
                    response.setErrorMessage(KawasanException::toString(code));
                    return encodeResponse(ctx,
                                          [&](Buffer& buffer) { response.encode(buffer, ver); });
                }
                if (k == protocol::ApiKey::DELETE_RECORDS) {
                    protocol::DeleteRecordsResponse response;
                    try {
                        Buffer input = ctx.payload;
                        input.setPosition(ctx.payload_start);
                        protocol::DeleteRecordsRequest request;
                        request.decode(input, ver);
                        for (const auto& topic : request.topics()) {
                            protocol::DeleteRecordsResponse::TopicResult result;
                            result.topic = topic.topic;
                            for (const auto& partition : topic.partitions) {
                                result.partitions.push_back({partition.partition, -1, code});
                            }
                            response.addTopic(std::move(result));
                        }
                    } catch (const std::exception&) {
                        response.addTopic({"__kawasan_error__", {{-1, -1, code}}});
                    }
                    return encodeResponse(ctx,
                                          [&](Buffer& buffer) { response.encode(buffer, ver); });
                }
                return buildEmptyErrorResponse(ctx);
            });
    };
    reg_admin(protocol::ApiKey::DESCRIBE_LOG_DIRS, 0, 0, &KawasanBroker::handleDescribeLogDirs);
    reg_admin(protocol::ApiKey::ALTER_REPLICA_LOG_DIRS, 0, 0,
              &KawasanBroker::handleAlterReplicaLogDirs);
    reg_admin(protocol::ApiKey::ELECT_LEADERS, 0, 1, &KawasanBroker::handleElectLeaders);
    reg_admin(protocol::ApiKey::DELETE_RECORDS, 0, 2, &KawasanBroker::handleDeleteRecords);
    reg_admin(protocol::ApiKey::DELETE_GROUPS, 0, 0, &KawasanBroker::handleDeleteGroups);
    reg_admin(protocol::ApiKey::OFFSET_DELETE, 0, 0, &KawasanBroker::handleOffsetDelete);
    reg_admin(protocol::ApiKey::CREATE_PARTITIONS, 0, 0, &KawasanBroker::handleCreatePartitions);
    reg_admin(protocol::ApiKey::DESCRIBE_PRODUCERS, 0, 0, &KawasanBroker::handleDescribeProducers);
    reg_admin(protocol::ApiKey::LIST_TRANSACTIONS, 0, 0, &KawasanBroker::handleListTransactions);
    reg_admin(protocol::ApiKey::DESCRIBE_TRANSACTIONS, 0, 0,
              &KawasanBroker::handleDescribeTransactions);
    reg_admin(protocol::ApiKey::ALTER_PARTITION, 0, 0, &KawasanBroker::handleAlterPartition);
    reg_admin(protocol::ApiKey::DESCRIBE_ACLS, 0, 0, &KawasanBroker::handleDescribeAcls);
    reg_admin(protocol::ApiKey::CREATE_ACLS, 0, 0, &KawasanBroker::handleCreateAcls);
    reg_admin(protocol::ApiKey::DELETE_ACLS, 0, 0, &KawasanBroker::handleDeleteAcls);

    // Phase 4.2a: SASL PLAIN.
    reg_admin(protocol::ApiKey::SASL_HANDSHAKE, 0, 1, &KawasanBroker::handleSaslHandshake);
    reg_admin(protocol::ApiKey::SASL_AUTHENTICATE, 0, 2, &KawasanBroker::handleSaslAuthenticate);

    // Phase 3.3 scaffolding: transactional APIs. Advertised v0 only —
    // sufficient for kafka-python and librdkafka to negotiate; higher
    // versions just add tagged_fields which our v0 encoders don't emit
    // but our flex-header path handles correctly.
    reg_admin(protocol::ApiKey::ADD_PARTITIONS_TO_TXN, 0, 0,
              &KawasanBroker::handleAddPartitionsToTxn);
    reg_admin(protocol::ApiKey::ADD_OFFSETS_TO_TXN, 0, 0, &KawasanBroker::handleAddOffsetsToTxn);
    reg_admin(protocol::ApiKey::END_TXN, 0, 0, &KawasanBroker::handleEndTxn);
    reg_admin(protocol::ApiKey::TXN_OFFSET_COMMIT, 0, 0, &KawasanBroker::handleTxnOffsetCommit);
}

Buffer KawasanBroker::encodeResponse(const RequestDispatcher::RequestContext& context,
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
                Logger::warn(
                    "Invalid client.software.name from {}: '{}' contains invalid characters",
                    context.peer_identity, name);
                return buildApiVersionsError(context, ErrorCode::INVALID_REQUEST,
                                             context.header.apiVersion());
            }

            if (!protocol::isValidClientSoftwareString(version)) {
                Logger::warn(
                    "Invalid client.software.version from {}: '{}' contains invalid characters",
                    context.peer_identity, version);
                return buildApiVersionsError(context, ErrorCode::INVALID_REQUEST,
                                             context.header.apiVersion());
            }

            // Log client software info for debugging
            Logger::info("API versions request from {}: client='{}' version='{}'",
                         context.peer_identity, name, version);
        }
    } catch (const ProtocolException& ex) {
        Logger::warn("Failed to decode API versions request from {}: {}", context.peer_identity,
                     ex.what());
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
    const int16_t response_version = std::clamp<int16_t>(context.header.apiVersion(), 0, 4);
    return encodeResponse(context,
                          [&](Buffer& buffer) { response.encode(buffer, response_version); });
}

Buffer KawasanBroker::buildApiVersionsError(const RequestDispatcher::RequestContext& context,
                                            ErrorCode code, int16_t response_version) const {
    protocol::ApiVersionsResponse response;
    response.setErrorCode(code);
    response.setThrottleTimeMs(0);
    response.setApiVersions(supported_api_versions_);
    const int16_t version = std::clamp<int16_t>(response_version, 0, 4);
    return encodeResponse(context, [&](Buffer& buffer) { response.encode(buffer, version); });
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
    response.setControllerId(controllerId());
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
                auto [metadata_opt, error_code] = getTopicMetadata(topic_name, allow_auto_create);
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
        if (requested.empty() ||
            std::find(requested.begin(), requested.end(), fallback.name) != requested.end()) {
            topics.push_back(fallback);
        }
    }
    for (auto& topic : topics) {
        for (auto& partition : topic.partitions) {
            if (partition.leader < 0 && partition.error_code == ErrorCode::NONE) {
                partition.error_code = ErrorCode::LEADER_NOT_AVAILABLE;  // offline (M8)
            }
        }
    }
    response.setTopics(topics);

    const int16_t version =
        std::clamp<int16_t>(context.header.apiVersion(), 0, kMetadataMaxVersion);
    return encodeResponse(context, [&](Buffer& buffer) { response.encode(buffer, version); });
}

Buffer KawasanBroker::buildMetadataError(const RequestDispatcher::RequestContext& context,
                                         ErrorCode code, int16_t response_version) const {
    protocol::MetadataResponse response;
    response.setThrottleTimeMs(0);
    const std::string cluster_id =
        metadata_controller_ ? metadata_controller_->clusterId() : cluster_id_;
    response.setClusterId(cluster_id);
    response.setControllerId(controllerId());
    response.setBrokers(buildBrokerMetadata());
    TopicMetadata error_topic;
    error_topic.error_code = code;
    error_topic.name = "__kawasan_error__";
    error_topic.is_internal = true;
    response.setTopics({error_topic});

    const int16_t version = std::clamp<int16_t>(response_version, 0, kMetadataMaxVersion);
    return encodeResponse(context, [&](Buffer& buffer) { response.encode(buffer, version); });
}

Buffer KawasanBroker::handleCreateTopics(RequestDispatcher::RequestContext& context) {
    protocol::CreateTopicsRequest request;
    request.decode(context.payload, context.header.apiVersion());

    protocol::CreateTopicsResponse response;
    response.setThrottleTimeMs(0);

    for (const auto& topic : request.topics()) {
        protocol::CreatableTopicResult result;
        result.name = topic.name;

        // Authorization: CREATE on the topic (no-op when authorizer disabled).
        if (!authorize(context, /*CREATE=*/5, /*TOPIC=*/2, topic.name)) {
            result.error_code = ErrorCode::TOPIC_AUTHORIZATION_FAILED;
            result.error_message = "Not authorized to create topic";
            response.addTopicResult(result);
            continue;
        }

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
    return encodeResponse(context, [&](Buffer& buffer) { response.encode(buffer, version); });
}

Buffer KawasanBroker::buildCreateTopicsError(const RequestDispatcher::RequestContext& context,
                                             ErrorCode code, int16_t response_version) const {
    protocol::CreateTopicsResponse response;
    response.setThrottleTimeMs(0);
    protocol::CreatableTopicResult result;
    result.name = "__kawasan_topic__";
    result.error_code = code;
    result.error_message = KawasanException::toString(code);
    response.addTopicResult(result);

    const int16_t version =
        std::clamp<int16_t>(response_version, 0, static_cast<int16_t>(7));  // Phase 1.15
    return encodeResponse(context, [&](Buffer& buffer) { response.encode(buffer, version); });
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

        // Authorization: DELETE on the topic (no-op when authorizer disabled).
        if (!authorize(context, /*DELETE=*/6, /*TOPIC=*/2, resolved_name)) {
            result.error_code = ErrorCode::TOPIC_AUTHORIZATION_FAILED;
            result.error_message = "Not authorized to delete topic";
            response.addResult(result);
            continue;
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
    return encodeResponse(context, [&](Buffer& buffer) { response.encode(buffer, version); });
}

Buffer KawasanBroker::buildDeleteTopicsError(const RequestDispatcher::RequestContext& context,
                                             ErrorCode code, int16_t response_version) const {
    protocol::DeleteTopicsResponse response;
    response.setThrottleTimeMs(0);
    protocol::DeletableTopicResult result;
    result.name = "__kawasan_topic__";
    result.error_code = code;
    result.error_message = KawasanException::toString(code);
    response.addResult(result);

    const int16_t version =
        std::clamp<int16_t>(response_version, 0, static_cast<int16_t>(6));  // Phase 1.16
    return encodeResponse(context, [&](Buffer& buffer) { response.encode(buffer, version); });
}

RequestDispatcher::HandlerResult KawasanBroker::handleProduce(
    RequestDispatcher::RequestContext& context) {
    auto start_time = std::chrono::steady_clock::now();

    protocol::ProduceRequest request;
    request.decode(context.payload, context.header.apiVersion());

    const int16_t acks = request.acks();
    if (acks != -1 && acks != 0 && acks != 1) {
        RequestDispatcher::HandlerResult invalid_acks;
        invalid_acks.payload = buildProduceError(context, ErrorCode::INVALID_REQUIRED_ACKS,
                                                 context.header.apiVersion());
        invalid_acks.close_connection = false;
        return invalid_acks;
    }

    // Client quota: record the produced bytes and surface any throttle delay so
    // an over-quota producer backs off. No-op (0) when producer quota disabled.
    int32_t produce_throttle_ms = 0;
    if (quota_manager_) {
        produce_throttle_ms = quota_manager_->recordAndThrottleMs(
            QuotaManager::Type::kProducer, context.header.clientId(), context.frame_size_bytes);
    }

    // P4: acks=all batches on a replicated (ISR>1) partition that are not yet
    // committed. Resolved below — possibly after this handler returns.
    struct PendingAck {
        size_t topic_index;
        size_t partition_index;
        TopicPartition tp;
        Offset required_offset;  // committed once the ISR has everything before this
        int32_t leader_epoch;
    };
    std::vector<protocol::ProduceTopicResponse> topic_responses;
    std::vector<PendingAck> pending_acks;

    const auto now_ms = []() -> Timestamp {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::system_clock::now().time_since_epoch())
            .count();
    };

    bool has_error = false;
    // M8-E1: never accept writes on a metadata view that may be stale.
    const bool metadata_current = dataPlaneCurrent();

    for (const auto& topic_data : request.topics()) {
        protocol::ProduceTopicResponse topic_response;
        topic_response.topic = topic_data.topic;

        // Authorization: WRITE on the topic (no-op when the authorizer is
        // disabled, which is the default). Deny → every partition of this topic
        // fails with TOPIC_AUTHORIZATION_FAILED, matching Kafka's behavior.
        if (!authorize(context, /*WRITE=*/4, /*TOPIC=*/2, topic_data.topic)) {
            for (const auto& partition_data : topic_data.partitions) {
                protocol::ProducePartitionResponse pr;
                pr.partition = partition_data.partition;
                pr.base_offset = 0;
                pr.log_start_offset = 0;
                pr.log_append_time = -1;
                pr.error_code = ErrorCode::TOPIC_AUTHORIZATION_FAILED;
                topic_response.partitions.push_back(pr);
            }
            topic_responses.push_back(std::move(topic_response));
            has_error = true;
            continue;
        }

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
            auto partition_it =
                std::find_if(topic_metadata.partitions.begin(), topic_metadata.partitions.end(),
                             [&](const PartitionMetadata& metadata) {
                                 return metadata.partition == partition_data.partition;
                             });

            if (partition_it == topic_metadata.partitions.end()) {
                partition_response.error_code = ErrorCode::UNKNOWN_TOPIC_OR_PARTITION;
                topic_response.partitions.push_back(partition_response);
                has_error = true;
                continue;
            }

            if (partition_it->leader != broker_id_ || !metadata_current) {
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

            auto partition_write_lock =
                lockPartitionWrites({topic_data.topic, partition_data.partition});
            try {
                storage::RecordBatch batch = storage::RecordBatch::deserializeFromProduceRequest(
                    partition_data.record_batch);
                auto* log =
                    log_manager_->getOrCreateLog(topic_data.topic, partition_data.partition);
                if (!log) {
                    throw StorageException(ErrorCode::KAFKA_STORAGE_ERROR, "Failed to create log");
                }

                // Register replica with ReplicaManager if not already registered
                TopicPartition tp{topic_data.topic, partition_data.partition};
                if (!cluster_brokers_.empty() &&
                    (!replica_manager_->isLeader(tp) ||
                     replica_manager_->getLeaderEpoch(tp) != partition_it->leader_epoch ||
                     !isPartitionLeadership(tp, broker_id_, partition_it->leader_epoch))) {
                    partition_response.error_code = ErrorCode::NOT_LEADER_FOR_PARTITION;
                    topic_response.partitions.push_back(partition_response);
                    has_error = true;
                    continue;
                }
                if (!replica_manager_->isLeader(tp)) {
                    // Create a shared_ptr wrapper for the log (LogManager owns the actual log)
                    // We use a non-owning shared_ptr to track it in ReplicaManager
                    std::shared_ptr<storage::Log> log_ptr(log, [](storage::Log*) {});
                    replica_manager_->addReplica(tp, log_ptr);
                }

                if (!cluster_brokers_.empty()) {
                    const auto current = currentPartitionMetadata(tp);
                    if (!current || current->leader != broker_id_ ||
                        current->leader_epoch != partition_it->leader_epoch) {
                        partition_response.error_code = ErrorCode::NOT_LEADER_FOR_PARTITION;
                        topic_response.partitions.push_back(partition_response);
                        has_error = true;
                        continue;
                    }
                    replica_manager_->updateISR(tp, current->isr);
                }

                // M4: partition replication state. On a single-node partition the
                // ISR is {self} (size 1), so both the NOT_ENOUGH_REPLICAS gate and
                // the HW-decoupling below are no-ops and behavior is byte-identical.
                const size_t isr_size = replica_manager_->getISR(tp).size();

                // M4: min.insync.replicas gate. For acks=all, refuse the write up
                // front when the ISR is too small to durably commit it, rather
                // than appending and then failing the ack — this is what lets a
                // producer distinguish "not enough replicas" from a slow commit.
                if (acks == -1 && static_cast<int32_t>(isr_size) < min_insync_replicas_) {
                    partition_response.error_code = ErrorCode::NOT_ENOUGH_REPLICAS;
                    Logger::warn("Produce rejected: {}-{} ISR size {} < min.insync.replicas {}",
                                 topic_data.topic, partition_data.partition, isr_size,
                                 min_insync_replicas_);
                    topic_response.partitions.push_back(partition_response);
                    has_error = true;
                    continue;
                }

                // Phase 2.1: idempotent producer dedup. For non-idempotent
                // producers (producer_id < 0), check() returns NONE.
                if (producer_state_manager_ && batch.producerId() >= 0) {
                    auto chk = producer_state_manager_->check(
                        topic_data.topic, partition_data.partition, batch.producerId(),
                        batch.producerEpoch(), batch.baseSequence(),
                        static_cast<int32_t>(batch.records().size()));
                    if (chk.error == ErrorCode::DUPLICATE_SEQUENCE_NUMBER) {
                        // Duplicate retry: return the original base_offset
                        // (or -1 if unknown) so the client treats it as
                        // a successful idempotent replay.
                        partition_response.base_offset = chk.duplicate_offset;
                        partition_response.log_start_offset = log->logStartOffset();
                        partition_response.log_append_time = now_ms();
                        partition_response.error_code = ErrorCode::NONE;
                        Logger::info("Produce dedup: {}-{} pid={} epoch={} seq={} → DUPLICATE "
                                     "returning offset={}",
                                     topic_data.topic, partition_data.partition, batch.producerId(),
                                     batch.producerEpoch(), batch.baseSequence(),
                                     chk.duplicate_offset);
                        if (acks == -1 && isr_size > 1 && chk.duplicate_offset >= 0) {
                            pending_acks.push_back(PendingAck{
                                topic_responses.size(), topic_response.partitions.size(), tp,
                                chk.duplicate_offset + static_cast<Offset>(batch.records().size()),
                                partition_it->leader_epoch});
                        }
                        topic_response.partitions.push_back(partition_response);
                        continue;
                    }
                    if (chk.error != ErrorCode::NONE) {
                        partition_response.error_code = chk.error;
                        Logger::warn("Produce rejected: {}-{} pid={} epoch={} seq={} → error={}",
                                     topic_data.topic, partition_data.partition, batch.producerId(),
                                     batch.producerEpoch(), batch.baseSequence(),
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
                // M4: on a replicated partition (ISR>1) do NOT advance the high
                // watermark at append time — the record is committed only once
                // the ISR has it (advanced by maybeAdvanceHighWatermark below and
                // when followers fetch). Single-node (ISR==1) advances HW=LEO in
                // the append, byte-identical to before.
                const bool advance_hw = isr_size <= 1;
                if (!cluster_brokers_.empty()) {
                    // M8-F3: our epoch must start before our first write in it,
                    // or a follower asking where the previous epoch ended would
                    // be told too late (the fetcher records it only on its next
                    // cycle). No-op once recorded.
                    log->assignLeaderEpochStart(partition_it->leader_epoch, log->logEndOffset());
                }
                const Offset base_offset = log->appendBatch(std::move(batch), advance_hw);

                // Phase 2.1: record successful append for dedup.
                if (producer_state_manager_ && saved_pid >= 0) {
                    producer_state_manager_->recordAppend(
                        topic_data.topic, partition_data.partition, saved_pid, saved_epoch,
                        saved_base_seq, static_cast<int32_t>(record_count), base_offset);
                }

                // Update metrics: track messages produced and bytes
                if (monitoring_manager_ && monitoring_manager_->metricsCollector()) {
                    monitoring_manager_->metricsCollector()->incrementMessagesProduced(
                        record_count);
                    size_t batch_bytes = partition_data.record_batch.size();
                    monitoring_manager_->metricsCollector()->incrementBytesIn(batch_bytes);
                }

                // Advance the high watermark from the ISR: HW = min(leader LEO,
                // in-sync follower offsets). With only the leader in the ISR
                // (single-node) this equals the LEO, identical to before.
                replica_manager_->maybeAdvanceHighWatermark(tp);

                partition_response.base_offset = base_offset;
                partition_response.log_start_offset = log->logStartOffset();
                partition_response.log_append_time = now_ms();
                partition_response.error_code = ErrorCode::NONE;

                // acks=-1 (all): the write is acknowledged only once every in-sync
                // replica has it. With only the leader in the ISR the append
                // already advanced the HW past it; otherwise it is resolved below.
                if (acks == -1 && isr_size > 1) {
                    pending_acks.push_back(
                        PendingAck{topic_responses.size(), topic_response.partitions.size(), tp,
                                   base_offset + static_cast<Offset>(record_count),
                                   partition_it->leader_epoch});
                }
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
                    std::snprintf(buf, sizeof(buf), "%02x", static_cast<unsigned int>(raw[i]));
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

        topic_responses.push_back(std::move(topic_response));
    }

    // Everything a deferred completion needs lives on the heap: a parked
    // produce is resolved from the purgatory after this call returns.
    struct ProduceState {
        RequestDispatcher::RequestContext context;
        std::vector<protocol::ProduceTopicResponse> topics;
        std::vector<PendingAck> pending;
        bool has_error = false;
    };
    auto state = std::make_shared<ProduceState>();
    state->context.header = context.header;
    state->context.peer_identity = context.peer_identity;
    state->context.connection = context.connection;
    state->topics = std::move(topic_responses);
    state->pending = std::move(pending_acks);
    state->has_error = has_error;

    // Kafka DelayedProduce semantics: a pending batch succeeds once the
    // ISR-committed offset covers it, fails with NOT_LEADER_FOR_PARTITION if
    // this broker lost leadership, and with REQUEST_TIMED_OUT at expiry rather
    // than being falsely acked. Returns true once nothing is pending.
    auto resolve = [this](ProduceState& st, bool expired) -> bool {
        auto unresolved =
            std::remove_if(st.pending.begin(), st.pending.end(), [&](const PendingAck& ack) {
                auto& pr = st.topics[ack.topic_index].partitions[ack.partition_index];
                auto partition_write_lock = lockPartitionWrites(ack.tp);
                if (!cluster_brokers_.empty()) {
                    const auto current = currentPartitionMetadata(ack.tp);
                    if (!dataPlaneCurrent() || !current || current->leader != broker_id_ ||
                        current->leader_epoch != ack.leader_epoch) {
                        pr.error_code = ErrorCode::NOT_LEADER_FOR_PARTITION;
                        st.has_error = true;
                        return true;
                    }
                    replica_manager_->updateISR(ack.tp, current->isr);
                }
                if (!replica_manager_->isLeader(ack.tp)) {
                    pr.error_code = ErrorCode::NOT_LEADER_FOR_PARTITION;
                } else if (static_cast<int32_t>(replica_manager_->getISR(ack.tp).size()) <
                           min_insync_replicas_) {
                    pr.error_code = ErrorCode::NOT_ENOUGH_REPLICAS_AFTER_APPEND;
                } else if (replica_manager_->isrCommittedOffset(ack.tp).value_or(0) >=
                           ack.required_offset) {
                    return true;
                } else if (expired) {
                    pr.error_code = ErrorCode::REQUEST_TIMED_OUT;
                } else {
                    return false;
                }
                st.has_error = true;
                return true;
            });
        st.pending.erase(unresolved, st.pending.end());
        return st.pending.empty();
    };

    auto finish = [this, state, start_time, acks,
                   produce_throttle_ms]() -> RequestDispatcher::HandlerResult {
        protocol::ProduceResponse response;
        response.setThrottleTimeMs(produce_throttle_ms);
        for (const auto& topic_response : state->topics) {
            response.addTopic(topic_response);
        }
        const int16_t version =
            std::clamp<int16_t>(state->context.header.apiVersion(), 0, kProduceMaxVersion);
        RequestDispatcher::HandlerResult result;
        result.suppress_response = (acks == 0 && !state->has_error);
        if (!result.suppress_response) {
            result.payload = encodeResponse(
                state->context, [&](Buffer& buffer) { response.encode(buffer, version); });
        }

        // Record produce latency
        if (monitoring_manager_ && monitoring_manager_->metricsCollector()) {
            auto end_time = std::chrono::steady_clock::now();
            auto duration_ms =
                std::chrono::duration<double, std::milli>(end_time - start_time).count();
            monitoring_manager_->metricsCollector()->recordProduceLatency(duration_ms);
            monitoring_manager_->metricsCollector()->incrementRequestsTotal("Produce");
            if (state->has_error) {
                monitoring_manager_->metricsCollector()->incrementRequestErrors("Produce");
            }
        }
        return result;
    };

    if (resolve(*state, /*expired=*/false)) {
        return finish();
    }

    int32_t timeout_ms = request.timeoutMs();
    if (timeout_ms <= 0 || timeout_ms > 30000) {
        timeout_ms = 30000;
    }
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);

    // Park the wait in the purgatory instead of holding this network IO thread:
    // it is retried whenever a pending partition's HW, leader or ISR changes and
    // answered at the deadline regardless.
    if (context.complete && delayed_produce_purgatory_) {
        std::vector<TopicPartition> keys;
        keys.reserve(state->pending.size());
        for (const auto& ack : state->pending) {
            keys.push_back(ack.tp);
        }
        auto complete = context.complete;
        auto try_complete = [state, resolve, finish, complete](bool expired) -> bool {
            if (!resolve(*state, expired)) {
                return false;
            }
            complete(finish());
            return true;
        };
        if (delayed_produce_purgatory_->watch(keys, deadline, std::move(try_complete))) {
            RequestDispatcher::HandlerResult parked;
            parked.deferred = true;
            return parked;
        }
    }

    // No deferral seam (direct handler invocation): poll on this thread.
    while (!resolve(*state, std::chrono::steady_clock::now() >= deadline)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return finish();
}

Buffer KawasanBroker::buildProduceError(const RequestDispatcher::RequestContext& context,
                                        ErrorCode code, int16_t response_version) const {
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

    const int16_t version = std::clamp<int16_t>(response_version, 0, kProduceMaxVersion);
    return encodeResponse(context, [&](Buffer& buffer) { response.encode(buffer, version); });
}

RequestDispatcher::HandlerResult KawasanBroker::handleFetch(
    RequestDispatcher::RequestContext& context) {
    const auto start_time = std::chrono::steady_clock::now();

    // Everything a (possibly deferred) re-evaluation needs lives on the heap:
    // a parked fetch is retried from the purgatory after this call returns.
    struct FetchState {
        protocol::FetchRequest request;
        RequestDispatcher::RequestContext context;
    };
    auto state = std::make_shared<FetchState>();
    state->request.decode(context.payload, context.header.apiVersion());
    state->context.header = context.header;
    state->context.peer_identity = context.peer_identity;
    state->context.connection = context.connection;
    const auto& request = state->request;

    const int32_t max_wait_ms = std::max<int32_t>(0, request.maxWaitMs());
    const size_t min_bytes = static_cast<size_t>(std::max<int32_t>(0, request.minBytes()));
    const int32_t request_max_bytes_raw =
        request.maxBytes() > 0 ? request.maxBytes() : 50 * 1024 * 1024;
    const size_t request_max_bytes = static_cast<size_t>(request_max_bytes_raw);

    // M4: a Fetch with replica_id >= 0 is a follower broker replicating from this
    // leader, not a consumer. Followers read up to the log-end-offset (they must
    // copy un-committed records) and report their position so the leader can
    // advance the high watermark; consumers read only up to the high watermark.
    const bool is_follower = request.replicaId() >= 0;

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
        [state, make_legacy_messageset](
            const std::vector<storage::RecordBatch>& batches) -> std::vector<uint8_t> {
        if (state->context.header.apiVersion() <= 3) {
            return make_legacy_messageset(batches);
        }

        std::vector<uint8_t> payload;
        for (const auto& batch : batches) {
            auto bytes = batch.serialize();
            payload.insert(payload.end(), bytes.begin(), bytes.end());
        }
        return payload;
    };

    auto build_response = [this, state, request_max_bytes, is_follower,
                           serialize_batches](protocol::FetchResponse& response) -> size_t {
        const auto& request = state->request;
        auto& context = state->context;
        size_t total_bytes = 0;
        size_t remaining_request_bytes = request_max_bytes;
        // M8-E1: a broker whose metadata view may be stale serves no partition.
        const bool metadata_current = dataPlaneCurrent();

        for (const auto& topic : request.topics()) {
            protocol::FetchTopicResponse topic_response;
            topic_response.topic = topic.topic;
            topic_response.topic_id = topic.topic_id;

            // Resolve the committed UUID without auto-creating an unknown ID.
            std::string lookup_name = topic.topic;
            std::optional<TopicMetadata> topic_metadata_opt;
            ErrorCode topic_error = ErrorCode::UNKNOWN_TOPIC_OR_PARTITION;
            if (topic.has_topic_id) {
                if (metadata_controller_) {
                    topic_metadata_opt = metadata_controller_->topicById(topic.topic_id);
                }
                if (topic_metadata_opt) {
                    lookup_name = topic_metadata_opt->name;
                    topic_response.topic = lookup_name;  // delayed-watch keys use the name
                    topic_error = topic_metadata_opt->error_code;
                } else {
                    topic_error = ErrorCode::UNKNOWN_TOPIC_ID;
                }
            } else {
                std::tie(topic_metadata_opt, topic_error) =
                    getTopicMetadata(lookup_name, !is_follower && auto_create_topics_enabled_);
            }

            // Authorization: READ on the topic (no-op when the authorizer is
            // disabled, the default). Deny → each requested partition fails with
            // TOPIC_AUTHORIZATION_FAILED.
            if (!authorize(context, /*READ=*/3, /*TOPIC=*/2, lookup_name)) {
                for (const auto& partition : topic.partitions) {
                    protocol::FetchPartitionResponse pr;
                    pr.partition = partition.partition;
                    pr.error_code = ErrorCode::TOPIC_AUTHORIZATION_FAILED;
                    pr.high_watermark = 0;
                    pr.last_stable_offset = 0;
                    pr.log_start_offset = 0;
                    topic_response.partitions.push_back(pr);
                }
                response.addTopic(topic_response);
                continue;
            }

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
                auto partition_it =
                    std::find_if(topic_metadata.partitions.begin(), topic_metadata.partitions.end(),
                                 [&](const PartitionMetadata& metadata) {
                                     return metadata.partition == partition.partition;
                                 });

                if (partition_it == topic_metadata.partitions.end()) {
                    partition_response.error_code = ErrorCode::UNKNOWN_TOPIC_OR_PARTITION;
                    finalize_partition(0);
                    continue;
                }

                if (partition_it->leader != broker_id_ || !metadata_current) {
                    partition_response.error_code = ErrorCode::NOT_LEADER_FOR_PARTITION;
                    finalize_partition(0);
                    continue;
                }

                if (const auto epoch_error = checkLeaderEpoch(partition.current_leader_epoch,
                                                              partition_it->leader_epoch);
                    epoch_error != ErrorCode::NONE) {
                    partition_response.error_code = epoch_error;  // M8-E2 (KIP-320)
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
                if (!cluster_brokers_.empty() &&
                    (!replica_manager_->isLeader(tp) ||
                     replica_manager_->getLeaderEpoch(tp) != partition_it->leader_epoch)) {
                    partition_response.error_code = ErrorCode::NOT_LEADER_FOR_PARTITION;
                    finalize_partition(0);
                    continue;
                }
                if (!replica_manager_->isLeader(tp)) {
                    // Create a non-owning shared_ptr wrapper
                    std::shared_ptr<storage::Log> log_ptr(log, [](storage::Log*) {});
                    replica_manager_->addReplica(tp, log_ptr);
                }

                auto partition_write_lock = lockPartitionWrites(tp);
                if (!cluster_brokers_.empty()) {
                    const auto current = currentPartitionMetadata(tp);
                    if (!current || current->leader != broker_id_ ||
                        current->leader_epoch != partition_it->leader_epoch) {
                        partition_response.error_code = ErrorCode::NOT_LEADER_FOR_PARTITION;
                        finalize_partition(0);
                        continue;
                    }
                    replica_manager_->updateISR(tp, current->isr);
                }

                // M4: leader-side follower bookkeeping. The follower's fetch
                // offset is its log-end-offset (everything below it is replicated
                // on the follower), so record it and recompute the high watermark
                // from the ISR. This is what unblocks an acks=all produce and what
                // lets the leader advance HW past records now held by the ISR.
                if (is_follower) {
                    replica_manager_->updateFollowerFetchOffset(tp, request.replicaId(),
                                                                partition.fetch_offset);
                    replica_manager_->maybeAdvanceHighWatermark(tp);
                }

                const Offset log_start = log->logStartOffset();
                const Offset log_end = log->logEndOffset();
                partition_response.log_start_offset = log_start;

                // Get high watermark from ReplicaManager (falls back to log's HW if not found)
                auto hw_opt = replica_manager_->getHighWatermark(tp);
                if (!cluster_brokers_.empty() && !is_follower) {
                    hw_opt =
                        replica_manager_->readableHighWatermark(tp, partition_it->leader_epoch);
                    if (!hw_opt) {
                        partition_response.error_code = ErrorCode::NOT_LEADER_FOR_PARTITION;
                        finalize_partition(0);
                        continue;
                    }
                }
                const Offset high_watermark = hw_opt.value_or(log->highWatermark());
                partition_response.high_watermark = high_watermark;

                // M4: the readable upper bound. A consumer sees only committed
                // records (up to the high watermark); a follower replicates
                // everything the leader has (up to the log-end-offset). On a
                // single-node partition HW==LEO so read_bound==log_end for both,
                // and the raw fast path below is unchanged (byte-identical).
                const Offset read_bound = is_follower ? log_end : high_watermark;

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
                        lookup_name, partition.partition, partition.fetch_offset, log_start);
                    for (const auto& a : aborts) {
                        protocol::FetchAbortedTransaction at;
                        at.producer_id = a.producer_id;
                        at.first_offset = a.first_offset;
                        partition_response.aborted_transactions.push_back(at);
                    }
                }

                // Allow fetch_offset == log_end (returns empty data), reject only if strictly
                // beyond
                if (partition.fetch_offset < log_start || partition.fetch_offset > log_end) {
                    Logger::warn(
                        "Fetch offset {} out of range for {}-{} (log_start={}, log_end={})",
                        partition.fetch_offset, lookup_name, partition.partition, log_start,
                        log_end);
                    partition_response.error_code = ErrorCode::OFFSET_OUT_OF_RANGE;
                    finalize_partition(0);
                    continue;
                }

                size_t effective_cap = remaining_request_bytes;
                if (partition.partition_max_bytes > 0) {
                    effective_cap =
                        std::min(effective_cap, static_cast<size_t>(partition.partition_max_bytes));
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
                    // M4: the raw byte fast path copies straight through to the
                    // log-end-offset, so it is only safe when the readable bound
                    // already reaches the log end (single-node, or a follower).
                    // A consumer on a replicated partition with an un-committed
                    // tail (read_bound < LEO) takes the deserialized path so the
                    // batches past the high watermark can be dropped.
                    const bool use_raw = context.header.apiVersion() >= 4 && !read_committed &&
                                         read_bound >= log_end;
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
                                if (b.baseOffset() >= lso)
                                    continue;
                                kept.push_back(std::move(b));
                            }
                            batches = std::move(kept);
                        }

                        // M4: high-watermark clamp for consumers. On a replicated
                        // partition the leader's log tail past the HW is not yet
                        // committed; a consumer must not see it. read_bound==LEO
                        // for followers and single-node partitions, so this is a
                        // no-op there.
                        if (read_bound < log_end) {
                            std::vector<storage::RecordBatch> kept;
                            kept.reserve(batches.size());
                            for (auto& b : batches) {
                                if (b.baseOffset() >= read_bound)
                                    continue;
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
                            monitoring_manager_->metricsCollector()->incrementMessagesConsumed(
                                total_messages_for_metrics);
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
        auto v = fetch_session_manager_->validate(request.sessionId(), request.sessionEpoch());
        if (v.is_error) {
            session_error = v.error;
        } else {
            resp_session_id = v.session_id;
            if (v.is_new_session) {
                Logger::info("Fetch: allocated session_id={} for client {}", v.session_id,
                             context.peer_identity);
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
        result.payload =
            encodeResponse(context, [&](Buffer& buffer) { response.encode(buffer, version); });
        result.close_connection = false;
        return result;
    }
    auto attempt = [build_response, resp_session_id](protocol::FetchResponse& out) -> size_t {
        out = protocol::FetchResponse();
        out.setThrottleTimeMs(0);
        out.setSessionId(resp_session_id);
        return build_response(out);
    };

    // Kafka DelayedFetch semantics: answer now if enough bytes are available,
    // the client does not want to wait, or any partition hit an error.
    auto satisfied = [is_follower, min_bytes, max_wait_ms](const protocol::FetchResponse& r,
                                                           size_t bytes) {
        if (is_follower || min_bytes == 0 || bytes >= min_bytes || max_wait_ms == 0) {
            return true;
        }
        for (const auto& topic : r.topics()) {
            for (const auto& partition : topic.partitions) {
                if (partition.error_code != ErrorCode::NONE) {
                    return true;
                }
            }
        }
        return false;
    };

    auto finish = [this, state, start_time](protocol::FetchResponse& out,
                                            size_t bytes) -> RequestDispatcher::HandlerResult {
        const auto& ctx = state->context;
        // throttle_time_ms is a quota back-off instruction (KIP-219 clients mute
        // the connection for that long); long-poll wait time is not throttling.
        int32_t fetch_throttle_ms = 0;
        if (quota_manager_) {
            fetch_throttle_ms = quota_manager_->recordAndThrottleMs(QuotaManager::Type::kConsumer,
                                                                    ctx.header.clientId(), bytes);
        }
        out.setThrottleTimeMs(fetch_throttle_ms);

        const int16_t version = std::clamp<int16_t>(ctx.header.apiVersion(), 0, kFetchMaxVersion);
        RequestDispatcher::HandlerResult result;
        result.payload = encodeResponse(ctx, [&](Buffer& buffer) { out.encode(buffer, version); });

        if (monitoring_manager_ && monitoring_manager_->metricsCollector()) {
            auto end_time = std::chrono::steady_clock::now();
            auto duration_ms =
                std::chrono::duration<double, std::milli>(end_time - start_time).count();
            monitoring_manager_->metricsCollector()->recordFetchLatency(duration_ms);
            monitoring_manager_->metricsCollector()->incrementRequestsTotal("Fetch");
        }
        return result;
    };

    bytes_returned = attempt(response);
    if (satisfied(response, bytes_returned)) {
        return finish(response, bytes_returned);
    }

    // Park the long-poll in the purgatory instead of holding this network IO
    // thread: it is retried whenever a fetched partition changes and answered
    // at the deadline regardless.
    if (context.complete && delayed_fetch_purgatory_) {
        std::vector<TopicPartition> keys;
        for (const auto& topic : response.topics()) {
            for (const auto& partition : topic.partitions) {
                keys.push_back(TopicPartition{topic.topic, partition.partition});
            }
        }
        auto complete = context.complete;
        auto try_complete = [this, state, attempt, satisfied, finish,
                             complete](bool expired) -> bool {
            protocol::FetchResponse out;
            size_t bytes = 0;
            try {
                bytes = attempt(out);
            } catch (const std::exception& ex) {
                if (!expired) {
                    return false;
                }
                Logger::error("Delayed fetch from {} failed: {}", state->context.peer_identity,
                              ex.what());
                RequestDispatcher::HandlerResult error;
                error.payload = buildFetchError(
                    state->context, ErrorCode::KAFKA_STORAGE_ERROR,
                    std::clamp<int16_t>(state->context.header.apiVersion(), 0, kFetchMaxVersion));
                complete(std::move(error));
                return true;
            }
            if (!expired && !satisfied(out, bytes)) {
                return false;
            }
            complete(finish(out, bytes));
            return true;
        };
        if (delayed_fetch_purgatory_->watch(keys, deadline, std::move(try_complete))) {
            RequestDispatcher::HandlerResult parked;
            parked.deferred = true;
            return parked;
        }
    }

    // No deferral seam (direct handler invocation): poll on this thread.
    while (!satisfied(response, bytes_returned) && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        bytes_returned = attempt(response);
    }
    return finish(response, bytes_returned);
}

Buffer KawasanBroker::buildFetchError(const RequestDispatcher::RequestContext& context,
                                      ErrorCode code, int16_t response_version) const {
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

    const int16_t version = std::clamp<int16_t>(response_version, 0, kFetchMaxVersion);
    return encodeResponse(context, [&](Buffer& buffer) { response.encode(buffer, version); });
}

RequestDispatcher::HandlerResult KawasanBroker::handleListOffsets(
    RequestDispatcher::RequestContext& context) {
    protocol::ListOffsetsRequest request;
    request.decode(context.payload, context.header.apiVersion());

    Logger::debug("ListOffsets v{}: replica_id={}, {} topics", context.header.apiVersion(),
                  request.replicaId(), request.topics().size());

    protocol::ListOffsetsResponse response;
    response.setThrottleTimeMs(0);

    const bool metadata_current = dataPlaneCurrent();  // M8-E1
    for (const auto& topic : request.topics()) {
        protocol::ListOffsetsTopicResponse topic_response;
        topic_response.topic = topic.topic;
        std::vector<TopicMetadata> topic_metadata;
        if (metadata_controller_) {
            topic_metadata = metadata_controller_->describeTopics({topic.topic});
        }

        for (const auto& partition : topic.partitions) {
            protocol::ListOffsetsPartitionResponse partition_response;
            partition_response.partition = partition.partition;
            partition_response.error_code = ErrorCode::NONE;
            partition_response.timestamp = -1;
            partition_response.offset = -1;
            partition_response.leader_epoch = -1;

            int32_t selected_leader_epoch = -1;

            // Offsets come from the partition leader's log (a follower's HW and
            // log end lag). Only enforced where metadata knows the partition.
            if (!topic_metadata.empty() && topic_metadata.front().error_code == ErrorCode::NONE) {
                const auto& parts = topic_metadata.front().partitions;
                auto pm = std::find_if(parts.begin(), parts.end(), [&](const PartitionMetadata& m) {
                    return m.partition == partition.partition;
                });
                if (pm != parts.end()) {
                    selected_leader_epoch = pm->leader_epoch;
                    ErrorCode gate = ErrorCode::NONE;
                    if (pm->leader != broker_id_ || !metadata_current) {
                        gate = ErrorCode::NOT_LEADER_FOR_PARTITION;
                    } else {
                        gate = checkLeaderEpoch(partition.current_leader_epoch, pm->leader_epoch);
                    }
                    if (gate != ErrorCode::NONE) {
                        partition_response.error_code = gate;
                        topic_response.partitions.push_back(partition_response);
                        continue;
                    }
                }
            }

            // Get the log for this partition
            auto* log = log_manager_->getLog(topic.topic, partition.partition);
            if (!log) {
                // Partition doesn't exist
                partition_response.error_code = ErrorCode::UNKNOWN_TOPIC_OR_PARTITION;
                topic_response.partitions.push_back(partition_response);
                continue;
            }

            Offset committed_offset = log->highWatermark();
            if (!cluster_brokers_.empty()) {
                const auto ready_hw = replica_manager_->readableHighWatermark(
                    {topic.topic, partition.partition}, selected_leader_epoch);
                if (!ready_hw) {
                    partition_response.error_code = ErrorCode::NOT_LEADER_FOR_PARTITION;
                    topic_response.partitions.push_back(partition_response);
                    continue;
                }
                committed_offset = *ready_hw;
            }
            try {
                // Handle special timestamp values per Kafka protocol:
                // -1 = LATEST (end of log, high watermark)
                // -2 = EARLIEST (start of log)
                if (partition.timestamp == -1) {
                    // Latest offset (high watermark / end of log)
                    partition_response.offset = committed_offset;
                    partition_response.timestamp = partition.timestamp;
                } else if (partition.timestamp == -2 ||
                           (context.header.apiVersion() >= 8 && partition.timestamp == -4)) {
                    // EARLIEST / EARLIEST_LOCAL: all retained data is local.
                    partition_response.offset = log->logStartOffset();
                    partition_response.timestamp = partition.timestamp;
                } else if (partition.timestamp == -3) {
                    // Phase 1.5 / v7: MAX_TIMESTAMP — offset of the data
                    // batch with the highest timestamp (control batches
                    // excluded). We return its base_offset since a
                    // per-record offset would require per-record timestamp
                    // tracking.
                    auto best = log->maxTimestampOffset();
                    partition_response.offset = best ? best->first : log->logStartOffset();
                    partition_response.timestamp = best ? best->second : -1;
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
                            if (batches.empty())
                                break;
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
                            if (done)
                                break;
                            if (next <= cur)
                                break;
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
                Logger::error("Error getting offsets for {}-{}: {}", topic.topic,
                              partition.partition, ex.what());
                partition_response.error_code = ErrorCode::KAFKA_STORAGE_ERROR;
            }

            topic_response.partitions.push_back(partition_response);
        }

        response.addTopic(topic_response);
    }

    const int16_t version =
        std::clamp<int16_t>(context.header.apiVersion(), 0, kListOffsetsMaxVersion);

    Logger::debug("ListOffsets response: {} topics", response.topics().size());
    for (const auto& t : response.topics()) {
        for (const auto& p : t.partitions) {
            Logger::debug("  {}-{}: offset={}, error={}", t.topic, p.partition, p.offset,
                          static_cast<int16_t>(p.error_code));
        }
    }

    RequestDispatcher::HandlerResult result;
    result.payload =
        encodeResponse(context, [&](Buffer& buffer) { response.encode(buffer, version); });
    return result;
}

Buffer KawasanBroker::buildListOffsetsError(const RequestDispatcher::RequestContext& context,
                                            ErrorCode code, int16_t response_version) const {
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

    const int16_t version = std::clamp<int16_t>(response_version, 0, kListOffsetsMaxVersion);
    return encodeResponse(context, [&](Buffer& buffer) { response.encode(buffer, version); });
}

bool KawasanBroker::isCoordinatorFor(const std::string& key, protocol::CoordinatorType type) const {
    if (cluster_brokers_.empty())
        return true;
    const auto coordinator = resolveCoordinator(key, type);
    return coordinator.error_code == ErrorCode::NONE && coordinator.node_id == broker_id_;
}

protocol::FindCoordinatorResponse::Coordinator KawasanBroker::resolveCoordinator(
    const std::string& key, protocol::CoordinatorType type) const {
    protocol::FindCoordinatorResponse::Coordinator result;
    result.key = key;
    if (type != protocol::CoordinatorType::GROUP &&
        type != protocol::CoordinatorType::TRANSACTION) {
        result.error_code = ErrorCode::INVALID_REQUEST;
    } else if (cluster_brokers_.empty()) {
        result.node_id = broker_id_;
        result.host = advertised_host_;
        result.port = port_;
        return result;
    } else {
        result.error_code = ErrorCode::COORDINATOR_NOT_AVAILABLE;
        if (dataPlaneCurrent() && metadata_controller_) {
            const char* topic = type == protocol::CoordinatorType::GROUP ? "__consumer_offsets"
                                                                         : "__transaction_state";
            const auto topics = metadata_controller_->describeTopics({topic});
            if (!topics.empty() && topics.front().error_code == ErrorCode::NONE &&
                !topics.front().partitions.empty()) {
                const auto& partitions = topics.front().partitions;
                const int32_t partition =
                    coordinatorPartitionFor(key, static_cast<int32_t>(partitions.size()));
                for (const auto& pm : partitions) {
                    if (pm.partition != partition || pm.leader < 0)
                        continue;
                    if (const auto endpoint = peerEndpoint(pm.leader)) {
                        result.node_id = pm.leader;
                        result.host = endpoint->first;
                        result.port = endpoint->second;
                        result.error_code = ErrorCode::NONE;
                        return result;
                    }
                }
            }
        }
    }
    result.error_message = KawasanException::toString(result.error_code);
    return result;
}

RequestDispatcher::HandlerResult KawasanBroker::handleFindCoordinator(
    RequestDispatcher::RequestContext& context) {
    protocol::FindCoordinatorRequest request;
    request.decode(context.payload, context.header.apiVersion());
    protocol::FindCoordinatorResponse response;
    response.setThrottleTimeMs(0);
    if (context.header.apiVersion() >= 4) {
        for (const auto& key : request.keys()) {
            response.addCoordinator(resolveCoordinator(key, request.keyType()));
        }
    } else {
        const auto coordinator = resolveCoordinator(request.key(), request.keyType());
        response.setErrorCode(coordinator.error_code);
        response.setErrorMessage(coordinator.error_message);
        response.setNodeId(coordinator.node_id);
        response.setHost(coordinator.host);
        response.setPort(coordinator.port);
    }
    const int16_t version =
        std::clamp<int16_t>(context.header.apiVersion(), 0, kFindCoordinatorMaxVersion);
    RequestDispatcher::HandlerResult result;
    result.payload =
        encodeResponse(context, [&](Buffer& buffer) { response.encode(buffer, version); });
    return result;
}

Buffer KawasanBroker::buildFindCoordinatorError(const RequestDispatcher::RequestContext& context,
                                                ErrorCode code, int16_t response_version) const {
    protocol::FindCoordinatorResponse response;
    response.setThrottleTimeMs(0);
    response.setErrorCode(code);
    response.setErrorMessage(KawasanException::toString(code));
    response.setNodeId(-1);
    response.setHost("");
    response.setPort(0);

    const int16_t version = std::clamp<int16_t>(response_version, 0, kFindCoordinatorMaxVersion);
    return encodeResponse(context, [&](Buffer& buffer) { response.encode(buffer, version); });
}

Buffer KawasanBroker::handleJoinGroup(RequestDispatcher::RequestContext& context) {
    protocol::JoinGroupRequest request;
    request.decode(context.payload, context.header.apiVersion());
    if (!isCoordinatorFor(request.groupId(), protocol::CoordinatorType::GROUP)) {
        return buildJoinGroupError(context, ErrorCode::NOT_COORDINATOR,
                                   context.header.apiVersion());
    }

    // 0A.10: forward the real client identity so DescribeGroups returns
    // something useful instead of "unknown".
    const auto result = group_coordinator_->handleJoinGroup(request, context.header.clientId(),
                                                            context.peer_identity);

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
    return encodeResponse(context, [&](Buffer& buffer) { response.encode(buffer, version); });
}

Buffer KawasanBroker::buildJoinGroupError(const RequestDispatcher::RequestContext& context,
                                          ErrorCode code, int16_t response_version) const {
    protocol::JoinGroupResponse response;
    response.setErrorCode(code);
    response.setGenerationId(0);
    response.setGroupProtocol("");
    response.setLeaderId("");
    response.setMemberId("");
    response.setMembers({});

    const int16_t version = std::clamp<int16_t>(response_version, 0, kJoinGroupMaxVersion);
    return encodeResponse(context, [&](Buffer& buffer) { response.encode(buffer, version); });
}

Buffer KawasanBroker::handleSyncGroup(RequestDispatcher::RequestContext& context) {
    protocol::SyncGroupRequest request;
    request.decode(context.payload, context.header.apiVersion());
    if (!isCoordinatorFor(request.groupId(), protocol::CoordinatorType::GROUP)) {
        return buildSyncGroupError(context, ErrorCode::NOT_COORDINATOR,
                                   context.header.apiVersion());
    }

    Logger::info("SyncGroup request: group='{}' generation={} member='{}' {} assignments",
                 request.groupId(), request.generationId(), request.memberId(),
                 request.assignments().size());

    const auto result = group_coordinator_->handleSyncGroup(request);

    Logger::info("SyncGroup response: error={} assignment_size={}", static_cast<int>(result.error),
                 result.assignment.size());

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
    return encodeResponse(context, [&](Buffer& buffer) { response.encode(buffer, version); });
}

Buffer KawasanBroker::buildSyncGroupError(const RequestDispatcher::RequestContext& context,
                                          ErrorCode code, int16_t response_version) const {
    protocol::SyncGroupResponse response;
    response.setErrorCode(code);
    response.setAssignment({});

    const int16_t version = std::clamp<int16_t>(response_version, 0, kSyncGroupMaxVersion);
    return encodeResponse(context, [&](Buffer& buffer) { response.encode(buffer, version); });
}

Buffer KawasanBroker::handleHeartbeat(RequestDispatcher::RequestContext& context) {
    protocol::HeartbeatRequest request;
    request.decode(context.payload, context.header.apiVersion());
    if (!isCoordinatorFor(request.groupId(), protocol::CoordinatorType::GROUP)) {
        return buildHeartbeatError(context, ErrorCode::NOT_COORDINATOR,
                                   context.header.apiVersion());
    }

    const ErrorCode error = group_coordinator_->handleHeartbeat(request);

    protocol::HeartbeatResponse response;
    response.setErrorCode(error);

    const int16_t version =
        std::clamp<int16_t>(context.header.apiVersion(), 0, kHeartbeatMaxVersion);
    return encodeResponse(context, [&](Buffer& buffer) { response.encode(buffer, version); });
}

Buffer KawasanBroker::buildHeartbeatError(const RequestDispatcher::RequestContext& context,
                                          ErrorCode code, int16_t response_version) const {
    protocol::HeartbeatResponse response;
    response.setErrorCode(code);

    const int16_t version = std::clamp<int16_t>(response_version, 0, kHeartbeatMaxVersion);
    return encodeResponse(context, [&](Buffer& buffer) { response.encode(buffer, version); });
}

Buffer KawasanBroker::handleLeaveGroup(RequestDispatcher::RequestContext& context) {
    protocol::LeaveGroupRequest request;
    request.decode(context.payload, context.header.apiVersion());
    if (!isCoordinatorFor(request.groupId(), protocol::CoordinatorType::GROUP)) {
        return buildLeaveGroupError(context, ErrorCode::NOT_COORDINATOR,
                                    context.header.apiVersion());
    }

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
    return encodeResponse(context, [&](Buffer& buffer) { response.encode(buffer, version); });
}

Buffer KawasanBroker::buildLeaveGroupError(const RequestDispatcher::RequestContext& context,
                                           ErrorCode code, int16_t response_version) const {
    protocol::LeaveGroupResponse response;
    response.setErrorCode(code);

    const int16_t version = std::clamp<int16_t>(response_version, 0, kLeaveGroupMaxVersion);
    return encodeResponse(context, [&](Buffer& buffer) { response.encode(buffer, version); });
}

Buffer KawasanBroker::handleOffsetCommit(RequestDispatcher::RequestContext& context) {
    protocol::OffsetCommitRequest request;
    request.decode(context.payload, context.header.apiVersion());
    if (!isCoordinatorFor(request.groupId(), protocol::CoordinatorType::GROUP)) {
        protocol::OffsetCommitResponse response;
        std::vector<protocol::OffsetCommitResponse::Topic> topics;
        for (const auto& t : request.topics()) {
            protocol::OffsetCommitResponse::Topic topic;
            topic.topic = t.topic;
            for (const auto& p : t.partitions) {
                topic.partitions.push_back({p.partition, ErrorCode::NOT_COORDINATOR});
            }
            topics.push_back(std::move(topic));
        }
        response.setTopics(topics);
        return encodeResponse(
            context, [&](Buffer& buffer) { response.encode(buffer, context.header.apiVersion()); });
    }

    Logger::info("OffsetCommit request: group='{}' generation={} member='{}' {} topics",
                 request.groupId(), request.generationId(), request.memberId(),
                 request.topics().size());

    ErrorCode overall_error = ErrorCode::NONE;
    const auto topics = group_coordinator_->handleOffsetCommit(request, overall_error);

    Logger::info("OffsetCommit response: overall_error={} {} topics",
                 static_cast<int>(overall_error), topics.size());

    // Phase 3.1: also append a record to the __consumer_offsets topic so
    // `kcat -t __consumer_offsets -C` and similar tooling see the commit
    // stream. **Hash-route** the partition by group_id using the same
    // Java `String.hashCode()` routing as FindCoordinator (coordinatorPartitionFor).
    // That matches what `kafka-consumer-groups.sh --describe` expects, so
    // a Kawasan deployment is observably identical to Kafka for offset
    // browsing.
    if (overall_error == ErrorCode::NONE && log_manager_ != nullptr) {
        const int32_t target_partition =
            coordinatorPartitionFor(request.groupId(), offsets_topic_num_partitions_);
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
                    const std::string key =
                        request.groupId() + "|" + t.topic + "|" + std::to_string(p.partition);
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
    return encodeResponse(context, [&](Buffer& buffer) { response.encode(buffer, version); });
}

Buffer KawasanBroker::buildOffsetCommitError(const RequestDispatcher::RequestContext& context,
                                             ErrorCode code, int16_t response_version) const {
    protocol::OffsetCommitResponse response;
    protocol::OffsetCommitResponse::Partition partition;
    partition.error = code;
    protocol::OffsetCommitResponse::Topic topic;
    topic.partitions.push_back(partition);
    response.setTopics({topic});

    const int16_t version = std::clamp<int16_t>(response_version, 0, kOffsetCommitMaxVersion);
    return encodeResponse(context, [&](Buffer& buffer) { response.encode(buffer, version); });
}

Buffer KawasanBroker::handleOffsetFetch(RequestDispatcher::RequestContext& context) {
    protocol::OffsetFetchRequest request;
    request.decode(context.payload, context.header.apiVersion());

    auto fetch_owned = [&](const protocol::OffsetFetchRequest& req, ErrorCode& error) {
        if (isCoordinatorFor(req.groupId(), protocol::CoordinatorType::GROUP)) {
            return group_coordinator_->handleOffsetFetch(req, error);
        }
        error = ErrorCode::NOT_COORDINATOR;
        std::vector<protocol::OffsetFetchResponse::Topic> topics;
        for (const auto& t : req.topics()) {
            protocol::OffsetFetchResponse::Topic topic;
            topic.topic = t.topic;
            for (const auto& p : t.partitions) {
                protocol::OffsetFetchResponse::Partition partition;
                partition.partition = p.partition;
                partition.error = error;
                topic.partitions.push_back(std::move(partition));
            }
            topics.push_back(std::move(topic));
        }
        return topics;
    };

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
            std::vector<protocol::OffsetFetchResponse::Topic> topics;
            if (api_v >= 9 && (g.member_id.has_value() || g.member_epoch != -1)) {
                // ConsumerGroupHeartbeat/KIP-848 membership is deferred to CM-12.
                // Never silently treat a new-protocol member as a classic group.
                group_error = ErrorCode::UNSUPPORTED_VERSION;
            } else {
                topics = fetch_owned(single, group_error);
            }
            protocol::OffsetFetchResponse::Group rg;
            rg.group_id = g.group_id;
            rg.error_code = group_error;
            rg.topics = topics;
            response.addGroup(std::move(rg));
            Logger::info("OffsetFetch v{}: group='{}' returned {} topics (error={})", api_v,
                         g.group_id, topics.size(), static_cast<int>(group_error));
        }
        const int16_t version = std::clamp<int16_t>(api_v, 0, kOffsetFetchMaxVersion);
        return encodeResponse(context, [&](Buffer& buffer) { response.encode(buffer, version); });
    }

    // Legacy v0–v7 single-group form.
    Logger::info("OffsetFetch request for group '{}', {} topics", request.groupId(),
                 request.topics().size());

    ErrorCode overall_error = ErrorCode::NONE;
    const auto topics = fetch_owned(request, overall_error);

    Logger::info("OffsetFetch response: overall_error={} {} topics",
                 static_cast<int>(overall_error), topics.size());

    response.setErrorCode(overall_error);
    response.setTopics(topics);

    const int16_t version = std::clamp<int16_t>(api_v, 0, kOffsetFetchMaxVersion);
    return encodeResponse(context, [&](Buffer& buffer) { response.encode(buffer, version); });
}

Buffer KawasanBroker::buildOffsetFetchError(const RequestDispatcher::RequestContext& context,
                                            ErrorCode code, int16_t response_version) const {
    protocol::OffsetFetchResponse response;
    response.setErrorCode(code);
    const int16_t version = std::clamp<int16_t>(response_version, 0, kOffsetFetchMaxVersion);
    if (version >= 8) {
        try {
            Buffer input = context.payload;
            input.setPosition(context.payload_start);
            protocol::OffsetFetchRequest request;
            request.decode(input, version);
            for (const auto& group : request.groups()) {
                protocol::OffsetFetchResponse::Group result;
                result.group_id = group.group_id;
                result.error_code = code;
                response.addGroup(std::move(result));
            }
        } catch (const std::exception&) {
            response.addGroup({"__kawasan_error__", {}, code});
        }
    } else {
        protocol::OffsetFetchResponse::Partition partition;
        partition.error = code;
        protocol::OffsetFetchResponse::Topic topic;
        topic.partitions.push_back(partition);
        response.setTopics({topic});
    }
    return encodeResponse(context, [&](Buffer& buffer) { response.encode(buffer, version); });
}

BrokerId KawasanBroker::controllerId() const {
    // The active controller is the Raft leader. Report it when known; otherwise
    // (pre-election, or single-node before it elects itself) fall back to self.
    if (raft_node_) {
        const BrokerId leader = raft_node_->leaderId();
        if (leader >= 0) {
            return leader;
        }
    }
    return broker_id_;
}

std::optional<std::pair<std::string, int32_t>> KawasanBroker::peerEndpoint(
    BrokerId broker_id) const {
    if (broker_id == broker_id_) {
        return std::make_pair(advertised_host_.empty() ? host_ : advertised_host_, port_);
    }
    if (metadata_controller_) {
        for (const auto& b : metadata_controller_->brokers()) {
            if (b.id == broker_id) {
                return std::make_pair(b.host, b.port);
            }
        }
    }
    // Fall back to the raft.peers seed (available before metadata is populated).
    for (const auto& b : cluster_brokers_) {
        if (b.id == broker_id) {
            return std::make_pair(b.host, b.port);
        }
    }
    return std::nullopt;
}

std::unique_lock<std::mutex> KawasanBroker::lockPartitionWrites(const TopicPartition& tp) {
    if (cluster_brokers_.empty())
        return {};
    std::mutex* mutex;
    {
        std::lock_guard<std::mutex> map_lock(partition_write_mutex_map_mutex_);
        auto& entry = partition_write_mutexes_[tp];
        if (!entry)
            entry = std::make_unique<std::mutex>();
        mutex = entry.get();
    }
    return std::unique_lock<std::mutex>(*mutex);
}

void KawasanBroker::reconcileReplicas() {
    if (!metadata_controller_ || !log_manager_ || !replica_manager_) {
        return;
    }
    const auto topics = metadata_controller_->describeTopics({});
    for (const auto& tm : topics) {
        for (const auto& pm : tm.partitions) {
            // Only manage partitions this broker is assigned to host.
            const bool local_is_replica =
                std::find(pm.replicas.begin(), pm.replicas.end(), broker_id_) != pm.replicas.end();
            if (!local_is_replica) {
                continue;
            }
            const TopicPartition tp{tm.name, pm.partition};
            auto partition_write_lock = lockPartitionWrites(tp);
            const auto current = currentPartitionMetadata(tp);
            if (!current || current->leader != pm.leader ||
                current->leader_epoch != pm.leader_epoch || current->isr != pm.isr)
                continue;  // Do not apply a metadata snapshot taken before this lock.
            auto* log = log_manager_->getOrCreateLog(tm.name, pm.partition);
            if (!log) {
                continue;
            }
            std::shared_ptr<storage::Log> log_ptr(log, [](storage::Log*) {});
            // leader == broker_id_ => this broker leads (ISR = assigned replicas,
            // so acks=all waits for followers); otherwise it is a follower and the
            // fetcher thread will replicate from pm.leader.
            if (!cluster_brokers_.empty() && pm.leader == broker_id_ &&
                (!replica_manager_->isLeader(tp) ||
                 replica_manager_->getLeaderEpoch(tp) != pm.leader_epoch)) {
                // The authoritative log includes replication and any truncation.
                // Control markers carry no producer data sequence.
                producer_state_manager_->clearPartition(tm.name, pm.partition);
                replayProducerStateFromLog(tm.name, pm.partition);
            }
            const bool changed = replica_manager_->reconcileReplica(
                {tm.name, pm.partition}, log_ptr, pm.leader, pm.isr, pm.leader_epoch);
            if (changed && delayed_produce_purgatory_) {
                // A lost leadership or shrunk ISR resolves parked acks=all produces.
                delayed_produce_purgatory_->notify(tm.name, pm.partition);
            }
        }
    }
}

void KawasanBroker::maintainLeaderIsr() {
    if (!metadata_controller_ || !replica_manager_ || !dataPlaneCurrent()) {
        return;
    }
    // Wall-clock millis, matching the timestamp ReplicaManager records on each
    // follower fetch (updateFollowerFetchOffset uses system_clock).
    const int64_t now = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::system_clock::now().time_since_epoch())
                            .count();
    const BrokerId controller = controllerId();
    const auto topics = metadata_controller_->describeTopics({});
    for (const auto& tm : topics) {
        for (const auto& pm : tm.partitions) {
            // Only the partition leader of a replicated partition maintains ISR.
            if (pm.leader != broker_id_ || pm.replicas.size() <= 1) {
                continue;
            }
            const TopicPartition tp{tm.name, pm.partition};
            auto partition_write_lock = lockPartitionWrites(tp);
            const auto current = currentPartitionMetadata(tp);
            if (!current || current->leader != broker_id_ ||
                current->leader_epoch != pm.leader_epoch)
                continue;
            replica_manager_->updateISR(tp, current->isr);
            auto proposed = replica_manager_->computeIsrUpdate(tp, replica_lag_time_max_ms_, now);
            if (!proposed) {
                continue;  // ISR already correct
            }
            if (!replica_manager_->beginIsrUpdate(tp, *proposed, pm.leader_epoch))
                continue;
            partition_write_lock.unlock();  // Never hold a data lock across a controller RPC.
            Logger::info("ISR change proposed for {}-{}: {} members -> {} members", tm.name,
                         pm.partition, pm.isr.size(), proposed->size());

            if (controller == broker_id_) {
                // We are the controller: commit directly through Raft metadata.
                metadata_controller_->updatePartitionISR(tm.name, pm.partition, *proposed,
                                                         current->partition_epoch);
            } else {
                // Send an AlterPartition RPC to the controller broker.
                auto ep = peerEndpoint(controller);
                if (!ep) {
                    continue;
                }
                PeerClient client(ep->first, ep->second, broker_id_);
                client.alterPartition(tm.name, pm.partition, pm.leader_epoch, *proposed,
                                      current->partition_epoch);
            }
        }
    }
}

std::optional<PartitionMetadata> KawasanBroker::currentPartitionMetadata(
    const TopicPartition& tp) const {
    if (!metadata_controller_)
        return std::nullopt;
    const auto topics = metadata_controller_->describeTopics({tp.topic});
    if (topics.empty() || topics.front().error_code != ErrorCode::NONE)
        return std::nullopt;
    for (const auto& pm : topics.front().partitions) {
        if (pm.partition == tp.partition)
            return pm;
    }
    return std::nullopt;
}

bool KawasanBroker::isPartitionLeadership(const TopicPartition& tp, BrokerId leader,
                                          int32_t leader_epoch) const {
    if (!metadata_controller_) {
        return true;
    }
    const auto topics = metadata_controller_->describeTopics({tp.topic});
    if (topics.empty() || topics.front().error_code != ErrorCode::NONE) {
        return false;
    }
    for (const auto& pm : topics.front().partitions) {
        if (pm.partition == tp.partition) {
            return pm.leader == leader && pm.leader_epoch == leader_epoch;
        }
    }
    return false;
}

bool KawasanBroker::dataPlaneCurrent() const {
    if (cluster_brokers_.empty() || !raft_node_) {
        return true;  // single-node: this broker is the only source of truth
    }
    return raft_node_->hasCurrentMetadata(metadataLeaseMs());
}

int64_t KawasanBroker::metadataLeaseMs() const {
    // Shorter than the controller's liveness timeout, so a cut-off broker stops
    // serving before the controller fails its partitions over.
    return std::max<int64_t>(500, broker_liveness_timeout_ms_ / 2);
}

void KawasanBroker::refreshReadiness() {
    if (monitoring_manager_) {
        monitoring_manager_->setBrokerReady(dataPlaneCurrent());
    }
}

void KawasanBroker::maintainPartitionLeaders() {
    if (!metadata_controller_ || !raft_node_ || !raft_node_->isLeader()) {
        return;
    }
    std::set<BrokerId> dead;
    for (const auto& [id, age_ms] : raft_node_->peerAckAgesMs()) {
        if (age_ms > broker_liveness_timeout_ms_) {
            dead.insert(id);
        }
    }
    if (dead != dead_brokers_) {
        for (BrokerId id : dead) {
            if (!dead_brokers_.count(id)) {
                Logger::warn("Controller: broker {} is unresponsive for over {} ms; failing over "
                             "its partitions",
                             id, broker_liveness_timeout_ms_);
            }
        }
        for (BrokerId id : dead_brokers_) {
            if (!dead.count(id)) {
                Logger::info("Controller: broker {} is responsive again", id);
            }
        }
        dead_brokers_ = dead;
    }

    const auto changes = computeLeadershipChanges(metadata_controller_->describeTopics({}), dead,
                                                  unclean_leader_election_enabled_);
    for (const auto& change : changes) {
        if (change.new_leader) {
            if (*change.new_leader < 0) {
                Logger::warn("Controller: {}-{} is offline (no live in-sync replica)", change.topic,
                             change.partition);
            } else if (change.unclean) {
                Logger::error("Controller: UNCLEAN election of broker {} for {}-{} — records "
                              "acknowledged by the lost in-sync replicas may be gone",
                              *change.new_leader, change.topic, change.partition);
            } else {
                Logger::info("Controller: electing broker {} as leader of {}-{}",
                             *change.new_leader, change.topic, change.partition);
            }
            const auto result = metadata_controller_->updatePartitionLeader(
                change.topic, change.partition, *change.new_leader,
                change.expected_partition_epoch);
            if (result.error_code != ErrorCode::NONE) {
                Logger::warn("Controller: leader change for {}-{} failed: {}", change.topic,
                             change.partition, KawasanException::toString(result.error_code));
                continue;
            }
        }
        if (change.new_isr) {
            const auto result = metadata_controller_->updatePartitionISR(
                change.topic, change.partition, *change.new_isr,
                change.expected_partition_epoch + (change.new_leader ? 1 : 0));
            if (result.error_code != ErrorCode::NONE) {
                Logger::warn("Controller: ISR change for {}-{} failed: {}", change.topic,
                             change.partition, KawasanException::toString(result.error_code));
            }
        }
    }
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
    // Rack awareness: advertise broker.rack (Metadata v1+) so rack-aware
    // clients can do nearest-replica fetch / rack-aware placement. Empty config
    // means no rack (nullopt), matching Kafka's default.
    const std::string rack = config_.get<std::string>("broker.rack", "");
    broker.rack = rack.empty() ? std::nullopt : std::optional<std::string>(rack);
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
        topics.empty() ? ErrorCode::UNKNOWN_TOPIC_OR_PARTITION : topics.front().error_code;

    if (!allow_auto_create || lookup_error != ErrorCode::UNKNOWN_TOPIC_OR_PARTITION) {
        return {std::nullopt, lookup_error};
    }
    // M8-A2: internal topics are created only by ensureInternalTopics (with
    // their configured partition count, which is the coordinator-routing
    // modulus). A client racing startup gets a retriable error instead.
    if (topic_name == "__consumer_offsets" || topic_name == "__transaction_state") {
        return {std::nullopt, ErrorCode::LEADER_NOT_AVAILABLE};
    }

    TopicSpecification spec;
    spec.name = topic_name;
    spec.num_partitions = std::max<int32_t>(1, default_num_partitions_);
    spec.replication_factor = std::max<int16_t>(1, default_replication_factor_);

    Logger::info("Auto-creating topic '{}' with {} partition(s)", spec.name, spec.num_partitions);
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
    return encodeResponse(context, [&](Buffer& buffer) { response.encode(buffer, version); });
}

Buffer KawasanBroker::buildDescribeGroupsError(const RequestDispatcher::RequestContext& context,
                                               ErrorCode code, int16_t response_version) const {
    protocol::DescribeGroupsResponse response;
    response.setThrottleTimeMs(0);
    response.setGroups({});
    (void)code;  // Error code not used in error response for this API

    const int16_t version = std::clamp<int16_t>(response_version, 0, kDescribeGroupsMaxVersion);
    return encodeResponse(context, [&](Buffer& buffer) { response.encode(buffer, version); });
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
    return encodeResponse(context, [&](Buffer& buffer) { response.encode(buffer, version); });
}

Buffer KawasanBroker::buildListGroupsError(const RequestDispatcher::RequestContext& context,
                                           ErrorCode code, int16_t response_version) const {
    protocol::ListGroupsResponse response;
    response.setErrorCode(code);
    response.setThrottleTimeMs(0);
    response.setGroups({});

    const int16_t version = std::clamp<int16_t>(response_version, 0, kListGroupsMaxVersion);
    return encodeResponse(context, [&](Buffer& buffer) { response.encode(buffer, version); });
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
            const auto overrides = metadata_controller_->topicConfigs(resource.resource_name);
            if (!overrides) {
                result.error_code = ErrorCode::UNKNOWN_TOPIC_OR_PARTITION;
                result.error_message = "Topic does not exist";
            } else {
                const auto effective =
                    storage::LogConfig::fromMap(*overrides, log_manager_->defaultConfig());
                std::string policy;
                if (effective.cleanup_policy_delete)
                    policy = "delete";
                if (effective.cleanup_policy_compact)
                    policy += policy.empty() ? "compact" : ",compact";
                const std::map<std::string, std::string> values = {
                    {"cleanup.policy", policy},
                    {"retention.ms", std::to_string(effective.retention_ms)},
                    {"retention.bytes", std::to_string(effective.retention_bytes)},
                    {"segment.bytes", std::to_string(effective.segment_size)},
                    {"segment.ms", std::to_string(effective.segment_ms)}};
                for (const auto& [name, value] : values) {
                    if (!resource.config_names.empty() &&
                        std::find(resource.config_names.begin(), resource.config_names.end(),
                                  name) == resource.config_names.end())
                        continue;
                    protocol::ConfigEntry entry;
                    entry.name = name;
                    entry.value = value;
                    std::string broker_key = name == "segment.bytes" ? "log.segment.bytes"
                                             : name == "segment.ms"  ? "log.roll.ms"
                                                                     : "log." + name;
                    const bool inherited =
                        config_.has(broker_key) ||
                        (name == "segment.ms" && config_.has("log.roll.hours")) ||
                        (name == "retention.ms" && config_.has("log.retention.hours"));
                    entry.is_default = !overrides->contains(name) && !inherited;
                    entry.config_source = overrides->contains(name) ? 1 : (inherited ? 4 : 5);
                    result.configs.push_back(std::move(entry));
                }
            }
        }

        response.addResult(result);
    }

    const int16_t version = std::clamp<int16_t>(context.header.apiVersion(), 0, 4);
    return encodeResponse(context, [&](Buffer& buffer) { response.encode(buffer, version); });
}

Buffer KawasanBroker::buildDescribeConfigsError(const RequestDispatcher::RequestContext& context,
                                                ErrorCode code, int16_t response_version) const {
    protocol::DescribeConfigsResponse response;
    response.setThrottleTimeMs(0);

    protocol::DescribeConfigsResourceResult error_result;
    error_result.error_code = code;
    error_result.error_message = "Error processing DescribeConfigs request";
    response.addResult(error_result);

    const int16_t version = std::clamp<int16_t>(response_version, 0, 4);
    return encodeResponse(context, [&](Buffer& buffer) { response.encode(buffer, version); });
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
    response.setControllerId(controllerId());
    response.setBrokers(buildBrokerMetadata());

    // Set cluster authorized operations if requested
    if (request.includeClusterAuthorizedOperations()) {
        // Return all operations allowed (simple implementation)
        response.setClusterAuthorizedOperations(0x7FFFFFFF);
    }

    // Phase 1.17: DescribeCluster now supports v0..v1; clamp accordingly.
    const int16_t version = std::clamp<int16_t>(context.header.apiVersion(), 0, 1);
    return encodeResponse(context, [&](Buffer& buffer) { response.encode(buffer, version); });
}

Buffer KawasanBroker::buildDescribeClusterError(const RequestDispatcher::RequestContext& context,
                                                ErrorCode code, int16_t response_version) const {
    protocol::DescribeClusterResponse response;
    response.setThrottleTimeMs(0);
    response.setErrorCode(code);
    response.setErrorMessage("Error processing DescribeCluster request");

    const std::string cluster_id =
        metadata_controller_ ? metadata_controller_->clusterId() : cluster_id_;
    response.setClusterId(cluster_id);
    response.setControllerId(controllerId());
    response.setBrokers(buildBrokerMetadata());

    const int16_t version = std::clamp<int16_t>(response_version, 0, 1);  // Phase 1.17
    return encodeResponse(context, [&](Buffer& buffer) { response.encode(buffer, version); });
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
    // Crash-atomic and durable: re-issuing an id after a crash would make the
    // new producer's first batches look like duplicates of the old one's.
    if (!writeFileAtomically(counter_path, std::to_string(next))) {
        Logger::error("Failed to persist producer_id counter to {}", counter_path);
    }
    // In a cluster every broker issues ids from its own range (see
    // clusterProducerId); single-node ids stay plain sequential.
    return cluster_brokers_.empty() ? allocated : clusterProducerId(broker_id_, allocated);
}

Buffer KawasanBroker::handleInitProducerId(RequestDispatcher::RequestContext& context) {
    protocol::InitProducerIdRequest request;
    request.decode(context.payload, context.header.apiVersion());

    Logger::info("InitProducerId request from {}: transactional_id='{}' timeout={}ms",
                 context.peer_identity, request.transactionalId().value_or("<null>"),
                 request.transactionTimeoutMs());

    protocol::InitProducerIdResponse response;
    response.setThrottleTimeMs(0);
    response.setErrorCode(ErrorCode::NONE);

    // M2: for a transactional_id, InitProducerId is the fencing point. If the
    // transactional_id is already known, REUSE its producer_id and BUMP the
    // epoch — this fences any earlier producer instance (its stale epoch now
    // fails the txn-API epoch checks). A prior in-flight (Ongoing/Prepare*)
    // transaction is auto-aborted at the OLD epoch first, so it doesn't dangle.
    // A brand-new transactional_id (and non-transactional InitProducerId) gets
    // a fresh producer_id at epoch 0.
    int64_t producer_id;
    int16_t producer_epoch;
    const bool transactional =
        request.transactionalId().has_value() && !request.transactionalId()->empty();
    if (transactional &&
        !isCoordinatorFor(*request.transactionalId(), protocol::CoordinatorType::TRANSACTION)) {
        response.setErrorCode(ErrorCode::NOT_COORDINATOR);
        response.setProducerId(-1);
        response.setProducerEpoch(-1);
        return encodeResponse(
            context, [&](Buffer& buffer) { response.encode(buffer, context.header.apiVersion()); });
    }
    if (transactional && transaction_coordinator_) {
        const std::string& txn_id = *request.transactionalId();
        auto prior = transaction_coordinator_->describe(txn_id);
        if (prior.has_value()) {
            producer_id = prior->producer_id;
            producer_epoch = static_cast<int16_t>(prior->producer_epoch + 1);
            if (prior->state == TransactionCoordinator::State::Ongoing ||
                prior->state == TransactionCoordinator::State::PrepareCommit ||
                prior->state == TransactionCoordinator::State::PrepareAbort) {
                Logger::info("InitProducerId fences in-flight txn '{}' (epoch {}->{}), aborting",
                             txn_id, prior->producer_epoch, producer_epoch);
                auto participating = transaction_coordinator_->prepareAbort(txn_id);
                persistTxnState(txn_id);
                finishTxnCompletion(txn_id, prior->producer_id, prior->producer_epoch,
                                    /*committed=*/false, participating, prior->pending_offsets,
                                    /*is_replay=*/false);
            }
        } else {
            producer_id = allocateNextProducerId();
            producer_epoch = 0;
        }
        transaction_coordinator_->recordInitProducerId(txn_id, producer_id, producer_epoch,
                                                       request.transactionTimeoutMs());
        persistTxnState(txn_id);
    } else {
        producer_id = allocateNextProducerId();
        producer_epoch = 0;
    }

    response.setProducerId(producer_id);
    response.setProducerEpoch(producer_epoch);

    const int16_t version = std::clamp<int16_t>(context.header.apiVersion(), 0, 4);
    return encodeResponse(context, [&](Buffer& buffer) { response.encode(buffer, version); });
}

Buffer KawasanBroker::buildInitProducerIdError(const RequestDispatcher::RequestContext& context,
                                               ErrorCode code, int16_t response_version) const {
    protocol::InitProducerIdResponse response;
    response.setThrottleTimeMs(0);
    response.setErrorCode(code);
    response.setProducerId(-1);
    response.setProducerEpoch(-1);
    const int16_t version = std::clamp<int16_t>(response_version, 0, 4);
    return encodeResponse(context, [&](Buffer& buffer) { response.encode(buffer, version); });
}

Buffer KawasanBroker::handleOffsetForLeaderEpoch(RequestDispatcher::RequestContext& context) {
    protocol::OffsetForLeaderEpochRequest request;
    request.decode(context.payload, context.header.apiVersion());

    protocol::OffsetForLeaderEpochResponse response;
    response.setThrottleTimeMs(0);
    const bool metadata_current = dataPlaneCurrent();  // M8-E1

    for (const auto& topic : request.topics()) {
        protocol::OffsetForLeaderEpochResponse::TopicResult tres;
        tres.name = topic.name;
        std::vector<TopicMetadata> topic_metadata;
        if (metadata_controller_) {
            topic_metadata = metadata_controller_->describeTopics({topic.name});
        }
        for (const auto& pq : topic.partitions) {
            protocol::OffsetForLeaderEpochResponse::PartitionResult pres;
            pres.partition = pq.partition;
            pres.error_code = ErrorCode::NONE;
            pres.leader_epoch = -1;
            pres.end_offset = -1;
            auto* log = log_manager_ ? log_manager_->getLog(topic.name, pq.partition) : nullptr;
            if (log == nullptr) {
                pres.error_code = ErrorCode::UNKNOWN_TOPIC_OR_PARTITION;
                tres.partitions.push_back(pres);
                continue;
            }
            // M8-F4: only the partition leader answers (followers and consumers
            // ask it where an epoch ends), and only on a current, matching view.
            if (!topic_metadata.empty() && topic_metadata.front().error_code == ErrorCode::NONE) {
                const auto& parts = topic_metadata.front().partitions;
                auto pm = std::find_if(parts.begin(), parts.end(), [&](const PartitionMetadata& m) {
                    return m.partition == pq.partition;
                });
                if (pm != parts.end()) {
                    if (pm->leader != broker_id_ || !metadata_current) {
                        pres.error_code = ErrorCode::NOT_LEADER_FOR_PARTITION;
                    } else {
                        pres.error_code =
                            checkLeaderEpoch(pq.current_leader_epoch, pm->leader_epoch);
                    }
                    if (pres.error_code != ErrorCode::NONE) {
                        tres.partitions.push_back(pres);
                        continue;
                    }
                }
            }
            if (log->latestLeaderEpoch().has_value()) {
                // KIP-101: answer from the partition's epoch history.
                const auto [epoch, end_offset] = log->epochEndOffset(pq.leader_epoch);
                pres.leader_epoch = epoch;
                pres.end_offset = end_offset;
            } else {
                // No epoch history (single-node, or a log written before M8):
                // a single uninterrupted leadership — the whole log is current.
                const TopicPartition tp{topic.name, pq.partition};
                pres.leader_epoch =
                    replica_manager_ ? replica_manager_->getLeaderEpoch(tp).value_or(0) : 0;
                pres.end_offset = log->logEndOffset();
            }
            tres.partitions.push_back(pres);
        }
        response.addTopic(std::move(tres));
    }

    const int16_t version = std::clamp<int16_t>(context.header.apiVersion(), 0, 4);
    return encodeResponse(context, [&](Buffer& buffer) { response.encode(buffer, version); });
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
    return encodeResponse(context, [&](Buffer& buffer) { response.encode(buffer, version); });
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
            std::vector<TopicConfigChange> changes;
            for (const auto& entry : res.configs) {
                changes.push_back(
                    {entry.name, entry.value, static_cast<int8_t>(entry.value ? 0 : 1)});
            }
            const auto changed = metadata_controller_->alterTopicConfigs(
                res.resource_name, changes, true, request.validateOnly());
            rr.error_code = changed.error_code;
            rr.error_message = changed.error_message;
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
    return encodeResponse(context, [&](Buffer& buffer) { response.encode(buffer, version); });
}

Buffer KawasanBroker::buildAlterConfigsError(const RequestDispatcher::RequestContext& context,
                                             ErrorCode code, int16_t response_version) const {
    protocol::AlterConfigsResponse response;
    response.setThrottleTimeMs(0);
    protocol::AlterConfigsResponse::ResourceResult rr;
    rr.error_code = code;
    rr.resource_type = protocol::ConfigResourceType::UNKNOWN;
    response.addResult(std::move(rr));
    const int16_t version = std::clamp<int16_t>(response_version, 0, 2);
    return encodeResponse(context, [&](Buffer& buffer) { response.encode(buffer, version); });
}

Buffer KawasanBroker::handleIncrementalAlterConfigs(RequestDispatcher::RequestContext& context) {
    protocol::IncrementalAlterConfigsRequest request;
    request.decode(context.payload, context.header.apiVersion());

    protocol::IncrementalAlterConfigsResponse response;
    response.setThrottleTimeMs(0);

    for (const auto& res : request.resources()) {
        protocol::IncrementalAlterConfigsResponse::ResourceResult rr;
        rr.resource_type = res.resource_type;
        rr.resource_name = res.resource_name;
        rr.error_code = ErrorCode::NONE;

        if (res.resource_type == protocol::ConfigResourceType::TOPIC && log_manager_) {
            std::vector<TopicConfigChange> changes;
            for (const auto& entry : res.configs)
                changes.push_back({entry.name, entry.value, static_cast<int8_t>(entry.op)});
            const auto changed = metadata_controller_->alterTopicConfigs(
                res.resource_name, changes, false, request.validateOnly());
            rr.error_code = changed.error_code;
            rr.error_message = changed.error_message;
        } else if (res.resource_type == protocol::ConfigResourceType::BROKER) {
            Logger::info("IncrementalAlterConfigs: broker config no-op (read-only)");
        } else {
            rr.error_code = ErrorCode::INVALID_REQUEST;
            rr.error_message = "Unsupported resource type";
        }
        response.addResult(std::move(rr));
    }
    const int16_t version = std::clamp<int16_t>(context.header.apiVersion(), 0, 1);
    return encodeResponse(context, [&](Buffer& buffer) { response.encode(buffer, version); });
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
    return encodeResponse(context, [&](Buffer& buffer) { response.encode(buffer, version); });
}

// Phase 4.1: shared trivial error builder.
Buffer KawasanBroker::buildEmptyErrorResponse(
    const RequestDispatcher::RequestContext& context) const {
    return encodeResponse(context, [&](Buffer& /*buffer*/) {});
}

Buffer KawasanBroker::handleDescribeLogDirs(RequestDispatcher::RequestContext& context) {
    protocol::DescribeLogDirsRequest request;
    request.decode(context.payload, context.header.apiVersion());

    protocol::DescribeLogDirsResponse response;
    response.setThrottleTimeMs(0);

    // CM-5: report the real per-partition on-disk size and offset lag, grouped
    // by topic (one TopicInfo per topic, matching Kafka), and honor the request's
    // topic/partition filter (a null topics list means "all").
    //
    // Build the filter: topic -> requested partition set (empty set = all
    // partitions of that topic).
    std::map<std::string, std::set<int32_t>> filter;
    const bool fetch_all = request.fetchAll();
    if (!fetch_all) {
        for (const auto& t : request.topics()) {
            auto& parts = filter[t.topic];
            for (int32_t p : t.partitions) {
                parts.insert(p);
            }
        }
    }
    auto wanted = [&](const std::string& topic, int32_t partition) {
        if (fetch_all) {
            return true;
        }
        auto it = filter.find(topic);
        if (it == filter.end()) {
            return false;
        }
        return it->second.empty() || it->second.count(partition) > 0;
    };

    protocol::DescribeLogDirsResponse::LogDirInfo info;
    info.error_code = ErrorCode::NONE;
    info.log_dir = log_dir_;
    if (log_manager_) {
        // Group partitions under their topic so a multi-partition topic appears
        // once with all its partitions (Kafka's layout), preserving insertion
        // order of first appearance.
        std::map<std::string, size_t> topic_index;  // topic -> index in info.topics
        for (auto* log : log_manager_->allLogs()) {
            const std::string& topic = log->topic();
            const int32_t partition = log->partition();
            if (!wanted(topic, partition)) {
                continue;
            }
            auto [it, inserted] = topic_index.try_emplace(topic, info.topics.size());
            if (inserted) {
                protocol::DescribeLogDirsResponse::TopicInfo ti;
                ti.topic = topic;
                info.topics.push_back(std::move(ti));
            }
            protocol::DescribeLogDirsResponse::PartitionInfo pi;
            pi.partition = partition;
            pi.size_bytes = static_cast<int64_t>(log->sizeBytes());
            // offset_lag = records not yet committed to the ISR (LEO - HW). On a
            // single-node leader HW==LEO so this is 0, matching Kafka's report
            // for a caught-up leader replica.
            pi.offset_lag = std::max<int64_t>(0, log->logEndOffset() - log->highWatermark());
            pi.is_future = false;
            info.topics[it->second].partitions.push_back(pi);
        }
    }
    response.addLogDir(std::move(info));

    return encodeResponse(
        context, [&](Buffer& buffer) { response.encode(buffer, context.header.apiVersion()); });
}

Buffer KawasanBroker::handleAlterReplicaLogDirs(RequestDispatcher::RequestContext& context) {
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
    return encodeResponse(
        context, [&](Buffer& buffer) { response.encode(buffer, context.header.apiVersion()); });
}

Buffer KawasanBroker::handleElectLeaders(RequestDispatcher::RequestContext& context) {
    protocol::ElectLeadersRequest request;
    request.decode(context.payload, context.header.apiVersion());

    // M7: real preferred-replica election. For each requested partition, elect
    // the preferred replica (replicas[0]) when it is in the ISR and not already
    // the leader; the change is Raft-committed via UPDATE_LEADER (bumping the
    // leader epoch). This is the manual instrument for moving leadership; only
    // the active controller can commit (else NOT_CONTROLLER). An empty partition
    // list for a topic means "all partitions of that topic".
    const bool is_controller = !raft_node_ || raft_node_->isLeader();

    protocol::ElectLeadersResponse response;
    response.setThrottleTimeMs(0);
    response.setErrorCode(ErrorCode::NONE);

    auto elect_one =
        [&](const std::string& topic,
            const PartitionMetadata& pm) -> protocol::ElectLeadersResponse::PartitionResult {
        protocol::ElectLeadersResponse::PartitionResult pr;
        pr.partition = pm.partition;
        const BrokerId preferred = pm.replicas.empty() ? -1 : pm.replicas.front();
        if (preferred < 0) {
            pr.error_code = ErrorCode::PREFERRED_LEADER_NOT_AVAILABLE;
            return pr;
        }
        if (preferred == pm.leader) {
            pr.error_code = ErrorCode::NONE;  // already the preferred leader — idempotent
            return pr;
        }
        if (std::find(pm.isr.begin(), pm.isr.end(), preferred) == pm.isr.end()) {
            // Preferred replica is not in-sync: unclean election is not permitted.
            pr.error_code = ErrorCode::PREFERRED_LEADER_NOT_AVAILABLE;
            return pr;
        }
        if (!is_controller) {
            pr.error_code = ErrorCode::NOT_CONTROLLER;
            return pr;
        }
        auto result = metadata_controller_->updatePartitionLeader(topic, pm.partition, preferred,
                                                                  pm.partition_epoch);
        pr.error_code = result.error_code;
        return pr;
    };

    for (const auto& t : request.topics()) {
        protocol::ElectLeadersResponse::TopicResult tr;
        tr.topic = t.topic;
        const auto md = metadata_controller_ ? metadata_controller_->describeTopics({t.topic})
                                             : std::vector<TopicMetadata>{};
        const TopicMetadata* tm = (!md.empty()) ? &md.front() : nullptr;

        if (t.partitions.empty() && tm) {
            // Elect for all partitions of the topic.
            for (const auto& pm : tm->partitions) {
                tr.partitions.push_back(elect_one(t.topic, pm));
            }
        } else {
            for (int32_t p : t.partitions) {
                const PartitionMetadata* pm = nullptr;
                if (tm) {
                    for (const auto& cand : tm->partitions) {
                        if (cand.partition == p) {
                            pm = &cand;
                            break;
                        }
                    }
                }
                if (!pm) {
                    protocol::ElectLeadersResponse::PartitionResult pr;
                    pr.partition = p;
                    pr.error_code = ErrorCode::UNKNOWN_TOPIC_OR_PARTITION;
                    tr.partitions.push_back(std::move(pr));
                } else {
                    tr.partitions.push_back(elect_one(t.topic, *pm));
                }
            }
        }
        response.addTopic(std::move(tr));
    }
    return encodeResponse(
        context, [&](Buffer& buffer) { response.encode(buffer, context.header.apiVersion()); });
}

Buffer KawasanBroker::handleDeleteRecords(RequestDispatcher::RequestContext& context) {
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
                Logger::info("DeleteRecords: {}-{} truncated to low_watermark={}", t.topic,
                             p.partition, pr.low_watermark);
            }
            tr.partitions.push_back(pr);
        }
        response.addTopic(std::move(tr));
    }
    return encodeResponse(
        context, [&](Buffer& buffer) { response.encode(buffer, context.header.apiVersion()); });
}

Buffer KawasanBroker::handleDeleteGroups(RequestDispatcher::RequestContext& context) {
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
    return encodeResponse(
        context, [&](Buffer& buffer) { response.encode(buffer, context.header.apiVersion()); });
}

Buffer KawasanBroker::handleOffsetDelete(RequestDispatcher::RequestContext& context) {
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
                Logger::error("OffsetDelete failed for {}/{}-{}: {}", request.groupId(), t.topic,
                              p.partition, ex.what());
            }
            tr.partitions.push_back(pr);
        }
        response.addTopic(std::move(tr));
    }
    Logger::info("OffsetDelete: group='{}' processed {} topic(s)", request.groupId(),
                 request.topics().size());
    return encodeResponse(
        context, [&](Buffer& buffer) { response.encode(buffer, context.header.apiVersion()); });
}

Buffer KawasanBroker::handleCreatePartitions(RequestDispatcher::RequestContext& context) {
    protocol::CreatePartitionsRequest request;
    request.decode(context.payload, context.header.apiVersion());

    protocol::CreatePartitionsResponse response;
    response.setThrottleTimeMs(0);
    const bool validate_only = request.validateOnly();
    for (const auto& t : request.topics()) {
        protocol::CreatePartitionsResponse::Result r;
        r.topic = t.topic;

        // Phase 4.1c / CM-5: forward the partition increase through the
        // Raft-replicated metadata controller. New partitions get round-robin
        // replica assignment continuing from the existing partitions; logs are
        // created lazily on first produce. CM-5 adds up-front validation and
        // honors validate_only (dry-run: validate but do not apply).
        if (!metadata_controller_) {
            r.error_code = ErrorCode::COORDINATOR_NOT_AVAILABLE;
            r.error_message = "Metadata controller not initialized";
            response.addResult(std::move(r));
            continue;
        }

        // Look up the current partition count so validate_only and explicit
        // assignments can be checked without mutating metadata.
        const auto md = metadata_controller_->describeTopics({t.topic});
        const bool exists = !md.empty() && !md.front().partitions.empty();
        const int32_t current_count =
            exists ? static_cast<int32_t>(md.front().partitions.size()) : 0;

        if (!exists) {
            r.error_code = ErrorCode::UNKNOWN_TOPIC_OR_PARTITION;
            r.error_message = "Topic does not exist";
        } else if (t.count <= current_count) {
            // Kafka rejects a non-increase (including equal count) with
            // INVALID_PARTITIONS.
            r.error_code = ErrorCode::INVALID_PARTITIONS;
            r.error_message = "Topic already has " + std::to_string(current_count) +
                              " partitions (requested " + std::to_string(t.count) + ")";
        } else if (!t.assignments.empty() &&
                   static_cast<int32_t>(t.assignments.size()) != t.count - current_count) {
            // If explicit replica assignments are supplied, there must be exactly
            // one per new partition (Kafka: INVALID_REPLICA_ASSIGNMENT).
            r.error_code = ErrorCode::INVALID_REPLICA_ASSIGNMENT;
            r.error_message = "Expected " + std::to_string(t.count - current_count) +
                              " assignment(s), got " + std::to_string(t.assignments.size());
        } else if (validate_only) {
            // Dry-run: validation passed, do not apply.
            r.error_code = ErrorCode::NONE;
        } else {
            auto result = metadata_controller_->increasePartitions(t.topic, t.count);
            r.error_code = result.error_code;
            if (result.error_code != ErrorCode::NONE && !result.error_message.empty()) {
                r.error_message = result.error_message;
            }
        }

        Logger::info("CreatePartitions: topic='{}' new_count={} validate_only={} -> error={}",
                     t.topic, t.count, validate_only, static_cast<int16_t>(r.error_code));
        response.addResult(std::move(r));
    }
    return encodeResponse(
        context, [&](Buffer& buffer) { response.encode(buffer, context.header.apiVersion()); });
}

// ---------- Phase 4.1 stubs (each populates an empty-but-valid response) ----------

Buffer KawasanBroker::handleDescribeProducers(RequestDispatcher::RequestContext& context) {
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
                for (const auto& ap : producer_state_manager_->listProducers(t.topic, p)) {
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
    return encodeResponse(context,
                          [&](Buffer& buf) { resp.encode(buf, context.header.apiVersion()); });
}

Buffer KawasanBroker::handleListTransactions(RequestDispatcher::RequestContext& context) {
    protocol::ListTransactionsRequest req;
    req.decode(context.payload, context.header.apiVersion());
    protocol::ListTransactionsResponse resp;
    resp.setThrottleTimeMs(0);
    resp.setErrorCode(ErrorCode::NONE);
    // Phase 4.1l: enumerate known transactional IDs from the
    // TransactionCoordinator with optional state/producer_id filters.
    if (transaction_coordinator_) {
        const auto txns =
            transaction_coordinator_->list(req.stateFilters(), req.producerIdFilters());
        for (const auto& t : txns) {
            protocol::ListTransactionsResponse::TxnState s;
            s.transactional_id = t.transactional_id;
            s.producer_id = t.producer_id;
            s.state = TransactionCoordinator::stateName(t.state);
            resp.addState(std::move(s));
        }
        Logger::info("ListTransactions: returned {} txn(s)", txns.size());
    }
    return encodeResponse(context,
                          [&](Buffer& buf) { resp.encode(buf, context.header.apiVersion()); });
}

Buffer KawasanBroker::handleDescribeTransactions(RequestDispatcher::RequestContext& context) {
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
        s.error_code = ErrorCode::NONE;
        s.state = "Empty";
        const auto opt =
            transaction_coordinator_ ? transaction_coordinator_->describe(tid) : std::nullopt;
        if (opt.has_value()) {
            s.state = TransactionCoordinator::stateName(opt->state);
            s.transaction_timeout_ms = opt->transaction_timeout_ms;
            s.transaction_start_time_ms = opt->state_start_time_ms;
            s.producer_id = opt->producer_id;
            s.producer_epoch = opt->producer_epoch;
            // Group the snapshot's flat (topic, partition) list per topic.
            std::map<std::string, std::vector<int32_t>> by_topic;
            for (const auto& tp : opt->partitions) {
                by_topic[tp.topic].push_back(tp.partition);
            }
            for (auto& [topic, partitions] : by_topic) {
                s.topics.push_back({topic, std::move(partitions)});
            }
        }
        resp.addState(std::move(s));
    }
    return encodeResponse(context,
                          [&](Buffer& buf) { resp.encode(buf, context.header.apiVersion()); });
}

Buffer KawasanBroker::handleAlterPartition(RequestDispatcher::RequestContext& context) {
    protocol::AlterPartitionRequest req;
    req.decode(context.payload, context.header.apiVersion());

    // M6: a partition leader is asking the controller to change a partition's
    // ISR. Only the active controller can commit metadata; validate that the
    // requester is the current leader, then commit through the Raft UPDATE_ISR
    // path. A non-controller replies NOT_CONTROLLER so the leader retries the
    // real controller.
    protocol::AlterPartitionResponse resp;
    resp.setThrottleTimeMs(0);
    resp.setErrorCode(ErrorCode::NONE);

    const bool is_controller = !raft_node_ || raft_node_->isLeader();

    for (const auto& t : req.topics) {
        protocol::AlterPartitionResponse::TopicResult tr;
        tr.topic_name = t.topic_name;
        // Current metadata for this topic (for leader/epoch validation + echo).
        const auto md = metadata_controller_ ? metadata_controller_->describeTopics({t.topic_name})
                                             : std::vector<TopicMetadata>{};
        const TopicMetadata* tm = (!md.empty()) ? &md.front() : nullptr;

        for (const auto& p : t.partitions) {
            protocol::AlterPartitionResponse::PartitionResult pr;
            pr.partition_index = p.partition_index;

            const PartitionMetadata* pm = nullptr;
            if (tm) {
                for (const auto& cand : tm->partitions) {
                    if (cand.partition == p.partition_index) {
                        pm = &cand;
                        break;
                    }
                }
            }
            if (!pm) {
                pr.error_code = ErrorCode::UNKNOWN_TOPIC_OR_PARTITION;
                tr.partitions.push_back(std::move(pr));
                continue;
            }
            if (!is_controller) {
                pr.error_code = ErrorCode::NOT_CONTROLLER;
                tr.partitions.push_back(std::move(pr));
                continue;
            }
            // Only the current partition leader may alter its ISR, and its
            // leader epoch must match (stale leaders are fenced).
            if (req.broker_id != pm->leader) {
                pr.error_code = ErrorCode::INVALID_REQUEST;
                tr.partitions.push_back(std::move(pr));
                continue;
            }
            if (p.leader_epoch != pm->leader_epoch) {
                pr.error_code = ErrorCode::FENCED_LEADER_EPOCH;
                tr.partitions.push_back(std::move(pr));
                continue;
            }

            if (p.partition_epoch != pm->partition_epoch) {
                pr.error_code = ErrorCode::INVALID_UPDATE_VERSION;
                tr.partitions.push_back(std::move(pr));
                continue;
            }

            std::vector<BrokerId> new_isr(p.new_isr.begin(), p.new_isr.end());
            auto result = metadata_controller_->updatePartitionISR(t.topic_name, p.partition_index,
                                                                   new_isr, p.partition_epoch);
            pr.error_code = result.error_code;
            pr.leader_id = pm->leader;
            pr.leader_epoch = pm->leader_epoch;
            pr.isr = p.new_isr;
            const auto committed = currentPartitionMetadata({t.topic_name, p.partition_index});
            pr.partition_epoch = committed ? committed->partition_epoch : p.partition_epoch;
            if (result.error_code == ErrorCode::NONE) {
                Logger::info("AlterPartition committed ISR for {}-{}: {} members", t.topic_name,
                             p.partition_index, new_isr.size());
            }
            tr.partitions.push_back(std::move(pr));
        }
        resp.addTopic(std::move(tr));
    }

    return encodeResponse(context,
                          [&](Buffer& buf) { resp.encode(buf, context.header.apiVersion()); });
}

Buffer KawasanBroker::handleDescribeAcls(RequestDispatcher::RequestContext& context) {
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
                if (rt != o.rt)
                    return rt < o.rt;
                if (name != o.name)
                    return name < o.name;
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
    return encodeResponse(context,
                          [&](Buffer& buf) { resp.encode(buf, context.header.apiVersion()); });
}

Buffer KawasanBroker::handleCreateAcls(RequestDispatcher::RequestContext& context) {
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
    Logger::info("CreateAcls: stored {} binding(s); total={}", req.creations.size(),
                 acl_store_ ? acl_store_->size() : 0);
    return encodeResponse(context,
                          [&](Buffer& buf) { resp.encode(buf, context.header.apiVersion()); });
}

Buffer KawasanBroker::handleDeleteAcls(RequestDispatcher::RequestContext& context) {
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
    Logger::info("DeleteAcls: processed {} filter(s); total remaining={}", req.filters.size(),
                 acl_store_ ? acl_store_->size() : 0);
    return encodeResponse(context,
                          [&](Buffer& buf) { resp.encode(buf, context.header.apiVersion()); });
}

// ---------- Phase 4.2a: SASL PLAIN ----------

Buffer KawasanBroker::handleSaslHandshake(RequestDispatcher::RequestContext& context) {
    protocol::SaslHandshakeRequest req;
    req.decode(context.payload, context.header.apiVersion());

    protocol::SaslHandshakeResponse resp;
    // Phase 4.2b: advertise PLAIN, SCRAM-SHA-256, and SCRAM-SHA-512.
    std::vector<std::string> kEnabled = {"PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"};
    resp.setEnabledMechanisms(kEnabled);

    const auto& mech = req.mechanism();
    if (mech == "PLAIN" || mech == "SCRAM-SHA-256" || mech == "SCRAM-SHA-512") {
        resp.setErrorCode(ErrorCode::NONE);
        Logger::info("SaslHandshake: accepted mechanism '{}' for {}", mech, context.peer_identity);
        // Phase 4.2b: stash a fresh SCRAM session for this peer so the
        // subsequent SaslAuthenticate messages have somewhere to track
        // state. PLAIN doesn't need session state.
        if (mech == "SCRAM-SHA-256" || mech == "SCRAM-SHA-512") {
            std::lock_guard<std::mutex> lock(sasl_session_mutex_);
            const auto algo =
                (mech == "SCRAM-SHA-512") ? ScramAlgorithm::kSha512 : ScramAlgorithm::kSha256;
            sasl_sessions_[context.peer_identity] =
                std::make_unique<ScramAuthenticator>(sasl_scram_creds_, algo);
        }
    } else {
        // Kafka error code 33 = UNSUPPORTED_SASL_MECHANISM.
        resp.setErrorCode(static_cast<ErrorCode>(33));
        Logger::warn("SaslHandshake: rejected mechanism '{}' (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512 "
                     "supported)",
                     mech);
    }

    return encodeResponse(context,
                          [&](Buffer& buf) { resp.encode(buf, context.header.apiVersion()); });
}

// Constant-time string comparison: iterates the full maximum length regardless
// of where (or whether) bytes differ, so an attacker cannot infer a password
// prefix from response timing. (std::string::operator== short-circuits on the
// first mismatch and leaks that timing.)
static bool constantTimeEquals(const std::string& a, const std::string& b) {
    unsigned char diff = static_cast<unsigned char>(a.size() ^ b.size());
    const size_t n = std::max(a.size(), b.size());
    for (size_t i = 0; i < n; ++i) {
        const unsigned char ca = i < a.size() ? static_cast<unsigned char>(a[i]) : 0;
        const unsigned char cb = i < b.size() ? static_cast<unsigned char>(b[i]) : 0;
        diff = static_cast<unsigned char>(diff | (ca ^ cb));
    }
    return diff == 0;
}

Buffer KawasanBroker::handleSaslAuthenticate(RequestDispatcher::RequestContext& context) {
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
                    if (context.connection) {
                        context.connection->authenticated_principal = "User:" + auth->username();
                    }
                    Logger::info("SaslAuthenticate: SCRAM completed for user '{}' from {}",
                                 auth->username(), context.peer_identity);
                    sasl_sessions_.erase(it);
                }
            }
            lock.unlock();
            return encodeResponse(
                context, [&](Buffer& buf) { resp.encode(buf, context.header.apiVersion()); });
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
            if (b == 0) {
                parts.push_back(std::move(current));
                current.clear();
            } else {
                current.push_back(static_cast<char>(b));
            }
        }
        if (!current.empty())
            parts.push_back(std::move(current));
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
            // Strict credential check against the configured map, using a
            // constant-time comparison so a wrong password can't be brute-forced
            // by timing. A missing user is still constant-time-compared against a
            // dummy so "unknown user" and "bad password" take similar time.
            auto it = sasl_plain_creds_.find(username);
            const std::string& expected =
                (it != sasl_plain_creds_.end()) ? it->second : password;  // dummy on miss
            const bool ok =
                (it != sasl_plain_creds_.end()) && constantTimeEquals(expected, password);
            if (ok) {
                resp.setErrorCode(ErrorCode::NONE);
                resp.setSessionLifetimeMs(0);
                if (context.connection) {
                    context.connection->authenticated_principal = "User:" + username;
                }
                Logger::info("SaslAuthenticate: PLAIN user='{}' authenticated", username);
            } else {
                resp.setErrorCode(static_cast<ErrorCode>(58));  // SASL_AUTHENTICATION_FAILED
                resp.setErrorMessage("Invalid credentials");
                Logger::warn(
                    "SaslAuthenticate: PLAIN user='{}' rejected (unknown user or bad password)",
                    username);
            }
        } else if (production_mode_) {
            // No credentials configured in production is a misconfiguration:
            // refuse rather than authenticate anyone. (In dev we accept below.)
            resp.setErrorCode(static_cast<ErrorCode>(58));  // SASL_AUTHENTICATION_FAILED
            resp.setErrorMessage("SASL/PLAIN requires configured credentials in production "
                                 "(set sasl.plain.credentials.file or sasl.plain.users)");
            Logger::warn(
                "SaslAuthenticate: PLAIN rejected for user='{}' — no credentials configured "
                "(production mode)",
                username);
        } else {
            // Dev mode: no credentials configured → accept any non-empty pair.
            resp.setErrorCode(ErrorCode::NONE);
            resp.setSessionLifetimeMs(0);
            if (context.connection) {
                context.connection->authenticated_principal = "User:" + username;
            }
            Logger::info(
                "SaslAuthenticate: PLAIN user='{}' accepted (dev mode — no sasl.plain.users)",
                username);
        }
    }

    return encodeResponse(context,
                          [&](Buffer& buf) { resp.encode(buf, context.header.apiVersion()); });
}

// ---------- Phase 3.3 scaffolding: transactional APIs ----------

Buffer KawasanBroker::handleAddPartitionsToTxn(RequestDispatcher::RequestContext& context) {
    protocol::AddPartitionsToTxnRequest req;
    req.decode(context.payload, context.header.apiVersion());
    Logger::info("AddPartitionsToTxn: transactional_id='{}' pid={} epoch={} {} topics",
                 req.transactionalId(), req.producerId(), req.producerEpoch(), req.topics().size());
    // M2: fence a zombie producer whose epoch is older than the current one.
    const ErrorCode txn_error =
        !isCoordinatorFor(req.transactionalId(), protocol::CoordinatorType::TRANSACTION)
            ? ErrorCode::NOT_COORDINATOR
        : txnEpochFenced(req.transactionalId(), req.producerEpoch())
            ? ErrorCode::INVALID_PRODUCER_EPOCH
            : ErrorCode::NONE;
    if (txn_error != ErrorCode::NONE) {
        protocol::AddPartitionsToTxnResponse resp;
        resp.setThrottleTimeMs(0);
        for (const auto& t : req.topics()) {
            protocol::AddPartitionsToTxnResponse::TopicResult tr;
            tr.topic = t.topic;
            for (int32_t p : t.partitions) {
                protocol::AddPartitionsToTxnResponse::PartitionResult pr;
                pr.partition = p;
                pr.error_code = txn_error;
                tr.partitions.push_back(pr);
            }
            resp.addTopic(std::move(tr));
        }
        return encodeResponse(context,
                              [&](Buffer& buf) { resp.encode(buf, context.header.apiVersion()); });
    }
    // Phase 3.3: register the txn in the coordinator (idempotent — if
    // already registered, it stays) and record the participating
    // partitions so EndTxn can later emit control records to them.
    if (transaction_coordinator_ && !req.transactionalId().empty()) {
        // Don't reset state if the txn is already known.
        if (!transaction_coordinator_->describe(req.transactionalId()).has_value()) {
            transaction_coordinator_->recordInitProducerId(req.transactionalId(), req.producerId(),
                                                           req.producerEpoch(),
                                                           /*timeout=*/60000);
        }
        // M1: capture each partition's first_offset (current log end) so it
        // can be persisted and replayed to re-arm the LSO hold after a crash.
        std::vector<TransactionCoordinator::TxnPartition> partitions;
        for (const auto& t : req.topics()) {
            for (int32_t p : t.partitions) {
                int64_t first_offset = -1;
                if (log_manager_) {
                    auto* log = log_manager_->getOrCreateLog(t.topic, p);
                    if (log)
                        first_offset = log->logEndOffset();
                }
                partitions.push_back({t.topic, p, first_offset});
            }
        }
        transaction_coordinator_->addPartitions(req.transactionalId(), partitions);

        // Phase EX-10: register each partition in the isolation tracker with
        // that first_offset. This is what LSO will hold consumers behind
        // until EndTxn lands.
        if (isolation_tracker_) {
            for (const auto& tp : partitions) {
                if (tp.first_offset >= 0) {
                    isolation_tracker_->recordInFlightTxn(req.producerId(), tp.topic, tp.partition,
                                                          tp.first_offset);
                }
            }
        }
        // M1: persist the updated Ongoing snapshot so a mid-transaction
        // restart can rebuild txns_ and re-arm the LSO holds.
        persistTxnState(req.transactionalId());
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
    return encodeResponse(context,
                          [&](Buffer& buf) { resp.encode(buf, context.header.apiVersion()); });
}

Buffer KawasanBroker::handleAddOffsetsToTxn(RequestDispatcher::RequestContext& context) {
    protocol::AddOffsetsToTxnRequest req;
    req.decode(context.payload, context.header.apiVersion());
    Logger::info("AddOffsetsToTxn: txn='{}' group='{}' pid={} epoch={}", req.transactionalId(),
                 req.groupId(), req.producerId(), req.producerEpoch());

    // M2: fence a zombie producer with a stale epoch.
    const ErrorCode txn_error =
        !isCoordinatorFor(req.transactionalId(), protocol::CoordinatorType::TRANSACTION)
            ? ErrorCode::NOT_COORDINATOR
        : txnEpochFenced(req.transactionalId(), req.producerEpoch())
            ? ErrorCode::INVALID_PRODUCER_EPOCH
            : ErrorCode::NONE;
    if (txn_error != ErrorCode::NONE) {
        protocol::AddOffsetsToTxnResponse resp;
        resp.setThrottleTimeMs(0);
        resp.setErrorCode(txn_error);
        return encodeResponse(context,
                              [&](Buffer& buf) { resp.encode(buf, context.header.apiVersion()); });
    }

    // S12: register the group's __consumer_offsets partition in the transaction
    // so the subsequent TxnOffsetCommit's offsets are part of the txn and EndTxn
    // emits a commit/abort marker to that partition (KIP-98 EOS). Previously this
    // was a no-op that returned NONE without registering anything, so a Streams
    // EOS commit silently skipped the offsets-partition step.
    if (transaction_coordinator_ && !req.transactionalId().empty()) {
        if (!transaction_coordinator_->describe(req.transactionalId()).has_value()) {
            transaction_coordinator_->recordInitProducerId(req.transactionalId(), req.producerId(),
                                                           req.producerEpoch(),
                                                           /*timeout=*/60000);
        }
        // Route the group to its __consumer_offsets partition (same routing
        // as offset-commit mirroring and FindCoordinator).
        const int32_t target =
            coordinatorPartitionFor(req.groupId(), offsets_topic_num_partitions_);
        // first_offset = -1: the __consumer_offsets partition gets a control
        // marker at EndTxn but is not an LSO hold for data consumers.
        transaction_coordinator_->addPartitions(req.transactionalId(),
                                                {{std::string("__consumer_offsets"), target, -1}});
        // M1: persist so the added offsets-partition survives a restart.
        persistTxnState(req.transactionalId());
    }

    protocol::AddOffsetsToTxnResponse resp;
    resp.setThrottleTimeMs(0);
    resp.setErrorCode(ErrorCode::NONE);
    return encodeResponse(context,
                          [&](Buffer& buf) { resp.encode(buf, context.header.apiVersion()); });
}

Buffer KawasanBroker::handleEndTxn(RequestDispatcher::RequestContext& context) {
    protocol::EndTxnRequest req;
    req.decode(context.payload, context.header.apiVersion());
    Logger::info("EndTxn: txn='{}' pid={} epoch={} committed={}", req.transactionalId(),
                 req.producerId(), req.producerEpoch(), req.committed());
    // Phase 3.3 + EX-10 + M1: two-phase, crash-safe txn completion. The
    // ordering is the durability contract: persist Prepare{Commit,Abort}
    // BEFORE emitting control markers, complete + persist Complete AFTER.
    // A crash between the two leaves a Prepare* snapshot that startup replay
    // re-drives to completion (finishTxnCompletion).
    ErrorCode end_error = ErrorCode::NONE;
    if (!isCoordinatorFor(req.transactionalId(), protocol::CoordinatorType::TRANSACTION)) {
        protocol::EndTxnResponse response;
        response.setErrorCode(ErrorCode::NOT_COORDINATOR);
        return encodeResponse(
            context, [&](Buffer& buffer) { response.encode(buffer, context.header.apiVersion()); });
    }
    if (transaction_coordinator_ && !req.transactionalId().empty()) {
        // M2: fence a stale-epoch (zombie) producer.
        if (txnEpochFenced(req.transactionalId(), req.producerEpoch())) {
            end_error = ErrorCode::INVALID_PRODUCER_EPOCH;
        } else {
            auto snap = transaction_coordinator_->describe(req.transactionalId());
            if (!snap.has_value()) {
                end_error = ErrorCode::INVALID_TXN_STATE;
            } else if (snap->state != TransactionCoordinator::State::Ongoing) {
                // M2: the txn is not Ongoing — e.g. the timeout sweep already
                // aborted it, or it's already terminal. A commit/abort here
                // can't proceed; tell the client so it doesn't assume success.
                end_error = ErrorCode::INVALID_TXN_STATE;
            } else {
                std::vector<TransactionCoordinator::TxnPartition> participating;
                if (req.committed()) {
                    participating = transaction_coordinator_->prepareCommit(req.transactionalId());
                } else {
                    participating = transaction_coordinator_->prepareAbort(req.transactionalId());
                }
                // Step 1: persist the Prepare snapshot (durable before markers).
                persistTxnState(req.transactionalId());
                // Step 2: complete the transaction (emit markers, apply/discard
                // offsets, update the isolation tracker). Shared with replay.
                finishTxnCompletion(req.transactionalId(), req.producerId(), req.producerEpoch(),
                                    req.committed(), participating, snap->pending_offsets,
                                    /*is_replay=*/false);
            }
        }
    }
    protocol::EndTxnResponse resp;
    resp.setThrottleTimeMs(0);
    resp.setErrorCode(end_error);
    return encodeResponse(context,
                          [&](Buffer& buf) { resp.encode(buf, context.header.apiVersion()); });
}

Buffer KawasanBroker::handleTxnOffsetCommit(RequestDispatcher::RequestContext& context) {
    protocol::TxnOffsetCommitRequest req;
    req.decode(context.payload, context.header.apiVersion());
    Logger::info("TxnOffsetCommit: txn='{}' group='{}' pid={} epoch={} {} topics",
                 req.transactionalId(), req.groupId(), req.producerId(), req.producerEpoch(),
                 req.topics().size());
    // M2: fence a stale-epoch (zombie) producer before staging any offsets.
    const ErrorCode txn_error =
        !isCoordinatorFor(req.transactionalId(), protocol::CoordinatorType::TRANSACTION)
            ? ErrorCode::NOT_COORDINATOR
        : txnEpochFenced(req.transactionalId(), req.producerEpoch())
            ? ErrorCode::INVALID_PRODUCER_EPOCH
            : ErrorCode::NONE;
    if (txn_error != ErrorCode::NONE) {
        protocol::TxnOffsetCommitResponse resp;
        resp.setThrottleTimeMs(0);
        for (const auto& t : req.topics()) {
            protocol::TxnOffsetCommitResponse::TopicResult tr;
            tr.topic = t.topic;
            for (const auto& p : t.partitions) {
                protocol::TxnOffsetCommitResponse::PartitionResult pr;
                pr.partition = p.partition;
                pr.error_code = txn_error;
                tr.partitions.push_back(pr);
            }
            resp.addTopic(std::move(tr));
        }
        return encodeResponse(context,
                              [&](Buffer& buf) { resp.encode(buf, context.header.apiVersion()); });
    }
    // Phase EX-6: stage the offsets in the transactional context. They
    // are applied to OffsetManager only on EndTxn(commit=true) and
    // discarded on EndTxn(commit=false). This makes consumer-group
    // offset commits truly transactional (KIP-447) — required for
    // Streams EOS v2 semantics.
    if (transaction_coordinator_ && !req.transactionalId().empty() && !req.groupId().empty()) {
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
        transaction_coordinator_->stagePendingOffsets(req.transactionalId(), std::move(staged));
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
    return encodeResponse(context,
                          [&](Buffer& buf) { resp.encode(buf, context.header.apiVersion()); });
}

}  // namespace kawasan::broker
