# Changelog

All notable changes to Kawasan are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

For configuration details see [./CONFIGURATION.md](./CONFIGURATION.md); for upgrade and operational procedures see [./OPERATIONS.md](./OPERATIONS.md); for the full protocol surface see [./api_coverage_matrix.md](./api_coverage_matrix.md).

## [0.2.0-alpha] - 2025-11-24

### Added

#### Protocol & APIs
- 16 core Kafka protocol APIs with multi-version support: ApiVersions (v0-v3), Metadata (v0-v12), Produce (v0-v9, `acks=0/1/-1`), Fetch (v0-v13), CreateTopics (v0-v7), DeleteTopics (v0-v6), ListOffsets (v0-v7), FindCoordinator (v0-v4), JoinGroup (v0-v9), SyncGroup (v0-v5), Heartbeat (v0-v4), LeaveGroup (v0-v5), OffsetCommit (v0-v8), OffsetFetch (v0-v8), DescribeGroups (v0-v5), ListGroups (v0-v4).
- Request validation, structured error responses, and correlation-ID tracking for request/response matching.
- Replica manager for single-node replica tracking.

#### Storage & Durability
- RocksDB-backed log segments with checkpoints; time- and size-based log retention.
- Compression codecs: Snappy, LZ4, Zstd.
- Persistent consumer offsets and consumer-group metadata in RocksDB. Offset commits use synchronous writes (`sync=true`) and are batched across partitions into a single `WriteBatch`; group metadata uses async writes (`sync=false`) protected by the WAL. Message/log writes are synchronous for at-least-once durability.
- Automatic group expiration (default retention 7 days) and member timeout handling (default 30s).

#### Replication & Consensus
- Raft RPC protocol with message serialization/deserialization.
- Raft transport layer (Boost.Asio) with connection pooling and retry, wired into `RaftNode` for elections and log replication.
- Multi-broker configuration parsing (`broker.id`, `raft.peers`), round-robin replica assignment.
- ISR (in-sync replica) tracking, replica-lag tracking (default max lag 10,000 messages), and ISR updates propagated over Raft.
- Follower-fetch background thread; metadata consistency check tool (`tools/kawasan-metadata-check`); cluster bootstrap script (`scripts/start_cluster.sh`).

#### Monitoring & Observability
- `MetricsCollector` with 20+ metrics exported in Prometheus format at `/metrics`.
- HTTP health endpoints: `/health`, `/readiness`, `/liveness`.
- Per-group/topic/partition consumer-lag tracking (computed on a 30s interval to bound mutex contention).
- Grafana dashboard template (`monitoring/grafana-dashboard.json`) and structured logging with correlation IDs.

#### Security
- TLS configuration keys for both the Kafka protocol and Raft inter-broker traffic, plus a certificate-generation helper (`scripts/generate_test_certs.sh`). See Known Limitations below for the current wiring status.

#### Deployment & Operations
- Multi-stage production Docker image (non-root `kawasan` user, built-in health check), Docker Compose for a 3-broker cluster, systemd unit with hardening options, and an installation script (`scripts/install.sh`).
- Operations guide with incident-response runbook ([./OPERATIONS.md](./OPERATIONS.md)).

#### CLI Tools
- `tools/kawasan-topics` (topic management), `tools/kawasan-groups` (group management), and `tools/kawasan-metadata-check`.

#### Performance & Benchmarking
- Throughput, load, and replication benchmarks under `tests/benchmark/`. Approximate single-node baseline: ~30k produce msg/s, ~18k consume msg/s. Targets: 100k+ msg/s, p99 < 5ms, < 1GB idle memory.

### Changed
- `GroupCoordinator` now uses `OffsetManager` for persistent offset storage instead of in-memory maps.
- Consumer-lag computation reworked to minimize mutex contention, removing the latency spikes it previously caused on the consume path.
- TLS configuration uses PEM certificate/key files (`ssl.cert.file`, `ssl.key.file`, `ssl.key.password`) rather than Java KeyStore.
- Logging distinguishes single-node from multi-broker mode.
- Build: `CMakeLists.txt` updated for Boost 1.89+; fixed `Boost::system` linking and LZ4 detection on Homebrew/macOS.

### Fixed
- `DescribeGroups` now serializes correctly across its supported version range (previously v0 only).
- Eliminated consumer latency spikes and `GroupCoordinator` mutex contention introduced by the lag-computation thread.
- `Boost::system` linker errors and LZ4 detection failures on Homebrew installs.

### Breaking Changes
- **Consumer offsets moved from in-memory to RocksDB.** Offsets committed under 0.1.0-alpha are not carried forward (see Migration Guide).
- **TLS configuration uses PEM files** instead of Java KeyStore format.
- **Default data directory is no longer hardcoded.** `log.dirs` must be set explicitly in config (the old implicit `/tmp/kawasan-logs` default is gone).

### Known Limitations
- **Kafka-protocol TLS is not implemented.** When `security.protocol=SSL` / `ssl.enabled=true` is configured, the broker refuses to start (throws at init) rather than serving plaintext under a TLS-claiming listener. Use `security.protocol=PLAINTEXT`.
- **Raft inter-broker traffic is always plaintext.** The `raft.ssl.*` keys are parsed and validated (`src/broker/kawasan_broker.cpp`), but no SSL is wired into the transport — `src/raft/raft_transport.{cpp,h}` contains no TLS code. Setting `raft.ssl.enabled=true` does not encrypt Raft traffic.
- Multi-broker Raft clusters are functional but not production-hardened; the single-node broker is the primary supported mode.
- Transactional APIs are advertised at v0 as scaffolding; full exactly-once delivery (control batches via `WRITE_TXN_MARKERS`, API 27) is incomplete. Delegation tokens, client quotas, and partition-reassignment APIs are not implemented.

### Migration Guide (0.1.0-alpha → 0.2.0-alpha)

1. **Configuration files.** The loader accepts **both** formats and auto-detects by the first non-whitespace byte: `{` or `[` is parsed as JSON (e.g. `config/broker.dev.properties`), anything else as Kafka-style `key=value` properties (e.g. `config/broker-0.properties`). No format conversion is required on upgrade. Both forms support environment-variable substitution: `${VAR}` and `${VAR:default}`.

2. **Consumer offsets.** Offsets now persist to RocksDB. Offsets committed under 0.1.0-alpha (in-memory only) are **not** migrated; affected consumer groups resume from earliest or latest per their `auto.offset.reset`.

3. **Data directory.** Set `log.dirs` explicitly — there is no implicit default path. Update deployment scripts accordingly.

4. **TLS configuration.** Replace KeyStore keys with PEM paths:
   ```properties
   # Old: ssl.keystore.location, ssl.keystore.password
   # New: ssl.cert.file, ssl.key.file, ssl.key.password
   ```
   Note that Kafka-protocol TLS is not yet implemented (see Known Limitations); configuring `security.protocol=SSL` causes the broker to refuse startup.

5. **Monitoring port.** The default `monitoring.port` is **9094** (used by `broker.dev`, `broker.docker`, and `broker-N` configs). The staging and production configs override it to **8080** via `${KAWASAN_MONITORING_PORT:8080}`, and `monitoring/prometheus.yml` scrapes `:8080`. Align your scrape target and firewall rules with the config you deploy.

---

## [0.1.0-alpha] - 2025-11-17

### Added
- Initial single-node broker with RocksDB storage.
- Core Kafka protocol support: ApiVersions, Metadata, Produce, Fetch.
- In-memory topic/partition management and in-memory consumer groups.
- Example producer/consumer programs, a basic Dockerfile, GitHub Actions CI, and a development quick-start script.

### Known Issues
- Consumer offsets in-memory only (not persisted).
- No replication, TLS/SSL, or metrics export.
- Limited protocol API coverage; single-node only.

---

[0.2.0-alpha]: https://github.com/kawasan/kawasan/releases/tag/v0.2.0-alpha
[0.1.0-alpha]: https://github.com/kawasan/kawasan/releases/tag/v0.1.0-alpha
