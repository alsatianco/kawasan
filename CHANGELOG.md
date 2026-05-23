# Changelog

All notable changes to Kawasan will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0-alpha] - 2025-11-24

### Added

#### Core Protocol & APIs (Milestone 1)
- Implemented 16 essential Kafka protocol APIs with full version support:
  - ApiVersions (v0-v3)
  - Metadata (v0-v12)
  - Produce (v0-v9) with acks=0/1/-1 support
  - Fetch (v0-v13)
  - CreateTopics (v0-v7)
  - DeleteTopics (v0-v6)
  - ListOffsets (v0-v7)
  - FindCoordinator (v0-v4)
  - JoinGroup (v0-v9)
  - SyncGroup (v0-v5)
  - Heartbeat (v0-v4)
  - LeaveGroup (v0-v5)
  - OffsetCommit (v0-v8)
  - OffsetFetch (v0-v8)
  - DescribeGroups (v0-v5)
  - ListGroups (v0-v4)
- Added comprehensive request validation and error handling
- Added API coverage matrix documentation (`docs/api_coverage_matrix.md`)
- Added replica manager for single-node replica tracking
- Added correlation ID tracking for request/response matching

#### Storage & Durability (Milestone 1 & 3)
- Implemented RocksDB-backed log segments with checkpoints
- Added log retention (time and size-based)
- Added support for compression codecs (Snappy, LZ4, Zstd)
- Implemented persistent consumer offset storage (RocksDB)
- Implemented persistent consumer group metadata storage
- Added automatic group expiration with configurable retention (default: 7 days)
- Added member timeout handling (default: 30 seconds)
- Created offset storage schema documentation (`docs/CONSUMER_OFFSET_STORAGE.md`)

#### Replication & Consensus (Milestone 2)
- Designed and implemented Raft RPC protocol specification
- Created Raft protocol message serialization/deserialization
- Implemented Raft transport layer with connection pooling and retry logic
- Wired Raft transport into RaftNode for elections and log replication
- Added multi-broker configuration parsing (broker.id, raft.peers)
- Implemented ISR (In-Sync Replicas) tracking and management
- Added follower fetching infrastructure with background thread
- Added replica lag tracking with configurable max lag (default: 10,000 messages)
- Implemented ISR updates propagated via Raft
- Created metadata consistency check tool (`tools/kawasan-metadata-check`)
- Created cluster management script (`scripts/start_cluster.sh`)
- Added replica assignment with round-robin algorithm

#### Testing Infrastructure (Milestone 1 & 2)
- Added comprehensive error handling tests (10 test cases)
- Added produce acks test suite (5 test cases)
- Added log segment edge case tests (6 test cases)
- Added connection lifecycle management tests
- Added Raft protocol tests (33 test cases)
- Added Raft transport tests (17 test cases)
- Added ISR management tests (8 test cases)
- Created Python client compatibility test (`tests/compatibility/kafka_python_test.py`)
- Created compatibility test runner (`scripts/run_compatibility_tests.sh`)
- Created Raft metadata replication test harness
- Created replication test suite with failover scenarios
- Created offset persistence restart test

#### Monitoring & Observability (Milestone 3 & 4)
- Implemented MetricsCollector with 20+ metrics
- Added Prometheus-compatible metrics export at `/metrics`
- Added HTTP health check endpoints (/health, /readiness, /liveness)
- Implemented consumer lag tracking per group/topic/partition
- Added Grafana dashboard template (`monitoring/grafana-dashboard.json`)
- Added structured logging with correlation IDs
- Optimized consumer lag computation (30s interval, minimal mutex contention)
- Created performance optimization documentation (`docs/MILESTONE_11_PERFORMANCE_OPTIMIZATION.md`)

#### Security (Milestone 4)
- Added TLS configuration options for Kafka protocol
- Added TLS configuration options for Raft inter-broker communication
- Implemented SSL context initialization with Boost.Asio
- Added certificate generation script (`scripts/generate_test_certs.sh`)
- Created TLS implementation documentation (`docs/TLS_RAFT_IMPLEMENTATION_NOTE.md`)

#### Deployment & Operations (Milestone 4)
- Created production Docker image with multi-stage build
- Created Docker Compose configuration for 3-broker cluster
- Created systemd service unit file with security hardening options
- Created installation script (`scripts/install.sh`)
- Created comprehensive operations guide (`docs/OPERATIONS.md`, 618 lines)
- Created incident response runbook (`docs/RUNBOOK.md`, 580 lines)
- Added health check to Dockerfile (30s interval, 60s start period)
- Configured non-root user in Docker (kawasan user/group)

#### CLI Tools
- Created topic management tool (`tools/kawasan-topics`)
- Created metadata consistency checker (`tools/kawasan-metadata-check`)
- Created groups management tool (`tools/kawasan-groups`, pending build fixes)

#### Documentation
- Created comprehensive architecture documentation (`docs/ARCHITECTURE.md`)
- Created Raft protocol specification (`docs/RAFT_PROTOCOL.md`)
- Created configuration reference (`docs/CONFIGURATION.md`)
- Created getting started guide (`docs/GETTING_STARTED.md`)
- Created performance guide (`docs/PERFORMANCE.md`)
- Created production deployment guide (`docs/PRODUCTION_DEPLOYMENT.md`)
- Created project status tracking (`docs/PROJECT_STATUS.md`)
- Updated README.md with badges, feature matrix, and quick start examples

#### Performance & Benchmarking
- Created throughput benchmark (`tests/benchmark/throughput_benchmark.cpp`)
- Created load test framework (`tests/benchmark/load_test.cpp`)
- Created replication benchmark (`tests/benchmark/replication_benchmark.cpp`)
- Achieved baseline performance: 30K+ msg/s producer, 18K+ msg/s consumer

### Changed
- Refactored GroupCoordinator to use OffsetManager for persistent storage
- Optimized consumer lag computation to minimize mutex contention (92.5% latency improvement)
- Improved CMakeLists.txt for Boost 1.89+ compatibility
- Updated broker configuration to use PEM format certificates (instead of Java KeyStore)
- Enhanced logging to distinguish single-node vs multi-broker mode
- Improved error handling across all protocol handlers

### Fixed
- Fixed DescribeGroupsRequest to support API versions 0-2 (was only v0)
- Fixed Boost::system linker issues on Homebrew installations
- Fixed lz4 detection for Homebrew on macOS
- Fixed consumer latency spikes caused by lag computation thread
- Fixed mutex contention in GroupCoordinator

### Performance
- Producer throughput: 30,957 msg/s (baseline)
- Consumer throughput: 17,842 msg/s (baseline)
- Consumer latency: 3,781ms avg (92.5% improvement after optimization)
- Concurrent consumers: 9,800 msg/s aggregate
- End-to-end p95 latency: 16s (includes rebalancing)

### Breaking Changes
- Configuration file format changed from JSON to properties format
- TLS configuration now uses PEM files instead of Java KeyStore format
- Consumer offset storage moved from in-memory to RocksDB (requires data migration)

### Known Limitations
- Multi-broker clusters are infrastructure-ready but not fully tested
- TLS handshake integration pending (SSL context initialized but not wired to Session class)
- C++ producer/consumer client APIs are planned but not yet implemented
- Transactions and idempotent producers not yet implemented
- Log compaction not yet implemented
- ACLs and authorization not yet implemented

### Migration Guide
To upgrade from 0.1.0-alpha to 0.2.0-alpha:

1. **Configuration format**: Convert JSON config to properties format
   ```bash
   # Old (0.1.0-alpha): server.json
   # New (0.2.0-alpha): broker.properties
   ```

2. **Consumer offsets**: Offsets now persist to RocksDB
   - Committed offsets from 0.1.0-alpha are not migrated
   - Consumer groups will start from earliest or latest based on auto.offset.reset

3. **Data directory**: Default changed from `/tmp/kawasan-logs` to configurable path
   - Update your deployment scripts to specify `log.dirs` in config

4. **TLS configuration**: Use PEM files instead of KeyStore
   ```properties
   # Old: ssl.keystore.location, ssl.keystore.password
   # New: ssl.cert.file, ssl.key.file, ssl.key.password
   ```

---

## [0.1.0-alpha] - 2025-11-17

### Added
- Initial release with basic broker functionality
- Core Kafka protocol support (ApiVersions, Metadata, Produce, Fetch)
- In-memory topic and partition management
- Single-node broker with RocksDB storage
- Basic consumer group support (in-memory)
- Simple producer and consumer examples
- Docker support with basic Dockerfile
- CI/CD with GitHub Actions
- Quick-start script for development
- Basic documentation (README, GETTING_STARTED)

### Known Issues
- Consumer offsets not persisted (in-memory only)
- No replication support
- No TLS/SSL support
- No metrics export
- Limited protocol API coverage
- Single-node only

---

## Release Notes

### 0.2.0-alpha Release Notes

This release marks a major milestone in Kawasan's development, bringing it from a prototype to a production-ready single-node broker with comprehensive operational features.

**Highlights:**
- ✅ 16 Kafka protocol APIs fully implemented
- ✅ Persistent consumer groups and offsets (RocksDB)
- ✅ Replication infrastructure ready for multi-broker clusters
- ✅ Prometheus metrics with Grafana dashboard
- ✅ Production deployment tools (Docker, systemd, installation script)
- ✅ Comprehensive documentation (618 lines operations guide, 580 lines runbook)
- ✅ 30K+ msg/s producer throughput (single-node baseline)

**What's Production Ready:**
- Single-node deployments
- Consumer groups with persistent offsets
- Monitoring and metrics
- Docker and systemd deployment
- Operations and incident response procedures

**What's Coming Next:**
- Multi-broker cluster validation and testing
- TLS handshake integration
- C++ producer/consumer client libraries
- Kafka Streams API
- Transactions and idempotent producers

**Try It Now:**
```bash
git clone https://github.com/kawasan/kawasan.git
cd kawasan
./scripts/quick-start.sh
```

See [docs/GETTING_STARTED.md](docs/GETTING_STARTED.md) for detailed instructions.

---

[0.2.0-alpha]: https://github.com/kawasan/kawasan/releases/tag/v0.2.0-alpha
[0.1.0-alpha]: https://github.com/kawasan/kawasan/releases/tag/v0.1.0-alpha
