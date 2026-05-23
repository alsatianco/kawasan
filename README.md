# Kawasan — single-server Kafka drop-in replacement

[![Compat: 19/19](https://img.shields.io/badge/ecosystem%20harness-19%2F19%20PASS-brightgreen.svg)](docs/improv/improve-extra--opus.md)
[![ctest: 26+](https://img.shields.io/badge/ctest-26%2B%20green-brightgreen.svg)](#testing)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![C++](https://img.shields.io/badge/C%2B%2B-20-blue.svg)](https://en.cppreference.com/w/cpp/20)

**Turn off Kafka, spin up Kawasan, your ecosystem keeps running.**

Kawasan is a Kafka wire-protocol compatible broker written in C++20. You can swap an `apache/kafka:4.2.0` container for Kawasan and the Confluent ecosystem (**Schema Registry**, **Kafka Connect**, **ksqlDB**, **kafka-ui**, **kcat**) keeps working unmodified, as does the Java AdminClient + Producer + Consumer surface that every `kafka-*.sh` CLI script exercises.

## Run it now

```bash
docker run -p 9092:9092 -p 9094:9094 kawasan/kawasan:latest
# wait 3s, then point any Kafka client at localhost:9092
```

That's the entire setup. Prometheus metrics scrape from `:9094/metrics`; `/readiness` and `/liveness` are HTTP probes for K8s. See [tests/ecosystem/](tests/ecosystem/) for the 19 harness scripts that prove the drop-in claim — Confluent Schema Registry, Kafka Connect, kafka-ui, and ksqlDB all boot against the same broker.

## What's in the box

Kawasan is a high-performance message streaming platform that provides wire-protocol compatibility with Apache Kafka while being built from the ground up in modern C++. It implements Raft-based consensus for metadata management, eliminating the need for ZooKeeper.

## Features

### Core Broker (✅ Production Ready)
- ✅ **Wire-Protocol Compatible**: Works with existing Kafka clients (kafka-python, Java, Node.js, Go)
- ✅ **Raft Consensus**: Raft-based metadata management (single-node and multi-broker ready)
- ✅ **Topic Management**: Create, delete, list topics with configurable partitions
- ✅ **Producer Support**: acks=0/1/-1 with compression (Snappy, LZ4, Zstd)
- ✅ **Consumer Groups**: Persistent offsets with RocksDB backend, automatic rebalancing
- ✅ **Replication**: ISR tracking, follower fetching infrastructure (multi-broker ready)
- ✅ **Storage**: RocksDB-backed log segments with retention and compaction
- ✅ **Performance**: 30K+ msg/sec producer, 18K+ msg/sec consumer (single-node baseline)

### Operational Features (✅ Production Ready)
- ✅ **Security**: TLS/SSL configuration ready (Kafka and Raft protocols)
- ✅ **Monitoring**: Prometheus metrics endpoint with 20+ metrics
- ✅ **Health Checks**: HTTP endpoints for health, readiness, liveness
- ✅ **Consumer Lag**: Real-time lag tracking per group/topic/partition
- ✅ **Docker**: Production-ready multi-stage Dockerfile with health checks
- ✅ **Deployment**: Systemd service, Docker Compose cluster (3-broker)
- ✅ **Documentation**: Operations guide, runbook, performance tuning

### Client APIs (⏳ Planned)
- ⏳ **Producer API**: Full-featured C++ producer client
- ⏳ **Consumer API**: Full-featured C++ consumer client
- ⏳ **Admin API**: Complete administrative operations

### Advanced Features (⏳ Planned)
- ⏳ **Kawasan Streams**: Stateless and stateful stream processing
- ⏳ **Kawasan Connect**: Source and sink connector framework
- ⏳ **Transactions**: Idempotent producers and transactional writes
- ⏳ **Log Compaction**: Topic compaction for changelog topics
- ⏳ **Quotas**: Rate limiting and throttling

### Feature Matrix

| Feature | Status | Notes |
|---------|--------|-------|
| **Protocol APIs** |
| ApiVersions | ✅ Complete | v0-v3 |
| Metadata | ✅ Complete | v0-v12 |
| Produce | ✅ Complete | v0-v9, all acks modes |
| Fetch | ✅ Complete | v0-v13 |
| CreateTopics | ✅ Complete | v0-v7 |
| DeleteTopics | ✅ Complete | v0-v6 |
| ListOffsets | ✅ Complete | v0-v7 |
| FindCoordinator | ✅ Complete | v0-v4 |
| JoinGroup | ✅ Complete | v0-v9 |
| SyncGroup | ✅ Complete | v0-v5 |
| Heartbeat | ✅ Complete | v0-v4 |
| LeaveGroup | ✅ Complete | v0-v5 |
| OffsetCommit | ✅ Complete | v0-v8 |
| OffsetFetch | ✅ Complete | v0-v8 |
| DescribeGroups | ✅ Complete | v0-v5 |
| ListGroups | ✅ Complete | v0-v4 |
| **Storage & Durability** |
| RocksDB Log Segments | ✅ Complete | Segmented logs with checkpoints |
| Offset Persistence | ✅ Complete | Consumer offsets persist to RocksDB |
| Group State Persistence | ✅ Complete | Group metadata persists across restarts |
| Log Retention | ✅ Complete | Time and size-based retention |
| Compression | ✅ Complete | Snappy, LZ4, Zstd |
| Log Compaction | ⏳ Planned | Changelog topic compaction |
| **Replication & HA** |
| Raft Consensus (Single-node) | ✅ Complete | Metadata management |
| Raft Networking | ✅ Infrastructure | Multi-broker ready |
| ISR Tracking | ✅ Complete | In-sync replica management |
| Follower Fetching | ✅ Infrastructure | Background fetch from leaders |
| Leader Election | ✅ Infrastructure | Raft-based leader election |
| Failover Testing | ✅ Infrastructure | Test harness ready |
| **Monitoring & Operations** |
| Prometheus Metrics | ✅ Complete | 20+ metrics exported |
| Health Endpoints | ✅ Complete | /health, /readiness, /liveness |
| Consumer Lag Metrics | ✅ Complete | Per group/topic/partition |
| Grafana Dashboard | ✅ Complete | Pre-configured template |
| Structured Logging | ✅ Complete | spdlog with correlation IDs |
| **Security** |
| TLS/SSL Configuration | ✅ Complete | Server-side TLS ready |
| TLS for Kafka Protocol | ✅ Infrastructure | SSL context initialization |
| TLS for Raft Protocol | ⏳ Configuration | Implementation pending |
| SASL/SCRAM | ⏳ Planned | Authentication |
| ACLs | ⏳ Planned | Authorization |
| **Deployment** |
| Docker Image | ✅ Complete | Multi-stage, non-root user |
| Docker Compose | ✅ Complete | 3-broker cluster |
| Systemd Service | ✅ Complete | Production service config |
| Installation Script | ✅ Complete | Automated setup |
| Operations Guide | ✅ Complete | Comprehensive documentation |
| Runbook | ✅ Complete | Incident response procedures |

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                         Kawasan Broker                       │
├─────────────────────────────────────────────────────────────┤
│  Request Handler  │  Replica Manager  │  Coordinators      │
├─────────────────────────────────────────────────────────────┤
│  Raft Consensus Protocol  │  Metadata Cache        │
├─────────────────────────────────────────────────────────────┤
│  Storage Layer (RocksDB-backed Log Segments)                │
├─────────────────────────────────────────────────────────────┤
│  Network Layer (Boost.Asio TCP)                             │
└─────────────────────────────────────────────────────────────┘
```

## Requirements

- **Compiler**: GCC 11+, Clang 13+, or MSVC 19.29+
- **CMake**: 3.20 or later
- **Dependencies**:
  - Boost 1.75+ (Asio, Beast, Program Options)
  - RocksDB 7.0+
  - spdlog 1.9+
  - nlohmann/json 3.10+
  - Google Test (for testing)
  - OpenSSL (for security)
  - Compression libraries (zlib, snappy, lz4, zstd)

## Building (Linux & macOS)

The commands below have been verified on Ubuntu 22.04/24.04 and macOS 13+/14+ (both Intel and Apple Silicon). Other distros generally work as long as you have a modern compiler (GCC ≥ 11 or Clang ≥ 13) and CMake ≥ 3.20.

### 1. Install prerequisite packages

**Linux (Ubuntu/Debian)**

```bash
sudo apt update
sudo apt install -y build-essential cmake ninja-build git pkg-config \
    libssl-dev librocksdb-dev libspdlog-dev nlohmann-json3-dev \
    libgtest-dev zlib1g-dev libsnappy-dev liblz4-dev libzstd-dev
```

**macOS (Homebrew)**

```bash
brew update
brew install cmake ninja git llvm boost rocksdb spdlog nlohmann-json \
    googletest openssl@3 lz4 zstd snappy

# Use Homebrew LLVM for the newest Clang toolchain
export CC="$(brew --prefix llvm)/bin/clang"
export CXX="$(brew --prefix llvm)/bin/clang++"
```

> Prefer a single dependency story? Set up [vcpkg](https://github.com/microsoft/vcpkg) once and let CMake pull everything from `vcpkg.json` on both Linux and macOS.

### 2. Clone the repository and (optionally) bootstrap vcpkg

```bash
git clone https://github.com/kawasan/kawasan.git
cd kawasan

# Optional but recommended so dependencies stay consistent across platforms
git clone https://github.com/microsoft/vcpkg.git external/vcpkg
./external/vcpkg/bootstrap-vcpkg.sh
export VCPKG_ROOT="$(pwd)/external/vcpkg"
```

### 3. Configure the build

```bash
cmake -S . -B build \
  -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  ${VCPKG_ROOT:+-DCMAKE_TOOLCHAIN_FILE=$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake}
```

If you installed libraries via your system package manager/Homebrew, simply omit the `CMAKE_TOOLCHAIN_FILE` line.

### 4. Build, test, and (optionally) install

```bash
# Build (nproc on Linux, sysctl on macOS)
cmake --build build -j"$(nproc 2>/dev/null || sysctl -n hw.logicalcpu)"

# Run the test suite
ctest --test-dir build --output-on-failure

# Install to /usr/local (requires sudo on Linux)
cmake --install build
```

### Frequently used CMake options

```bash
cmake -DKAWASAN_BUILD_TESTS=ON \      # Build unit tests (default: ON)
      -DKAWASAN_BUILD_TOOLS=ON \      # Build CLI tools (default: ON)
      -DKAWASAN_BUILD_EXAMPLES=ON \   # Build examples (default: ON)
      -DKAWASAN_ENABLE_ASAN=OFF \     # Enable AddressSanitizer (default: OFF)
      -DKAWASAN_ENABLE_TSAN=OFF \     # Enable ThreadSanitizer (default: OFF)
      ..
```

## Quick Start

### Starting a broker (Linux & macOS)

**Option A – automated quick-start script:**

```bash
# Clone and run the quick-start script
git clone https://github.com/kawasan/kawasan.git
cd kawasan
./scripts/quick-start.sh
```

The script checks your toolchain, builds Kawasan (reusing `build/` if it exists), writes a minimal config under `/tmp`, and starts a single broker listening on `localhost:9092`.

**Option B – manual steps:**

```bash
# 1) Build the project (see Building section above)
cmake -S . -B build -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build build -j$(nproc 2>/dev/null || sysctl -n hw.logicalcpu)

# 2) Start the broker with the default configuration
./build/tools/kawasan-broker --config config/broker.macos.properties

# Or on Linux:
./build/tools/kawasan-broker --config config/broker.dev.properties
```

The broker writes logs to `/tmp/kawasan-broker-logs/` and data to `/tmp/kawasan-logs/` by default. Stop it with `Ctrl+C` or check if it's running with:

```bash
# Linux
netstat -an | grep 9092

# macOS
lsof -i :9092
```

### Verify the broker is working

```bash
# Run the simple test suite
./scripts/simple-test.sh

# Expected output:
# ✅ Simple Producer Test
# ✅ Simple Consumer Test  
# ✅ Producer Performance Test: ~30K msg/s
# ✅ Consumer Performance Test: ~18K msg/s
# ✅ All tests passed
```

### Using the Producer API

**Note**: Full-featured C++ producer/consumer APIs are planned for future releases. Currently, use Python clients for application development.

```python
# Python example with kafka-python
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092')
future = producer.send('my-topic', b'Hello, Kawasan!')
result = future.get(timeout=10)
print(f"Sent to partition {result.partition} at offset {result.offset}")
producer.close()
```

### Using the Consumer API

```python
# Python example with kafka-python
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'my-topic',
    bootstrap_servers='localhost:9092',
    group_id='my-group',
    auto_offset_reset='earliest'
)

for message in consumer:
    print(f"Received: {message.value.decode('utf-8')}")
    if message.offset >= 10:  # Read 10 messages
        break

consumer.close()
```

### Using CLI tools

```bash
# Create a topic
./build/tools/kawasan-topics --bootstrap-server localhost:9092 \
    --create --topic my-topic --partitions 3 --replication-factor 1

# List topics
./build/tools/kawasan-topics --bootstrap-server localhost:9092 --list

# View topic details
cat /tmp/kawasan-logs/meta/topics.json | python3 -m json.tool
```

### Using Python (kafka-python)

```bash
# Install kafka-python
pip3 install kafka-python

# Run a simple producer/consumer test
python3 scripts/tests/test_simple_offset_commit.py
```

### Testing with Docker

```bash
# Build and start a 3-broker cluster
docker-compose -f docker-compose-cluster.yml up -d

# Check broker health
docker ps
docker-compose -f docker-compose-cluster.yml logs broker-0

# Test with CLI tools
./build/tools/kawasan-topics --bootstrap-server localhost:9092 --list

# Cleanup
docker-compose -f docker-compose-cluster.yml down -v
```

## Testing

```bash
# Run all tests
cd build
ctest --output-on-failure

# Run specific test suites
ctest -R KafkaBrokerErrorTest      # Protocol error handling
ctest -R LogSegmentTest             # Storage layer
ctest -R RaftProtocolTest           # Raft protocol
ctest -R ISRManagementTest          # Replication

# Run integration tests
./scripts/simple-test.sh            # Basic functionality
./scripts/run_compatibility_tests.sh  # Client compatibility (requires kafka-python)

# Run performance benchmarks
./tests/benchmark/throughput_benchmark  # Throughput test
./tests/benchmark/load_test            # Load test
```

## Monitoring

Kawasan exposes Prometheus-compatible metrics at `http://localhost:9094/metrics`:

```bash
# View all metrics
curl http://localhost:9094/metrics

# Check broker health
curl http://localhost:9094/health

# Import Grafana dashboard
# 1. Open Grafana UI
# 2. Go to Dashboards -> Import
# 3. Upload monitoring/grafana-dashboard.json
```

### Key Metrics

- `kawasan_messages_produced_total` - Total messages produced
- `kawasan_messages_consumed_total` - Total messages consumed
- `kawasan_bytes_in_total` / `kawasan_bytes_out_total` - Network throughput
- `kawasan_consumer_lag{group,topic,partition}` - Consumer lag
- `kawasan_produce_latency_ms` / `kawasan_fetch_latency_ms` - Latency percentiles
- `kawasan_active_connections` - Current active connections
- `kawasan_topics` / `kawasan_partitions` - Metadata stats

## Performance

Kawasan delivers production-ready performance (baseline on single-node, mid-range hardware):

- **Producer Throughput**: 30K+ messages/second
- **Consumer Throughput**: 18K+ messages/second  
- **Concurrent Producers**: 2K+ msg/sec per producer (5 concurrent)
- **Concurrent Consumers**: 9K+ msg/sec aggregate (3 concurrent)
- **Latency**: 
  - Producer p99: ~15ms
  - Consumer p99: ~5ms
  - End-to-end p95: ~16s (with rebalancing)
- **Memory**: Efficient RocksDB-backed storage with configurable limits
- **Compression**: Snappy, LZ4, Zstd supported

Performance tested on:
- Ubuntu 22.04 / macOS 14+
- 8-core CPU, 16GB RAM
- SSD storage

See [docs/PERFORMANCE.md](docs/PERFORMANCE.md) for detailed benchmarks and tuning guide.

## Project Status

Kawasan 0.2.0-alpha is production-ready for single-node deployments and testing. Multi-broker clusters are infrastructure-ready but require additional testing.

### Completed (✅)
- Core broker with 16 Kafka protocol APIs
- RocksDB-backed storage with retention
- Consumer groups with persistent offsets
- ISR tracking and replication infrastructure
- Raft consensus (single-node operational, multi-broker ready)
- TLS/SSL configuration
- Prometheus metrics and Grafana dashboard
- Docker and systemd deployment
- Comprehensive documentation (operations, runbook, performance)

### In Progress (🔄)
- Multi-broker cluster testing and validation
- TLS handshake integration (infrastructure complete)
- Full replication testing (failover scenarios)

### Planned (⏳)
- C++ producer/consumer client libraries
- Kawasan Streams API
- Kawasan Connect framework
- Transactions and idempotent producers
- Log compaction
- ACLs and authorization

See [docs/PROJECT_STATUS.md](docs/PROJECT_STATUS.md) for detailed status and [plan.md](plan.md) for the development roadmap.

## Contributing

Contributions are welcome! Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details.

## License

Apache License 2.0 - See [LICENSE](LICENSE) for details.

## Acknowledgments

- Inspired by [Apache Kafka](https://kafka.apache.org/)
- Built with modern C++ best practices
- Special thanks to the open-source community

## Documentation

- **Getting Started**
  - [Getting Started Guide](docs/GETTING_STARTED.md) - Build and run your first broker
  - [Configuration Reference](docs/CONFIGURATION.md) - All configuration options
  
- **Operations**
  - [Operations Guide](docs/OPERATIONS.md) - Installation, scaling, monitoring
  - [Runbook](docs/RUNBOOK.md) - Incident response procedures
  - [Production Deployment](docs/PRODUCTION_DEPLOYMENT.md) - Production best practices
  
- **Architecture & Development**
  - [Architecture Overview](docs/ARCHITECTURE.md) - System design and components
  - [Raft Protocol](docs/RAFT_PROTOCOL.md) - Consensus protocol specification
  - [API Coverage Matrix](docs/api_coverage_matrix.md) - Implemented Kafka APIs
  
- **Performance**
  - [Performance Guide](docs/PERFORMANCE.md) - Benchmarks and tuning
  - [Performance Optimization](docs/PERFORMANCE_OPTIMIZATION.md) - Optimization strategies

- **Project Status**
  - [Project Status](docs/PROJECT_STATUS.md) - Current implementation status
  - [Implementation Plan](plan.md) - Development roadmap

## Trademarks

Apache Kafka is a registered trademark of the [Apache Software Foundation](https://www.apache.org/). Kawasan is an independent project and is not affiliated with, endorsed by, or sponsored by the Apache Software Foundation.

## Contact

- GitHub Issues: Report bugs and request features
- Discussions: General questions and community support
