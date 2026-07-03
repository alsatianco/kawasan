# Kawasan — a single-server Kafka drop-in replacement

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![C++](https://img.shields.io/badge/C%2B%2B-20-blue.svg)](https://en.cppreference.com/w/cpp/20)
[![Kafka wire protocol](https://img.shields.io/badge/Kafka%20wire%20protocol-compatible-brightgreen.svg)](docs/api_coverage_matrix.md)

**Turn off Kafka, spin up Kawasan, and your ecosystem keeps running.**

Kawasan is a Kafka wire-protocol–compatible broker written in modern C++20. Swap an `apache/kafka` container for Kawasan and existing Kafka clients and tooling — kafka-python, the Java AdminClient/Producer/Consumer, kafkajs, sarama, `kafka-*.sh` CLI scripts, kcat, and Kafka UI — keep working unmodified. It uses Raft for metadata consensus (no ZooKeeper) and RocksDB for durable storage.

> **Maturity:** the single-node broker is well-tested and suitable for development, testing, and single-server production. Multi-broker Raft replication is implemented but not yet production-hardened — see [docs/FAQ.md](docs/FAQ.md).

## Quick start

```bash
# 1. Build and launch a local broker (checks toolchain, builds, starts on :9092)
git clone https://github.com/kawasan/kawasan.git
cd kawasan
./scripts/quick-start.sh
```

Or run a containerized broker / cluster with Compose:

```bash
docker compose up -d                                   # single broker
docker compose -f docker-compose-cluster.yml up -d     # 3-broker cluster
```

Then point any Kafka client at `localhost:9092`. Prometheus metrics scrape from `:9094/metrics`; `/health`, `/readiness`, and `/liveness` are HTTP probes.

## Features

| Area | Capability | Status |
|------|-----------|--------|
| **Core broker** | Topics, Produce/Fetch (acks 0/1/-1), Metadata, compression (Snappy/LZ4/Zstd) | Stable |
| **Consumer groups** | Persistent offsets (RocksDB), rebalancing, lag tracking | Stable |
| **Storage** | RocksDB-backed segmented logs, time/size retention | Stable |
| **Transactions** | Idempotent producers, two-phase transaction coordinator, read-committed isolation | Partial — single-node transaction state is **durable across restart** (persisted to `__transaction_state`, replayed on startup); multi-broker replication of it awaits follower fetch |
| **Streams** | DSL (KStream/KTable), windowing, joins | Experimental — the topology/DSL builds, but the task **runtime is incomplete** (does not yet run end-to-end) |
| **Connect** | Source/sink connector + task + worker framework | Experimental — framework only; **no REST API**, standalone, cannot host JVM Connect plugins |
| **Security** | SASL/PLAIN + SASL/SCRAM (SHA-256/512) auth; ACL **enforcement** (opt-in via `authorizer.enabled`); per-client quotas (opt-in) | Implemented (no client/broker TLS — see below) |
| **Consensus** | Raft metadata (single-node operational; multi-broker not hardened) | Partial |
| **Monitoring** | Prometheus metrics, health endpoints, Grafana dashboard | Stable |
| **Deployment** | Docker, Compose, systemd, macOS launchd, Helm, k8s manifests | Stable |

Known limitations: client/broker **TLS is not implemented** (the broker refuses an `SSL` config rather than serve plaintext under a TLS listener — terminate TLS at a proxy), and `raft.ssl.*` is accepted but not enforced, so inter-broker Raft traffic is plaintext. Details in [docs/FAQ.md](docs/FAQ.md); full per-API coverage is in the [API coverage matrix](docs/api_coverage_matrix.md).

## Building (Linux & macOS)

Verified on Ubuntu 22.04/24.04 and macOS 13+/14+ (Intel and Apple Silicon). Requires a modern compiler (GCC ≥ 11 or Clang ≥ 13) and CMake ≥ 3.20.

**Install prerequisites**

```bash
# Linux (Ubuntu/Debian)
sudo apt update && sudo apt install -y build-essential cmake ninja-build git pkg-config \
    libssl-dev librocksdb-dev libspdlog-dev nlohmann-json3-dev \
    libgtest-dev zlib1g-dev libsnappy-dev liblz4-dev libzstd-dev

# macOS (Homebrew)
brew install cmake ninja git llvm boost rocksdb spdlog nlohmann-json googletest openssl@3 lz4 zstd snappy
export CC="$(brew --prefix llvm)/bin/clang" CXX="$(brew --prefix llvm)/bin/clang++"
```

Prefer one dependency story across platforms? Bootstrap [vcpkg](https://github.com/microsoft/vcpkg) and let CMake pull everything from `vcpkg.json`.

**Configure, build, test, install**

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  ${VCPKG_ROOT:+-DCMAKE_TOOLCHAIN_FILE=$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake}
cmake --build build -j"$(nproc 2>/dev/null || sysctl -n hw.logicalcpu)"
ctest --test-dir build --output-on-failure
cmake --install build        # optional; installs to /usr/local
```

Common CMake flags: `-DKAWASAN_BUILD_TESTS`, `-DKAWASAN_BUILD_TOOLS`, `-DKAWASAN_BUILD_EXAMPLES` (default ON); `-DKAWASAN_ENABLE_ASAN`, `-DKAWASAN_ENABLE_TSAN` (default OFF). Full config reference: [docs/CONFIGURATION.md](docs/CONFIGURATION.md).

## Using Kafka clients

Kawasan speaks the Kafka wire protocol, so any client works. Point it at `localhost:9092`.

**Python (kafka-python)**

```python
from kafka import KafkaProducer, KafkaConsumer

producer = KafkaProducer(bootstrap_servers='localhost:9092')
producer.send('my-topic', b'Hello, Kawasan!'); producer.flush()

consumer = KafkaConsumer('my-topic', bootstrap_servers='localhost:9092',
                         group_id='my-group', auto_offset_reset='earliest')
for msg in consumer:
    print(msg.value.decode()); break
```

**Java (kafka-clients)**

```java
Properties p = new Properties();
p.put("bootstrap.servers", "localhost:9092");
p.put("key.serializer",   "org.apache.kafka.common.serialization.StringSerializer");
p.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
try (Producer<String,String> producer = new KafkaProducer<>(p)) {
    producer.send(new ProducerRecord<>("my-topic", "key", "Hello, Kawasan!"));
}
```

**Node.js (kafkajs)**

```javascript
const { Kafka } = require('kafkajs');
const producer = new Kafka({ brokers: ['localhost:9092'] }).producer();
await producer.connect();
await producer.send({ topic: 'my-topic', messages: [{ value: 'Hello, Kawasan!' }] });
```

**Go (sarama)**

```go
p, _ := sarama.NewSyncProducer([]string{"localhost:9092"}, sarama.NewConfig())
p.SendMessage(&sarama.ProducerMessage{Topic: "my-topic", Value: sarama.StringEncoder("Hello, Kawasan!")})
```

**Official Kafka CLI tools** work too:

```bash
kafka-topics.sh --bootstrap-server localhost:9092 --create --topic my-topic --partitions 3 --replication-factor 1
kafka-console-producer.sh --bootstrap-server localhost:9092 --topic my-topic
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
```

…or the bundled CLIs: `kawasan-topics`, `kawasan-groups`, `kawasan-metadata-check`. Connecting Kafka UI and other admin tools (advertised-host setup) is covered in [docs/OPERATIONS.md](docs/OPERATIONS.md).

## Monitoring

Prometheus metrics are exposed at `http://localhost:9094/metrics` (default; staging/prod use `:8080`). Key series include `kawasan_messages_produced_total`, `kawasan_messages_consumed_total`, `kawasan_bytes_in_total` / `kawasan_bytes_out_total`, `kawasan_consumer_lag{group,topic,partition}`, and `kawasan_produce_latency_ms` / `kawasan_fetch_latency_ms`. A ready-made dashboard lives at `monitoring/grafana-dashboard.json`. Setup details: [docs/OPERATIONS.md](docs/OPERATIONS.md).

## Performance

Targets are 100k+ msg/sec with p99 < 5ms and < 1GB idle memory. A single-node baseline on mid-range hardware (8-core, 16GB, SSD) measures roughly 30k produce / 18k consume msg/sec. Tuning and profiling recipes: [docs/OPERATIONS.md](docs/OPERATIONS.md).

## Documentation

| Guide | Purpose |
|-------|---------|
| [Architecture](docs/ARCHITECTURE.md) | System design, modules, data flow, Raft/Streams/Connect, storage |
| [Configuration](docs/CONFIGURATION.md) | Every broker config key |
| [Operations](docs/OPERATIONS.md) | Install, deploy, monitor, back up, troubleshoot, tune |
| [FAQ](docs/FAQ.md) | Common questions |
| [API coverage matrix](docs/api_coverage_matrix.md) | Implemented Kafka APIs and versions |
| [Contributing](docs/CONTRIBUTING.md) | Dev workflow, style, PRs |
| [Changelog](docs/CHANGELOG.md) | Release history and migration |
| [Helm chart](helm/kawasan/README.md) | Kubernetes deployment |

## Contributing

Contributions are welcome — see [docs/CONTRIBUTING.md](docs/CONTRIBUTING.md).

## License

Apache License 2.0 — see [LICENSE](LICENSE).

## Trademarks

Apache Kafka is a registered trademark of the [Apache Software Foundation](https://www.apache.org/). Kawasan is an independent project, not affiliated with, endorsed by, or sponsored by the ASF.
