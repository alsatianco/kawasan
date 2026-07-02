# Kawasan FAQ

Short answers to common questions about Kawasan. For deeper coverage, follow the links to the topic-owning docs: [Architecture](./ARCHITECTURE.md), [Configuration](./CONFIGURATION.md), [Operations](./OPERATIONS.md), and the [API coverage matrix](./api_coverage_matrix.md).

## Contents

- [What is Kawasan, and is it really Kafka-compatible?](#what-is-kawasan-and-is-it-really-kafka-compatible)
- [Which Kafka clients work?](#which-kafka-clients-work)
- [How many Kafka protocol APIs are implemented?](#how-many-kafka-protocol-apis-are-implemented)
- [What config-file format does it use?](#what-config-file-format-does-it-use)
- [How do I point a Kafka client at it?](#how-do-i-point-a-kafka-client-at-it)
- [Does it support TLS?](#does-it-support-tls)
- [Does it support SASL / authentication?](#does-it-support-sasl--authentication)
- [Is it single-node or multi-broker today?](#is-it-single-node-or-multi-broker-today)
- [Where is data stored?](#where-is-data-stored)
- [How do I back up data?](#how-do-i-back-up-data)
- [What performance can I expect?](#what-performance-can-i-expect)
- [How do I monitor it, and on what port?](#how-do-i-monitor-it-and-on-what-port)
- [Is it production-ready?](#is-it-production-ready)
- [How do I build it or contribute?](#how-do-i-build-it-or-contribute)

## What is Kawasan, and is it really Kafka-compatible?

Kawasan is a single-server message streaming broker written in C++20 that speaks the Apache Kafka wire protocol, so existing Kafka clients and ecosystem tools connect to it unmodified. It uses Raft for metadata consensus and RocksDB for log storage — there is no ZooKeeper. Compatibility is at the wire-protocol level for the subset of APIs it advertises (see below); it is an independent project, not affiliated with the Apache Software Foundation.

## Which Kafka clients work?

Any client that negotiates supported protocol versions via ApiVersions works, including kafka-python, the Java AdminClient/Producer/Consumer (and the `kafka-*.sh` CLI scripts), librdkafka-based clients (kcat, Go, Node.js), and admin UIs such as kafka-ui. Clients that require genuinely absent APIs — delegation tokens, client quotas, partition reassignment, and the KRaft quorum APIs — will fail on those specific operations; see the [API coverage matrix](./api_coverage_matrix.md).

## How many Kafka protocol APIs are implemented?

Kawasan advertises 42 of the 68 Kafka protocol APIs in its ApiVersions response, covering Produce, Fetch, Metadata, the consumer-group lifecycle (JoinGroup/SyncGroup/Heartbeat/LeaveGroup), offset commit/fetch, topic create/delete, SASL handshake/authenticate, ACL management, transactional APIs (v0), and admin operations (DescribeCluster/DescribeConfigs/AlterConfigs/CreatePartitions, etc.). Unadvertised APIs return `UNSUPPORTED_VERSION`. The full per-key list with version ranges is the [API coverage matrix](./api_coverage_matrix.md), which is auto-generated from the broker source.

## What config-file format does it use?

The config loader (`src/common/config.cpp`) accepts **both** a JSON object (e.g. `config/broker.dev.properties`) and Kafka-style `key=value` lines (e.g. `config/broker-0.properties`), auto-detecting which one a file is. Keys are Kafka-style dotted names (`broker.id`, `log.dirs`, `monitoring.port`). Values support environment-variable substitution with `${VAR}` and `${VAR:default}`. See [Configuration](./CONFIGURATION.md) for the full key reference.

## How do I point a Kafka client at it?

Point the client's bootstrap server at the broker's host and port (default `localhost:9092`). For example, kafka-python: `KafkaProducer(bootstrap_servers='localhost:9092')`; kcat: `kcat -b localhost:9092 -L`. The broker must advertise an address the client can actually reach — set `advertised.host` / `advertised.port` accordingly, which matters most for Docker and admin UIs. Operational walkthroughs (including running behind Kafka UI) live in [Operations](./OPERATIONS.md).

## Does it support TLS?

For the **Kafka client protocol**, TLS is **not** implemented in this build: the TCP session uses a plain socket, and the broker deliberately refuses to start if `security.protocol=SSL` or `ssl.enabled=true` is set (`src/broker/kawasan_broker.cpp`) rather than silently serving plaintext on a "TLS" port. Use `security.protocol=PLAINTEXT` (terminate TLS at a proxy/load balancer if you need encryption in transit). Note that `config/broker.production.properties` ships with `security.protocol=SSL` / `ssl.enabled=true` defaults that will fail to start until you override them to PLAINTEXT.

For **inter-broker Raft traffic**, the `raft.ssl.*` keys are parsed and validated (`src/broker/kawasan_broker.cpp`), but they are not wired into the transport — `src/raft/raft_transport.{cpp,h}` contains no SSL code — so Raft traffic is **plaintext even when `raft.ssl.enabled=true`**.

## Does it support SASL / authentication?

Yes, partially. SASL/PLAIN (credentials from `sasl.plain.credentials.file` or inline `sasl.plain.users`) and SASL/SCRAM (SHA-256 and SHA-512) are implemented, and the `SASL_HANDSHAKE` / `SASL_AUTHENTICATE` APIs are advertised. ACLs are implemented, with `DESCRIBE`/`CREATE`/`DELETE_ACLS` advertised. These mechanisms are functional but not yet hardened for hostile multi-tenant use, so validate them against your threat model and keep network-layer access control in place. Because client/broker TLS is not available (see above), SASL credentials travel in plaintext unless you terminate TLS at a proxy.

## Is it single-node or multi-broker today?

The single-node broker is the primary, production-intended mode and is fully functional. Multi-broker Raft replication exists as infrastructure — leader election and metadata consensus work end-to-end, ISR is tracked, and a 3-broker Docker Compose ships — but it is **not** production-hardened. In particular the follower record-fetcher is still a stub (`ReplicaManager::fetchFromLeader` is a no-op with a TODO), so followers do not yet copy partition data from the leader; treat clustering as experimental and validate failover yourself before relying on it.

## Where is data stored?

All log data lives under the directory given by `log.dirs`, which you set in config (the dev/docker profiles use `/tmp/kawasan-logs`; production uses `/var/lib/kawasan/data`). Within it, partition logs are RocksDB-backed segments, consumer offsets and group state persist to RocksDB, and topic metadata is kept under a `meta/` subdirectory. See [Architecture](./ARCHITECTURE.md) for the storage layout.

## How do I back up data?

Stop the broker (or snapshot the filesystem) and copy the entire `log.dirs` tree, which is the single source of truth for messages, offsets, group state, and metadata. Restore by placing the tree back under the same `log.dirs` path before starting the broker. Procedures and operational caveats are in [Operations](./OPERATIONS.md).

## What performance can I expect?

As an approximate single-node baseline on mid-range hardware, expect roughly 30k produce and 18k consume messages/second; design targets are 100k+ msg/sec, p99 latency under 5 ms, and under 1 GB idle memory. Actual numbers depend heavily on message size, `acks`, compression, and disk; see [Operations](./OPERATIONS.md) for tuning guidance.

## How do I monitor it, and on what port?

The broker exposes Prometheus metrics at `/metrics` plus `/health`, `/readiness`, and `/liveness` probes over HTTP. The `monitoring.port` default is **9094** (used by the dev, docker, and `broker-N` profiles); the staging and production profiles override it to **8080** via `${KAWASAN_MONITORING_PORT:8080}`, and `monitoring/prometheus.yml` scrapes `:8080`. Durability commits and message/log writes use synchronous RocksDB writes (`sync=true`) for at-least-once durability, while group metadata uses async writes protected by the WAL.

## Is it production-ready?

For **single-node** deployments it is the intended, supported mode: durable storage, persistent consumer offsets, monitoring, health probes, and Docker/systemd packaging are all in place. The main gaps are multi-broker clustering (Raft replication is unhardened) and client/broker TLS (not implemented — terminate at a proxy). SASL/SCRAM and ACLs are implemented but should be validated for your threat model. Choose Kawasan where a single durable Kafka-compatible node, with TLS terminated upstream if needed, is acceptable.

## How do I build it or contribute?

Build with CMake: `cmake -S . -B build -DCMAKE_BUILD_TYPE=RelWithDebInfo`, then `cmake --build build`, and run the suite with `ctest --test-dir build --output-on-failure`. Full prerequisites and platform notes are in [../README.md](../README.md) and [../CLAUDE.md](../CLAUDE.md); contribution workflow and conventions are in [Contributing](./CONTRIBUTING.md).
