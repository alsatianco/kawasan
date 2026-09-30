# Kawasan Architecture

Kawasan is a C++20 message streaming platform that is wire-protocol compatible with Apache Kafka. A single broker process embeds the Kafka protocol layer, RocksDB-backed log storage, a Raft consensus implementation for metadata, consumer-group coordination, and a Prometheus monitoring endpoint. The same source tree also ships a producer client, a Kawasan Streams DSL, and a Connect framework as separate library modules.

The single-node broker is the primary, fully supported mode. Multi-broker replication works end-to-end for the data plane — followers fetch from leaders, `acks=all` is governed by the ISR, the ISR shrinks/expands under controller authority, a divergent follower truncates and re-syncs, and preferred-replica election is available via `ElectLeaders` — but **automatic partition-leader failover is not implemented**: a leader loss strands the partition until leadership is manually re-elected. Treat multi-broker as experimental.

This document explains how the system is structured and how the pieces fit together. For installing, configuring, and operating a broker see [./OPERATIONS.md](./OPERATIONS.md) and [./CONFIGURATION.md](./CONFIGURATION.md); for the full set of supported protocol APIs see [./api_coverage_matrix.md](./api_coverage_matrix.md).

## Contents

- [System overview](#system-overview)
- [Module dependency graph](#module-dependency-graph)
- [Module responsibilities](#module-responsibilities)
- [Request data flows](#request-data-flows)
- [Threading model](#threading-model)
- [On-disk storage layout](#on-disk-storage-layout)
- [Durability trade-offs](#durability-trade-offs)
- [Consumer offset storage](#consumer-offset-storage)
- [Raft consensus](#raft-consensus)
- [Kawasan Streams](#kawasan-streams)
- [Kawasan Connect](#kawasan-connect)
- [Monitoring](#monitoring)

## System overview

Clients speak the Kafka wire protocol over TCP. A broker accepts connections on its Boost.Asio TCP server, decodes each request frame, routes it through the `RequestDispatcher` to the matching handler method on `KawasanBroker`, and writes the encoded response back on the same connection.

```
┌──────────────────────────────────────────────────────────────┐
│  Clients (producers, consumers, admin, Streams, Connect)     │
└──────────────────────────────────────────────────────────────┘
                     │  Kafka wire protocol (TCP)
                     ▼
┌──────────────────────────────────────────────────────────────┐
│  KawasanBroker                                                │
│   TcpServer (Boost.Asio)  →  RequestDispatcher  →  handlers   │
│                                                              │
│   ReplicaManager   GroupCoordinator   TransactionCoordinator │
│   OffsetManager    MetadataController  ProducerStateManager   │
│   FetchSessionManager  IsolationTracker  AclStore            │
│                              │                               │
│   LogManager (storage)      RaftNode (raft)                  │
│   MonitoringManager (HTTP metrics/health)                    │
└──────────────────────────────────────────────────────────────┘
                     │
                     ▼
        RocksDB log segments on local disk (log.dirs)
```

The central object is `KawasanBroker` (`include/kawasan/broker/kawasan_broker.h`, `src/broker/kawasan_broker.cpp`). It owns every subsystem and exposes one `handleXxx(RequestDispatcher::RequestContext&)` method per protocol API (`handleProduce`, `handleFetch`, `handleMetadata`, `handleJoinGroup`, and so on). The dispatcher maps each Kafka API key + version to the right handler.

## Module dependency graph

The project is nine static-library modules combined into a single `kawasan` archive, then linked into the CLI tools. Dependencies are strictly layered — `common` is the base and depends on nothing internal.

```
tools/  (kawasan-broker, kawasan-topics, kawasan-groups, kawasan-metadata-check)
  └─ kawasan (combined static library)
       ├─ broker    → protocol, storage, raft, common, monitoring
       ├─ protocol  → common
       ├─ storage   → common
       ├─ raft      → common
       ├─ client    → protocol, common
       ├─ streams   → common
       ├─ connect   → common
       ├─ admin     → common   (placeholder)
       └─ common    → (base types, Config, error codes, logging)
```

Public headers live under `include/kawasan/<module>/`; implementation under `src/<module>/`. Each module builds as an OBJECT library and is aggregated into the final `kawasan` static archive. Monitoring sources live under `src/broker/monitoring/` and headers under `include/kawasan/broker/monitoring/`; they are part of the broker module rather than a standalone library.

| Module | Headers | Sources | Depends on |
|--------|---------|---------|------------|
| `common` | `include/kawasan/common/` | `src/common/` | — |
| `protocol` | `include/kawasan/protocol/` | `src/protocol/` | common |
| `storage` | `include/kawasan/storage/` | `src/storage/` | common |
| `raft` | `include/kawasan/raft/` | `src/raft/` | common |
| `broker` | `include/kawasan/broker/` | `src/broker/` | protocol, storage, raft, common, monitoring |
| `client` | `include/kawasan/client/` | `src/client/` | protocol, common |
| `streams` | `include/kawasan/streams/` | `src/streams/` | common |
| `connect` | `include/kawasan/connect/` | `src/connect/` | common |
| `admin` | — | `src/admin/` | common (placeholder) |

## Module responsibilities

### common

Foundation types shared by every module: `Buffer` (byte buffer / cursor used by protocol codecs), `Config` (configuration loader, see [On-disk storage layout](#on-disk-storage-layout) and [./CONFIGURATION.md](./CONFIGURATION.md)), `ErrorCode` and `error.h`, the `Logger` wrapper over spdlog, and the core scalar types in `types.h` (`Offset`, `PartitionId`, `BrokerId`, `TopicPartition`, etc.).

### protocol

One request/response pair per Kafka API key. Each `*_request.{h,cpp}` file owns the serialization and deserialization for that API (for example `produce_request`, `fetch_request`, `metadata_request`, `join_group_request`, `offset_commit_request`). `request_header.h` defines the framed request/response headers and correlation IDs; `api_keys.h` enumerates the supported API keys; `api_versions.h` drives version negotiation. The `protocol/generated` directory holds machine-generated schema helpers. The protocol module does pure encode/decode — it holds no broker state.

### storage

Log-structured, append-only storage backed by RocksDB.

| Class | Responsibility |
|-------|----------------|
| `LogManager` (`log_manager.h`) | Owns every per-topic-partition `Log`. Resolves/creates log directories, holds per-topic `LogConfig` overrides, runs the background cleanup thread, and exposes `flushAll` / `cleanupAll`. |
| `Log` (`log.h`) | One topic-partition. Holds an ordered list of `LogSegment`s, tracks the high watermark (checkpointed atomically to `checkpoint.meta`, periodically rather than per append; single-node recovers HW = log end on open), appends record batches (`appendBatch`, plus the offset-preserving `appendReplicatedBatch` used by follower replication), truncates its tail (`truncateSuffix`), serves reads, rolls new segments, and applies retention/compaction. |
| `LogSegment` (`log_segment.h`) | A single segment, named by its base offset and backed by its own RocksDB instance (`std::unique_ptr<rocksdb::DB>`). Stores encoded record batches, serves both decoded `RecordBatch` reads and raw byte reads, and supports tail truncation (`truncateTo`). |
| `RecordBatch` (`record_batch.h`) | The Kafka record-batch unit of storage and transfer. |
| `lz4_decoder.h` | Decodes LZ4-compressed batches; other codecs (gzip, snappy, zstd) are handled via the linked compression libraries. |

`Log::readRaw` / `LogSegment::readRaw` return stored batch bytes without re-deserializing into `RecordBatch` objects, so the fetch path can stream stored bytes straight to the client; `Log::read` is the decoding variant used off the hot path.

### raft

Raft consensus used for metadata replication. See [Raft consensus](#raft-consensus) for the protocol detail.

| Class | Responsibility |
|-------|----------------|
| `RaftNode` (`raft_node.h`) | The state machine: `NodeState` (FOLLOWER / CANDIDATE / LEADER), log, commit index, and `applyCommittedEntries`. Runs three dedicated threads — a CV-driven election-timeout thread, a leader heartbeat/snapshot thread, and an apply thread that feeds committed entries to the state machine. Holds `PeerInfo` for each peer and a `SnapshotAccumulator` for incoming snapshot chunks. |
| `RaftProtocol` (`raft_protocol.h`) | Wire encode/decode of the six RPC message types plus `ByteBuffer` and `ProtocolException`. |
| `RaftTransport` (`raft_transport.h`) | Boost.Asio TCP transport. Listens for peer connections, dials peers, and frames each message with a 4-byte big-endian length prefix. |

### broker

The orchestration layer. `KawasanBroker` constructs and wires every subsystem below, plus the `TcpServer` (`network/`), `RequestDispatcher`, and `MonitoringManager`.

| Class | Responsibility |
|-------|----------------|
| `KawasanBroker` | Top-level broker. Owns all subsystems; one handler method per protocol API. |
| `RequestDispatcher` | Maps `(ApiKey, version)` to a registered `HandlerFunc`, runs it, and frames the response. Builds protocol-correct error payloads for unsupported versions or handler errors. |
| `TcpServer` (`network/`) | Boost.Asio TCP server: accepts connections across one or more acceptors, reads length-prefixed request frames, calls the dispatcher, and writes responses. Optionally TLS-wrapped for the Kafka protocol. |
| `ReplicaManager` | Leader/follower bookkeeping and replication. Leader side: records each follower's fetch offset, computes the ISR-derived high watermark, tracks leader epochs, and proposes ISR shrink/expand from fetch recency (`computeIsrUpdate`). Follower side (multi-broker): the fetcher thread pulls batches from partition leaders via per-leader `PeerClient`s. In single-node mode this broker is always the leader for every partition. |
| `PeerClient` | Minimal broker-to-broker Kafka client: replica Fetch (v4, `replica_id` = own broker id) and AlterPartition RPCs to peers. |
| `MetadataController` | Applies metadata mutations through the Raft log (five commands: `CREATE_TOPIC`, `DELETE_TOPIC`, `UPDATE_ISR`, `INCREASE_PARTITIONS`, `UPDATE_LEADER`) and serves cached metadata back to handlers. |
| `MetadataStore` / `metadata_types.h` | Persistent and in-memory representation of cluster metadata (topics, partitions, brokers). |
| `GroupCoordinator` | Consumer-group lifecycle: JoinGroup/SyncGroup/Heartbeat/LeaveGroup, rebalance generations, and member tracking. Persists group state and offsets through `OffsetManager`. |
| `OffsetManager` | RocksDB-backed consumer offset and group-metadata store. See [Consumer offset storage](#consumer-offset-storage). |
| `TransactionCoordinator` | Transactional-producer and exactly-once support (InitProducerId with epoch fencing, AddPartitionsToTxn, two-phase EndTxn, TxnOffsetCommit). |
| `TransactionStateManager` | Persists transaction-coordinator snapshots to the `__transaction_state` internal topic (fsynced regardless of `log.durability`) and replays them on startup. |
| `QuotaManager` | Per-client produce/consume byte-rate quotas (`quota.producer.default` / `quota.consumer.default`), returning `throttle_time_ms`. |
| `ProducerStateManager` | Tracks `(topic, partition, producer_id)` state for idempotent/duplicate detection; exposes entry/eviction metrics. |
| `FetchSessionManager` | Incremental-fetch session state (KIP-227 style fetch sessions). |
| `IsolationTracker` | Tracks the last stable offset for `read_committed` consumers. |
| `AclStore` / `ScramAuthenticator` | ACL storage and SASL/SCRAM credential handling for authn/authz. |
| `MonitoringManager` (`monitoring/`) | HTTP server exposing Prometheus metrics and health endpoints. See [Monitoring](#monitoring). |

### client

`Producer` (`include/kawasan/client/producer.h`, `src/client/producer.cpp`) — an in-process producer used by tests, examples, and the Streams/Connect runtimes to publish to a broker over the Kafka protocol.

### streams

The Kawasan Streams DSL (KStream/KTable, `Topology`, state stores, windowing). See [Kawasan Streams](#kawasan-streams).

### connect

The Connect framework (connectors, tasks, workers, offset storage). See [Kawasan Connect](#kawasan-connect).

## Request data flows

Every client request follows the same envelope: `TcpServer` reads a length-prefixed frame, builds a `RequestDispatcher::RequestContext`, and `RequestDispatcher::dispatch` selects and invokes the handler. The handler decodes the typed request via the `protocol` module, performs its work, and returns an encoded `Buffer`; the dispatcher frames it and the server writes it back.

### Produce

1. `handleProduce` decodes the `ProduceRequest`.
2. For each topic-partition it resolves the leader via `ReplicaManager` (always local in single-node mode) and obtains the `Log` from `LogManager` (`getOrCreateLog`).
3. With idempotent/transactional producers, `ProducerStateManager` validates the producer id and sequence numbers.
4. The record batch is appended with `Log::appendBatch`, which writes to the active `LogSegment` (RocksDB) and rolls a new segment if size/time thresholds are exceeded. Write durability follows `log.durability` (default `sync` → fsync per acked produce) — see [Durability trade-offs](#durability-trade-offs).
5. The high watermark is advanced (single-node) or advanced after ISR acknowledgement (multi-broker), honoring the request `acks`.
6. `handleProduce` encodes a `ProduceResponse` with the base offset and per-partition error codes.

### Fetch

1. `handleFetch` decodes the `FetchRequest`; if it carries a session id, `FetchSessionManager` resolves the incremental session.
2. For each requested partition it reads from the local `Log` starting at the requested offset. Consumer fetches (`replica_id == -1`) are bounded by the high watermark (and the last stable offset via `IsolationTracker` for `read_committed`); a replica fetch from a follower broker (`replica_id >= 0`) reads up to the log-end offset and updates the leader's follower-progress tracking.
3. Reads use `Log::readRaw` / `LogSegment::readRaw`, returning stored batch bytes without re-encoding, so compressed batches are served as stored.
4. `handleFetch` assembles a `FetchResponse` with the record bytes and per-partition high watermark / log-start metadata.

### Metadata

1. `handleMetadata` decodes the `MetadataRequest`.
2. It reads the cached cluster view from `MetadataController` / `MetadataStore` — broker list, topics, partitions, leaders, and ISR.
3. Topic-altering admin APIs (`handleCreateTopics`, `handleDeleteTopics`, `handleCreatePartitions`) route the mutation through `MetadataController`, which proposes it to the Raft log; the change becomes visible once committed and applied. ISR changes arrive two ways: the partition leader's own ISR maintenance (`maintainLeaderIsr`, driven by `replica.lag.time.max.ms`) commits directly when the leader is the controller, or sends an **AlterPartition** RPC to the controller (`handleAlterPartition` validates leader + leader-epoch, then commits `UPDATE_ISR`). **ElectLeaders** (`handleElectLeaders`) performs preferred-replica election by committing `UPDATE_LEADER`, which bumps the partition's `leader_epoch`. In single-node mode the broker is the sole voter, so commits are immediate.
4. `handleMetadata` encodes a `MetadataResponse` describing brokers and topic/partition leadership.

## Threading model

The broker is event-driven on top of one Boost.Asio `io_context`. The number of IO threads comes from `network.io_threads` (defaulting to `std::thread::hardware_concurrency()`).

| Thread(s) | Owner | Role |
|-----------|-------|------|
| Network IO threads | `TcpServer` / `io_context` | Accept connections, read request frames, run the dispatcher handler **inline**, and write responses. There is no separate request-handler thread pool — a handler runs to completion on the IO thread that read its frame. |
| Fetch purgatory workers | `DelayedOperationPurgatory` | `fetch.purgatory.threads` (default 2) workers that hold long-poll Fetches (see below) and complete them when a watched partition changes or `max_wait_ms` expires. |
| Produce purgatory worker | `DelayedOperationPurgatory` | One worker that completes `acks=all` Produces parked while the ISR catches up (see below). |
| IO context thread | `KawasanBroker` | Drives `io_context_.run()` for the Raft transport; kept alive by a work guard so it does not exit when idle. |
| HW checkpoint thread | `LogManager` | Writes dirty high-watermark checkpoints every `replica.high.watermark.checkpoint.interval.ms` (default 5 s); final flush on `stop()`. |
| Log cleanup thread | `LogManager` | Periodic retention/compaction sweep across all logs (interval from `cleanup_interval_ms_`, default 5 minutes). |
| Group cleanup thread | `GroupCoordinator` | Expires empty groups and times out stale members (`startCleanupThread`). |
| Replica fetcher thread | `ReplicaManager` | Multi-broker only (started only when `raft.peers` is non-empty — never created single-node): each cycle reconciles replica assignments from Raft metadata, fetches from partition leaders for followed partitions, and runs the leader-side ISR maintenance pass. |
| Raft election / heartbeat / apply threads | `RaftNode` | Three dedicated threads: CV-driven election timeout, periodic leader heartbeat + snapshot check, and application of committed entries to the state machine. |
| Controller bootstrap thread | `KawasanBroker` | One-shot startup thread that waits for Raft leadership resolution and bootstraps internal topics. |
| Transaction sweep thread | `KawasanBroker` | Auto-aborts transactions past their timeout (`transaction.abort.timed.out.transaction.cleanup.interval.ms`). |
| Producer-state snapshot thread | `KawasanBroker` | Periodically checkpoints idempotent-producer state (`producer.state.snapshot.interval.ms`). |
| Consumer-lag thread | `KawasanBroker` | Recomputes per-group consumer-lag metrics on an interval. |
| Monitoring HTTP thread | `MonitoringManager` | Serves `/metrics` and health endpoints (Boost.Beast). |

Because handlers run inline on IO threads, shared subsystems (`LogManager`, `GroupCoordinator`, `OffsetManager`, `ReplicaManager`) provide their own internal synchronization — for example `LogManager` guards its log map with a `std::shared_mutex` and each `Log` guards its segments with a mutex.

### Delayed (parked) Fetch and Produce responses

A Fetch that can't be answered yet (below `min_bytes`, no error, `max_wait_ms > 0`) is not polled on the IO thread. `handleFetch` parks it in `DelayedOperationPurgatory`, keyed by its topic-partitions, and returns `HandlerResult{deferred=true}`. The session doesn't re-arm its read, so each connection still has one request in flight and responses stay in order. `LogManager` installs a change listener on every `Log`. Appends, replicated appends, truncation, and high-watermark changes call `purgatory.notify(topic, partition)` (a single atomic load when nothing is parked). A worker then re-runs the fetch. When the fetch is satisfied or its deadline passes, the response goes through `RequestContext::complete` → `RequestDispatcher::finalize` → `TcpSession::deliver`, which is posted back to the socket's executor. Retries for one operation are coalesced, and it never runs concurrently with itself. On shutdown the purgatory stops before the TCP server and drops parked ops. Handlers called without a transport sink (unit tests) fall back to the old in-handler wait loop. `throttle_time_ms` in the response carries only quota throttling, never the long-poll wait.

An `acks=all` Produce to a replicated partition (ISR > 1) uses the same mechanism through a separate single-worker produce purgatory. `handleProduce` appends every partition first. It then records each batch whose end offset the ISR hasn't committed yet and parks one operation keyed by those partitions. The operation completes when every pending batch is resolved. A batch is committed when `ReplicaManager::isrCommittedOffset` covers it, fails with `NOT_LEADER_FOR_PARTITION` if the broker lost leadership, or fails with `REQUEST_TIMED_OUT` at the producer's timeout (capped at 30 s). Wake-ups come from the same `Log` listener (high-watermark changes after follower fetches) and from `KawasanBroker::reconcileReplicas` when a partition's leader, ISR or epoch changes. An ISR change on a partition this broker leads also re-computes the high watermark, so a shrink commits what the dropped follower was holding back. Single-node partitions never park because the append itself advances the high watermark.

## On-disk storage layout

Partition logs live under the directories named by `log.dirs`. Each topic-partition gets its own directory, and within it each segment is a subdirectory named by its base offset; each segment directory is an independent RocksDB instance.

```
<log.dirs>/
  ├── orders-0/                 # topic "orders", partition 0
  │   ├── checkpoint.meta       # start/end offsets + high watermark (atomic temp+fsync+rename)
  │   ├── 0/                    # segment, base offset 0 (RocksDB instance)
  │   ├── 1048576/              # segment, base offset 1048576
  │   └── ...
  ├── orders-1/
  └── ...
```

The directory for a partition is `<log.dirs>/<topic>-<partition>`; segment subdirectories are the base offset rendered as a plain decimal string. On startup `Log::loadSegments` scans the partition directory, parses each subdirectory name as a base offset, and reopens the segments in order. A new segment is rolled when the active segment exceeds `log.segment.bytes` (`LogConfig::segment_size`, default 1 GB) or, when enabled, the time bound `segment.ms` (`LogConfig::segment_ms`, default disabled). Retention (`retention.bytes` / time-based) and `cleanup.policy` (delete vs compact) are applied by the cleanup thread.

Consumer offsets and group metadata are stored in a separate RocksDB database (not under the partition log dirs) — see [Consumer offset storage](#consumer-offset-storage). Streams state stores keep their own RocksDB directories — see [Kawasan Streams](#kawasan-streams).

### Configuration formats

The configuration loader (`src/common/config.cpp`) accepts **two** file formats and auto-detects which one a file uses by its first non-whitespace byte:

- A leading `{` or `[` is parsed as a **JSON object** (e.g. `config/broker.dev.properties`, `config/broker.docker.properties`).
- Anything else is parsed as **Kafka-style `key=value` properties**, one per line, with `#` comments (e.g. `config/broker-0.properties`).

Both formats produce the same typed config map, so `broker.id`, `log.dirs`, and the rest behave identically regardless of format. String values in either format support environment-variable substitution with `${VAR}` and `${VAR:default}` syntax. The full key reference is in [./CONFIGURATION.md](./CONFIGURATION.md).

## Durability trade-offs

Kawasan tunes RocksDB write durability per data class to balance at-least-once safety against throughput:

| Data | Write mode | Rationale |
|------|-----------|-----------|
| Message / log writes | `log.durability` (default `sync`) | With `sync`, each acked produce is fsynced so partition data survives a power loss; `async` is WAL-buffered (lower latency, crash-loss window on the un-flushed tail). Durability-critical internal topics (`__transaction_state`) force fsync regardless. |
| Consumer offset commits | `sync=true`, WAL on | Offsets back at-least-once consumption; a lost commit could silently skip records. Commits across partitions are coalesced into a single RocksDB `WriteBatch` so one fsync covers the whole commit. |
| Consumer-group metadata | `sync=false`, WAL on | Member lists, assignments, and generation IDs are reconstructable by rejoin/rebalance, so async writes (protected by the WAL) are an acceptable trade for throughput. |

In short: offsets are always synchronous and messages are synchronous by default; group metadata is asynchronous but WAL-protected, with a small (~WAL-flush-interval) loss window on crash. After a crash the RocksDB WAL replays all writes, including the async group-metadata ones.

## Consumer offset storage

`OffsetManager` (`include/kawasan/broker/offset_manager.h`, `src/broker/offset_manager.cpp`) persists committed offsets and consumer-group state to RocksDB, so groups survive broker restarts. It uses a dedicated RocksDB database opened with `OptimizeForPointLookup` (64 MB block cache); `GroupCoordinator` calls into it on commit/fetch and on group state changes.

### Key layout

Offsets and group metadata are stored under distinct key prefixes:

| Entity | Key format | Example |
|--------|-----------|---------|
| Committed offset | `offset:<group>:<topic>:<partition>` | `offset:my-group:orders:2` |
| Group metadata | `group:<group>` | `group:my-group` |

The `offset:` prefix supports efficient range scans (e.g. all offsets for a group), and the separate `group:` prefix lets group state be updated atomically and independently of offsets. Keys assume group/topic names contain no `:` separator.

### Value layout

Values are JSON. An offset value carries:

```json
{
  "offset": 12345,
  "metadata": "optional consumer metadata",
  "commit_timestamp": 1700419200000,
  "expiry_timestamp": 1701024000000
}
```

A group value carries the generation, protocol type/name, leader id, state (`Empty` / `Stable` / `PreparingRebalance` / `Dead`), member list (member id, client id/host, encoded metadata and assignment), and last-modified timestamp — see the `OffsetManager::GroupMetadata` / `MemberMetadata` structs.

### Retention and expiry

Each commit stamps `commit_timestamp = now` and computes:

```
expiry_timestamp = commit_timestamp + retention_ms
```

`retention_ms` defaults to 7 days (`offsets.retention.minutes` × 60 × 1000; the in-code default is `7 * 24 * 60 * 60 * 1000`). `deleteExpiredOffsets(now)` removes every offset whose `expiry_timestamp` is in the past. Offset commits use `sync=true` and a single `WriteBatch` for multi-partition commits; group-metadata writes use `sync=false` with WAL on (see [Durability trade-offs](#durability-trade-offs)).

The relevant `OffsetManager` API includes `commitOffset` / `commitOffsetBatch`, `fetchOffset` / `fetchOffsetWithMetadata`, `saveGroupMetadata` / `loadGroupMetadata`, `deleteGroup`, `listGroups`, and `deleteExpiredOffsets`.

## Raft consensus

Raft replicates cluster metadata across brokers without ZooKeeper. `MetadataController` proposes metadata commands to the Raft log; once an entry commits, every node applies it through `RaftNode::applyCommittedEntries`. In single-node mode the broker is the only voter, so commits are immediate.

The metadata command set (`MetadataCommandType`, `include/kawasan/broker/metadata_types.h`):

| Command | Effect |
|---------|--------|
| `CREATE_TOPIC` / `DELETE_TOPIC` | Topic lifecycle. |
| `UPDATE_ISR` | Commits an ISR shrink/expand proposed by the partition leader (directly or via the AlterPartition RPC). |
| `INCREASE_PARTITIONS` | Applies a `CreatePartitions` count increase. |
| `UPDATE_LEADER` | Sets the partition leader (assigned replicas only) and bumps `leader_epoch` — the primitive behind `ElectLeaders`. |

A `RaftNode` is always in one of three states — `FOLLOWER`, `CANDIDATE`, or `LEADER`:

```
            timeout / start election
 Follower ─────────────────────────► Candidate
    ▲                                    │ majority votes
    │ discover leader or higher term     ▼
    └──────────────────────────────── Leader
```

### Wire protocol

Each RPC message is `[Version (1 byte)] [MessageType (1 byte)] [Payload]`. The version is `RAFT_PROTOCOL_VERSION = 1`; multi-byte integers are big-endian; variable-length fields (strings, byte arrays) are 4-byte length-prefixed; booleans are a single byte. `RaftTransport` additionally frames every message on the wire with a 4-byte big-endian total-length prefix.

There are six message types (`enum class RaftMessageType : uint8_t`):

| Type | Value | Purpose |
|------|-------|---------|
| `REQUEST_VOTE_REQ` | 1 | Candidate solicits a vote during election. |
| `REQUEST_VOTE_RESP` | 2 | Vote grant/deny + current term. |
| `APPEND_ENTRIES_REQ` | 3 | Leader replicates log entries (empty = heartbeat). |
| `APPEND_ENTRIES_RESP` | 4 | Follower success/failure + last log index. |
| `INSTALL_SNAPSHOT_REQ` | 5 | Leader ships a snapshot chunk to a lagging follower. |
| `INSTALL_SNAPSHOT_RESP` | 6 | Snapshot-chunk acknowledgement. |

Log entries (`struct LogEntry`) carry a term, index, command type, and serialized command bytes. For the exact per-field byte layout and encode/decode of each message, see `include/kawasan/raft/raft_protocol.h` (and the `encode`/`decode` functions in `src/raft/raft_protocol.cpp`) — that header is the authoritative wire spec rather than reproducing every field table here.

### Known limitation: Raft TLS is not enforced

The broker parses and validates the `raft.ssl.*` configuration keys (`raft.ssl.enabled`, `raft.ssl.cert.file`, `raft.ssl.key.file`, `raft.ssl.key.password`, `raft.ssl.ca.file`) in `src/broker/kawasan_broker.cpp`, and rejects startup if `raft.ssl.enabled=true` without cert/key paths. **However, the validated TLS settings are never wired into the transport** — `src/raft/raft_transport.{h,cpp}` contains no SSL/TLS code, only plain TCP with a length prefix. Consequently **inter-broker Raft traffic is always PLAINTEXT, even when `raft.ssl.enabled=true`.** Do not rely on Raft-level encryption; protect inter-broker traffic at the network layer instead. (This is distinct from the Kafka client protocol, where `TcpServer` does support TLS.)

## Kawasan Streams

Kawasan Streams (`include/kawasan/streams/`) is a declarative stream-processing DSL mirroring Kafka Streams concepts in C++20 with templated, type-safe operators. **Status: the DSL/topology layer is implemented; the execution runtime is not** — see [Execution](#execution-tasks-and-threads) below. (The `client` module currently provides only a `Producer`; there is no consumer class, which is the main reason the runtime cannot yet poll and process.)

### Topology and the DSL

A `Topology` (`topology.h`) is a directed acyclic graph of `TopologyNode`s, each typed `NodeType` (SOURCE, PROCESSOR, SINK). Nodes are added with `addSource`, `addProcessor`, `addSink`, and `addStateStore`; the graph is validated for cycles and orphans at build time. `StreamsBuilder` (`streams_builder.h`) is the high-level entry point: `builder.stream<K,V>("topic")` yields a `KStream<K,V>`, `builder.table<K,V>("topic")` yields a `KTable<K,V>`, and `builder.build()` produces the `Topology`. A `KawasanStreams` runtime (`kawasan_streams.h`, `enum class State`) executes it.

### KStream / KTable duality

- **`KStream<K,V>`** is an unbounded, append-only sequence of immutable records — a log of independent events.
- **`KTable<K,V>`** is a changelog: each record updates the latest value for its key, and a null value is a tombstone (delete).

The two convert into each other: `KStream` → `KTable` via `groupByKey().aggregate()` (or `count`/`reduce`), and `KTable` → `KStream` via `toStream()`. `KTable` nodes are materialized into a state store, which is itself backed by a changelog topic.

### Execution: tasks and threads

The intended design groups input partitions into `StreamTask`s — the smallest unit of parallelism — distributed across stream threads (`stream_task.h`). **This layer is scaffolding, not yet functional:** `StreamTask::process()`/`processRecord()`/`commit()` are TODO stubs, and `KawasanStreams` does not yet create tasks from a topology or start stream threads (`src/streams/kawasan_streams.cpp`, `src/streams/stream_task.cpp`). A topology can be built and validated but not executed end-to-end.

### State stores

State-store interfaces (`state_store.h`, `window_store.h`) and builders (`stores.h`) provide:

| Store | Header | Use |
|-------|--------|-----|
| `KeyValueStore<K,V>` | `state_store.h` | General key-value state (aggregations, joins). |
| `WindowStore<K,V>` | `window_store.h` | Time-windowed key-value state. |

Backends are RocksDB (`RocksDBKeyValueStore`, `RocksDBWindowStore` in `rocksdb_store.h`) for persistence or `InMemoryKeyValueStore` for tests/ephemeral state. Each store lives in its own RocksDB directory under the application's working area. Serialization is via `Serde<T>` (`serde.h`, `json_serde.h`); windowed stores prefix the value key with the window timestamp via `WindowedKeySerde`.

### Changelog and fault tolerance

The intended design backs each state store with a changelog topic named `<store-name>-changelog`, replayed on restart to rebuild the store. **This is not yet implemented:** `RocksDBKeyValueStore::sendToChangelog` is a no-op placeholder and `StreamTask::restoreStateStores` is a TODO stub — state stores persist locally in RocksDB but are not fault-tolerant across a lost disk.

### Windowing and time

`windows.h` defines window types via factory APIs:

| Window | API | Shape |
|--------|-----|-------|
| Tumbling | `TimeWindows::of(size)` | Fixed, non-overlapping (advance = size). |
| Hopping | `TimeWindows::of(size).advanceBy(advance)` | Fixed, overlapping (advance < size). |
| Session | `SessionWindows::with(gap)` | Dynamic, gap-based on inactivity. |

`TimeWindows` also supports a `grace(...)` period for late records. Record time is supplied by a `TimestampExtractor` (`timestamp_extractor.h`) — event time (record timestamp), ingestion time (broker append time), or processing time (wall clock).

### Processing guarantees

The intended default guarantee is **at-least-once** (offset commits independent of state writes, so a crash causes reprocessing). The broker side of **exactly-once** — idempotent producers, the `TransactionCoordinator`, and `read_committed` isolation — is real and usable by external clients, but the Streams runtime does not yet execute topologies (see above), so neither guarantee is currently delivered by Kawasan Streams itself. Running real JVM Kafka Streams applications against the broker is the supported route today.

## Kawasan Connect

Kawasan Connect (`include/kawasan/connect/`) is a framework for moving data between external systems and Kawasan topics, following the Kafka Connect model adapted to C++.

### Connector / task / worker model

| Abstraction | Class | Role |
|-------------|-------|------|
| Connector | `Connector` → `SourceConnector` / `SinkConnector` | Validates configuration and splits work into tasks. Source connectors read external systems and write to topics; sink connectors read topics and write to external systems. |
| Task | `Task` → `SourceTask` / `SinkTask` | Unit of parallelism. `SourceTask::poll()` returns new records; `SinkTask::put(records)` writes them out. Tasks are stateless — all progress lives in offsets. |
| Worker | `ConnectWorker` | Standalone runtime that hosts connectors and tasks, distributes tasks across a thread pool, manages offsets, and restarts failed tasks. |

Connectors are registered programmatically through `ConnectorFactory` (and tasks via `TaskFactory`) and configured with a `Properties` map (`name`, `connector.class`, `tasks.max`, plus connector-specific keys). `WorkerConfig` controls bootstrap servers, group id, offset-storage file, flush interval, worker threads, and task-restart backoff. `ConnectorStatus` / `TaskStatus` track lifecycle state. Built-in examples include `FileSourceConnector` / `FileSinkConnector`, `JdbcSinkConnector`, and the in-header `GeneratorSourceConnector` / `ConsoleSinkConnector`. Distributed multi-worker mode is not implemented.

### Records

`SourceRecord` (`connector.h`) carries the source partition and source offset maps, the destination topic and optional partition, an optional key and a value, a timestamp, and headers. `SinkRecord` carries the origin topic/partition/offset, key/value, timestamp, and headers.

### Offset storage

Source-task progress is tracked as opaque key-value maps via `OffsetStorage` (`offset_storage.h`). An `OffsetKey` combines the connector name with a **source partition** map that identifies the data source (e.g. `{"filename": "/var/log/app.log"}`); the stored **offset** value map records the position within that source (e.g. `{"position": "12345"}`). The standalone worker persists these to a local JSON file (`offsetStorageFile`), flushed on `offsetFlushIntervalMs` (default 60 s). Sink-task progress uses ordinary Kawasan consumer offsets. A topic-backed offset store for distributed mode is not implemented.

## Monitoring

`MonitoringManager` (`include/kawasan/broker/monitoring/`) runs a Boost.Beast HTTP server that exposes Prometheus metrics and health/readiness/liveness endpoints, backed by a `MetricsCollector`.

| Endpoint | Purpose |
|----------|---------|
| `/metrics` | Prometheus exposition (request rates/latency, storage, replication, group, Raft, producer-state metrics). |
| `/health`, `/healthz` | Overall health. |
| `/readiness`, `/ready` | Readiness probe. |
| `/liveness`, `/live` | Liveness probe. |

The listen port is `monitoring.port`. It **defaults to 9094** (used by the dev, docker, and per-broker `broker-N` configs); the staging and production configs override it to **8080** via `${KAWASAN_MONITORING_PORT:8080}`, and `monitoring/prometheus.yml` scrapes `:8080`. Operational details (probe wiring, dashboards, alerts) are in [./OPERATIONS.md](./OPERATIONS.md).

## See also

- [../README.md](../README.md) — project intro and quick start
- [./CONFIGURATION.md](./CONFIGURATION.md) — full configuration-key reference
- [./OPERATIONS.md](./OPERATIONS.md) — run, deploy, monitor, and operate a broker
- [./api_coverage_matrix.md](./api_coverage_matrix.md) — supported Kafka protocol APIs and versions
- [./FAQ.md](./FAQ.md) · [./CONTRIBUTING.md](./CONTRIBUTING.md) · [./CHANGELOG.md](./CHANGELOG.md)
- [../helm/kawasan/README.md](../helm/kawasan/README.md) — Helm chart
- [../CLAUDE.md](../CLAUDE.md) — build commands and coding conventions
