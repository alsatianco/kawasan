# Kawasan Configuration Reference

Broker configuration-key reference for Kawasan. This page documents the file formats, environment-variable substitution, the per-category key tables, and the startup-validation rules. For runtime tuning recipes and per-environment deployment guidance see [./OPERATIONS.md](./OPERATIONS.md); for the broker architecture see [./ARCHITECTURE.md](./ARCHITECTURE.md).

## Contents

- [File formats](#file-formats)
- [Environment-variable substitution](#environment-variable-substitution)
- [Which keys are actually honored](#which-keys-are-actually-honored)
- [Identity](#identity)
- [Topics and auto-creation](#topics-and-auto-creation)
- [Protocol compatibility](#protocol-compatibility)
- [Network](#network)
- [Storage paths](#storage-paths)
- [Log segments and retention](#log-segments-and-retention)
- [Replication and Raft](#replication-and-raft)
- [Consumer-group and offsets](#consumer-group-and-offsets)
- [Monitoring](#monitoring)
- [Kafka-protocol TLS/SSL](#kafka-protocol-tlsssl)
- [SASL authentication](#sasl-authentication)
- [Accepted-but-inert keys](#accepted-but-inert-keys)
- [Startup validation](#startup-validation)
- [Command-line overrides](#command-line-overrides)

## File formats

The config loader (`src/common/config.cpp`) auto-detects the file format from the first non-whitespace byte:

| First byte | Format | Example file |
|------------|--------|--------------|
| `{` or `[` | JSON object | `config/broker.dev.properties`, `config/broker.dev.json`, `config/broker.production.properties` |
| anything else | Kafka-style `key=value` properties | `config/broker-0.properties`, `config/broker-1.properties`, `config/broker-2.properties` |

Both formats are first-class — there is no "JSON-only" mode. The `.properties` extension on the JSON files is historical; the loader ignores the extension and dispatches purely on content. See [../CLAUDE.md](../CLAUDE.md) for this gotcha.

Properties parsing rules: keys/values are split on the first `=` (falling back to the first `:`); leading/trailing whitespace is trimmed; lines beginning with `#` or `!` are comments; malformed lines (no separator) are skipped silently. Property values are coerced to a typed JSON value so that `getInt`/`getBool`/`getString` behave identically regardless of source format — `true`/`false` become booleans, all-digit strings (optional leading `-`) become integers, everything else stays a string.

JSON files conventionally carry comments as dummy string keys (e.g. `"# Broker Identity": ""`); these unknown keys are loaded and harmlessly ignored.

## Environment-variable substitution

Both formats expand `${...}` references inside string values before the value is stored:

| Syntax | Behavior |
|--------|----------|
| `${VAR}` | Required. The broker throws `Required environment variable not set: VAR` at load time if `VAR` is unset or empty. |
| `${VAR:default}` | Optional. Uses `default` when `VAR` is unset or empty. |

An unset env var with no default is fatal — load fails with `Error in config key '<key>': Required environment variable not set: VAR`. Substitution applies to string values only; numeric/boolean JSON literals pass through unchanged.

```json
{
  "broker.id": "${KAWASAN_BROKER_ID}",
  "host": "${KAWASAN_HOST:0.0.0.0}",
  "monitoring.port": "${KAWASAN_MONITORING_PORT:8080}"
}
```

## Which keys are actually honored

This build of the single-node broker reads a specific subset of keys; the remaining Kafka-compatible keys present in the sample configs are accepted (stored and round-trippable) but are not wired to any subsystem. Inert keys are catalogued in [Accepted-but-inert keys](#accepted-but-inert-keys) so the sample files stay drop-in compatible with Kafka tooling without implying behavior that does not exist. The tables below mark each key **Honored** or **Inert**.

## Identity

Read in `src/broker/kawasan_broker.cpp`.

| Key | Type | Default | Status | Description |
|-----|------|---------|--------|-------------|
| `broker.id` | int | — (required) | Honored | Unique broker id. Must be present and `>= 0`. |
| `host` | string | `localhost` | Honored | Bind address for the Kafka TCP listener (`0.0.0.0` for all interfaces). |
| `advertised.host` | string | value of `host` | Honored | Hostname/IP returned to clients in Metadata responses. |
| `port` | int | `9092` | Honored | Kafka protocol listener port (1–65535). |
| `cluster.id` | string | `kawasan-cluster` | Honored | Initial cluster id; once metadata is persisted, the stored cluster id takes precedence. |
| `broker.rack` | string | `""` (no rack) | Honored | Rack identifier advertised to clients (Metadata v1+ and DescribeCluster) for rack-aware fetch. Empty = no rack, matching Kafka's default. |

## Topics and auto-creation

Read in `src/broker/kawasan_broker.cpp`; also surfaced through DescribeConfigs.

| Key | Type | Default | Status | Description |
|-----|------|---------|--------|-------------|
| `auto.create.topics.enable` | bool | `true` | Honored | Auto-create a topic on first produce/metadata/fetch (honored only when the client also allows auto-creation). Disable to require explicit CreateTopics. |
| `num.partitions` | int | `1` | Honored | Default partition count for auto-created topics. Values `<= 0` fall back to 1. |
| `delete.topic.enable` | bool | `true` | Inert | Accepted for Kafka tooling compatibility; DeleteTopics is always served regardless. |

## Protocol compatibility

| Key | Type | Default | Status | Purpose |
|-----|------|---------|--------|---------|
| `compatibility.max.api.version.profile` | string | `4.x` | Honored | `4.x` advertises Produce 11, Fetch 13, ListOffsets 8, OffsetFetch 9, DeleteRecords 2 and SASL Authenticate 2. `3.x` caps these at 9, 12, 7, 8, 0 and 1 respectively. Other APIs retain their own supported ranges. Unknown profiles fail before services start. |

The same table sets ApiVersions advertisement and dispatcher acceptance. A
request above a profile cap is rejected without running its handler; where
its codec is known, errors preserve the requested wire shape. Fetch v13 uses
persisted controller-assigned UUIDs and returns `UNKNOWN_TOPIC_ID` for missing
IDs without auto-creation. OffsetFetch v9 supports classic groups with null
member ID and epoch -1; KIP-848 member references return `UNSUPPORTED_VERSION`
until ConsumerGroupHeartbeat semantics land in CM-12.

The container config accepts `KAWASAN_COMPAT_API_PROFILE`; ecosystem/client
Compose checks expose it as `COMPAT_API_PROFILE`. For example:

```bash
COMPAT_API_PROFILE=3.x bash tests/clients/run.sh rdkafka-2.8.0
```

## Network

| Key | Type | Default | Status | Description |
|-----|------|---------|--------|-------------|
| `network.io_threads` | int | hardware concurrency (≥1) | Honored | TCP server I/O thread count. Values `<= 0` fall back to the hardware default. |
| `fetch.purgatory.threads` | int | `2` | Honored | Worker threads that complete parked long-poll Fetches (woken by partition appends/HW changes or `max_wait_ms` expiry). Parked fetches no longer occupy `network.io_threads`. |
| `network.max_frame_bytes` | long | `16777216` (16 MiB) | Honored | Maximum accepted request frame size. |
| `network.tcp_nodelay` | bool | `true` | Honored | Sets `TCP_NODELAY` on accepted client sockets (disables Nagle; Kafka parity). |
| `network.tcp_keepalive` | bool | `true` | Honored | Sets `SO_KEEPALIVE` on accepted client sockets so half-open peers are detected. |
| `network.socket_send_buffer_bytes` | int | `0` (OS default) | Honored | `SO_SNDBUF` for accepted client sockets; `0` keeps the kernel default. |
| `network.socket_recv_buffer_bytes` | int | `0` (OS default) | Honored | `SO_RCVBUF` for accepted client sockets; `0` keeps the kernel default. |
| `network.idle_connection_timeout_seconds` | long | `600` | Honored | Idle client connections are closed after this long; reap interval is `clamp(timeout/2, 5s, 60s)`. `0` disables reaping. |
| `num.network.threads` | int | `8` | Inert | Accepted; only range-validated. Use `network.io_threads`. |
| `num.io.threads` | int | `8` | Inert | Accepted; only range-validated. |
| `socket.send.buffer.bytes` | int | `102400` | Inert | Accepted; not applied — use `network.socket_send_buffer_bytes` (the shipped configs carry small values that would shrink kernel defaults if honored). |
| `socket.receive.buffer.bytes` | int | `102400` | Inert | Accepted; not applied — use `network.socket_recv_buffer_bytes`. |
| `socket.request.max.bytes` | int | `104857600` | Inert | Accepted; frame cap is `network.max_frame_bytes`. |

## Storage paths

| Key | Type | Default | Status | Description |
|-----|------|---------|--------|-------------|
| `log.dirs` | string | `/tmp/kawasan-logs` | Honored | Directory for RocksDB-backed log segments. Required by validation. |
| `metadata.dir` | string | `<log.dirs>/meta` | Honored | Directory for cluster metadata and persisted Raft state (`<metadata.dir>/raft`). |

## Log segments and retention

Read in `src/broker/kawasan_broker.cpp`. Time-based keys are resolved most-specific-first.

| Key | Type | Default | Status | Description |
|-----|------|---------|--------|-------------|
| `log.segment.bytes` | long | `1073741824` (1 GiB) | Honored | Max size of a single segment. Validation requires `>= 1024`. |
| `log.roll.ms` | long | (internal default) | Honored | Time-based segment roll interval. Takes precedence over `log.roll.hours`. |
| `log.roll.hours` | long | — | Honored | Segment roll interval in hours; used only when `log.roll.ms <= 0`. |
| `log.retention.ms` | long | (internal default) | Honored | Time-based retention. Takes precedence over `log.retention.hours`. |
| `log.retention.hours` | long | — | Honored | Retention in hours; used only when `log.retention.ms <= 0`. Validation requires `>= -1`. |
| `log.retention.bytes` | long | `-1` (unlimited) | Honored | Size-based retention cap. |
| `log.cleaner.interval.ms` | long | `300000` (5 min) | Honored | Compaction + retention sweep interval. |
| `log.cleanup.policy` | string | `delete` | Inert | Accepted; `delete` vs `compact` selection is per-topic, not driven by this broker-level key. |
| `log.retention.check.interval.ms` | long | `300000` | Inert | Accepted; sweep cadence is `log.cleaner.interval.ms`. |
| `log.durability` | string | `sync` | Honored | Partition-log write durability. `sync` (default) fsyncs each acked produce (`WriteOptions.sync=true`) so an acknowledged record survives a power loss / OS crash; `async` is WAL-buffered only (lower latency, but a machine crash before the next flush loses the tail). |
| `replica.high.watermark.checkpoint.interval.ms` | long | `5000` | Honored | How often each partition's high-watermark checkpoint (`checkpoint.meta`) is rewritten if it changed. Before this key existed, the checkpoint was fsynced on every append, which was about 95% of the cost of an async append. It is also flushed on clean shutdown and immediately for durability-critical internal writes. On a single node, HW is recovered to the log end at open, so a lagging checkpoint never hides records. |
| `log.flush.interval.messages` | long | `10000` | Inert | Accepted; not applied. Durability is controlled by `log.durability`. |
| `log.flush.interval.ms` | long | `1000` | Inert | Accepted; not applied. |
| `offsets.topic.num.partitions` | int | `16` | Honored | Partition count for the internal `__consumer_offsets` topic. Each partition is its own RocksDB instance, so a lower count reduces the startup file-descriptor footprint (Kafka's default is 50). Fixed at first creation. |
| `transaction.state.topic.num.partitions` | int | `16` | Honored | Partition count for the internal `__transaction_state` topic. Same FD trade-off as above. |
| `producer.state.snapshot.interval.ms` | long | `60000` | Honored | Interval of the background writer that checkpoints per-partition idempotent-producer state to `producer-<offset>.psnap` files (atomic temp+rename, CRC-32C framed, newest two kept), plus one snapshot on graceful shutdown. On restart the broker restores the newest valid snapshot and replays only the log tail after its offset instead of the whole log; a corrupt snapshot is skipped and recovery falls back to an older snapshot or a full replay. Clamped to a 1s minimum. |
| `transaction.abort.timed.out.transaction.cleanup.interval.ms` | long | `10000` | Honored | Interval of the background sweep that auto-aborts transactions that exceeded their client-requested `transaction.timeout.ms` (M2). Clamped to a 1s floor. The sweep starts after startup transaction-state replay. |
| `compression.type` | string | `none` | Inert (validated) | Accepted; value is validated against `none/gzip/snappy/lz4/zstd` but compression selection is per-batch from the client. |

Durability note: with `log.durability=sync` (the default) message/log writes are fsynced (`sync=true`) before the produce is acknowledged, as are offset commits, for at-least-once durability; consumer-group metadata uses async writes (`sync=false`) protected by the WAL. The high-watermark checkpoint is written atomically (temp file + fsync + rename), periodically (`replica.high.watermark.checkpoint.interval.ms`) rather than per append. Log start and end offsets are always rebuilt from the segments. See [./OPERATIONS.md](./OPERATIONS.md) for the durability model.

## Replication and Raft

Single-node is the primary, hardened mode. Multi-broker Raft replication exists but is not production-hardened.

| Key | Type | Default | Status | Description |
|-----|------|---------|--------|-------------|
| `default.replication.factor` | int | `1` | Honored (clamped) | Default RF for new topics. Values above the **cluster size** (1 + number of commas in `raft.peers`, so 1 for single-node) are clamped to it with a warning; `< 1` is corrected to 1. In a 3-broker cluster RF=3 is honored (M5 follower replication). With `deployment.mode=production`, RF > cluster size is **rejected at startup** instead of silently clamped (see Deployment mode). |
| `raft.port` | int | `9093` | Honored | Inter-broker Raft listener port. |
| `raft.peers` | string | `""` (single-node) | Honored | Comma-separated `id:host:port` list, e.g. `0:host0:9093,1:host1:9093`. Empty = single-node. If non-empty, **this broker's `broker.id` must appear in the list** or startup fails. |
| `raft.ssl.enabled` | bool | `false` | Parsed, **not enforced** | When `true`, the cert/key/ca paths below are read and validated, but `src/raft/raft_transport.{cpp,h}` contains no TLS code — inter-broker Raft traffic stays PLAINTEXT regardless. See [../CLAUDE.md](../CLAUDE.md). |
| `raft.ssl.cert.file` | string | `""` | Parsed, not enforced | PEM server certificate (required by `isValid()` when `raft.ssl.enabled=true`). |
| `raft.ssl.key.file` | string | `""` | Parsed, not enforced | PEM private key. |
| `raft.ssl.key.password` | string | `""` | Parsed, not enforced | Private-key password. |
| `raft.ssl.ca.file` | string | `""` | Parsed, not enforced | PEM CA bundle for peer verification. |
| `min.insync.replicas` | int | `1` | Honored (acks=all) | Enforced at write time (M4): an `acks=all` produce is rejected with `NOT_ENOUGH_REPLICAS` when the partition's ISR has fewer than this many members. With the default `1` and a single-node ISR of `{self}`, the gate never fires (`acks=all` behaves exactly as before). Still range/cross-checked at startup (`>= 1`, `<= default.replication.factor` when that key is set). With `deployment.mode=production`, a value above the **cluster size** is **rejected at startup** (so minISR=2 is fine in a 3-broker production cluster, but impossible single-node). |
| `offsets.topic.replication.factor` | int | `1` | Inert | Accepted; not applied. |
| `transaction.state.log.replication.factor` | int | `1` | Inert | Accepted; not applied. |
| `transaction.state.log.min.isr` | int | `1` | Inert | Accepted; not applied. |
| `replica.lag.time.max.ms` | long | `30000` | Honored (multi-broker) | M6: a follower whose last replica-fetch is older than this is dropped from the ISR by the partition leader (shrink), so a dead/slow follower stops blocking `acks=all`; it is re-added once it fetches again and catches up to the high watermark. Only active in multi-broker mode (the replica fetcher runs only when `raft.peers` is set). Clamped to a 1s floor. |
| `broker.liveness.timeout.ms` | long | `9000` | Honored (multi-broker) | M8: the controller (Raft leader) considers a peer broker dead when it hasn't answered a Raft AppendEntries for this long. It then elects new leaders, from the in-sync replicas, for the partitions that broker led, and drops it from every ISR. A newly elected controller gives every peer one full window before judging it. Half of this value (minimum 500 ms) is also the metadata lease (M8-E1): a broker that can't confirm its Raft view within the lease stops serving Produce/Fetch and reports not-ready. Clamped to a 1 s floor. Only active in multi-broker mode. |
| `unclean.leader.election.enable` | bool | `false` | Honored (multi-broker) | M8: when a partition's leader dies and no in-sync replica is alive, `false` leaves the partition **offline** (no leader; Metadata reports `LEADER_NOT_AVAILABLE`) until an in-sync replica returns. `true` elects the first live out-of-sync replica instead. That restores availability but **may lose acknowledged records**, and the controller logs an error. |
| `num.replica.fetchers` | int | `1` | Inert | Accepted; not applied. |
| `num.recovery.threads.per.data.dir` | int | `1` | Inert | Accepted; not applied. |

The `broker-0/1/2.properties` files also use Kafka-native `listeners` / `advertised.listeners` keys (e.g. `PLAINTEXT://0.0.0.0:9092`). These are not parsed by the broker — the listener is configured via `host`/`port`. Treat `listeners`/`advertised.listeners` in those files as documentation, not active config.

Enabling `raft.ssl.enabled=true` produces a broker that logs "TLS enabled for Raft protocol" yet sends plaintext on the wire. Do not rely on it for confidentiality.

## Consumer-group and offsets

The group coordinator and offset manager use hardcoded defaults; the keys below are accepted but not read from config. Per-member session and rebalance timeouts come from the client's JoinGroup request (clamped to a safe range), and offset/group retention defaults to 7 days in code.

| Key | Type | Default | Status | Description |
|-----|------|---------|--------|-------------|
| `group.initial.rebalance.delay.ms` | int | `0` | Inert | Rebalance is driven by client-supplied timeouts. |
| `group.min.session.timeout.ms` | int | `6000` | Inert | Session-timeout clamp is internal. |
| `group.max.session.timeout.ms` | int | `300000` | Inert | Session-timeout clamp is internal. |
| `group.retention.ms` | long | `604800000` (7 days) | Inert | Group retention is a hardcoded 7-day default. |
| `offsets.retention.minutes` | int | (7 days in code) | Inert | Offset retention is a hardcoded 7-day default. |
| `consumer.lag.metrics.enabled` | bool | `false` | Inert | Present in dev configs; not wired in this build. |
| `consumer.lag.check.interval.ms` | int | `30000` | Inert | Not wired. |

Offset-commit durability: commits use synchronous RocksDB writes and are batched across partitions into a single `WriteBatch`. See [./OPERATIONS.md](./OPERATIONS.md).

## Monitoring

Read in `src/broker/kawasan_broker.cpp`. Serves Prometheus metrics at `/metrics` and health endpoints (Boost.Beast HTTP).

| Key | Type | Default | Status | Description |
|-----|------|---------|--------|-------------|
| `monitoring.host` | string | `0.0.0.0` | Honored | Bind address for the HTTP monitoring server. |
| `monitoring.port` | int | `9094` | Honored | HTTP port for metrics/health. Must differ from `port` (validation error if equal); range 1–65535. |
| `monitoring.enabled` | bool | `true` | Inert | Accepted; the monitoring server is started regardless in this build. |
| `metrics.recording.level` | string | `INFO` | Inert | Accepted; not applied. |

Port note: the default is **9094** (used by `broker.dev`, `broker.docker`, and `broker-N` configs). The staging and production sample configs override it to **8080** via `${KAWASAN_MONITORING_PORT:8080}`, and `monitoring/prometheus.yml` scrapes `:8080`. Pick the port to match your scrape target.

## Kafka-protocol TLS/SSL

| Key | Type | Default | Status | Description |
|-----|------|---------|--------|-------------|
| `security.protocol` | string | `PLAINTEXT` | Honored (gating) | `PLAINTEXT` or `SSL`. `SSL` sets the TLS-enabled flag — see refusal below. `SASL_*` values are not gating in this build. |
| `ssl.enabled` | bool | `false` | Honored (gating) | Alternative to `security.protocol=SSL`; either one enables the TLS path. |

**TLS for the Kafka protocol is not implemented in this build.** If `security.protocol=SSL` or `ssl.enabled=true`, the broker **refuses to start** and throws (`kawasan_broker.cpp`) rather than silently listening as plaintext. To run this build, keep `security.protocol=PLAINTEXT` / `ssl.enabled=false`.

Sample configs: `config/broker.production.properties` now defaults `security.protocol` to `PLAINTEXT` and `ssl.enabled` to `false` (so it starts out of the box). You can still opt into the TLS path via `KAWASAN_SECURITY_PROTOCOL`/`KAWASAN_SSL_ENABLED`, but the broker will then refuse to start until client TLS is implemented.

The following client-TLS keys appear in `server.properties.example` / `broker.production.properties` but are **not read** by the broker (the TLS path refuses before they would be used): `ssl.cert.file`, `ssl.key.file`, `ssl.ca.file`, `ssl.key.password`, `ssl.client.auth`, `ssl.keystore.location`, `ssl.keystore.password`, `ssl.truststore.location`, `ssl.truststore.password`, `ssl.protocol`, `ssl.cipher.suites`. The two sample files even disagree on scheme (PEM `ssl.cert.file` vs JKS `ssl.keystore.*`); neither is consumed.

## SASL authentication

SASL/PLAIN and SASL/SCRAM-SHA-256 credentials are loaded from two sources each (a credentials file, then an inline JSON object); both sources merge into one credential map. If no credentials are configured, PLAIN falls back to "accept any non-empty user/password" (dev mode) — but **only outside production**: with `deployment.mode=production`, PLAIN auth with no configured credentials is rejected. PLAIN password comparison is constant-time.

| Key | Type | Default | Status | Description |
|-----|------|---------|--------|-------------|
| `sasl.plain.credentials.file` | string | `""` | Honored | Path to a `user:password`-per-line file (`#` comments). Missing file is fatal. |
| `sasl.plain.users` | string (JSON) | `""` | Honored | Inline `{"user":"password", ...}` object. |
| `sasl.scram.credentials.file` | string | `""` | Honored | SCRAM-SHA-256 `user:password`-per-line file; salt/keys derived at startup. |
| `sasl.scram.users` | string (JSON) | `""` | Honored | Inline `{"user":"password", ...}` for SCRAM-SHA-256. |
| `sasl.enabled` | bool | `false` | Inert | Accepted; SASL handlers are available irrespective of this flag. |
| `sasl.mechanism` | string | `PLAIN` | Inert | Accepted; offered mechanisms are determined by which credential maps are populated. |
| `sasl.jaas.config` | string | `""` | Inert | Accepted; not parsed. |

## Authorization (ACLs)

ACL **enforcement** is opt-in. When disabled (the default), bindings can still be created/described/deleted via the ACL APIs but are not consulted on requests — behavior is unchanged from earlier builds. When enabled, the broker consults `AclStore` on Produce (WRITE), Fetch (READ), CreateTopics (CREATE), and DeleteTopics (DELETE), with Kafka semantics: an explicit DENY beats any ALLOW; if no binding matches, the result is `allow.everyone.if.no.acl.found`. The principal is the authenticated SASL user (`User:<name>`) or `User:ANONYMOUS`.

| Key | Type | Default | Status | Description |
|-----|------|---------|--------|-------------|
| `authorizer.enabled` | bool | `false` | Honored | Master switch for ACL enforcement. Off = no enforcement (bindings stored only). |
| `allow.everyone.if.no.acl.found` | bool | `false` | Honored | When the authorizer is enabled and no binding matches a request, allow it (`true`) or deny it (`false`, Kafka default). |
| `super.users` | string | `""` | Honored | `;`-separated principals that bypass all ACL checks, e.g. `User:admin;User:svc`. |

## Client quotas

Per-client byte-rate throttling. Disabled (unlimited) when `<= 0` (the default). When set, a client exceeding its rate receives a `throttle_time_ms` in its Produce/Fetch response so well-behaved clients back off.

| Key | Type | Default | Status | Description |
|-----|------|---------|--------|-------------|
| `quota.producer.default` | long | `0` (unlimited) | Honored | Per-client produce byte/sec quota. |
| `quota.consumer.default` | long | `0` (unlimited) | Honored | Per-client consume byte/sec quota. |

## Deployment mode

| Key | Type | Default | Status | Description |
|-----|------|---------|--------|-------------|
| `deployment.mode` | string | `""` | Honored | Set to `production` (or env `KAWASAN_DEPLOYMENT_MODE=production`) to fail fast at startup on settings the broker cannot honor (TLS-implying `security.protocol`, `default.replication.factor` or `min.insync.replicas` above the cluster size derived from `raft.peers`) and to refuse SASL/PLAIN "accept-any" when no credentials are configured. |

## Accepted-but-inert keys

These keys are present in the sample configs for Kafka tooling compatibility but have no effect in this build (stored and round-trippable; some are validated). Do not rely on them to change behavior.

| Category | Keys |
|----------|------|
| Network | `num.network.threads`, `num.io.threads`, `socket.send.buffer.bytes`, `socket.receive.buffer.bytes`, `socket.request.max.bytes` |
| Flush / cleanup | `log.flush.interval.messages`, `log.flush.interval.ms`, `log.flush.scheduler.interval.ms`, `log.retention.check.interval.ms`, `log.cleanup.policy` |
| Replication | `offsets.topic.replication.factor`, `transaction.state.log.replication.factor`, `transaction.state.log.min.isr`, `num.replica.fetchers`, `num.recovery.threads.per.data.dir` |
| Producer / consumer | `max.request.size`, `fetch.min.bytes`, `fetch.max.wait.ms`, `compression.type` |
| Groups / offsets | `group.initial.rebalance.delay.ms`, `group.min.session.timeout.ms`, `group.max.session.timeout.ms`, `group.retention.ms`, `offsets.retention.minutes`, `consumer.lag.metrics.enabled`, `consumer.lag.check.interval.ms` |
| Monitoring | `monitoring.enabled`, `metrics.recording.level` |
| Logging | `log.level` (set via `--log-level` flag), `log.format` (inert) |
| ZooKeeper / TXN | `zookeeper.connect`, `zookeeper.connection.timeout.ms`, `background.threads`, `log.message.timestamp.type` |
| TLS (Kafka protocol) | `ssl.cert.file`, `ssl.key.file`, `ssl.ca.file`, `ssl.key.password`, `ssl.client.auth`, `ssl.keystore.*`, `ssl.truststore.*`, `ssl.protocol`, `ssl.cipher.suites` |
| Listeners | `listeners`, `advertised.listeners` (use `host`/`port` instead) |

Topic auto-creation keys (`auto.create.topics.enable`, `num.partitions`) are honored and documented under [Topics and auto-creation](#topics-and-auto-creation).

## Startup validation

`Config::validate()` (`src/common/config.cpp`) runs before the broker is constructed (called from `tools/kawasan-broker.cpp`). Any error aborts startup with `Configuration validation failed:` followed by the list; warnings print to stderr and continue.

Required keys (each missing key is an error): `broker.id`, `host`, `port`, `log.dirs`.

| Rule | Severity |
|------|----------|
| `broker.id >= 0` | error |
| `1 <= port <= 65535` | error |
| `num.network.threads >= 1` | error |
| `num.network.threads <= 1024` | warning ("very high … may cause resource issues") |
| `num.io.threads >= 1` | error |
| `log.segment.bytes >= 1024` | error |
| `log.retention.hours >= -1` | error |
| `default.replication.factor >= 1` | error |
| `min.insync.replicas >= 1` | error |
| `min.insync.replicas <= default.replication.factor` | error |
| `1 <= monitoring.port <= 65535` | error |
| `monitoring.port != port` | error |
| `compression.type` ∈ `{none, gzip, snappy, lz4, zstd}` | error |

Validation only checks the keys above; it does not warn about inert keys or about TLS-refusal config (that refusal happens later, during broker construction).

## Command-line overrides

`kawasan-broker` (Boost.Program_options) accepts a config file and a few overrides applied **after** the file loads and **before** validation:

| Flag | Maps to | Notes |
|------|---------|-------|
| `--config, -c <path>` | — | Config file to load (auto-detected JSON or properties). |
| `--broker-id <int>` | `broker.id` | Override. |
| `--host <string>` | `host` | Override. |
| `--port, -p <int>` | `port` | Override. |
| `--log-dir <path>` | `log.dirs` | Override. |
| `--log-level <level>` | logger only | `trace`/`debug`/`info`/`warn`/`error`/`critical`; defaults to `info`. Initializes the logger; not stored as a config key. |
| `--help, -h` | — | Print options and exit. |

```bash
./build/tools/kawasan-broker --config config/broker.dev.properties --log-level debug
```

See [./OPERATIONS.md](./OPERATIONS.md) for tuning and deployment, [./ARCHITECTURE.md](./ARCHITECTURE.md) for subsystem design, and [./FAQ.md](./FAQ.md) for common issues.
