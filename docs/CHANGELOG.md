# Changelog

All notable changes to Kawasan are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

For configuration details see [./CONFIGURATION.md](./CONFIGURATION.md); for upgrade and operational procedures see [./OPERATIONS.md](./OPERATIONS.md); for the full protocol surface see [./api_coverage_matrix.md](./api_coverage_matrix.md).

## [Unreleased]

Single-node durability, security and quotas; durable single-node transactions (M1–M3); and multi-broker replication, automatic clean leader failover, fencing, epoch-based divergence recovery, and coordinator routing (M4–M8). Single-node remains the primary supported mode. Multi-broker remains experimental pending randomized-fault validation and durable coordinator failover.

### Fixed

- Hold acknowledgements on prospective ISR members before submitting expansion, retaining the barrier across stale metadata polls and ambiguous controller RPCs. Data handlers refresh the committed ISR under the partition lock; deferred acknowledgements recheck metadata ownership/epoch and minISR, including idempotent retries. Shrinking below minISR after append returns `NOT_ENOUGH_REPLICAS_AFTER_APPEND` instead of success.

- Defer consumer Fetch and ListOffsets while a new leader confirms its inherited log tail with the current ISR, preventing stale follower/checkpoint high watermarks from appearing as regressing committed offsets. Replica Fetch remains available to complete recovery; the M9 observer now fails immediately on an I3 regression.

- Check the exact acknowledged offsets and payloads of fully visible transactions even when their commit result is ambiguous; all-or-nothing visibility alone cannot validate their data.

- Pin the executable in every chaos evidence directory so rebuilding the worktree cannot change the candidate between restarts. Record the nemesis hash alongside the binary and checker hashes.

- Keep abort-history completion idempotent when recovery re-drives a transaction whose ABORT marker is already durable, so duplicate abort entries cannot hide a later committed transaction.

- Restore historical aborted transaction ranges from durable data/control batches before serving traffic. Fetch includes aborts when reading inside a transaction, retains history beyond 1,000 entries, and prunes ranges only after log retention passes their markers. M9 commit/abort/crash checks now survive repeated SIGKILLs without exposing aborted records.

- Rebuild idempotent producer state from the retained log before a follower becomes a writable leader. Partition writes, replica ingestion/truncation and promotion replay share a lock; follower snapshots are skipped and transaction control markers no longer overwrite sequence state. The M9 three-broker checker passes all six pause/SIGKILL role combinations after this fix.

- OffsetFetch reads durable group offsets even without JoinGroup membership, including after restart. M9 transaction checks exposed that manual consumers could commit offsets successfully yet read the unset sentinel.

### Added

- M9 live consistency driver with durable acknowledgements, independent I1–I4 checks, min-ISR-preserving pause/SIGKILL schedules, retained evidence and a disposable broker HW mutation proof. The new chaos nightly runs a 30-minute cluster workload plus single-node durability and transaction/crash modes. Seven scheduled green nightlies remain the acceptance gate.

- M9 subprocess SIGKILL regression proves exact acknowledged record recovery, idempotent producer sequence replay/deduplication, and durable transactional producer epochs without graceful shutdown.

- M9 seeded nemesis planner preserves min ISR and Raft quorum, requires healing between faults, and covers leader/follower/controller kills and pauses. The process harness adds validated SIGKILL, safe restarts, configurable port bases, retained failure data, and true single-node mode.

- M9 durable JSONL acknowledgement ledger and independent I1–I4 consistency checks, including offline evidence replay and corruption fixtures registered with CTest.

- Ecosystem verification now distinguishes failures and skips, retains per-check evidence, runs durability/SASL against the selected Docker broker, verifies real producer deliveries, and uses Kafka-valid compaction settings. Pinned Python clients and shared container networking keep both oracle and candidate legs reproducible.

- Docker builds use Ubuntu 24.04 and bounded compiler parallelism, omit local artifacts, and ship a container config with persistent storage and environment substitution. Linux builds normalize distro zstd targets and avoid glibc endian-macro collisions and transitive-header assumptions.

- M8 acceptance verified with 69/69 CTest tests, 20 consecutive leader/controller failover iterations, 20 consecutive divergence recoveries, and a real two-consumer single-coordinator proof. The operations guide includes cluster recovery steps.

- Coordinator ownership guards (M8-G2) reject misrouted group and transaction requests with NOT_COORDINATOR before accessing local state. Batched OffsetFetch checks each group independently; stale brokers refuse coordinator traffic. Nontransactional producer-ID allocation remains local.

- FindCoordinator routes group and transaction keys to the leader of their hashed internal-topic partition (M8-G1), including per-key v4 responses. Missing or offline owners return COORDINATOR_NOT_AVAILABLE; single-node routing stays local.

- **Followers reconcile divergent logs with a new leader (KIP-101, M8-E3).** When a broker becomes a follower, its leader or epoch changes, or a fetch is out of range or leaves a gap, it first asks the leader where its own latest epoch ends (OffsetForLeaderEpoch). It then truncates its log there before fetching. Without epoch history on either side it falls back to its high watermark. Replica fetches now use Fetch v9 and carry the follower's `current_leader_epoch`, so a stale leader or follower is fenced instead of served. Fixes found along the way:
  - A new leader records its epoch start before its first write, not on the next fetcher cycle.
  - A fetch task that spans a leadership change is re-checked against committed metadata before its data is ingested. Without this, a just-elected leader could append its predecessor's divergent records.
  - A replicated batch that overlaps the follower's log end replaces the tail instead of being skipped forever.
  - Proven by `scripts/tests/test_divergence_truncation_m8.py`: an isolated leader's unreplicated acks=1 writes are truncated, and every acks=all record survives after it leads again.
- **Real `OffsetForLeaderEpoch` (KIP-101, M8-F4).** The partition leader now answers from the partition's leader-epoch history. For a requested epoch it returns the largest known epoch at or below it, plus the offset where that epoch ends, or `-1/-1` if the epoch is newer than anything known. It enforces the caller's `current_leader_epoch` (fenced/unknown) and refuses on non-leaders or stale metadata. Logs without epoch history, including all single-node logs, keep the previous answer: the current epoch and the log end.
- **Leader-epoch fencing for Fetch and ListOffsets (KIP-320, M8-E2).** The broker now checks the `current_leader_epoch` that clients send (Fetch v9+, ListOffsets v4+) against the partition's epoch. A client holding an older epoch gets `FENCED_LEADER_EPOCH` and refreshes its metadata. A newer epoch gets the retriable `UNKNOWN_LEADER_EPOCH`, and `-1` opts out. ListOffsets is now answered only by the partition leader, and only while its metadata is current (`NOT_LEADER_FOR_PARTITION` otherwise). Previously any broker answered from its own log, so a follower returned stale offsets.
- **Stale-metadata fencing for multi-broker (M8-E1).** A broker serves Produce and Fetch only while it can prove its metadata is current. As Raft leader it needs a majority of recent acks. As follower it must have caught up with a leader since starting and have heard from it within half of `broker.liveness.timeout.ms`. Otherwise it answers `NOT_LEADER_FOR_PARTITION` and `/ready` returns 503. So a restarted ex-leader can't accept writes on its pre-crash view of partition leadership (`scripts/tests/test_stale_leader_fencing_m8.py`), and a cut-off broker stops serving before the controller fails its partitions over. A newly elected Raft leader also commits a no-op entry, so the previous leader's committed changes apply promptly. Single-node is unaffected.
- **Automatic partition-leader failover (multi-broker, M8).** The controller (Raft leader) tracks when each broker last answered Raft heartbeats. When a broker is silent for `broker.liveness.timeout.ms` (default 9 s), the controller elects a new leader for each partition that broker led. The new leader is the first live in-sync replica in assignment order, and the leader epoch is bumped. The dead broker is also dropped from every ISR, so `acks=all` keeps working. If no in-sync replica is alive, the partition goes offline (Metadata reports `LEADER_NOT_AVAILABLE`) until one returns, unless `unclean.leader.election.enable=true`. A new controller waits one liveness window before failing anything over. On a live 3-broker cluster, failover takes about 4 to 5 s with a 3 s liveness timeout, including when the controller itself dies (`scripts/tests/test_leader_failover_m8.py`). Stale-leader fencing and KIP-101 epoch reconciliation are implemented; see Known Limitations for the remaining cluster scope.
- **Durable produce.** `log.durability` config (default `sync`) fsyncs each acknowledged produce before responding, so an acked record survives a power loss/OS crash; `async` keeps the prior WAL-buffered behavior. See [./CONFIGURATION.md](./CONFIGURATION.md).
- **Production mode.** `deployment.mode=production` (or `KAWASAN_DEPLOYMENT_MODE`) fails fast at startup on settings the build cannot honor (TLS-implying `security.protocol`, `default.replication.factor` or `min.insync.replicas` above the cluster size) and refuses SASL/PLAIN "accept-any" when no credentials are configured.
- **ACL enforcement (opt-in).** `authorizer.enabled` gates Produce/Fetch/Create/DeleteTopics against the ACL store, with `super.users` and `allow.everyone.if.no.acl.found`. ACLs were always stored; they are now enforceable.
- **Per-client quotas (opt-in).** `quota.producer.default` / `quota.consumer.default` (bytes/sec; `0` = unlimited) return a real `throttle_time_ms` so well-behaved clients back off.
- **SASL/SCRAM** (SHA-256/512) authentication alongside SASL/PLAIN, with constant-time credential comparison; the authenticated principal is carried per-connection and used by the ACL gate.
- **Producer state survives restart.** Idempotent-producer dedup/epoch state is rebuilt at startup by replaying persisted record-batch headers (`KawasanBroker::replayProducerStateFromLog`), closing a silent-duplicate-on-restart gap.
- **`broker.rack`** is advertised to clients (Metadata v1+/DescribeCluster) for rack-aware fetch.
- **Rebuilt internal-topic partition counts.** `offsets.topic.num.partitions` / `transaction.state.topic.num.partitions` are configurable (default 16); each partition is a RocksDB instance, so the count drives the startup file-descriptor footprint.
- Multi-broker metadata plane: Raft election/commit fixes (elections and commit now complete), commit-apply moved off the Raft lock, cross-broker replica assignment for RF > 1, and a controller-startup-race fix.
- **Socket tuning + connection lifecycle metrics.** Accepted client sockets get `TCP_NODELAY` and `SO_KEEPALIVE` by default plus configurable kernel buffers (`network.tcp_nodelay`, `network.tcp_keepalive`, `network.socket_send_buffer_bytes`, `network.socket_recv_buffer_bytes`); the idle-connection timeout is configurable (`network.idle_connection_timeout_seconds`, reap interval now adaptive). New Prometheus counters `kawasan_connections_created_total` and `kawasan_connections_closed_total{reason=normal|idle_timeout|protocol_error|io_error}`; the TCP server is now actually wired to the metrics collector (bytes/connection gauges previously never updated).

- **Log truncation + manual leader election (M7 core).** Three pieces toward safe failover. (1) `Log::truncateSuffix(offset)` (with `LogSegment::truncateTo`) removes a log's tail — dropping whole trailing segments and truncating the segment that holds the boundary, clamping the high watermark down, surviving reopen — the corruption-risk primitive covered by an exhaustive unit suite (`TruncateSuffixTest`). (2) Follower divergence resolution: a follower whose replica-fetch is rejected with `OFFSET_OUT_OF_RANGE` truncates its **un-committed** tail down to its own high watermark (never discarding acknowledged data — everything ≤ HW was ISR-committed) and re-syncs, so a divergent follower reconciles instead of stalling. (3) Real `ElectLeaders`: preferred-replica election backed by a new Raft-replicated `UPDATE_LEADER` metadata command that sets the partition leader (only to an assigned replica) and bumps `leader_epoch` (KIP-101), unit-tested by `MetadataLeaderElectionTest` (election, epoch bump, invalid-replica/unknown-topic rejection, persistence across reload). M8 adds the full KIP-101 epoch cache and the cluster-level divergence-truncation proof; the original M7 path used high-watermark truncation.
- **Controller-authoritative ISR shrink/expand (M6).** A dead or lagging follower no longer blocks `acks=all` forever. The partition leader tracks each follower's fetch progress and, when a follower's last fetch is older than `replica.lag.time.max.ms`, proposes a shrunk ISR; it commits the change through the controller — directly via the Raft `UPDATE_ISR` path when the leader *is* the controller, otherwise by sending an **AlterPartition RPC** (`PeerClient::alterPartition`) to the controller, whose now-real `handleAlterPartition` validates the requester is the current leader with a matching leader epoch (`FENCED_LEADER_EPOCH` otherwise) and commits. `reconcileReplicas` propagates the committed ISR to every broker, so the leader's high-watermark computation stops waiting on the removed follower and `acks=all` proceeds with the surviving in-sync replicas (subject to `min.insync.replicas`). A follower that resumes fetching and catches up to the high watermark is re-added. Verified on a live 3-broker cluster (`scripts/tests/test_isr_shrink_m6.py`: kill a follower of a partition led by a *non-controller* broker → the AlterPartition RPC shrinks the ISR and `acks=all` stays available). The cluster harness (`scripts/tests/cluster_harness.sh`) gained `kill`/`restart` subcommands and `LAG_MS`/`MIN_ISR` knobs. Single-node is unaffected (the ISR-maintenance pass runs only on the multi-broker replica-fetcher thread). M8 adds automatic failover and broker liveness.
- **Followers actually replicate data (M5).** Multi-broker replication now has a working data plane, not just a metadata plane. In multi-broker mode (i.e. when `raft.peers` lists peers) each broker runs a follower-fetcher thread that reconciles its replica assignments from the Raft-committed metadata (`KawasanBroker::reconcileReplicas`) and, for every partition it follows, pulls records from the partition leader over a new `PeerClient` — a minimal broker-to-broker Kafka client that issues a Fetch v4 with `replica_id` = its own broker id (which the M4 leader path serves up to the log-end-offset). Fetched batches are appended by a new offset-preserving `Log::appendReplicatedBatch`, which keeps the leader's base offsets exactly (validating strict contiguity — a gap is refused, a duplicate skipped — rather than relabeling like `appendBatch`) and does not advance the high watermark; the follower instead adopts the leader's reported high watermark, clamped to what it holds. Because the leader (M4) advances its high watermark from the ISR, `acks=all` on an RF>1 topic now genuinely waits for the followers, and a successful `acks=all` produce is proof the followers replicated. Verified end-to-end on a live 3-broker cluster (`scripts/tests/test_replication_m5.py`: RF=3 topic, 50 records committed with `acks=all`, full round-trip consume) plus a deterministic unit test for the append primitive (`ReplicatedAppendTest`). Single-node is byte-identical: the fetcher thread and metadata reconciliation only run when peers are configured. M6–M8 add ISR shrink, failover and epoch reconciliation. Randomized-fault safety verification remains M9; multi-broker stays experimental.
- **Admin surface truthing (CM-5).** `DescribeLogDirs` now reports the real per-partition on-disk size (`Log::sizeBytes()`, sum of segment sizes) and offset lag (`LEO - HW`, `0` on a caught-up single-node leader), groups partitions under one entry per topic, and honors the request's topic/partition filter instead of always dumping every log with zeroed sizes. `CreatePartitions` validates up front (unknown topic → `UNKNOWN_TOPIC_OR_PARTITION`, non-increase → `INVALID_PARTITIONS`, mismatched explicit assignments → `INVALID_REPLICA_ASSIGNMENT`) and honors `validate_only` as a true dry-run (validate without applying). `Metadata`/`DescribeCluster` now report the real controller id (the current Raft leader via `raft_node_->leaderId()`, falling back to self when no leader is known) instead of hardcoding this broker. The admin request/response types gained the client-side setters/getters (`DescribeLogDirsRequest::{setFetchAll,addTopic}`, `CreatePartitionsRequest::{addTopic,setValidateOnly,setTimeoutMs}`, `*Response::{logDirs,results}`) needed to build and read these messages. New `tests/integration/admin_surface_test.cpp`.
- **Leader-side replica-fetch protocol (M4).** The leader now speaks the replica-fetch protocol so the high watermark is governed by the ISR rather than by local append: a Fetch with `replica_id >= 0` is treated as a follower broker — the leader records the follower's log-end-offset (`ReplicaManager::updateFollowerFetchOffset`), recomputes the high watermark as `min(leader LEO, slowest in-sync follower)` (`maybeAdvanceHighWatermark`), does not long-poll, and lets the follower read up to the log-end-offset; a consumer (`replica_id == -1`) reads only up to the high watermark, so a replicated partition's un-committed tail is invisible until the ISR has it. `Log::appendBatch` gained an `advance_high_watermark` flag (default true) so an RF>1 leader appends without force-advancing HW=LEO, and `acks=all` is rejected up front with `NOT_ENOUGH_REPLICAS` when the ISR is smaller than `min.insync.replicas` (now enforced at write time). `ReplicaManager` learns this broker's real `broker.id` via `setLocalBrokerId`. Single-node partitions stay byte-identical: with an ISR of `{self}` the high watermark equals the log-end-offset, the raw fetch fast path is unchanged, and `min.insync.replicas=1` never trips the gate. This is the leader half of replication; the follower half (actually fetching from a peer) lands in M5. New `tests/integration/replica_fetch_test.cpp` (fake-follower over a socket: consumer HW-bounding, follower-driven HW advance, `NOT_ENOUGH_REPLICAS`).
- **Producer-state snapshots for bounded startup replay (M3).** Idempotent-producer dedup/epoch state is now checkpointed to per-partition snapshot files (`producer-<offset>.psnap`, written atomically via temp + rename with a CRC-32C frame, pruned to the newest two) by a periodic writer thread (`producer.state.snapshot.interval.ms`, default 60s) and once more on graceful shutdown. On restart the broker loads the newest *valid* snapshot per partition and replays only the log tail after its offset instead of the whole log — turning restart from `O(log-size)` to `O(tail)`. A torn/corrupt snapshot fails its CRC check and is skipped (falling back to the next-older snapshot, then to a full replay from offset 0, Kafka's no-snapshot behavior), so a bad checkpoint can never poison or truncate dedup state. The snapshot offset is captured *before* the entries so it never runs ahead of the state it encodes; replay is idempotent, so a batch landing between the two reads is merely re-applied. New `ProducerStateSnapshot` helper and `ProducerStateManager::{snapshotEntries,restoreEntries}`; tests extend `IdempotentReplayTest` (snapshot+tail replay equals full replay, the snapshot is load-bearing, corrupt-snapshot fallback, serialize round-trip + CRC rejection).
- **Producer-epoch fencing + transaction-timeout auto-abort (M2).** `InitProducerId` for an existing `transactional_id` now reuses the producer_id and bumps the epoch (auto-aborting any prior in-flight transaction at the old epoch), fencing the earlier producer instance; `AddPartitionsToTxn`, `AddOffsetsToTxn`, `TxnOffsetCommit`, and `EndTxn` reject a stale-epoch (zombie) producer with `INVALID_PRODUCER_EPOCH`, and `EndTxn` returns `INVALID_TXN_STATE` for a non-Ongoing transaction. A background sweep (`transaction.abort.timed.out.transaction.cleanup.interval.ms`, default 10s) auto-aborts Ongoing transactions past their `transaction.timeout.ms`, so a hung producer never blocks read_committed consumers. Verified end-to-end with librdkafka (a second producer fences the first with `_FENCED`; a hung transaction is auto-aborted). The coordinator gained an injectable clock for deterministic timeout tests.
- **Durable single-node transactions (M1).** Transaction-coordinator state is now persisted to the `__transaction_state` internal topic and replayed on startup, so a crash mid-transaction resolves deterministically instead of losing state. EndTxn is genuinely two-phase — the `PrepareCommit`/`PrepareAbort` snapshot is fsynced *before* control markers are emitted and the terminal `CompleteCommit`/`CompleteAbort` snapshot *after* — and startup replay rebuilds the coordinator and the read_committed isolation tracker (in-flight transactions re-hold their LSO at the persisted `first_offset`) and re-drives any transaction caught mid-EndTxn to completion (with an idempotency guard so a marker is never written twice). Transaction-state writes fsync regardless of the broker's `log.durability` (a new `Log::append(records, force_sync=true)` path), because transaction state must survive a power loss even under `async` data durability. New `TransactionStateManager`; tests: `TransactionStateManagerTest`, `TransactionStateRecoveryTest`.
- **Benchmark methodology overhaul.** `bench/` now has a librdkafka (confluent-kafka) load path alongside kafka-python, a consume benchmark (`consume_perf.py`), a produce/consume concurrency sweep (`run_suite.sh`), and an environment fingerprint (CPU/platform/git-sha) in every result. `bench/baseline.json` is re-recorded from real measurements (single-node, async durability: ~100 MB/s / ~102k msg/s produce, ~774 MB/s consume on an M3 Pro) with explicit caveats; `check_regression.py` gains an A/B same-runner mode (`--base`/`--head`) and scenario-keyed JSONL comparison.

### Changed

- **The high-watermark checkpoint is periodic, not per append.** `Log` used to rewrite and fsync `checkpoint.meta` (temp + fsync + rename) on every append, under the partition lock. It now marks the checkpoint dirty, and `LogManager` flushes it every `replica.high.watermark.checkpoint.interval.ms` (default 5000, as in Kafka), on stop/close, and synchronously for `force_sync` internal writes. The microbenchmark showed an async append going from 160 µs to 7.6 µs and a sync append from 5.4 ms to 4.2 ms. A single-node broker recovers each log's HW to its log end at open, so records acked before a crash stay visible (verified with SIGKILL and restart).
- **Long-poll Fetches are parked off the network IO threads (DelayedFetch purgatory).** Previously each waiting Fetch busy-waited (1 ms sleeps) on an IO thread for up to `max_wait_ms`. A few idle consumers could starve every other request: with `network.io_threads=2`, produce p50 rose from 5 ms to about 1.8 s with 8 idle consumers. Waiting fetches now sit in a `DelayedOperationPurgatory` and are woken by partition appends or high-watermark changes, or by `max_wait_ms` expiry. With 32 idle consumers on 2 IO threads, produce p50 is now about 5 ms. New key: `fetch.purgatory.threads` (default 2).
- **`acks=all` produces waiting on the ISR are parked off the network IO threads (DelayedProduce purgatory).** On a replicated partition the produce used to busy-wait (1 ms sleeps, taking the `ReplicaManager` lock each time) on its IO thread until the followers caught up or the request timed out, blocking every other connection on that thread. It now sits in a second `DelayedOperationPurgatory`. It is woken by high-watermark changes and leader/ISR reconciliation, and answers `REQUEST_TIMED_OUT` at the producer's timeout (capped at 30 s) or `NOT_LEADER_FOR_PARTITION` if this broker lost leadership. An ISR shrink now advances the leader's high watermark immediately (Kafka semantics), so records the dropped follower was holding back become committed and visible without waiting for the next produce. Single-node partitions (ISR = {self}) never wait and are unchanged.
- **`AddOffsetsToTxn`** now registers the group's `__consumer_offsets` partition into the transaction (previously a no-op returning `NONE`), so `TxnOffsetCommit` offsets participate in the transaction.
- The durable high-watermark checkpoint is written atomically (temp file + fsync + rename).
- `acks=all` waits (leader-side, with timeout) for the ISR-committed offset before acknowledging.

### Fixed

- Consumer groups reject a leader assignment that omits members admitted while SyncGroup was pending, then rejoin with a fresh generation. Same-subscription rejoins preserve stable generations; stale assignments are cleared when membership changes. Two consumers bootstrapped at different brokers now share one group and disjoint partitions.

- Broker destruction keeps its Asio context alive until Raft transports and peer sockets are destroyed, preventing a crash on clean cluster shutdown.

- Raft elections serialize timer and term changes, reject obsolete vote replies, and collect heartbeats independently per peer. A frozen peer no longer stretches healthy-peer heartbeat timing or queues repeated AppendEntries. ISR maintenance waits for current metadata after a broker resumes.

- **Raft could fail to elect a leader, or lose it repeatedly, while one peer was frozen.** Four bugs, each found with the M8 SIGSTOP nemesis:
  - Elections and heartbeats waited on peer responses in list order under one shared 100 ms budget. A frozen peer early in the list used up the budget and hid a live peer's granted vote or ack, so two live nodes out of three never elected a leader.
  - Outgoing RPCs shared one client pool, so a backlog to a frozen peer starved RPCs to the others. Each peer now has its own RPC thread.
  - Reconnecting to a peer used an unbounded `connect()` while holding the transport-wide lock. A frozen peer's full listen backlog made it hang and stall every RPC. Connects are now bounded and done outside the lock, and the broker-to-broker client gets the same bounded connect.
  - A follower that had been cut off counted its metadata as current after one heartbeat, before applying what it missed. It must now catch up again after any gap over 1 s in leader contact, or after stepping down.
- **Idempotent producers could lose acknowledged writes after a leader failover (multi-broker).** Producer ids came from a per-broker counter, so two brokers issued the same ids. After a failover, a new producer could get an id that the new leader's idempotence state (rebuilt from the replicated log) already knew. Its first batches were then treated as retries, acked, and never written. In a cluster each broker now issues ids from its own range: broker id in the high 32 bits, local sequence in the low 32. Single-node ids are unchanged. The id counter is now persisted crash-atomically (temp file + fsync + rename), so a power loss can't re-issue ids even on a single node. **Upgrade note (multi-broker, experimental):** producers holding ids issued before this change keep working, but recreate the cluster for a clean id space.
- **A frozen partition leader could stall replication and controller failover.** The broker-to-broker client (`PeerClient`: follower fetch, AlterPartition) used unbounded blocking reads on the replica-fetcher thread. That thread also runs reconciliation and the controller's failover sweep, so one SIGSTOPped or black-holed peer stopped all three. Every PeerClient request now has a deadline: 2 s for fetch, 10 s for AlterPartition. The poll-based helper is shared with the Raft transport.
- **Raft could wedge permanently after a peer stalled, or when two nodes called each other at once.** Outgoing Raft RPCs ran as blocking reads with no timeout on the broker's single Raft io thread, which also serves incoming RPCs. One frozen peer (GC pause, SIGSTOP, network black hole) wedged the thread forever, and two candidates exchanging RequestVotes could deadlock. Either way, elections never converged again. RPCs now run on a private client pool with per-RPC deadlines. Relatedly, a Raft node that stopped with committed but unapplied entries after its state machine had detached could hang shutdown. It now exits, and those entries are re-delivered on the next start.
- **Restart replayed every metadata command, which could delete a topic's data (single-node too).** Raft doesn't persist its commit index, so after a restart the first metadata change re-committed the whole Raft log and re-applied it on top of the already up-to-date metadata file. A topic that had once been deleted and re-created got its partition data deleted by the replayed `DELETE_TOPIC`. Every past leader election also bumped `leader_epoch` again. The metadata store now persists the last applied Raft index together with its state, and the controller skips entries at or below it. A wiped Raft log resets the index. The metadata file is also now written crash-atomically (temp file + fsync + rename), where previously it was truncated and rewritten in place, so a crash mid-write could leave a broker unable to start.
- **RF=1 topics had a different leader on every broker in a cluster.** A `CREATE_TOPIC` or `INCREASE_PARTITIONS` command with replication factor 1 was assigned to whichever broker applied it. Every broker therefore recorded itself as the leader of every RF=1 partition, including all of `__consumer_offsets` and `__transaction_state`. Assignment is now a deterministic round-robin over the sorted broker ids, and in a cluster the controller writes it into the replicated command. Single-node is unchanged. **Upgrade note (multi-broker only, which is experimental):** existing cluster metadata keeps the old per-broker views, so recreate the cluster's metadata (fresh data directories) to get consistent RF=1 and internal-topic leadership. A Metadata or Produce request can also no longer auto-create `__consumer_offsets` or `__transaction_state` with the wrong partition count; until the controller creates them, it gets the retriable `LEADER_NOT_AVAILABLE`.
- **Fetch sessions leaked on every librdkafka Fetch.** librdkafka sends `session_id=0, epoch=-1` (Kafka's legacy sessionless full fetch) on each Fetch v7+, but the broker treated that as "allocate a new session" and never reaped sessions. One consumer leaked about 10 sessions per second and logged each one at `info`. `0/-1` is now sessionless, as in Kafka. Sessions that are still allocated are reaped after 120 s idle, which `kawasan_fetch_session_evictions_total` now reflects.
- **Producer byte-rate quotas never engaged over TCP.** The request size handed to handlers was read after the frame buffer had been moved out, so every request counted as 0 bytes. `quota.producer.default` was inert, and request-size metrics read 0. Both now use the real frame size.
- **Per-request `info` log on the network hot path.** Each request logged its API key and correlation id at `info`. That message is now `debug`.
- **A partition's data could be silently wiped on open failure.** `LogManager::getOrCreateLog` used to `remove_all` the partition directory and start an empty log after *any* open error. That included environmental errors such as a RocksDB `LOCK` held by another process, fd exhaustion, permissions, or a full disk. Environmental errors now propagate with the data left untouched. Other failures, treated as corruption, rename the directory to `<topic>-<partition>.corrupt-<epoch_ms>` for manual recovery before an empty log is created.
- **Idle broker pinned a full CPU core.** When the node was the Raft leader (always true single-node), `RaftNode::electionThread` computed a zero wait from the stale `last_heartbeat_` and spun. A leader now sleeps a full election-timeout period, and idle CPU dropped from 100% to about 0%.
- **Fetch `throttle_time_ms` reported the long-poll wait time.** KIP-219 clients treat that field as a quota throttle and muted the connection for that long. It now carries only quota throttling.
- **Broker shutdown could hang indefinitely.** `MetadataController::stop()` failed only the waiters already registered, so a `replicateAndAwait` that registered after `stop()` blocked forever, and `KawasanBroker::stop()` hung joining the controller-bootstrap thread. Waiters now check a stopped flag and are bounded at 30 s (`REQUEST_TIMED_OUT`), so a lost quorum also can't pin a request thread. `cluster_harness.sh down` now waits for brokers to exit, which removes the `Address already in use` flakes between back-to-back cluster tests.
- **Build against RocksDB 11+.** RocksDB 11 removed the raw-pointer `DB::Open` overload; all call sites now go through `kawasan::openRocksDb` (`include/kawasan/common/rocksdb_compat.h`), which picks whichever overload the installed RocksDB provides (works with both Homebrew RocksDB 11 and older vcpkg baselines).
- **Compressed topics were unreadable by strict clients (e.g. librdkafka).** When re-serializing a produced batch for storage, `RecordBatch::encodeRecords()` wrote the records payload **uncompressed** while the batch header still advertised the client's codec (snappy/lz4/zstd/gzip), so a consumer read **zero** records from any compressed topic (uncompressed worked). `encodeRecords()` now compresses the payload to match the codec bits; verified end-to-end (librdkafka produce+consume, all five codecs) and by `RecordBatchCompressionTest`.
- **Unbounded-allocation DoS in request decoders (found by fuzzing).** Produce/Fetch/Metadata (and the forgotten-topics/partition arrays) `reserve()`/`resize()`-ed to an attacker-declared array count before reading any elements, so a ~10-byte packet claiming ~2³¹ entries drove a multi-gigabyte allocation. A new `Buffer::readArrayLength(flexible)` rejects any count larger than the bytes left in the frame (each element needs ≥1 wire byte). New libFuzzer targets cover the Produce/Fetch/Metadata request parsers and all five compression codecs; `DecodeBoundsTest` locks the guard.
- **Wire format of DescribeProducers (61), DescribeTransactions (65), ListTransactions (66), and AlterPartition (56)** now uses flexible encoding (compact strings/arrays + tagged fields) as the Kafka spec requires for these v0-flexible APIs — previously the bodies were non-flexible while the headers were flexible, so Java AdminClient/kafka-ui would fail to parse them. `DescribeTransactions` additionally gained the missing v0 schema fields (`transaction_start_time_ms`, per-transaction topic/partition list, INT16 producer epoch) and now populates real coordinator data; `AlterPartition` gained its full v0 request/response codec (previously decoded nothing). Golden-bytes tests (`AdminStubsWireTest`) lock the layouts.
- **Compaction can no longer touch the active segment**, structurally: `LogSegment::deleteBatchAt` refuses destructive ops while its segment is active (previously the only protection was the shape of `Log::cleanup()`'s loop bound), guarded by a concurrent compaction-vs-produce stress test.
- **`ListOffsets` MAX_TIMESTAMP (-3) excludes control batches** — transaction COMMIT/ABORT markers carry wall-clock timestamps and previously could win the scan, pointing consumers at a marker instead of a data record.

### Known Limitations

- **Client/broker TLS is not implemented** and **Raft inter-broker traffic is always plaintext** (`raft.ssl.*` is parsed/validated but not wired into the transport) — unchanged from 0.2.0-alpha.
- **Multi-broker remains experimental.** M8 implements automatic failover, stale-leader fencing, epoch reconciliation and coordinator routing. M9 must validate randomized faults before failover is advertised for production. Internal topics remain RF=1, so coordinator-broker loss makes its groups and transactional IDs unavailable until it returns; durable coordinator failover and cross-broker EOS are M10.
- **Multi-broker transactions are incomplete.** Single-node transaction state is durable (M1), but internal topics remain RF=1, the coordinator does not fail over between brokers and `WRITE_TXN_MARKERS` (API 27) is unimplemented (M10).

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
- Follower-fetch background thread scaffold (the record-fetch step was a stub then; real follower fetch landed in Unreleased, M5); metadata consistency check tool (`tools/kawasan-metadata-check`); cluster bootstrap script (`scripts/start_cluster.sh`).

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

[Unreleased]: https://github.com/kawasan/kawasan/compare/v0.2.0-alpha...HEAD
[0.2.0-alpha]: https://github.com/kawasan/kawasan/releases/tag/v0.2.0-alpha
[0.1.0-alpha]: https://github.com/kawasan/kawasan/releases/tag/v0.1.0-alpha
