# Replicated coordinator state and the experimental-cluster upgrade boundary

Status: accepted design for M10; strict committed-partition transaction replay
primitive implemented. Clustered format, acquisition integration and acceptance
tests remain pending.

M10 will make committed `__consumer_offsets` and `__transaction_state` log
records authoritative in clustered mode. Each new owner must rebuild only its
owned coordinator partitions from records below the high watermark before it
serves requests. RocksDB is then a disposable local cache. RF is
`min(3, configured cluster size)`; acknowledgements require the internal-topic
ISR commit path, local fsync, and a still-current ownership epoch.

Existing clustered data cannot safely be upgraded by replaying today's internal
logs. OffsetManager stores offsets, group membership and deletions in a separate
RocksDB database. The offset records appended by `handleOffsetCommit` are a
best-effort observability mirror, without complete group/tombstone history.
TransactionStateManager reads to LEO and appends through `Log::append`, which
advances HW locally. Raising internal-topic RF alone would therefore create
false durability and potentially rewind consumer offsets. Historical design
references to a metadata observer are also inaccurate: reconciliation is polled
by the replica fetcher.

The first M10 clustered format requires a fresh cluster. It will **not perform
an automatic in-place conversion or silently interpret a legacy cluster as an
empty coordinator cache**. This is a deliberate breaking boundary for the
experimental clustered deployment, not for existing single-node installations.
Single-node deployments retain their existing persistence and restart behavior.

Implementation requirements before enabling the new clustered format:

- Write a durable, versioned coordinator-format manifest tied to cluster ID,
  routing algorithm and internal-topic partition counts. All brokers must agree;
  refuse unsupported formats, mixed modes or changed routing/counts.
- Detect legacy clustered coordinator storage before replay or mutation and fail
  startup with an actionable migration error. Do not delete, rename or reset it.
  Tests must cover legacy offsets, group state, txn state, and partial manifests.
- An unformatted joining broker with empty local state may join only after it
  obtains the committed cluster-format declaration; missing local state on a
  formatted broker is a cache rebuild, never a reason to bootstrap an empty log.
- On acquisition, clear stale cache entries for the partition and replay the
  committed prefix, including group state, offsets, transactional offsets and
  deletion tombstones. Serve COORDINATOR_LOAD_IN_PROGRESS until replay completes;
  fence immediately on ownership loss. Redrive durable Prepare states afterward.
- Persist internal state before updating the cache or acknowledging clients.
  Propagate append/fsync/ISR failures; today's swallowed persist exceptions are
  incompatible with the new source-of-truth contract.
- Keep `coordinatorPartitionFor` placement for this format. Java-compatible
  positive-hash routing is a separate migration, not an incidental M10 change.

Operators of an existing experimental cluster must quiesce producers/consumers,
resolve transactions, retain backups of every broker's data and metadata, and
plan a separate fresh-cluster cutover. User-topic data and required committed
offsets need an explicit, verified external transfer; the broker will not claim
a supported converter until one is built and tested. Do not delete the old
cluster as part of startup. A deployment that requires an in-place upgrade must
remain on its existing version pending that converter.

Rejected for the first implementation: online dual writes (no atomic boundary
between today's RocksDB state and the replicated log), a best-effort startup
copy (missing historical group/deletion/transaction information), and silently
reusing RF=1 internal topics (cannot provide coordinator failover durability).

M10 remains gated on CM-1, CM-3 and CM-4 and must prove offset continuity,
coordinator-kill recovery, cross-broker marker retries and transaction-mode chaos.
The M9 seven-scheduled-nightly gate stays separate and clustering remains
experimental while either acceptance gate is open.
