# Client compatibility checks

The [client matrix workflow](../.github/workflows/client-matrix.yml) runs each
pinned Java, librdkafka and kafka-python profile against Kafka 4.2 first, then
against Kawasan. The profiles and required cases are defined in
[results.py](../tests/clients/results.py). Java uses a containerized Maven build;
local Maven is unnecessary. Both classic group membership and idempotence are
explicitly configured. Java tests cover AdminClient, ordered produce/consume,
commit/abort visibility, producer fencing, and transactional consumer offsets.
A raw ApiVersions probe checks modern hot-path maxima even when a client could
silently downgrade. The librdkafka profiles check acknowledged offsets, record
values, transaction visibility and transactional offset commits.

Run one profile from the repository root:

```bash
CLIENT_MATRIX_EVIDENCE=/tmp/kawasan-client-java42 \
  bash tests/clients/run.sh java-4.2.0
python3 -m unittest discover -s tests/clients -p test_results.py
```

Docker and Python 3 are required. Use an isolated `DOCKER_CONFIG` if your local
credential helper needs it. The runner uses the ecosystem compose definitions
and their published ports, so run profiles sequentially on one machine. CI
isolates each profile in its own runner. Broker volumes created by the harness
are removed on exit; every run retains startup logs, broker logs, client logs
and JSONL case results under the evidence directory. Compilation, startup,
missing tools and interrupted runs fail the leg.

[known_failures.txt](../tests/clients/known_failures.txt) is a tab-separated
allowlist with profile, case, observed error substring, owning milestone and
reason. It applies only to the candidate. The oracle must pass every case.
Unknown failures, changed errors, skipped/missing/duplicate cases and unexpected
passes fail the gate. Remove an entry when its case starts passing; never widen
a signature to hide a regression. A candidate run with known failures is not
an all-cases pass.

For the checker sensitivity proof, set `CM1_SEED_PRODUCE_BUG=1` on a Java
leg. It deliberately corrupts one produced value; the Kafka oracle must fail
the ordered value assertion and the runner must stop before the candidate.
Never use this setting for acceptance.

These checks prove client behavior against a single broker. Cluster acceptance
still requires the recovery harness and M9 scheduled nightlies; clustering
remains experimental.

Wire regression fixtures are emitted by Kafka 4.2's generated codecs in
[WireFixtures.java](../tests/clients/java/src/fixtures/java/WireFixtures.java),
independent of Kawasan. The generator uses the opt-in `wire-fixtures` Maven
profile, keeping Kafka 4.2 internal schema classes out of the Java 3.9 matrix
build. Fixtures include UUID, SASL, DeleteRecords and OffsetFetch shapes;
regressions are added before each corresponding correctness repair. Regenerate with:

```bash
bash tests/clients/java/generate-fixtures.sh tests/clients/fixtures/kafka-4.2-wire.json
ctest --test-dir build -R ApiVersionParityTest --output-on-failure
```

DescribeGroups v4/v5 responses include the nullable static-member instance ID
between member ID and client ID, as required by the
[Kafka schema](https://github.com/apache/kafka/blob/4.2.0/clients/src/main/resources/common/message/DescribeGroupsResponse.json).
This field must be encoded even for dynamic members (as null).

Fetch v13 and DeleteTopics v6 UUIDs occupy exactly 16 raw bytes, including
Fetch's forgotten-topic entries. Golden comparisons cover both request and
response codecs, and Fetch's reported sizes follow the encoded wire shape.
Fetch v13 is advertised in the default `4.x` profile after indexed UUID lookup,
UNKNOWN_TOPIC_ID handling and socket/profile gates.

SASL v2 and DeleteRecords v2 use compact bodies with nested tagged fields;
OffsetFetch v9 inserts nullable member ID and member epoch before group topics.
Golden tests cover these shapes independently of Kawasan's encoders, including
classic null membership. The default `4.x` profile advertises these versions. KIP-848 member references
are explicitly rejected; classic null/-1 references are supported.

The `3.x` profile is tested with pinned librdkafka 2.8.0 in an additional CI leg.
ApiVersions and dispatcher limits share one table; native socket checks cover
above-cap error replies, UUID record fetching and long-poll wakeup, ListOffsets
EARLIEST_LOCAL (-4), and classic OffsetFetch v9. The matrix retains broker debug
request logs so actual negotiated versions can be inspected alongside case
results. See [the profile settings](CONFIGURATION.md#protocol-compatibility).

Topic config round trips use replicated, persisted overrides. DescribeConfigs
reports Kafka source IDs (dynamic topic 1, static broker 4, default 5). Kafka-generated
v1/v4 response fixtures cover source encoding; socket tests cover validation,
restart, override removal and changes to already-open logs. The three-broker check
is `scripts/tests/test_topic_configs_cm3.py`, retaining controller-failover evidence
under its fresh `BASE` directory.

DescribeLogDirs and DescribeAcls support v1, the minimum supported by Java 4.2.
Kafka-generated fixtures cover filtered/all-log requests, sizes/lag and ACL
patterns. Socket tests verify advertised caps, real partition sizes and stored
literal ACL filtering. DescribeLogDirs bounds every array level before allocating.
ACL storage remains local and in memory until CM-9; this version change adds no
persistence or cluster authorization guarantee.

Transactional mutation APIs require a producer initialized with InitProducerId.
An unmapped ID or unknown/empty transaction returns INVALID_PRODUCER_ID_MAPPING;
an unequal epoch returns INVALID_PRODUCER_EPOCH. Socket regressions cover all
four APIs and confirm rejected requests preserve the valid transaction.
AddPartitionsToTxn, AddOffsetsToTxn, EndTxn and TxnOffsetCommit support v0–v3.

Kafka-generated transactional fixtures cover AddPartitionsToTxn, AddOffsetsToTxn,
EndTxn and TxnOffsetCommit v3. TxnOffsetCommit v2 inserts committed leader epoch
before metadata; v3 adds generation, member and nullable instance ID before topics.
Codec reuse resets those group fields and replaces topics. Transactional offset
commits validate classic group generation and member identity. Static instance
fencing precedes generation checks; current members can commit during rebalance.
Legacy commits without membership metadata remain supported. Socket regressions
prove rejected requests do not stage offsets, commit publishes the checkpoint,
and abort leaves it unchanged. Committed leader epoch survives transaction
snapshots and offset-cache restart; legacy records read with epoch -1.

Multi-broker transaction completion and coordinator replay remain experimental
M10 work. OffsetFetch supports null topics (fetch all committed checkpoints)
and returns UNSTABLE_OFFSET_COMMIT with unset offset/epoch/metadata when
`require_stable` encounters a pending transactional checkpoint. Abort restores
the previous stable checkpoint; completion publishes its offset and epoch.
Group IDs containing colons remain distinct during scans and deletion.

Kafka-generated header fixtures cover each transactional API at v2/v3, including
flexible request and response tagged fields. Java verification requires actual
v3 requests on API 24/25/26/28. The JVM leg has a 300-second execution limit;
interrupted or incomplete result files fail the matrix gate.
