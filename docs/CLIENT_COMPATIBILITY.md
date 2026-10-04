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
