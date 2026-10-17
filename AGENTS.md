# Working agreements

- After every meaningful change, make a commit with a concise, one-line message.
- Do not run `git push` unless the user explicitly asks for it.
- GitHub tokens are stored in `~/.secrets/` (`github_token`,
  `github_token_concise`, and `github_token_admin`). For authorized GitHub
  operations, use a token with the required scope via process-local `GH_TOKEN`;
  never display token contents or commit credentials.

# Lessons from CI failures

These notes capture avoidable mistakes, not limits on future development.
Features, experiments, test organization, and benchmark methods may evolve.
When changing an assumption below, explain the reason and validate the new
behavior; the current implementation and numeric settings are not frozen.

- **Budget for the work a CTest entry actually runs.** The former cluster
  persistence entry ran 19 cases that each started and stopped three durable
  brokers. On macOS, passing cases cumulatively exhausted its 120-second limit.
  Prefer individual case registration for these expensive cases, with bounded
  timeouts. When adding or regrouping cases, account for startup, shutdown, and
  synchronous disk writes on CI runners, not just local execution speed.
- **Preserve coverage and account for disk contention.** When changing filters
  or discovery, verify that every intended case is registered exactly once.
  Keep disk-heavy suites isolated where concurrent execution distorts timing
  (`RUN_SERIAL` is used for cluster persistence). Validate the affected CTest
  entries; avoid treating a timeout alone as evidence that an assertion failed.
- **Distinguish benchmark uncertainty from confirmed regressions.** Alternating
  base/head runs on the same runner still contain noise. Retain raw paired
  samples and inspect their variability when a median barely crosses the
  threshold. The current gate uses nine pairs, a 10% threshold, and a one-sided
  95% paired-bootstrap confidence bound. Changes to sampling or decision rules
  should demonstrate both tolerance of noisy borderline results and detection
  of sustained throughput and latency regressions, including an anomalous pair.
  Do not raise thresholds or discard samples merely to make a failure pass.
- **Match build requirements to new CMake features.** Filtered
  `gtest_discover_tests(TEST_FILTER ...)` requires CMake 3.22. Keep the declared
  minimum version consistent with features introduced in build/test changes.

Relevant files: `tests/integration/CMakeLists.txt`,
`.github/workflows/bench-regression.yml`, `bench/run_comparison.py`,
`bench/check_regression.py`, and `bench/test_bench.py`.
