# Contributing to Kawasan

This guide covers how to set up a development environment, the coding and commit conventions, the pull-request workflow, and how to run the test suites. For the canonical build/configure/run commands see [../CLAUDE.md](../CLAUDE.md) and [../README.md](../README.md); this document summarizes and links rather than duplicating them.

## Contents

- [Development setup](#development-setup)
- [Coding style and naming](#coding-style-and-naming)
- [Commit messages](#commit-messages)
- [Pull-request workflow](#pull-request-workflow)
- [Running the tests](#running-the-tests)
- [Areas for contribution](#areas-for-contribution)
- [Licensing](#licensing)

## Development setup

Prerequisites:

| Tool | Minimum |
|------|---------|
| C++ compiler | GCC 11+, Clang 13+, or MSVC 19.29+ (C++20) |
| CMake | 3.20+ |
| Dependencies | vcpkg (`vcpkg.json`) or system packages: Boost, RocksDB, spdlog, nlohmann/json, GTest, OpenSSL, zlib, snappy, lz4, zstd |
| Git | any recent |

Configure, build, run a broker, and run tests using the commands in [../CLAUDE.md](../CLAUDE.md) (Build Commands and CMake Options). In short: `cmake -S . -B build`, `cmake --build build -j`, then `ctest --test-dir build`. The toolchain enforces `-std=c++20 -Wall -Wextra -Wpedantic -Werror`, so a clean build must be warning-free.

Relevant CMake options (full table in [../CLAUDE.md](../CLAUDE.md)): `KAWASAN_BUILD_TESTS`, `KAWASAN_BUILD_TOOLS`, `KAWASAN_BUILD_EXAMPLES`, `KAWASAN_ENABLE_ASAN`, `KAWASAN_ENABLE_TSAN`, and `KAWASAN_BUILD_FUZZ` (libFuzzer harnesses, requires Clang).

Configuration files live under `config/`. The loader (`src/common/config.cpp`) auto-detects and accepts **both** JSON objects (e.g. `config/broker.dev.properties`) and Kafka-style `key=value` text (e.g. `config/broker-0.properties`); both support `${VAR}` / `${VAR:default}` environment-variable substitution. See [./CONFIGURATION.md](./CONFIGURATION.md) for the full key reference.

## Coding style and naming

Format every file before committing with `clang-format -i <file>` (Google-based `.clang-format`, 4-space indent, 100-column limit, `Attach` braces). `.clang-tidy` enforces the naming rules below; the full rule set is in [../CLAUDE.md](../CLAUDE.md).

| Element | Convention | Example |
|---------|-----------|---------|
| Namespaces | `lower_case` | `kawasan::storage` |
| Classes / Structs | `CamelCase` | `LogSegment` |
| Functions | `camelBack` | `getMetadata()` |
| Variables | `lower_case` | `partition_id` |
| Constants | `UPPER_CASE` | `MAX_BATCH_SIZE` |
| Member variables | `lower_case_` (trailing underscore) | `base_offset_` |

Additional expectations:

- Public headers live under `include/kawasan/<module>/`; sources under `src/<module>/`. Keep new code inside the existing nine-module layering (see [./ARCHITECTURE.md](./ARCHITECTURE.md)) — do not introduce upward dependencies (e.g. `common` must not depend on `broker`).
- Use RAII and smart pointers; prefer `const`/`constexpr`; avoid raw owning pointers.
- Document public APIs with Doxygen-style comments.
- Add tests for new functionality (see [Running the tests](#running-the-tests)).

## Commit messages

Use [Conventional Commits](https://www.conventionalcommits.org/): `<type>: <subject>`, with an optional body and footer.

```
feat: add cooperative-sticky assignment to group coordinator

Implements the protocol path for incremental rebalances and wires it
into JoinGroup/SyncGroup handling.

Refs #123
```

| Type | Use for |
|------|---------|
| `feat` | New feature |
| `fix` | Bug fix |
| `docs` | Documentation only |
| `style` | Formatting / non-behavioral style |
| `refactor` | Code restructure with no behavior change |
| `perf` | Performance improvement |
| `test` | Adding or updating tests |
| `chore` | Build, tooling, maintenance |

Keep the subject in the imperative mood and reference issue numbers in the body or footer where applicable.

## Pull-request workflow

1. **Branch** off the default branch with a descriptive name:
   - `feature/<short-description>` — new functionality
   - `fix/<issue-or-description>` — bug fixes
   - `docs/<what-changed>` — documentation
2. **Implement** the change with tests and, where relevant, doc updates.
3. **Format**: run `clang-format -i` on every changed source/header.
4. **Build clean**: `cmake --build build` must succeed with `-Werror`.
5. **Test**: run the relevant suites (below); a green `ctest --test-dir build` is the baseline bar.
6. **Open the PR** with a clear description of the what and why, linked issues, and a note on which suites you ran. Ensure CI passes.

Keep PRs focused — one logical change per PR makes review and bisection tractable. Do not commit or push on a contributor's behalf without their explicit request; if you must commit on the default branch, branch first.

## Running the tests

The C++ tests are GTest binaries registered with CTest; the broader compatibility and ecosystem suites are script-driven against a running broker. For operational/runtime context that some of these harnesses depend on (broker startup, ports, config), see [./OPERATIONS.md](./OPERATIONS.md).

### C++ unit, integration, and benchmark tests

```bash
ctest --test-dir build --output-on-failure          # all suites
ctest --test-dir build -R KawasanBrokerErrorTest     # one suite by name
./build/tests/unit/kawasan_broker_error_test         # one binary directly
```

| Location | Contents |
|----------|----------|
| `tests/unit/` | One GTest binary per file (suffix `_test.cpp`), each registered via `add_test`. Includes `BufferTest`, `RecordBatchTest`, `KawasanBrokerErrorTest`, `LogSegmentTest`, `RaftProtocolTest`, `RaftTransportTest`, `ISRManagementTest`, `StreamsTopologyTest`, and more. |
| `tests/integration/` | End-to-end broker paths: produce/fetch, offset persistence, log recovery, replication, transactional recovery, Streams, Connect, shutdown. |
| `tests/benchmark/` | Throughput, load, replication, and Streams performance benchmarks. |
| `tests/fuzz/` | libFuzzer harnesses for wire decoding (`fuzz_request_header`, `fuzz_record_batch`); build with `-DKAWASAN_BUILD_FUZZ=ON` using Clang. |

Place a new unit test in `tests/unit/` as `<thing>_test.cpp` and register it in the relevant `CMakeLists.txt`; integration tests go in `tests/integration/`.

### Client-compatibility and ecosystem suites

These run against a live broker and validate the Kafka drop-in contract:

| Suite | What it does | How to run |
|-------|--------------|------------|
| `tests/compatibility/` | Client-library round-trips (kafka-python implemented; KafkaJS, Sarama, Java client present). | `scripts/run_compatibility_tests.sh` starts a broker and drives the clients. |
| `tests/ecosystem/` | Drop-in contract harness: each check runs against **both** Apache Kafka (oracle) and Kawasan (candidate) via Docker Compose, covering api-compat, idempotent/transactional produce, compaction, ACLs, SASL, Schema Registry, Kafka UI, Kafka Connect, ksqlDB, Streams EOS, etc. | `tests/ecosystem/scripts/run_all.sh kafka` for the oracle baseline, then `run_all.sh kawasan` for the candidate. See [Operations](./OPERATIONS.md) for harness details. |
| `scripts/tests/` | Ad-hoc Python integration scripts (kafka-python / librdkafka) for offset, ordering, consumer-group, and metadata behaviors against a running broker. | `python3 scripts/tests/<script>.py` with a broker up. |

## Areas for contribution

**Good first issues:** documentation, additional unit tests, code cleanup/refactoring, and well-scoped bug fixes.

**Larger efforts:** protocol-API coverage (see [./api_coverage_matrix.md](./api_coverage_matrix.md) for current support), the Streams and Connect surfaces, performance optimization, and security features. Multi-broker Raft replication exists but is **not** production-hardened — for example, `raft.ssl.*` keys are parsed and validated but are not wired into the transport (`src/raft/raft_transport.{cpp,h}` has no TLS), so inter-broker Raft traffic is plaintext even with `raft.ssl.enabled=true`. Hardening this path is a high-value area. The single-node broker is the primary, production-ready mode.

## Licensing

By contributing to Kawasan, you agree that your contributions are licensed under the **Apache License 2.0** (see [../LICENSE](../LICENSE)). Do not contribute code you do not have the right to license under these terms.
