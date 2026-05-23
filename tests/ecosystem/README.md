# Kawasan Ecosystem Compatibility Harness

This directory is the **contract** for "Kawasan is a drop-in replacement for
single-server Kafka". Every test runs against *both* brokers — Apache Kafka
4.2.0 as the behavioral oracle, and Kawasan as the candidate — so the diff
between PASS-on-Kafka and PASS-on-Kawasan is always the answer to "is this
a real bug or a test bug?".

Implements **Phase 0 Track B** of `docs/improv/improve-opus.md`.

## Layout

```
tests/ecosystem/
├── docker-compose.kafka.yml      # oracle: apache/kafka:4.2.0
├── docker-compose.kawasan.yml    # candidate: locally-built Kawasan image
├── scripts/                       # per-service smoke checks
│   ├── 01_apicompat.sh
│   ├── 02_producer_basic.sh
│   ├── ...
│   └── run_all.sh                 # orchestrator
├── golden/                        # captured Kafka 4.2 wire fixtures
└── apps/                          # heavier test apps (java-perf, streams, ...)
```

## Quick start

```sh
# Oracle baseline (should be all PASS on a working Docker host)
./scripts/run_all.sh kafka

# Candidate baseline (will be RED until Phase 4 — see baseline-failures.md)
./scripts/run_all.sh kawasan
```

The first run records its result into `docs/improv/baseline-failures.md` so
later regressions are observable. As Phases 1–4 land, the candidate column
flips from red to green; once the matrix is fully green the
`compat-smoke` CI job (allowed-to-fail today) becomes blocking.

## Status

The harness is intentionally **allowed to fail** today — the broker has
known gaps. The infrastructure is what matters now: every later PR can
look at this directory's status to know whether it moved compatibility
forward, backward, or sideways.
