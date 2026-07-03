#!/usr/bin/env bash
# P1: run the full benchmark matrix against a running broker and emit one
# JSON line per scenario to stdout (JSONL). Covers produce + consume across
# a concurrency sweep. Each scenario is a fresh topic.
#
# Usage:
#   bench/run_suite.sh [HOST] [CLIENT] [RECORDS]
# Defaults: localhost:9092  confluent  200000
set -euo pipefail

HOST="${1:-localhost:9092}"
CLIENT="${2:-confluent}"
RECORDS="${3:-200000}"
HERE="$(cd "$(dirname "$0")" && pwd)"
STAMP="$(date +%s)"

for procs in 1 8 64; do
    topic="bench-p-${procs}-${STAMP}"
    python3 "${HERE}/produce_perf.py" \
        --records "${RECORDS}" --record-size 1024 --producers "${procs}" \
        --client "${CLIENT}" --host "${HOST}" --topic "${topic}"
done

# Consume sweep reuses a freshly-seeded topic per consumer count.
for cons in 1 8; do
    topic="bench-c-${cons}-${STAMP}"
    python3 "${HERE}/consume_perf.py" \
        --records "${RECORDS}" --record-size 1024 --consumers "${cons}" \
        --client "${CLIENT}" --host "${HOST}" --topic "${topic}" --prepopulate
done
