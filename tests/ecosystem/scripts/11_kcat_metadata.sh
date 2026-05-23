#!/usr/bin/env bash
# 11_kcat_metadata.sh: librdkafka metadata querying.
# Phase 5.3 gauntlet: `kcat -L` exercises Metadata v12 (flexible) +
# DescribeCluster v0+ through librdkafka. Verifies that listing
# returns the broker and one or more topics in the right shape.
set -euo pipefail
TARGET="${1:-kawasan}"

if ! command -v kcat >/dev/null 2>&1; then
    echo "SKIP: kcat not installed"
    exit 0
fi

BROKERS="localhost:9092"

# Produce something so at least one user topic exists.
TOPIC="compat-11-meta-$$"
echo "x" | kcat -b "$BROKERS" -t "$TOPIC" -P -X message.timeout.ms=5000 2>/dev/null
sleep 1

# `kcat -L` does a Metadata query, which librdkafka uses for bootstrap
# and refresh paths. The output is human-readable but parseable.
out=$(kcat -b "$BROKERS" -L 2>&1)
echo "$out" | head -20

# Expect to see "1 brokers:" and our topic.
if ! echo "$out" | grep -q "1 brokers:"; then
    echo "FAIL: expected '1 brokers:' in kcat -L output"
    exit 1
fi
if ! echo "$out" | grep -q "$TOPIC"; then
    echo "FAIL: expected topic '$TOPIC' in kcat -L output"
    exit 1
fi

echo "PASS: librdkafka metadata query via kcat -L"
