#!/usr/bin/env bash
# 08_kcat_smoke.sh: kcat (librdkafka-based CLI) basic smoke test.
# Phase 5.3 gauntlet item — verifies that the broker's Metadata/Produce/Fetch
# wire format is compatible with librdkafka, not just kafka-python.
set -euo pipefail
TARGET="${1:-kawasan}"

if ! command -v kcat >/dev/null 2>&1; then
    echo "SKIP: kcat not installed"
    exit 0
fi

TOPIC="compat-08-kcat-$$"
BROKERS="localhost:9092"

# Try to produce 3 messages.
for i in 1 2 3; do
    echo "kcat-msg-$i" | kcat -b "$BROKERS" -t "$TOPIC" -P -X "message.timeout.ms=5000"
done
echo "Produced 3 messages via kcat -P"

sleep 1

# List topics — confirms Metadata works.
kcat -b "$BROKERS" -L -t "$TOPIC" 2>&1 | head -10

# Consume those 3 messages — confirms Fetch + ListOffsets work.
got=$(kcat -b "$BROKERS" -t "$TOPIC" -C -e -q 2>/dev/null | wc -l | tr -d ' ')
echo "kcat -C read $got messages"

if [ "$got" -lt 3 ]; then
    echo "FAIL: expected ≥3 messages, got $got"
    exit 1
fi

echo "PASS: kcat produce + list + consume round-trip"
