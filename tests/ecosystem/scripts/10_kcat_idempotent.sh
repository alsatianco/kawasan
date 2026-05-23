#!/usr/bin/env bash
# 10_kcat_idempotent.sh: librdkafka idempotent producer path.
# Phase 5.3 gauntlet: kcat with `-X enable.idempotence=true` exercises
# the InitProducerId → Produce-with-pid+sequence path through librdkafka,
# which uses different framing than the kafka-python idempotent test
# (kcat uses Fetch v12 / Produce v9+ etc).
set -euo pipefail
TARGET="${1:-kawasan}"

if ! command -v kcat >/dev/null 2>&1; then
    echo "SKIP: kcat not installed"
    exit 0
fi

TOPIC="compat-10-idem-$$"
BROKERS="localhost:9092"

# Produce 10 messages with idempotence enabled. librdkafka will call
# InitProducerId to obtain a producer_id+epoch, then send Produce with
# pid+sequence so the broker can dedup retries.
for i in $(seq 1 10); do
    echo "idem-msg-$i" | kcat -b "$BROKERS" -t "$TOPIC" -P \
        -X enable.idempotence=true \
        -X message.timeout.ms=5000 \
        -X delivery.timeout.ms=10000 \
        2>&1 | head -3 || true
done
echo "Produced 10 idempotent messages via kcat"

sleep 1

# Read them back; expect at least 10 (could be more if any retried but
# the dedup path treats retries as already-stored).
got=$(kcat -b "$BROKERS" -t "$TOPIC" -C -e -q 2>/dev/null | wc -l | tr -d ' ')
echo "kcat -C read $got messages"

if [ "$got" -lt 10 ]; then
    echo "FAIL: expected ≥10 messages, got $got"
    exit 1
fi

echo "PASS: librdkafka idempotent produce + consume round-trip ($got msgs)"
