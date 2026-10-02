#!/usr/bin/env bash
# 12_kcat_transactional.sh: librdkafka transactional producer path.
# Phase 3.3 / 5.3 gauntlet: exercises InitProducerId →
# AddPartitionsToTxn → Produce → EndTxn(committed=true) flow through
# librdkafka. The Kawasan broker's transactional handlers are
# scaffolding only (no LSO / control records yet), so this validates
# that the wire-level handshake completes without errors.
set -euo pipefail
TARGET="${1:-kawasan}"

if ! command -v kcat >/dev/null 2>&1; then
    echo "SKIP: kcat not installed"
    exit 0
fi

TOPIC="compat-12-txn-$$"
BROKERS="localhost:9092"
TXN_ID="kcat-txn-$$"

# librdkafka transactional producer: configure transactional.id, then
# kcat will call InitProducerId, AddPartitionsToTxn, Produce, EndTxn.
for i in 1 2 3 4 5; do
    echo "txn-msg-$i" | kcat -b "$BROKERS" -t "$TOPIC" -P \
        -X transactional.id="$TXN_ID" \
        -X transaction.timeout.ms=10000 \
        -X message.timeout.ms=5000
done

sleep 1

got=$(kcat -b "$BROKERS" -t "$TOPIC" -C -e -q -X isolation.level=read_committed 2>/dev/null | wc -l | tr -d ' ')
echo "kcat -C read $got messages after txn produce"

if [ "$got" -lt 5 ]; then
    echo "FAIL: expected ≥5 messages, got $got"
    exit 1
fi

echo "PASS: librdkafka transactional producer wire-level handshake + produce"
