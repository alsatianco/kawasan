#!/usr/bin/env bash
# 16_ksqldb.sh: ksqlDB against Kawasan.
# Phase 5.3 gauntlet: ksqlDB uses Kafka transactional producers + a
# consumer group + several internal topics. This test validates that
# the server boots and the /info endpoint responds.
set -euo pipefail
TARGET="${1:-kawasan}"

if ! command -v docker >/dev/null 2>&1; then
    echo "SKIP: docker not available"
    exit 0
fi
if ! command -v curl >/dev/null 2>&1; then
    echo "SKIP: curl not installed"
    exit 0
fi

COMPOSE_FILE="$(dirname "$0")/../docker-compose.ksqldb.yml"

cleanup() {
    docker compose -f "$COMPOSE_FILE" down -v >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker compose -f "$COMPOSE_FILE" up -d ksqldb-server
echo "ksqlDB starting..."

# ksqlDB takes a while to boot — give it 120s
for i in $(seq 1 120); do
    if curl -sf http://localhost:8088/info >/dev/null 2>&1; then
        echo "ksqlDB is ready (after ${i}s)"
        break
    fi
    sleep 1
done

if ! curl -sf http://localhost:8088/info >/dev/null 2>&1; then
    echo "FAIL: ksqlDB /info never came up"
    docker logs kawasan-ksqldb 2>&1 | tail -10
    exit 1
fi

INFO=$(curl -sf http://localhost:8088/info)
echo "ksqlDB info: $(echo "$INFO" | head -c 200)"

if ! echo "$INFO" | grep -q 'KsqlServerInfo'; then
    echo "FAIL: ksqlDB /info response missing KsqlServerInfo"
    exit 1
fi

echo "PASS: ksqlDB server boots + /info responds against Kawasan"
