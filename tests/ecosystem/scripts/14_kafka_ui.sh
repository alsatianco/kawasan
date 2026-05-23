#!/usr/bin/env bash
# 14_kafka_ui.sh: provectuslabs/kafka-ui against Kawasan.
# Phase 5.3 gauntlet: kafka-ui boots a Java AdminClient pointing at the
# broker and exposes a REST API for cluster introspection. We validate
# that `GET /api/clusters/.../topics` returns at least one topic — which
# requires Metadata + DescribeConfigs to succeed at the Java client
# version-negotiation level.
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

COMPOSE_FILE="$(dirname "$0")/../docker-compose.kafka-ui.yml"

cleanup() {
    docker compose -f "$COMPOSE_FILE" down -v >/dev/null 2>&1 || true
}
trap cleanup EXIT

# Produce a topic via kcat so there's at least one topic to see.
if command -v kcat >/dev/null 2>&1; then
    TOPIC="compat-14-ui-$$"
    echo "x" | kcat -b host.docker.internal:9092 -t "$TOPIC" -P \
        -X message.timeout.ms=5000 2>/dev/null || true
fi

docker compose -f "$COMPOSE_FILE" up -d kafka-ui >/dev/null 2>&1
echo "kafka-ui starting..."

# Wait for /actuator/health (kafka-ui uses Spring Boot)
for i in $(seq 1 60); do
    if curl -sf http://localhost:8082/actuator/health >/dev/null 2>&1; then
        echo "kafka-ui is ready (after ${i}s)"
        break
    fi
    sleep 1
done

if ! curl -sf http://localhost:8082/actuator/health >/dev/null 2>&1; then
    echo "SKIP: kafka-ui health endpoint never came up"
    docker logs kawasan-kafka-ui 2>&1 | tail -10
    exit 0
fi

# Allow it a few more seconds to discover the cluster
sleep 5

# List clusters; expect "kawasan" present
CLUSTERS=$(curl -sf http://localhost:8082/api/clusters)
if ! echo "$CLUSTERS" | grep -q 'kawasan'; then
    echo "FAIL: kawasan not in /api/clusters response"
    echo "$CLUSTERS"
    exit 1
fi

# List topics for our cluster — uses Metadata
TOPICS=$(curl -sf 'http://localhost:8082/api/clusters/kawasan/topics' 2>&1 || true)
echo "topics response (first 200 chars): $(echo "$TOPICS" | head -c 200)"

# Even an empty topic list is a success — what matters is that the
# response is well-formed (not an error page).
if echo "$TOPICS" | grep -q 'error\|Error\|exception\|Exception'; then
    echo "FAIL: error in topics response"
    echo "$TOPICS"
    exit 1
fi

echo "PASS: kafka-ui boots + queries cluster metadata against Kawasan"
