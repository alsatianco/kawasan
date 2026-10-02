#!/usr/bin/env bash
# 13_schema_registry.sh: Confluent Schema Registry against Kawasan.
# Phase 5.3 gauntlet: this validates that the Java AdminClient + Producer
# + Consumer (which Schema Registry uses internally) can drive Kawasan
# through a full register-and-retrieve flow against the _schemas
# storage topic.
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

COMPOSE_FILE="$(dirname "$0")/../docker-compose.schema-registry.yml"
if [ ! -f "$COMPOSE_FILE" ]; then
    echo "SKIP: $COMPOSE_FILE missing"
    exit 0
fi

cleanup() {
    docker compose -f "$COMPOSE_FILE" down -v >/dev/null 2>&1 || true
}
trap cleanup EXIT

# Bring Schema Registry up against the host-running Kawasan.
docker compose -f "$COMPOSE_FILE" up -d schema-registry
echo "Schema Registry starting..."

# Wait for it to be ready (up to 60s — SR takes a while to boot)
for i in $(seq 1 60); do
    if curl -sf http://localhost:8081/subjects >/dev/null 2>&1; then
        echo "Schema Registry is ready (after ${i}s)"
        break
    fi
    sleep 1
done

if ! curl -sf http://localhost:8081/subjects >/dev/null 2>&1; then
    echo "FAIL: Schema Registry leader election timed out"
    docker logs kawasan-schema-registry 2>&1 | tail -3
    exit 1
fi

# Register an Avro schema
SCHEMA='{"schema":"{\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"}]}"}'
REGISTER=$(curl -sf -X POST -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
    -d "$SCHEMA" http://localhost:8081/subjects/compat-13-value/versions)
echo "Register: $REGISTER"

SCHEMA_ID=$(echo "$REGISTER" | grep -o '"id":[0-9]*' | head -1 | sed 's/"id"://')
if [ -z "$SCHEMA_ID" ]; then
    echo "FAIL: register did not return an id"
    exit 1
fi

# Retrieve the schema by ID. The schema is JSON-string-escaped inside
# the response (so we look for `User` without surrounding quotes —
# the actual bytes are `\"User\"`).
RETRIEVED=$(curl -sf "http://localhost:8081/schemas/ids/$SCHEMA_ID")
if ! echo "$RETRIEVED" | grep -q 'User'; then
    echo "FAIL: retrieved schema doesn't contain expected User record"
    echo "$RETRIEVED"
    exit 1
fi

# List subjects
SUBJECTS=$(curl -sf http://localhost:8081/subjects)
if ! echo "$SUBJECTS" | grep -q '"compat-13-value"'; then
    echo "FAIL: subject not in list"
    echo "$SUBJECTS"
    exit 1
fi

echo "PASS: Schema Registry register + retrieve + list flow against Kawasan"
