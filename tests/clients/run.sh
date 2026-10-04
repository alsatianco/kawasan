#!/usr/bin/env bash
# Every leg proves itself against Kafka before running against Kawasan.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
PROFILE="${1:?Usage: run.sh PROFILE}"
case "$PROFILE" in java-3.9.1|java-4.2.0|rdkafka-2.8.0|rdkafka-2.15.1|python) ;; *) exit 2 ;; esac
EVIDENCE="${CLIENT_MATRIX_EVIDENCE:-$(mktemp -d /tmp/kawasan-client-matrix-XXXXXX)}"
mkdir -p "$EVIDENCE"
EVIDENCE="$(cd "$EVIDENCE" && pwd)"
echo "Evidence: $EVIDENCE"
export CLIENT_MATRIX_BROKER_LOG_LEVEL="${CLIENT_MATRIX_BROKER_LOG_LEVEL:-debug}"
TARGET=""
COMPOSE=""
BROKER=""
cleanup() {
    if [[ -n "$BROKER" ]]; then
        docker logs "$BROKER" > "$EVIDENCE/$TARGET-broker.log" 2>&1 || true
        docker compose -f "$COMPOSE" down -v > "$EVIDENCE/$TARGET-cleanup.log" 2>&1 || true
    fi
}
trap cleanup EXIT
if [[ "$PROFILE" == rdkafka-* ]]; then
    docker build --build-arg "CLIENT_VERSION=${PROFILE#rdkafka-}" -t "kawasan-client:$PROFILE" "$ROOT/tests/clients/librdkafka" > "$EVIDENCE/client-build.log" 2>&1
fi
for TARGET in kafka kawasan; do
    COMPOSE="$ROOT/tests/ecosystem/docker-compose.$TARGET.yml"
    if [[ "$TARGET" == kafka ]]; then BROKER=kawasan-harness-kafka-oracle; else BROKER=kawasan-harness-candidate; fi
    mkdir -p "$EVIDENCE/$TARGET"
    docker compose -f "$COMPOSE" up -d --build --wait --wait-timeout 120 > "$EVIDENCE/$TARGET-startup.log" 2>&1
    if [[ "$PROFILE" == java-* ]]; then
        # Copy rather than mounting the project read-write: no root-owned build files in checkout.
        mkdir -p "$EVIDENCE/$TARGET/java"
        cp -R "$ROOT/tests/clients/java/." "$EVIDENCE/$TARGET/java/"
        docker run --rm --network="container:$BROKER" \
            -e "CLIENT_VERSION=${PROFILE#java-}" -e BOOTSTRAP=localhost:9092 -e CM1_SEED_PRODUCE_BUG \
            -v "$EVIDENCE/$TARGET/java:/work" -v "$EVIDENCE/$TARGET:/evidence" \
            -v kawasan-client-maven:/root/.m2 maven:3.9.9-eclipse-temurin-21 \
            bash /work/run.sh > "$EVIDENCE/$TARGET/client.log" 2>&1
    elif [[ "$PROFILE" == rdkafka-* ]]; then
        docker run --rm --network="container:$BROKER" \
            -e "CLIENT_VERSION=${PROFILE#rdkafka-}" -e BOOTSTRAP=localhost:9092 \
            -v "$EVIDENCE/$TARGET:/evidence" "kawasan-client:$PROFILE" > "$EVIDENCE/$TARGET/client.log" 2>&1
    else
        docker run --rm --network="container:$BROKER" \
            -v "$ROOT/tests/clients/kafka_python.py:/work/matrix.py:ro" -v "$EVIDENCE/$TARGET:/evidence" \
            python:3.11-slim-bookworm bash -c 'pip install --only-binary=:all: kafka-python==2.2.15 && python /work/matrix.py' \
            > "$EVIDENCE/$TARGET/client.log" 2>&1
    fi
    python3 "$ROOT/tests/clients/results.py" "$PROFILE" "$TARGET" "$EVIDENCE/$TARGET/results.jsonl"
    cleanup
    BROKER=""
    if [[ "$TARGET" == kawasan ]]; then
        python3 "$ROOT/tests/clients/negotiated.py" "$PROFILE" "${COMPAT_API_PROFILE:-4.x}" \
            "$EVIDENCE/kawasan-broker.log" > "$EVIDENCE/negotiated.json"
    fi
done
