#!/usr/bin/env bash
# Run the same numbered contract checks against Kafka or Kawasan.
set -euo pipefail
TARGET="${1:-}"
KEEP_UP="${2:-}"
case "$TARGET" in
    kafka) export HARNESS_BROKER_CONTAINER=kawasan-harness-kafka-oracle ;;
    kawasan) export HARNESS_BROKER_CONTAINER=kawasan-harness-candidate ;;
    *) echo "Usage: $0 {kafka|kawasan} [--keep-up]" >&2; exit 2 ;;
esac
ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
cd "$ROOT"
COMPOSE_FILE="$ROOT/tests/ecosystem/docker-compose.$TARGET.yml"
LOG_DIR="${ECOSYSTEM_LOG_DIR:-$(mktemp -d /tmp/kawasan-ecosystem-$TARGET-XXXXXX)}"
mkdir -p "$LOG_DIR"
export ECOSYSTEM_EVIDENCE_DIR="$LOG_DIR"
echo "==> Evidence: $LOG_DIR"
command -v kcat >/dev/null || { echo 'ERROR: install kcat'; exit 2; }
python3 -c 'from kafka.admin.acl_resource import ACL; import confluent_kafka'
docker info >/dev/null
cleanup() {
    docker logs "$HARNESS_BROKER_CONTAINER" > "$LOG_DIR/broker.log" 2>&1 || true
    if [[ "$KEEP_UP" != --keep-up ]]; then
        docker compose -f "$COMPOSE_FILE" down -v
    fi
}
trap cleanup EXIT
export HARNESS_BROKER_COMPOSE="$COMPOSE_FILE"
echo "==> Bringing up $TARGET"
docker compose -f "$COMPOSE_FILE" up -d --build
healthy=0
for _ in $(seq 1 90); do
    status=$(docker inspect -f '{{.State.Health.Status}}' "$HARNESS_BROKER_CONTAINER")
    if [[ "$status" == healthy ]]; then healthy=1; break; fi
    if [[ "$status" == unhealthy ]]; then break; fi
    sleep 1
done
[[ "$healthy" == 1 ]] || { echo 'FAIL: broker did not become healthy'; exit 1; }
FAIL=0; PASS=0; SKIP=0
: > "$LOG_DIR/results.txt"
for script in "$ROOT"/tests/ecosystem/scripts/[0-9][0-9]_*.sh; do
    name=$(basename "$script")
    echo "==> Running $name"
    if bash "$script" "$TARGET" > "$LOG_DIR/$name.log" 2>&1; then
        if grep -q '^SKIP:' "$LOG_DIR/$name.log"; then
            result=SKIP; SKIP=$((SKIP + 1))
        else
            result=PASS; PASS=$((PASS + 1))
        fi
    else
        result=FAIL; FAIL=$((FAIL + 1))
    fi
    cat "$LOG_DIR/$name.log"
    echo "$result $name" | tee -a "$LOG_DIR/results.txt"
done
echo "Pass: $PASS  Fail: $FAIL  Skip: $SKIP"
[[ "$FAIL" == 0 && "$SKIP" == 0 ]]
