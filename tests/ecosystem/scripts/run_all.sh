#!/usr/bin/env bash
# 0B.3 + 0B.5: orchestrate the per-service smoke scripts against either
# the kafka oracle or the kawasan candidate. Designed for both local use
# and CI (compat-smoke job).
#
# Usage:
#   ./run_all.sh kafka     # bring up oracle, run all smoke scripts, tear down
#   ./run_all.sh kawasan   # bring up candidate, run all smoke scripts, tear down
#   ./run_all.sh kawasan --keep-up    # don't tear down after (useful for debugging)
#
# Exit codes:
#   0  — every script returned 0
#   1  — at least one script returned non-zero
#   2  — usage error (bad target)

set -uo pipefail

TARGET="${1:-}"
KEEP_UP=0
if [[ "${2:-}" == "--keep-up" ]]; then
    KEEP_UP=1
fi

case "$TARGET" in
    kafka)
        COMPOSE_FILE="docker-compose.kafka.yml"
        BROKER_NAME="kafka oracle"
        ;;
    kawasan)
        COMPOSE_FILE="docker-compose.kawasan.yml"
        BROKER_NAME="kawasan candidate"
        ;;
    *)
        echo "Usage: $0 {kafka|kawasan} [--keep-up]" >&2
        exit 2
        ;;
esac

cd "$(dirname "$0")/.."

# Choose the docker compose CLI flavor (newer Docker uses `docker compose`,
# older needs the standalone binary).
if docker compose version >/dev/null 2>&1; then
    DC="docker compose"
elif command -v docker-compose >/dev/null 2>&1; then
    DC="docker-compose"
else
    echo "ERROR: neither 'docker compose' nor 'docker-compose' is available" >&2
    exit 2
fi

echo "==> Bringing up $BROKER_NAME"
$DC -f "$COMPOSE_FILE" up -d --build

# Wait for the broker container to report healthy. We give the kafka oracle
# a bit longer because its KRaft cluster setup is heavier.
echo "==> Waiting for broker to be healthy"
for i in $(seq 1 60); do
    status=$($DC -f "$COMPOSE_FILE" ps --format '{{.Status}}' | head -1 || true)
    if [[ "$status" == *"healthy"* ]]; then
        echo "    healthy after ${i}s"
        break
    fi
    sleep 1
done

# Run every numbered smoke script in order.
FAIL=0
PASS=0
declare -a RESULTS
for script in scripts/[0-9][0-9]_*.sh; do
    if [[ ! -x "$script" ]]; then
        continue
    fi
    name=$(basename "$script")
    echo ""
    echo "==> Running $name"
    if "$script" "$TARGET"; then
        echo "    PASS: $name"
        RESULTS+=("PASS $name")
        PASS=$((PASS + 1))
    else
        echo "    FAIL: $name"
        RESULTS+=("FAIL $name")
        FAIL=$((FAIL + 1))
    fi
done

if [[ $KEEP_UP -eq 0 ]]; then
    echo ""
    echo "==> Tearing down $BROKER_NAME"
    $DC -f "$COMPOSE_FILE" down -v
fi

echo ""
echo "================================================================"
echo " Compat harness summary for $BROKER_NAME"
echo "================================================================"
for line in "${RESULTS[@]}"; do
    echo "  $line"
done
echo ""
echo "  Pass: $PASS  Fail: $FAIL"

if [[ $FAIL -eq 0 ]]; then
    exit 0
else
    exit 1
fi
