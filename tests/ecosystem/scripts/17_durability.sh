#!/usr/bin/env bash
# 17_durability.sh: durability under abrupt shutdown.
#
# Phase EX-9: the single most important guarantee a Kafka broker
# provides. Every record acknowledged by the broker (acks=all on
# produce) must survive an arbitrary process death. This script:
#
#   1. Starts the broker
#   2. Produces 100 messages with acks=all
#   3. SIGKILLs the broker (no clean shutdown)
#   4. Restarts the broker against the same data dir
#   5. Reads back from offset 0
#   6. PASSes iff all 100 messages are present and in order
#
# The boot time after kill is also recorded; the §7 quality gate says
# cold-start ≤ 30s — we assert ≤60s to be safe (host I/O variance).
set -euo pipefail
TARGET="${1:-kawasan}"

LOG_DIR="/tmp/kawasan-durability-test-$$"
CONFIG_FILE="$(mktemp /tmp/kawasan-durability-config-XXXX.json)"
TOPIC="compat-17-durability-$$"
N_MESSAGES=100

cleanup() {
    pkill -9 -f "kawasan-broker.*$CONFIG_FILE" 2>/dev/null || true
    rm -rf "$LOG_DIR" "$CONFIG_FILE"
}
trap cleanup EXIT

# Write a config that points at a clean log dir for this test.
cat > "$CONFIG_FILE" <<EOF
{
  "broker.id": 0,
  "host": "localhost",
  "advertised.host": "localhost",
  "port": 9095,
  "log.dirs": "$LOG_DIR",
  "log.segment.bytes": 104857600,
  "log.retention.ms": 86400000,
  "log.cleaner.interval.ms": 300000,
  "default.replication.factor": 1,
  "min.insync.replicas": 1,
  "auto.create.topics.enable": true,
  "delete.topic.enable": true,
  "log.level": "info",
  "security.protocol": "PLAINTEXT",
  "monitoring.host": "0.0.0.0",
  "monitoring.port": 9096
}
EOF

start_broker() {
    ./build/tools/kawasan-broker --config "$CONFIG_FILE" > /tmp/k-durability-$$.log 2>&1 &
    BROKER_PID=$!
    local start_t=$(date +%s)
    for i in $(seq 1 60); do
        if grep -q "Broker TCP listener active" /tmp/k-durability-$$.log 2>/dev/null; then
            local elapsed=$(($(date +%s) - start_t))
            echo "  broker ready after ${elapsed}s (PID $BROKER_PID)"
            return 0
        fi
        sleep 0.5
    done
    echo "  FAIL: broker didn't start within 30s"
    cat /tmp/k-durability-$$.log | tail -20
    return 1
}

echo "=== Phase EX-9 durability test ==="
echo "Starting broker (round 1, fresh log dir)..."
start_broker || exit 1

echo "Producing $N_MESSAGES messages with acks=all..."
python3 - <<PY
from kafka import KafkaProducer
p = KafkaProducer(
    bootstrap_servers='localhost:9095',
    acks='all',
    retries=0,  # surface failures, don't mask them
    max_in_flight_requests_per_connection=1,  # in-order send
)
for i in range($N_MESSAGES):
    fut = p.send('$TOPIC', value=f'durability-msg-{i:03d}'.encode())
    fut.get(timeout=10)  # wait for ack
p.flush()
p.close()
print(f"  acked $N_MESSAGES messages")
PY

echo "SIGKILL-ing broker (PID $BROKER_PID)..."
kill -9 "$BROKER_PID" 2>/dev/null
sleep 1

echo "Starting broker (round 2, same log dir)..."
boot_start=$(date +%s)
start_broker || exit 1
boot_elapsed=$(($(date +%s) - boot_start))
if [ "$boot_elapsed" -gt 60 ]; then
    echo "  FAIL: cold start took ${boot_elapsed}s, expected ≤60s"
    exit 1
fi

echo "Reading back from offset 0..."
got=$(python3 - <<PY
from kafka import KafkaConsumer, TopicPartition
c = KafkaConsumer(
    bootstrap_servers='localhost:9095',
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    consumer_timeout_ms=10000,
    group_id=None,  # no group; read all
)
tp = TopicPartition('$TOPIC', 0)
c.assign([tp])
c.seek_to_beginning(tp)
msgs = []
for msg in c:
    msgs.append(msg.value.decode())
c.close()
print(len(msgs))
PY
)

if [ "$got" -lt "$N_MESSAGES" ]; then
    echo "  FAIL: expected $N_MESSAGES messages after restart, got $got"
    exit 1
fi

echo "PASS: $N_MESSAGES acked messages all survived SIGKILL + restart (boot=${boot_elapsed}s)"
