#!/usr/bin/env bash
# 18_sasl_plain.sh: SASL/PLAIN authentication.
#
# Phase EX-8: exercises the SASL/PLAIN code path end-to-end:
#   1. Start broker with file-backed credentials (one user/password)
#   2. Connect with correct credentials → produce + consume succeed
#   3. Connect with wrong password → authentication fails
#   4. Connect with unknown user → authentication fails
#
# This is the missing acceptance test for Phase 4.2a.
set -euo pipefail
TARGET="${1:-kawasan}"

LOG_DIR="/tmp/kawasan-sasl-test-$$"
CONFIG_FILE="$(mktemp /tmp/kawasan-sasl-config-XXXX.json)"
CREDS_FILE="$(mktemp /tmp/kawasan-sasl-creds-XXXX.txt)"
TOPIC="compat-18-sasl-$$"

cleanup() {
    pkill -9 -f "kawasan-broker.*$CONFIG_FILE" 2>/dev/null || true
    rm -rf "$LOG_DIR" "$CONFIG_FILE" "$CREDS_FILE"
}
trap cleanup EXIT

# Credentials file: alice can authenticate; eve cannot.
cat > "$CREDS_FILE" <<EOF
alice:wonderland-42
bob:second-user
EOF

cat > "$CONFIG_FILE" <<EOF
{
  "broker.id": 0,
  "host": "localhost",
  "advertised.host": "localhost",
  "port": 9097,
  "log.dirs": "$LOG_DIR",
  "auto.create.topics.enable": true,
  "default.replication.factor": 1,
  "monitoring.host": "0.0.0.0",
  "monitoring.port": 9098,
  "security.protocol": "PLAINTEXT",
  "sasl.plain.credentials.file": "$CREDS_FILE"
}
EOF

./build/tools/kawasan-broker --config "$CONFIG_FILE" > /tmp/k-sasl-$$.log 2>&1 &
B=$!
for i in $(seq 1 30); do
    if grep -q "Broker TCP listener active" /tmp/k-sasl-$$.log 2>/dev/null; then break; fi
    sleep 0.5
done

echo "Broker started with file-backed credentials"

# Test 1: correct credentials
python3 - <<'PY'
from kafka import KafkaProducer, KafkaConsumer
import sys
p = KafkaProducer(
    bootstrap_servers='localhost:9097',
    security_protocol='SASL_PLAINTEXT',
    sasl_mechanism='PLAIN',
    sasl_plain_username='alice',
    sasl_plain_password='wonderland-42',
    api_version=(2, 6, 0),
)
for i in range(5):
    p.send('compat-18-sasl-topic', value=f'sasl-msg-{i}'.encode()).get(timeout=10)
p.close()
print("PASS test-1: alice/wonderland-42 produced 5 messages")

c = KafkaConsumer(
    'compat-18-sasl-topic',
    bootstrap_servers='localhost:9097',
    security_protocol='SASL_PLAINTEXT',
    sasl_mechanism='PLAIN',
    sasl_plain_username='alice',
    sasl_plain_password='wonderland-42',
    api_version=(2, 6, 0),
    auto_offset_reset='earliest',
    consumer_timeout_ms=5000,
    group_id=None,
)
msgs = list(c)
c.close()
if len(msgs) != 5:
    print(f"FAIL test-1: expected 5 messages, got {len(msgs)}", file=sys.stderr)
    sys.exit(1)
print(f"PASS test-1: alice consumed {len(msgs)} messages")
PY

# Test 2: wrong password → should fail authentication.
# In dev mode (no credentials configured) Kawasan accepts anything; but
# with credentials configured, mismatches must be rejected.
python3 - <<'PY'
from kafka import KafkaProducer
from kafka.errors import KafkaError
import sys
try:
    p = KafkaProducer(
        bootstrap_servers='localhost:9097',
        security_protocol='SASL_PLAINTEXT',
        sasl_mechanism='PLAIN',
        sasl_plain_username='alice',
        sasl_plain_password='wrong-password',
        api_version=(2, 6, 0),
        request_timeout_ms=5000,
        api_version_auto_timeout_ms=5000,
    )
    fut = p.send('compat-18-sasl-topic', value=b'should-not-arrive')
    fut.get(timeout=5)
    print("FAIL test-2: wrong password was accepted", file=sys.stderr)
    sys.exit(1)
except Exception as e:
    # Expected: NodeNotReadyError / NoBrokersAvailable / KafkaTimeoutError
    # The specific exception varies by kafka-python version; what matters
    # is that no successful produce happened.
    print(f"PASS test-2: wrong password rejected ({type(e).__name__})")
PY

# Test 3: unknown user → should fail.
python3 - <<'PY'
from kafka import KafkaProducer
import sys
try:
    p = KafkaProducer(
        bootstrap_servers='localhost:9097',
        security_protocol='SASL_PLAINTEXT',
        sasl_mechanism='PLAIN',
        sasl_plain_username='eve',
        sasl_plain_password='anything',
        api_version=(2, 6, 0),
        request_timeout_ms=5000,
        api_version_auto_timeout_ms=5000,
    )
    fut = p.send('compat-18-sasl-topic', value=b'should-not-arrive')
    fut.get(timeout=5)
    print("FAIL test-3: unknown user was accepted", file=sys.stderr)
    sys.exit(1)
except Exception as e:
    print(f"PASS test-3: unknown user rejected ({type(e).__name__})")
PY

echo ""
echo "PASS: SASL/PLAIN authentication round-trip (auth + reject-wrong-pw + reject-unknown-user)"
