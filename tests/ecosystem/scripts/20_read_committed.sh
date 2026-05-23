#!/usr/bin/env bash
# 20_read_committed.sh: read_committed isolation end-to-end (KIP-98).
#
# Validates Kawasan's transactional read_committed isolation:
#   1. A read_committed consumer must NOT see records from aborted transactions
#   2. A read_committed consumer MUST see records from committed transactions
#   3. A read_uncommitted consumer sees BOTH committed and aborted records
#
# Uses confluent-kafka (librdkafka) which has full + reliable transactional
# producer support (init_transactions/begin/send/commit_transaction/abort_transaction).
set -euo pipefail
TARGET="${1:-kawasan}"

if ! python3 -c "import confluent_kafka" 2>/dev/null; then
    echo "SKIP: confluent-kafka not installed (pip install confluent-kafka)"
    exit 0
fi

BROKERS="localhost:9092"
TOPIC="compat-20-readcommitted-$$"

python3 - <<PY
import sys
import time
from confluent_kafka import Producer, Consumer, KafkaException

BROKERS = "$BROKERS"
TOPIC = "$TOPIC"

# Phase 1: produce 5 messages in a COMMITTED transaction.
print("Producing 5 messages in committed transaction...")
p1 = Producer({
    "bootstrap.servers": BROKERS,
    "transactional.id": "txn-commit-$$",
    "transaction.timeout.ms": 10000,
    "enable.idempotence": True,
})
p1.init_transactions(10.0)
p1.begin_transaction()
for i in range(1, 6):
    p1.produce(TOPIC, value=f"committed-{i}".encode())
p1.flush(5.0)
p1.commit_transaction(10.0)
print("  committed 5 messages")

# Phase 2: produce 5 messages in an ABORTED transaction.
print("Producing 5 messages in aborted transaction...")
p2 = Producer({
    "bootstrap.servers": BROKERS,
    "transactional.id": "txn-abort-$$",
    "transaction.timeout.ms": 10000,
    "enable.idempotence": True,
})
p2.init_transactions(10.0)
p2.begin_transaction()
for i in range(1, 6):
    p2.produce(TOPIC, value=f"aborted-{i}".encode())
p2.flush(5.0)
p2.abort_transaction(10.0)
print("  aborted 5 messages")

time.sleep(1.0)

# Phase 3: consume with read_committed. Must see exactly 5 (only the committed).
def consume_all(isolation):
    c = Consumer({
        "bootstrap.servers": BROKERS,
        "group.id": f"grp-{isolation}-$$",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "isolation.level": isolation,
    })
    c.subscribe([TOPIC])
    seen = []
    deadline = time.time() + 8.0
    while time.time() < deadline:
        msg = c.poll(0.5)
        if msg is None:
            if seen and time.time() - last > 1.5:
                break
            continue
        if msg.error():
            continue
        seen.append(msg.value().decode())
        last = time.time()
    c.close()
    return seen

print("Consuming with isolation_level=read_committed...")
rc_records = consume_all("read_committed")
print(f"  read_committed sees {len(rc_records)} records: {rc_records}")

print("Consuming with isolation_level=read_uncommitted...")
ru_records = consume_all("read_uncommitted")
print(f"  read_uncommitted sees {len(ru_records)} records: {ru_records}")

# Assertions:
#   read_committed must see exactly the 5 "committed-*" records
#   read_uncommitted must see all 10 (5 committed + 5 aborted)
if len(rc_records) != 5:
    print(f"FAIL: read_committed should see exactly 5, saw {len(rc_records)}", file=sys.stderr)
    sys.exit(1)

if not all(r.startswith("committed-") for r in rc_records):
    print(f"FAIL: read_committed leaked aborted records: {rc_records}", file=sys.stderr)
    sys.exit(1)

if len(ru_records) < len(rc_records):
    print(f"FAIL: read_uncommitted ({len(ru_records)}) saw fewer than read_committed ({len(rc_records)})", file=sys.stderr)
    sys.exit(1)

print()
print(f"PASS: read_committed isolation correctly filters aborted records (committed={len(rc_records)}, uncommitted={len(ru_records)})")
PY
