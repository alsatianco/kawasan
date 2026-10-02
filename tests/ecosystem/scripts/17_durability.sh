#!/usr/bin/env bash
# SIGKILL the selected Docker broker, retain its data, and verify exact offsets.
set -euo pipefail
TARGET="${1:-kawasan}"
BROKER="${HARNESS_BROKER_CONTAINER:-kawasan-harness-candidate}"
TOPIC="compat-17-durability-$$"
LEDGER="$(mktemp /tmp/kawasan-durability-XXXXXX)"
trap 'rm -f "$LEDGER"' EXIT
export TOPIC LEDGER
python3 - <<'PY'
import json, os
from kafka import KafkaProducer
p = KafkaProducer(bootstrap_servers='localhost:9092', acks='all')
ledger = []
for i in range(100):
    value = f'durability-{i}'.encode()
    m = p.send(os.environ['TOPIC'], partition=0, value=value).get(timeout=20)
    ledger.append([m.offset, value.decode()])
p.close()
with open(os.environ['LEDGER'], 'w') as f:
    json.dump(ledger, f); f.flush(); os.fsync(f.fileno())
print('100 acknowledged records durably recorded')
PY
docker kill --signal KILL "$BROKER"
docker start "$BROKER"
python3 - <<'PY'
import json, os, time
from kafka import KafkaConsumer, TopicPartition
acked = dict(json.load(open(os.environ['LEDGER'])))
deadline = time.monotonic() + 90
while True:
    try:
        c = KafkaConsumer(bootstrap_servers='localhost:9092', group_id=None,
                          enable_auto_commit=False, consumer_timeout_ms=10000,
                          isolation_level='read_committed')
        break
    except Exception:
        if time.monotonic() >= deadline: raise
        time.sleep(1)
c.assign([TopicPartition(os.environ['TOPIC'], 0)])
c.seek_to_beginning()
got = {m.offset: m.value.decode() for m in c}
c.close()
assert all(got.get(o) == v for o, v in acked.items()), (acked, got)
print('PASS: every acknowledged record survived SIGKILL at its exact offset')
PY
