#!/usr/bin/env bash
# 02_producer_basic.sh: kafka-python produce 100 messages → consume them
# back via a fresh consumer group. Exercises Produce, Fetch, Metadata,
# JoinGroup, SyncGroup, Heartbeat, OffsetCommit, OffsetFetch end-to-end.
set -euo pipefail
TARGET="${1:-kawasan}"
TOPIC="compat-02-$$"

python3 - <<PY
from kafka import KafkaProducer, KafkaConsumer
p = KafkaProducer(bootstrap_servers='localhost:9092', client_id='compat-02-p', acks=1)
for i in range(100):
    p.send('$TOPIC', f'msg-{i}'.encode()).get(timeout=10)
p.flush(); p.close()
c = KafkaConsumer('$TOPIC', bootstrap_servers='localhost:9092',
                  group_id='compat-02-g', client_id='compat-02-c',
                  auto_offset_reset='earliest', consumer_timeout_ms=10000)
got = [m.value for m in c]
print(f'consumed: {len(got)}')
c.close()
assert len(got) == 100, f'expected 100, got {len(got)}'
print('PASS')
PY
