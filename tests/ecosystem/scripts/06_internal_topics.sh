#!/usr/bin/env bash
# 06_internal_topics.sh: __consumer_offsets must be a real Kafka topic
# (browsable via kafka-python). Phase 3 target — expected to fail on Kawasan
# today because the topic lives in RocksDB only.
set -euo pipefail
TARGET="${1:-kawasan}"

python3 - <<PY
from kafka import KafkaConsumer, KafkaProducer
import time

# Produce + commit something so __consumer_offsets has content.
p = KafkaProducer(bootstrap_servers='localhost:9092')
p.send('compat-06-source', b'x').get(timeout=10)
p.close()
c = KafkaConsumer('compat-06-source', bootstrap_servers='localhost:9092',
                  group_id='compat-06-g', auto_offset_reset='earliest',
                  consumer_timeout_ms=5000)
for _ in c:
    pass
c.commit()
c.close()
time.sleep(2)

# Read __consumer_offsets directly. enable_auto_commit=False so the inspector
# itself doesn't write commits back into __consumer_offsets — otherwise we get
# a feedback loop (inspector reads → triggers its own auto-commit → writes a
# new record → inspector sees that record → never times out).
oc = KafkaConsumer('__consumer_offsets', bootstrap_servers='localhost:9092',
                   group_id='compat-06-inspector', auto_offset_reset='earliest',
                   consumer_timeout_ms=5000, enable_auto_commit=False)
n = sum(1 for _ in oc)
oc.close()
print(f'__consumer_offsets records: {n}')
assert n > 0, '__consumer_offsets is empty (not a real topic?)'
print('PASS')
PY
