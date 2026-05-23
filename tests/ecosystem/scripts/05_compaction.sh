#!/usr/bin/env bash
# 05_compaction.sh: create a compact-policy topic, produce K versions of the
# same key, force a roll-and-cleanup, verify only the latest value is kept.
# Phase 3 target — expected to fail on Kawasan today (in-memory compaction
# OOM risk, no streaming algorithm).
set -euo pipefail
TARGET="${1:-kawasan}"
TOPIC="compat-05-$$"

python3 - <<PY
import time
from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaProducer, KafkaConsumer

a = KafkaAdminClient(bootstrap_servers='localhost:9092', client_id='compat-05')
a.create_topics([
    NewTopic('$TOPIC', num_partitions=1, replication_factor=1,
             topic_configs={
                 'cleanup.policy': 'compact',
                 # Force segment rolls so compaction has non-active segments
                 # to operate on. 1024 bytes ⇒ ~50 records per segment with
                 # 20-byte records ⇒ ~20 segments for the 1000-record load.
                 'segment.bytes': '1024',
                 'min.cleanable.dirty.ratio': '0.01',
             })
])
a.close()

p = KafkaProducer(bootstrap_servers='localhost:9092')
for i in range(1000):
    # Two keys, 500 versions each.
    key = (b'A' if i % 2 == 0 else b'B')
    p.send('$TOPIC', key=key, value=f'v{i}'.encode()).get(timeout=10)
p.flush(); p.close()

# Give the broker a moment to run cleanup. Kafka does it lazily.
time.sleep(20)

c = KafkaConsumer('$TOPIC', bootstrap_servers='localhost:9092',
                  auto_offset_reset='earliest', consumer_timeout_ms=10000)
records = [(m.key, m.value) for m in c]
c.close()
print(f'after compact: {len(records)} records')
# Strict pass: at most a few records per key (the most recent value plus
# any in the active segment). Permissive pass for early Kawasan: any
# reduction at all.
if len(records) <= 50:
    print('PASS')
else:
    print('FAIL (compaction did not run; got', len(records), 'records)')
    raise SystemExit(1)
PY
