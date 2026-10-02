#!/usr/bin/env bash
# Compaction uses Kafka's legal minimum segment size and keeps the active tail.
set -euo pipefail
python3 - <<'PY'
import os, time
from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaProducer, KafkaConsumer, TopicPartition
topic = f'compat-05-{os.getpid()}'
a = KafkaAdminClient(bootstrap_servers='localhost:9092')
a.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1,
                         topic_configs={'cleanup.policy':'compact', 'segment.bytes':'1048576',
                                        'min.cleanable.dirty.ratio':'0.01',
                                        'max.compaction.lag.ms':'1000'})])
a.close()
p = KafkaProducer(bootstrap_servers='localhost:9092', compression_type=None)
latest = {}
for i in range(250):
    key = b'A' if i % 2 == 0 else b'B'
    value = f'v{i}:'.encode() + b'x' * 32768
    p.send(topic, key=key, value=value).get(timeout=10)
    latest[key] = value
p.close()
deadline = time.monotonic() + 90
while True:
    c = KafkaConsumer(bootstrap_servers='localhost:9092', group_id=None,
                      consumer_timeout_ms=2000, enable_auto_commit=False)
    c.assign([TopicPartition(topic,0)]); c.seek_to_beginning()
    records = [(m.key, m.value) for m in c]; c.close()
    assert all((k,v) in records for k,v in latest.items()), 'latest value lost'
    if len(records) < 100:
        print(f'PASS: compaction reduced 250 versions to {len(records)}, preserving latest values')
        break
    if time.monotonic() >= deadline:
        raise AssertionError(f'compaction did not run: {len(records)} records')
    time.sleep(2)
PY
