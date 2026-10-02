#!/usr/bin/env bash
# Check delivery callbacks and read back the idempotent producer's records.
set -euo pipefail
TOPIC="compat-03-$$"
python3 - <<PY
from confluent_kafka import Producer
from kafka import KafkaConsumer, TopicPartition
p = Producer({'bootstrap.servers':'localhost:9092', 'enable.idempotence':True,
              'message.timeout.ms':20000})
acks = []
for i in range(10):
    p.produce('$TOPIC', f'idm-{i}'.encode(), partition=0,
              on_delivery=lambda err, msg: acks.append(err))
assert p.flush(30) == 0 and len(acks) == 10 and not any(acks), acks
c = KafkaConsumer(bootstrap_servers='localhost:9092', group_id=None,
                  enable_auto_commit=False, consumer_timeout_ms=5000)
c.assign([TopicPartition('$TOPIC',0)]); c.seek_to_beginning()
got = [m.value for m in c]; c.close()
assert got == [f'idm-{i}'.encode() for i in range(10)], got
print('PASS: idempotent deliveries acknowledged and read back in order')
PY
