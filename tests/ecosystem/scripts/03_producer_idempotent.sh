#!/usr/bin/env bash
# 03_producer_idempotent.sh: Java-3.x-default produce path requires
# InitProducerId (API 22) + producer-state dedup. This is Phase 1's headline
# blocker; the script is the smallest reproducer.
#
# Expected today:
#   kafka       → PASS (Apache Kafka 4.2 supports InitProducerId since 0.11)
#   kawasan     → FAIL with kafka.errors.UnknownTopicOrPartitionError or
#                 KafkaError-22 (UNSUPPORTED_VERSION) because InitProducerId
#                 is not yet registered (improve-opus.md G3).
set -euo pipefail
TARGET="${1:-kawasan}"
TOPIC="compat-03-$$"

# confluent-kafka-python uses librdkafka, which defaults enable.idempotence=True
# in newer releases. Fall back to kafka-python with enable_idempotence=True if
# confluent-kafka isn't available.
python3 - <<PY
try:
    from confluent_kafka import Producer
    impl = 'confluent_kafka (librdkafka)'
    def make_producer():
        return Producer({
            'bootstrap.servers': 'localhost:9092',
            'client.id': 'compat-03-p',
            'enable.idempotence': True,
        })
except ImportError:
    from kafka import KafkaProducer
    impl = 'kafka-python'
    def make_producer():
        return KafkaProducer(
            bootstrap_servers='localhost:9092',
            client_id='compat-03-p',
            enable_idempotence=True,
            acks='all',
        )

print(f'using {impl}')
p = make_producer()
if impl.startswith('confluent'):
    for i in range(10):
        p.produce('$TOPIC', f'idm-{i}'.encode())
    p.flush()
else:
    for i in range(10):
        p.send('$TOPIC', f'idm-{i}'.encode()).get(timeout=10)
    p.flush(); p.close()
print('PASS')
PY
