#!/usr/bin/env python3
"""Ultra-simple test: produce 3 messages, consume twice, check if offsets persist."""

import time
from kafka import KafkaProducer, KafkaConsumer

TOPIC = 'offset-debug'
GROUP = 'offset-debug-group'
BOOTSTRAP = 'localhost:9092'

print("Producing 3 messages...")
producer = KafkaProducer(bootstrap_servers=BOOTSTRAP)
for i in range(3):
    producer.send(TOPIC, value=f"Msg{i}".encode())
producer.flush()
producer.close()
print("✓ Produced\n")

# First consumption
print("First consumer session:")
consumer1 = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP,
    group_id=GROUP,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    auto_commit_interval_ms=1000,
    consumer_timeout_ms=5000,
)
first = []
for msg in consumer1:
    first.append(msg.offset)
    print(f"  offset={msg.offset}")
consumer1.close()
print(f"✓ Consumed {len(first)} messages\n")

time.sleep(2)  # Wait for auto-commit

# Second consumption
print("Second consumer session (should be EMPTY if offsets work):")
consumer2 = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP,
    group_id=GROUP,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    consumer_timeout_ms=5000,
)
second = []
for msg in consumer2:
    second.append(msg.offset)
    print(f"  offset={msg.offset}")
consumer2.close()

print(f"\nResult: First={first}, Second={second}")
if len(second) == 0:
    print("✅ SUCCESS - offsets persisted!")
else:
    print("❌ FAILURE - offsets NOT persisted!")
