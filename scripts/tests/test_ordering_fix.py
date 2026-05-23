#!/usr/bin/env python3
"""Test to verify message ordering is fixed."""

from kafka import KafkaConsumer

TOPIC = 'test-fix'
GROUP = 'test-fix-group'
BOOTSTRAP = 'localhost:9092'

print("Testing message ordering...\n")

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP,
    group_id=GROUP,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    consumer_timeout_ms=5000,
    value_deserializer=lambda m: m.decode('utf-8')
)

offsets = []
for msg in consumer:
    offsets.append(msg.offset)
    print(f"{len(offsets):2d}. offset={msg.offset:2d}: {msg.value}")

consumer.close()

print(f"\nReceived {len(offsets)} messages")
print(f"Offsets: {offsets}")

# Check ordering
expected = list(range(len(offsets)))
if offsets == expected:
    print("\n✅ SUCCESS: Messages are in correct order!")
else:
    print(f"\n❌ FAILURE: Expected {expected}, got {offsets}")
