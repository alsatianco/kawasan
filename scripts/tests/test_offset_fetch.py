#!/usr/bin/env python3
"""Test to verify OffsetFetch is returning committed offsets properly."""

import time
from kafka import KafkaProducer, KafkaConsumer
from kafka.structs import TopicPartition, OffsetAndMetadata

BOOTSTRAP = 'localhost:9092'
TOPIC = 'fetch-test'
GROUP_ID = 'fetch-test-group'
PARTITION = 0

print("=" * 60)
print("Testing OffsetFetch Behavior")
print("=" * 60)

# Step 1: Produce 10 messages
print("\n[1] Producing 10 messages...")
producer = KafkaProducer(bootstrap_servers=BOOTSTRAP)
for i in range(10):
    msg = f"Message {i}".encode('utf-8')
    producer.send(TOPIC, value=msg)
producer.flush()
producer.close()
print("  ✓ Produced 10 messages")

# Step 2: Consume first 5 and commit
print("\n[2] Consuming first 5 messages...")
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP,
    group_id=GROUP_ID,
    auto_offset_reset='earliest',
    enable_auto_commit=False,  # Manual commit for precise control
    consumer_timeout_ms=5000,
    value_deserializer=lambda v: v.decode('utf-8'),
)

count = 0
last_offset = -1
for msg in consumer:
    print(f"  Consumed: offset={msg.offset}, value={msg.value}")
    last_offset = msg.offset
    count += 1
    if count >= 5:
        break

# Manually commit offset=5 (next offset to read)
print(f"\n[3] Manually committing offset={last_offset + 1}...")
tp = TopicPartition(TOPIC, PARTITION)
consumer.commit({tp: OffsetAndMetadata(last_offset + 1, None)})
print(f"  ✓ Committed offset={last_offset + 1}")

# Close consumer
consumer.close()
time.sleep(1)  # Wait for commit to process

# Step 3: Check what offset is returned by OffsetFetch
print("\n[4] Creating new consumer to check OffsetFetch...")
consumer2 = KafkaConsumer(
    bootstrap_servers=BOOTSTRAP,
    group_id=GROUP_ID,
    enable_auto_commit=False,
    value_deserializer=lambda v: v.decode('utf-8'),
)

# Get committed offset
committed = consumer2.committed(tp)
print(f"  OffsetFetch returned: {committed}")

if committed == last_offset + 1:
    print(f"  ✅ SUCCESS: OffsetFetch returned correct offset {committed}")
else:
    print(f"  ❌ FAILURE: Expected {last_offset + 1}, got {committed}")

# Step 4: Subscribe and see what messages are consumed
print("\n[5] Subscribing to topic to see what gets consumed...")
consumer2.subscribe([TOPIC])
consumer2.poll(timeout_ms=100)  # Trigger assignment
consumer2.seek(tp, committed if committed is not None else 0)

consumed_offsets = []
for msg in consumer2:
    consumed_offsets.append(msg.offset)
    print(f"  Consumed: offset={msg.offset}, value={msg.value}")
    if len(consumed_offsets) >= 5:
        break

consumer2.close()

print("\n" + "=" * 60)
print("RESULT")
print("=" * 60)
if consumed_offsets and consumed_offsets[0] == last_offset + 1:
    print(f"✅ SUCCESS: Started consuming from correct offset {consumed_offsets[0]}")
else:
    print(f"❌ FAILURE: Expected to start from {last_offset + 1}, started from {consumed_offsets[0] if consumed_offsets else 'none'}")
print("=" * 60)
