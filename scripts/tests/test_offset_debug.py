#!/usr/bin/env python3
"""Debug script to test offset commit behavior."""

import time
from kafka import KafkaProducer, KafkaConsumer

BOOTSTRAP = 'localhost:9092'
TOPIC = 'debug-topic'
GROUP_ID = 'debug-group'

print("=" * 60)
print("DEBUG: Testing Offset Commit Behavior")
print("=" * 60)

# Step 1: Produce 3 messages
print("\n[1] Producing 3 messages...")
producer = KafkaProducer(bootstrap_servers=BOOTSTRAP)

for i in range(3):
    msg = f"Message {i}".encode('utf-8')
    future = producer.send(TOPIC, value=msg)
    record_metadata = future.get(timeout=10)
    print(f"  Produced: Message {i} -> offset={record_metadata.offset}")

producer.flush()
producer.close()
print("  ✓ Production complete")

# Step 2: First consumption
print("\n[2] First consumption...")
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP,
    group_id=GROUP_ID,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    auto_commit_interval_ms=500,  # Commit every 500ms
    consumer_timeout_ms=3000,  # Stop after 3s of no messages
    value_deserializer=lambda v: v.decode('utf-8'),
)

first_run = []
for msg in consumer:
    first_run.append(msg.offset)
    print(f"  Consumed: offset={msg.offset}, value={msg.value}")

print(f"  ✓ First run consumed {len(first_run)} messages")
consumer.close()

# Wait to ensure offsets are committed
print("\n[3] Waiting 2 seconds for offset commit...")
time.sleep(2)

# Step 3: Second consumption (should be empty if offsets work)
print("\n[4] Second consumption (should be EMPTY)...")
consumer2 = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP,
    group_id=GROUP_ID,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    auto_commit_interval_ms=500,
    consumer_timeout_ms=3000,
    value_deserializer=lambda v: v.decode('utf-8'),
)

second_run = []
for msg in consumer2:
    second_run.append(msg.offset)
    print(f"  RE-CONSUMED: offset={msg.offset}, value={msg.value}")

print(f"  Second run consumed {len(second_run)} messages")
consumer2.close()

# Analysis
print("\n" + "=" * 60)
print("RESULT")
print("=" * 60)
if len(second_run) == 0:
    print("✅ SUCCESS: Offsets were committed properly!")
else:
    print(f"❌ FAILURE: {len(second_run)} messages were re-consumed!")
    print(f"   First run:  {first_run}")
    print(f"   Second run: {second_run}")
print("=" * 60)
