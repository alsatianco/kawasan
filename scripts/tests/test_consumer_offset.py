#!/usr/bin/env python3
"""
Test script to reproduce the consumer offset issue.
This script:
1. Creates a topic
2. Produces 5 messages
3. Consumes messages and observes if the same messages are consumed repeatedly
"""

import time
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError

BOOTSTRAP = 'localhost:9092'
TOPIC = 'offset-test'
GROUP_ID = 'offset-test-group'

print("=" * 60)
print("Testing Consumer Offset Behavior")
print("=" * 60)

# Step 1: Produce 5 messages
print("\n[1] Producing 5 messages...")
producer = KafkaProducer(bootstrap_servers=BOOTSTRAP)

for i in range(5):
    msg = f"Message {i}".encode('utf-8')
    future = producer.send(TOPIC, value=msg)
    try:
        record_metadata = future.get(timeout=10)
        print(f"  Produced: Message {i} -> partition={record_metadata.partition}, offset={record_metadata.offset}")
    except KafkaError as e:
        print(f"  Error producing Message {i}: {e}")

producer.flush()
producer.close()
print("  ✓ All messages produced")

# Step 2: First consumption
print("\n[2] First consumption (should see messages 0-4)...")
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP,
    group_id=GROUP_ID,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    auto_commit_interval_ms=1000,  # Commit every second
    consumer_timeout_ms=5000,  # Stop after 5s of no messages
    value_deserializer=lambda v: v.decode('utf-8'),
)

first_run_messages = []
print("  Consuming messages...")
for msg in consumer:
    first_run_messages.append(msg.offset)
    print(f"  Consumed: offset={msg.offset}, value={msg.value}")

consumer.close()
print(f"  ✓ First run consumed {len(first_run_messages)} messages: offsets={first_run_messages}")

# Wait a bit to ensure offsets are committed
time.sleep(2)

# Step 3: Second consumption
print("\n[3] Second consumption (should see NO messages if offsets were committed)...")
consumer2 = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP,
    group_id=GROUP_ID,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    auto_commit_interval_ms=1000,
    consumer_timeout_ms=5000,
    value_deserializer=lambda v: v.decode('utf-8'),
)

second_run_messages = []
print("  Consuming messages...")
for msg in consumer2:
    second_run_messages.append(msg.offset)
    print(f"  Consumed: offset={msg.offset}, value={msg.value}")

consumer2.close()
print(f"  ✓ Second run consumed {len(second_run_messages)} messages: offsets={second_run_messages}")

# Step 4: Analysis
print("\n" + "=" * 60)
print("ANALYSIS")
print("=" * 60)
if len(second_run_messages) == 0:
    print("✅ SUCCESS: Offsets were properly committed!")
    print("   The consumer did not re-consume already processed messages.")
else:
    print("❌ PROBLEM DETECTED: Consumer re-consumed messages!")
    print(f"   First run:  {first_run_messages}")
    print(f"   Second run: {second_run_messages}")
    print("   This indicates offset commits are not working properly.")
print("=" * 60)
