#!/usr/bin/env python3
"""
Simple test to debug the consumer loop issue.
"""
import time
from kafka import KafkaProducer, KafkaConsumer

BOOTSTRAP = 'localhost:9092'
TOPIC = 'loop-test'
GROUP_ID = 'loop-test-group'

print("=" * 60)
print("Testing Consumer Loop Issue")
print("=" * 60)

# Step 1: Produce 5 messages
print("\n[1] Producing 5 messages...")
producer = KafkaProducer(bootstrap_servers=BOOTSTRAP)

for i in range(5):
    msg = f"Message {i}".encode('utf-8')
    future = producer.send(TOPIC, value=msg)
    record_metadata = future.get(timeout=10)
    print(f"  Produced: Message {i} -> offset={record_metadata.offset}")

producer.flush()
producer.close()
print("  ✓ Production complete")

# Step 2: Consume with auto-commit enabled
print("\n[2] Starting consumer (auto-commit enabled)...")
print("  Consuming for 10 seconds, then we'll see if it loops...")

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP,
    group_id=GROUP_ID,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    auto_commit_interval_ms=1000,  # Commit every second
    consumer_timeout_ms=10000,  # Timeout after 10s
    value_deserializer=lambda v: v.decode('utf-8'),
)

consumed_count = {}
start_time = time.time()

for msg in consumer:
    offset = msg.offset
    value = msg.value
    elapsed = time.time() - start_time
    
    if offset not in consumed_count:
        consumed_count[offset] = 0
    consumed_count[offset] += 1
    
    print(f"  [{elapsed:.1f}s] offset={offset}, value={value}, count={consumed_count[offset]}")
    
    # If we've consumed the same message more than once, it's a loop!
    if consumed_count[offset] > 1:
        print(f"\n❌ LOOP DETECTED! offset={offset} consumed {consumed_count[offset]} times")
        break

consumer.close()

# Analysis
print("\n" + "=" * 60)
print("RESULT")
print("=" * 60)

total_consumed = sum(consumed_count.values())
unique_consumed = len(consumed_count)
loops_detected = sum(1 for count in consumed_count.values() if count > 1)

print(f"Total messages consumed: {total_consumed}")
print(f"Unique offsets consumed: {unique_consumed}")
print(f"Offsets with loops: {loops_detected}")

if loops_detected > 0:
    print("\n❌ FAILURE: Consumer is stuck in a loop!")
    for offset, count in sorted(consumed_count.items()):
        if count > 1:
            print(f"   offset={offset} consumed {count} times")
else:
    print("\n✅ SUCCESS: No loops detected!")

print("=" * 60)
