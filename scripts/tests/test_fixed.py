#!/usr/bin/env python3
"""Test to verify the consumer offset fix."""
import time
import sys
from kafka import KafkaProducer, KafkaConsumer

BOOTSTRAP = 'localhost:9092'
TOPIC = 'fix-test'
GROUP_ID = 'fix-test-group'

print("Testing consumer offset fix...")
print("=" * 60)

# Produce messages
print("\n[1] Producing 10 messages...")
producer = KafkaProducer(bootstrap_servers=BOOTSTRAP)
for i in range(10):
    msg = f"Message {i}".encode('utf-8')
    producer.send(TOPIC, value=msg)
producer.flush()
producer.close()
print("  ✓ Produced 10 messages")

# Consume with auto-commit
print("\n[2] Consuming messages (15 second test)...")
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP,
    group_id=GROUP_ID,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    auto_commit_interval_ms=1000,
    consumer_timeout_ms=15000,  # 15 seconds
    value_deserializer=lambda v: v.decode('utf-8'),
)

consumed = {}
start = time.time()

for msg in consumer:
    offset = msg.offset
    if offset not in consumed:
        consumed[offset] = []
    consumed[offset].append(time.time() - start)
    
    # Print first 20 consumptions
    if sum(len(v) for v in consumed.values()) <= 20:
        print(f"  [{consumed[offset][-1]:.1f}s] offset={offset} (count={len(consumed[offset])})")
    
    # Early detection of loop
    if any(len(v) > 2 for v in consumed.values()):
        print(f"\n❌ LOOP DETECTED after {time.time() - start:.1f}s!")
        break

consumer.close()

# Results
print("\n" + "=" * 60)
print("RESULTS")
print("=" * 60)
total = sum(len(v) for v in consumed.values())
unique = len(consumed)
loops = sum(1 for v in consumed.values() if len(v) > 1)

print(f"Total consumptions: {total}")
print(f"Unique offsets: {unique}")
print(f"Offsets re-consumed: {loops}")

if loops == 0:
    print("\n✅ SUCCESS: No message re-consumption detected!")
    sys.exit(0)
else:
    print("\n❌ FAILURE: Messages were re-consumed in a loop")
    for offset, times in sorted(consumed.items()):
        if len(times) > 1:
            print(f"  offset={offset}: consumed {len(times)} times at {[f'{t:.1f}s' for t in times]}")
    sys.exit(1)
