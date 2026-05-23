#!/usr/bin/env python3
"""Test to reproduce the infinite loop with test-topic."""

import time
from kafka import KafkaConsumer

TOPIC = 'test-topic'
GROUP = 'simple-consumer-group'
BOOTSTRAP = 'localhost:9092'

print("Starting consumer on test-topic...")
print("Ctrl+C after seeing ~50 messages\n")

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP,
    group_id=GROUP,
    auto_offset_reset='latest',
    enable_auto_commit=True,
    auto_commit_interval_ms=5000,
    value_deserializer=lambda m: m.decode('utf-8')
)

message_count = 0
offsets_seen = []

try:
    for message in consumer:
        message_count += 1
        offsets_seen.append(message.offset)
        print(f"{message_count:3d}. offset={message.offset:3d}: {message.value}")
        
        if message_count >= 50:
            print("\n\n50 messages received, stopping...")
            break
            
except KeyboardInterrupt:
    pass

consumer.close()

print(f"\nTotal messages: {message_count}")
print(f"Unique offsets: {len(set(offsets_seen))}")
print(f"Offset pattern: {offsets_seen[:30]}")

# Check for loops
offset_counts = {}
for offset in offsets_seen:
    offset_counts[offset] = offset_counts.get(offset, 0) + 1

repeated = {k: v for k, v in offset_counts.items() if v > 1}
if repeated:
    print(f"\n❌ LOOP DETECTED: Offsets consumed multiple times: {repeated}")
else:
    print(f"\n✅ NO LOOP: Each offset consumed once")
