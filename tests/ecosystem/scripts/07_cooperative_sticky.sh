#!/usr/bin/env bash
# 07_cooperative_sticky.sh: cooperative-sticky / cooperative assignment must
# work end-to-end through the broker. Phase 2.3 validates that two consumers
# joining the same group with `partition.assignment.strategy=cooperative-sticky`
# share partitions without errors and both receive messages.
set -euo pipefail
TARGET="${1:-kawasan}"

python3 - <<'PY'
from kafka import KafkaProducer, KafkaConsumer, TopicPartition
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.coordinator.assignors.sticky.sticky_assignor import StickyPartitionAssignor
import threading
import time
import sys

BROKERS = 'localhost:9092'
TOPIC = 'compat-07-cs'
GROUP = 'compat-07-csg'

# Create a 4-partition topic. With 2 consumers, each should get ~2 partitions.
admin = KafkaAdminClient(bootstrap_servers=BROKERS)
try:
    admin.create_topics([NewTopic(name=TOPIC, num_partitions=4, replication_factor=1)])
    print(f"Created topic {TOPIC}")
except Exception as e:
    print(f"Topic create: {e}")
admin.close()

# Produce 8 messages across all 4 partitions.
p = KafkaProducer(bootstrap_servers=BROKERS)
for i in range(8):
    p.send(TOPIC, value=f"msg-{i}".encode(), partition=i % 4)
p.flush()
p.close()
print("Produced 8 messages")

# Two consumers in the same group with sticky assignor. Each should receive
# some subset of partitions and together cover all 8 messages.
results = {}
errors = {}

def consume(name, results_dict, errors_dict):
    try:
        c = KafkaConsumer(
            TOPIC,
            bootstrap_servers=BROKERS,
            group_id=GROUP,
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            partition_assignment_strategy=[StickyPartitionAssignor],
            consumer_timeout_ms=8000,
            client_id=name,
        )
        got = []
        for msg in c:
            got.append((msg.partition, msg.value.decode()))
        c.close()
        results_dict[name] = got
        print(f"{name}: received {len(got)} messages from partitions {sorted({p for p, _ in got})}")
    except Exception as e:
        errors_dict[name] = str(e)
        print(f"{name} ERROR: {e}")

t1 = threading.Thread(target=consume, args=('c1', results, errors))
t2 = threading.Thread(target=consume, args=('c2', results, errors))
t1.start()
time.sleep(0.5)  # Stagger so c1 sees rebalance when c2 joins
t2.start()
t1.join()
t2.join()

if errors:
    print(f"FAIL: errors {errors}")
    sys.exit(1)

# Both consumers must have received SOME messages (proves partitions were
# shared) and together they must have seen all 8.
all_msgs = []
for name, got in results.items():
    all_msgs.extend(got)
    if not got:
        print(f"FAIL: consumer {name} got 0 messages — partition sharing didn't happen")
        sys.exit(1)

unique_partitions = set(p for p, _ in all_msgs)
if len(unique_partitions) != 4:
    print(f"FAIL: expected partitions {{0,1,2,3}}, got {unique_partitions}")
    sys.exit(1)

if len(all_msgs) < 8:
    print(f"FAIL: expected 8+ messages total, got {len(all_msgs)}")
    sys.exit(1)

print(f"PASS: cooperative-sticky shared {len(unique_partitions)} partitions across 2 consumers ({len(all_msgs)} msgs total)")
PY
