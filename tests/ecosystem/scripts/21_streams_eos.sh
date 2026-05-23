#!/usr/bin/env bash
# 17_streams_eos.sh: Streams EOS v2 (KIP-447) end-to-end harness.
#
# Validates the full Kafka Streams exactly_once_v2 cycle:
#   1. Consume from input (read_committed)
#   2. Process records (toy WordCount)
#   3. Produce result to output (transactional producer)
#   4. Stage consumer-group offsets via TxnOffsetCommit
#   5. Commit transaction (offsets become visible)
#
#   --or--
#
#   5'. Abort transaction (offsets discarded, output records hidden from
#       read_committed consumers)
#
# A second harness phase verifies the abort path:
#   - Aborted output records must NOT be visible to read_committed consumers
#   - Aborted staged offsets must NOT be applied (consumer can re-read input)
#
# We don't run a full kafka-streams JVM here — that would require docker
# compose + a streams app. Instead we use confluent-kafka's transactional
# Producer + consumer API which exercises the exact same wire-level path
# the streams runtime takes (it's all KIP-447 underneath).
set -euo pipefail
TARGET="${1:-kawasan}"

if ! python3 -c "import confluent_kafka" 2>/dev/null; then
    echo "SKIP: confluent-kafka not installed (pip install confluent-kafka)"
    exit 0
fi

BROKERS="localhost:9092"
INPUT="streams-eos-in-$$"
OUTPUT="streams-eos-out-$$"
GROUP="streams-eos-grp-$$"

python3 - <<PY
import sys
import time
from confluent_kafka import Producer, Consumer, KafkaError, TopicPartition

BROKERS = "$BROKERS"
INPUT = "$INPUT"
OUTPUT = "$OUTPUT"
GROUP = "$GROUP"

# Step 0: seed input topic with non-transactional records.
print("Seeding input topic with 6 records...")
seed = Producer({"bootstrap.servers": BROKERS})
for i in range(1, 7):
    seed.produce(INPUT, value=f"input-{i}".encode())
seed.flush(5.0)
print("  seeded.")
time.sleep(0.5)

def make_streams_producer(txn_id):
    return Producer({
        "bootstrap.servers": BROKERS,
        "transactional.id": txn_id,
        "transaction.timeout.ms": 30000,
        "enable.idempotence": True,
    })

def make_streams_consumer(group, isolation="read_committed"):
    return Consumer({
        "bootstrap.servers": BROKERS,
        "group.id": group,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "isolation.level": isolation,
    })

# Phase 1: SUCCESSFUL EOS cycle. Consume 3 records, transform, produce
# transactionally, commit offsets, commit txn. All records must
# appear in OUTPUT after read_committed consume.
print("Phase 1: EOS-commit cycle (3 records)...")
p = make_streams_producer("eos-commit-$$")
p.init_transactions(10.0)

c = make_streams_consumer(GROUP)
c.subscribe([INPUT])

p.begin_transaction()

consumed = []
deadline = time.time() + 8.0
while len(consumed) < 3 and time.time() < deadline:
    msg = c.poll(0.5)
    if msg is None or msg.error():
        continue
    consumed.append(msg)

assert len(consumed) == 3, f"expected 3 input records, got {len(consumed)}"

# Transform: uppercase the value.
for m in consumed:
    transformed = m.value().decode().upper().encode()
    p.produce(OUTPUT, value=transformed)
p.flush(5.0)

# Stage consumer offsets in the transaction (KIP-447).
last_pos = [TopicPartition(m.topic(), m.partition(), m.offset() + 1)
            for m in consumed]
p.send_offsets_to_transaction(
    last_pos, c.consumer_group_metadata(), 10.0)

p.commit_transaction(10.0)
print(f"  EOS-commit: produced {len(consumed)} records, staged offsets, committed.")
c.close()

# Phase 2: ABORTED EOS cycle. Consume next 3 records, produce
# transactionally, but ABORT. Output records must NOT be visible to
# read_committed consumers and offsets must NOT be applied (consumer
# can re-read the same 3 records).
print("Phase 2: EOS-abort cycle (next 3 records)...")
p2 = make_streams_producer("eos-abort-$$")
p2.init_transactions(10.0)

c2 = make_streams_consumer(GROUP)
c2.subscribe([INPUT])

p2.begin_transaction()
consumed2 = []
deadline = time.time() + 8.0
while len(consumed2) < 3 and time.time() < deadline:
    msg = c2.poll(0.5)
    if msg is None or msg.error():
        continue
    consumed2.append(msg)

assert len(consumed2) == 3, f"expected 3 more input records, got {len(consumed2)}"

# Verify these are the NEXT records after the committed-offset
# checkpoint (offsets 3, 4, 5 — committed phase consumed 0, 1, 2).
expected_inputs = {f"input-{i}" for i in (4, 5, 6)}
got_inputs = {m.value().decode() for m in consumed2}
assert got_inputs == expected_inputs, (
    f"offset checkpoint broken: expected {expected_inputs}, got {got_inputs}"
)

for m in consumed2:
    p2.produce(OUTPUT, value=f"ABORTED-{m.value().decode()}".encode())
p2.flush(5.0)

# Stage offsets that we will then discard via abort.
last_pos2 = [TopicPartition(m.topic(), m.partition(), m.offset() + 1)
             for m in consumed2]
p2.send_offsets_to_transaction(
    last_pos2, c2.consumer_group_metadata(), 10.0)

p2.abort_transaction(10.0)
print("  EOS-abort: produced 3 records, staged offsets, aborted.")
c2.close()

# Validation 1: read_committed consumer must see exactly 3 records on
# OUTPUT (the uppercased records from phase 1, none from phase 2).
print("Validating OUTPUT under read_committed...")
v = make_streams_consumer(f"validate-rc-$$")
v.subscribe([OUTPUT])
out_seen = []
deadline = time.time() + 8.0
last = time.time()
while time.time() < deadline:
    msg = v.poll(0.5)
    if msg is None:
        if out_seen and time.time() - last > 2.0:
            break
        continue
    if msg.error():
        continue
    out_seen.append(msg.value().decode())
    last = time.time()
v.close()
print(f"  OUTPUT (read_committed): {out_seen}")

# Should be exactly INPUT-1, INPUT-2, INPUT-3 (uppercased), none of the
# ABORTED-* records.
expected_out = {"INPUT-1", "INPUT-2", "INPUT-3"}
if set(out_seen) != expected_out:
    print(f"FAIL: OUTPUT read_committed mismatch — expected {expected_out}, "
          f"got {set(out_seen)}", file=sys.stderr)
    if any("ABORTED-" in v for v in out_seen):
        print("FAIL: aborted records leaked into read_committed OUTPUT",
              file=sys.stderr)
    sys.exit(1)

# Validation 2: consumer offsets — phase 1's offsets were committed,
# phase 2's were aborted. So after both phases, the committed offset
# for INPUT should be 3 (not 6). A fresh consumer in GROUP should
# re-read records 4, 5, 6.
print("Validating committed offsets after abort...")
v2 = make_streams_consumer(GROUP)
v2.subscribe([INPUT])
re_consumed = []
deadline = time.time() + 6.0
last = time.time()
while time.time() < deadline:
    msg = v2.poll(0.5)
    if msg is None:
        if re_consumed and time.time() - last > 2.0:
            break
        continue
    if msg.error():
        continue
    re_consumed.append(msg.value().decode())
    last = time.time()
v2.close()

expected_re = {"input-4", "input-5", "input-6"}
got_re = set(re_consumed)
if got_re != expected_re:
    print(f"FAIL: re-consume should yield {expected_re} (aborted offsets "
          f"discarded), got {got_re}", file=sys.stderr)
    sys.exit(1)

print()
print("PASS: Streams EOS v2 cycle validated")
print(f"  - committed txn: 3 records produced, offsets advanced to 3")
print(f"  - aborted txn:   3 records produced (invisible), offsets NOT advanced")
print(f"  - re-consume:    sees records 4-6 (correct — aborted offsets were discarded)")
PY
