#!/usr/bin/env python3
"""M5 end-to-end replication test against a local 3-broker Kawasan cluster.

Proves that FOLLOWERS actually replicate data from the leader — not just that
metadata propagates. The lever is acks=all: the leader only acknowledges once
every in-sync replica has the record, so a successful acks=all produce on an
RF=3 topic is direct evidence the two followers are fetching. Without the M5
follower fetcher, the produce would block until timeout.

Note on scope: partition-leader FAILOVER is milestone M8, so this test does NOT
kill the leader. It verifies the replication data path (M5) via acks=all + a
full-round-trip consume. Randomized-fault safety is milestone M9.

Usage:
    python3 scripts/tests/test_replication_m5.py
Requires: confluent-kafka, a built ./build/tools/kawasan-broker, and
scripts/tests/cluster_harness.sh.
"""
import os
import subprocess
import sys
import time
import uuid

try:
    from confluent_kafka import Producer, Consumer, KafkaException
    from confluent_kafka.admin import AdminClient, NewTopic
except ImportError:
    print("SKIP: confluent-kafka not installed", file=sys.stderr)
    sys.exit(0)

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
HARNESS = os.path.join(ROOT, "scripts", "tests", "cluster_harness.sh")
BROKER0 = "127.0.0.1:9092"  # broker 0 kafka port per cluster_harness.sh
N_BROKERS = 3
N_RECORDS = 50


def harness(*args, env=None):
    e = dict(os.environ)
    e["N"] = str(N_BROKERS)
    if env:
        e.update(env)
    return subprocess.run(["bash", HARNESS, *args], cwd=ROOT, env=e,
                          capture_output=True, text=True)


def fail(msg):
    print(f"FAIL: {msg}", file=sys.stderr)
    harness("down")
    sys.exit(1)


def main():
    if not os.path.exists(os.path.join(ROOT, "build", "tools", "kawasan-broker")):
        print("SKIP: build/tools/kawasan-broker not found (build first)", file=sys.stderr)
        sys.exit(0)

    print("[m5] bringing up 3-broker cluster ...")
    up = harness("up")
    sys.stdout.write(up.stdout)
    if "leader election observed" not in up.stdout:
        fail("cluster did not elect a Raft leader; multi-broker formation is a prerequisite")

    topic = "m5-repl-" + uuid.uuid4().hex[:8]
    admin = AdminClient({"bootstrap.servers": BROKER0})
    print(f"[m5] creating topic {topic} (partitions=1, RF=3, min.insync.replicas=2) ...")
    nt = NewTopic(topic, num_partitions=1, replication_factor=3,
                  config={"min.insync.replicas": "2"})
    fs = admin.create_topics([nt])
    try:
        fs[topic].result(timeout=30)
    except Exception as e:  # noqa: BLE001
        fail(f"create_topics failed: {e}")

    # Give the followers a moment to observe the new assignment and start fetching.
    time.sleep(2)

    print(f"[m5] producing {N_RECORDS} records with acks=all ...")
    delivered = {"ok": 0, "err": 0}

    def on_delivery(err, msg):
        if err is not None:
            delivered["err"] += 1
        else:
            delivered["ok"] += 1

    p = Producer({
        "bootstrap.servers": BROKER0,
        "acks": "all",
        "message.timeout.ms": 15000,
        "request.timeout.ms": 10000,
        "enable.idempotence": False,
    })
    for i in range(N_RECORDS):
        p.produce(topic, key=b"k", value=f"v{i}".encode(), on_delivery=on_delivery)
    p.flush(30)

    if delivered["ok"] != N_RECORDS:
        fail(f"acks=all delivered only {delivered['ok']}/{N_RECORDS} "
             f"({delivered['err']} errors) — followers are not replicating "
             f"(the leader never got ISR-commit confirmation)")
    print(f"[m5] acks=all: all {N_RECORDS} records committed (followers replicated).")

    print("[m5] consuming back all records ...")
    c = Consumer({
        "bootstrap.servers": BROKER0,
        "group.id": "m5-verify-" + uuid.uuid4().hex[:6],
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    c.subscribe([topic])
    seen = set()
    deadline = time.time() + 30
    while time.time() < deadline and len(seen) < N_RECORDS:
        msg = c.poll(1.0)
        if msg is None or msg.error():
            continue
        seen.add(msg.value().decode())
    c.close()

    if len(seen) != N_RECORDS:
        fail(f"consumed {len(seen)}/{N_RECORDS} records")
    print(f"[m5] consumed all {N_RECORDS} records back.")

    admin.delete_topics([topic])
    print("[m5] tearing down cluster ...")
    harness("down")
    print("PASS: M5 replication verified (RF=3 acks=all commit + full round-trip consume)")


if __name__ == "__main__":
    try:
        main()
    except KafkaException as e:
        fail(f"kafka error: {e}")
