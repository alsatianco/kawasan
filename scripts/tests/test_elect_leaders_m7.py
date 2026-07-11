#!/usr/bin/env python3
"""M7 (partial): manual ElectLeaders over the wire against a live 3-broker cluster.

Verifies the real handleElectLeaders path end-to-end: a preferred-replica
election request is accepted by the controller and returns success per partition.
For a freshly-created topic the preferred replica already leads, so this asserts
the idempotent "already preferred" success path (no error) — confirming the
handler, the UPDATE_LEADER metadata command wiring, and the client round-trip all
work. (Electing leadership AWAY from the preferred replica requires a prior
failover, which is M8; that end-to-end divergence-truncation path is exercised
there.)

Usage: python3 scripts/tests/test_elect_leaders_m7.py
"""
import os
import subprocess
import sys
import time
import uuid

try:
    from confluent_kafka.admin import AdminClient, NewTopic
    from confluent_kafka import TopicPartition
    try:
        from confluent_kafka.admin import ElectionType
    except ImportError:
        ElectionType = None
except ImportError:
    print("SKIP: confluent-kafka not installed", file=sys.stderr)
    sys.exit(0)

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
HARNESS = os.path.join(ROOT, "scripts", "tests", "cluster_harness.sh")
BROKER0 = "127.0.0.1:9092"
ENV = dict(os.environ, N="3")


def harness(*args):
    return subprocess.run(["bash", HARNESS, *args], cwd=ROOT, env=ENV,
                          capture_output=True, text=True)


def fail(msg):
    print(f"FAIL: {msg}", file=sys.stderr)
    harness("down")
    sys.exit(1)


def main():
    if not os.path.exists(os.path.join(ROOT, "build", "tools", "kawasan-broker")):
        print("SKIP: build/tools/kawasan-broker not found", file=sys.stderr)
        sys.exit(0)
    if ElectionType is None or not hasattr(AdminClient, "elect_leaders"):
        print("SKIP: confluent-kafka too old for elect_leaders", file=sys.stderr)
        sys.exit(0)

    print("[m7] bringing up 3-broker cluster ...")
    up = harness("up")
    sys.stdout.write(up.stdout)
    if "leader election observed" not in up.stdout:
        fail("no Raft leader elected")

    topic = "m7-elect-" + uuid.uuid4().hex[:8]
    admin = AdminClient({"bootstrap.servers": BROKER0})
    print(f"[m7] creating topic {topic} (partitions=3, RF=3) ...")
    try:
        admin.create_topics([NewTopic(topic, num_partitions=3, replication_factor=3)])[topic].result(30)
    except Exception as e:  # noqa: BLE001
        fail(f"create_topics failed: {e}")
    time.sleep(2)

    print("[m7] issuing preferred-leader ElectLeaders for all partitions ...")
    parts = [TopicPartition(topic, p) for p in range(3)]
    try:
        fut = admin.elect_leaders(ElectionType.PREFERRED, parts)
        result = fut.result(30)  # dict {TopicPartition: None|KafkaException}
    except Exception as e:  # noqa: BLE001
        # Some client versions raise if nothing needed electing; treat a clean
        # "election not needed" as success, anything else as failure.
        msg = str(e).lower()
        if "not needed" in msg or "elECTION_NOT_NEEDED".lower() in msg:
            print("[m7] elect_leaders reported election-not-needed (preferred already leads) — OK")
            harness("down")
            print("PASS: M7 ElectLeaders wire path verified (handler accepted preferred election)")
            return
        fail(f"elect_leaders raised: {e}")

    # result maps each partition to None (success) or an exception.
    bad = {tp: err for tp, err in (result or {}).items() if err is not None}
    if bad:
        fail(f"elect_leaders returned errors: {bad}")
    print(f"[m7] elect_leaders succeeded for {len(result or parts)} partition(s).")

    admin.delete_topics([topic])
    harness("down")
    print("PASS: M7 ElectLeaders wire path verified (handler accepted preferred election)")


if __name__ == "__main__":
    main()
