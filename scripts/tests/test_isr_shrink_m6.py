#!/usr/bin/env python3
"""M6 end-to-end test: a dead follower is shrunk out of the ISR so acks=all keeps
working, instead of blocking forever.

Setup: 3-broker cluster, RF=3 topic, broker-level min.insync.replicas=2, and a
low replica.lag.time.max.ms so the shrink happens quickly. We produce with
acks=all (works with all 3 in-sync), kill a follower that is neither the
partition leader nor the controller, then produce again with acks=all. Before
M6 the dead follower would stay in the ISR and the second produce would time out;
with M6 the leader detects the stalled follower, sends AlterPartition to the
controller, the ISR shrinks to the 2 live replicas, and the produce succeeds.

Usage: python3 scripts/tests/test_isr_shrink_m6.py
Requires: confluent-kafka, a built ./build/tools/kawasan-broker, cluster_harness.sh.
"""
import os
import subprocess
import sys
import time
import uuid

try:
    from confluent_kafka import Producer, KafkaException
    from confluent_kafka.admin import AdminClient, NewTopic
except ImportError:
    print("SKIP: confluent-kafka not installed", file=sys.stderr)
    sys.exit(0)

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
HARNESS = os.path.join(ROOT, "scripts", "tests", "cluster_harness.sh")
BROKER0 = "127.0.0.1:9092"
N_BROKERS = 3
LAG_MS = 3000

ENV = dict(os.environ, N=str(N_BROKERS), LAG_MS=str(LAG_MS), MIN_ISR="2")


def harness(*args):
    return subprocess.run(["bash", HARNESS, *args], cwd=ROOT, env=ENV,
                          capture_output=True, text=True)


def fail(msg):
    print(f"FAIL: {msg}", file=sys.stderr)
    print(harness("logs", "0").stdout[-2000:], file=sys.stderr)
    harness("down")
    sys.exit(1)


def produce_acks_all(topic, n, timeout_ms, partition=None):
    """Produce n records with acks=all (optionally to a fixed partition); return
    (ok, err) delivery counts."""
    counts = {"ok": 0, "err": 0}

    def cb(err, msg):
        counts["err" if err else "ok"] += 1

    p = Producer({
        "bootstrap.servers": BROKER0,
        "acks": "all",
        "message.timeout.ms": timeout_ms,
        "request.timeout.ms": 8000,
        "enable.idempotence": False,
        "retries": 20,
    })
    for i in range(n):
        kwargs = {"value": f"v{i}".encode(), "on_delivery": cb}
        if partition is not None:
            kwargs["partition"] = partition
        p.produce(topic, **kwargs)
    p.flush(timeout_ms / 1000 + 5)
    return counts["ok"], counts["err"]


def main():
    if not os.path.exists(os.path.join(ROOT, "build", "tools", "kawasan-broker")):
        print("SKIP: build/tools/kawasan-broker not found", file=sys.stderr)
        sys.exit(0)

    print(f"[m6] bringing up {N_BROKERS}-broker cluster (lag.max={LAG_MS}ms, minISR=2) ...")
    up = harness("up")
    sys.stdout.write(up.stdout)
    if "leader election observed" not in up.stdout:
        fail("no Raft leader elected")

    # 3 partitions so that partition i is led by broker i (leader = replicas[0] =
    # the i-th broker). This lets us target a partition whose leader is NOT the
    # controller, forcing the leader→controller AlterPartition RPC path (as
    # opposed to the controller committing its own partition's ISR directly).
    topic = "m6-isr-" + uuid.uuid4().hex[:8]
    admin = AdminClient({"bootstrap.servers": BROKER0})
    print(f"[m6] creating topic {topic} (partitions=3, RF=3) ...")
    try:
        admin.create_topics([NewTopic(topic, num_partitions=3, replication_factor=3,
                                      config={"min.insync.replicas": "2"})])[topic].result(30)
    except Exception as e:  # noqa: BLE001
        fail(f"create_topics failed: {e}")
    time.sleep(2)  # let followers begin fetching

    md = admin.list_topics(topic=topic, timeout=10)
    controller = md.controller_id
    parts = md.topics[topic].partitions
    # Choose a target partition led by a non-controller broker, and a victim
    # follower that is neither the controller nor that partition's leader.
    target_p, target_leader, victim = None, None, None
    for pid, meta in parts.items():
        if meta.leader != controller:
            others = [b for b in [0, 1, 2] if b != meta.leader and b != controller]
            if others:
                target_p, target_leader, victim = pid, meta.leader, others[0]
                break
    if target_p is None:
        fail(f"could not pick a non-controller-led partition (controller={controller})")
    print(f"[m6] controller={controller}; target partition {target_p} led by {target_leader} "
          f"(non-controller → AlterPartition RPC path); victim follower={victim}")

    # Baseline: acks=all works to the target partition with all three in-sync.
    ok, err = produce_acks_all(topic, 10, timeout_ms=10000, partition=target_p)
    if ok != 10:
        fail(f"baseline acks=all delivered {ok}/10 (err={err}) — replication not healthy")
    print(f"[m6] baseline: 10 records committed to partition {target_p} with acks=all.")

    print(f"[m6] killing follower broker {victim} ...")
    harness("kill", str(victim))

    # acks=all to the target partition must still succeed: its (non-controller)
    # leader must detect the dead follower, send AlterPartition to the controller,
    # and commit the shrunk ISR — all within the producer's timeout.
    print(f"[m6] producing to partition {target_p} with acks=all while broker {victim} is down ...")
    ok, err = produce_acks_all(topic, 10, timeout_ms=15000, partition=target_p)
    if ok != 10:
        fail(f"acks=all delivered only {ok}/10 (err={err}) after killing follower {victim} — "
             f"leader→controller AlterPartition ISR shrink did not unblock the committed path")
    print("[m6] acks=all succeeded — AlterPartition RPC shrink path works.")

    # Confirm the metadata ISR for the target partition actually shrank.
    md2 = admin.list_topics(topic=topic, timeout=10)
    isr2 = list(md2.topics[topic].partitions[target_p].isrs)
    print(f"[m6] partition {target_p} ISR after shrink: {isr2}")
    if victim in isr2:
        fail(f"dead follower {victim} still in partition {target_p} ISR {isr2}")
    if len(isr2) < 2:
        fail(f"ISR shrank below min.insync.replicas: {isr2}")

    admin.delete_topics([topic])
    print("[m6] tearing down cluster ...")
    harness("down")
    print("PASS: M6 verified (dead follower shrunk from ISR; acks=all stays available)")


if __name__ == "__main__":
    try:
        main()
    except KafkaException as e:
        fail(f"kafka error: {e}")
