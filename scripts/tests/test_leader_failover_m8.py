#!/usr/bin/env python3
"""M8 end-to-end test: automatic partition-leader failover.

A 3-broker cluster with an RF=3, min.insync.replicas=2 topic. We commit records
with acks=all, then SIGKILL a partition leader and assert that:

  1. the controller elects a new leader from the ISR (Metadata shows it within
     the liveness timeout + sweep + client refresh budget),
  2. acks=all produce to that partition resumes,
  3. every previously-acknowledged record is still readable.

Then we SIGKILL the CONTROLLER (which also leads a partition): the new Raft
leader must take over the sweep after its grace window and fail over that
partition too. Finally both brokers restart and must rejoin the ISR.

Usage: python3 scripts/tests/test_leader_failover_m8.py [--iterations N]
Requires: confluent-kafka, a built ./build/tools/kawasan-broker, cluster_harness.sh.
"""
import argparse
import os
import signal
import subprocess
import sys
import time
import uuid

try:
    from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition
    from confluent_kafka.admin import AdminClient, NewTopic
except ImportError:
    print("SKIP: confluent-kafka not installed", file=sys.stderr)
    sys.exit(0)

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
HARNESS = os.path.join(ROOT, "scripts", "tests", "cluster_harness.sh")
BASE = os.environ.get("BASE", "/tmp/kawasan-cluster")
N_BROKERS = 3
ALL_BROKERS = ",".join(f"127.0.0.1:{9092 + i * 100}" for i in range(N_BROKERS))
LIVENESS_MS = 3000
FAILOVER_BUDGET_S = 20

ENV = dict(os.environ, N=str(N_BROKERS), LAG_MS="3000", MIN_ISR="2",
           LIVENESS_MS=str(LIVENESS_MS))


def harness(*args):
    return subprocess.run(["bash", HARNESS, *args], cwd=ROOT, env=ENV,
                          capture_output=True, text=True)


class Failure(Exception):
    pass


def log(msg):
    print(f"[m8] {msg}", flush=True)


def sigkill(broker_id):
    pidf = os.path.join(BASE, f"broker-{broker_id}", "broker.pid")
    with open(pidf) as f:
        pid = int(f.read().strip())
    os.kill(pid, signal.SIGKILL)
    os.remove(pidf)
    log(f"SIGKILLed broker {broker_id} (pid {pid})")


def bootstrap(alive):
    return ",".join(f"127.0.0.1:{9092 + i * 100}" for i in sorted(alive))


def metadata(alive, topic):
    admin = AdminClient({"bootstrap.servers": bootstrap(alive)})
    return admin.list_topics(topic=topic, timeout=10)


def produce_acks_all(alive, topic, partition, values, timeout_ms=20000):
    acked = []

    def cb(err, msg):
        if err is None:
            acked.append((msg.offset(), msg.value().decode()))

    p = Producer({
        "bootstrap.servers": bootstrap(alive),
        "acks": "all",
        "message.timeout.ms": timeout_ms,
        "request.timeout.ms": 5000,
        "enable.idempotence": True,
        "topic.metadata.refresh.interval.ms": 1000,
    })
    for v in values:
        p.produce(topic, value=v.encode(), partition=partition, on_delivery=cb)
    p.flush(timeout_ms / 1000 + 5)
    return acked


def consume_all(alive, topic, partition, expected_count, timeout_s=30):
    c = Consumer({
        "bootstrap.servers": bootstrap(alive),
        "group.id": "m8-check-" + uuid.uuid4().hex[:6],
        "enable.auto.commit": False,
        "topic.metadata.refresh.interval.ms": 1000,
    })
    c.assign([TopicPartition(topic, partition, 0)])
    got = {}
    deadline = time.time() + timeout_s
    while time.time() < deadline and len(got) < expected_count:
        m = c.poll(0.5)
        if m is None or m.error():
            continue
        got[m.offset()] = m.value().decode()
    c.close()
    return got


def wait_for_new_leader(alive, topic, partition, dead):
    deadline = time.time() + FAILOVER_BUDGET_S
    start = time.time()
    while time.time() < deadline:
        try:
            pm = metadata(alive, topic).topics[topic].partitions[partition]
            if pm.leader >= 0 and pm.leader not in dead:
                return pm, time.time() - start
        except Exception:  # noqa: BLE001 — the broker may be mid-failover
            pass
        time.sleep(0.5)
    raise Failure(f"no new leader for {topic}-{partition} within {FAILOVER_BUDGET_S}s")


def check_acked_readable(alive, topic, partition, acked):
    got = consume_all(alive, topic, partition, len(acked))
    missing = [(o, v) for o, v in acked if got.get(o) != v]
    if missing:
        raise Failure(f"{len(missing)} acked records missing/different on {topic}-{partition}, "
                      f"e.g. {missing[:3]}")


def wait_isr_full(alive, topic, timeout_s=40):
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        md = metadata(alive, topic)
        parts = md.topics[topic].partitions
        if all(sorted(pm.isrs) == list(range(N_BROKERS)) for pm in parts.values()):
            return
        time.sleep(1)
    md = metadata(alive, topic)
    raise Failure("restarted brokers did not rejoin every ISR: " +
                  str({p: list(pm.isrs) for p, pm in md.topics[topic].partitions.items()}))


def run_once():
    up = harness("up")
    if "leader election observed" not in up.stdout:
        raise Failure("no Raft leader elected")
    alive = set(range(N_BROKERS))
    topic = "m8-failover-" + uuid.uuid4().hex[:8]
    admin = AdminClient({"bootstrap.servers": ALL_BROKERS})
    admin.create_topics([NewTopic(topic, num_partitions=3, replication_factor=3,
                                  config={"min.insync.replicas": "2"})])[topic].result(30)
    time.sleep(2)

    acked = {p: produce_acks_all(alive, topic, p, [f"p{p}-a{i}" for i in range(20)])
             for p in range(3)}
    for p in range(3):
        if len(acked[p]) != 20:
            raise Failure(f"baseline acks=all delivered {len(acked[p])}/20 to partition {p}")
    md = metadata(alive, topic)
    controller = md.controller_id
    leaders = {p: pm.leader for p, pm in md.topics[topic].partitions.items()}
    log(f"controller={controller}, leaders={leaders}; 60 records acked")

    # Scenario 1: a non-controller partition leader dies.
    p1 = next(p for p, l in leaders.items() if l != controller)
    victim1 = leaders[p1]
    sigkill(victim1)
    alive.discard(victim1)
    pm, took = wait_for_new_leader(alive, topic, p1, {victim1})
    log(f"partition {p1}: leader {victim1} -> {pm.leader} in {took:.1f}s, isr={list(pm.isrs)}")
    more = produce_acks_all(alive, topic, p1, [f"p{p1}-b{i}" for i in range(10)])
    if len(more) != 10:
        raise Failure(f"acks=all after failover delivered {len(more)}/10")
    acked[p1] += more
    check_acked_readable(alive, topic, p1, acked[p1])
    log(f"partition {p1}: produce resumed; all {len(acked[p1])} acked records readable")

    # Scenario 2: the controller (also a partition leader) dies. Only one
    # broker would remain, so first bring the first victim back.
    harness("restart", str(victim1))
    alive.add(victim1)
    wait_isr_full(alive, topic)
    md = metadata(alive, topic)
    controller = md.controller_id
    leaders = {p: pm.leader for p, pm in md.topics[topic].partitions.items()}
    led = [p for p, l in leaders.items() if l == controller]
    log(f"broker {victim1} rejoined every ISR; controller={controller}, leaders={leaders}")
    sigkill(controller)
    alive.discard(controller)
    for p in led:
        pm, took = wait_for_new_leader(alive, topic, p, {controller})
        log(f"partition {p}: controller-leader {controller} -> {pm.leader} in {took:.1f}s")
        more = produce_acks_all(alive, topic, p, [f"p{p}-c{i}" for i in range(10)])
        if len(more) != 10:
            raise Failure(f"acks=all after controller failover delivered {len(more)}/10")
        acked[p] += more
    for p in range(3):
        check_acked_readable(alive, topic, p, acked[p])
    log("controller failover OK; all acked records readable on every partition")

    harness("restart", str(controller))
    alive.add(controller)
    wait_isr_full(alive, topic)
    for p in range(3):
        check_acked_readable(alive, topic, p, acked[p])
    log("all brokers back in every ISR; all acked records readable")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--iterations", type=int, default=1)
    args = ap.parse_args()
    if not os.path.exists(os.path.join(ROOT, "build", "tools", "kawasan-broker")):
        print("SKIP: build/tools/kawasan-broker not found", file=sys.stderr)
        sys.exit(0)
    for i in range(args.iterations):
        log(f"iteration {i + 1}/{args.iterations}")
        try:
            run_once()
        except (Failure, KafkaException) as e:
            print(f"FAIL (iteration {i + 1}): {e}", file=sys.stderr)
            for b in range(N_BROKERS):
                print(f"--- broker {b} log tail ---", file=sys.stderr)
                print(harness("logs", str(b)).stdout[-1500:], file=sys.stderr)
            keep = f"/tmp/m8-failure-{int(time.time())}"
            subprocess.run(["cp", "-R", BASE, keep])
            print(f"cluster state preserved in {keep}", file=sys.stderr)
            harness("down")
            sys.exit(1)
        harness("down")
    print(f"PASS: M8 failover verified ({args.iterations} iteration(s))")


if __name__ == "__main__":
    main()
