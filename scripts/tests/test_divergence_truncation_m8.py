#!/usr/bin/env python3
"""M8 end-to-end test: a deposed leader's divergent tail is truncated (KIP-101).

3 brokers, RF=3 topic, partition P led by its preferred replica L.
  1. Freeze (SIGSTOP) both followers and immediately write acks=1 records to L.
     L accepts a few (until its metadata lease expires); they exist ONLY on L.
  2. Freeze L, thaw the followers: the controller fails P over to a follower N
     (new leader epoch), and acks=all records are committed on N at the same
     offsets L's divergent records occupy.
  3. Thaw L. It must notice the new leadership, ask N where its epoch ends
     (OffsetForLeaderEpoch), truncate its divergent tail, re-replicate, and
     rejoin the ISR.
  4. Make L the leader again (preferred election) and read P from it: every
     acks=all record is there at its acked offset, and no divergent record is.

Usage: python3 scripts/tests/test_divergence_truncation_m8.py
Requires: confluent-kafka, a built ./build/tools/kawasan-broker, cluster_harness.sh.
"""
import os
import subprocess
import sys
import time
import uuid

try:
    from confluent_kafka import Consumer, ElectionType, Producer, TopicPartition
    from confluent_kafka.admin import AdminClient, NewTopic
except ImportError:
    print("SKIP: confluent-kafka (with ElectionType) not installed", file=sys.stderr)
    sys.exit(0)

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
HARNESS = os.path.join(ROOT, "scripts", "tests", "cluster_harness.sh")
BASE = os.environ.get("BASE", "/tmp/kawasan-cluster")
N = 3
ENV = dict(os.environ, N=str(N), LAG_MS="3000", LIVENESS_MS="6000", MIN_ISR="1")


def harness(*args):
    return subprocess.run(["bash", HARNESS, *args], cwd=ROOT, env=ENV,
                          capture_output=True, text=True)


def log(msg):
    print(f"[div] {msg}", flush=True)


def fail(msg):
    print(f"FAIL: {msg}", file=sys.stderr)
    for b in range(N):
        harness("resume", str(b))
    keep = f"/tmp/m8-divergence-failure-{int(time.time())}"
    subprocess.run(["cp", "-R", BASE, keep])
    print(f"cluster state preserved in {keep}", file=sys.stderr)
    harness("down")
    sys.exit(1)


def addr(b):
    return f"127.0.0.1:{9092 + b * 100}"


def bootstrap(brokers):
    return ",".join(addr(b) for b in brokers)


def partition_meta(brokers, topic, p):
    try:
        md = AdminClient({"bootstrap.servers": bootstrap(brokers)}).list_topics(topic=topic,
                                                                               timeout=5)
        return md.topics[topic].partitions[p]
    except Exception:  # noqa: BLE001 — brokers may be mid-failover
        return None


def produce(brokers, topic, p, values, acks, timeout_ms):
    acked = []

    def cb(err, msg):
        if err is None:
            acked.append((msg.offset(), msg.value().decode()))

    prod = Producer({"bootstrap.servers": bootstrap(brokers), "acks": acks,
                     "message.timeout.ms": timeout_ms, "enable.idempotence": False,
                     "linger.ms": 0, "retries": 0 if acks == "1" else 100,
                     "topic.metadata.refresh.interval.ms": 1000})
    for v in values:
        prod.produce(topic, value=v.encode(), partition=p, on_delivery=cb)
        if acks == "1":
            prod.poll(0)
    prod.flush(timeout_ms / 1000 + 5)
    return acked


def read_all(brokers, topic, p, timeout_s=20):
    c = Consumer({"bootstrap.servers": bootstrap(brokers), "group.id": "div-" + uuid.uuid4().hex[:6],
                  "enable.auto.commit": False})
    c.assign([TopicPartition(topic, p, 0)])
    out = {}
    idle_until = time.time() + timeout_s
    while time.time() < idle_until:
        m = c.poll(0.5)
        if m is not None and not m.error():
            out[m.offset()] = m.value().decode()
            idle_until = min(idle_until, time.time() + 3)
    c.close()
    return out


def wait_until(pred, timeout_s, what):
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        v = pred()
        if v:
            return v
        time.sleep(0.5)
    fail(f"timed out waiting for {what}")


def main():
    try:
        run()
    except Exception as e:  # noqa: BLE001 — always tear the cluster down
        fail(f"unexpected error: {e!r}")


def run():
    if not os.path.exists(os.path.join(ROOT, "build", "tools", "kawasan-broker")):
        print("SKIP: build/tools/kawasan-broker not found", file=sys.stderr)
        sys.exit(0)
    if "leader election observed" not in harness("up").stdout:
        fail("no Raft leader elected")
    everyone = list(range(N))
    topic = "m8-div-" + uuid.uuid4().hex[:8]
    admin = AdminClient({"bootstrap.servers": bootstrap(everyone)})
    for attempt in range(20):  # NOT_CONTROLLER right after startup is retriable
        try:
            admin.create_topics([NewTopic(topic, num_partitions=3,
                                          replication_factor=3)])[topic].result(30)
            break
        except Exception as e:  # noqa: BLE001
            if "NOT_CONTROLLER" not in str(e) or attempt == 19:
                raise
            time.sleep(1)
    time.sleep(2)

    md = AdminClient({"bootstrap.servers": bootstrap(everyone)}).list_topics(topic=topic,
                                                                            timeout=10)
    controller = md.controller_id
    # A partition whose leader is NOT the controller: freezing the followers
    # then leaves the controller (a follower) frozen too, so nothing can fail
    # the partition away from L until we choose to.
    p, pm = next((p, pm) for p, pm in md.topics[topic].partitions.items()
                 if pm.leader != controller and pm.leader == pm.replicas[0])
    leader = pm.leader
    followers = [b for b in everyone if b != leader]
    committed = produce(everyone, topic, p, [f"base-{i}" for i in range(10)], "all", 15000)
    if len(committed) != 10:
        fail(f"baseline acks=all delivered {len(committed)}/10")
    log(f"controller={controller}; partition {p} led by {leader}; 10 committed")

    # A producer already connected to L (metadata cached), so the divergent
    # writes land within L's metadata lease (half the liveness timeout).
    acked_div = []
    lone = Producer({"bootstrap.servers": addr(leader), "acks": "1", "linger.ms": 0,
                     "message.timeout.ms": 3000, "enable.idempotence": False, "retries": 0})
    lone.produce(topic, value=b"warm", partition=p)
    lone.flush(10)
    for b in followers:
        harness("pause", str(b))

    def cb(err, msg):
        if err is None:
            acked_div.append((msg.offset(), msg.value().decode()))

    for i in range(50):
        lone.produce(topic, value=f"div-{i}".encode(), partition=p, on_delivery=cb)
        lone.poll(0)
    lone.flush(6)
    divergent = acked_div
    log(f"followers frozen; leader {leader} accepted {len(divergent)} unreplicated acks=1 "
        f"records at offsets {[o for o, _ in divergent][:3]}...")
    if not divergent:
        fail("the isolated leader accepted no writes; cannot create divergence")

    harness("pause", str(leader))
    for b in followers:
        harness("resume", str(b))
    new = wait_until(lambda: (lambda m: m.leader if m and m.leader in followers else None)(
        partition_meta(followers, topic, p)), 30, f"failover of partition {p}")
    log(f"leader {leader} frozen; partition {p} failed over to {new}")
    after = produce(followers, topic, p, [f"new-{i}" for i in range(20)], "all", 20000)
    if len(after) != 20:
        fail(f"acks=all on the new leader delivered {len(after)}/20")
    committed += after
    overlap = {o for o, _ in divergent} & {o for o, _ in after}
    log(f"20 committed on {new}; {len(overlap)} offsets overlap the divergent tail")

    harness("resume", str(leader))
    wait_until(lambda: (lambda m: m and sorted(m.isrs) == everyone)(
        partition_meta(everyone, topic, p)), 60, f"broker {leader} to rejoin the ISR")
    log(f"broker {leader} is back in the ISR of partition {p}")

    def elect_preferred():
        md = AdminClient({"bootstrap.servers": bootstrap(everyone)}).list_topics(
            topic=topic, timeout=5)
        current = md.topics[topic].partitions[p]
        if leader not in current.isrs or md.controller_id < 0:
            return False
        controller_admin = AdminClient({"bootstrap.servers": addr(md.controller_id)})
        res = controller_admin.elect_leaders(
            ElectionType.PREFERRED, [TopicPartition(topic, p)]).result(5)
        for tp, err in (res or {}).items():
            if err is not None and "ELECTION_NOT_NEEDED" not in str(err):
                if any(code in str(err) for code in (
                        "NOT_CONTROLLER", "PREFERRED_LEADER_NOT_AVAILABLE")):
                    return False
                fail(f"preferred election failed: {err}")
        return True

    wait_until(elect_preferred, 30, "preferred election after controller ISR convergence")
    wait_until(lambda: (lambda m: m and m.leader == leader)(partition_meta(everyone, topic, p)),
               30, f"broker {leader} to lead partition {p} again")
    log(f"broker {leader} leads partition {p} again; reading it back")

    got = read_all(everyone, topic, p)
    missing = [(o, v) for o, v in committed if got.get(o) != v]
    leaked = sorted(v for v in got.values() if v.startswith("div-"))
    if missing:
        fail(f"{len(missing)} committed records missing/different, e.g. {missing[:3]}")
    if leaked:
        fail(f"divergent records survived on the old leader: {leaked[:5]}")
    harness("down")
    print(f"PASS: M8 divergence verified ({len(divergent)} divergent records truncated; "
          f"{len(committed)} committed records intact)")


if __name__ == "__main__":
    main()
