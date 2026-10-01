#!/usr/bin/env python3
"""M8-E1 end-to-end test: a restarted ex-leader must not serve on stale metadata.

3 brokers, RF=3 topic. SIGKILL broker X, the leader of partition P, and wait for
the controller to fail P over. Then FREEZE (SIGSTOP) the other two brokers and
restart X. X's on-disk metadata still names it the leader of P, but it cannot
reach a Raft leader, so it must not prove its view current. It must refuse an
acks=1 produce to P (NOT_LEADER_FOR_PARTITION) and report not-ready on /ready.
After resuming the cluster, X catches up, becomes ready, and the refused record
appears nowhere.

Usage: python3 scripts/tests/test_stale_leader_fencing_m8.py
Requires: confluent-kafka, a built ./build/tools/kawasan-broker, cluster_harness.sh.
"""
import os
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid

try:
    from confluent_kafka import Consumer, Producer, TopicPartition
    from confluent_kafka.admin import AdminClient, NewTopic
except ImportError:
    print("SKIP: confluent-kafka not installed", file=sys.stderr)
    sys.exit(0)

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
HARNESS = os.path.join(ROOT, "scripts", "tests", "cluster_harness.sh")
BASE = os.environ.get("BASE", "/tmp/kawasan-cluster")
N = 3
ENV = dict(os.environ, N=str(N), LAG_MS="3000", LIVENESS_MS="3000")


def harness(*args):
    return subprocess.run(["bash", HARNESS, *args], cwd=ROOT, env=ENV,
                          capture_output=True, text=True)


def log(msg):
    print(f"[e1] {msg}", flush=True)


def fail(msg):
    print(f"FAIL: {msg}", file=sys.stderr)
    for b in range(N):
        print(f"--- broker {b} ---\n" + harness("logs", str(b)).stdout[-1200:], file=sys.stderr)
    for b in range(N):
        harness("resume", str(b))
    harness("down")
    sys.exit(1)


def addr(b):
    return f"127.0.0.1:{9092 + b * 100}"


def ready(b):
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{9094 + b * 100}/ready", timeout=2) as r:
            return r.status == 200
    except (urllib.error.URLError, OSError):
        return False


def produce(bootstrap, topic, partition, value, acks, timeout_ms):
    result = {}

    def cb(err, msg):
        result["err"] = err

    p = Producer({"bootstrap.servers": bootstrap, "acks": acks,
                  "message.timeout.ms": timeout_ms, "enable.idempotence": False,
                  "retries": 1000, "retry.backoff.ms": 200})
    p.produce(topic, value=value.encode(), partition=partition, on_delivery=cb)
    p.flush(timeout_ms / 1000 + 5)
    return result.get("err", "no delivery report")


def values_in(bootstrap, topic, partition, timeout_s=15):
    c = Consumer({"bootstrap.servers": bootstrap, "group.id": "e1-" + uuid.uuid4().hex[:6],
                  "enable.auto.commit": False})
    c.assign([TopicPartition(topic, partition, 0)])
    out = []
    idle_until = time.time() + timeout_s
    while time.time() < idle_until:
        m = c.poll(0.5)
        if m is not None and not m.error():
            out.append(m.value().decode())
            idle_until = min(idle_until, time.time() + 3)
    c.close()
    return out


def main():
    if not os.path.exists(os.path.join(ROOT, "build", "tools", "kawasan-broker")):
        print("SKIP: build/tools/kawasan-broker not found", file=sys.stderr)
        sys.exit(0)
    if "leader election observed" not in harness("up").stdout:
        fail("no Raft leader elected")
    everyone = ",".join(addr(b) for b in range(N))
    topic = "e1-fence-" + uuid.uuid4().hex[:8]
    admin = AdminClient({"bootstrap.servers": everyone})
    admin.create_topics([NewTopic(topic, num_partitions=3, replication_factor=3)])[topic].result(30)
    time.sleep(2)

    md = admin.list_topics(topic=topic, timeout=10)
    controller = md.controller_id
    p, x = next((p, pm.leader) for p, pm in md.topics[topic].partitions.items()
                if pm.leader != controller)
    others = [b for b in range(N) if b != x]
    if produce(everyone, topic, p, "baseline", "all", 15000) is not None:
        fail("baseline acks=all produce failed")
    log(f"controller={controller}; partition {p} led by {x}")

    with open(os.path.join(BASE, f"broker-{x}", "broker.pid")) as f:
        os.kill(int(f.read().strip()), signal.SIGKILL)
    deadline = time.time() + 20
    while time.time() < deadline:
        try:
            leader = AdminClient({"bootstrap.servers": ",".join(addr(b) for b in others)}) \
                .list_topics(topic=topic, timeout=5).topics[topic].partitions[p].leader
            if leader >= 0 and leader != x:
                break
        except Exception:  # noqa: BLE001
            pass
        time.sleep(0.5)
    else:
        fail(f"partition {p} was not failed over after killing {x}")
    log(f"killed {x}; partition {p} failed over to {leader}")

    for b in others:
        harness("pause", str(b))
    harness("restart", str(x))
    time.sleep(6)  # X is up, loaded its stale metadata, and cannot reach a leader
    if ready(x):
        fail(f"restarted broker {x} reports ready while it cannot reach the cluster")
    err = produce(addr(x), topic, p, "stale-write", "1", 6000)
    if err is None:
        fail(f"restarted broker {x} ACCEPTED a write on stale leadership of partition {p}")
    log(f"isolated restarted broker {x} is not ready and refused the write ({err})")

    for b in others:
        harness("resume", str(b))
    deadline = time.time() + 30
    while time.time() < deadline and not ready(x):
        time.sleep(0.5)
    if not ready(x):
        fail(f"broker {x} never became ready after the cluster resumed")
    vals = values_in(everyone, topic, p)
    if "baseline" not in vals:
        fail(f"baseline record missing from partition {p}: {vals}")
    if "stale-write" in vals:
        fail(f"refused stale write is present in partition {p}: {vals}")
    log(f"broker {x} caught up and is ready; partition {p} holds {vals}")
    harness("down")
    print("PASS: M8-E1 verified (a restarted ex-leader refuses writes until its metadata is "
          "current)")


if __name__ == "__main__":
    main()
