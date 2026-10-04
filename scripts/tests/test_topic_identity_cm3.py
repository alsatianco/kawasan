#!/usr/bin/env python3
"""CM-3: real Raft-created UUIDs converge, survive restart, and change on recreation."""
import json
import os
from pathlib import Path
import socket
import subprocess
import time
import uuid
import urllib.request
from confluent_kafka.admin import AdminClient, NewTopic

ROOT = Path(__file__).resolve().parents[2]
BASE = Path(os.environ.get("BASE", "/tmp/kawasan-topic-identity-cm3"))
ENV = dict(os.environ, BASE=str(BASE), N="3", KEEP_DATA="1", KAFKA_BASE="19092",
           RAFT_BASE="19093", MON_BASE="19094")
BOOTSTRAP = ",".join(f"127.0.0.1:{19092 + i*100}" for i in range(3))


def harness(*args):
    r = subprocess.run(["bash", str(ROOT / "scripts/tests/cluster_harness.sh"), *args],
                       env=ENV, cwd=ROOT, capture_output=True, text=True, timeout=120)
    print(r.stdout, flush=True)
    if r.returncode:
        raise RuntimeError(r.stderr or r.stdout)


def eventually(fn, seconds=45):
    end = time.monotonic() + seconds
    last = None
    while time.monotonic() < end:
        try:
            return fn()
        except (AssertionError, OSError, json.JSONDecodeError) as e:
            last = e
            time.sleep(0.1)
    raise AssertionError(f"condition did not converge: {last}")


def ids(topic):
    result = []
    for i in range(3):
        with socket.create_connection(("127.0.0.1", 19092+i*100), timeout=1):
            pass
        path = BASE / f"broker-{i}" / "data/meta/topics.json"
        topics = json.loads(path.read_text())["topics"]
        found = [t["topic_id"] for t in topics if t["name"] == topic]
        assert len(found) == 1, (i, found)
        assert len(found[0]) == 16 and any(found[0]), (i, found)
        result.append(found[0])
    assert result[0] == result[1] == result[2], result
    return result[0]


def deleted(topic):
    for i in range(3):
        topics = json.loads((BASE / f"broker-{i}" / "data/meta/topics.json").read_text())["topics"]
        assert all(t["name"] != topic for t in topics)


def main():
    assert not BASE.exists(), f"use a fresh evidence BASE; retained data at {BASE}"
    topic = "cm3-identity-" + uuid.uuid4().hex
    try:
        harness("up")
        # Metadata ownership must converge as well as the listener and UUID files.
        def ready():
            for i in range(3):
                with urllib.request.urlopen(f"http://127.0.0.1:{19094+i*100}/ready", timeout=1) as r:
                    assert r.status == 200
        eventually(ready)
        admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
        admin.create_topics([NewTopic(topic, 1, 3)])[topic].result(45)
        before = eventually(lambda: ids(topic))
        owner = admin.list_topics(timeout=10).controller_id
        assert owner in range(3), owner
        harness("kill9", str(owner))
        harness("restart", str(owner))
        after = eventually(lambda: ids(topic))
        assert before == after, (before, after)
        eventually(ready)
        admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
        admin.delete_topics([topic])[topic].result(45)
        eventually(lambda: deleted(topic))
        admin.create_topics([NewTopic(topic, 1, 3)])[topic].result(45)
        recreated = eventually(lambda: ids(topic))
        assert recreated != before
        (BASE / "result.json").write_text(json.dumps({"status": "PASS", "restarted_controller": owner, "before": before,
                                                     "after": after, "recreated": recreated}, indent=2))
        print("PASS: three-broker UUID convergence, restart, and recreation", flush=True)
    finally:
        if BASE.exists():
            harness("down")


if __name__ == "__main__":
    main()
