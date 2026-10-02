#!/usr/bin/env python3
"""M8-G: consumers bootstrapped at different brokers form exactly one group."""
import os
import shutil
import socket
import struct
import subprocess
import sys
import time
import uuid

from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient, NewTopic

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
HARNESS = os.path.join(ROOT, "scripts", "tests", "cluster_harness.sh")
BASE = os.environ.get("BASE", "/tmp/kawasan-group-routing")
ENV = dict(os.environ, BASE=BASE, N="3")


def harness(*args):
    return subprocess.run(["bash", HARNESS, *args], env=ENV, cwd=ROOT,
                          capture_output=True, text=True)


def string(value):
    data = value.encode()
    return struct.pack(">h", len(data)) + data


class Reader:
    def __init__(self, data):
        self.data = data
        self.pos = 0

    def number(self, fmt):
        n = struct.calcsize(fmt)
        value = struct.unpack_from(fmt, self.data, self.pos)[0]
        self.pos += n
        return value

    def string(self):
        n = self.number(">h")
        if n < 0:
            return None
        result = self.data[self.pos:self.pos + n].decode()
        self.pos += n
        return result


def rpc(broker, api, version, body=b""):
    request = struct.pack(">hhi", api, version, 1) + string("m8-group") + body
    with socket.create_connection(("127.0.0.1", 9092 + broker * 100), timeout=5) as sock:
        sock.sendall(struct.pack(">i", len(request)) + request)

        def read(n):
            out = b""
            while len(out) < n:
                chunk = sock.recv(n - len(out))
                if not chunk:
                    raise RuntimeError("broker closed the response")
                out += chunk
            return out

        size = struct.unpack(">i", read(4))[0]
        r = Reader(read(size))
        assert r.number(">i") == 1
        return r


def coordinator(broker, group):
    r = rpc(broker, 10, 1, string(group) + b"\0")
    r.number(">i")  # throttle
    error = r.number(">h")
    r.string()  # error message
    owner = r.number(">i")
    host, port = r.string(), r.number(">i")
    if error == 15:
        return None
    assert error == 0, (broker, error)
    assert host == "127.0.0.1" and port == 9092 + owner * 100
    return owner


def groups(broker):
    r = rpc(broker, 16, 0)
    assert r.number(">h") == 0
    result = []
    for _ in range(r.number(">i")):
        result.append(r.string())
        r.string()
    return result


def main():
    consumers = []
    try:
        assert "leader election observed" in harness("up").stdout
        group = "m8-group-" + uuid.uuid4().hex
        topic = "m8-group-topic-" + uuid.uuid4().hex[:8]
        admin = AdminClient({"bootstrap.servers": "127.0.0.1:9092,127.0.0.1:9192,127.0.0.1:9292"})
        for attempt in range(20):
            try:
                admin.create_topics([NewTopic(topic, num_partitions=3,
                                               replication_factor=3)])[topic].result(10)
                break
            except Exception as exc:
                if "NOT_CONTROLLER" not in str(exc) or attempt == 19:
                    raise
                time.sleep(0.5)
        ready_until = time.monotonic() + 20
        while True:
            owners = [coordinator(i, group) for i in range(3)]
            if all(owner is not None for owner in owners) and len(set(owners)) == 1:
                break
            assert time.monotonic() < ready_until, owners
            time.sleep(0.2)
        for i in (0, 2):
            c = Consumer({"bootstrap.servers": f"127.0.0.1:{9092 + i * 100}",
                          "group.id": group, "enable.auto.commit": False,
                          "partition.assignment.strategy": "range",
                          "session.timeout.ms": 6000})
            c.subscribe([topic])
            consumers.append(c)
        deadline = time.monotonic() + 45
        while time.monotonic() < deadline:
            for c in consumers:
                c.poll(0.1)
            assignments = [{p.partition for p in c.assignment()} for c in consumers]
            if all(assignments) and not (assignments[0] & assignments[1]) and \
                    assignments[0] | assignments[1] == {0, 1, 2}:
                break
        else:
            raise AssertionError(f"one group did not converge: {assignments}")
        listed = [i for i in range(3) if group in groups(i)]
        assert listed == [owners[0]], (owners, listed)
        print(f"PASS: two consumers form one group on broker {owners[0]}; "
              f"assignments={assignments}", flush=True)
    except Exception:
        keep = f"/tmp/m8-group-failure-{int(time.time())}"
        if os.path.exists(BASE):
            shutil.copytree(BASE, keep)
        print(f"cluster state preserved in {keep}", file=sys.stderr)
        raise
    finally:
        for c in consumers:
            c.close()
        harness("down")


if __name__ == "__main__":
    main()
