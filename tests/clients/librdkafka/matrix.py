"""Real librdkafka client cases; every acknowledgement and visible value is checked."""
import json
import os
import time
import uuid

import confluent_kafka as ck
from confluent_kafka.admin import AdminClient, NewTopic

BOOTSTRAP = os.environ.get("BOOTSTRAP", "localhost:9092")
RUN = "cm1-" + uuid.uuid4().hex
BASE = {"bootstrap.servers": BOOTSTRAP}
print("confluent-kafka", ck.version(), "librdkafka", ck.libversion(), flush=True)
package_version = ck.version()
assert (package_version[0] if isinstance(package_version, tuple) else package_version) == os.environ["CLIENT_VERSION"]
assert ck.libversion()[0] == os.environ["CLIENT_VERSION"]


def topic(suffix):
    name = RUN + "-" + suffix
    admin = AdminClient(BASE)
    admin.create_topics([NewTopic(name, 1, 1)])[name].result(30)
    return name


def producer(txn=None):
    cfg = dict(BASE, **{"enable.idempotence": True, "acks": "all", "message.timeout.ms": 30000})
    if txn:
        cfg.update({"transactional.id": txn, "transaction.timeout.ms": 30000})
    return ck.Producer(cfg)


def send(p, t, value):
    delivered = []
    p.produce(t, partition=0, key=b"key", value=value.encode(), on_delivery=lambda err, msg: delivered.append((err, msg.offset())))
    assert p.flush(30) == 0, "undelivered messages"
    assert len(delivered) == 1 and delivered[0][0] is None, repr(delivered)
    return delivered[0][1]


def consumer(group):
    return ck.Consumer(dict(BASE, **{"group.id": group, "auto.offset.reset": "earliest", "enable.auto.commit": False, "isolation.level": "read_committed"}))


def scan(t, expected):
    c = consumer(RUN + "-scan-" + t)
    c.assign([ck.TopicPartition(t, 0, ck.OFFSET_BEGINNING)])
    values = []
    end = time.monotonic() + 30
    last = time.monotonic()
    try:
        while time.monotonic() < end:
            m = c.poll(0.25)
            if m:
                assert not m.error(), m.error()
                values.append(m.value().decode())
                last = time.monotonic()
            if len(values) >= expected and time.monotonic() - last > 2:
                break
        return values
    finally:
        c.close()


def admin_topics():
    t = topic("admin")
    a = AdminClient(BASE)
    assert t in a.list_topics(timeout=30).topics
    assert len(a.list_topics(t, timeout=30).topics[t].partitions) == 1
    a.delete_topics([t])[t].result(30)
    end = time.monotonic() + 30
    while t in a.list_topics(timeout=10).topics and time.monotonic() < end:
        time.sleep(0.1)
    assert t not in a.list_topics(timeout=10).topics


def produce_consume():
    t = topic("roundtrip")
    p = producer()
    for i in range(10):
        assert send(p, t, "value-" + str(i)) == i
    assert scan(t, 10) == ["value-" + str(i) for i in range(10)]


def transactions():
    t = topic("txn")
    p = producer(RUN + "-txn")
    p.init_transactions(30)
    p.begin_transaction(); send(p, t, "committed"); p.commit_transaction(30)
    p.begin_transaction(); send(p, t, "aborted"); p.abort_transaction(30)
    assert scan(t, 1) == ["committed"]


def eos_offsets():
    inp, out = topic("eos-in"), topic("eos-out")
    send(producer(), inp, "input")
    c = consumer(RUN + "-eos-group")
    p = producer(RUN + "-eos")
    c.subscribe([inp])
    try:
        end = time.monotonic() + 30
        m = None
        while m is None and time.monotonic() < end:
            m = c.poll(0.25)
        assert m is not None and not m.error(), repr(m)
        p.init_transactions(30)
        p.begin_transaction(); send(p, out, "committed")
        p.send_offsets_to_transaction([ck.TopicPartition(inp, 0, m.offset() + 1)], c.consumer_group_metadata(), 30)
        p.commit_transaction(30)
        assert c.committed([ck.TopicPartition(inp, 0)], 30)[0].offset == 1
        p.begin_transaction(); send(p, out, "aborted")
        p.send_offsets_to_transaction([ck.TopicPartition(inp, 0, 2)], c.consumer_group_metadata(), 30)
        p.abort_transaction(30)
        assert c.committed([ck.TopicPartition(inp, 0)], 30)[0].offset == 1
    finally:
        c.close()
    assert scan(out, 1) == ["committed"]


CASES = {"admin-topics": admin_topics, "produce-consume": produce_consume, "transactions": transactions, "eos-offsets": eos_offsets}
with open(os.environ.get("RESULTS", "/evidence/results.jsonl"), "w") as results:
    for name, test in CASES.items():
        try:
            test()
            row = {"case": name, "status": "PASS"}
        except Exception as e:
            import traceback
            traceback.print_exc()
            row = {"case": name, "status": "FAIL", "error": type(e).__name__ + ": " + str(e)}
        results.write(json.dumps(row) + "\n")
        results.flush()
