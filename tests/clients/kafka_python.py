"""Keep kafka-python as a checked baseline rather than a skipped matrix leg."""
import json
import os
import uuid
from kafka import KafkaProducer, KafkaConsumer, TopicPartition
from kafka.admin import KafkaAdminClient, NewTopic

rows = []
try:
    name = "cm1-python-" + uuid.uuid4().hex
    bootstrap = os.environ.get("BOOTSTRAP", "localhost:9092")
    a = KafkaAdminClient(bootstrap_servers=bootstrap)
    try:
        a.create_topics([NewTopic(name, 1, 1)])
    finally:
        a.close()
    p = KafkaProducer(bootstrap_servers=bootstrap, acks="all")
    for i in range(10):
        assert p.send(name, str(i).encode(), partition=0).get(30).offset == i
    p.close()
    c = KafkaConsumer(bootstrap_servers=bootstrap, enable_auto_commit=False, consumer_timeout_ms=10000)
    c.assign([TopicPartition(name, 0)]); c.seek_to_beginning()
    assert [m.value for m in c] == [str(i).encode() for i in range(10)]
    c.close()
    rows.append({"case": "produce-consume", "status": "PASS"})
except Exception as e:
    import traceback
    traceback.print_exc()
    rows.append({"case": "produce-consume", "status": "FAIL", "error": type(e).__name__ + ": " + str(e)})
with open(os.environ.get("RESULTS", "/evidence/results.jsonl"), "w") as f:
    for row in rows:
        f.write(json.dumps(row) + "\n")
