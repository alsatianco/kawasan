#!/usr/bin/env python3
"""CM-3: committed topic configs converge and survive controller failover."""
import json
import os
import uuid
import time
from confluent_kafka import KafkaException, KafkaError
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from confluent_kafka.admin import AdminClient, NewTopic, ConfigResource, ResourceType, ConfigEntry, AlterConfigOpType, ConfigSource
import test_topic_identity_cm3 as cluster


def admin(broker=None):
    return AdminClient({"bootstrap.servers": cluster.BOOTSTRAP if broker is None else f"127.0.0.1:{19092+broker*100}", "socket.timeout.ms": 5000, "reconnect.backoff.ms": 100, "reconnect.backoff.max.ms": 1000})


def mutate(topic, key, value, operation=AlterConfigOpType.SET, validate_only=False, broker=None):
    resource = ConfigResource(ResourceType.TOPIC, topic)
    resource.add_incremental_config(ConfigEntry(key, value, incremental_operation=operation))
    deadline = time.monotonic() + 45
    while True:
        client = admin(broker)
        try:
            client.incremental_alter_configs([resource], validate_only=validate_only, request_timeout=10)[resource].result(15)
            return
        except KafkaException as error:
            # A controller election can reject preflight. Retry the explicit
            # ownership error, and require an actual successful commit afterward.
            if error.args[0].code() != KafkaError.NOT_CONTROLLER or time.monotonic() >= deadline:
                raise
            print(f"retry controller election: {error}", flush=True)
            time.sleep(0.2)



def state(topic, expected):
    for i in range(3):
        path = cluster.BASE / f"broker-{i}" / "data/meta/topics.json"
        found = [t for t in json.loads(path.read_text())["topics"] if t["name"] == topic]
        assert len(found) == 1
        assert found[0]["configs"] == expected, (i, found[0]["configs"], expected)
        resource = ConfigResource(ResourceType.TOPIC, topic)
        client = admin(i)
        described = client.describe_configs([resource])[resource].result(10)
        for key, value in expected.items():
            assert described[key].value == value, (i, key, described[key].value)
            assert described[key].source == ConfigSource.DYNAMIC_TOPIC_CONFIG.value, (i, key, described[key].source)


def ready():
    for i in range(3):
        with urllib.request.urlopen(f"http://127.0.0.1:{19094+i*100}/ready", timeout=1) as response:
            assert response.status == 200


def main():
    assert not cluster.BASE.exists(), "Use a fresh evidence BASE"
    topic = "cm3-config-" + uuid.uuid4().hex
    try:
        cluster.harness("up")
        cluster.eventually(ready)
        client = admin()
        client.create_topics([NewTopic(topic, 1, 3)])[topic].result(45)
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(mutate, topic, "retention.ms", "300000"),
                       pool.submit(mutate, topic, "segment.bytes", "4096")]
            for future in futures:
                future.result()
        expected = {"retention.ms": "300000", "segment.bytes": "4096"}
        cluster.eventually(lambda: state(topic, expected))
        mutate(topic, "retention.ms", "400000", validate_only=True)
        cluster.eventually(lambda: state(topic, expected))
        owner = client.list_topics(timeout=10).controller_id
        assert owner in range(3)
        client = None
        cluster.harness("kill9", str(owner))
        def new_controller():
            for broker in range(3):
                if broker == owner:
                    continue
                client = admin(broker)
                candidate = client.list_topics(timeout=3).controller_id
                if candidate in range(3) and candidate != owner:
                    return candidate
            raise AssertionError("waiting for surviving brokers to report the new controller")
        new_owner = cluster.eventually(new_controller)
        # Commit a delta under the new controller while the old one is down.
        mutate(topic, "retention.bytes", "65536", broker=new_owner)
        expected["retention.bytes"] = "65536"
        cluster.harness("restart", str(owner))
        cluster.eventually(ready)
        cluster.eventually(lambda: state(topic, expected))
        mutate(topic, "retention.ms", None, AlterConfigOpType.DELETE)
        del expected["retention.ms"]
        cluster.eventually(lambda: state(topic, expected))
        (cluster.BASE / "result.json").write_text(json.dumps({"status": "PASS", "restarted_controller": owner, "new_controller": new_owner,
            "checks": ["concurrent-deltas", "validate-only", "controller-kill", "new-controller-write", "restart-replay", "override-delete"],
            "configs": expected}, indent=2))
        print("PASS: replicated topic configs through controller failover", flush=True)
    finally:
        if cluster.BASE.exists():
            cluster.harness("down")


if __name__ == "__main__":
    main()
