#!/usr/bin/env python3
# P1: consume throughput. Assumes the topic is already populated (run
# produce_perf.py first, or pass --prepopulate to seed it). Reports
# consume throughput_mb_s + msg_per_s with an environment fingerprint.
#
# Usage:
#   python3 bench/consume_perf.py --records 1000000 --record-size 1024 \
#       --topic <topic> [--consumers 8] [--prepopulate] [--host localhost:9092]

import argparse
import json
import sys
import threading
import time

from lib_bench import Producer, env_fingerprint


def prepopulate(client, host, topic, records, record_size):
    p = Producer(client, host)
    payload = b"x" * record_size
    for _ in range(records):
        p.send(topic, payload)
    p.flush()
    p.close()


def consume_confluent(host, topic, group, deadline_s, counter, idx, expected):
    # Explicit assignment from offset 0 across all partitions: a throughput
    # benchmark wants deterministic full-log reads, not group-rebalance
    # semantics. Stops early once `expected` messages have been read.
    from confluent_kafka import Consumer, TopicPartition, OFFSET_BEGINNING
    c = Consumer({
        "bootstrap.servers": host,
        "group.id": group,
        "enable.auto.commit": False,
    })
    md = c.list_topics(topic, timeout=10)
    parts = list(md.topics[topic].partitions.keys())
    c.assign([TopicPartition(topic, p, OFFSET_BEGINNING) for p in parts])
    n = 0
    end = time.perf_counter() + deadline_s
    while time.perf_counter() < end and (expected <= 0 or n < expected):
        msg = c.poll(0.5)
        if msg is None or msg.error():
            continue
        n += 1
    c.close()
    counter[idx] = n


def consume_kafka_python(host, topic, group, deadline_s, counter, idx, expected):
    from kafka import KafkaConsumer
    c = KafkaConsumer(
        topic, bootstrap_servers=host, group_id=group,
        auto_offset_reset="earliest",
        consumer_timeout_ms=int(deadline_s * 1000),
    )
    n = 0
    for _ in c:
        n += 1
        if expected > 0 and n >= expected:
            break
    c.close()
    counter[idx] = n


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--records", type=int, default=100000)
    ap.add_argument("--record-size", type=int, default=1024)
    ap.add_argument("--consumers", type=int, default=1)
    ap.add_argument("--client", choices=["confluent", "kafka-python"], default="confluent")
    ap.add_argument("--topic", required=True)
    ap.add_argument("--host", default="localhost:9092")
    ap.add_argument("--prepopulate", action="store_true")
    ap.add_argument("--deadline-s", type=float, default=60.0)
    args = ap.parse_args()

    if args.client == "confluent":
        try:
            import confluent_kafka  # noqa: F401
        except ImportError:
            print("# note: confluent-kafka unavailable; using kafka-python", file=sys.stderr)
            args.client = "kafka-python"

    if args.prepopulate:
        prepopulate(args.client, args.host, args.topic, args.records, args.record_size)

    group = f"bench-consume-{int(time.time())}"
    counter = [0] * args.consumers
    threads = []
    consume = consume_confluent if args.client == "confluent" else consume_kafka_python
    # Each consumer independently reads the whole log (distinct groups), so
    # each expects the full record count.
    expected = args.records
    start = time.perf_counter()
    for i in range(args.consumers):
        t = threading.Thread(
            target=consume,
            args=(args.host, args.topic, f"{group}-{i}", args.deadline_s, counter, i, expected),
        )
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
    elapsed = time.perf_counter() - start

    total = sum(counter)
    total_bytes = total * args.record_size
    result = {
        "benchmark": "consume",
        "client": args.client,
        "records_consumed": total,
        "record_size": args.record_size,
        "consumers": args.consumers,
        "wall_clock_s": round(elapsed, 3),
        "throughput_mb_s": round((total_bytes / (1024 * 1024)) / elapsed, 2) if elapsed else 0.0,
        "msg_per_s": round(total / elapsed, 1) if elapsed else 0.0,
        "env": env_fingerprint(),
    }
    print(json.dumps(result))


if __name__ == "__main__":
    main()
