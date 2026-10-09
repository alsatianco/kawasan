#!/usr/bin/env python3
# P1 (§7 numeric quality gates): produce throughput + latency.
#
# Produces N records of size S across P concurrent producer threads to a
# topic with acks=all + idempotence + compression. Reports throughput_mb_s,
# msg_per_s, p50/p95/p99 latency (ms), plus an environment fingerprint, as a
# JSON line on stdout.
#
# Usage:
#   python3 bench/produce_perf.py --records 1000000 --record-size 1024 \
#       [--producers 8] [--client confluent|kafka-python] [--host localhost:9092]

import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import sys
import time

from lib_bench import Producer, env_fingerprint, percentiles


def warm_up_producer(p, topic, count, payload):
    # Complete topic discovery, producer ID allocation, and the first append
    # before timing. Their retry/backoff delays are not sustained throughput.
    for _ in range(count):
        p.send(topic, payload)
    p.flush()


def run_producer(p, topic, count, payload, sample_every):
    local_lat = []
    for i in range(count):
        send_t = time.perf_counter()
        p.send(topic, payload)
        if i % sample_every == 0:
            # Best-effort per-sample latency: flush forces the broker ack.
            p.flush()
            local_lat.append((time.perf_counter() - send_t) * 1000)
    p.flush()
    return local_lat


def measure(client, host, topic, count, payload, producers, warmup_records):
    clients = []
    try:
        with ThreadPoolExecutor(max_workers=producers) as pool:
            for _ in range(producers):
                clients.append(Producer(client, host))
            warmups = [pool.submit(warm_up_producer, p, topic, warmup_records, payload)
                       for p in clients]
            for warmup in warmups:
                warmup.result()

            sample_every = max(1, count // 100)
            start = time.perf_counter()
            runs = [pool.submit(run_producer, p, topic, count, payload, sample_every)
                    for p in clients]
            latencies = [latency for run in runs for latency in run.result()]
            elapsed = time.perf_counter() - start
        return elapsed, sorted(latencies)
    finally:
        # The executor drains workers before closing clients, including when
        # initialization or a measured send fails. Propagate failures to CI.
        for p in clients:
            p.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--records", type=int, default=100000)
    ap.add_argument("--record-size", type=int, default=1024)
    ap.add_argument("--producers", type=int, default=1)
    ap.add_argument("--client", choices=["confluent", "kafka-python"], default="confluent")
    ap.add_argument("--topic", default=f"bench-{int(time.time())}")
    ap.add_argument("--host", default="localhost:9092")
    ap.add_argument("--warmup-records", type=int, default=10000,
                    help="untimed records per producer before measurement")
    args = ap.parse_args()
    if (args.producers < 1 or args.records < args.producers or
            args.record_size < 1 or args.warmup_records < 1):
        ap.error("require positive sizes, warm-up records, and at least one record per producer")

    # Fall back to kafka-python if confluent-kafka isn't installed so the
    # bench still runs in minimal environments.
    if args.client == "confluent":
        try:
            import confluent_kafka  # noqa: F401
        except ImportError:
            print("# note: confluent-kafka unavailable; using kafka-python", file=sys.stderr)
            args.client = "kafka-python"

    payload = b"x" * args.record_size
    per_thread = args.records // args.producers
    elapsed, latencies = measure(args.client, args.host, args.topic, per_thread,
                                 payload, args.producers, args.warmup_records)

    total_records = per_thread * args.producers
    total_bytes = total_records * args.record_size
    p50, p95, p99 = percentiles(latencies, 0.50, 0.95, 0.99)

    result = {
        "benchmark": "produce",
        "client": args.client,
        "records": total_records,
        "record_size": args.record_size,
        "producers": args.producers,
        "warmup_records_per_producer": args.warmup_records,
        "measurement": "steady-state",
        "wall_clock_s": round(elapsed, 3),
        "throughput_mb_s": round((total_bytes / (1024 * 1024)) / elapsed, 2),
        "msg_per_s": round(total_records / elapsed, 1),
        "p50_ms": p50,
        "p95_ms": p95,
        "p99_ms": p99,
        "env": env_fingerprint(),
    }
    print(json.dumps(result))


if __name__ == "__main__":
    main()
