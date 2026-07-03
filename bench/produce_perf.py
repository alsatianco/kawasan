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
import json
import sys
import threading
import time

from lib_bench import Producer, env_fingerprint, percentiles


def run_producer(client, host, topic, count, payload, sample_every, latencies_out):
    p = Producer(client, host)
    local_lat = []
    for i in range(count):
        send_t = time.perf_counter()
        p.send(topic, payload)
        if i % sample_every == 0:
            # Best-effort per-sample latency: flush forces the broker ack.
            p.flush()
            local_lat.append((time.perf_counter() - send_t) * 1000)
    p.flush()
    p.close()
    latencies_out.extend(local_lat)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--records", type=int, default=100000)
    ap.add_argument("--record-size", type=int, default=1024)
    ap.add_argument("--producers", type=int, default=1)
    ap.add_argument("--client", choices=["confluent", "kafka-python"], default="confluent")
    ap.add_argument("--topic", default=f"bench-{int(time.time())}")
    ap.add_argument("--host", default="localhost:9092")
    args = ap.parse_args()

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
    sample_every = max(1, per_thread // 100)

    all_latencies = []
    threads = []
    start = time.perf_counter()
    for _ in range(args.producers):
        lat = []
        all_latencies.append(lat)
        t = threading.Thread(
            target=run_producer,
            args=(args.client, args.host, args.topic, per_thread, payload,
                  sample_every, lat),
        )
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
    elapsed = time.perf_counter() - start

    total_records = per_thread * args.producers
    total_bytes = total_records * args.record_size
    latencies = sorted(x for lat in all_latencies for x in lat)
    p50, p95, p99 = percentiles(latencies, 0.50, 0.95, 0.99)

    result = {
        "benchmark": "produce",
        "client": args.client,
        "records": total_records,
        "record_size": args.record_size,
        "producers": args.producers,
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
