#!/usr/bin/env python3
# Phase EX-11 (§7 numeric quality gates): produce throughput + latency.
#
# Produces N records of size S to a single partition with
# acks=all + idempotence=true + snappy compression. Reports
# throughput_mb_s, p50/p95/p99 latency in milliseconds as a JSON line
# on stdout.
#
# Usage:
#   python3 bench/produce_perf.py --records 1000000 --record-size 1024 [--host localhost:9092]

import argparse
import json
import statistics
import time
import sys
from kafka import KafkaProducer

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--records", type=int, default=100000)
    ap.add_argument("--record-size", type=int, default=1024)
    ap.add_argument("--topic", default=f"bench-{int(time.time())}")
    ap.add_argument("--host", default="localhost:9092")
    args = ap.parse_args()

    payload = b"x" * args.record_size

    kw = dict(
        bootstrap_servers=args.host,
        acks="all",
        enable_idempotence=True,
        batch_size=64 * 1024,
        linger_ms=10,
        max_in_flight_requests_per_connection=1,  # required by kafka-python's idempotent producer
    )
    # Use snappy if available; fall back to no compression so the
    # benchmark still runs in minimal environments. Print which path
    # was taken for reproducibility.
    try:
        import snappy  # noqa: F401
        kw["compression_type"] = "snappy"
    except ImportError:
        kw["compression_type"] = None
        print("# note: python-snappy unavailable; using no compression", file=sys.stderr)

    p = KafkaProducer(**kw)

    latencies_ms = []
    start = time.perf_counter()
    for i in range(args.records):
        send_t = time.perf_counter()
        fut = p.send(args.topic, value=payload)
        # Don't block per-record (kills throughput); sample latency every 1000th.
        if i % 1000 == 0:
            try:
                fut.get(timeout=10)
                latencies_ms.append((time.perf_counter() - send_t) * 1000)
            except Exception as e:
                print(f"send failed at i={i}: {e}", file=sys.stderr)
                sys.exit(1)
    p.flush()
    elapsed = time.perf_counter() - start
    p.close()

    total_bytes = args.records * args.record_size
    throughput_mb_s = (total_bytes / (1024 * 1024)) / elapsed
    latencies_ms.sort()

    def pct(p):
        if not latencies_ms:
            return 0.0
        i = max(0, int(len(latencies_ms) * p) - 1)
        return latencies_ms[i]

    result = {
        "records": args.records,
        "record_size": args.record_size,
        "wall_clock_s": round(elapsed, 3),
        "throughput_mb_s": round(throughput_mb_s, 2),
        "p50_ms": round(pct(0.50), 3),
        "p95_ms": round(pct(0.95), 3),
        "p99_ms": round(pct(0.99), 3),
    }
    print(json.dumps(result))

if __name__ == "__main__":
    main()
