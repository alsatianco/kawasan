#!/usr/bin/env python3
# Phase EX-11 / EX-4 (§6.4): bench regression gate. Reads a current
# bench output and compares against a checked-in baseline. Exits
# non-zero if throughput dropped by more than --threshold-pct or p99
# latency increased by more than --threshold-pct.

import argparse
import json
import sys

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--baseline", required=True)
    ap.add_argument("--current", required=True)
    ap.add_argument("--threshold-pct", type=float, default=10.0)
    args = ap.parse_args()

    with open(args.baseline) as f:
        baseline = json.load(f)
    with open(args.current) as f:
        current = json.load(f)

    failures = []
    # Throughput: lower-is-worse
    b_thru = baseline.get("throughput_mb_s", 0)
    c_thru = current.get("throughput_mb_s", 0)
    if b_thru > 0:
        delta_pct = (c_thru - b_thru) / b_thru * 100
        if delta_pct < -args.threshold_pct:
            failures.append(
                f"throughput regression: {b_thru:.1f} → {c_thru:.1f} MB/s "
                f"({delta_pct:.1f}%, threshold -{args.threshold_pct}%)"
            )
        else:
            print(f"  throughput: {b_thru:.1f} → {c_thru:.1f} MB/s ({delta_pct:+.1f}%) OK")

    # Latency: higher-is-worse
    for k in ("p50_ms", "p95_ms", "p99_ms"):
        b = baseline.get(k, 0)
        c = current.get(k, 0)
        if b > 0:
            delta_pct = (c - b) / b * 100
            if delta_pct > args.threshold_pct:
                failures.append(
                    f"{k} regression: {b:.2f} → {c:.2f} ms "
                    f"({delta_pct:+.1f}%, threshold +{args.threshold_pct}%)"
                )
            else:
                print(f"  {k}: {b:.2f} → {c:.2f} ms ({delta_pct:+.1f}%) OK")

    if failures:
        print("\nREGRESSIONS:", file=sys.stderr)
        for f in failures:
            print(f"  {f}", file=sys.stderr)
        sys.exit(1)

    print("\nAll metrics within ±%g%% threshold." % args.threshold_pct)

if __name__ == "__main__":
    main()
