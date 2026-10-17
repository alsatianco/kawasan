#!/usr/bin/env python3
# Phase EX-11 / EX-4 (§6.4) + P1: bench regression gate.
#
# Two modes:
#   --baseline FILE --current FILE   compare current run to a checked-in
#                                    baseline (nightly absolute gate).
#   --base FILE --head FILE          A/B mode: compare a base-branch run to a
#                                    head-branch run measured back-to-back on
#                                    the SAME runner. Aggregated A/B results
#                                    use paired samples to quantify runner noise.
# Exits non-zero if throughput dropped or p99 latency rose by more than
# --threshold-pct. Both files may be a single JSON object or JSONL (one
# object per scenario) — in JSONL mode scenarios are matched by a key built
# from benchmark/client/producers/consumers and each is checked.

import argparse
import json
import math
import random
import statistics
import sys


def regression_lower_bound(baseline, current, metric):
    """One-sided 95% paired-bootstrap bound on the median regression (%).

    Adjacent base/head measurements form pairs; resampling those pairs keeps
    their shared runner conditions together. A point estimate just beyond the
    threshold is inconclusive when the uncertainty still includes that threshold.
    Legacy single measurements retain the strict point-estimate gate.
    """
    if "samples" not in baseline and "samples" not in current:
        return None
    pairs = []
    for result in (baseline, current):
        rows = result.get("samples", [])
        if (result.get("aggregation") != "median" or len(rows) < 3 or
                result.get("sample_count") != len(rows)):
            raise ValueError("expected at least three median-aggregated benchmark samples")
        values = [row[metric] for row in rows]
        if any(not math.isfinite(v) or v <= 0 for v in values):
            raise ValueError(f"invalid {metric} in benchmark samples")
        if result[metric] != statistics.median(values):
            raise ValueError(f"aggregate {metric} does not match benchmark samples")
        pairs.append(values)
    base, head = pairs
    if len(base) != len(head):
        raise ValueError("base/head benchmark sample counts differ")
    rng = random.Random(0)
    regressions = []
    for _ in range(10000):
        indices = rng.choices(range(len(base)), k=len(base))
        b = statistics.median(base[i] for i in indices)
        h = statistics.median(head[i] for i in indices)
        delta = (h - b) / b * 100
        regressions.append(-delta if metric == "throughput_mb_s" else delta)
    return sorted(regressions)[499]


def confirmed_regression(skey, baseline, current, metric, threshold):
    bound = regression_lower_bound(baseline, current, metric)
    if bound is None or bound > threshold:
        return True
    print(f"  [{skey}] {metric}: inconclusive regression; "
          f"95% lower bound {bound:+.1f}% does not exceed +{threshold:g}%")
    return False


def load(path):
    """Return a dict of scenario-key -> metrics, from a JSON object or JSONL."""
    with open(path) as f:
        text = f.read().strip()
    objs = []
    try:
        obj = json.loads(text)
        objs = obj if isinstance(obj, list) else [obj]
    except json.JSONDecodeError:
        objs = [json.loads(line) for line in text.splitlines() if line.strip()]

    def key(o):
        return "/".join(str(o.get(k, "")) for k in
                        ("benchmark", "client", "producers", "consumers"))

    return {key(o): o for o in objs}


def compare(baseline, current, threshold, label_base, label_head):
    failures = []
    for skey, b in baseline.items():
        c = current.get(skey)
        if c is None:
            print(f"  [{skey}] missing in {label_head}; skipping")
            continue
        _compare_one(skey, b, c, threshold, failures)
    return failures


def _compare_one(skey, baseline, current, threshold, failures):
    # Throughput: lower-is-worse.
    b_thru = baseline.get("throughput_mb_s", 0)
    c_thru = current.get("throughput_mb_s", 0)
    if b_thru > 0:
        delta_pct = (c_thru - b_thru) / b_thru * 100
        if delta_pct < -threshold:
            if confirmed_regression(skey, baseline, current, "throughput_mb_s", threshold):
                failures.append(
                    f"[{skey}] throughput regression: {b_thru:.1f} → {c_thru:.1f} MB/s "
                    f"({delta_pct:.1f}%, threshold -{threshold}%)"
                )
        else:
            print(f"  [{skey}] throughput: {b_thru:.1f} → {c_thru:.1f} MB/s ({delta_pct:+.1f}%) OK")

    # Latency: higher-is-worse.
    for k in ("p50_ms", "p95_ms", "p99_ms"):
        b = baseline.get(k, 0)
        c = current.get(k, 0)
        if b > 0:
            delta_pct = (c - b) / b * 100
            if delta_pct > threshold:
                if confirmed_regression(skey, baseline, current, k, threshold):
                    failures.append(
                        f"[{skey}] {k} regression: {b:.2f} → {c:.2f} ms "
                        f"({delta_pct:+.1f}%, threshold +{threshold}%)"
                    )
            else:
                print(f"  [{skey}] {k}: {b:.2f} → {c:.2f} ms ({delta_pct:+.1f}%) OK")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--baseline")
    ap.add_argument("--current")
    ap.add_argument("--base")
    ap.add_argument("--head")
    ap.add_argument("--threshold-pct", type=float, default=10.0)
    args = ap.parse_args()

    if args.base and args.head:
        base_path, cur_path, lb, lh = args.base, args.head, "base", "head"
    elif args.baseline and args.current:
        base_path, cur_path, lb, lh = args.baseline, args.current, "baseline", "current"
    else:
        ap.error("provide either --baseline/--current or --base/--head")

    baseline = load(base_path)
    current = load(cur_path)
    failures = compare(baseline, current, args.threshold_pct, lb, lh)

    if failures:
        print("\nREGRESSIONS:", file=sys.stderr)
        for f in failures:
            print(f"  {f}", file=sys.stderr)
        sys.exit(1)

    print("\nNo confirmed regressions beyond %g%%." % args.threshold_pct)


if __name__ == "__main__":
    main()
