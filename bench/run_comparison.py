#!/usr/bin/env python3
"""Measure alternating base/head runs and retain raw samples plus medians."""
import argparse
import json
import math
from pathlib import Path
import statistics
import subprocess


def aggregate(samples):
    if not samples:
        raise ValueError("no benchmark samples")
    result = dict(samples[0])
    metrics = ("wall_clock_s", "throughput_mb_s", "msg_per_s", "p50_ms", "p95_ms", "p99_ms")
    identity = {k: v for k, v in result.items() if k not in metrics}
    for sample in samples:
        if {k: v for k, v in sample.items() if k not in metrics} != identity:
            raise ValueError("benchmark samples have different scenarios or environments")
    for metric in metrics:
        values = [sample[metric] for sample in samples]
        if any(not math.isfinite(v) or v <= 0 for v in values):
            raise ValueError(f"invalid {metric} in benchmark samples")
        result[metric] = statistics.median(values)
    result.update(sample_count=len(samples), aggregation="median", samples=samples)
    return result


def run_comparison(base_binary, head_binary, output_dir, sample_count):
    output_dir.mkdir(parents=True, exist_ok=True)
    script = Path(__file__).with_name("run_ci.sh")
    binaries = {"base": base_binary, "head": head_binary}
    samples = {"base": [], "head": []}
    for index in range(sample_count):
        # Avoid making one revision always pay the first-run cost or always
        # measure later in a period of changing shared-runner load.
        order = ("base", "head") if index % 2 == 0 else ("head", "base")
        for label in order:
            path = output_dir / f"{label}-{index + 1:02d}.json"
            print(f"Sample {index + 1}/{sample_count}: {label}", flush=True)
            subprocess.run(["bash", str(script), str(binaries[label]), str(path)], check=True)
            samples[label].append(json.loads(path.read_text()))
    for label, rows in samples.items():
        result = aggregate(rows)
        (output_dir / f"{label}.json").write_text(json.dumps(result) + "\n")
        print(f"{label} median: {result['throughput_mb_s']:.2f} MB/s; "
              f"p99 {result['p99_ms']:.3f} ms ({sample_count} samples)", flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base-binary", type=Path, required=True)
    ap.add_argument("--head-binary", type=Path, required=True)
    ap.add_argument("--output-dir", type=Path, required=True)
    # Nine pairs let the confidence gate tolerate one anomalous pair without
    # losing the ability to detect a sustained regression in the other eight.
    ap.add_argument("--samples", type=int, default=9)
    args = ap.parse_args()
    if args.samples < 3 or args.samples % 2 == 0:
        ap.error("--samples must be an odd number of at least 3")
    run_comparison(args.base_binary, args.head_binary, args.output_dir, args.samples)


if __name__ == "__main__":
    main()
