"""Benchmark timing fixtures; no Kafka client or broker is required."""
import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import check_regression
import lib_bench
import produce_perf
import run_comparison


class ProduceMeasurementTest(unittest.TestCase):
    def measure_with_startup(self, startup_delay, fail_at=None):
        clock = [0.0]
        clients = []

        class Producer:
            def __init__(self, client, host):
                clock[0] += startup_delay
                self.sent = 0
                self.closed = False
                clients.append(self)

            def send(self, topic, payload):
                self.sent += 1
                if self.sent == fail_at:
                    raise RuntimeError("send failed")
                clock[0] += 1

            def flush(self):
                # Model metadata discovery/InitProducerId on the first flush.
                if self.sent == 2:
                    clock[0] += startup_delay
                clock[0] += 1

            def close(self):
                self.closed = True
                clock[0] += startup_delay

        with patch.object(produce_perf, "Producer", Producer), \
                patch.object(produce_perf.time, "perf_counter", lambda: clock[0]):
            try:
                elapsed, latencies = produce_perf.measure(
                    "confluent", "host", "topic", 3, b"x", 1, 2)
            finally:
                self.assertTrue(all(p.closed for p in clients))
        self.assertEqual(clients[0].sent, 5)  # Two warm-up + three timed records.
        self.assertEqual(elapsed, 7)  # Sends, sampled ACKs, and final flush only.
        self.assertEqual(latencies, [2000, 2000, 2000])
        return elapsed

    def test_startup_and_shutdown_delays_do_not_change_throughput(self):
        self.assertEqual(self.measure_with_startup(10), self.measure_with_startup(10000))

    def test_worker_failure_is_not_reported_as_success(self):
        for fail_at in (1, 3):  # Warm-up and measured send failures.
            with self.subTest(fail_at=fail_at), self.assertRaisesRegex(RuntimeError, "send failed"):
                self.measure_with_startup(10, fail_at=fail_at)

    def test_all_producers_finish_warmup_before_timer_starts(self):
        clients = []

        class Producer:
            def __init__(self, *args):
                self.warmed = False
                clients.append(self)

            def send(self, *args):
                pass

            def flush(self):
                self.warmed = True

            def close(self):
                pass

        def now():
            self.assertEqual(len(clients), 3)
            self.assertTrue(all(p.warmed for p in clients))
            return 0

        with patch.object(produce_perf, "Producer", Producer), \
                patch.object(produce_perf.time, "perf_counter", now):
            produce_perf.measure("confluent", "host", "topic", 3, b"x", 3, 2)


class ComparisonTest(unittest.TestCase):
    def sample(self, throughput=350, p99=20):
        return {"benchmark": "produce", "client": "confluent", "producers": 1,
                "records": 1000000, "record_size": 1024, "measurement": "steady-state",
                "wall_clock_s": 3, "throughput_mb_s": throughput, "msg_per_s": 350000,
                "p50_ms": 10, "p95_ms": 15, "p99_ms": p99, "env": {"git_sha": "revision"}}

    def test_median_retains_all_samples_and_limits_single_outlier(self):
        rows = [self.sample(v) for v in (350, 349, 348, 351, 280)]
        result = run_comparison.aggregate(rows)
        self.assertEqual(result["throughput_mb_s"], 349)
        self.assertEqual(result["samples"], rows)
        self.assertEqual(result["sample_count"], 5)

    def test_sustained_throughput_and_latency_regressions_still_fail(self):
        baseline = run_comparison.aggregate([self.sample()] * 9)
        for rows in ([self.sample(v) for v in (292, 293, 291, 290, 292, 293, 291, 290, 350)],
                     [self.sample(p99=v) for v in (25, 26, 24, 25, 25, 26, 24, 25, 20)]):
            with self.subTest(rows=rows), contextlib.redirect_stdout(io.StringIO()):
                current = run_comparison.aggregate(rows)
                failures = check_regression.compare({"scenario": baseline},
                                                     {"scenario": current}, 10, "base", "head")
                self.assertEqual(len(failures), 1)

    def test_invalid_or_mismatched_samples_fail(self):
        for rows in ([], [self.sample(), {**self.sample(), "records": 42}],
                     [self.sample(float("nan"))], [self.sample(0)]):
            with self.subTest(rows=rows), self.assertRaises(ValueError):
                run_comparison.aggregate(rows)

    def test_borderline_noisy_regression_is_inconclusive(self):
        baseline = run_comparison.aggregate([self.sample()] * 5)
        current = run_comparison.aggregate(
            [self.sample(p99=v) for v in (20, 21, 22.4, 23, 24)])
        with contextlib.redirect_stdout(io.StringIO()) as output:
            failures = check_regression.compare({"scenario": baseline},
                                                {"scenario": current}, 10, "base", "head")
        self.assertEqual(failures, [])
        self.assertIn("inconclusive", output.getvalue())

    def test_shared_runner_drift_preserves_paired_regression(self):
        # The large common drift cancels within each adjacent measurement pair.
        baseline = run_comparison.aggregate(
            [self.sample(p99=v) for v in (10, 20, 30, 40, 50)])
        current = run_comparison.aggregate(
            [self.sample(p99=v) for v in (12, 24, 36, 48, 60)])
        self.assertAlmostEqual(
            check_regression.regression_lower_bound(baseline, current, "p99_ms"), 20)

    def test_single_measurement_keeps_strict_threshold(self):
        with contextlib.redirect_stdout(io.StringIO()):
            failures = check_regression.compare({"scenario": self.sample()},
                {"scenario": self.sample(p99=22.4)}, 10, "base", "head")
        self.assertEqual(len(failures), 1)

    def test_invalid_raw_samples_cannot_hide_regression(self):
        baseline = run_comparison.aggregate([self.sample()] * 5)
        current = run_comparison.aggregate([self.sample(p99=25)] * 5)
        for invalid in ({**current, "samples": []},
                        {**current, "sample_count": 4},
                        {**current, "p99_ms": 26},
                        run_comparison.aggregate([self.sample(p99=25)] * 3),
                        {**current, "samples": [self.sample(p99=float("nan"))] * 5}):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                check_regression.regression_lower_bound(baseline, invalid, "p99_ms")

    def test_comparison_alternates_order_and_writes_raw_and_aggregate_results(self):
        calls = []

        def run(command, check):
            calls.append(command[2])
            Path(command[3]).write_text(json.dumps(self.sample()))

        with tempfile.TemporaryDirectory() as directory, \
                patch.object(run_comparison.subprocess, "run", run), \
                contextlib.redirect_stdout(io.StringIO()):
            output = Path(directory)
            run_comparison.run_comparison(Path("base-binary"), Path("head-binary"), output, 3)
            self.assertEqual(calls, ["base-binary", "head-binary", "head-binary",
                                     "base-binary", "base-binary", "head-binary"])
            for label in ("base", "head"):
                result = json.loads((output / f"{label}.json").read_text())
                self.assertEqual(result["sample_count"], 3)
                self.assertEqual(len(list(output.glob(f"{label}-*.json"))), 3)

    def test_failed_benchmark_stops_comparison(self):
        with tempfile.TemporaryDirectory() as directory:
            with patch.object(run_comparison.subprocess, "run", side_effect=RuntimeError("failed")), \
                    contextlib.redirect_stdout(io.StringIO()), self.assertRaisesRegex(RuntimeError, "failed"):
                run_comparison.run_comparison(Path("base"), Path("head"), Path(directory), 3)
            self.assertFalse((Path(directory) / "base.json").exists())

    def test_base_fingerprint_overrides_head_job_sha(self):
        with patch.dict(lib_bench.os.environ, {"BENCH_GIT_SHA": "base", "GITHUB_SHA": "head"}):
            self.assertEqual(lib_bench.env_fingerprint()["git_sha"], "base")


if __name__ == "__main__":
    unittest.main()
