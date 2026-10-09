"""Benchmark timing fixtures; no Kafka client or broker is required."""
import unittest
from unittest.mock import patch

import produce_perf


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


if __name__ == "__main__":
    unittest.main()
