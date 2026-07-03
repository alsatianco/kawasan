#!/usr/bin/env python3
# P1: shared benchmark helpers — client abstraction (kafka-python or
# librdkafka via confluent-kafka), environment fingerprint, and percentile
# math. The librdkafka path is the source of truth for the 100k msg/s target
# because pure-Python kafka-python becomes the bottleneck as the broker
# speeds up.

import os
import platform
import subprocess
import time


def env_fingerprint():
    """Best-effort machine/build fingerprint recorded into every result so
    absolute numbers are only ever compared within the same environment."""
    cpu_model = platform.processor() or platform.machine()
    try:
        if platform.system() == "Darwin":
            cpu_model = subprocess.check_output(
                ["sysctl", "-n", "machdep.cpu.brand_string"], text=True
            ).strip()
        elif platform.system() == "Linux":
            with open("/proc/cpuinfo") as f:
                for line in f:
                    if line.startswith("model name"):
                        cpu_model = line.split(":", 1)[1].strip()
                        break
    except Exception:
        pass

    git_sha = os.environ.get("GITHUB_SHA", "")
    if not git_sha:
        try:
            git_sha = subprocess.check_output(
                ["git", "rev-parse", "--short", "HEAD"], text=True
            ).strip()
        except Exception:
            git_sha = "unknown"

    return {
        "cpu_model": cpu_model,
        "cpu_count": os.cpu_count(),
        "platform": platform.platform(),
        "git_sha": git_sha,
    }


def percentiles(sorted_ms, *ps):
    if not sorted_ms:
        return [0.0 for _ in ps]
    out = []
    for p in ps:
        idx = max(0, min(len(sorted_ms) - 1, int(len(sorted_ms) * p) - 1))
        out.append(round(sorted_ms[idx], 3))
    return out


class Producer:
    """Uniform producer over kafka-python and confluent-kafka (librdkafka)."""

    def __init__(self, client, host, acks="all", idempotence=True,
                 compression="snappy", linger_ms=10):
        self.client = client
        if client == "confluent":
            from confluent_kafka import Producer as CKProducer
            conf = {
                "bootstrap.servers": host,
                "acks": "all" if acks == "all" else str(acks),
                "enable.idempotence": idempotence,
                "linger.ms": linger_ms,
                "batch.num.messages": 10000,
                "queue.buffering.max.messages": 1000000,
            }
            if compression:
                conf["compression.type"] = compression
            self._p = CKProducer(conf)
        else:
            from kafka import KafkaProducer
            kw = dict(
                bootstrap_servers=host,
                acks="all" if acks == "all" else acks,
                enable_idempotence=idempotence,
                batch_size=64 * 1024,
                linger_ms=linger_ms,
                max_in_flight_requests_per_connection=1 if idempotence else 5,
            )
            try:
                import snappy  # noqa: F401
                kw["compression_type"] = compression
            except ImportError:
                kw["compression_type"] = None
            self._p = KafkaProducer(**kw)

    def send(self, topic, value):
        if self.client == "confluent":
            while True:
                try:
                    self._p.produce(topic, value=value)
                    return
                except BufferError:
                    self._p.poll(0.01)
        else:
            self._p.send(topic, value=value)

    def flush(self):
        self._p.flush()

    def close(self):
        if self.client == "kafka-python":
            self._p.close()
