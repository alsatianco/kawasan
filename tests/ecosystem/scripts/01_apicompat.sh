#!/usr/bin/env bash
# 01_apicompat.sh: verify the broker responds to ApiVersions and Metadata.
# Both Kafka 4.2 and Kawasan must complete this handshake; if Kawasan fails,
# every later script will too — making this the first canary.
set -euo pipefail
TARGET="${1:-kawasan}"

# Use kafka-python from the local Python (already a test dependency).
python3 - <<'PY'
from kafka import KafkaAdminClient
a = KafkaAdminClient(bootstrap_servers='localhost:9092', client_id='compat-01', request_timeout_ms=10000)
topics = a.list_topics()
print('topics:', topics)
a.close()
print('PASS')
PY
