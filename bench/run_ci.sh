#!/usr/bin/env bash
# Measure each binary with identical settings and fresh data on the CI runner.
set -euo pipefail

binary=$1
result=$2
bench_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
data_dir=$(mktemp -d)
broker_pid=''
cleanup() {
    if [[ -n "$broker_pid" ]]; then
        kill "$broker_pid" 2>/dev/null || true
        for ((attempt = 0; attempt < 50; attempt++)); do
            kill -0 "$broker_pid" 2>/dev/null || break
            sleep 0.1
        done
        kill -9 "$broker_pid" 2>/dev/null || true
        wait "$broker_pid" 2>/dev/null || true
    fi
    rm -rf "$data_dir"
}
trap cleanup EXIT

python3 - "$bench_dir/../config/broker.dev.properties" "$data_dir" <<'PY'
import json, pathlib, sys
config = json.loads(pathlib.Path(sys.argv[1]).read_text())
config.update({"log.dirs": sys.argv[2] + "/logs", "log.durability": "async",
               "host": "127.0.0.1", "advertised.host": "127.0.0.1",
               "monitoring.enabled": False})
pathlib.Path(sys.argv[2], "config.json").write_text(json.dumps(config))
PY

"$binary" --config "$data_dir/config.json" > "$result.broker.log" 2>&1 &
broker_pid=$!
python3 - "$broker_pid" <<'PY'
import os, socket, sys, time
for attempt in range(100):
    os.kill(int(sys.argv[1]), 0)
    try:
        with socket.create_connection(("127.0.0.1", 9092), timeout=0.2):
            break
    except OSError:
        time.sleep(0.2)
else:
    raise SystemExit("Benchmark broker did not become ready")
PY

timeout 180 python3 "$bench_dir/produce_perf.py" --records 1000000 --record-size 1024 \
    --producers 1 --client confluent --host 127.0.0.1:9092 > "$result"
cat "$result"
