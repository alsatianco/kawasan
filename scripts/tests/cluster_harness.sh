#!/usr/bin/env bash
# Local multi-broker cluster harness for Phase B replication development.
#
# Launches an N-broker Kawasan cluster as local processes (no Docker) using the
# already-built ./build/tools/kawasan-broker binary, so the edit→build→test loop
# stays fast while developing the follower-fetcher. Each broker gets localhost
# Kafka/Raft/monitoring ports and a temp data dir; raft.peers wires them into one
# Raft group.
#
# Usage:
#   scripts/tests/cluster_harness.sh up        # start cluster, wait for formation
#   scripts/tests/cluster_harness.sh down      # stop cluster + clean temp dirs
#   scripts/tests/cluster_harness.sh status    # show broker liveness + leader
#   scripts/tests/cluster_harness.sh logs [id]  # tail a broker log
#
# Env: N=3 (broker count), BASE=/tmp/kawasan-cluster (work dir).
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BROKER_BIN="${BROKER_BIN:-$ROOT/build/tools/kawasan-broker}"
N="${N:-3}"
BASE="${BASE:-/tmp/kawasan-cluster}"

KAFKA_BASE=9092   # broker i Kafka port = KAFKA_BASE + i*100
RAFT_BASE=9093    # broker i Raft  port = RAFT_BASE  + i*100
MON_BASE=9094     # broker i monitoring port = MON_BASE + i*100

kafka_port() { echo $((KAFKA_BASE + $1 * 100)); }
raft_port()  { echo $((RAFT_BASE  + $1 * 100)); }
mon_port()   { echo $((MON_BASE   + $1 * 100)); }

peers_csv() {
  local csv=""
  for i in $(seq 0 $((N - 1))); do
    [ -n "$csv" ] && csv+=","
    csv+="$i:127.0.0.1:$(raft_port "$i")"
  done
  echo "$csv"
}

write_config() {
  local id="$1" dir="$2"
  mkdir -p "$dir/data"
  cat > "$dir/broker.properties" <<EOF
broker.id=$id
host=127.0.0.1
port=$(kafka_port "$id")
advertised.host=127.0.0.1
advertised.port=$(kafka_port "$id")
raft.port=$(raft_port "$id")
raft.peers=$(peers_csv)
log.dirs=$dir/data
metadata.dir=$dir/data/meta
num.partitions=3
default.replication.factor=$N
monitoring.enabled=true
monitoring.port=$(mon_port "$id")
auto.create.topics.enable=true
network.io_threads=2
replica.lag.time.max.ms=${LAG_MS:-30000}
min.insync.replicas=${MIN_ISR:-1}
EOF
}

launch_broker() {
  local id="$1"
  local dir="$BASE/broker-$id"
  "$BROKER_BIN" --config "$dir/broker.properties" --log-level "${LOG_LEVEL:-info}" \
    >> "$dir/broker.log" 2>&1 &
  echo $! > "$dir/broker.pid"
}

cmd_up() {
  echo "[harness] starting $N-broker cluster under $BASE"
  mkdir -p "$BASE"
  for i in $(seq 0 $((N - 1))); do
    local dir="$BASE/broker-$i"
    write_config "$i" "$dir"
    launch_broker "$i"
    echo "[harness] broker $i: pid $(cat "$dir/broker.pid"), kafka :$(kafka_port "$i"), raft :$(raft_port "$i")"
  done

  echo "[harness] waiting for Raft leader election (up to 30s)..."
  for _ in $(seq 1 60); do
    if grep -qiE "became leader|elected leader|transition.*leader|is now leader|state.*LEADER" "$BASE"/broker-*/broker.log 2>/dev/null; then
      echo "[harness] leader election observed:"
      grep -iE "became leader|elected leader|is now leader|state.*LEADER" "$BASE"/broker-*/broker.log 2>/dev/null | head
      break
    fi
    sleep 0.5
  done
  cmd_status
}

cmd_status() {
  echo "[harness] broker liveness:"
  for i in $(seq 0 $((N - 1))); do
    local pidf="$BASE/broker-$i/broker.pid"
    if [ -f "$pidf" ] && kill -0 "$(cat "$pidf")" 2>/dev/null; then
      echo "  broker $i: UP (pid $(cat "$pidf"))"
    else
      echo "  broker $i: DOWN"
    fi
  done
}

cmd_logs() {
  local id="${1:-0}"
  tail -n 60 "$BASE/broker-$id/broker.log"
}

cmd_down() {
  echo "[harness] stopping cluster"
  for i in $(seq 0 $((N - 1))); do
    local pidf="$BASE/broker-$i/broker.pid"
    if [ -f "$pidf" ]; then
      kill "$(cat "$pidf")" 2>/dev/null && echo "  killed broker $i"
    fi
  done
  sleep 1
  rm -rf "$BASE"
  echo "[harness] cleaned $BASE"
}

cmd_kill() {
  local id="${1:?usage: kill <broker-id>}"
  local pidf="$BASE/broker-$id/broker.pid"
  if [ -f "$pidf" ]; then
    kill "$(cat "$pidf")" 2>/dev/null && echo "[harness] killed broker $id"
    rm -f "$pidf"
  fi
}

cmd_restart() {
  local id="${1:?usage: restart <broker-id>}"
  launch_broker "$id"
  echo "[harness] restarted broker $id: pid $(cat "$BASE/broker-$id/broker.pid")"
}

case "${1:-up}" in
  up) cmd_up ;;
  down) cmd_down ;;
  status) cmd_status ;;
  logs) cmd_logs "${2:-0}" ;;
  kill) cmd_kill "${2:-}" ;;
  restart) cmd_restart "${2:-}" ;;
  *) echo "usage: $0 {up|down|status|logs [id]|kill <id>|restart <id>}"; exit 1 ;;
esac
