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
#   scripts/tests/cluster_harness.sh kill|kill9|restart|pause|resume <id>
#
# Env: N=3 (broker count), BASE=/tmp/kawasan-cluster (work dir), LAG_MS
# (replica.lag.time.max.ms), MIN_ISR, LIVENESS_MS (broker.liveness.timeout.ms),
# UNCLEAN (unclean.leader.election.enable), LOG_LEVEL.
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BROKER_BIN="${BROKER_BIN:-$ROOT/build/tools/kawasan-broker}"
N="${N:-3}"
BASE="${BASE:-/tmp/kawasan-cluster}"

KAFKA_BASE=${KAFKA_BASE:-9092}   # broker i Kafka port = KAFKA_BASE + i*100
RAFT_BASE=${RAFT_BASE:-9093}    # broker i Raft  port = RAFT_BASE  + i*100
MON_BASE=${MON_BASE:-9094}     # broker i monitoring port = MON_BASE + i*100

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
raft.peers=$([ "$N" -gt 1 ] && peers_csv || true)
log.durability=sync
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
broker.liveness.timeout.ms=${LIVENESS_MS:-9000}
unclean.leader.election.enable=${UNCLEAN:-false}
EOF
}

launch_broker() {
  local id="$1"
  if [ -f "$BASE/broker-$id/broker.pid" ] && kill -0 "$(cat "$BASE/broker-$id/broker.pid")" 2>/dev/null; then
    echo "[harness] broker $id already has a live PID" >&2
    return 1
  fi
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
    launch_broker "$i" || return 1
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
  local pids=()
  for i in $(seq 0 $((N - 1))); do
    local pidf="$BASE/broker-$i/broker.pid"
    if [ -f "$pidf" ]; then
      local pid
      pid="$(broker_pid "$i")" || continue
      kill -CONT "$pid" 2>/dev/null || true
      kill "$pid" 2>/dev/null && echo "  killed broker $i" && pids+=("$pid")
    fi
  done
  # Wait for real exit so a following `up` can rebind the ports.
  local waited=0
  for pid in "${pids[@]+"${pids[@]}"}"; do
    while kill -0 "$pid" 2>/dev/null && [ "$waited" -lt 300 ]; do
      sleep 0.1
      waited=$((waited + 1))
    done
    if kill -0 "$pid" 2>/dev/null; then
      echo "  broker pid $pid did not exit within 30s; sending SIGKILL"
      kill -9 "$pid" 2>/dev/null
    fi
  done
  echo "  brokers exited after ~$((waited / 10)).$((waited % 10))s"
  if [ "${KEEP_DATA:-0}" = 1 ]; then
    echo "[harness] retained $BASE"
  else
    rm -rf "$BASE"
    echo "[harness] cleaned $BASE"
  fi
}

# Refuse to signal an unrelated process after a stale PID file / PID reuse.
broker_pid() {
  local id="${1:?broker id required}" pid args
  [[ "$id" =~ ^[0-9]+$ ]] && [ "$id" -lt "$N" ] || return 1
  pid="$(cat "$BASE/broker-$id/broker.pid")" || return 1
  [[ "$pid" =~ ^[0-9]+$ ]] || return 1
  args="$(ps -ww -p "$pid" -o args=)" || return 1
  [[ "$args" == *"$BROKER_BIN"* && "$args" == *"$BASE/broker-$id/broker.properties"* ]] || return 1
  echo "$pid"
}

cmd_kill() {
  local id="${1:?usage: kill <broker-id>}" pid
  pid="$(broker_pid "$id")" || return 1
  kill "$pid" || return 1
  wait_exit "$pid" || return 1
  rm -f "$BASE/broker-$id/broker.pid"
  echo "[harness] stopped broker $id"
}

wait_exit() {
  local pid="$1"
  for _ in $(seq 1 300); do
    if ! kill -0 "$pid" 2>/dev/null; then return 0; fi
    sleep 0.1
  done
  echo "[harness] pid $pid failed to exit" >&2
  return 1
}

cmd_kill9() {
  local id="${1:?usage: kill9 <broker-id>}" pid
  pid="$(broker_pid "$id")" || return 1
  kill -KILL "$pid" || return 1
  wait_exit "$pid" || return 1
  rm -f "$BASE/broker-$id/broker.pid"
  echo "[harness] SIGKILLed broker $id"
}

# SIGSTOP/SIGCONT: a frozen broker keeps its sockets but stops responding — a
# nemesis for stale-leader / divergence scenarios.
cmd_pause() {
  local id="${1:?usage: pause <broker-id>}"
  local pid
  pid="$(broker_pid "$id")" || return 1
  kill -STOP "$pid" && echo "[harness] paused broker $id"
}

cmd_resume() {
  local id="${1:?usage: resume <broker-id>}"
  local pid
  pid="$(broker_pid "$id")" || return 1
  kill -CONT "$pid" && echo "[harness] resumed broker $id"
}

cmd_restart() {
  local id="${1:?usage: restart <broker-id>}"
  [ -f "$BASE/broker-$id/broker.properties" ] || return 1
  if [ -f "$BASE/broker-$id/broker.pid" ] && kill -0 "$(cat "$BASE/broker-$id/broker.pid")" 2>/dev/null; then
    echo "[harness] refusing to restart live broker $id" >&2
    return 1
  fi
  launch_broker "$id" || return 1
  echo "[harness] restarted broker $id: pid $(cat "$BASE/broker-$id/broker.pid")"
}

case "${1:-up}" in
  up) cmd_up ;;
  down) cmd_down ;;
  status) cmd_status ;;
  logs) cmd_logs "${2:-0}" ;;
  kill) cmd_kill "${2:-}" ;;
  kill9) cmd_kill9 "${2:-}" ;;
  restart) cmd_restart "${2:-}" ;;
  pause) cmd_pause "${2:-}" ;;
  resume) cmd_resume "${2:-}" ;;
  *) echo "usage: $0 {up|down|status|logs [id]|kill <id>|restart <id>}"; exit 1 ;;
esac
