#!/bin/bash
# Script to start N Kawasan brokers with generated configurations
# Used for testing multi-broker Raft clusters

set -e

# Configuration defaults
NUM_BROKERS="${1:-3}"
# 0A.9: assign two ports per broker (kafka and raft) with stride 2 so brokers
# don't collide. Previously `BASE_PORT=9092 RAFT_BASE_PORT=9093` with stride 1
# meant broker N's kafka port equaled broker N-1's raft port (9093, 9094, ...).
KAFKA_PORT_STRIDE=2
RAFT_PORT_STRIDE=2
BASE_PORT=9092         # kafka ports: 9092, 9094, 9096, ...
RAFT_BASE_PORT=9093    # raft  ports: 9093, 9095, 9097, ...
BASE_DIR="/tmp/kawasan-cluster-$$"
BROKER_BIN="${BROKER_BIN:-./build/tools/kawasan-broker}"
WAIT_TIMEOUT=30

# Color output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log() {
    echo -e "${BLUE}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $1"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1" >&2
}

success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

# Cleanup function
cleanup() {
    log "Cleaning up cluster..."
    for ((i=0; i<NUM_BROKERS; i++)); do
        local PID_FILE="${BASE_DIR}/broker-${i}.pid"
        if [ -f "$PID_FILE" ]; then
            local PID=$(cat "$PID_FILE")
            if kill -0 "$PID" 2>/dev/null; then
                log "Stopping broker $i (PID: $PID)"
                kill "$PID" 2>/dev/null || true
                # Wait up to 5 seconds for graceful shutdown
                for ((j=0; j<50; j++)); do
                    if ! kill -0 "$PID" 2>/dev/null; then
                        break
                    fi
                    sleep 0.1
                done
                # Force kill if still running
                if kill -0 "$PID" 2>/dev/null; then
                    warn "Force killing broker $i"
                    kill -9 "$PID" 2>/dev/null || true
                fi
            fi
            rm -f "$PID_FILE"
        fi
    done
    
    if [ "${KEEP_DATA:-0}" != "1" ]; then
        log "Removing data directory: $BASE_DIR"
        rm -rf "$BASE_DIR"
    else
        log "Keeping data directory: $BASE_DIR"
    fi
}

# Handle signals
trap cleanup EXIT INT TERM

# Create base directory
log "Creating cluster base directory: $BASE_DIR"
mkdir -p "$BASE_DIR"

# Generate peer list for Raft configuration
generate_peer_list() {
    local peer_list=""
    for ((i=0; i<NUM_BROKERS; i++)); do
        local raft_port=$((RAFT_BASE_PORT + i * RAFT_PORT_STRIDE))
        if [ -n "$peer_list" ]; then
            peer_list="${peer_list},"
        fi
        peer_list="${peer_list}${i}:localhost:${raft_port}"
    done
    echo "$peer_list"
}

PEER_LIST=$(generate_peer_list)
log "Raft peer list: $PEER_LIST"

# Start each broker
for ((i=0; i<NUM_BROKERS; i++)); do
    BROKER_ID=$i
    KAFKA_PORT=$((BASE_PORT + i * KAFKA_PORT_STRIDE))
    RAFT_PORT=$((RAFT_BASE_PORT + i * RAFT_PORT_STRIDE))
    BROKER_DIR="${BASE_DIR}/broker-${BROKER_ID}"
    LOG_DIR="${BROKER_DIR}/logs"
    CONFIG_FILE="${BROKER_DIR}/broker.properties"
    PID_FILE="${BASE_DIR}/broker-${BROKER_ID}.pid"
    LOG_FILE="${BROKER_DIR}/broker.log"
    
    log "Starting broker ${BROKER_ID}..."
    log "  Kafka port: ${KAFKA_PORT}"
    log "  Raft port: ${RAFT_PORT}"
    log "  Data dir: ${LOG_DIR}"
    
    # Create directories
    mkdir -p "$LOG_DIR"
    
    # 0A.9: Kawasan's config loader is JSON-only; previous .properties output
    # caused "Failed to parse config" startup errors. Emit valid JSON. The
    # 0A.14 task in improve-opus.md will unify both formats end-to-end.
    cat > "$CONFIG_FILE" <<EOF
{
  "broker.id": ${BROKER_ID},
  "host": "127.0.0.1",
  "port": ${KAFKA_PORT},
  "network.io_threads": 2,
  "network.max_frame_bytes": 1048576,
  "log.dirs": "${LOG_DIR}",
  "log.segment.bytes": 104857600,
  "log.retention.hours": 168,
  "log.retention.check.interval.ms": 300000,
  "raft.port": ${RAFT_PORT},
  "raft.peers": "${PEER_LIST}",
  "auto.create.topics.enable": true,
  "log.level": "info"
}
EOF

    # Start broker in background
    if [ ! -f "$BROKER_BIN" ]; then
        error "Broker binary not found: $BROKER_BIN"
        error "Please build the project first: cd build && make kawasan-broker"
        exit 1
    fi

    "$BROKER_BIN" --config "$CONFIG_FILE" > "$LOG_FILE" 2>&1 &
    # 0A.9: `local` is only valid inside a function; the broker-start loop runs
    # at top level. Use a plain assignment.
    BROKER_PID=$!
    echo "$BROKER_PID" > "$PID_FILE"
    
    log "Broker ${BROKER_ID} started with PID: ${BROKER_PID}"
    
    # Give broker a moment to initialize
    sleep 1
    
    # Check if broker is still running
    if ! kill -0 "$BROKER_PID" 2>/dev/null; then
        error "Broker ${BROKER_ID} failed to start. Check log: $LOG_FILE"
        tail -20 "$LOG_FILE"
        exit 1
    fi
done

# Wait for brokers to be ready
log "Waiting for brokers to be ready (timeout: ${WAIT_TIMEOUT}s)..."

wait_for_broker() {
    local broker_id=$1
    local port=$((BASE_PORT + broker_id))
    local timeout=$WAIT_TIMEOUT
    local elapsed=0
    
    while [ $elapsed -lt $timeout ]; do
        if nc -z localhost "$port" 2>/dev/null; then
            return 0
        fi
        sleep 1
        elapsed=$((elapsed + 1))
    done
    return 1
}

ALL_READY=true
for ((i=0; i<NUM_BROKERS; i++)); do
    log "Waiting for broker ${i}..."
    if wait_for_broker "$i"; then
        success "Broker ${i} is ready"
    else
        error "Broker ${i} failed to become ready"
        ALL_READY=false
    fi
done

if [ "$ALL_READY" != true ]; then
    error "Not all brokers became ready"
    exit 1
fi

success "All ${NUM_BROKERS} brokers are ready!"
log ""
log "Cluster information:"
log "  Base directory: $BASE_DIR"
log "  Kafka ports: $BASE_PORT-$((BASE_PORT + NUM_BROKERS - 1))"
log "  Raft ports: $RAFT_BASE_PORT-$((RAFT_BASE_PORT + NUM_BROKERS - 1))"
log ""
log "To view broker logs:"
for ((i=0; i<NUM_BROKERS; i++)); do
    log "  Broker ${i}: tail -f ${BASE_DIR}/broker-${i}/broker.log"
done
log ""
log "To stop the cluster: kill $$"
log "Or press Ctrl+C"
log ""

# Keep script running to maintain the cluster
if [ "${BACKGROUND:-0}" != "1" ]; then
    log "Cluster is running. Press Ctrl+C to stop..."
    wait
else
    # Export cluster info for caller
    echo "CLUSTER_BASE_DIR=${BASE_DIR}"
    echo "CLUSTER_NUM_BROKERS=${NUM_BROKERS}"
    echo "CLUSTER_BASE_PORT=${BASE_PORT}"
    echo "CLUSTER_RAFT_BASE_PORT=${RAFT_BASE_PORT}"
fi
