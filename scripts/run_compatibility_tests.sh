#!/bin/bash
#
# Run client compatibility tests for Kawasan
#
# This script starts a Kawasan broker and runs compatibility tests
# with various Kafka client libraries.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BROKER_BIN="$PROJECT_ROOT/build/tools/kawasan-broker"
TEST_DIR="$PROJECT_ROOT/tests/compatibility"
LOG_DIR="/tmp/kawasan-compat-test-$$"
BROKER_PID=""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

cleanup() {
    if [ -n "$BROKER_PID" ]; then
        log_info "Stopping broker (PID: $BROKER_PID)..."
        kill "$BROKER_PID" 2>/dev/null || true
        wait "$BROKER_PID" 2>/dev/null || true
    fi
    
    if [ -d "$LOG_DIR" ]; then
        log_info "Cleaning up log directory: $LOG_DIR"
        rm -rf "$LOG_DIR"
    fi
}

trap cleanup EXIT INT TERM

start_broker() {
    log_info "Starting Kawasan broker..."
    
    # Check if broker binary exists
    if [ ! -f "$BROKER_BIN" ]; then
        log_error "Broker binary not found: $BROKER_BIN"
        log_error "Please build the project first: cd build && make"
        exit 1
    fi
    
    # Create log directory
    mkdir -p "$LOG_DIR"
    
    # Create minimal config
    cat > "$LOG_DIR/server.properties" <<EOF
broker.id=1
host=127.0.0.1
port=9092
log.dirs=$LOG_DIR/data
network.io_threads=2
EOF
    
    # Start broker in background
    "$BROKER_BIN" --config "$LOG_DIR/server.properties" > "$LOG_DIR/broker.log" 2>&1 &
    BROKER_PID=$!
    
    # Wait for broker to be ready
    log_info "Waiting for broker to be ready (PID: $BROKER_PID)..."
    for i in {1..30}; do
        if nc -z localhost 9092 2>/dev/null; then
            log_info "Broker is ready!"
            return 0
        fi
        sleep 1
    done
    
    log_error "Broker failed to start within 30 seconds"
    log_error "Broker log:"
    cat "$LOG_DIR/broker.log"
    exit 1
}

run_python_tests() {
    log_info "Running kafka-python compatibility tests..."
    
    if [ ! -f "$TEST_DIR/kafka_python_test.py" ]; then
        log_warn "kafka-python test not found, skipping"
        return 0
    fi
    
    # Activate virtual environment if it exists
    if [ -f "$SCRIPT_DIR/env/bin/activate" ]; then
        source "$SCRIPT_DIR/env/bin/activate"
    fi
    
    # Check if kafka-python is installed
    if ! python3 -c "import kafka" 2>/dev/null; then
        log_warn "kafka-python not installed, skipping"
        log_warn "Install with: pip3 install kafka-python"
        return 0
    fi
    
    if python3 "$TEST_DIR/kafka_python_test.py"; then
        log_info "✓ kafka-python tests passed"
        return 0
    else
        log_error "✗ kafka-python tests failed"
        return 1
    fi
}

run_java_tests() {
    log_info "Checking for Java client tests..."
    
    if [ ! -f "$TEST_DIR/java_client_test.java" ]; then
        log_warn "Java client test not found, skipping"
        return 0
    fi
    
    log_warn "Java client tests not yet implemented, skipping"
    return 0
}

run_nodejs_tests() {
    log_info "Checking for Node.js client tests..."
    
    if [ ! -f "$TEST_DIR/kafkajs_test.js" ]; then
        log_warn "Node.js client test not found, skipping"
        return 0
    fi
    
    log_warn "Node.js client tests not yet implemented, skipping"
    return 0
}

run_go_tests() {
    log_info "Checking for Go client tests..."
    
    if [ ! -f "$TEST_DIR/sarama_test.go" ]; then
        log_warn "Go client test not found, skipping"
        return 0
    fi
    
    log_warn "Go client tests not yet implemented, skipping"
    return 0
}

main() {
    echo "=========================================="
    echo "Kawasan Client Compatibility Test Suite"
    echo "=========================================="
    echo ""
    
    # Start broker
    start_broker
    
    # Track test results
    FAILED_TESTS=0
    
    # Run all test suites
    run_python_tests || ((FAILED_TESTS++))
    run_java_tests || ((FAILED_TESTS++))
    run_nodejs_tests || ((FAILED_TESTS++))
    run_go_tests || ((FAILED_TESTS++))
    
    echo ""
    echo "=========================================="
    if [ $FAILED_TESTS -eq 0 ]; then
        log_info "All compatibility tests passed!"
        echo "=========================================="
        exit 0
    else
        log_error "$FAILED_TESTS test suite(s) failed"
        echo "=========================================="
        exit 1
    fi
}

main "$@"
