#!/bin/bash
# Test script to verify two consumers can join the same group

echo "=== Testing Two Consumers ==="
echo ""

# Clean up any existing consumers
pkill -f "simple_consumer.py" 2>/dev/null

# Start first consumer in background
echo "Starting first consumer..."
python3 scripts/simple_consumer.py --group test-group-multi --topic test-topic > /tmp/consumer1.log 2>&1 &
CONSUMER1_PID=$!
echo "Consumer 1 PID: $CONSUMER1_PID"

# Wait a bit for first consumer to join
sleep 3

# Start second consumer in background
echo "Starting second consumer..."
python3 scripts/simple_consumer.py --group test-group-multi --topic test-topic > /tmp/consumer2.log 2>&1 &
CONSUMER2_PID=$!
echo "Consumer 2 PID: $CONSUMER2_PID"

# Wait for second consumer to join
sleep 3

# Check if both consumers are still running
if ps -p $CONSUMER1_PID > /dev/null; then
    echo "✓ Consumer 1 is running"
else
    echo "✗ Consumer 1 died!"
    cat /tmp/consumer1.log
fi

if ps -p $CONSUMER2_PID > /dev/null; then
    echo "✓ Consumer 2 is running"
else
    echo "✗ Consumer 2 died!"
    cat /tmp/consumer2.log
fi

# Show logs
echo ""
echo "=== Consumer 1 Log ==="
head -20 /tmp/consumer1.log

echo ""
echo "=== Consumer 2 Log ==="
head -20 /tmp/consumer2.log

# Kill consumers
echo ""
echo "Cleaning up..."
kill $CONSUMER1_PID $CONSUMER2_PID 2>/dev/null
wait $CONSUMER1_PID $CONSUMER2_PID 2>/dev/null

echo "Done!"
