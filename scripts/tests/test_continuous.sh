#!/bin/bash
cd /Users/duc.nguyen/git2/kawasan
source scripts/env/bin/activate

echo "Running 30-second continuous test..."
python3 scripts/kafka_thread_test.py -N 1 -M 1 --bootstrap localhost:9092 -T 2 > /tmp/test_continuous.log 2>&1 &
TESTPID=$!

echo "Test running with PID $TESTPID"
echo "Waiting 30 seconds..."
sleep 30

echo "Stopping test..."
kill $TESTPID 2>/dev/null || true
sleep 1

echo ""
echo "============================================================"
echo "TEST RESULTS"
echo "============================================================"

TOTAL_LINES=$(wc -l < /tmp/test_continuous.log)
echo "Total log lines: $TOTAL_LINES"

# Count unique messages
UNIQUE=$(grep "consumer thread.*consume message Message" /tmp/test_continuous.log | awk '{print $(NF-3)}' | sort -u | wc -l)
echo "Unique messages consumed: $UNIQUE"

# Count total consumptions
TOTAL=$(grep "consumer thread.*consume message Message" /tmp/test_continuous.log | wc -l)
echo "Total consumptions: $TOTAL"

# Check for loops
if [ "$TOTAL" -gt $((UNIQUE * 2)) ]; then
    echo ""
    echo "❌ FAILURE: Detected message re-consumption loop!"
    echo "   Expected ~$UNIQUE consumptions, got $TOTAL"
    echo ""
    echo "Sample of repeated messages:"
    grep "consumer thread.*consume message Message" /tmp/test_continuous.log | \
        awk '{print $(NF-3)}' | sort | uniq -c | sort -rn | head -5
    exit 1
else
    echo ""
    echo "✅ SUCCESS: No excessive re-consumption detected!"
    echo "   Consumptions per message: $(echo "scale=2; $TOTAL / $UNIQUE" | bc)"
    exit 0
fi
