# 1. Stop the broker first (if running)
lsof -ti:9092 | xargs kill -9 2>/dev/null
kill $(cat /tmp/kawasan-broker.pid) 2>/dev/null || true

# 2. Delete everything
rm -rf /tmp/kawasan-*

# 3. Start from scratch
bash scripts/quick-start.sh
