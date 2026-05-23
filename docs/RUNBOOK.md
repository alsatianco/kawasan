# Kawasan Incident Response Runbook

**Version**: 0.2.0-alpha  
**Last Updated**: November 24, 2025

This runbook provides step-by-step procedures for responding to common incidents in Kawasan deployments.

---

## Table of Contents

1. [Broker Down](#broker-down)
2. [Disk Full](#disk-full)
3. [Slow Consumer](#slow-consumer)
4. [Replication Lag](#replication-lag)
5. [Split Brain](#split-brain)
6. [High CPU Usage](#high-cpu-usage)
7. [Memory Exhaustion](#memory-exhaustion)
8. [Network Issues](#network-issues)
9. [Data Corruption](#data-corruption)
10. [Escalation Procedures](#escalation-procedures)

---

## General Incident Response Process

### 1. Detection & Alert
- Monitor alerts from Prometheus/Grafana
- Review health check failures
- Check system logs

### 2. Assessment
- Determine severity (P1: Critical, P2: High, P3: Medium, P4: Low)
- Identify affected services and users
- Estimate impact scope

### 3. Response
- Follow incident-specific procedures below
- Document all actions taken
- Communicate with stakeholders

### 4. Resolution
- Verify issue is resolved
- Restore normal operations
- Document root cause

### 5. Post-Incident
- Conduct post-mortem
- Update runbook if needed
- Implement preventive measures

---

## Broker Down

### Severity: P1 (Critical)

### Symptoms
- Broker process not running
- Health checks failing: `curl http://localhost:9094/health` returns error
- Clients cannot connect
- Systemd shows service as `inactive (dead)`

### Immediate Actions

#### 1. Check Broker Status

```bash
# Check systemd status
sudo systemctl status kawasan-broker

# Check if process is running
ps aux | grep kawasan-broker

# Check port availability
nc -zv localhost 9092
```

#### 2. Review Logs

```bash
# View recent logs
sudo journalctl -u kawasan-broker -n 200 --no-pager

# Search for errors
sudo journalctl -u kawasan-broker | grep -i error

# Check for crashes
sudo journalctl -u kawasan-broker | grep -i "terminated\|killed\|segfault"
```

#### 3. Identify Root Cause

Common causes:
- **Configuration error**: Invalid config file syntax
- **Port conflict**: Port 9092 already in use
- **Permission denied**: Data directory not writable
- **Out of memory**: OOM killer terminated process
- **Disk full**: Cannot write logs or data
- **Segmentation fault**: Bug in broker code

#### 4. Resolution Steps

**If configuration error:**
```bash
# Validate config
cat /etc/kawasan/broker.properties

# Fix configuration
sudo vim /etc/kawasan/broker.properties

# Restart broker
sudo systemctl restart kawasan-broker
```

**If port conflict:**
```bash
# Find process using port
sudo lsof -i :9092
sudo netstat -tulpn | grep 9092

# Kill conflicting process or change broker port
sudo kill -9 <pid>

# Or change port in config
sudo vim /etc/kawasan/broker.properties
# Update: listeners=PLAINTEXT://0.0.0.0:9093

sudo systemctl restart kawasan-broker
```

**If permission denied:**
```bash
# Check data directory ownership
ls -la /var/lib/kawasan/data

# Fix permissions
sudo chown -R kawasan:kawasan /var/lib/kawasan
sudo chown -R kawasan:kawasan /var/log/kawasan

# Restart broker
sudo systemctl restart kawasan-broker
```

**If out of memory:**
```bash
# Check memory usage
free -h
vmstat 1 5

# Check systemd memory limits
sudo systemctl show kawasan-broker | grep Memory

# Increase memory limit
sudo vim /etc/systemd/system/kawasan-broker.service
# Add under [Service]:
# MemoryMax=8G

sudo systemctl daemon-reload
sudo systemctl restart kawasan-broker
```

**If disk full:**
See [Disk Full](#disk-full) section.

**If segmentation fault:**
```bash
# Check for core dump
ls -la /var/lib/kawasan/core.*

# Analyze with gdb (if available)
gdb /usr/local/bin/kawasan-broker /var/lib/kawasan/core.*

# Report bug with backtrace
# Restart with debug logging
sudo vim /etc/systemd/system/kawasan-broker.service
# Change: --log-level debug

sudo systemctl daemon-reload
sudo systemctl restart kawasan-broker
```

#### 5. Verify Recovery

```bash
# Check broker is running
sudo systemctl status kawasan-broker

# Test health endpoint
curl http://localhost:9094/health

# Test basic operations
./kawasan-topics --bootstrap-server localhost:9092 --list

# Monitor logs
sudo journalctl -u kawasan-broker -f
```

### Post-Incident Actions
- Review and update monitoring alerts
- If repeating issue, consider:
  - Increasing resource limits
  - Upgrading hardware
  - Fixing identified bugs

---

## Disk Full

### Severity: P1 (Critical)

### Symptoms
- Error: "No space left on device"
- Broker stops accepting writes
- Logs show disk full errors
- `df -h` shows 100% usage

### Immediate Actions

#### 1. Assess Disk Usage

```bash
# Check overall disk usage
df -h /var/lib/kawasan

# Check directory sizes
du -sh /var/lib/kawasan/data/*
du -sh /var/log/kawasan/*

# Find largest files
du -ah /var/lib/kawasan/data | sort -rh | head -20
```

#### 2. Free Up Immediate Space

**Option A: Reduce Log Retention (Quickest)**

```bash
# Stop broker to safely modify retention
sudo systemctl stop kawasan-broker

# Edit config to reduce retention
sudo vim /etc/kawasan/broker.properties
# Set:
# log.retention.hours=24
# log.retention.bytes=10737418240  # 10GB

# Restart broker (will cleanup old segments)
sudo systemctl start kawasan-broker

# Monitor cleanup
sudo journalctl -u kawasan-broker -f | grep "cleanup"
```

**Option B: Manual Cleanup**

⚠️ **Caution**: Only delete old log segments, never delete current segments.

```bash
# Stop broker
sudo systemctl stop kawasan-broker

# Identify old segments (older than 7 days)
find /var/lib/kawasan/data -name "*.log" -mtime +7 -ls

# Remove old segments
find /var/lib/kawasan/data -name "*.log" -mtime +7 -delete
find /var/lib/kawasan/data -name "*.index" -mtime +7 -delete

# Restart broker
sudo systemctl start kawasan-broker
```

**Option C: Move Data to Larger Volume**

```bash
# Stop broker
sudo systemctl stop kawasan-broker

# Mount larger volume
sudo mkdir /mnt/kawasan-data
sudo mount /dev/sdb1 /mnt/kawasan-data

# Move data
sudo rsync -av /var/lib/kawasan/data/ /mnt/kawasan-data/

# Update config
sudo vim /etc/kawasan/broker.properties
# Set: log.dirs=/mnt/kawasan-data

# Update permissions
sudo chown -R kawasan:kawasan /mnt/kawasan-data

# Restart broker
sudo systemctl restart kawasan-broker
```

#### 3. Verify Recovery

```bash
# Check disk space
df -h /var/lib/kawasan

# Verify broker is running
sudo systemctl status kawasan-broker

# Test produce/consume
# (use your test scripts)
```

### Preventive Measures
- Set up disk usage alerts (e.g., at 80% threshold)
- Configure appropriate log retention
- Monitor growth trends
- Plan capacity upgrades proactively

---

## Slow Consumer

### Severity: P2 (High)

### Symptoms
- Consumer lag growing
- Messages piling up in topics
- Metrics show increasing lag: `kawasan_consumer_lag`
- Consumer processing falls behind production rate

### Immediate Actions

#### 1. Measure Consumer Lag

```bash
# Check lag metrics
curl http://localhost:9094/metrics | grep consumer_lag

# Example output:
# kawasan_consumer_lag{group="my-group",topic="my-topic",partition="0"} 10000
```

#### 2. Identify Bottleneck

**Check consumer health:**
```bash
# If using kafka-python, check consumer logs
# Look for errors, timeouts, or slow processing
```

**Check broker performance:**
```bash
# Check broker CPU/memory
top -p $(pgrep kawasan-broker)

# Check disk I/O
iostat -x 1 5

# Check network
iftop
```

#### 3. Resolution Steps

**Option A: Scale Consumer Horizontally**

Add more consumer instances to the group:

```bash
# Start additional consumer instances
# They will automatically join the group and rebalance

# Example with Python:
# Terminal 1:
python consumer.py --group my-group --topic my-topic

# Terminal 2:
python consumer.py --group my-group --topic my-topic

# Terminal 3:
python consumer.py --group my-group --topic my-topic
```

**Option B: Increase Partitions**

More partitions = more parallelism:

```bash
# Create topic with more partitions
./kawasan-topics --bootstrap-server localhost:9092 \
  --create --topic my-topic-v2 \
  --partitions 12 --replication-factor 1

# Migrate consumers to new topic
```

**Option C: Optimize Consumer Code**

```python
# Example optimizations:

# 1. Increase batch size
consumer = KafkaConsumer(
    max_poll_records=500,  # Process more messages per poll
    fetch_min_bytes=1024   # Fetch larger batches
)

# 2. Use batching in processing
messages = []
for message in consumer:
    messages.append(message)
    if len(messages) >= 100:
        process_batch(messages)
        messages = []

# 3. Commit offsets less frequently
consumer = KafkaConsumer(
    enable_auto_commit=True,
    auto_commit_interval_ms=5000  # Commit every 5 seconds
)
```

**Option D: Temporary Backpressure**

If lag is acceptable and temporary:

```bash
# Monitor lag trend
watch -n 5 'curl -s http://localhost:9094/metrics | grep consumer_lag'

# If lag stabilizes or decreases, no action needed
```

#### 4. Verify Recovery

```bash
# Monitor lag over time
curl http://localhost:9094/metrics | grep consumer_lag

# Should see lag decreasing over time
# Example:
# Time 0:  lag=10000
# Time 60: lag=8000
# Time 120: lag=5000
```

### Post-Incident Actions
- Set up consumer lag alerts
- Document expected processing rate
- Consider auto-scaling for consumers

---

## Replication Lag

### Severity: P2 (High)

### Symptoms
- Follower brokers lagging behind leader
- ISR (In-Sync Replicas) shrinking
- Metrics show high replication lag
- Warning logs: "Replica X is lagging behind"

⚠️ **Note**: Multi-broker replication is not yet fully implemented in Kawasan. This section is for future reference.

### Immediate Actions

#### 1. Identify Lagging Replicas

```bash
# Check ISR status
./kawasan-topics --bootstrap-server localhost:9092 \
  --describe --topic my-topic

# Expected output:
# Topic: my-topic  Partition: 0  Leader: 0  Replicas: 0,1,2  Isr: 0,1,2
#                                                              ^^^^^
# If ISR is smaller than Replicas, some replicas are out of sync
```

#### 2. Check Follower Health

```bash
# Check follower broker status
sudo systemctl status kawasan-broker@1

# Check follower logs
sudo journalctl -u kawasan-broker@1 -n 200 | grep -i replication

# Check network connectivity
ping broker-1
nc -zv broker-1 9092
```

#### 3. Resolution Steps

**If follower is down:**
```bash
# Restart follower
sudo systemctl restart kawasan-broker@1

# Monitor catch-up
sudo journalctl -u kawasan-broker@1 -f | grep -i "catch.*up"
```

**If network issues:**
```bash
# Check network latency
ping -c 10 broker-1

# Check packet loss
mtr broker-1

# Check firewall rules
sudo iptables -L -n
```

**If follower is overloaded:**
```bash
# Check follower resources
ssh broker-1 "top -b -n 1 | head -20"

# Scale up resources if needed
# Or reduce load on follower
```

**If replication backlog is large:**
```bash
# Increase replication throughput
# Edit follower config:
sudo vim /etc/kawasan/broker.properties
# Increase:
# num.io.threads=16
# num.network.threads=8

sudo systemctl restart kawasan-broker@1
```

#### 4. Verify Recovery

```bash
# Monitor ISR
watch -n 5 './kawasan-topics --bootstrap-server localhost:9092 \
  --describe --topic my-topic'

# Wait for ISR to include all replicas
# Isr: 0,1,2 (all replicas in sync)
```

### Post-Incident Actions
- Monitor replication lag metrics
- Set up ISR shrink/grow alerts
- Review network capacity
- Consider increasing `replica.lag.max.messages` if false alarms

---

## Split Brain

### Severity: P1 (Critical)

### Symptoms
- Multiple brokers think they are leader
- Clients see inconsistent metadata
- Raft logs diverge
- Warning: "Split brain detected"

⚠️ **Note**: This scenario is prevented by Raft consensus in properly configured clusters.

### Immediate Actions

#### 1. Identify Split

```bash
# Check Raft leader on each broker
for broker in broker-0 broker-1 broker-2; do
  echo "=== $broker ==="
  ssh $broker "grep 'Raft leader' /var/log/kawasan/broker.log | tail -1"
done

# Should see only ONE broker claiming leadership
```

#### 2. Stop All Brokers

⚠️ **This will cause downtime**

```bash
# Stop all brokers simultaneously
for i in 0 1 2; do
  sudo systemctl stop kawasan-broker@$i
done

# Verify all stopped
for i in 0 1 2; do
  sudo systemctl status kawasan-broker@$i
done
```

#### 3. Identify Correct Leader

```bash
# Check Raft log index on each broker
for i in 0 1 2; do
  echo "=== Broker $i ==="
  # Check latest Raft commit index
  grep "commit index" /var/lib/kawasan-$i/data/raft.log | tail -1
done

# Broker with highest commit index is most up-to-date
```

#### 4. Recovery Steps

**Option A: Clean Restart (Safest)**

```bash
# Start brokers one at a time
# Start broker with highest commit index first
sudo systemctl start kawasan-broker@0
sleep 10

# Wait for it to become leader
grep "became leader" /var/log/kawasan-0/broker.log

# Start other brokers
sudo systemctl start kawasan-broker@1
sleep 5
sudo systemctl start kawasan-broker@2

# They should join as followers
```

**Option B: Reset Minority**

⚠️ **This will lose data on minority brokers**

```bash
# Identify majority (e.g., broker 0 and 1 agree)
# Stop minority (broker 2)
sudo systemctl stop kawasan-broker@2

# Clear minority's Raft state
sudo rm -rf /var/lib/kawasan-2/data/raft-log*

# Restart minority - it will sync from majority
sudo systemctl restart kawasan-broker@2
```

#### 5. Verify Recovery

```bash
# Check cluster health
./kawasan-metadata-check --brokers \
  localhost:9092 localhost:9093 localhost:9094

# Should report: "✓ All brokers have consistent metadata"

# Check Raft status
for i in 0 1 2; do
  echo "=== Broker $i ==="
  grep "Raft.*state" /var/log/kawasan-$i/broker.log | tail -5
done

# Should see: one LEADER, two FOLLOWERS
```

### Preventive Measures
- Always use odd number of brokers (3, 5, 7)
- Ensure reliable network connectivity
- Monitor Raft leader elections
- Set up split-brain detection alerts

---

## High CPU Usage

### Severity: P3 (Medium)

### Symptoms
- CPU usage sustained at 80-100%
- Slow request processing
- High latency metrics

### Immediate Actions

#### 1. Identify CPU Hog

```bash
# Check broker CPU
top -p $(pgrep kawasan-broker)

# Check system-wide CPU
htop
mpstat 1 5

# Check per-thread CPU (Linux)
top -H -p $(pgrep kawasan-broker)
```

#### 2. Profile Hot Paths

**Linux:**
```bash
# Capture profile (30 seconds)
sudo perf record -p $(pgrep kawasan-broker) -g sleep 30

# Analyze
sudo perf report

# Look for hot functions:
# - Serialization/deserialization
# - RocksDB operations
# - Network I/O
```

**macOS:**
```bash
# Use Instruments
sudo instruments -t "Time Profiler" \
  -p $(pgrep kawasan-broker) \
  -l 30000

# Analyze in Instruments.app
```

#### 3. Resolution Steps

**If serialization is hot:**
```bash
# No immediate fix, but note for optimization
# Consider using compression to reduce serialization overhead
```

**If RocksDB is hot:**
```bash
# Check if compaction is running
grep "compaction" /var/log/kawasan/broker.log

# If excessive, tune RocksDB
# (Future config option)
```

**If too many clients:**
```bash
# Check connection count
curl http://localhost:9094/metrics | grep active_connections

# If very high, consider:
# 1. Rate limiting clients
# 2. Increasing `num.network.threads`
# 3. Scaling horizontally
```

**Quick mitigation:**
```bash
# Temporarily reduce load
# 1. Reduce producer rate
# 2. Add more brokers to cluster
# 3. Increase CPU allocation
```

#### 4. Verify Improvement

```bash
# Monitor CPU over time
sar -u 1 60

# Check if latency improved
curl http://localhost:9094/metrics | grep latency
```

### Post-Incident Actions
- Set CPU usage alerts
- Plan capacity upgrade if sustained high load
- File optimization requests for hot paths

---

## Memory Exhaustion

### Severity: P1 (Critical)

### Symptoms
- Broker killed by OOM killer
- Memory usage at 90-100%
- Swap usage high
- Dmesg shows OOM events

### Immediate Actions

#### 1. Check Memory Usage

```bash
# Overall memory
free -h

# Broker memory
ps aux | grep kawasan-broker

# Check OOM kills
sudo dmesg | grep -i "killed process"
sudo journalctl -k | grep -i "out of memory"
```

#### 2. Identify Memory Leak

```bash
# Monitor memory over time
watch -n 5 'ps aux | grep kawasan-broker | grep -v grep'

# If memory grows continuously, likely a leak
```

#### 3. Resolution Steps

**Immediate mitigation:**

```bash
# Restart broker (frees memory)
sudo systemctl restart kawasan-broker

# Set memory limits
sudo vim /etc/systemd/system/kawasan-broker.service
# Add:
# [Service]
# MemoryMax=4G
# MemoryHigh=3G

sudo systemctl daemon-reload
sudo systemctl restart kawasan-broker
```

**Reduce memory usage:**

```bash
# Tune config
sudo vim /etc/kawasan/broker.properties
# Reduce:
# num.io.threads=4
# num.network.threads=2

sudo systemctl restart kawasan-broker
```

**If memory leak suspected:**

```bash
# Run with Valgrind (development only)
valgrind --leak-check=full --log-file=valgrind.log \
  /usr/local/bin/kawasan-broker --config /etc/kawasan/broker.properties

# Analyze valgrind.log for leaks
# Report bug with details
```

#### 4. Verify Stability

```bash
# Monitor memory for 1 hour
sar -r 1 3600 > memory-profile.txt

# Should see stable memory usage, not growing
```

### Post-Incident Actions
- Set memory usage alerts
- If leak confirmed, file bug report
- Consider adding more RAM

---

## Network Issues

### Severity: P2 (High)

### Symptoms
- Clients cannot connect
- Intermittent connection drops
- High network latency
- Timeout errors

### Immediate Actions

#### 1. Test Connectivity

```bash
# From client machine:
nc -zv broker-0 9092
telnet broker-0 9092

# Check DNS resolution
nslookup broker-0
dig broker-0

# Check routing
traceroute broker-0
mtr broker-0
```

#### 2. Check Network Configuration

```bash
# Check listeners
sudo netstat -tulpn | grep kawasan

# Check firewall
sudo iptables -L -n -v
sudo firewall-cmd --list-all

# Check network interface
ifconfig
ip addr show
```

#### 3. Resolution Steps

**If firewall blocking:**

```bash
# Allow Kafka port
sudo firewall-cmd --permanent --add-port=9092/tcp
sudo firewall-cmd --reload

# Or iptables:
sudo iptables -A INPUT -p tcp --dport 9092 -j ACCEPT
sudo iptables-save > /etc/iptables/rules.v4
```

**If DNS issues:**

```bash
# Use IP addresses instead
# Update client config:
bootstrap.servers=192.168.1.10:9092

# Or fix DNS
sudo vim /etc/hosts
# Add: 192.168.1.10 broker-0
```

**If network saturation:**

```bash
# Check bandwidth usage
iftop -i eth0

# Implement QoS if needed
# Or upgrade network capacity
```

#### 4. Verify Recovery

```bash
# Test from multiple clients
for i in {1..10}; do
  nc -zv broker-0 9092 && echo "Success $i" || echo "Fail $i"
done

# Should see all successes
```

### Post-Incident Actions
- Document network topology
- Set up network monitoring
- Review firewall rules regularly

---

## Data Corruption

### Severity: P1 (Critical)

### Symptoms
- Broker fails to start with "corrupt data" error
- RocksDB errors in logs
- Fetch requests return corrupted messages
- Checksum mismatch errors

### Immediate Actions

#### 1. Assess Damage

```bash
# Check logs for corruption errors
sudo journalctl -u kawasan-broker | grep -i corrupt

# Check RocksDB integrity
ldb --db=/var/lib/kawasan/data/meta check
```

#### 2. Stop Broker

```bash
sudo systemctl stop kawasan-broker
```

#### 3. Recovery Steps

**Option A: Restore from Backup**

```bash
# Stop broker
sudo systemctl stop kawasan-broker

# Clear corrupted data
sudo rm -rf /var/lib/kawasan/data/*

# Restore from backup
sudo tar -xzf /backup/kawasan-backup-YYYYMMDD.tar.gz \
  -C /var/lib/kawasan/data

# Fix permissions
sudo chown -R kawasan:kawasan /var/lib/kawasan/data

# Restart
sudo systemctl start kawasan-broker
```

**Option B: Rebuild from Replicas**

(For multi-broker clusters)

```bash
# Remove corrupted broker from cluster
# Let it re-sync from healthy replicas

# On corrupted broker:
sudo systemctl stop kawasan-broker
sudo rm -rf /var/lib/kawasan/data/*

# Restart - will sync from leader
sudo systemctl start kawasan-broker

# Monitor sync progress
sudo journalctl -u kawasan-broker -f | grep -i sync
```

**Option C: Manual Repair**

⚠️ **Last resort - may lose some data**

```bash
# Try RocksDB repair
ldb --db=/var/lib/kawasan/data/meta repair

# Remove corrupted segments
find /var/lib/kawasan/data -name "*.log" -size 0 -delete

# Restart
sudo systemctl start kawasan-broker
```

#### 4. Verify Recovery

```bash
# Check broker starts
sudo systemctl status kawasan-broker

# Test operations
./kawasan-topics --bootstrap-server localhost:9092 --list

# Verify data integrity
# (Use your test scripts)
```

### Post-Incident Actions
- Root cause analysis (hardware failure? software bug?)
- Verify backup procedures
- Consider RAID for data redundancy
- File bug report if software issue

---

## Escalation Procedures

### When to Escalate

- Unable to resolve within SLA timeframe
- Data loss suspected
- Cluster-wide outage
- Unknown root cause
- Repeated incidents

### Escalation Levels

#### Level 1: On-Call Engineer
**Response Time**: 15 minutes  
**Handles**: Standard incidents, following runbook

#### Level 2: Senior Engineer  
**Response Time**: 30 minutes  
**Handles**: Complex issues, multiple component failures

#### Level 3: Engineering Lead
**Response Time**: 1 hour  
**Handles**: Critical incidents, architecture decisions

#### Level 4: Vendor/Community
**Response Time**: Best effort  
**Handles**: Software bugs, feature limitations

### Escalation Channels

- **Slack**: `#kawasan-incidents`
- **PagerDuty**: Escalation policy configured
- **Email**: `oncall@company.com`
- **Phone**: Emergency contact list

### Information to Provide

1. **Incident ID**: Unique identifier
2. **Severity**: P1/P2/P3/P4
3. **Summary**: One-line description
4. **Timeline**: When detected, actions taken
5. **Impact**: Affected services, user count
6. **Logs**: Relevant log excerpts
7. **Metrics**: Screenshots or data
8. **Current State**: What's working, what's not

---

## Post-Incident Review Template

### Incident Summary
- **Date**: YYYY-MM-DD
- **Duration**: HH:MM
- **Severity**: PX
- **Impact**: Description

### Timeline
- **HH:MM** - Incident detected
- **HH:MM** - Response began
- **HH:MM** - Root cause identified
- **HH:MM** - Fix applied
- **HH:MM** - Incident resolved

### Root Cause
- What caused the incident?
- Why wasn't it caught earlier?

### Resolution
- What fixed it?
- What was the impact?

### Action Items
1. [ ] Update monitoring
2. [ ] Add preventive measures
3. [ ] Update runbook
4. [ ] Schedule post-mortem

---

**Document Version**: 1.0  
**Kawasan Version**: 0.2.0-alpha  
**Last Reviewed**: November 24, 2025

