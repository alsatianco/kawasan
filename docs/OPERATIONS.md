# Kawasan Operations Guide

**Version**: 0.2.0-alpha  
**Last Updated**: November 24, 2025

This guide provides comprehensive information for deploying, configuring, and operating Kawasan brokers in production environments.

---

## Table of Contents

1. [Installation](#installation)
2. [Configuration Reference](#configuration-reference)
3. [Starting, Stopping, and Restarting](#starting-stopping-and-restarting)
4. [Monitoring](#monitoring)
5. [Backup and Restore](#backup-and-restore)
6. [Scaling](#scaling)
7. [Troubleshooting](#troubleshooting)
8. [Performance Tuning](#performance-tuning)

---

## Installation

### Prerequisites

- **Operating System**: Linux (Ubuntu 20.04+, CentOS 8+, RHEL 8+) or macOS 11+
- **Memory**: Minimum 4GB RAM (8GB+ recommended for production)
- **Disk**: SSD with at least 100GB free space
- **CPU**: 4+ cores recommended
- **Dependencies**:
  - Boost 1.74+
  - RocksDB 6.11+
  - OpenSSL 1.1+
  - spdlog 1.9+
  - CMake 3.20+

### Installation Methods

#### 1. Using the Installation Script (Linux)

The recommended way to install Kawasan on Linux systems:

```bash
# Build from source
cd kawasan
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
cmake --build . -j$(nproc)

# Run installation script (requires sudo)
cd ..
sudo ./scripts/install.sh
```

This script will:
- Create the `kawasan` user and group
- Install binaries to `/usr/local/bin`
- Create configuration directory at `/etc/kawasan`
- Set up data directory at `/var/lib/kawasan`
- Set up log directory at `/var/log/kawasan`
- Install systemd service file

#### 2. Using Docker

Single broker:

```bash
docker build -t kawasan:latest .
docker run -d \
  -p 9092:9092 \
  -v kawasan-data:/var/lib/kawasan/data \
  --name kawasan-broker \
  kawasan:latest
```

Three-broker cluster:

```bash
docker-compose -f docker-compose-cluster.yml up -d
```

#### 3. Using Kubernetes

```bash
# Apply Kubernetes manifests
kubectl apply -f k8s/kawasan-configmap.yaml
kubectl apply -f k8s/kawasan-statefulset.yaml
kubectl apply -f k8s/kawasan-service.yaml
```

#### 4. Manual Installation

1. Build the project:
   ```bash
   mkdir build && cd build
   cmake -DCMAKE_BUILD_TYPE=Release ..
   cmake --build . -j$(nproc)
   ```

2. Copy binaries to desired location:
   ```bash
   sudo cp build/tools/kawasan-broker /usr/local/bin/
   sudo cp build/tools/kawasan-topics /usr/local/bin/
   ```

3. Create configuration directory:
   ```bash
   sudo mkdir -p /etc/kawasan
   sudo cp config/server.properties.example /etc/kawasan/broker.properties
   ```

4. Create data and log directories:
   ```bash
   sudo mkdir -p /var/lib/kawasan /var/log/kawasan
   sudo chown -R <user>:<group> /var/lib/kawasan /var/log/kawasan
   ```

---

## Configuration Reference

### Essential Configuration Properties

#### Broker Identity

```properties
# Unique identifier for this broker (required in multi-broker clusters)
broker.id=0
```

#### Network Configuration

```properties
# Network interfaces to bind to
listeners=PLAINTEXT://0.0.0.0:9092

# Addresses clients use to connect (hostname or IP)
advertised.listeners=PLAINTEXT://broker-0:9092

# Number of network threads handling requests
num.network.threads=3

# Number of I/O threads
num.io.threads=8
```

#### Storage Configuration

```properties
# Directory where log data is stored
log.dirs=/var/lib/kawasan/data

# Default number of partitions per topic
num.partitions=3

# Default replication factor for topics
default.replication.factor=1

# Log segment size (1GB)
log.segment.bytes=1073741824
```

#### Log Retention Configuration

```properties
# Time-based retention (hours)
log.retention.hours=168

# Size-based retention (bytes, -1 = unlimited)
log.retention.bytes=-1

# Interval for log cleanup (milliseconds)
log.retention.check.interval.ms=300000
```

#### Raft Configuration (Multi-Broker)

```properties
# Port for Raft inter-broker communication
raft.port=9093

# Peer list: broker_id:host:port,broker_id:host:port,...
raft.peers=0:broker-0:9093,1:broker-1:9093,2:broker-2:9093
```

#### Consumer Group Configuration

```properties
# How long to retain consumer group metadata (7 days)
group.retention.ms=604800000

# Interval for consumer lag computation (30 seconds)
consumer.lag.check.interval.ms=30000
```

#### Monitoring Configuration

```properties
# Enable monitoring server
monitoring.enabled=true

# Monitoring server host
monitoring.host=0.0.0.0

# Monitoring server port (Prometheus metrics endpoint)
monitoring.port=9094
```

#### TLS/SSL Configuration

```properties
# Enable SSL for client connections
ssl.enabled=false

# Server certificate (PEM format)
ssl.cert.file=/path/to/server-cert.pem

# Server private key (PEM format)
ssl.key.file=/path/to/server-key.pem

# Private key password (optional)
ssl.key.password=

# CA certificate for client verification (PEM format)
ssl.ca.file=/path/to/ca-cert.pem

# Client authentication: none, requested, required
ssl.client.auth=none
```

#### Raft TLS Configuration

```properties
# Enable SSL for Raft inter-broker communication
raft.ssl.enabled=false

# Raft SSL certificate/key/CA files
raft.ssl.cert.file=/path/to/raft-cert.pem
raft.ssl.key.file=/path/to/raft-key.pem
raft.ssl.ca.file=/path/to/raft-ca.pem
```

### Advanced Configuration

#### Auto-Create Topics

```properties
# Automatically create topics when produced to
auto.create.topics.enable=true
```

#### Compression

```properties
# Compression type for log segments (none, gzip, snappy, lz4, zstd)
compression.type=none
```

#### Resource Limits

```properties
# Maximum message size (1MB)
message.max.bytes=1048576

# Maximum request size (1MB)
max.request.size=1048576
```

---

## Starting, Stopping, and Restarting

### Using Systemd (Linux)

#### Start the Broker

```bash
sudo systemctl start kawasan-broker
```

#### Stop the Broker

```bash
sudo systemctl stop kawasan-broker
```

#### Restart the Broker

```bash
sudo systemctl restart kawasan-broker
```

#### Enable Auto-Start on Boot

```bash
sudo systemctl enable kawasan-broker
```

#### Check Status

```bash
sudo systemctl status kawasan-broker
```

#### View Logs

```bash
# Follow logs in real-time
sudo journalctl -u kawasan-broker -f

# View last 100 lines
sudo journalctl -u kawasan-broker -n 100

# View logs since today
sudo journalctl -u kawasan-broker --since today
```

### Manual Start (Development)

```bash
# Start broker in foreground
./build/tools/kawasan-broker --config config/broker.macos.properties

# Start broker in background
./build/tools/kawasan-broker --config config/broker.macos.properties &

# With custom log level
./build/tools/kawasan-broker --config config/broker.macos.properties --log-level debug
```

### Using Docker

```bash
# Start container
docker start kawasan-broker

# Stop container
docker stop kawasan-broker

# Restart container
docker restart kawasan-broker

# View logs
docker logs -f kawasan-broker
```

### Using Docker Compose

```bash
# Start cluster
docker-compose -f docker-compose-cluster.yml up -d

# Stop cluster
docker-compose -f docker-compose-cluster.yml down

# Restart specific broker
docker-compose -f docker-compose-cluster.yml restart broker-0

# View logs
docker-compose -f docker-compose-cluster.yml logs -f broker-0
```

---

## Monitoring

### Health Checks

#### HTTP Endpoints

Kawasan exposes several health check endpoints:

```bash
# General health check
curl http://localhost:9094/health

# Readiness probe (ready to serve traffic)
curl http://localhost:9094/readiness

# Liveness probe (process is alive)
curl http://localhost:9094/liveness
```

Response format:
```json
{
  "status": "UP",
  "healthy": true,
  "ready": true
}
```

### Metrics

#### Prometheus Metrics

Access metrics in Prometheus format:

```bash
curl http://localhost:9094/metrics
```

Key metrics include:

**Broker Metrics:**
- `kawasan_broker_uptime_seconds` - Broker uptime
- `kawasan_active_connections` - Current active client connections
- `kawasan_topics` - Number of topics
- `kawasan_partitions` - Number of partitions
- `kawasan_consumer_groups` - Number of consumer groups

**Throughput Metrics:**
- `kawasan_messages_produced_total` - Total messages produced
- `kawasan_messages_consumed_total` - Total messages consumed
- `kawasan_bytes_in_total` - Total bytes received
- `kawasan_bytes_out_total` - Total bytes sent

**Latency Metrics:**
- `kawasan_produce_latency_ms` - Produce request latency (p50, p95, p99, p999)
- `kawasan_fetch_latency_ms` - Fetch request latency (p50, p95, p99, p999)

**Consumer Lag:**
- `kawasan_consumer_lag{group="...",topic="...",partition="..."}` - Messages behind

#### Setting Up Prometheus

1. Install Prometheus:
   ```bash
   # Using Docker
   docker run -d -p 9090:9090 \
     -v $PWD/monitoring/prometheus.yml:/etc/prometheus/prometheus.yml \
     prom/prometheus
   ```

2. Configure Prometheus (`monitoring/prometheus.yml`):
   ```yaml
   scrape_configs:
     - job_name: 'kawasan'
       static_configs:
         - targets: ['localhost:9094']
   ```

3. Access Prometheus UI: `http://localhost:9090`

#### Setting Up Grafana

1. Install Grafana:
   ```bash
   docker run -d -p 3000:3000 grafana/grafana
   ```

2. Import dashboard:
   - Navigate to `http://localhost:3000` (admin/admin)
   - Go to Dashboards → Import
   - Upload `monitoring/grafana-dashboard.json`

### Log Monitoring

Logs are written to:
- **Systemd**: `/var/log/syslog` or view with `journalctl`
- **Docker**: Container logs via `docker logs`
- **File**: Configured via spdlog (default: stdout)

Log levels:
- `trace` - Very detailed debugging
- `debug` - Debugging information
- `info` - Informational messages (default)
- `warn` - Warning messages
- `error` - Error messages
- `critical` - Critical errors

Change log level:
```bash
./kawasan-broker --config broker.properties --log-level debug
```

---

## Backup and Restore

### Data Directories

Important directories to backup:

```
/var/lib/kawasan/data/       # Log segments and indexes
/etc/kawasan/                # Configuration files
```

### Backup Procedures

#### 1. Snapshot Backup (Recommended)

```bash
# Stop the broker
sudo systemctl stop kawasan-broker

# Create backup
sudo tar -czf kawasan-backup-$(date +%Y%m%d).tar.gz \
  /var/lib/kawasan/data \
  /etc/kawasan

# Restart the broker
sudo systemctl start kawasan-broker
```

#### 2. Hot Backup (RocksDB Checkpoint)

Kawasan uses RocksDB which supports online backups through checkpoints:

```bash
# Create checkpoint (broker keeps running)
# This creates a consistent snapshot in the checkpoint directory
cp -r /var/lib/kawasan/data /backup/kawasan-checkpoint-$(date +%Y%m%d)
```

#### 3. Volume Snapshots (Cloud)

For cloud deployments, use provider-specific snapshot capabilities:

**AWS EBS:**
```bash
aws ec2 create-snapshot \
  --volume-id vol-xxxxx \
  --description "Kawasan data backup $(date +%Y%m%d)"
```

**Docker Volumes:**
```bash
docker run --rm \
  -v kawasan-data:/data \
  -v $(pwd):/backup \
  ubuntu tar czf /backup/kawasan-data-backup.tar.gz /data
```

### Restore Procedures

#### 1. From Snapshot Backup

```bash
# Stop the broker
sudo systemctl stop kawasan-broker

# Clear existing data
sudo rm -rf /var/lib/kawasan/data/*

# Extract backup
sudo tar -xzf kawasan-backup-YYYYMMDD.tar.gz -C /

# Fix permissions
sudo chown -R kawasan:kawasan /var/lib/kawasan/data

# Restart broker
sudo systemctl start kawasan-broker
```

#### 2. From Checkpoint

```bash
# Stop the broker
sudo systemctl stop kawasan-broker

# Replace data directory
sudo rm -rf /var/lib/kawasan/data
sudo cp -r /backup/kawasan-checkpoint-YYYYMMDD /var/lib/kawasan/data
sudo chown -R kawasan:kawasan /var/lib/kawasan/data

# Restart broker
sudo systemctl start kawasan-broker
```

### Backup Best Practices

1. **Schedule Regular Backups**: Use cron or systemd timers
2. **Retention Policy**: Keep 7 daily, 4 weekly, 12 monthly backups
3. **Test Restores**: Regularly verify backups can be restored
4. **Off-Site Storage**: Store backups in different location/region
5. **Monitor Backup Size**: Track growth trends
6. **Encrypt Backups**: Use encryption for sensitive data

---

## Scaling

### Vertical Scaling

#### Increase Broker Resources

1. **CPU**: Add more cores, increase `num.io.threads` and `num.network.threads`
2. **Memory**: Allocate more RAM for better caching
3. **Disk**: Use faster SSDs, add more disk space

Configuration adjustments for larger machines:

```properties
# 16-core machine
num.io.threads=16
num.network.threads=8

# Larger batch sizes
log.segment.bytes=2147483648  # 2GB segments
```

Update systemd service limits:

```ini
[Service]
LimitNOFILE=200000
MemoryMax=8G
CPUQuota=800%
```

### Horizontal Scaling

#### Adding Brokers to Cluster

**Prerequisites:**
- Raft networking must be fully implemented (currently single-node)
- All brokers must have unique `broker.id`
- Network connectivity between brokers

**Steps:**

1. Configure new broker with unique ID:
   ```properties
   broker.id=3
   raft.port=9093
   raft.peers=0:broker-0:9093,1:broker-1:9093,2:broker-2:9093,3:broker-3:9093
   ```

2. Update existing brokers' `raft.peers` configuration:
   ```properties
   # Add new broker to peers list
   raft.peers=0:broker-0:9093,1:broker-1:9093,2:broker-2:9093,3:broker-3:9093
   ```

3. Rolling restart existing brokers:
   ```bash
   for i in 0 1 2; do
     sudo systemctl restart kawasan-broker@$i
     sleep 30  # Wait for broker to rejoin cluster
   done
   ```

4. Start new broker:
   ```bash
   sudo systemctl start kawasan-broker@3
   ```

5. Verify cluster membership:
   ```bash
   ./kawasan-topics --bootstrap-server localhost:9092 --describe
   ```

#### Removing Brokers

⚠️ **Note**: Multi-broker replication is not yet implemented. This is for future reference.

1. Reassign partitions to remaining brokers
2. Gracefully shut down broker to remove
3. Update `raft.peers` on remaining brokers
4. Rolling restart remaining brokers

### Partition Management

#### Increasing Partitions

More partitions = higher parallelism for consumers:

```bash
# Create topic with 12 partitions
./kawasan-topics --bootstrap-server localhost:9092 \
  --create --topic high-throughput \
  --partitions 12 --replication-factor 3
```

Guidelines:
- More partitions = more parallelism but more overhead
- Aim for 2-4 partitions per broker
- Don't exceed 1000 partitions per broker

---

## Troubleshooting

### Common Issues

#### Broker Won't Start

**Symptoms:**
- Service fails to start
- Error in logs: "Address already in use"

**Solutions:**
1. Check if port is already in use:
   ```bash
   sudo lsof -i :9092
   sudo netstat -tulpn | grep 9092
   ```

2. Check permissions on data directory:
   ```bash
   ls -la /var/lib/kawasan/data
   sudo chown -R kawasan:kawasan /var/lib/kawasan
   ```

3. Check configuration file syntax:
   ```bash
   cat /etc/kawasan/broker.properties
   ```

4. View detailed logs:
   ```bash
   sudo journalctl -u kawasan-broker -n 200 --no-pager
   ```

#### Connection Refused

**Symptoms:**
- Clients cannot connect to broker
- Error: "Connection refused" or "Connection timeout"

**Solutions:**
1. Verify broker is running:
   ```bash
   sudo systemctl status kawasan-broker
   ```

2. Check listener configuration:
   ```properties
   listeners=PLAINTEXT://0.0.0.0:9092
   ```

3. Test connectivity:
   ```bash
   nc -zv localhost 9092
   telnet localhost 9092
   ```

4. Check firewall rules:
   ```bash
   sudo iptables -L -n
   sudo firewall-cmd --list-all
   ```

#### High CPU Usage

**Symptoms:**
- CPU at 100%
- Slow request processing

**Solutions:**
1. Check metrics for hot paths:
   ```bash
   curl http://localhost:9094/metrics | grep latency
   ```

2. Increase I/O threads:
   ```properties
   num.io.threads=16
   ```

3. Review log retention/cleanup:
   ```properties
   log.retention.check.interval.ms=600000
   ```

4. Profile with system tools:
   ```bash
   # Linux
   perf record -p <broker-pid>
   perf report
   
   # macOS
   sudo instruments -t "Time Profiler" -p <broker-pid>
   ```

#### High Memory Usage

**Symptoms:**
- Memory usage keeps growing
- OOM killer kills broker

**Solutions:**
1. Check metrics:
   ```bash
   curl http://localhost:9094/metrics | grep memory
   ```

2. Limit memory in systemd:
   ```ini
   [Service]
   MemoryMax=4G
   MemoryHigh=3G
   ```

3. Reduce cache sizes (future config)
4. Check for memory leaks with valgrind (development only)

#### Slow Consumer

**Symptoms:**
- Consumer lag growing
- Slow message processing

**Solutions:**
1. Check consumer lag:
   ```bash
   curl http://localhost:9094/metrics | grep consumer_lag
   ```

2. Increase consumer parallelism:
   - Add more consumer instances
   - Increase topic partitions

3. Check consumer configuration:
   ```python
   # kafka-python example
   consumer = KafkaConsumer(
       max_poll_records=500,  # Increase batch size
       fetch_min_bytes=1024   # Increase fetch size
   )
   ```

4. Optimize consumer processing code

#### Disk Full

**Symptoms:**
- Broker stops accepting writes
- Error: "No space left on device"

**Solutions:**
1. Check disk usage:
   ```bash
   df -h /var/lib/kawasan
   du -sh /var/lib/kawasan/data/*
   ```

2. Reduce retention:
   ```properties
   log.retention.hours=24
   log.retention.bytes=10737418240  # 10GB
   ```

3. Manually clean old segments:
   ```bash
   sudo systemctl stop kawasan-broker
   # Remove old log segments carefully
   sudo systemctl start kawasan-broker
   ```

4. Add more disk space or move to larger volume

### Debug Techniques

#### Enable Debug Logging

```bash
# Temporary (current session)
./kawasan-broker --config broker.properties --log-level debug

# Permanent (edit service file)
sudo vim /etc/systemd/system/kawasan-broker.service
# Change --log-level info to --log-level debug
sudo systemctl daemon-reload
sudo systemctl restart kawasan-broker
```

#### Network Debugging

```bash
# Capture packets
sudo tcpdump -i any -w kawasan-traffic.pcap port 9092

# View Kafka protocol traffic (requires kafka tools)
kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic test --from-beginning
```

#### RocksDB Debugging

```bash
# Check RocksDB stats
ldb --db=/var/lib/kawasan/data/meta dump_live_files

# Compact database
ldb --db=/var/lib/kawasan/data/meta compact
```

---

## Performance Tuning

### Operating System Tuning

#### Linux System Limits

Edit `/etc/security/limits.conf`:

```
kawasan soft nofile 100000
kawasan hard nofile 100000
kawasan soft nproc 32768
kawasan hard nproc 32768
```

#### Kernel Parameters

Edit `/etc/sysctl.conf`:

```bash
# Network settings
net.core.somaxconn=1024
net.ipv4.tcp_max_syn_backlog=4096
net.core.netdev_max_backlog=5000

# Memory settings
vm.swappiness=1
vm.dirty_ratio=15
vm.dirty_background_ratio=5

# File descriptor limits
fs.file-max=2097152
```

Apply:
```bash
sudo sysctl -p
```

#### Disk I/O Scheduler

For SSDs, use `noop` or `none`:

```bash
echo noop | sudo tee /sys/block/sda/queue/scheduler
# Or add to /etc/default/grub:
# GRUB_CMDLINE_LINUX="elevator=noop"
```

### Broker Configuration Tuning

#### High Throughput

```properties
# More threads
num.io.threads=16
num.network.threads=8

# Larger segments
log.segment.bytes=2147483648

# Compression
compression.type=lz4

# Batch sizes
batch.size=65536
```

#### Low Latency

```properties
# Fewer threads (reduce context switching)
num.io.threads=4
num.network.threads=2

# Smaller segments for faster rotation
log.segment.bytes=536870912

# Disable compression
compression.type=none

# Smaller batches
batch.size=16384
```

#### Balanced

```properties
num.io.threads=8
num.network.threads=3
log.segment.bytes=1073741824
compression.type=snappy
batch.size=32768
```

### Hardware Recommendations

#### Production Deployment

**Recommended:**
- **CPU**: 8-16 cores (Intel Xeon or AMD EPYC)
- **Memory**: 16-32 GB RAM
- **Disk**: NVMe SSD with 500+ GB
- **Network**: 1 Gbps minimum, 10 Gbps preferred

**Minimum:**
- **CPU**: 4 cores
- **Memory**: 8 GB RAM
- **Disk**: SSD with 100 GB
- **Network**: 100 Mbps

#### Storage Sizing

Estimate required storage:

```
Storage = (messages/day) × (message_size) × (retention_days) × (replication_factor)

Example:
1M msgs/day × 1KB × 7 days × 3 replicas = 21 GB
```

Add 20-30% overhead for indexes and overhead.

---

## Security Best Practices

1. **Run as non-root user**: Always use dedicated `kawasan` user
2. **Enable TLS**: Use SSL/TLS for production deployments
3. **Firewall**: Restrict access to broker ports
4. **File permissions**: Ensure data directories are not world-readable
5. **Regular updates**: Keep dependencies up to date
6. **Audit logging**: Enable logging for all operations
7. **Network segmentation**: Isolate broker network
8. **Backup encryption**: Encrypt backups at rest

---

## Getting Help

### Resources

- **Documentation**: `https://github.com/yourusername/kawasan/docs`
- **GitHub Issues**: `https://github.com/yourusername/kawasan/issues`
- **Discussions**: `https://github.com/yourusername/kawasan/discussions`

### Reporting Issues

When reporting issues, include:

1. Kawasan version: `kawasan-broker --version`
2. Operating system and version
3. Configuration file (sanitized)
4. Relevant logs
5. Steps to reproduce
6. Expected vs. actual behavior

---

**Document Version**: 1.0  
**Kawasan Version**: 0.2.0-alpha  
**Last Reviewed**: November 24, 2025
