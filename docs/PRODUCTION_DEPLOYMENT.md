# Production Deployment Guide

This guide covers deploying Kawasan broker in production environments on both Linux (systemd) and macOS (launchd) systems.

## Table of Contents

- [Prerequisites](#prerequisites)
- [Linux Deployment (systemd)](#linux-deployment-systemd)
  - [Quick Installation](#quick-installation)
  - [Manual Installation](#manual-installation)
  - [Configuration](#configuration)
  - [Service Management](#service-management)
- [macOS Deployment (launchd)](#macos-deployment-launchd)
  - [Quick Installation (macOS)](#quick-installation-macos)
  - [Manual Installation (macOS)](#manual-installation-macos)
  - [Service Management (macOS)](#service-management-macos)
- [Monitoring and Logging](#monitoring-and-logging)
- [Health Checks and Metrics](#health-checks-and-metrics)
- [Security](#security)
- [Performance Tuning](#performance-tuning)
- [Troubleshooting](#troubleshooting)
- [Upgrade Procedures](#upgrade-procedures)

---

## Prerequisites

### System Requirements

- **Operating System**: Linux with systemd (Ubuntu 18.04+, CentOS 7+, RHEL 7+, Debian 9+)
- **CPU**: 4+ cores recommended
- **Memory**: 4GB+ RAM minimum, 8GB+ recommended
- **Disk**: SSD recommended for low-latency operations
- **Network**: 1Gbps+ network interface

### Software Requirements

- CMake 3.15+
- GCC 9+ or Clang 10+
- Build dependencies (see `docs/GETTING_STARTED.md`)

### Build the Project

Before installation, build Kawasan:

```bash
cd /path/to/kawasan
./scripts/quick-start.sh
```

This will create the `build/` directory with compiled binaries.

---

## Linux Deployment (systemd)

This section covers deployment on Linux systems using systemd.

## Quick Installation

The automated installation script handles all setup steps:

```bash
# From the kawasan project root
sudo ./scripts/install-systemd-service.sh

# Or specify a custom build directory
sudo ./scripts/install-systemd-service.sh /path/to/build
```

This script will:

1. Create the `kawasan` system user and group
2. Set up directory structure in `/opt/kawasan`, `/etc/kawasan`, `/var/lib/kawasan`, `/var/log/kawasan`
3. Install binaries to `/usr/local/bin/`
4. Copy configuration files
5. Install and enable the systemd service
6. Set appropriate permissions

After installation, configure and start the service:

```bash
# Edit configuration
sudo nano /etc/kawasan/broker.properties

# Start the service
sudo systemctl start kawasan-broker

# Check status
sudo systemctl status kawasan-broker
```

---

## Manual Installation

If you prefer manual installation or need to customize the process:

### 1. Create System User

```bash
sudo useradd --system --home-dir /opt/kawasan --shell /bin/false \
    --comment "Kawasan Broker Service" kawasan
```

### 2. Create Directory Structure

```bash
sudo mkdir -p /opt/kawasan
sudo mkdir -p /etc/kawasan
sudo mkdir -p /var/lib/kawasan/data
sudo mkdir -p /var/log/kawasan
```

### 3. Install Binaries

```bash
sudo cp build/tools/kawasan-broker /usr/local/bin/
sudo cp build/tools/kawasan-topics /usr/local/bin/
sudo chmod +x /usr/local/bin/kawasan-broker
sudo chmod +x /usr/local/bin/kawasan-topics
```

### 4. Install Configuration

```bash
sudo cp config/broker.production.properties /etc/kawasan/broker.properties
sudo chmod 644 /etc/kawasan/broker.properties
```

### 5. Set Permissions

```bash
sudo chown -R kawasan:kawasan /opt/kawasan
sudo chown -R kawasan:kawasan /var/lib/kawasan
sudo chown -R kawasan:kawasan /var/log/kawasan
```

### 6. Install Systemd Service

```bash
sudo cp systemd/kawasan-broker.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable kawasan-broker
```

---

## Configuration

### Production Configuration File

The main configuration file is `/etc/kawasan/broker.properties` (JSON format):

```json
{
  "broker.id": 0,
  "host": "0.0.0.0",
  "port": 9092,
  "log.dirs": "/var/lib/kawasan/data",
  
  "num.network.threads": 8,
  "num.io.threads": 8,
  "socket.send.buffer.bytes": 102400,
  "socket.receive.buffer.bytes": 102400,
  
  "log.segment.bytes": 1073741824,
  "log.retention.hours": 168,
  "log.retention.check.interval.ms": 300000,
  
  "default.replication.factor": 3,
  "min.insync.replicas": 2,
  
  "compression.type": "snappy"
}
```

### Key Configuration Parameters

| Parameter | Description | Default | Production Recommendation |
|-----------|-------------|---------|---------------------------|
| `broker.id` | Unique broker ID | 0 | Set uniquely per broker (0, 1, 2...) |
| `host` | Bind address | localhost | `0.0.0.0` for all interfaces |
| `port` | Broker port | 9092 | 9092 (standard Kafka port) |
| `log.dirs` | Data directory | /tmp | `/var/lib/kawasan/data` |
| `num.network.threads` | Network threads | 3 | 8-16 for high throughput |
| `num.io.threads` | I/O threads | 8 | Match CPU cores |
| `log.retention.hours` | Log retention | 168 | Adjust based on storage |
| `compression.type` | Message compression | none | `snappy` or `lz4` |

### Environment Variables

The systemd service sets these environment variables:

- `KAWASAN_HOME=/opt/kawasan`
- `KAWASAN_LOG_DIR=/var/log/kawasan`
- `KAWASAN_DATA_DIR=/var/lib/kawasan`

You can override them in `/etc/systemd/system/kawasan-broker.service`.

---

## Service Management

### Basic Commands

```bash
# Start the broker
sudo systemctl start kawasan-broker

# Stop the broker
sudo systemctl stop kawasan-broker

# Restart the broker
sudo systemctl restart kawasan-broker

# Check status
sudo systemctl status kawasan-broker

# Enable auto-start on boot
sudo systemctl enable kawasan-broker

# Disable auto-start
sudo systemctl disable kawasan-broker
```

### Verify Service is Running

```bash
# Check if process is running
sudo systemctl is-active kawasan-broker

# View recent logs
sudo journalctl -u kawasan-broker -n 100

# Follow logs in real-time
sudo journalctl -u kawasan-broker -f

# Check port is listening
sudo ss -tlnp | grep 9092
```

### Graceful Shutdown

The service is configured for graceful shutdown with a 30-second timeout:

```bash
sudo systemctl stop kawasan-broker
```

This sends `SIGTERM` and waits up to 30 seconds before forcing shutdown.

---

## Monitoring and Logging

### Log Management

Logs are managed by systemd's journald:

```bash
# View all logs
sudo journalctl -u kawasan-broker

# View logs from today
sudo journalctl -u kawasan-broker --since today

# View logs from last hour
sudo journalctl -u kawasan-broker --since "1 hour ago"

# View logs with priority ERROR and above
sudo journalctl -u kawasan-broker -p err

# Export logs to file
sudo journalctl -u kawasan-broker > kawasan-logs.txt
```

### JSON Logging

For centralized logging systems (ELK, Splunk, Loki), enable JSON format:

```bash
# Edit configuration to enable JSON logging
sudo nano /etc/kawasan/broker.properties

# Add or modify:
# log.format=json
```

Logs will be output in JSON format:
```json
{"timestamp":"2025-11-18T10:30:45.123","level":"info","thread":"12345","message":"Broker started"}
```

### Log Rotation

Journald automatically rotates logs. Configure rotation in `/etc/systemd/journald.conf`:

```ini
[Journal]
SystemMaxUse=1G
SystemMaxFileSize=100M
MaxRetentionSec=1week
```

Apply changes:

```bash
sudo systemctl restart systemd-journald
```

---

## Health Checks and Metrics

Kawasan broker exposes HTTP endpoints for health checks and Prometheus metrics on port **8080** by default.

### Health Check Endpoints

#### Overall Health Status
```bash
curl http://localhost:8080/health
# or
curl http://localhost:8080/healthz
```

Response:
```json
{
  "status": "UP",
  "healthy": true,
  "ready": true
}
```

#### Readiness Probe (Kubernetes)
```bash
curl http://localhost:8080/ready
# or
curl http://localhost:8080/readiness
```

Response:
```json
{
  "status": "READY",
  "ready": true
}
```

#### Liveness Probe (Kubernetes)
```bash
curl http://localhost:8080/live
# or
curl http://localhost:8080/liveness
```

Response:
```json
{
  "status": "ALIVE",
  "alive": true
}
```

### Prometheus Metrics

Metrics are exposed in Prometheus text format:

```bash
curl http://localhost:8080/metrics
```

#### Key Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `kawasan_broker_uptime_seconds` | Gauge | Broker uptime in seconds |
| `kawasan_messages_produced_total` | Counter | Total messages produced |
| `kawasan_messages_consumed_total` | Counter | Total messages consumed |
| `kawasan_bytes_in_total` | Counter | Total bytes received |
| `kawasan_bytes_out_total` | Counter | Total bytes sent |
| `kawasan_active_connections` | Gauge | Current active connections |
| `kawasan_topics` | Gauge | Total number of topics |
| `kawasan_partitions` | Gauge | Total number of partitions |
| `kawasan_consumer_groups` | Gauge | Total number of consumer groups |
| `kawasan_disk_usage_bytes` | Gauge | Disk usage in bytes |
| `kawasan_memory_usage_bytes` | Gauge | Memory usage in bytes |
| `kawasan_produce_latency_ms` | Histogram | Produce request latency (P50, P95, P99) |
| `kawasan_fetch_latency_ms` | Histogram | Fetch request latency (P50, P95, P99) |
| `kawasan_requests_total` | Counter | Total requests by API |
| `kawasan_request_errors_total` | Counter | Total request errors by API |

### Monitoring Stack Setup

A complete monitoring stack (Prometheus + Grafana + Alertmanager) is available in the `monitoring/` directory.

#### Quick Start with Docker Compose

```bash
cd monitoring/
docker-compose -f docker-compose.monitoring.yml up -d

# Access Grafana at http://localhost:3000 (admin/admin)
# Access Prometheus at http://localhost:9090
# Access Alertmanager at http://localhost:9093
```

#### Prometheus Configuration

Edit `monitoring/prometheus.yml` to add your broker targets:

```yaml
scrape_configs:
  - job_name: 'kawasan-broker'
    static_configs:
      - targets: ['broker-1:8080', 'broker-2:8080', 'broker-3:8080']
```

#### Grafana Dashboard

Import the pre-built dashboard:

1. Access Grafana at http://localhost:3000
2. Go to **Dashboards** → **Import**
3. Upload `monitoring/grafana-dashboard.json`
4. Select Prometheus data source
5. Click **Import**

The dashboard includes:
- Broker status and uptime
- Message and network throughput
- Latency metrics (P50, P95, P99)
- Resource utilization (CPU, memory, disk)
- Request rates and errors by API
- Active connections and consumer groups

#### Alerting Rules

Prometheus alerting rules are defined in `monitoring/alerts.yml`:

- **Broker Down**: Alert when broker is unavailable
- **High Latency**: Alert on P99 latency > 1000ms
- **Disk Space**: Warning at 80GB, critical at 90GB
- **Memory Usage**: Alert when memory > 6GB
- **High Error Rate**: Alert on excessive request errors
- **Connection Issues**: Alert on too many or no connections

Configure Alertmanager in `monitoring/alertmanager.yml` for notifications (Slack, email, PagerDuty, etc.).

For detailed monitoring setup instructions, see [`monitoring/README.md`](../monitoring/README.md).

---

## macOS Deployment (launchd)

This section covers deployment on macOS systems using launchd.

### Quick Installation (macOS)

The automated installation script handles all setup steps:

```bash
# From the kawasan project root
sudo ./scripts/install-macos-service.sh

# Or specify custom paths
sudo ./scripts/install-macos-service.sh \
    --broker-path ./build/tools/kawasan-broker \
    --config-path ./config/broker.macos.properties

# Install to run as specific user
sudo ./scripts/install-macos-service.sh --user kawasan
```

This script will:

1. Create the installation directories
2. Copy broker binary to `/usr/local/bin/kawasan-broker`
3. Copy configuration to `/usr/local/etc/kawasan/broker.properties`
4. Create data directory at `/usr/local/var/kawasan`
5. Create log directory at `/usr/local/var/log/kawasan`
6. Install launchd service plist to `/Library/LaunchDaemons/com.kawasan.broker.plist`
7. Set appropriate permissions
8. Load and start the service

After installation:

```bash
# Check status
sudo launchctl list | grep kawasan

# View logs
tail -f /usr/local/var/log/kawasan/broker.log
```

### Manual Installation (macOS)

If you prefer manual installation:

#### 1. Create Directory Structure

```bash
sudo mkdir -p /usr/local/bin
sudo mkdir -p /usr/local/etc/kawasan
sudo mkdir -p /usr/local/var/kawasan/data
sudo mkdir -p /usr/local/var/log/kawasan
```

#### 2. Install Binaries

```bash
sudo cp build/tools/kawasan-broker /usr/local/bin/
sudo cp build/tools/kawasan-topics /usr/local/bin/
sudo chmod 755 /usr/local/bin/kawasan-broker
sudo chmod 755 /usr/local/bin/kawasan-topics
```

#### 3. Install Configuration

```bash
sudo cp config/broker.macos.properties /usr/local/etc/kawasan/broker.properties
sudo chmod 644 /usr/local/etc/kawasan/broker.properties
```

#### 4. Create launchd Service Plist

Create `/Library/LaunchDaemons/com.kawasan.broker.plist`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.kawasan.broker</string>

    <key>ProgramArguments</key>
    <array>
        <string>/usr/local/bin/kawasan-broker</string>
        <string>--config</string>
        <string>/usr/local/etc/kawasan/broker.properties</string>
        <string>--log-level</string>
        <string>info</string>
    </array>

    <key>RunAtLoad</key>
    <true/>

    <key>KeepAlive</key>
    <dict>
        <key>SuccessfulExit</key>
        <false/>
        <key>Crashed</key>
        <true/>
    </dict>

    <key>WorkingDirectory</key>
    <string>/usr/local/var/kawasan</string>

    <key>StandardOutPath</key>
    <string>/usr/local/var/log/kawasan/broker.log</string>

    <key>StandardErrorPath</key>
    <string>/usr/local/var/log/kawasan/broker-error.log</string>

    <key>EnvironmentVariables</key>
    <dict>
        <key>PATH</key>
        <string>/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin</string>
    </dict>

    <key>ProcessType</key>
    <string>Background</string>

    <key>ThrottleInterval</key>
    <integer>10</integer>

    <key>ExitTimeOut</key>
    <integer>30</integer>

    <key>SoftResourceLimits</key>
    <dict>
        <key>NumberOfFiles</key>
        <integer>65536</integer>
    </dict>

    <key>HardResourceLimits</key>
    <dict>
        <key>NumberOfFiles</key>
        <integer>65536</integer>
    </dict>
</dict>
</plist>
```

Set proper permissions:

```bash
sudo chmod 644 /Library/LaunchDaemons/com.kawasan.broker.plist
```

#### 5. Load the Service

```bash
sudo launchctl load /Library/LaunchDaemons/com.kawasan.broker.plist
```

### Service Management (macOS)

#### Basic Commands

```bash
# Start the broker
sudo launchctl load /Library/LaunchDaemons/com.kawasan.broker.plist

# Stop the broker
sudo launchctl unload /Library/LaunchDaemons/com.kawasan.broker.plist

# Restart the broker
sudo launchctl unload /Library/LaunchDaemons/com.kawasan.broker.plist
sudo launchctl load /Library/LaunchDaemons/com.kawasan.broker.plist

# Check if service is loaded
sudo launchctl list | grep kawasan

# Get detailed service info
sudo launchctl list com.kawasan.broker
```

#### Verify Service is Running

```bash
# Check if process is running
ps aux | grep kawasan-broker | grep -v grep

# Check port is listening
lsof -i :9092
# or
netstat -an | grep 9092

# View logs
tail -f /usr/local/var/log/kawasan/broker.log
tail -f /usr/local/var/log/kawasan/broker-error.log
```

#### Uninstall Service (macOS)

Use the provided uninstall script:

```bash
# Complete uninstallation (removes everything)
sudo ./scripts/uninstall-macos-service.sh

# Keep data and config
sudo ./scripts/uninstall-macos-service.sh --keep-data --keep-config

# Keep only data
sudo ./scripts/uninstall-macos-service.sh --keep-data
```

Or manually:

```bash
# Unload service
sudo launchctl unload /Library/LaunchDaemons/com.kawasan.broker.plist

# Remove plist
sudo rm /Library/LaunchDaemons/com.kawasan.broker.plist

# Remove binaries (optional)
sudo rm /usr/local/bin/kawasan-broker
sudo rm /usr/local/bin/kawasan-topics

# Remove data and config (optional - only if you want complete removal)
sudo rm -rf /usr/local/etc/kawasan
sudo rm -rf /usr/local/var/kawasan
sudo rm -rf /usr/local/var/log/kawasan
```

#### macOS-Specific Configuration

The macOS production configuration (`config/broker.macos.properties`) uses:

- Data directory: `/usr/local/var/kawasan/data`
- Lower thread counts (4 network threads, 4 I/O threads) suitable for typical macOS hardware
- Smaller log segment size (512MB vs 1GB on Linux)
- Single replica factor (suitable for single-broker development/testing)

For production multi-broker clusters on macOS, adjust replication settings accordingly.

#### macOS System Limits

Increase system limits for better performance:

```bash
# Check current limits
launchctl limit maxfiles

# Increase file descriptor limits (requires reboot)
sudo launchctl limit maxfiles 65536 200000
```

Or create `/Library/LaunchDaemons/limit.maxfiles.plist`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple Computer//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>limit.maxfiles</string>
    <key>ProgramArguments</key>
    <array>
        <string>launchctl</string>
        <string>limit</string>
        <string>maxfiles</string>
        <string>65536</string>
        <string>200000</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>ServiceIPC</key>
    <false/>
</dict>
</plist>
```

Then load it:

```bash
sudo launchctl load -w /Library/LaunchDaemons/limit.maxfiles.plist
```

---

## Monitoring and Logging

Check broker health:

```bash
# Check if broker is listening
nc -zv localhost 9092

# Check process status
ps aux | grep kawasan-broker

# Check resource usage
top -p $(pgrep kawasan-broker)
```

### Metrics and Monitoring

For production monitoring, integrate with:

- **Prometheus**: Export metrics endpoint (future enhancement)
- **Grafana**: Visualize metrics and create dashboards
- **ELK Stack**: Centralized log aggregation
- **Nagios/Icinga**: Service availability monitoring

---

## Security

### User Isolation

The service runs as the unprivileged `kawasan` user:

```bash
# Verify user
ps aux | grep kawasan-broker | grep -v grep
```

### File Permissions

Secure configuration files:

```bash
# Configuration should be readable by kawasan user
sudo chmod 644 /etc/kawasan/broker.properties
sudo chown root:root /etc/kawasan/broker.properties

# Data directories owned by kawasan
sudo chown -R kawasan:kawasan /var/lib/kawasan
sudo chmod -R 755 /var/lib/kawasan
```

### Firewall Configuration

#### Using firewalld (CentOS/RHEL/Fedora)

```bash
sudo firewall-cmd --permanent --add-port=9092/tcp
sudo firewall-cmd --reload
```

#### Using ufw (Ubuntu/Debian)

```bash
sudo ufw allow 9092/tcp
sudo ufw reload
```

#### Using iptables

```bash
sudo iptables -A INPUT -p tcp --dport 9092 -j ACCEPT
sudo iptables-save > /etc/iptables/rules.v4
```

#### Using macOS Application Firewall

```bash
# Allow incoming connections to kawasan-broker
sudo /usr/libexec/ApplicationFirewall/socketfilterfw --add /usr/local/bin/kawasan-broker
sudo /usr/libexec/ApplicationFirewall/socketfilterfw --unblockapp /usr/local/bin/kawasan-broker

# Enable firewall (if not already enabled)
sudo /usr/libexec/ApplicationFirewall/socketfilterfw --setglobalstate on
```

### Security Hardening (Optional)

Uncomment security options in `/etc/systemd/system/kawasan-broker.service`:

```ini
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/var/lib/kawasan /var/log/kawasan
```

Then reload and restart:

```bash
sudo systemctl daemon-reload
sudo systemctl restart kawasan-broker
```

---

## Performance Tuning

### OS-Level Tuning

Create `/etc/sysctl.d/99-kawasan.conf`:

```ini
# Increase file descriptor limits
fs.file-max = 100000

# Disable swap for better performance
vm.swappiness = 1

# Increase dirty page thresholds
vm.dirty_ratio = 80
vm.dirty_background_ratio = 5

# Network buffer sizes
net.core.rmem_max = 134217728
net.core.wmem_max = 134217728
net.ipv4.tcp_rmem = 4096 87380 134217728
net.ipv4.tcp_wmem = 4096 65536 134217728

# Connection handling
net.ipv4.tcp_max_syn_backlog = 4096
net.core.somaxconn = 1024
```

Apply changes:

```bash
sudo sysctl -p /etc/sysctl.d/99-kawasan.conf
```

### File Descriptor Limits

The systemd service sets `LimitNOFILE=100000`. Verify:

```bash
# Check current limits for the service
sudo systemctl show kawasan-broker | grep LimitNOFILE
```

### CPU and Memory Limits

Uncomment and adjust in `/etc/systemd/system/kawasan-broker.service`:

```ini
CPUQuota=400%          # 4 CPU cores max
MemoryMax=4G           # Hard memory limit
MemoryHigh=3G          # Soft memory limit (triggers throttling)
```

### Disk I/O Optimization

Use SSDs for `/var/lib/kawasan/data` and mount with optimal options:

```bash
# In /etc/fstab
/dev/sdb1  /var/lib/kawasan  ext4  noatime,nodiratime  0  2
```

---

## Troubleshooting

### Service Won't Start

1. **Check logs**:
   ```bash
   sudo journalctl -u kawasan-broker -n 50
   ```

2. **Verify configuration**:
   ```bash
   cat /etc/kawasan/broker.properties
   ```

3. **Check permissions**:
   ```bash
   ls -la /var/lib/kawasan
   ls -la /var/log/kawasan
   ```

4. **Verify binary exists**:
   ```bash
   ls -la /usr/local/bin/kawasan-broker
   ```

### Port Already in Use

Check what's using port 9092:

```bash
sudo ss -tlnp | grep 9092
sudo lsof -i :9092
```

Kill conflicting process or change port in configuration.

### High Memory Usage

Check memory consumption:

```bash
sudo systemctl status kawasan-broker
ps aux | grep kawasan-broker
```

Adjust `MemoryMax` in the systemd service file.

### Slow Performance

1. Check disk I/O:
   ```bash
   iostat -x 5
   ```

2. Check network:
   ```bash
   iftop
   ```

3. Check CPU:
   ```bash
   top -p $(pgrep kawasan-broker)
   ```

4. Review configuration tuning parameters

### Service Crashes Repeatedly

The service has automatic restart configured with:
- `Restart=on-failure`
- `RestartSec=10s`
- `StartLimitBurst=5` (max 5 restarts in 5 minutes)

Check why it's crashing:

```bash
sudo journalctl -u kawasan-broker -p err
```

---

## Upgrade Procedures

### Minor Version Upgrade

1. **Build new version**:
   ```bash
   cd /path/to/kawasan
   git pull
   ./scripts/quick-start.sh
   ```

2. **Stop service**:
   ```bash
   sudo systemctl stop kawasan-broker
   ```

3. **Backup**:
   ```bash
   sudo cp -a /var/lib/kawasan /var/lib/kawasan.backup
   sudo cp /etc/kawasan/broker.properties /etc/kawasan/broker.properties.backup
   ```

4. **Install new binary**:
   ```bash
   sudo cp build/tools/kawasan-broker /usr/local/bin/
   ```

5. **Start service**:
   ```bash
   sudo systemctl start kawasan-broker
   ```

6. **Verify**:
   ```bash
   sudo systemctl status kawasan-broker
   sudo journalctl -u kawasan-broker -f
   ```

### Major Version Upgrade

Follow the same steps as minor upgrade, but also:

1. Review release notes for breaking changes
2. Update configuration file format if changed
3. Test in staging environment first
4. Plan for downtime or rolling upgrade (cluster)

### Rollback

If upgrade fails:

```bash
# Stop service
sudo systemctl stop kawasan-broker

# Restore binary
sudo cp /usr/local/bin/kawasan-broker.old /usr/local/bin/kawasan-broker

# Restore data
sudo rm -rf /var/lib/kawasan
sudo mv /var/lib/kawasan.backup /var/lib/kawasan

# Restore config
sudo cp /etc/kawasan/broker.properties.backup /etc/kawasan/broker.properties

# Start service
sudo systemctl start kawasan-broker
```

---

## Production Checklist

### Linux (systemd) Deployment Checklist

Before going to production on Linux:

- [ ] Build Kawasan in Release or RelWithDebInfo mode
- [ ] Create dedicated `kawasan` system user
- [ ] Set up proper directory structure (`/opt/kawasan`, `/etc/kawasan`, `/var/lib/kawasan`, `/var/log/kawasan`)
- [ ] Configure production settings in `/etc/kawasan/broker.properties`
- [ ] Set correct file permissions (644 for config, 755 for directories)
- [ ] Install and enable systemd service
- [ ] Configure firewall to allow port 9092
- [ ] Set up OS-level tuning (sysctl, ulimits)
- [ ] Configure log rotation via journald
- [ ] Set up monitoring and alerting
- [ ] Document backup and recovery procedures
- [ ] Test graceful shutdown and restart
- [ ] Test automatic restart on failure
- [ ] Create runbooks for common operations
- [ ] Plan upgrade procedures
- [ ] Set up staging environment for testing
- [ ] Configure TLS/SSL (when available)
- [ ] Set up authentication (when available)

### macOS (launchd) Deployment Checklist

Before going to production on macOS:

- [ ] Build Kawasan in Release or RelWithDebInfo mode
- [ ] Set up directory structure (`/usr/local/bin`, `/usr/local/etc/kawasan`, `/usr/local/var/kawasan`, `/usr/local/var/log/kawasan`)
- [ ] Configure production settings in `/usr/local/etc/kawasan/broker.properties`
- [ ] Set correct file permissions (644 for config, 755 for directories)
- [ ] Install and load launchd service
- [ ] Configure application firewall to allow port 9092
- [ ] Increase system file descriptor limits (launchctl limit maxfiles)
- [ ] Set up log rotation or monitoring of log file sizes
- [ ] Set up monitoring and alerting
- [ ] Document backup and recovery procedures
- [ ] Test graceful shutdown and restart (launchctl unload/load)
- [ ] Test automatic restart on crash (KeepAlive configuration)
- [ ] Create runbooks for common operations
- [ ] Plan upgrade procedures
- [ ] Consider using dedicated user (not current user) for production
- [ ] Configure TLS/SSL (when available)
- [ ] Set up authentication (when available)

---

## Additional Resources

- [Getting Started Guide](GETTING_STARTED.md)
- [Architecture Documentation](ARCHITECTURE.md)
- [API Coverage Matrix](api_coverage_matrix.md)
- [Project Status](PROJECT_STATUS.md)

---

## Support

For issues, questions, or contributions:

- GitHub Issues: https://github.com/yourusername/kawasan/issues
- Documentation: `/docs` directory
- Community: (add community links)

---

**Last Updated**: 2025-11-18
