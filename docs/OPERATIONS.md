# Kawasan Operations Guide

How to install, run, monitor, back up, scale, troubleshoot, and tune a Kawasan broker. This guide covers the operational surface; for system design see [./ARCHITECTURE.md](./ARCHITECTURE.md), for the full config-key reference see [./CONFIGURATION.md](./CONFIGURATION.md), and for protocol/API support see [./api_coverage_matrix.md](./api_coverage_matrix.md).

The **single-node broker is the primary, supported deployment**. Multi-broker Raft replication exists but is not production-hardened; caveats are called out where relevant.

## Table of Contents

- [Installation](#installation)
  - [Build](#build)
  - [Install script (Linux)](#install-script-linux)
  - [Docker](#docker)
  - [Docker Compose cluster](#docker-compose-cluster)
  - [Kubernetes](#kubernetes)
  - [macOS service (launchd)](#macos-service-launchd)
  - [Linux service (systemd)](#linux-service-systemd)
- [Configuration essentials](#configuration-essentials)
- [Running and lifecycle management](#running-and-lifecycle-management)
- [Monitoring](#monitoring)
  - [Ports](#ports)
  - [Health endpoints](#health-endpoints)
  - [Metrics catalog](#metrics-catalog)
  - [Prometheus and Grafana](#prometheus-and-grafana)
- [Backup and restore](#backup-and-restore)
- [Scaling](#scaling)
- [Troubleshooting playbook](#troubleshooting-playbook)
- [Incident severity](#incident-severity)
- [Performance tuning](#performance-tuning)
  - [Targets](#targets)
  - [OS tuning](#os-tuning)
  - [Profiling](#profiling)
- [Connecting Kafka UI and admin tools](#connecting-kafka-ui-and-admin-tools)
- [Running the test suites](#running-the-test-suites)

---

## Installation

### Build

All installation paths start from a build. See [../README.md](../README.md) for prerequisites and the canonical build commands. In brief:

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build build -j$(nproc 2>/dev/null || sysctl -n hw.logicalcpu)
```

This produces the broker and CLI tools under `build/tools/`: `kawasan-broker`, `kawasan-topics`, `kawasan-groups`, `kawasan-metadata-check`.

The broker accepts these CLI flags (any of which override the config file): `--config`/`-c`, `--broker-id`, `--host`, `--port`/`-p`, `--log-dir`, `--log-level` (`trace|debug|info|warn|error|critical`, default `info`), and `--help`/`-h`.

```bash
./build/tools/kawasan-broker --config config/broker.dev.properties
./build/tools/kawasan-broker --config config/broker.macos.properties --log-level debug
```

### Install script (Linux)

`scripts/install.sh` installs a system-wide Linux deployment. It must run as root and expects the project already built (`build/tools/kawasan-broker` present).

```bash
sudo ./scripts/install.sh
```

It performs the following:

| Step | Detail |
|------|--------|
| User/group | Creates the `kawasan` system user and group |
| Binaries | Installs to `/usr/local/bin` |
| Config | `/etc/kawasan` |
| Data | `/var/lib/kawasan` |
| Logs | `/var/log/kawasan` |
| Service | Installs `systemd/kawasan-broker.service` to `/etc/systemd/system` (skipped with a warning if the unit file is absent) |

`scripts/install-systemd-service.sh` (Linux) and `scripts/install-macos-service.sh` (macOS) are the service-focused variants used in the [systemd](#linux-service-systemd) and [launchd](#macos-service-launchd) sections below.

### Docker

The repository ships a multi-stage `Dockerfile`. The image exposes the Kafka port `9092` and the Raft port `9093`, runs `kawasan-broker --config /etc/kawasan/server.properties`, and includes a `nc -z localhost 9092` health check.

```bash
docker build -t kawasan:latest .

docker run -d \
  -p 9092:9092 -p 9094:9094 \
  -v kawasan-data:/var/lib/kawasan/data \
  --name kawasan-broker \
  kawasan:latest
```

The image's baked-in `/etc/kawasan/server.properties` already points `log.dirs` at `/var/lib/kawasan/data`, matching the volume above. If you mount your own config over it, make sure its `log.dirs` is `/var/lib/kawasan/data` — and note that `config/broker.docker.properties` is **not** suitable for this (it configures a broker running on the host with clients in Docker: `log.dirs=/tmp/...`, `advertised.host=host.docker.internal`).

A single-broker `docker-compose.yml` is also provided (service `kawasan-broker-1`, additional brokers commented out as a starting point).

```bash
docker compose up -d
docker compose logs -f
docker compose down
```

### Docker Compose cluster

`docker-compose-cluster.yml` brings up a three-broker layout (`broker-0`, `broker-1`, `broker-2`), each mounting its own config from `config/broker-N.properties` and publishing host ports `9092/9192/9292` (Kafka) and `9093/9193/9293` (Raft).

```bash
docker compose -f docker-compose-cluster.yml up -d
docker compose -f docker-compose-cluster.yml logs -f broker-0
docker compose -f docker-compose-cluster.yml restart broker-0
docker compose -f docker-compose-cluster.yml down
```

> Multi-broker Raft is not production-hardened. Use the cluster compose file for local experimentation and compatibility testing, not for production traffic. See [Scaling](#scaling).

### Kubernetes

Two paths are supported: the Helm chart (recommended) and the raw manifests in `k8s/`.

**Helm** — chart at `helm/kawasan/`, documented in [../helm/kawasan/README.md](../helm/kawasan/README.md):

```bash
helm install kawasan ./helm/kawasan --namespace kawasan --create-namespace
helm upgrade kawasan ./helm/kawasan --namespace kawasan --values custom-values.yaml
helm rollback kawasan -n kawasan
helm uninstall kawasan -n kawasan
```

**Raw manifests** — the `k8s/` directory contains `kawasan-configmap.yaml`, `kawasan-service.yaml`, `kawasan-statefulset.yaml`, `kawasan-deployment.yaml`, and `kawasan-pvc.yaml`:

```bash
kubectl create namespace kawasan
kubectl apply -n kawasan -f k8s/kawasan-configmap.yaml
kubectl apply -n kawasan -f k8s/kawasan-service.yaml
kubectl apply -n kawasan -f k8s/kawasan-statefulset.yaml   # or kawasan-deployment.yaml for dev
```

Verify and reach the broker:

```bash
kubectl get pods,svc,pvc -n kawasan
kubectl logs -f -n kawasan kawasan-broker-0
kubectl port-forward -n kawasan svc/kawasan-broker 9092:9092
```

Common pod-level checks:

```bash
kubectl describe pod kawasan-broker-0 -n kawasan          # events, scheduling, probes
kubectl logs kawasan-broker-0 -n kawasan --previous       # logs from a crashed instance
kubectl top pods -n kawasan                               # resource usage
```

For per-parameter Helm values (replica count, persistence, resources, ServiceMonitor) see [../helm/kawasan/README.md](../helm/kawasan/README.md).

### macOS service (launchd)

`scripts/install-macos-service.sh` installs the broker as a launchd daemon using the `macos/com.kawasan.broker.plist` template.

```bash
sudo ./scripts/install-macos-service.sh
# Optional overrides:
sudo ./scripts/install-macos-service.sh \
  --broker-path ./build/tools/kawasan-broker \
  --config-path ./config/broker.macos.properties \
  --user kawasan
```

Layout created: binary at `/usr/local/bin/kawasan-broker`, config at `/usr/local/etc/kawasan/broker.properties`, data at `/usr/local/var/kawasan`, logs at `/usr/local/var/log/kawasan/{broker.log,broker-error.log}`, plist at `/Library/LaunchDaemons/com.kawasan.broker.plist`. The plist enables `RunAtLoad`, restart-on-crash via `KeepAlive` (crash only, not clean exit), a 30s graceful-shutdown timeout, and a 65536 file-descriptor soft/hard limit.

Lifecycle:

```bash
sudo launchctl load   /Library/LaunchDaemons/com.kawasan.broker.plist   # start
sudo launchctl unload /Library/LaunchDaemons/com.kawasan.broker.plist   # stop
sudo launchctl list | grep kawasan                                      # status
tail -f /usr/local/var/log/kawasan/broker.log                           # logs
plutil -lint /Library/LaunchDaemons/com.kawasan.broker.plist            # validate plist
```

Uninstall (`--keep-data` / `--keep-config` preserve state):

```bash
sudo ./scripts/uninstall-macos-service.sh
```

The macOS profile (`config/broker.macos.properties`) uses data dir `/usr/local/var/kawasan/data`, lower thread counts (4 network / 4 I/O), 512MB log segments, and replication factor 1 — appropriate for single-node development.

### Linux service (systemd)

`scripts/install-systemd-service.sh` installs and enables the systemd unit (`systemd/kawasan-broker.service`).

```bash
sudo ./scripts/install-systemd-service.sh            # or pass a build dir
sudo nano /etc/kawasan/broker.properties
sudo systemctl start kawasan-broker
```

The unit is configured with `Restart=on-failure`, `RestartSec=10s`, a restart burst limit, a 30s graceful-shutdown (`SIGTERM`) timeout, and `LimitNOFILE=100000`. Optional hardening directives (`NoNewPrivileges`, `PrivateTmp`, `ProtectSystem=strict`, `ProtectHome`, `ReadWritePaths`) and resource caps (`CPUQuota`, `MemoryMax`, `MemoryHigh`) can be enabled by editing the unit, then `sudo systemctl daemon-reload && sudo systemctl restart kawasan-broker`.

> Per-broker systemd template units (`kawasan-broker@N`) are **not** provided. Run one broker per host with the single `kawasan-broker` unit.

---

## Configuration essentials

The full key reference lives in [./CONFIGURATION.md](./CONFIGURATION.md). The operationally critical points:

**Both config formats are supported and auto-detected.** `src/common/config.cpp` dispatches on the first non-whitespace byte: a leading `{` or `[` is parsed as a **JSON object** (e.g. `config/broker.dev.properties`, `config/broker.docker.properties`); anything else is parsed as **Kafka-style `key=value` properties** (e.g. `config/broker-0.properties`). Despite the `.properties` extension, several shipped files are JSON — open the file to see which form it uses.

**Environment-variable substitution** applies to both formats: `${VAR}` expands to the variable's value (and errors if unset), and `${VAR:default}` falls back to `default` when unset. This is how the staging/production configs parameterize ports and toggles.

The handful of keys you adjust most often when operating a broker:

| Key | Purpose | Typical value |
|-----|---------|---------------|
| `broker.id` | Unique broker identity | `0` (single node) |
| `host` / `listeners` | Bind address | `0.0.0.0` |
| `port` | Kafka client port | `9092` |
| `advertised.host` | Host clients are told to use (the advertised port is always `port`) | matches reachable address (see [Kafka UI](#connecting-kafka-ui-and-admin-tools)) |
| `log.dirs` | Data directory | `/var/lib/kawasan/data` |
| `log.retention.hours` / `log.retention.bytes` | Retention | `168` / `-1` |
| `network.io_threads` | Network concurrency | defaults to hardware concurrency; tune to cores |
| `monitoring.host` / `monitoring.port` | HTTP metrics + health server (always runs; `monitoring.enabled` is accepted but not honored) | see [Ports](#ports) |

**Durability behavior**: message/log writes follow the `log.durability` broker config (default `sync` → fsync per acked produce, so an acked record survives power loss; `async` is WAL-buffered — lower latency but a machine crash can lose the un-flushed tail). Durability-critical internal topics (`__transaction_state`) force fsync regardless. Consumer offset commits are always synchronous (`sync=true`), batched across partitions into a single `WriteBatch`; consumer-group metadata uses async writes (`sync=false`) protected by the RocksDB WAL.

**Raft TLS is a known limitation.** The `raft.ssl.*` keys are parsed and validated at broker startup (`src/broker/kawasan_broker.cpp`), so an invalid combination throws at boot. However, the Raft transport (`src/raft/raft_transport.{cpp,h}`) contains no SSL code, so **inter-broker Raft traffic is plaintext even when `raft.ssl.enabled=true`**. Do not rely on Raft TLS for confidentiality; isolate inter-broker traffic at the network layer instead.

---

## Running and lifecycle management

### systemd (Linux)

```bash
sudo systemctl start   kawasan-broker
sudo systemctl stop    kawasan-broker      # SIGTERM, up to 30s graceful
sudo systemctl restart kawasan-broker
sudo systemctl enable  kawasan-broker      # start on boot
sudo systemctl status  kawasan-broker
sudo systemctl is-active kawasan-broker
```

Logs via journald:

```bash
sudo journalctl -u kawasan-broker -f             # follow
sudo journalctl -u kawasan-broker -n 200 --no-pager
sudo journalctl -u kawasan-broker --since today
sudo journalctl -u kawasan-broker -p err         # errors only
```

Journald rotation is configured in `/etc/systemd/journald.conf` (`SystemMaxUse`, `SystemMaxFileSize`, `MaxRetentionSec`); apply with `sudo systemctl restart systemd-journald`.

### launchd (macOS)

See [macOS service](#macos-service-launchd). Restart = `unload` then `load`.

### Docker

```bash
docker start|stop|restart kawasan-broker
docker logs -f kawasan-broker
```

### Manual / development

```bash
./build/tools/kawasan-broker --config config/broker.dev.properties
./build/tools/kawasan-broker --config config/broker.dev.properties --log-level debug
```

Logs are structured (spdlog) and written to stdout and the configured log directory; control verbosity with `--log-level` (`trace|debug|info|warn|error|critical`).

### Verifying the broker is up

```bash
nc -zv localhost 9092
curl -s http://localhost:9094/health
./build/tools/kawasan-topics --bootstrap-server localhost:9092 --list
```

---

## Monitoring

The broker runs an embedded HTTP server (Boost.Beast) that serves Prometheus metrics and health endpoints. It is controlled by `monitoring.host` and `monitoring.port` and always runs (`monitoring.enabled` appears in shipped configs but is not honored).

### Ports

| Port | Service | Default in |
|------|---------|-----------|
| `9092` | Kafka client protocol | all configs |
| `9093` | Raft inter-broker (multi-broker only) | cluster configs |
| `9094` | HTTP monitoring (metrics + health) | dev / docker / `broker-N.properties` |
| `8080` | HTTP monitoring (metrics + health) | staging / production, via `${KAWASAN_MONITORING_PORT:8080}` |

The **default monitoring port is 9094** in the development, Docker, and numbered-broker configs. The **staging and production configs override it to 8080** through `"monitoring.port": "${KAWASAN_MONITORING_PORT:8080}"`. Accordingly, `monitoring/prometheus.yml` scrapes `:8080`. Use whichever port your active profile binds — substitute it for `9094` in the examples below if you run a staging/production config.

### Health endpoints

```bash
curl http://localhost:9094/health      # alias: /healthz  → overall status
curl http://localhost:9094/ready       # alias: /readiness → ready to serve
curl http://localhost:9094/live        # alias: /liveness  → process alive
```

Example response:

```json
{ "status": "UP", "healthy": true, "ready": true }
```

These map directly to Kubernetes liveness/readiness probes (use `/live` and `/ready`).

### Metrics catalog

```bash
curl http://localhost:9094/metrics
```

| Metric | Type | Description |
|--------|------|-------------|
| `kawasan_broker_uptime_seconds` | Gauge | Broker uptime |
| `kawasan_active_connections` | Gauge | Current active client connections |
| `kawasan_topics` | Gauge | Number of topics |
| `kawasan_partitions` | Gauge | Number of partitions |
| `kawasan_consumer_groups` | Gauge | Number of consumer groups |
| `kawasan_messages_produced_total` | Counter | Total messages produced |
| `kawasan_messages_consumed_total` | Counter | Total messages consumed |
| `kawasan_bytes_in_total` | Counter | Total bytes received |
| `kawasan_bytes_out_total` | Counter | Total bytes sent |
| `kawasan_requests_total` | Counter | Total requests, by API |
| `kawasan_request_errors_total` | Counter | Total request errors, by API |
| `kawasan_produce_latency_ms` | Histogram | Produce latency (compute P50/P95/P99 with `histogram_quantile`) |
| `kawasan_fetch_latency_ms` | Histogram | Fetch latency (compute quantiles downstream) |
| `kawasan_request_latency_ms` | Histogram | Per-API request latency |
| `kawasan_connections_created_total` | Counter | Client connections opened |
| `kawasan_connections_closed_total` | Counter | Client connections closed, labeled `{reason}` |
| `kawasan_disk_usage_bytes` | Gauge | Disk usage |
| `kawasan_memory_usage_bytes` | Gauge | Memory usage |
| `kawasan_consumer_lag` | Gauge | Messages behind, labeled `{group,topic,partition}` |

The authoritative list is the collector itself (`src/broker/monitoring/metrics_collector.cpp`) — check `/metrics` on a running broker for the full set.

### Prometheus and Grafana

A complete stack lives in [`monitoring/`](../monitoring/), which contains `prometheus.yml`, `alerts.yml`, `alertmanager.yml`, `grafana-datasource.yml`, `grafana-dashboard.json`, and `docker-compose.monitoring.yml`.

Bring the stack up (Prometheus `:9090`, Grafana `:3000` admin/admin, Alertmanager `:9093`):

```bash
cd monitoring
docker compose -f docker-compose.monitoring.yml up -d
```

Point Prometheus at your broker(s) in `monitoring/prometheus.yml` — adjust the port to match your profile (`8080` for staging/prod, `9094` for dev/docker):

```yaml
scrape_configs:
  - job_name: 'kawasan-broker'
    static_configs:
      - targets: ['localhost:8080']
      # cluster: ['broker-0:8080', 'broker-1:8080', 'broker-2:8080']
```

Import the Grafana dashboard: **Dashboards → Import →** upload `monitoring/grafana-dashboard.json`, select the Prometheus data source. It visualizes status/uptime, throughput, P50/P95/P99 latency, requests and errors by API, connections, consumer groups, and disk/memory.

Alerting rules in `monitoring/alerts.yml` cover broker-down, high P99 latency, disk-space thresholds, high memory, high error rate, and connection anomalies. Wire notifications (Slack/email/PagerDuty) in `monitoring/alertmanager.yml`.

On Kubernetes, the Helm chart can emit a `ServiceMonitor` for the Prometheus Operator (`metrics.serviceMonitor.enabled=true`); see [../helm/kawasan/README.md](../helm/kawasan/README.md).

---

## Backup and restore

Back up two locations: the data directory (log segments + RocksDB state) and the config directory.

```
/var/lib/kawasan/data/    # log segments, indexes, RocksDB
/etc/kawasan/             # configuration
```

### Snapshot backup (offline, simplest)

```bash
sudo systemctl stop kawasan-broker
sudo tar -czf kawasan-backup-$(date +%Y%m%d).tar.gz /var/lib/kawasan/data /etc/kawasan
sudo systemctl start kawasan-broker
```

### Online copy (approximate)

RocksDB supports consistent online copies. The simplest operational form copies the data directory while the broker runs; for a transactionally consistent snapshot, prefer a filesystem/volume snapshot (below) over a plain `cp` of a live directory.

```bash
cp -r /var/lib/kawasan/data /backup/kawasan-checkpoint-$(date +%Y%m%d)
```

### Volume snapshots (cloud / Kubernetes)

```bash
# AWS EBS
aws ec2 create-snapshot --volume-id vol-xxxx \
  --description "kawasan $(date +%Y%m%d)"

# Docker volume → tarball
docker run --rm -v kawasan-data:/data -v "$PWD:/backup" \
  ubuntu tar czf /backup/kawasan-data-backup.tar.gz /data
```

On Kubernetes use a `VolumeSnapshot` against the broker's PVC (`data-kawasan-broker-0`); restore by creating a new PVC with that snapshot as `dataSource`. See the manifests in [`k8s/`](../k8s/).

### Restore (tar + chown flow)

```bash
sudo systemctl stop kawasan-broker
sudo rm -rf /var/lib/kawasan/data/*
sudo tar -xzf kawasan-backup-YYYYMMDD.tar.gz -C /
sudo chown -R kawasan:kawasan /var/lib/kawasan/data
sudo systemctl start kawasan-broker
```

From a checkpoint copy:

```bash
sudo systemctl stop kawasan-broker
sudo rm -rf /var/lib/kawasan/data
sudo cp -r /backup/kawasan-checkpoint-YYYYMMDD /var/lib/kawasan/data
sudo chown -R kawasan:kawasan /var/lib/kawasan/data
sudo systemctl start kawasan-broker
```

The `chown` step is mandatory after any restore — extracted or copied files are owned by `root` and the broker runs as the unprivileged `kawasan` user.

**Practices:** schedule via cron/systemd timers, keep a tiered retention (e.g. 7 daily / 4 weekly / 12 monthly), store copies off-host, periodically rehearse a restore, and encrypt backups containing sensitive payloads.

---

## Scaling

**Single-node is the primary, supported topology.** Scale a single broker vertically first; treat multi-broker as experimental.

### Vertical scaling

| Resource | Action |
|----------|--------|
| CPU | Add cores; raise `num.io.threads` and `num.network.threads` |
| Memory | Add RAM for OS page cache / RocksDB block cache |
| Disk | Faster NVMe, more capacity; larger `log.segment.bytes` |

Example for a 16-core host:

```properties
num.io.threads=16
num.network.threads=8
log.segment.bytes=2147483648
```

On systemd, raise the caps to match (`LimitNOFILE`, `MemoryMax`, `CPUQuota`). On Kubernetes, bump resource requests/limits via Helm (`resources.*`) or `kubectl scale`.

### Partition parallelism

Consumer parallelism is bounded by partition count. Create topics with enough partitions up front:

```bash
./build/tools/kawasan-topics --bootstrap-server localhost:9092 \
  --create --topic high-throughput --partitions 12 --replication-factor 1
```

Guidance: more partitions = more parallelism but more per-partition overhead; do not over-provision (avoid thousands of partitions on one broker).

### Horizontal scaling (experimental)

Multi-broker Raft replication is **not production-hardened**. Followers replicate data, the ISR shrinks and expands automatically, and `acks=all` waits on the ISR. When a partition leader stops answering Raft heartbeats for `broker.liveness.timeout.ms` (default 9 s), the controller automatically elects a new leader from the in-sync replicas. A partition with no live in-sync replica stays offline (`LEADER_NOT_AVAILABLE`) until one returns, unless `unclean.leader.election.enable=true`. A broker that can't confirm its metadata is current (it can't reach the Raft quorum or leader within half the liveness timeout, or it has just restarted and not caught up yet) refuses Produce/Fetch with `NOT_LEADER_FOR_PARTITION` and returns 503 from `/ready`. A broker that stays not-ready therefore usually can't reach its Raft peers on `raft.port`. Consumer-group coordinators are not yet routed cluster-wide. The `docker-compose-cluster.yml` and `k8s` StatefulSet exist for experimentation and compatibility testing. If you run a multi-broker cluster:

- Every broker needs a unique `broker.id`.
- Configure `raft.port` and `raft.peers` (`id:host:port,...`) consistently across brokers.
- Use an odd broker count (3, 5) so Raft can form a majority.
- Remember inter-broker Raft traffic is **plaintext** regardless of `raft.ssl.*` (see [Configuration essentials](#configuration-essentials)) — isolate it at the network layer.

For production-grade durability today, run a single broker with synchronous writes (default) plus disciplined backups rather than relying on replication.

---

## Troubleshooting playbook

### Broker won't start

```bash
sudo systemctl status kawasan-broker
sudo journalctl -u kawasan-broker -n 200 --no-pager      # read the actual error
```

Most common causes and checks:

```bash
# Port already in use
sudo lsof -i :9092
sudo netstat -tulpn | grep 9092        # Linux; macOS: lsof -i :9092

# Data directory permissions
ls -la /var/lib/kawasan/data
sudo chown -R kawasan:kawasan /var/lib/kawasan

# Config error (invalid JSON, bad ${VAR} with no value, invalid raft.ssl.* combo)
#   → the broker logs the offending key/line and exits; fix and restart.

# Binary present and executable
ls -la /usr/local/bin/kawasan-broker
```

If a port conflict is the broker's own stale process, stop it cleanly (`systemctl stop` / `launchctl unload`) rather than `kill -9` to allow a graceful flush.

### Connection refused

```bash
sudo systemctl status kawasan-broker        # is it running?
nc -zv localhost 9092                        # is the port reachable?
```

If the broker is up but clients still fail, the cause is almost always the **advertised address** — the metadata response points clients at an address they can't reach. See [Connecting Kafka UI and admin tools](#connecting-kafka-ui-and-admin-tools). Also check firewall rules:

```bash
sudo iptables -L -n                          # iptables
sudo firewall-cmd --list-all                 # firewalld
sudo ufw status                              # ufw
```

### High CPU

```bash
top -p $(pgrep kawasan-broker)
top -H -p $(pgrep kawasan-broker)            # per-thread (Linux)
curl -s http://localhost:9094/metrics | grep latency
curl -s http://localhost:9094/metrics | grep active_connections
```

Mitigations: raise `num.network.threads` for connection-bound load; check whether RocksDB compaction is running hot; profile to find the hot path (see [Profiling](#profiling)). If the connection count is very high, rate-limit producers or add capacity.

### High memory / OOM

```bash
free -h
ps aux | grep kawasan-broker
sudo dmesg | grep -i "killed process"        # OOM killer
sudo journalctl -k | grep -i "out of memory"
watch -n 5 'ps aux | grep [k]awasan-broker'  # growing RSS ⇒ investigate
```

Immediate mitigation: restart to reclaim, then cap memory in the service unit (`MemoryMax`, `MemoryHigh`) so the broker is throttled before the host OOM-kills it. Sustained, unbounded growth under steady load should be captured (build with `-DKAWASAN_ENABLE_ASAN=ON` in a non-prod environment) and reported.

### Slow consumer / growing lag

```bash
curl -s http://localhost:9094/metrics | grep consumer_lag
watch -n 5 'curl -s http://localhost:9094/metrics | grep consumer_lag'
./build/tools/kawasan-groups --bootstrap-server localhost:9092 --describe <group>
./build/tools/kawasan-groups --bootstrap-server localhost:9092 --list
```

Lag growing faster than it drains means consumers can't keep up with production. Options: add consumer instances to the group (parallelism is capped by partition count), create higher-partition topics, increase client `max.poll.records` / `fetch.min.bytes`, or commit offsets less frequently. If lag is bounded and draining, no action is needed.

### Disk full

```bash
df -h /var/lib/kawasan
du -ah /var/lib/kawasan/data | sort -rh | head -20
```

Fastest recovery is to reduce retention so cleanup reclaims space on the next pass:

```bash
sudo systemctl stop kawasan-broker
# edit config: log.retention.hours=24  and/or  log.retention.bytes=10737418240
sudo systemctl start kawasan-broker
sudo journalctl -u kawasan-broker -f | grep -i cleanup
```

Or move the data dir to a larger volume and repoint `log.dirs` (stop broker, `rsync` data, update config, `chown`, start). Never hand-delete the active (newest) segment of a partition.

### RocksDB inspection

```bash
ldb --db=/var/lib/kawasan/data/meta dump_live_files
ldb --db=/var/lib/kawasan/data/meta compact
```

### Cluster metadata consistency (multi-broker)

```bash
./build/tools/kawasan-metadata-check --brokers localhost:9092 localhost:9192 localhost:9292
# Reports "✓ All brokers have consistent metadata" when aligned.
```

---

## Incident severity

| Sev | Definition | Examples | First move |
|-----|------------|----------|-----------|
| **P1** | Service down or data at risk | Broker down, disk full blocking writes, OOM crash loop, suspected data corruption | Page on-call; restore service before root-causing |
| **P2** | Major degradation, no outage | Sustained consumer lag, network instability, client connection failures | Mitigate (scale/restart), then diagnose |
| **P3** | Minor / contained degradation | Sustained high CPU without latency breach, single noisy client | Investigate during business hours |
| **P4** | Cosmetic / informational | Log noise, transient blips that self-resolve | Track in backlog |

General flow for any incident: **detect** (alert / health failure) → **assess** severity and impact → **mitigate** to restore service (the playbook above) → **verify** (`systemctl status`, `/health`, a produce/consume round-trip) → **review** root cause and capture preventive follow-ups. When restoring, confirm recovery with an end-to-end check, not just process liveness:

```bash
curl -s http://localhost:9094/health
./build/tools/kawasan-topics --bootstrap-server localhost:9092 --list
```

---

## Performance tuning

### Targets

| Dimension | Target |
|-----------|--------|
| Throughput | 100k+ msg/sec (single broker, ~10 partitions, 100-byte messages) |
| Latency | p99 < 5ms for produce |
| Memory | < 1GB idle, stable under load |

Approximate single-node baseline (1-partition smoke test, 100-byte messages): ~30k msg/sec produce, ~18k msg/sec consume. Treat this as a rough baseline, not a guaranteed number — it varies with hardware, partition count, batch size, and compression. The throughput and replication benchmarks live under `tests/benchmark/` (built when `KAWASAN_BUILD_TESTS=ON`):

```bash
./build/tests/benchmark/throughput_benchmark
```

### OS tuning

**File descriptors** — `/etc/security/limits.conf` (Linux):

```
kawasan soft nofile 100000
kawasan hard nofile 100000
```

The systemd unit also sets `LimitNOFILE=100000`; verify with `sudo systemctl show kawasan-broker | grep LimitNOFILE`. On macOS, raise the launchd limit (`sudo launchctl limit maxfiles 65536 200000`); the broker plist already requests 65536.

**Kernel parameters** — `/etc/sysctl.d/99-kawasan.conf` (Linux), then `sudo sysctl -p /etc/sysctl.d/99-kawasan.conf`:

```ini
# Network
net.core.somaxconn = 1024
net.ipv4.tcp_max_syn_backlog = 4096
net.core.rmem_max = 134217728
net.core.wmem_max = 134217728
net.ipv4.tcp_rmem = 4096 87380 134217728
net.ipv4.tcp_wmem = 4096 65536 134217728

# Memory
vm.swappiness = 1
vm.dirty_ratio = 80
vm.dirty_background_ratio = 5

# File descriptors
fs.file-max = 2097152
```

**Disk** — use SSD/NVMe for `log.dirs`; mount with `noatime,nodiratime`. For SSDs prefer the `none`/`noop` I/O scheduler.

**Broker knobs** — start from a balanced profile and adjust toward throughput (more threads, larger segments, `lz4`/`snappy` compression) or latency (fewer threads, smaller segments, no compression). See [./CONFIGURATION.md](./CONFIGURATION.md) for the full set.

### Profiling

**Linux (`perf`):**

```bash
sudo perf record -g -F 999 -p $(pgrep kawasan-broker) -- sleep 60
sudo perf report
```

**macOS (Instruments):**

```bash
instruments -t "Time Profiler" -D profile.trace -l 60000 \
  ./build/tests/benchmark/throughput_benchmark
open profile.trace
# Or attach to a running broker:
sudo instruments -t "Time Profiler" -p $(pgrep kawasan-broker) -l 60000
```

Hot paths to expect: record-batch encode/decode, RocksDB read/write and compaction, and network I/O. Capacity planning for storage: `messages/day × message_size × retention_days × replication_factor`, plus ~20–30% for indexes and overhead.

---

## Connecting Kafka UI and admin tools

Kafka clients (Kafka UI, `kcat`, console tools, language clients) discover brokers via the Metadata API: they connect to the bootstrap address, fetch metadata, then connect directly to the **advertised** address each broker reports. If the advertised address isn't reachable from the client, connections fail with "Connection refused" or "Timed out waiting for a node assignment" — even though the broker is up. Set `advertised.host`/`advertised.port` to an address the client can actually reach.

| Scenario | `host` | `advertised.host` |
|----------|--------|-------------------|
| Everything on the host | `localhost` | (omit; defaults to `host`) |
| UI in Docker, broker on host | `0.0.0.0` | `host.docker.internal` |
| Both in Docker (same network) | `0.0.0.0` | the broker's compose service name (e.g. `kawasan-broker`) |

The `kafka-ui` profile (implemented by `scripts/quick-start.sh`) generates a UI-ready config without hand-editing files. It sets `host=0.0.0.0`, `advertised.host=host.docker.internal` (override with `KAWASAN_ADVERTISED_HOST`), and matches `advertised.port` to the listener port (override with `KAWASAN_ADVERTISED_PORT`). `scripts/reset.sh` is a wrapper that **kills the broker and deletes all `/tmp/kawasan-*` data** before re-running quick-start:

```bash
KAWASAN_PROFILE=kafka-ui bash scripts/reset.sh
# or with overrides:
KAWASAN_PROFILE=kafka-ui \
KAWASAN_ADVERTISED_HOST=host.docker.internal \
bash scripts/reset.sh
```

Then start Kafka UI (the root `docker-compose-kafka-ui.yml` runs `provectuslabs/kafka-ui` on `:8088`, bootstrapping `host.docker.internal:9092` with `DYNAMIC_CONFIG_ENABLED=true`):

```bash
docker compose -f docker-compose-kafka-ui.yml up -d
# UI at http://localhost:8088
```

Verify the broker advertises the address you expect:

```bash
python3 scripts/debug_metadata.py localhost 9092
# Broker 0: id=0, host=host.docker.internal, port=9092, rack=None
```

If the UI still shows the cluster offline: check `docker logs kafka-ui`, confirm the advertised host is reachable from inside the UI container (`docker exec kafka-ui nc -zv host.docker.internal 9092`), and that port 9092 is open. Full background on the advertised-address model is covered in the Kafka UI section above.

---

## Running the test suites

### Ecosystem compatibility harness (dual-broker oracle)

`tests/ecosystem/` is the contract for "drop-in replacement for single-server Kafka". Every smoke test runs against **both** brokers — **Apache Kafka 4.2.0** (`apache/kafka:4.2.0`) as the behavioral oracle and **Kawasan** as the candidate — so the diff between PASS-on-Kafka and PASS-on-Kawasan answers "real bug or test bug?". It requires a working Docker host.

```bash
cd tests/ecosystem
./scripts/run_all.sh kafka              # oracle baseline (expected all-PASS)
./scripts/run_all.sh kawasan            # candidate (some checks may still be RED)
./scripts/run_all.sh kawasan --keep-up  # leave the stack running for debugging
```

`run_all.sh` selects the compose file (`docker-compose.kafka.yml` vs `docker-compose.kawasan.yml`), brings the broker up, runs the numbered smoke scripts in `scripts/` (`01_apicompat.sh` … `21_streams_eos.sh`, covering producer/idempotent/transactional paths, admin/ACLs, compaction, internal topics, cooperative-sticky rebalance, `kcat`, schema registry, Kafka UI, Kafka Connect, ksqlDB, durability, SASL/PLAIN, read-committed, and Streams EOS), and tears down. Exit `0` = all passed, `1` = at least one failed, `2` = usage error. The harness is allowed-to-fail today because the broker has known gaps; treat its status as a regression signal across changes.

### Per-language client compatibility

`tests/compatibility/` checks individual Kafka client libraries against a running Kawasan broker. The runner builds/starts a broker, runs each available test, and cleans up:

```bash
./scripts/run_compatibility_tests.sh
```

| Client | File | Status |
|--------|------|--------|
| Python (`kafka-python`) | `kafka_python_test.py` | Implemented (`pip3 install kafka-python`) |
| Java | `JavaClientTest.java` | Placeholder |
| Node.js (KafkaJS) | `kafkajs_test.js` | Placeholder |
| Go (Sarama) | `sarama_test.go` | Placeholder |

Each client test exercises connect → topic create → produce (100+) → consume/verify → offset commit → consumer-group behavior.

### Unit and integration tests

The C++ unit/integration suites are run via CTest (see [../README.md](../README.md) and [../CLAUDE.md](../CLAUDE.md)):

```bash
ctest --test-dir build --output-on-failure
ctest --test-dir build -R KawasanBrokerErrorTest --output-on-failure
```
