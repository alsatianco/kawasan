# Kawasan Monitoring Setup

This directory contains configuration files for monitoring Kawasan broker using Prometheus and Grafana.

## Overview

The monitoring stack consists of:

- **Health Checks**: HTTP endpoints for liveness and readiness probes
- **Metrics Collection**: Prometheus-compatible metrics endpoint
- **Alerting**: Prometheus alerting rules for critical conditions
- **Visualization**: Grafana dashboard for monitoring broker performance

## Quick Start with Docker Compose

The easiest way to set up the monitoring stack is using Docker Compose:

```bash
# Start Prometheus and Grafana
docker-compose -f docker-compose.monitoring.yml up -d

# Access Grafana at http://localhost:3000
# Default credentials: admin/admin

# Access Prometheus at http://localhost:9090
```

## Health Check Endpoints

The broker exposes the following health check endpoints on port 8080:

- `GET /health` - Overall health status (JSON)
- `GET /healthz` - Alias for /health
- `GET /ready` - Readiness check (JSON)
- `GET /readiness` - Alias for /ready
- `GET /live` - Liveness check (JSON)
- `GET /liveness` - Alias for /live

### Example Response

```bash
curl http://localhost:8080/health
```

```json
{
  "status": "UP",
  "healthy": true,
  "ready": true
}
```

## Metrics Endpoint

Prometheus metrics are exposed at:

- `GET /metrics` - Prometheus text format metrics

### Example Metrics

```bash
curl http://localhost:8080/metrics
```

### Key Metrics

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
| `kawasan_requests_total` | Counter | Total requests by API |
| `kawasan_request_errors_total` | Counter | Total request errors by API |
| `kawasan_produce_latency_ms` | Histogram | Produce request latency |
| `kawasan_fetch_latency_ms` | Histogram | Fetch request latency |

## Prometheus Setup

### Configuration

The `prometheus.yml` file configures Prometheus to scrape metrics from Kawasan brokers.

**For a single broker:**
```yaml
scrape_configs:
  - job_name: 'kawasan-broker'
    static_configs:
      - targets: ['localhost:8080']
```

**For multiple brokers:**
```yaml
scrape_configs:
  - job_name: 'kawasan-broker'
    static_configs:
      - targets: ['broker-1:8080', 'broker-2:8080', 'broker-3:8080']
```

### Alerting Rules

The `alerts.yml` file defines alerting rules for:

- **Broker availability**: Alert when broker is down
- **High latency**: Alert on P99 latency > 1000ms
- **Disk space**: Warning at 80GB, critical at 90GB
- **Memory usage**: Alert when memory > 6GB
- **Error rates**: Alert on high error rates
- **Connections**: Alert on too many or no connections

### Running Prometheus Standalone

```bash
# Download Prometheus
wget https://github.com/prometheus/prometheus/releases/download/v2.45.0/prometheus-2.45.0.darwin-amd64.tar.gz
tar xzf prometheus-2.45.0.darwin-amd64.tar.gz
cd prometheus-2.45.0.darwin-amd64

# Copy configuration
cp /path/to/kawasan/monitoring/prometheus.yml .
cp /path/to/kawasan/monitoring/alerts.yml .

# Start Prometheus
./prometheus --config.file=prometheus.yml
```

## Grafana Setup

### Import Dashboard

1. Access Grafana at `http://localhost:3000`
2. Go to **Dashboards** → **Import**
3. Upload `grafana-dashboard.json`
4. Select Prometheus data source
5. Click **Import**

### Dashboard Panels

The Grafana dashboard includes:

- **Broker Status**: Real-time broker health
- **Uptime**: Broker uptime tracking
- **Active Connections**: Current client connections
- **Topics & Partitions**: Cluster composition
- **Message Throughput**: Messages/sec produced and consumed
- **Network Throughput**: Bytes/sec in and out
- **Produce Latency**: P50, P95, P99 latencies
- **Fetch Latency**: P50, P95, P99 latencies
- **Requests by API**: Request rate per API type
- **Request Errors**: Error rate per API type
- **Disk Usage**: Storage utilization
- **Memory Usage**: Memory consumption
- **Consumer Groups**: Active consumer groups

### Running Grafana Standalone

```bash
# macOS with Homebrew
brew install grafana
brew services start grafana

# Access at http://localhost:3000
# Default credentials: admin/admin
```

## Kubernetes Monitoring

For Kubernetes deployments, the Helm chart includes built-in monitoring support:

```yaml
# values.yaml
monitoring:
  enabled: true
  serviceMonitor:
    enabled: true
    interval: 30s
```

This creates a ServiceMonitor for Prometheus Operator to automatically discover and scrape metrics.

## JSON Logging

Enable JSON-formatted logs for integration with centralized logging systems (ELK, Splunk, Loki):

```bash
./kawasan-broker --config server.properties --log-level info --log-format json
```

JSON log format:
```json
{"timestamp":"2025-11-18T10:30:45.123","level":"info","thread":"12345","message":"Broker started"}
```

## Best Practices

### Production Recommendations

1. **Metrics Retention**: Configure Prometheus with 30-90 days retention
2. **Alert Thresholds**: Adjust thresholds based on your workload
3. **Scrape Interval**: Use 15-30 second intervals for production
4. **Dashboard Refresh**: Set to 30s or 1m for live monitoring
5. **Resource Limits**: Set appropriate CPU/memory limits for Prometheus and Grafana

### Alertmanager Integration

Configure Alertmanager for notifications:

```yaml
# alertmanager.yml
global:
  slack_api_url: 'YOUR_SLACK_WEBHOOK_URL'

route:
  receiver: 'slack-notifications'
  group_by: ['alertname', 'instance']

receivers:
  - name: 'slack-notifications'
    slack_configs:
      - channel: '#alerts'
        text: '{{ range .Alerts }}{{ .Annotations.description }}{{ end }}'
```

### High Availability Monitoring

For production clusters:

1. Deploy Prometheus in HA mode with multiple replicas
2. Use Thanos or Cortex for long-term storage
3. Set up Grafana with multiple data sources
4. Configure alert deduplication in Alertmanager

## Troubleshooting

### Metrics not appearing

1. Check broker is running: `curl http://localhost:8080/health`
2. Verify metrics endpoint: `curl http://localhost:8080/metrics`
3. Check Prometheus targets: `http://localhost:9090/targets`
4. Verify network connectivity between Prometheus and broker

### High cardinality warnings

If you see high cardinality warnings in Prometheus:

1. Limit the number of API-specific metrics
2. Use recording rules to pre-aggregate metrics
3. Adjust Prometheus storage settings

### Dashboard not loading

1. Verify Prometheus data source is configured in Grafana
2. Check Prometheus is scraping metrics successfully
3. Verify time range in dashboard matches data availability

## References

- [Prometheus Documentation](https://prometheus.io/docs/)
- [Grafana Documentation](https://grafana.com/docs/)
- [Prometheus Alerting Best Practices](https://prometheus.io/docs/practices/alerting/)
- [Grafana Dashboard Best Practices](https://grafana.com/docs/grafana/latest/best-practices/)
