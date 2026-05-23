# Kawasan Configuration Guide

## Overview

This document provides comprehensive documentation for all Kawasan broker configuration options, environment variable support, and best practices for different deployment environments.

---

## Table of Contents

1. [Configuration Files](#configuration-files)
2. [Environment Variable Support](#environment-variable-support)
3. [Configuration Options Reference](#configuration-options-reference)
4. [Environment-Specific Configurations](#environment-specific-configurations)
5. [Validation Rules](#validation-rules)
6. [Security Best Practices](#security-best-practices)
7. [Performance Tuning](#performance-tuning)
8. [Examples](#examples)

---

## Configuration Files

Kawasan uses JSON-formatted configuration files with support for environment variable substitution.

### Available Configuration Templates

- **`config/broker.dev.properties`** - Development environment
  - Fast startup, minimal resources
  - Verbose logging for debugging
  - Short retention periods
  - No durability guarantees

- **`config/broker.staging.properties`** - Staging environment
  - Mirrors production topology
  - Moderate resource usage
  - Environment variable support
  - Suitable for testing

- **`config/broker.production.properties`** - Production environment
  - High durability and reliability
  - Optimized for performance
  - Comprehensive security settings
  - All secrets via environment variables

- **`config/server.properties.example`** - Basic example
  - Simple configuration for getting started
  - Minimal settings with defaults

---

## Environment Variable Support

Kawasan supports environment variable substitution in configuration files using the syntax:

```
${VARIABLE_NAME}           - Required variable (throws error if not set)
${VARIABLE_NAME:default}   - Optional variable with default value
```

### Examples

```json
{
  "broker.id": "${KAWASAN_BROKER_ID}",
  "host": "${KAWASAN_HOST:0.0.0.0}",
  "port": "${KAWASAN_PORT:9092}",
  "log.dirs": "${KAWASAN_LOG_DIRS:/var/lib/kawasan/data}"
}
```

In this example:
- `KAWASAN_BROKER_ID` is **required** (no default)
- `KAWASAN_HOST` defaults to `0.0.0.0` if not set
- `KAWASAN_PORT` defaults to `9092` if not set
- `KAWASAN_LOG_DIRS` defaults to `/var/lib/kawasan/data` if not set

### Setting Environment Variables

**Linux/macOS:**
```bash
export KAWASAN_BROKER_ID=1
export KAWASAN_ADVERTISED_HOST=broker1.example.com
export KAWASAN_SSL_KEYSTORE_PASSWORD=secret123
```

**Docker:**
```bash
docker run -e KAWASAN_BROKER_ID=1 \
           -e KAWASAN_ADVERTISED_HOST=broker1 \
           kawasan/broker
```

**Kubernetes:**
```yaml
env:
  - name: KAWASAN_BROKER_ID
    value: "1"
  - name: KAWASAN_SSL_KEYSTORE_PASSWORD
    valueFrom:
      secretKeyRef:
        name: kawasan-secrets
        key: keystore-password
```

---

## Configuration Options Reference

### Broker Identity

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `broker.id` | int | Yes | - | Unique identifier for this broker in the cluster |
| `host` | string | Yes | - | Host address to bind to (use `0.0.0.0` for all interfaces) |
| `advertised.host` | string | No | Same as `host` | Externally accessible hostname/IP for clients |
| `port` | int | Yes | 9092 | Port number for broker to listen on (1-65535) |

### Storage Configuration

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `log.dirs` | string | Yes | - | Directory for storing log segments (use SSD for production) |
| `log.segment.bytes` | long | No | 1073741824 | Maximum size of a single log segment file (1GB default) |
| `log.retention.hours` | long | No | 168 | Hours to retain log segments (168 = 7 days, -1 = unlimited) |
| `log.retention.bytes` | long | No | -1 | Maximum size of log before deletion (-1 = unlimited) |
| `log.retention.check.interval.ms` | long | No | 300000 | How often to check for log deletion (5 minutes) |
| `log.cleanup.policy` | string | No | delete | `delete` (time/size based) or `compact` (keep latest per key) |

### Network Settings

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `num.network.threads` | int | No | 8 | Number of threads for network I/O |
| `num.io.threads` | int | No | 8 | Number of threads for disk I/O |
| `socket.send.buffer.bytes` | int | No | 102400 | Socket send buffer size (100KB) |
| `socket.receive.buffer.bytes` | int | No | 102400 | Socket receive buffer size (100KB) |
| `socket.request.max.bytes` | int | No | 104857600 | Maximum request size (100MB) |

### Durability & Flush Settings

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `log.flush.interval.messages` | long | No | 10000 | Flush to disk after N messages |
| `log.flush.interval.ms` | long | No | 1000 | Flush to disk after N milliseconds |
| `log.flush.scheduler.interval.ms` | long | No | 60000 | Background flush check interval |

### Replication Settings

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `default.replication.factor` | int | No | 1 | Default replication factor for new topics |
| `min.insync.replicas` | int | No | 1 | Minimum replicas that must acknowledge writes |
| `offsets.topic.replication.factor` | int | No | 1 | Replication factor for offset topic |
| `transaction.state.log.replication.factor` | int | No | 1 | Replication factor for transaction log |
| `transaction.state.log.min.isr` | int | No | 1 | Min ISR for transaction log |
| `replica.lag.time.max.ms` | long | No | 30000 | Max lag before removing replica from ISR |
| `num.replica.fetchers` | int | No | 1 | Number of fetcher threads per broker |

### Recovery Settings

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `num.recovery.threads.per.data.dir` | int | No | 1 | Threads for log recovery and flushing |

### Compression

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `compression.type` | string | No | none | Compression type: `none`, `gzip`, `snappy`, `lz4`, `zstd` |

### Consumer Group Settings

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `group.initial.rebalance.delay.ms` | int | No | 0 | Delay before first rebalance (give consumers time to join) |
| `group.max.session.timeout.ms` | int | No | 300000 | Maximum session timeout (5 minutes) |
| `group.min.session.timeout.ms` | int | No | 6000 | Minimum session timeout (6 seconds) |

### Producer Settings

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `max.request.size` | int | No | 1048576 | Maximum size of a produce request (1MB) |

### Consumer Settings

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `fetch.min.bytes` | int | No | 1 | Minimum bytes to fetch in a request |
| `fetch.max.wait.ms` | int | No | 500 | Maximum time to wait for fetch |

### Partition Settings

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `num.partitions` | int | No | 1 | Default number of partitions for new topics |

### Metrics and Monitoring

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `metrics.recording.level` | string | No | INFO | Metrics detail level: `DEBUG`, `INFO`, `WARN`, `ERROR` |
| `monitoring.port` | int | No | 8080 | HTTP port for health checks and Prometheus metrics |
| `monitoring.enabled` | bool | No | true | Enable monitoring endpoints |

### Topic Management

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `auto.create.topics.enable` | bool | No | true | Auto-create topics on first produce/consume |
| `delete.topic.enable` | bool | No | true | Allow topic deletion |

### Logging

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `log.level` | string | No | info | Log level: `trace`, `debug`, `info`, `warn`, `error`, `critical` |
| `log.format` | string | No | text | Log format: `text` or `json` |

### Security Settings

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `security.protocol` | string | No | PLAINTEXT | Security protocol: `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, `SASL_SSL` |

### TLS/SSL Configuration

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `ssl.enabled` | bool | No | false | Enable TLS encryption |
| `ssl.keystore.location` | string | Conditional | - | Path to keystore file (required if SSL enabled) |
| `ssl.keystore.password` | string | Conditional | - | Keystore password (required if SSL enabled) |
| `ssl.key.password` | string | Conditional | - | Private key password (required if SSL enabled) |
| `ssl.truststore.location` | string | No | - | Path to truststore file |
| `ssl.truststore.password` | string | Conditional | - | Truststore password (if truststore used) |
| `ssl.protocol` | string | No | TLSv1.3 | TLS protocol version |
| `ssl.cipher.suites` | string | No | - | Comma-separated cipher suites (empty = defaults) |
| `ssl.client.auth` | string | No | none | Client auth: `none`, `requested`, `required` |

### SASL Authentication

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `sasl.enabled` | bool | No | false | Enable SASL authentication |
| `sasl.mechanism` | string | No | PLAIN | SASL mechanism: `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`, `GSSAPI` |
| `sasl.jaas.config` | string | Conditional | - | JAAS configuration (required if SASL enabled) |

### Performance Tuning

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `background.threads` | int | No | 10 | Number of background threads |
| `log.message.timestamp.type` | string | No | CreateTime | Timestamp type: `CreateTime`, `LogAppendTime` |

### Quota and Rate Limiting

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `quota.producer.default` | long | No | -1 | Producer quota (bytes/sec, -1 = unlimited) |
| `quota.consumer.default` | long | No | -1 | Consumer quota (bytes/sec, -1 = unlimited) |

### ZooKeeper Settings

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `zookeeper.connect` | string | No | - | ZooKeeper connection string (optional, Raft is default) |
| `zookeeper.connection.timeout.ms` | int | No | 18000 | ZooKeeper connection timeout |

---

## Environment-Specific Configurations

### Development Environment

**Use:** Local development, testing, debugging

**Key Characteristics:**
- Fast startup and restarts
- Verbose logging (debug level)
- Short retention (24 hours)
- No replication
- Minimal resource usage
- Data in `/tmp` (ephemeral)

**Configuration:** `config/broker.dev.properties`

```bash
./kawasan-broker --config config/broker.dev.properties
```

### Staging Environment

**Use:** Pre-production testing, integration testing

**Key Characteristics:**
- Mirrors production topology (3+ brokers)
- Moderate resource usage
- JSON logging for aggregation
- 72-hour retention
- Replication factor 3
- Environment variable support

**Configuration:** `config/broker.staging.properties`

```bash
export KAWASAN_BROKER_ID=1
export KAWASAN_ADVERTISED_HOST=staging-broker1.example.com
./kawasan-broker --config config/broker.staging.properties
```

### Production Environment

**Use:** Production deployments

**Key Characteristics:**
- High durability (min.insync.replicas=2)
- 7-day retention (configurable)
- TLS encryption enabled
- SASL authentication (optional)
- JSON logging for ELK/Splunk
- Prometheus metrics enabled
- All secrets from environment variables

**Configuration:** `config/broker.production.properties`

```bash
export KAWASAN_BROKER_ID=1
export KAWASAN_ADVERTISED_HOST=prod-broker1.example.com
export KAWASAN_SSL_KEYSTORE_PASSWORD=$(vault read -field=password secret/kawasan/ssl)
export KAWASAN_SSL_KEY_PASSWORD=$(vault read -field=keypassword secret/kawasan/ssl)
./kawasan-broker --config config/broker.production.properties
```

---

## Validation Rules

The broker performs automatic validation on startup. The following rules are enforced:

### Required Keys
- `broker.id` - Must be present
- `host` - Must be present
- `port` - Must be present
- `log.dirs` - Must be present

### Value Constraints
- `broker.id` ≥ 0
- `port` between 1 and 65535
- `num.network.threads` ≥ 1
- `num.io.threads` ≥ 1
- `log.segment.bytes` ≥ 1024
- `log.retention.hours` ≥ -1 (-1 means unlimited)
- `default.replication.factor` ≥ 1
- `min.insync.replicas` ≥ 1
- `min.insync.replicas` ≤ `default.replication.factor`
- `monitoring.port` between 1 and 65535
- `monitoring.port` ≠ `port` (must be different)
- `compression.type` must be one of: `none`, `gzip`, `snappy`, `lz4`, `zstd`

### Warnings
- `num.network.threads` > 1024 (may cause resource issues)

---

## Security Best Practices

### 1. Never Commit Secrets

❌ **Bad:**
```json
{
  "ssl.keystore.password": "mypassword123"
}
```

✅ **Good:**
```json
{
  "ssl.keystore.password": "${KAWASAN_SSL_KEYSTORE_PASSWORD}"
}
```

### 2. Use Secrets Managers

**HashiCorp Vault:**
```bash
export KAWASAN_SSL_KEYSTORE_PASSWORD=$(vault kv get -field=password secret/kawasan/ssl)
```

**AWS Secrets Manager:**
```bash
export KAWASAN_SSL_KEYSTORE_PASSWORD=$(aws secretsmanager get-secret-value \
  --secret-id kawasan/ssl/keystore --query SecretString --output text)
```

**Azure Key Vault:**
```bash
export KAWASAN_SSL_KEYSTORE_PASSWORD=$(az keyvault secret show \
  --name kawasan-ssl-keystore --vault-name mykeyvault --query value -o tsv)
```

### 3. Rotate Credentials Regularly

- SSL certificates: Every 90 days
- SASL passwords: Every 30 days
- Access keys: Every 90 days

### 4. Use Principle of Least Privilege

- Run broker as dedicated `kawasan` user (not root)
- Restrict file permissions: 600 for configs, 700 for data dirs
- Use network policies in Kubernetes
- Enable firewall rules

### 5. Enable TLS in Production

```json
{
  "security.protocol": "SSL",
  "ssl.enabled": true,
  "ssl.keystore.location": "/etc/kawasan/ssl/keystore.jks",
  "ssl.keystore.password": "${KAWASAN_SSL_KEYSTORE_PASSWORD}",
  "ssl.key.password": "${KAWASAN_SSL_KEY_PASSWORD}",
  "ssl.protocol": "TLSv1.3"
}
```

---

## Performance Tuning

### High Throughput Scenarios

**Increase network and I/O threads:**
```json
{
  "num.network.threads": 16,
  "num.io.threads": 16,
  "socket.send.buffer.bytes": 262144,
  "socket.receive.buffer.bytes": 262144
}
```

**Use efficient compression:**
```json
{
  "compression.type": "lz4"
}
```

**Increase partition count:**
```json
{
  "num.partitions": 12
}
```

### Low Latency Scenarios

**Reduce flush intervals:**
```json
{
  "log.flush.interval.messages": 1000,
  "log.flush.interval.ms": 100
}
```

**Optimize fetch settings:**
```json
{
  "fetch.min.bytes": 1,
  "fetch.max.wait.ms": 100
}
```

### High Durability Scenarios

**Increase replication:**
```json
{
  "default.replication.factor": 3,
  "min.insync.replicas": 2,
  "offsets.topic.replication.factor": 3,
  "transaction.state.log.replication.factor": 3,
  "transaction.state.log.min.isr": 2
}
```

**More aggressive flushing:**
```json
{
  "log.flush.interval.messages": 1000,
  "log.flush.interval.ms": 500
}
```

### Resource-Constrained Environments

**Reduce thread counts:**
```json
{
  "num.network.threads": 2,
  "num.io.threads": 2,
  "background.threads": 4
}
```

**Aggressive cleanup:**
```json
{
  "log.retention.hours": 24,
  "log.retention.bytes": 10737418240,
  "log.segment.bytes": 104857600
}
```

---

## Examples

### Example 1: Single Development Broker

```bash
# Start with development config
./kawasan-broker --config config/broker.dev.properties --log-level debug
```

### Example 2: Production Cluster (3 Brokers)

**Broker 1:**
```bash
export KAWASAN_BROKER_ID=1
export KAWASAN_ADVERTISED_HOST=broker1.prod.example.com
export KAWASAN_SSL_KEYSTORE_PASSWORD=$(vault read -field=password secret/kawasan/ssl)
export KAWASAN_SSL_KEY_PASSWORD=$(vault read -field=keypassword secret/kawasan/ssl)
./kawasan-broker --config config/broker.production.properties
```

**Broker 2:**
```bash
export KAWASAN_BROKER_ID=2
export KAWASAN_ADVERTISED_HOST=broker2.prod.example.com
# ... same SSL setup
./kawasan-broker --config config/broker.production.properties
```

**Broker 3:**
```bash
export KAWASAN_BROKER_ID=3
export KAWASAN_ADVERTISED_HOST=broker3.prod.example.com
# ... same SSL setup
./kawasan-broker --config config/broker.production.properties
```

### Example 3: Docker Deployment

```bash
docker run -d \
  --name kawasan-broker \
  -p 9092:9092 \
  -p 8080:8080 \
  -e KAWASAN_BROKER_ID=1 \
  -e KAWASAN_ADVERTISED_HOST=localhost \
  -e KAWASAN_LOG_DIRS=/var/lib/kawasan \
  -v kawasan-data:/var/lib/kawasan \
  -v $(pwd)/config/broker.production.properties:/etc/kawasan/broker.properties:ro \
  kawasan/broker \
  --config /etc/kawasan/broker.properties
```

### Example 4: Kubernetes StatefulSet

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: kawasan-config
data:
  broker.properties: |
    {
      "broker.id": "${KAWASAN_BROKER_ID}",
      "host": "0.0.0.0",
      "advertised.host": "${POD_NAME}.kawasan-headless.default.svc.cluster.local",
      "port": 9092,
      "log.dirs": "/var/lib/kawasan/data",
      "default.replication.factor": 3,
      "min.insync.replicas": 2
    }
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: kawasan
spec:
  serviceName: kawasan-headless
  replicas: 3
  selector:
    matchLabels:
      app: kawasan
  template:
    metadata:
      labels:
        app: kawasan
    spec:
      containers:
      - name: broker
        image: kawasan/broker:latest
        env:
        - name: POD_NAME
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        - name: KAWASAN_BROKER_ID
          value: "$(echo ${POD_NAME} | cut -d'-' -f2)"
        - name: KAWASAN_SSL_KEYSTORE_PASSWORD
          valueFrom:
            secretKeyRef:
              name: kawasan-ssl
              key: keystore-password
        volumeMounts:
        - name: config
          mountPath: /etc/kawasan
        - name: data
          mountPath: /var/lib/kawasan
      volumes:
      - name: config
        configMap:
          name: kawasan-config
  volumeClaimTemplates:
  - metadata:
      name: data
    spec:
      accessModes: ["ReadWriteOnce"]
      resources:
        requests:
          storage: 100Gi
```

---

## Troubleshooting

### Configuration File Not Found

```
Error: Failed to open configuration file: config/broker.properties
```

**Solution:** Ensure the file path is correct and the file exists.

### Environment Variable Not Set

```
Error in config key 'broker.id': Required environment variable not set: KAWASAN_BROKER_ID
```

**Solution:** Set the required environment variable:
```bash
export KAWASAN_BROKER_ID=1
```

### Validation Errors

```
Configuration validation failed:
  - port must be between 1 and 65535
  - min.insync.replicas cannot be greater than default.replication.factor
```

**Solution:** Fix the configuration values according to the validation rules.

### Port Already in Use

```
Error: Failed to bind to port 9092
```

**Solution:** Either:
1. Stop the process using the port
2. Change the `port` configuration
3. Use a different `monitoring.port`

---

## Additional Resources

- [Production Deployment Guide](PRODUCTION_DEPLOYMENT.md)
- [Monitoring Guide](../monitoring/README.md)
- [Kubernetes Deployment](../k8s/README.md)
- [Helm Chart Documentation](../helm/kawasan/README.md)
- [Architecture Documentation](ARCHITECTURE.md)

---

## Revision History

- **2025-11-18**: Initial configuration documentation for Phase 7
