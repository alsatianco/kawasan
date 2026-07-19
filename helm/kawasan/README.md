# Kawasan Helm Chart

This Helm chart deploys Kawasan message broker on Kubernetes.

## Installation

### Prerequisites

- Kubernetes 1.21+ (the PodDisruptionBudget template uses `policy/v1`)
- Helm 3.0+
- PersistentVolume provisioner support in the underlying infrastructure

### Install Chart

Install directly from this repository's chart directory:

```bash
# Install with default values
helm install kawasan ./helm/kawasan

# Install with custom values
helm install kawasan ./helm/kawasan -f values.yaml

# Install in a specific namespace
helm install kawasan ./helm/kawasan --namespace kawasan-system --create-namespace
```

The examples below use `kawasan/kawasan`; substitute `./helm/kawasan` when installing from a local checkout.

## Configuration

The following table lists the configurable parameters of the Kawasan chart and their default values.

### Image Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `image.repository` | Image repository | `kawasan/broker` |
| `image.tag` | Image tag | `latest` |
| `image.pullPolicy` | Image pull policy | `IfNotPresent` |

### Deployment Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `deploymentType` | Workload type — only `statefulset` is implemented (any other value renders no workload) | `statefulset` |
| `replicaCount` | Number of replicas — keep at `1`; see High Availability below | `1` |

### Resource Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `resources.requests.cpu` | CPU request | `1000m` |
| `resources.requests.memory` | Memory request | `2Gi` |
| `resources.limits.cpu` | CPU limit | `4000m` |
| `resources.limits.memory` | Memory limit | `8Gi` |

### Persistence Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `persistence.enabled` | Enable persistence | `true` |
| `persistence.size` | Persistent volume size | `100Gi` |
| `persistence.storageClass` | Storage class | `""` (default) |
| `persistence.accessMode` | Access mode | `ReadWriteOnce` |

### Broker Configuration

A **subset** of broker configuration is exposed under the `config` section — see `templates/configmap.yaml` for exactly which keys render (roughly: identity, network, log dirs/retention, topic defaults). Other broker keys (e.g. `monitoring.port`, `raft.*`, `log.durability`, security) are not currently settable through the chart.

## Examples

### Install with 200Gi storage

```bash
helm install kawasan kawasan/kawasan \
  --set persistence.size=200Gi
```

### Install with specific storage class

```bash
helm install kawasan kawasan/kawasan \
  --set persistence.storageClass=fast-ssd
```

### Install with custom resource limits

```bash
helm install kawasan kawasan/kawasan \
  --set resources.limits.cpu=8000m \
  --set resources.limits.memory=16Gi
```

## Upgrading

```bash
# Upgrade with new values (use ./helm/kawasan when installing from a local checkout)
helm upgrade kawasan kawasan/kawasan -f values.yaml
```

## Uninstalling

```bash
# Uninstall the release
helm uninstall kawasan

# Uninstall and delete PVCs (WARNING: This will delete all data)
helm uninstall kawasan
kubectl delete pvc -l app.kubernetes.io/name=kawasan
```

## Monitoring

The broker serves Prometheus metrics and health probes on its monitoring HTTP port (default **9094** — see [`docs/OPERATIONS.md`](../../docs/OPERATIONS.md)). **Known gaps in the current chart:** it ships no ServiceMonitor template (the `metrics.serviceMonitor.*` values render nothing), its `metrics.port: 9308` does not match the broker's 9094, and the `prometheus.io/port` pod annotation points at the Kafka port. Until these are fixed, scrape the pods directly on port 9094.

## Security

The chart follows security best practices:

- Runs as non-root user (UID 1000)
- Read-only root filesystem
- Dropped all capabilities
- No privilege escalation

## High Availability

**Run a single replica.** Raising `replicaCount` does **not** form a cluster: the chart's ConfigMap generates no `raft.peers`, so N replicas are N independent single-node brokers behind one Service — clients get routed to random brokers with disjoint data. Additionally, multi-broker Kawasan has no automatic partition-leader failover yet (see the repository README), so multi-replica deployments are doubly unsupported. For production: `replicaCount: 1`, fast (SSD) storage, and appropriate resource limits.

## Troubleshooting

### View broker logs

```bash
kubectl logs -l app=kawasan -f
```

### Check broker status

```bash
kubectl get pods -l app=kawasan
kubectl describe statefulset kawasan     # for release name "kawasan"; otherwise <release>-kawasan
```

### Access broker shell

```bash
kubectl exec -it kawasan-0 -- /bin/sh
```

## Support

For issues and questions, see [`docs/`](../../docs/) in this repository.
