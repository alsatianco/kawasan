# Kawasan Helm Chart

This Helm chart deploys Kawasan message broker on Kubernetes.

## Installation

### Prerequisites

- Kubernetes 1.19+
- Helm 3.0+
- PersistentVolume provisioner support in the underlying infrastructure

### Add Helm Repository

```bash
helm repo add kawasan https://charts.kawasan.io
helm repo update
```

### Install Chart

```bash
# Install with default values
helm install kawasan kawasan/kawasan

# Install with custom values
helm install kawasan kawasan/kawasan -f values.yaml

# Install in a specific namespace
helm install kawasan kawasan/kawasan --namespace kawasan-system --create-namespace
```

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
| `deploymentType` | Deployment type (statefulset/deployment) | `statefulset` |
| `replicaCount` | Number of replicas | `3` |

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

All broker configuration parameters are available under the `config` section. See `values.yaml` for the complete list.

## Examples

### Install with 5 brokers and 200Gi storage

```bash
helm install kawasan kawasan/kawasan \
  --set replicaCount=5 \
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
# Upgrade with new values
helm upgrade kawasan kawasan/kawasan -f values.yaml

# Upgrade to a new version
helm upgrade kawasan kawasan/kawasan --version 1.1.0
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

### Prometheus Integration

The chart supports Prometheus monitoring through ServiceMonitor CRD:

```yaml
metrics:
  enabled: true
  serviceMonitor:
    enabled: true
    interval: 30s
```

## Security

The chart follows security best practices:

- Runs as non-root user (UID 1000)
- Read-only root filesystem
- Dropped all capabilities
- No privilege escalation

## High Availability

For production deployments:

1. Set `replicaCount` to at least 3
2. Enable `podDisruptionBudget`
3. Use `required` pod anti-affinity
4. Configure appropriate resource limits
5. Use fast storage (SSD) for persistence

```yaml
replicaCount: 3
podDisruptionBudget:
  enabled: true
  minAvailable: 2
affinity:
  podAntiAffinity:
    type: required
persistence:
  storageClass: fast-ssd
```

## Troubleshooting

### View broker logs

```bash
kubectl logs -l app=kawasan -f
```

### Check broker status

```bash
kubectl get pods -l app=kawasan
kubectl describe statefulset kawasan-broker
```

### Access broker shell

```bash
kubectl exec -it kawasan-broker-0 -- /bin/sh
```

## Support

For issues and questions, please visit:
- GitHub: https://github.com/yourusername/kawasan
- Documentation: https://docs.kawasan.io
