# Kubernetes Deployment Guide for Kawasan

This guide provides instructions for deploying Kawasan message broker on Kubernetes.

## Table of Contents

- [Prerequisites](#prerequisites)
- [Deployment Options](#deployment-options)
- [Quick Start with kubectl](#quick-start-with-kubectl)
- [Helm Deployment](#helm-deployment)
- [Configuration](#configuration)
- [Production Deployment](#production-deployment)
- [Monitoring](#monitoring)
- [Scaling](#scaling)
- [Backup and Recovery](#backup-and-recovery)
- [Troubleshooting](#troubleshooting)

---

## Prerequisites

- **Kubernetes Cluster**: Version 1.19 or higher
- **kubectl**: Configured to access your cluster
- **Helm**: Version 3.0 or higher (for Helm deployments)
- **Persistent Storage**: Storage class with dynamic provisioning
- **Resources**: Sufficient cluster resources (CPU, memory, storage)

### Verify Prerequisites

```bash
# Check Kubernetes version
kubectl version

# Check available storage classes
kubectl get storageclass

# Check cluster resources
kubectl top nodes
```

---

## Deployment Options

Kawasan can be deployed on Kubernetes in two ways:

1. **Using kubectl with raw manifests** (k8s/ directory)
2. **Using Helm chart** (helm/kawasan/ directory) - Recommended

---

## Quick Start with kubectl

### 1. Deploy with Basic Configuration

```bash
# Create namespace
kubectl create namespace kawasan

# Deploy ConfigMap
kubectl apply -f k8s/kawasan-configmap.yaml -n kawasan

# Deploy Service
kubectl apply -f k8s/kawasan-service.yaml -n kawasan

# Deploy StatefulSet (for production)
kubectl apply -f k8s/kawasan-statefulset.yaml -n kawasan

# Or deploy Deployment (for development)
# kubectl apply -f k8s/kawasan-deployment.yaml -n kawasan
```

### 2. Verify Deployment

```bash
# Check pods
kubectl get pods -n kawasan -l app=kawasan

# Check services
kubectl get svc -n kawasan

# Check persistent volumes
kubectl get pvc -n kawasan

# View logs
kubectl logs -f -n kawasan kawasan-broker-0
```

### 3. Test Connection

```bash
# Port forward to access broker
kubectl port-forward -n kawasan svc/kawasan-broker 9092:9092

# In another terminal, test with producer/consumer
# (assuming you have Kafka tools installed)
kafka-console-producer.sh --broker-list localhost:9092 --topic test
```

---

## Helm Deployment

### 1. Install Helm Chart

```bash
# Install with default values
helm install kawasan ./helm/kawasan --namespace kawasan --create-namespace

# Install with custom values
helm install kawasan ./helm/kawasan \
  --namespace kawasan \
  --create-namespace \
  --values custom-values.yaml
```

### 2. Custom Values Example

Create a `custom-values.yaml` file:

```yaml
# Increase replicas for HA
replicaCount: 5

# Use specific storage class
persistence:
  enabled: true
  size: 200Gi
  storageClass: fast-ssd

# Increase resources
resources:
  requests:
    cpu: 2000m
    memory: 4Gi
  limits:
    cpu: 8000m
    memory: 16Gi

# Enable monitoring
metrics:
  enabled: true
  serviceMonitor:
    enabled: true

# Production-grade anti-affinity
affinity:
  podAntiAffinity:
    type: required
```

### 3. Upgrade Deployment

```bash
# Upgrade with new values
helm upgrade kawasan ./helm/kawasan \
  --namespace kawasan \
  --values custom-values.yaml

# Rollback if needed
helm rollback kawasan -n kawasan
```

### 4. Uninstall

```bash
# Uninstall release
helm uninstall kawasan -n kawasan

# Delete PVCs if needed (WARNING: This deletes all data!)
kubectl delete pvc -l app.kubernetes.io/name=kawasan -n kawasan
```

---

## Configuration

### Broker Configuration

Edit the ConfigMap (`k8s/kawasan-configmap.yaml`) or Helm values to customize:

```yaml
config:
  # Increase partitions for higher parallelism
  numPartitions: 6
  
  # Adjust retention
  logRetentionHours: 336  # 14 days
  logRetentionBytes: 5368709120  # 5GB
  
  # Enable compression
  compressionType: lz4
  
  # Replication settings
  defaultReplicationFactor: 3
  minInsyncReplicas: 2
```

### Storage Configuration

For production, use fast SSD storage:

```yaml
persistence:
  enabled: true
  size: 500Gi
  storageClass: fast-ssd  # AWS: gp3, GCP: pd-ssd, Azure: managed-premium
  accessMode: ReadWriteOnce
```

### Resource Configuration

Set appropriate resource limits based on workload:

```yaml
resources:
  requests:
    cpu: 2000m      # 2 cores
    memory: 4Gi
  limits:
    cpu: 8000m      # 8 cores
    memory: 16Gi
```

---

## Production Deployment

### 1. High Availability Setup

```yaml
# At least 3 replicas for HA
replicaCount: 3

# Required anti-affinity (brokers on different nodes)
affinity:
  podAntiAffinity:
    type: required
    topologyKey: kubernetes.io/hostname

# Pod Disruption Budget
podDisruptionBudget:
  enabled: true
  minAvailable: 2
```

### 2. Storage Best Practices

```yaml
persistence:
  enabled: true
  size: 500Gi
  storageClass: fast-ssd
  
# Example storage class for AWS EBS gp3
# apiVersion: storage.k8s.io/v1
# kind: StorageClass
# metadata:
#   name: fast-ssd
# provisioner: ebs.csi.aws.com
# parameters:
#   type: gp3
#   iopsPerGB: "10"
#   throughput: "125"
# allowVolumeExpansion: true
# volumeBindingMode: WaitForFirstConsumer
```

### 3. Security Configuration

```yaml
# Security context (already in defaults)
podSecurityContext:
  runAsUser: 1000
  runAsGroup: 1000
  fsGroup: 1000
  runAsNonRoot: true

securityContext:
  allowPrivilegeEscalation: false
  readOnlyRootFilesystem: true
  capabilities:
    drop:
    - ALL

# Network policies (optional)
networkPolicy:
  enabled: true
  ingress:
  - from:
    - namespaceSelector:
        matchLabels:
          name: application
    ports:
    - port: 9092
```

### 4. Resource Quotas

Create namespace resource quotas:

```yaml
apiVersion: v1
kind: ResourceQuota
metadata:
  name: kawasan-quota
  namespace: kawasan
spec:
  hard:
    requests.cpu: "20"
    requests.memory: 40Gi
    limits.cpu: "40"
    limits.memory: 80Gi
    persistentvolumeclaims: "10"
```

---

## Monitoring

### 1. Prometheus Metrics

Enable ServiceMonitor for Prometheus Operator:

```yaml
metrics:
  enabled: true
  serviceMonitor:
    enabled: true
    interval: 30s
    scrapeTimeout: 10s
    labels:
      prometheus: kube-prometheus
```

### 2. View Metrics

```bash
# Port forward metrics endpoint
kubectl port-forward -n kawasan svc/kawasan-broker 9308:9308

# Access metrics
curl http://localhost:9308/metrics
```

### 3. Key Metrics to Monitor

- Message throughput (messages/sec)
- Consumer lag
- Partition count
- Disk usage
- CPU and memory usage
- Network I/O

### 4. Logging

```bash
# View logs from all brokers
kubectl logs -n kawasan -l app=kawasan --tail=100 -f

# View logs from specific broker
kubectl logs -n kawasan kawasan-broker-0 -f

# Export logs to file
kubectl logs -n kawasan kawasan-broker-0 > broker-0.log
```

---

## Scaling

### Vertical Scaling (Resources)

```bash
# Update resource limits
helm upgrade kawasan ./helm/kawasan \
  --set resources.limits.cpu=16000m \
  --set resources.limits.memory=32Gi \
  --reuse-values
```

### Horizontal Scaling (Replicas)

```bash
# Scale to 5 brokers
helm upgrade kawasan ./helm/kawasan \
  --set replicaCount=5 \
  --reuse-values

# Or using kubectl
kubectl scale statefulset kawasan-broker --replicas=5 -n kawasan
```

### Storage Expansion

```bash
# Expand PVC (requires storage class with allowVolumeExpansion: true)
kubectl patch pvc data-kawasan-broker-0 -n kawasan \
  -p '{"spec":{"resources":{"requests":{"storage":"200Gi"}}}}'
```

---

## Backup and Recovery

### 1. Backup Data

```bash
# Create snapshot of PVC (cloud provider specific)
# AWS EBS example:
kubectl annotate pvc data-kawasan-broker-0 -n kawasan \
  snapshot.storage.kubernetes.io/enable=true

# Or use VolumeSnapshot
cat <<EOF | kubectl apply -f -
apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshot
metadata:
  name: kawasan-backup-$(date +%Y%m%d)
  namespace: kawasan
spec:
  volumeSnapshotClassName: csi-snapclass
  source:
    persistentVolumeClaimName: data-kawasan-broker-0
EOF
```

### 2. Restore from Backup

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: data-kawasan-broker-0-restored
spec:
  dataSource:
    name: kawasan-backup-20250118
    kind: VolumeSnapshot
    apiGroup: snapshot.storage.k8s.io
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 100Gi
```

### 3. Disaster Recovery

1. Regular automated snapshots
2. Multi-region replication (for critical systems)
3. Configuration backup in Git
4. Documented recovery procedures
5. Regular recovery testing

---

## Troubleshooting

### Pod Not Starting

```bash
# Check pod events
kubectl describe pod kawasan-broker-0 -n kawasan

# Check pod logs
kubectl logs kawasan-broker-0 -n kawasan

# Check previous pod logs (if crashed)
kubectl logs kawasan-broker-0 -n kawasan --previous
```

### Storage Issues

```bash
# Check PVC status
kubectl get pvc -n kawasan

# Check PV status
kubectl get pv

# Describe PVC for events
kubectl describe pvc data-kawasan-broker-0 -n kawasan
```

### Network Issues

```bash
# Check service endpoints
kubectl get endpoints -n kawasan

# Test DNS resolution
kubectl run -it --rm debug --image=busybox --restart=Never -n kawasan -- \
  nslookup kawasan-broker-headless.kawasan.svc.cluster.local

# Test connectivity
kubectl run -it --rm debug --image=busybox --restart=Never -n kawasan -- \
  telnet kawasan-broker-0.kawasan-broker-headless.kawasan.svc.cluster.local 9092
```

### Performance Issues

```bash
# Check resource usage
kubectl top pods -n kawasan

# Check node resources
kubectl top nodes

# Check for throttling
kubectl describe pod kawasan-broker-0 -n kawasan | grep -i throttl
```

### Common Issues

1. **Insufficient resources**: Increase node capacity or reduce resource requests
2. **Storage full**: Increase PVC size or adjust retention settings
3. **Slow storage**: Use faster storage class (SSD)
4. **Network latency**: Check network policies and service mesh configuration
5. **Pod eviction**: Increase resource requests or add priority class

---

## Advanced Topics

### Multi-Zone Deployment

```yaml
affinity:
  podAntiAffinity:
    requiredDuringSchedulingIgnoredDuringExecution:
    - labelSelector:
        matchLabels:
          app: kawasan
      topologyKey: topology.kubernetes.io/zone
```

### Custom Probes

```yaml
livenessProbe:
  httpGet:
    path: /health
    port: 8080
  initialDelaySeconds: 60
  periodSeconds: 10

readinessProbe:
  httpGet:
    path: /ready
    port: 8080
  initialDelaySeconds: 30
  periodSeconds: 5
```

### Init Containers

Already included for broker ID configuration. Add more if needed:

```yaml
initContainers:
- name: wait-for-dependencies
  image: busybox
  command: ['sh', '-c', 'until nc -z zookeeper 2181; do sleep 1; done']
```

---

## References

- [Kubernetes Documentation](https://kubernetes.io/docs/)
- [Helm Documentation](https://helm.sh/docs/)
- [Kubernetes Best Practices](https://kubernetes.io/docs/concepts/configuration/overview/)
- [StatefulSet Documentation](https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/)
- [Persistent Volumes](https://kubernetes.io/docs/concepts/storage/persistent-volumes/)

---

## Support

For issues and questions:
- GitHub Issues: https://github.com/yourusername/kawasan/issues
- Documentation: https://docs.kawasan.io
- Community Slack: https://kawasan.slack.com
