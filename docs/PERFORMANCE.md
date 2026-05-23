# Kawasan Performance Benchmarks

This document tracks performance benchmarks and optimizations for Kawasan.

## Overview

Performance targets:
- **Throughput**: 100k+ msg/sec (single broker, 10 partitions, 100-byte messages)
- **Latency**: p99 < 5ms for produce operations
- **Memory**: < 1GB idle, stable under load
- **CPU**: Efficient use of multi-core systems

---

## Baseline Performance (Pre-Optimization)

**Date**: November 21, 2025  
**Configuration**:
- Single broker
- Topic with 1 partition (from simple_test.sh)
- Message size: 100 bytes
- Test: 10,000 messages

**Results from simple_test.sh**:
- Producer Throughput: ~30,000 msg/sec (30 MB/sec)
- Consumer Throughput: ~18,000 msg/sec (17 MB/sec)
- Concurrent Producers (5x): ~2,000 msg/sec per producer
- Concurrent Consumers (3x): ~9,000 msg/sec total
- End-to-End Latency: avg ~8s, p95 ~16s, p99 ~17s (including rebalance overhead)

**Bottlenecks Identified**:
1. **Synchronous RocksDB writes**: Each produce request triggers individual RocksDB write
2. **No write batching**: Messages are written one at a time instead of batched
3. **Buffer copies**: RecordBatch decoding and encoding creates unnecessary copies
4. **No segment caching**: Fetch operations read from RocksDB every time
5. **Consumer lag computation**: Was causing periodic blocking (fixed in M11)

**Status**: ⏳ Below target (baseline: ~30k msg/sec, target: 100k+ msg/sec)

---

## Benchmark Infrastructure

### Throughput Benchmark

**Tool**: `tests/benchmark/throughput_benchmark.cpp`  
**Purpose**: Measure raw throughput and latency for single-broker produce workload

**Configuration**:
- Single broker on port 9092
- Topic: `benchmark-throughput` with 10 partitions
- Total messages: 1,000,000 (configurable)
- Message size: 100 bytes
- Replication factor: 1
- Client: Built-in Kawasan Producer

**How to run**:
```bash
cd build
./tests/benchmark/throughput_benchmark
```

**Output**: Results are displayed to console and appended to this file.

### Replication Benchmark

**Tool**: `tests/benchmark/replication_benchmark.cpp`  
**Purpose**: Measure throughput and latency for 3-broker cluster with RF=3 and acks=-1

**Configuration**:
- 3-broker cluster
- Replication factor: 3
- acks: -1 (all replicas)
- Total messages: 1,000,000
- Message size: 100 bytes

**Target**: > 50k msg/sec with acks=-1

---

## Optimization Plan (Task 14.2)

### 14.2.1: Produce Path Optimizations

**Goal**: Reduce produce latency and increase throughput

**Optimizations**:

1. **RocksDB Write Batching**
   - Use `rocksdb::WriteBatch` to batch multiple writes
   - Batch size: 100-1000 messages (configurable)
   - Expected improvement: 2-3x throughput

2. **Zero-Copy RecordBatch**
   - Eliminate unnecessary buffer copies during encode/decode
   - Use move semantics and shared_ptr for record data
   - Expected improvement: 10-20% latency reduction

3. **Compression Handling**
   - Ensure compressed batches stay compressed on broker
   - Avoid decompression/recompression cycles
   - Expected improvement: 30-50% CPU reduction for compressed workloads

**Implementation Status**: ⏳ Pending

### 14.2.2: Fetch Path Optimizations

**Goal**: Improve fetch throughput and reduce latency

**Optimizations**:

1. **Segment Prefetching**
   - Read-ahead next segment when current segment is nearly exhausted
   - Async prefetch to avoid blocking fetch requests
   - Expected improvement: 20-30% latency reduction

2. **Segment Caching**
   - LRU cache for recently read segments (configurable size, default 256MB)
   - Cache hit rate target: > 80% for typical workloads
   - Expected improvement: 2-5x throughput for hot data

**Implementation Status**: ⏳ Pending

### 14.2.3: Measurement and Documentation

After optimizations:
- Re-run throughput benchmark
- Compare before/after results
- Profile with Instruments (macOS) or perf (Linux)
- Document hotspots and improvements
- Update this file with results

**Target**: 100k+ msg/sec (2x baseline of ~30k-50k msg/sec)

---

## Memory & Resource Management (Task 14.3)

### 14.3.1: Memory Limits

**Configuration**:
```properties
memory.max.bytes=1073741824  # 1GB default
memory.connections.max.bytes=104857600  # 100MB for connections
memory.cache.max.bytes=268435456  # 256MB for segment cache
```

**Tracking**:
- Connection buffers and metadata
- Log segment cache
- Consumer group state
- Raft log (in-memory portion)

**Enforcement**:
- Reject new connections if memory limit exceeded
- Evict cache entries when near limit
- Log warnings at 80% threshold

**Implementation Status**: ⏳ Pending

### 14.3.2: Log Retention Optimization

**Current Implementation**:
- Periodic scan every 10 minutes
- Checks all segments for retention policy

**Optimizations**:
- Maintain sorted index of segment timestamps
- Binary search for expired segments
- Incremental cleanup (avoid scanning all segments)
- Skip segments known to be recent

**Target**: Handle 1M+ segments efficiently (< 1s cleanup time)

**Implementation Status**: ⏳ Pending

### 14.3.3: Load Testing

**Tool**: `tests/benchmark/load_test.cpp`

**Configuration**:
- 1000 concurrent clients
- Sustained load for 1 hour
- Mix of producers and consumers
- Monitor: memory, CPU, disk I/O, connection count

**Success Criteria**:
- Memory stable (< 1GB, no leaks)
- CPU < 80% on 4-core system
- No errors or crashes
- Throughput stable throughout test

**Implementation Status**: ⏳ Pending

---

## Profiling Notes

### Instruments (macOS)

To profile with Instruments:
```bash
# Start broker
./build/tools/kawasan-broker --config config/broker.macos.properties &

# Profile with Instruments
instruments -t "Time Profiler" -D profile.trace -l 60000 \
  ./build/tests/benchmark/throughput_benchmark

# Open trace
open profile.trace
```

### perf (Linux)

To profile with perf:
```bash
# Start broker
./build/tools/kawasan-broker --config config/broker.macos.properties &

# Profile with perf
perf record -g -F 999 -p $(pgrep kawasan-broker) -- sleep 60

# Analyze
perf report
```

---

## Historical Results

Results from previous benchmarks will be appended below as they are run.

---
