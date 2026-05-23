# Kawasan Architecture

## Overview

Kawasan is a modern C++20 reimplementation of Apache Kafka, designed for high performance, reliability, and wire-protocol compatibility with existing Kafka clients (eco-system)

## System Architecture

```
┌────────────────────────────────────────────────────────────────┐
│                        Client Applications                      │
│  (Producers, Consumers, Admin Clients, Streams Applications)   │
└────────────────────────────────────────────────────────────────┘
                              │
                    Kafka Wire Protocol
                              │
┌────────────────────────────────────────────────────────────────┐
│                       Kawasan Cluster                            │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │                   Broker Instance                         │  │
│  │  ┌─────────────────────────────────────────────────────┐ │  │
│  │  │  TCP Server (Boost.Asio)                            │ │  │
│  │  │  - Connection Management                            │ │  │
│  │  │  - Request Routing                                  │ │  │
│  │  └─────────────────────────────────────────────────────┘ │  │
│  │                          │                                │  │
│  │  ┌─────────────────────────────────────────────────────┐ │  │
│  │  │  Request Handler                                    │ │  │
│  │  │  - API Request Processing                           │ │  │
│  │  │  - Protocol Encoding/Decoding                       │ │  │
│  │  └─────────────────────────────────────────────────────┘ │  │
│  │                          │                                │  │
│  │  ┌──────────────┬────────────────┬────────────────────┐  │  │
│  │  │              │                │                    │  │  │
│  │  │  Replica     │  Group         │  Transaction       │  │  │
│  │  │  Manager     │  Coordinator   │  Coordinator       │  │  │
│  │  │              │                │                    │  │  │
│  │  └──────────────┴────────────────┴────────────────────┘  │  │
│  │                          │                                │  │
│  │  ┌─────────────────────────────────────────────────────┐ │  │
│  │  │  Raft Consensus Protocol                    │ │  │
│  │  │  - Leader Election                                  │ │  │
│  │  │  - Log Replication                                  │ │  │
│  │  │  - Metadata Management                              │ │  │
│  │  └─────────────────────────────────────────────────────┘ │  │
│  │                          │                                │  │
│  │  ┌─────────────────────────────────────────────────────┐ │  │
│  │  │  Storage Layer                                      │ │  │
│  │  │  ┌───────────────────────────────────────────────┐ │ │  │
│  │  │  │  Log Manager                                   │ │ │  │
│  │  │  │  - Topic-Partition Management                  │ │ │  │
│  │  │  │  - Segment Rolling                             │ │ │  │
│  │  │  │  - Cleanup & Retention                         │ │ │  │
│  │  │  └───────────────────────────────────────────────┘ │ │  │
│  │  │                      │                              │ │  │
│  │  │  ┌───────────────────────────────────────────────┐ │ │  │
│  │  │  │  Log (per topic-partition)                    │ │ │  │
│  │  │  │  - Multiple Segments                          │ │ │  │
│  │  │  │  - High Watermark Management                  │ │ │  │
│  │  │  └───────────────────────────────────────────────┘ │ │  │
│  │  │                      │                              │ │  │
│  │  │  ┌───────────────────────────────────────────────┐ │ │  │
│  │  │  │  Log Segment (RocksDB-backed)                 │ │ │  │
│  │  │  │  - Record Batch Storage                       │ │ │  │
│  │  │  │  - Index Management                           │ │ │  │
│  │  │  │  - Compaction Support                         │ │ │  │
│  │  │  └───────────────────────────────────────────────┘ │ │  │
│  │  └─────────────────────────────────────────────────────┘ │  │
│  └──────────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────┘
```

## Core Components

### 1. Protocol Layer (`src/protocol/`)

Implements the Kafka wire protocol for communication between clients and brokers.

**Key Classes:**
- `RequestHeader` / `ResponseHeader`: Protocol headers with correlation IDs
- `MetadataRequest` / `MetadataResponse`: Broker and topic discovery
- `ProduceRequest` / `ProduceResponse`: Record production
- `FetchRequest` / `FetchResponse`: Record consumption
- `ApiKey`: Enumeration of all API types

**Features:**
- Binary protocol encoding/decoding
- Version negotiation
- Backward compatibility with Kafka clients

### 2. Storage Layer (`src/storage/`)

Log-structured storage system backed by RocksDB.

**Key Classes:**
- `LogManager`: Manages all topic-partition logs
- `Log`: Represents a single topic-partition
- `LogSegment`: Individual segment file (RocksDB instance)
- `RecordBatch`: Unit of storage and replication

**Features:**
- Append-only log semantics
- Segment rolling (size/time based)
- Log compaction
- Retention policies (time/size)
- Offset indexing
- High watermark tracking

**Storage Format:**
```
/log.dirs/
  ├── topic-0/
  │   ├── 00000000000000000000/  (RocksDB directory - base offset)
  │   ├── 00000000000001000000/
  │   └── ...
  ├── topic-1/
  └── ...
```

### 3. Raft Consensus (`src/raft/`)

Raft-based consensus for metadata management (no ZooKeeper).

**Key Classes:**
- `RaftNode`: Single Raft participant
- `LogEntry`: Replicated log entry
- `RequestVoteRequest/Response`: Leader election RPCs
- `AppendEntriesRequest/Response`: Log replication RPCs

**Features:**
- Leader election
- Log replication
- Metadata management
- Quorum-based commits
- Snapshot support (future)

**State Transitions:**
```
Follower ─timeout─> Candidate ─majority votes─> Leader
    ↑                    │                          │
    │                    └─────── newer term ───────┘
    └─────────────── newer term ────────────────────┘
```

### 4. Broker Core (`src/broker/`)

Main broker logic and coordination.

**Key Classes:**
- `KafkaBroker`: Main broker entry point
- `TcpServer`: Network layer (Boost.Asio)
- `RequestHandler`: Routes API requests
- `ReplicaManager`: Partition replication
- `GroupCoordinator`: Consumer group management
- `TransactionCoordinator`: Transaction support

**Request Flow:**
```
Client → TCP Server → Request Handler → [Coordinator/Manager] → Storage → Response
```

### 5. Client Library (`src/client/`)

Producer and Consumer client APIs.

**Key Classes:**
- `Producer`: Asynchronous record production
- `Consumer`: Record consumption with group management
- `RecordMetadata`: Production result

**Producer Flow:**
```
send() → Partition Selection → Batching → Compression → Network Send → ACK
```

**Consumer Flow:**
```
poll() → Fetch Request → Broker Response → Decompression → Record Processing
```

## Data Flow

### Write Path (Producer)

1. Producer sends records to broker
2. Broker receives ProduceRequest
3. Request handler validates and routes to ReplicaManager
4. ReplicaManager appends to local log (leader)
5. Replicates to follower replicas
6. Waits for ISR acknowledgments (based on acks config)
7. Updates high watermark
8. Returns ProduceResponse to producer

### Read Path (Consumer)

1. Consumer sends FetchRequest to broker
2. Broker validates request
3. Request handler routes to ReplicaManager
4. ReplicaManager reads from local log
5. Returns records up to high watermark
6. Consumer processes records
7. Consumer commits offsets

### Metadata Management (Raft)

1. Metadata change proposal (e.g., create topic)
2. Leader appends to metadata log
3. Replicates to quorum of controllers
4. Commits when quorum achieved
5. All controllers apply change
6. Brokers receive metadata update

## Threading Model

- **Network Threads**: Handle I/O (Boost.Asio thread pool)
- **Request Handler Threads**: Process requests
- **Log Flush Thread**: Periodic fsync
- **Log Cleanup Thread**: Segment deletion/compaction
- **Raft Election Thread**: Handles timeouts and elections
- **Raft Heartbeat Thread**: Sends heartbeats (leader)

## Performance Optimizations

### Zero-Copy

- `sendfile()` for log segments
- Memory-mapped files for index
- Direct buffer passing

### Batching

- Producer batching (configurable size/time)
- Consumer fetch batching
- Log segment writes

### Compression

- Per-batch compression (gzip, snappy, lz4, zstd)
- Stored compressed, served compressed

### Caching

- Metadata cache at brokers
- Producer metadata cache
- Consumer partition assignment cache

## Failure Handling

### Broker Failure

- Raft leader election for new controller
- Partition leader election
- ISR management
- Automatic failover

### Network Partition

- Raft ensures one leader per term
- Stale metadata rejection
- Fencing with epochs

### Disk Failure

- Replica promotion
- Controller notification
- Automatic rebalancing

## Configuration

Key broker configurations:
- `broker.id`: Unique broker identifier
- `log.dirs`: Log directory paths
- `log.segment.bytes`: Segment size
- `log.retention.hours`: Retention time
- `num.partitions`: Default partition count
- `replication.factor`: Default replication

## Monitoring & Metrics

- Request metrics (rate, latency)
- Storage metrics (size, segments)
- Replication metrics (lag, ISR)
- Consumer group metrics (lag, members)
- Raft metrics (term, leader)

## Future Enhancements

1. **Tiered Storage**: Archive old segments to S3/cloud
2. **Rack Awareness**: Replica placement optimization
3. **Quotas**: Client rate limiting
4. **Security**: SASL, ACLs, encryption at rest
5. **Exactly-Once Semantics**: Full transactional support
6. **Streams DSL**: Complete Kawasan Streams implementation
7. **Connect Framework**: Connector ecosystem
8. **Schema Registry**: Schema management
9. **Multi-tenancy**: Resource isolation
10. **Observability**: OpenTelemetry integration

## Comparison with Apache Kafka

| Feature | Kawasan | Apache Kafka |
|---------|--------|--------------|
| Language | C++20 | Java/Scala |
| Protocol | Wire-compatible | Native |
| Metadata | Raft consensus | KRaft or ZooKeeper |
| Storage | RocksDB | Custom |
| Performance | High (C++) | High (JVM) |
| Memory | Lower | Higher (JVM overhead) |
| Startup | Fast | Slower (JVM) |
| Ecosystem | Growing | Mature |

## Build & Deployment

See [README.md](../README.md) for build instructions.

Docker deployment:
```bash
docker-compose up -d
```

Kubernetes deployment:
```bash
kubectl apply -f k8s/
```

## Testing

- Unit tests: Component-level testing
- Integration tests: Multi-broker scenarios
- Compatibility tests: Kafka client compatibility
- Performance tests: Throughput and latency benchmarks
- Chaos tests: Failure injection

## Contributing

See [CONTRIBUTING.md](../CONTRIBUTING.md) for guidelines.

