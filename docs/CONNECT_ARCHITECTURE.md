# Kawasan Connect Architecture

## Overview

Kawasan Connect provides a framework for building and running connectors that move data between external systems and Kawasan topics. It follows the architecture patterns established by the Apache Kafka Connect API while adapting them to C++.

## Core Concepts

### Connectors

A **Connector** is the high-level abstraction that defines how data should flow between an external system and Kawasan. Connectors are responsible for:

1. **Configuration validation** - Ensuring required properties are present and valid
2. **Task creation** - Splitting work into parallel tasks
3. **Lifecycle management** - Starting and stopping gracefully

There are two types of connectors:

- **Source Connectors**: Read data from external systems and write to Kawasan topics
- **Sink Connectors**: Read data from Kawasan topics and write to external systems

### Tasks

A **Task** is the unit of parallelism within a connector. Each task is responsible for a portion of the connector's work:

- **Source Tasks**: Poll external systems for new data and produce records
- **Sink Tasks**: Consume records from Kawasan and write to external systems

Tasks are designed to be stateless - all state is managed through offsets.

### Workers

A **Worker** is the runtime process that hosts connectors and tasks. Workers handle:

1. **Connector lifecycle** - Starting, stopping, and restarting connectors
2. **Task distribution** - Assigning tasks to threads
3. **Offset management** - Storing and retrieving task progress
4. **Health monitoring** - Detecting and recovering from failures

## Worker Architecture

### Standalone Mode

In standalone mode, a single worker process runs all connectors and tasks:

```
┌─────────────────────────────────────────────────────────────┐
│                    Standalone Worker                         │
│                                                             │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │ Connector A  │  │ Connector B  │  │ Connector C  │      │
│  │  (Source)    │  │  (Sink)      │  │  (Source)    │      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
│        │                 │                 │                │
│  ┌─────┴─────┐    ┌─────┴─────┐    ┌─────┴─────┐          │
│  │  Task 1   │    │  Task 1   │    │  Task 1   │          │
│  │  Task 2   │    │  Task 2   │    │  Task 2   │          │
│  └───────────┘    └───────────┘    └───────────┘          │
│                                                             │
│  ┌─────────────────────────────────────────────────────┐   │
│  │                Thread Pool                           │   │
│  │  [Worker 1] [Worker 2] [Worker 3] [Worker 4] ...    │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
│  ┌─────────────────────────────────────────────────────┐   │
│  │              Offset Storage                          │   │
│  │         (File or Kawasan Topic)                       │   │
│  └─────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

### Distributed Mode (Future)

In distributed mode, multiple workers coordinate via a Kawasan topic:

```
┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
│    Worker 1     │  │    Worker 2     │  │    Worker 3     │
│                 │  │                 │  │                 │
│  Connector A    │  │  Connector A    │  │  Connector B    │
│    Task 1       │  │    Task 2       │  │    Task 1       │
│    Task 2       │  │    Task 3       │  │    Task 2       │
└────────┬────────┘  └────────┬────────┘  └────────┬────────┘
         │                    │                    │
         └────────────────────┼────────────────────┘
                              │
                    ┌─────────┴─────────┐
                    │  Kawasan Cluster   │
                    │                   │
                    │  config-topic     │
                    │  offset-topic     │
                    │  status-topic     │
                    └───────────────────┘
```

## Task Model and Lifecycle

### Task Lifecycle

```
                    ┌─────────┐
                    │ Created │
                    └────┬────┘
                         │ start(config)
                         ▼
                    ┌─────────┐
         ┌─────────│ Running │◄────────┐
         │         └────┬────┘         │
         │              │              │
    error│         poll/put       restart
         │              │              │
         ▼              ▼              │
    ┌─────────┐    ┌─────────┐        │
    │ Failed  │────│ Paused  │────────┘
    └────┬────┘    └─────────┘
         │
         │ stop()
         ▼
    ┌─────────┐
    │ Stopped │
    └─────────┘
```

### Source Task Flow

```
while (running) {
    1. Poll external system for new records
    2. Transform records to SourceRecord format
    3. Return records to worker
    4. Worker produces records to Kawasan
    5. On success, worker commits offsets
}
```

### Sink Task Flow

```
while (running) {
    1. Worker fetches records from Kawasan
    2. Worker calls put(records) on task
    3. Task writes records to external system
    4. On success, worker commits offsets
}
```

## Connector Plugin System

### Plugin Discovery

Connectors are registered programmatically in C++:

```cpp
// Register a connector class
ConnectorFactory::registerConnector<FileSourceConnector>("FileSource");
ConnectorFactory::registerConnector<FileSinkConnector>("FileSink");

// Create connector by name
auto connector = ConnectorFactory::create("FileSource");
```

### Configuration

Connectors are configured via Properties (key-value pairs):

```cpp
Properties config;
config.set("name", "my-file-source");
config.set("connector.class", "FileSource");
config.set("file.path", "/var/log/app.log");
config.set("topic", "app-logs");
config.set("tasks.max", "2");
```

## Offset Storage

### Offset Model

Offsets track the progress of each connector task:

- **Source Offsets**: Track position in external system (e.g., file offset, database cursor)
- **Sink Offsets**: Track Kawasan consumer offsets (managed automatically)

Offsets are structured as key-value maps:

```cpp
// Source partition (identifies the data source)
std::map<std::string, std::string> partition = {
    {"filename", "/var/log/app.log"}
};

// Source offset (position within the source)
std::map<std::string, std::string> offset = {
    {"position", "12345"},
    {"line", "678"}
};
```

### Storage Backends

#### File Offset Storage

Stores offsets in a local JSON file:

```json
{
  "offsets": [
    {
      "partition": {"filename": "/var/log/app.log"},
      "offset": {"position": "12345"}
    }
  ]
}
```

Suitable for standalone mode and development.

#### Kawasan Offset Storage (Future)

Stores offsets in a dedicated Kawasan topic (`connect-offsets`):

- Enables distributed mode with shared offset state
- Supports exactly-once semantics via transactions
- Automatic compaction retains only latest offsets

### Commit Interval

Offsets are committed periodically (default: 60 seconds):

```cpp
WorkerConfig config;
config.offsetFlushIntervalMs = 60000;  // 60 seconds
```

## Configuration Management

### Worker Configuration

```cpp
struct WorkerConfig {
    // Basic settings
    std::string bootstrapServers = "localhost:9092";
    std::string groupId = "connect-cluster";

    // Offset storage
    std::string offsetStorageFile = "/tmp/connect-offsets.json";
    int64_t offsetFlushIntervalMs = 60000;

    // Task execution
    int workerThreads = 4;
    int taskRestartMaxAttempts = 3;
    int64_t taskRestartBackoffMs = 10000;
};
```

### Connector Configuration

Required properties for all connectors:

| Property | Description |
|----------|-------------|
| `name` | Unique connector name |
| `connector.class` | Connector implementation class |
| `tasks.max` | Maximum number of tasks |

Additional properties depend on the connector type.

## REST API (Future)

### Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | /connectors | List all connectors |
| POST | /connectors | Create a new connector |
| GET | /connectors/{name} | Get connector details |
| DELETE | /connectors/{name} | Delete a connector |
| GET | /connectors/{name}/status | Get connector status |
| PUT | /connectors/{name}/pause | Pause a connector |
| PUT | /connectors/{name}/resume | Resume a connector |
| POST | /connectors/{name}/restart | Restart a connector |
| GET | /connectors/{name}/tasks | List connector tasks |
| POST | /connectors/{name}/tasks/{id}/restart | Restart a task |

## Error Handling

### Retry Strategy

Failed tasks use exponential backoff:

```
Attempt 1: Wait 1s, then retry
Attempt 2: Wait 2s, then retry
Attempt 3: Wait 4s, then retry
...
Max wait: 60s
```

### Dead Letter Queue (Future)

Records that cannot be processed after retries are sent to a DLQ topic:

```
original-topic-dlq
```

## Data Formats

### SourceRecord

```cpp
struct SourceRecord {
    // Source tracking
    std::map<std::string, std::string> sourcePartition;
    std::map<std::string, std::string> sourceOffset;

    // Destination
    std::string topic;
    std::optional<int32_t> partition;

    // Record data
    std::optional<std::string> key;
    std::string value;

    // Metadata
    int64_t timestamp = 0;
    std::map<std::string, std::string> headers;
};
```

### SinkRecord

```cpp
struct SinkRecord {
    std::string topic;
    int32_t partition;
    int64_t offset;

    std::optional<std::string> key;
    std::string value;

    int64_t timestamp;
    std::map<std::string, std::string> headers;
};
```

## Thread Safety

### Worker Thread Model

```
Main Thread
    │
    ├── Connector Management
    │
    └── Task Thread Pool
            │
            ├── Task Runner 1 ──► Source/Sink Task
            ├── Task Runner 2 ──► Source/Sink Task
            ├── Task Runner 3 ──► Source/Sink Task
            └── Task Runner 4 ──► Source/Sink Task
```

### Synchronization Points

- Connector configuration: Protected by mutex
- Offset storage: Protected by mutex, batched writes
- Task state: Protected by atomic operations

## Monitoring

### Metrics

| Metric | Description |
|--------|-------------|
| `connect_source_records_total` | Total source records produced |
| `connect_sink_records_total` | Total sink records consumed |
| `connect_task_running` | Number of running tasks |
| `connect_task_failed` | Number of failed tasks |
| `connect_offset_commit_latency` | Offset commit latency |

### Health Checks

- Worker heartbeat
- Task polling status
- Connector error state

## Implementation Phases

### Phase 1: Foundation (Week 23)
- Core interfaces (Connector, Task, Worker)
- Standalone worker implementation
- File-based offset storage

### Phase 2: Source Connectors (Week 24)
- Source task framework
- File source connector
- Schema support (string, JSON, CSV)

### Phase 3: Sink Connectors (Week 25)
- Sink task framework
- File sink connector
- Console sink connector

### Phase 4: Production Features (Week 26)
- Kawasan offset storage
- Error handling improvements
- Metrics and monitoring
- REST API (basic)
