# Kawasan Streams Architecture

**Created**: November 24, 2025  
**Version**: 0.2.0-alpha  
**Status**: Design Document

---

## 1. Overview

Kawasan Streams is a high-level stream processing library built on top of the Kawasan broker and client. It provides a declarative API for building stream processing applications with support for stateless and stateful transformations, windowing, joins, and exactly-once semantics.

### Design Goals

1. **Kafka Compatibility**: API compatible with Kafka Streams where practical
2. **Type Safety**: Leverage C++20 templates and concepts for compile-time safety
3. **Performance**: Zero-copy operations, efficient state management
4. **Simplicity**: Clean DSL that hides complexity while allowing low-level control
5. **Testability**: Pure functions, mockable dependencies, deterministic behavior

---

## 2. Core Abstractions

### 2.1 Topology

A **Topology** is a directed acyclic graph (DAG) of processing nodes. Each node performs a transformation on the stream data and may maintain local state.

```
┌─────────────┐
│   Source    │  (e.g., topic: "input")
└──────┬──────┘
       │
       v
┌─────────────┐
│   Filter    │  (predicate)
└──────┬──────┘
       │
       v
┌─────────────┐
│     Map     │  (transform)
└──────┬──────┘
       │
       v
┌─────────────┐
│    Sink     │  (e.g., topic: "output")
└─────────────┘
```

**Properties**:
- Nodes have unique names (auto-generated or user-provided)
- Edges represent data flow between nodes
- Multiple child nodes create branching
- Multiple parent nodes create merging
- No cycles allowed (validated at build time)

**Implementation**:
```cpp
class Topology {
public:
    struct Node {
        std::string name;
        NodeType type;  // SOURCE, PROCESSOR, SINK
        std::vector<std::string> parents;
        std::vector<std::string> children;
        std::shared_ptr<ProcessorSupplier> processorSupplier;
        std::vector<std::string> stateStoreNames;
    };
    
    void addSource(std::string name, std::vector<std::string> topics);
    void addProcessor(std::string name, ProcessorSupplier supplier, 
                     std::vector<std::string> parents);
    void addSink(std::string name, std::string topic, 
                std::vector<std::string> parents);
    void addStateStore(std::string storeName, StoreBuilder builder, 
                      std::vector<std::string> processorNames);
    
    std::map<std::string, Node> nodes() const;
    bool validate() const;  // Check for cycles, orphaned nodes
};
```

### 2.2 Stream vs. KTable Semantics

**KStream**: An unbounded sequence of immutable records (event stream)
- Represents a log of facts
- Each record is an independent event
- Updates are appends (no overwriting)
- Example: click events, transactions, sensor readings

**KTable**: A changelog stream representing the latest value per key (table)
- Represents current state
- Each record with the same key updates the previous value
- Null value = tombstone (delete)
- Backed by a compacted topic
- Example: user profiles, product catalog, account balances

**Duality**:
```
KStream → KTable: groupByKey().aggregate()
KTable → KStream: toStream()
```

**Implementation Insight**:
- KStream and KTable share the same underlying `Topology` but with different semantics
- KTable nodes maintain a materialized state store
- State stores are automatically backed by changelog topics
- Changelog topics use log compaction for efficiency

### 2.3 Task Assignment & Parallelism

**Stream Partitions → Tasks**:
- Each task processes a subset of input partitions
- Task = smallest unit of parallelism
- One task = one or more input partitions with the same key space

**Assignment Algorithm** (simplified):
```
Given:
- Input topics: [T1, T2]
- T1 has 3 partitions: [T1-0, T1-1, T1-2]
- T2 has 3 partitions: [T2-0, T2-1, T2-2]

Create Tasks:
- Task 0: [T1-0, T2-0]
- Task 1: [T1-1, T2-1]
- Task 2: [T1-2, T2-2]

Each task processes all co-partitioned inputs for a partition set.
```

**Threads**:
- Stream threads = application-level threads (configurable)
- Tasks are distributed across threads
- One thread can handle multiple tasks
- Thread = independent consumer with its own state stores

```
┌─────────────────────────────────────┐
│     KafkaStreams Application        │
├─────────────────────────────────────┤
│  Thread 1           Thread 2        │
│  ┌──────┐           ┌──────┐        │
│  │Task 0│           │Task 2│        │
│  │      │           │      │        │
│  │Store │           │Store │        │
│  └──────┘           └──────┘        │
│  ┌──────┐                           │
│  │Task 1│                           │
│  │      │                           │
│  │Store │                           │
│  └──────┘                           │
└─────────────────────────────────────┘
```

**Standby Tasks** (future):
- Replicate state stores on other instances
- Enable fast failover when a task fails
- Standby = read-only replica updated from changelog

**Implementation**:
```cpp
class StreamTask {
    std::vector<TopicPartition> partitions;
    std::unique_ptr<Topology> topology;
    std::map<std::string, StateStore> stateStores;
    std::unique_ptr<Consumer> consumer;
    std::unique_ptr<Producer> producer;
    
    void process();  // Poll, process, produce
    void commit();   // Commit offsets and state
};

class StreamThread {
    std::vector<StreamTask> tasks;
    std::thread thread;
    
    void run();  // Task loop
};
```

### 2.4 State Store Interface

**State Store Types**:
1. **KeyValueStore**: Simple key-value storage (most common)
2. **WindowStore**: Time-windowed key-value storage
3. **SessionStore**: Session-windowed storage (future)

**KeyValueStore API**:
```cpp
template<typename K, typename V>
class KeyValueStore {
public:
    virtual void put(const K& key, const V& value) = 0;
    virtual std::optional<V> get(const K& key) = 0;
    virtual void remove(const K& key) = 0;
    
    // Range queries
    virtual KeyValueIterator<K, V> range(const K& from, const K& to) = 0;
    virtual KeyValueIterator<K, V> all() = 0;
    
    // Lifecycle
    virtual void flush() = 0;
    virtual void close() = 0;
    virtual std::string name() const = 0;
};
```

**WindowStore API**:
```cpp
template<typename K, typename V>
class WindowStore {
public:
    virtual void put(const K& key, const V& value, int64_t timestamp) = 0;
    virtual WindowStoreIterator<V> fetch(const K& key, 
                                         int64_t timeFrom, 
                                         int64_t timeTo) = 0;
    
    // Window management
    virtual void expire(int64_t retentionPeriod) = 0;
};
```

**Backend Implementation**:
- Primary: RocksDB (persistent, high performance)
- Alternative: In-memory (testing, ephemeral state)

**RocksDB Layout**:
```
State Store Name: "counts-store"
RocksDB Directory: /tmp/kawasan-streams/app-id/0_0/rocksdb/counts-store/

Key Encoding:
- KeyValueStore: <raw-key-bytes>
- WindowStore: <raw-key-bytes><timestamp-8-bytes>

Value Encoding:
- Serialized using Serde<V>
```

### 2.5 Changelog Topic Pattern

**Purpose**: Backup state stores for fault tolerance

**Mechanism**:
1. Every state store has a changelog topic: `<app-id>-<store-name>-changelog`
2. All `put()` and `delete()` operations are **written through** to the changelog
3. On restart, state is restored by replaying the changelog
4. Changelog topics use **log compaction** to prevent unbounded growth

**Example**:
```
State Store: "word-counts"
Changelog Topic: "wordcount-app-word-counts-changelog"

Operations:
put("hello", 1)  →  Produce to changelog: key="hello", value=1
put("hello", 2)  →  Produce to changelog: key="hello", value=2
put("world", 1)  →  Produce to changelog: key="world", value=1
delete("hello")  →  Produce to changelog: key="hello", value=null (tombstone)

After Compaction:
- key="world", value=1
- key="hello", value=null  (can be deleted after retention)
```

**Restoration Process**:
```cpp
void restoreStateStore(StateStore store, std::string changelogTopic) {
    auto consumer = createRestoreConsumer(changelogTopic);
    consumer.seekToBeginning();
    
    while (true) {
        auto records = consumer.poll(100ms);
        if (records.empty() && consumer.position() == consumer.endOffsets()) {
            break;  // Fully caught up
        }
        
        for (auto& record : records) {
            if (record.value.has_value()) {
                store.put(record.key, record.value.value());
            } else {
                store.remove(record.key);  // Tombstone
            }
        }
    }
}
```

**Optimization**: Standby tasks pre-restore state to minimize failover time.

### 2.6 Exactly-Once vs. At-Least-Once

**At-Least-Once Semantics** (default):
- Offset commits are independent of state commits
- If app crashes after processing but before committing offsets:
  - Records are reprocessed
  - State may be duplicated
  - Output may have duplicates
- **Guarantee**: Every record processed at least once

**Exactly-Once Semantics** (EOS):
- Uses Kafka transactions to atomically:
  1. Produce output records
  2. Produce changelog records
  3. Commit consumer offsets
- All three are in the same transaction
- If any step fails, all are rolled back
- **Guarantee**: Every record processed exactly once, end-to-end

**Configuration**:
```cpp
Properties config;
config["processing.guarantee"] = "exactly_once";  // or "at_least_once"
config["application.id"] = "my-app";  // Required for EOS
```

**Implementation Requirements**:
1. Idempotent producer (PID, sequence numbers)
2. Transaction coordinator in broker
3. Transactional producer API
4. Consumer isolation level: `read_committed`

**EOS Processing Loop**:
```cpp
while (running) {
    auto records = consumer.poll(100ms);
    if (records.empty()) continue;
    
    producer.beginTransaction();
    
    try {
        for (auto& record : records) {
            auto result = processRecord(record);
            producer.send(result.topic, result.key, result.value);
            
            // State store writes go to changelog (also transactional)
            stateStore.put(record.key, newValue);
        }
        
        // Commit offsets as part of transaction
        producer.sendOffsetsToTransaction(consumer.position(), consumer.groupId());
        producer.commitTransaction();
        
    } catch (...) {
        producer.abortTransaction();
        throw;
    }
}
```

---

## 3. DSL Design

### 3.1 StreamsBuilder API

**High-Level Usage**:
```cpp
StreamsBuilder builder;

// Create stream from topic
auto stream = builder.stream<std::string, std::string>("input-topic");

// Transform
auto filtered = stream.filter([](auto key, auto value) {
    return value.length() > 5;
});

auto mapped = filtered.mapValues([](auto value) {
    return std::toupper(value[0]) + value.substr(1);
});

// Write to topic
mapped.to("output-topic");

// Build topology
Topology topology = builder.build();

// Run
Properties config;
config["application.id"] = "my-app";
config["bootstrap.servers"] = "localhost:9092";

KafkaStreams streams(topology, config);
streams.start();
```

**Chaining**:
```cpp
builder.stream<std::string, std::string>("input")
    .filter([](auto k, auto v) { return v.size() > 0; })
    .mapValues([](auto v) { return v.size(); })
    .groupByKey()
    .count()
    .toStream()
    .to("output");
```

### 3.2 Type Safety with Templates

**Typed Transformations**:
```cpp
// KStream<std::string, std::string>
auto stream1 = builder.stream<std::string, std::string>("input");

// KStream<std::string, int>
auto stream2 = stream1.mapValues<int>([](const std::string& v) {
    return static_cast<int>(v.size());
});

// Compile error if types don't match
// stream2.filter([](auto k, std::string v) { ... });  // ERROR
```

**Serde Inference**:
```cpp
// Serdes deduced from template parameters
auto stream = builder.stream<std::string, User>("users");
// Automatically uses: Serdes::String(), Serdes::Json<User>()
```

### 3.3 State Store Builder

**Creating Stores**:
```cpp
auto storeBuilder = Stores::keyValueStoreBuilder<std::string, int>(
    "counts-store",
    Serdes::String(),
    Serdes::Integer()
);

topology.addStateStore(storeBuilder, {"word-count-processor"});
```

**Accessing Stores in Processors**:
```cpp
class WordCountProcessor : public Processor<std::string, std::string> {
    KeyValueStore<std::string, int>* store;
    
    void init(ProcessorContext context) override {
        store = context.getStateStore<std::string, int>("counts-store");
    }
    
    void process(const std::string& key, const std::string& value) override {
        int count = store->get(key).value_or(0);
        store->put(key, count + 1);
        context.forward(key, count + 1);
    }
};
```

---

## 4. Windowing & Time Semantics

### 4.1 Time Extraction

**Timestamp Sources**:
1. **Event Time**: Embedded in record (e.g., `timestamp` field)
2. **Ingestion Time**: Broker append timestamp
3. **Processing Time**: Wall clock when processed

**TimestampExtractor**:
```cpp
class TimestampExtractor {
public:
    virtual int64_t extract(const ConsumerRecord& record, 
                            int64_t previousTimestamp) = 0;
};

// Event time: use record timestamp
class FailOnInvalidTimestamp : public TimestampExtractor {
    int64_t extract(const ConsumerRecord& record, int64_t prev) override {
        if (record.timestamp < 0) {
            throw std::runtime_error("Invalid timestamp");
        }
        return record.timestamp;
    }
};

// Processing time: wall clock
class WallclockTimestampExtractor : public TimestampExtractor {
    int64_t extract(const ConsumerRecord& record, int64_t prev) override {
        return std::chrono::system_clock::now().time_since_epoch().count();
    }
};
```

### 4.2 Windowing Types

**Tumbling Windows** (fixed, non-overlapping):
```
Size: 5 seconds
Timeline: |--W1--|--W2--|--W3--|--W4--|
Records:  [  A  ][  B  ][  C  ][  D  ]
```

**Hopping Windows** (fixed, overlapping):
```
Size: 10 seconds, Advance: 5 seconds
Timeline: |----W1----|
          |    |----W2----|
          |    |    |----W3----|
Records:  [ A  ][ B  ][ C  ]
```

**Session Windows** (dynamic, gap-based):
```
Gap: 3 seconds
Timeline: |--A--|...|--B--C--|.....|--D--|
Windows:  [Session1]  [Session2]  [Session3]
```

**API**:
```cpp
// Tumbling
auto windows = TimeWindows::of(std::chrono::seconds(5));

// Hopping
auto windows = TimeWindows::of(std::chrono::seconds(10))
                           .advanceBy(std::chrono::seconds(5));

// Session
auto windows = SessionWindows::with(std::chrono::seconds(30));

// Usage
stream.groupByKey()
      .windowedBy(TimeWindows::of(5s))
      .count()
      .toStream()
      .to("windowed-counts");
```

### 4.3 Punctuation

**Periodic Actions**: Execute logic at regular intervals

**Types**:
1. **STREAM_TIME**: Triggered when stream time advances
2. **WALL_CLOCK_TIME**: Triggered at wall clock intervals

**Example**:
```cpp
class MyProcessor : public Processor<std::string, int> {
    void init(ProcessorContext context) override {
        // Every 10 seconds of stream time
        context.schedule(10s, STREAM_TIME, [this](int64_t timestamp) {
            flushAggregates(timestamp);
        });
        
        // Every 1 minute of wall clock time
        context.schedule(1min, WALL_CLOCK_TIME, [this](int64_t timestamp) {
            emitMetrics(timestamp);
        });
    }
};
```

---

## 5. Implementation Phases

### Phase 1: Foundation (Week 17)
- ✅ Architecture document
- ✅ Core abstractions: Topology, Node, Task
- ✅ StreamsBuilder and KStream skeleton
- ✅ Task assignment algorithm

### Phase 2: Stateless Operations (Week 18)
- Filter, map, flatMap, peek
- Branching and merging
- Terminal operations (to, foreach)
- Integration tests

### Phase 3: State Stores (Week 19)
- KeyValueStore interface
- RocksDB backend
- Changelog topic integration
- Stateful operations: groupBy, aggregate, reduce, count

### Phase 4: Windowing (Week 20)
- TimeWindows, SessionWindows
- WindowStore implementation
- Punctuation API
- Windowed aggregations

### Phase 5: Joins (Week 21)
- KTable implementation
- Stream-stream joins
- Stream-table joins
- Table-table joins

### Phase 6: Advanced (Week 22)
- Processor API
- Serialization framework
- Exactly-once semantics
- Comprehensive testing

---

## 6. Performance Considerations

### 6.1 State Store Optimization
- Batch writes to RocksDB
- Bloom filters for fast lookups
- Compaction during off-peak hours
- Memory-mapped files for reading

### 6.2 Network Efficiency
- Batch produce to changelog topics
- Compression (Snappy default)
- Prefetch from input topics
- Async commits

### 6.3 Memory Management
- Bounded buffers for task queues
- Lazy deserialization
- State store caches (LRU)
- Streaming iterators (avoid loading all data)

### 6.4 Scalability
- Horizontal: More tasks, more threads
- Vertical: More memory for caches, larger RocksDB buffers
- Standby tasks for fast failover

---

## 7. Testing Strategy

### 7.1 Unit Tests
- Topology validation
- State store operations
- Serde round-trip
- Windowing logic

### 7.2 Integration Tests
- End-to-end pipelines
- State restoration
- Rebalancing behavior
- Failure scenarios

### 7.3 Compatibility Tests
- Compare output with Kafka Streams
- Same topology, same data
- Assert identical results

---

## 8. Open Questions & Future Work

1. **Global State Stores**: Replicate entire store to all instances (for enrichment)
2. **Interactive Queries**: Query state stores from external apps (RPC)
3. **Standby Replicas**: Minimize failover time
4. **Custom Partitioners**: Control task assignment
5. **Metrics & Monitoring**: Expose lag, throughput, error rates
6. **Schema Registry Integration**: Avro/Protobuf support
7. **Exactly-Once Optimization**: Reduce transaction overhead

---

## 9. References

- [Kafka Streams Documentation](https://kafka.apache.org/documentation/streams/)
- [KIP-28: Exactly Once Semantics](https://cwiki.apache.org/confluence/display/KAFKA/KIP-28+-+Add+a+processor+client)
- [State Store Internals](https://kafka.apache.org/28/documentation/streams/architecture)
- Kawasan `docs/ARCHITECTURE.md`
- Kawasan `docs/RAFT_PROTOCOL.md`

---

**End of Document**
