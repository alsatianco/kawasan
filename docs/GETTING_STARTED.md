# Getting Started with Kawasan

## Prerequisites

### System Requirements

- **Operating System**: Linux (Ubuntu 20.04+, CentOS 8+), macOS 11+, or Windows with WSL2
- **CPU**: 2+ cores recommended
- **Memory**: 4GB+ RAM
- **Disk**: 10GB+ free space

### Development Tools

- **C++ Compiler**: 
  - GCC 11+ 
  - Clang 13+
  - MSVC 19.29+ (Windows)
- **CMake**: 3.20 or later
- **Build Tools**: make, ninja, or Visual Studio

### Dependencies

#### Using Package Manager (Ubuntu/Debian)

```bash
sudo apt-get update
sudo apt-get install -y \
    build-essential \
    cmake \
    git \
    libboost-all-dev \
    librocksdb-dev \
    libspdlog-dev \
    nlohmann-json3-dev \
    libgtest-dev \
    libssl-dev \
    zlib1g-dev \
    libsnappy-dev \
    liblz4-dev \
    libzstd-dev
```

#### Using vcpkg (Cross-platform)

```bash
# Install vcpkg
git clone https://github.com/Microsoft/vcpkg.git
./vcpkg/bootstrap-vcpkg.sh  # or .bat on Windows

# Dependencies will be installed automatically via vcpkg.json
```

#### Using Homebrew (macOS)

```bash
brew install cmake boost rocksdb spdlog nlohmann-json googletest openssl zlib snappy lz4 zstd
```

## Building Kawasan

### Quick Build

```bash
# Clone the repository
git clone https://github.com/kawasan/kawasan.git
cd kawasan

# Create build directory
mkdir build && cd build

# Configure (with vcpkg)
cmake -DCMAKE_TOOLCHAIN_FILE=<path-to-vcpkg>/scripts/buildsystems/vcpkg.cmake ..

# Or configure (with system packages)
cmake ..

# Build
cmake --build . -j$(nproc)

# Run tests
ctest --output-on-failure

# Install
sudo cmake --install .
```

### Build Options

```bash
cmake \
  -DCMAKE_BUILD_TYPE=Release \           # Release, Debug, RelWithDebInfo
  -DKAWASAN_BUILD_TESTS=ON \              # Build unit tests
  -DKAWASAN_BUILD_TOOLS=ON \              # Build CLI tools
  -DKAWASAN_BUILD_EXAMPLES=ON \           # Build examples
  -DKAWASAN_ENABLE_ASAN=OFF \             # Address Sanitizer
  -DKAWASAN_ENABLE_TSAN=OFF \             # Thread Sanitizer
  ..
```

### Docker Build

```bash
# Build Docker image
docker build -t kawasan:latest .

# Run with docker-compose
docker-compose up -d

# View logs
docker-compose logs -f
```

## Running Your First Broker

### Single Broker Setup

1. **Create configuration file:**

```bash
cat > server.properties << EOF
{
  "broker.id": 0,
  "host": "localhost",
  "port": 9092,
  "log.dirs": "/tmp/kawasan-logs",
  "log.segment.bytes": 1073741824,
  "log.retention.hours": 168
}
EOF
```

2. **Start the broker:**

```bash
./build/tools/kawasan-broker --config server.properties --log-level info
```

3. **Verify broker is running:**

```bash
# Check logs
tail -f /tmp/kawasan-logs/*.log

# Or check port
netstat -an | grep 9092
```

### Multi-Broker Cluster

1. **Create configurations for 3 brokers:**

```bash
# Broker 1
cat > broker-1.properties << EOF
{
  "broker.id": 0,
  "host": "localhost",
  "port": 9092,
  "log.dirs": "/tmp/kawasan-logs-1"
}
EOF

# Broker 2
cat > broker-2.properties << EOF
{
  "broker.id": 1,
  "host": "localhost",
  "port": 9093,
  "log.dirs": "/tmp/kawasan-logs-2"
}
EOF

# Broker 3
cat > broker-3.properties << EOF
{
  "broker.id": 2,
  "host": "localhost",
  "port": 9094,
  "log.dirs": "/tmp/kawasan-logs-3"
}
EOF
```

2. **Start all brokers:**

```bash
./build/tools/kawasan-broker --config broker-1.properties &
./build/tools/kawasan-broker --config broker-2.properties &
./build/tools/kawasan-broker --config broker-3.properties &
```

## Using Kafka Clients

Kawasan is wire-protocol compatible with Apache Kafka. You can use any Kafka client!

### Python (kafka-python)

```bash
pip install kafka-python
```

```python
from kafka import KafkaProducer, KafkaConsumer

# Producer
producer = KafkaProducer(bootstrap_servers='localhost:9092')
producer.send('my-topic', b'Hello, Kawasan!')
producer.flush()

# Consumer
consumer = KafkaConsumer('my-topic', bootstrap_servers='localhost:9092')
for message in consumer:
    print(message.value)
```

### Java (Kafka Client)

```xml
<!-- pom.xml -->
<dependency>
    <groupId>org.apache.kafka</groupId>
    <artifactId>kafka-clients</artifactId>
    <version>3.4.0</version>
</dependency>
```

```java
import org.apache.kafka.clients.producer.*;
import java.util.Properties;

Properties props = new Properties();
props.put("bootstrap.servers", "localhost:9092");
props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

Producer<String, String> producer = new KafkaProducer<>(props);
producer.send(new ProducerRecord<>("my-topic", "key", "Hello, Kawasan!"));
producer.close();
```

### Node.js (kafkajs)

```bash
npm install kafkajs
```

```javascript
const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['localhost:9092']
});

const producer = kafka.producer();
await producer.connect();
await producer.send({
  topic: 'my-topic',
  messages: [{ value: 'Hello, Kawasan!' }]
});
```

### Go (sarama)

```bash
go get github.com/Shopify/sarama
```

```go
package main

import (
    "github.com/Shopify/sarama"
)

func main() {
    config := sarama.NewConfig()
    producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, config)
    
    msg := &sarama.ProducerMessage{
        Topic: "my-topic",
        Value: sarama.StringEncoder("Hello, Kawasan!"),
    }
    
    partition, offset, err := producer.SendMessage(msg)
}
```

## Using Kawasan C++ Client

```cpp
#include <kawasan/client/producer.h>
#include <kawasan/client/consumer.h>

// Producer example
kawasan::client::ProducerConfig producer_config;
producer_config.bootstrap_servers = "localhost:9092";

kawasan::client::Producer producer(producer_config);
auto future = producer.send("my-topic", "key", "value");
auto metadata = future.get();

std::cout << "Sent to offset: " << metadata.offset << std::endl;

// Consumer example
kawasan::client::ConsumerConfig consumer_config;
consumer_config.bootstrap_servers = "localhost:9092";
consumer_config.group_id = "my-group";

kawasan::client::Consumer consumer(consumer_config);
consumer.subscribe({"my-topic"});

while (true) {
    auto records = consumer.poll(std::chrono::milliseconds(100));
    for (const auto& record : records) {
        std::cout << "Received: " << record.value() << std::endl;
    }
}
```

## Command-Line Tools

### Using Kafka CLI Tools

Since Kawasan is protocol-compatible, you can use official Kafka tools:

```bash
# Download Kafka tools
wget https://archive.apache.org/dist/kafka/3.4.0/kafka_2.13-3.4.0.tgz
tar -xzf kafka_2.13-3.4.0.tgz
cd kafka_2.13-3.4.0

# Create topic
bin/kafka-topics.sh --create --topic my-topic \
    --partitions 3 --replication-factor 1 \
    --bootstrap-server localhost:9092

# List topics
bin/kafka-topics.sh --list --bootstrap-server localhost:9092

# Produce messages
echo "Hello Kawasan" | bin/kafka-console-producer.sh \
    --topic my-topic --bootstrap-server localhost:9092

# Consume messages
bin/kafka-console-consumer.sh --topic my-topic \
    --from-beginning --bootstrap-server localhost:9092

# Describe topic
bin/kafka-topics.sh --describe --topic my-topic \
    --bootstrap-server localhost:9092

# Consumer groups
bin/kafka-consumer-groups.sh --list --bootstrap-server localhost:9092
```

## Configuration Guide

### Broker Configuration

Key settings in `server.properties`:

```json
{
  "broker.id": 0,                          // Unique broker ID
  "host": "localhost",                     // Broker host
  "port": 9092,                           // Broker port
  "log.dirs": "/var/lib/kawasan/data",     // Data directory
  "log.segment.bytes": 1073741824,        // 1GB segments
  "log.retention.hours": 168,             // 7 days
  "log.retention.bytes": -1,              // Unlimited
  "num.partitions": 3,                    // Default partitions
  "replication.factor": 3                 // Default replication
}
```

### Producer Configuration

```cpp
ProducerConfig config;
config.bootstrap_servers = "localhost:9092";
config.acks = -1;                           // Wait for all replicas
config.timeout_ms = 30000;                  // 30 seconds
config.batch_size = 16384;                  // 16KB batches
config.compression = CompressionType::LZ4;  // Compression
```

### Consumer Configuration

```cpp
ConsumerConfig config;
config.bootstrap_servers = "localhost:9092";
config.group_id = "my-consumer-group";
config.auto_offset_reset = "earliest";      // Start from beginning
config.enable_auto_commit = true;
config.auto_commit_interval_ms = 5000;
```

## Monitoring

### Logs

```bash
# Broker logs
tail -f /var/log/kawasan/broker.log

# Set log level
kawasan-broker --log-level debug
```

### Metrics

```bash
# View metrics (coming soon)
curl http://localhost:9092/metrics
```

## Troubleshooting

### Broker won't start

1. Check port availability: `netstat -an | grep 9092`
2. Check disk space: `df -h`
3. Check logs: `tail -f /tmp/kawasan-*.log`
4. Verify configuration: `cat server.properties`

### Connection refused

1. Verify broker is running: `ps aux | grep kawasan`
2. Check firewall: `sudo iptables -L`
3. Test connectivity: `telnet localhost 9092`

### Performance issues

1. Increase batch size for producer
2. Tune log segment size
3. Enable compression
4. Check disk I/O: `iostat -x 1`
5. Monitor CPU: `top`

## Next Steps

1. Read [Architecture Documentation](ARCHITECTURE.md)
2. Explore [Examples](../examples/)
3. Try [Advanced Features](ADVANCED.md)
4. Contribute: [CONTRIBUTING.md](../CONTRIBUTING.md)

## Support

- **Issues**: https://github.com/kawasan/kawasan/issues
- **Discussions**: https://github.com/kawasan/kawasan/discussions
- **Documentation**: https://kawasan.io/docs

## Learning Resources

- Apache Kafka Documentation
- Kafka: The Definitive Guide
- Raft Paper: "In Search of an Understandable Consensus Algorithm"
- RocksDB Documentation

