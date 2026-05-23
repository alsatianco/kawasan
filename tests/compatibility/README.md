# Client Compatibility Tests

This directory contains compatibility tests for various Kafka client libraries.

## Available Tests

### Python (kafka-python)
- **File**: `kafka_python_test.py`
- **Status**: ✅ Implemented
- **Setup**: `pip3 install kafka-python`
- **Run**: `python3 kafka_python_test.py` (with broker running)

### Java (Apache Kafka Client)
- **File**: `java_client_test.java`
- **Status**: ⏳ Placeholder (to be implemented)
- **Setup**: TBD
- **Run**: TBD

### Node.js (KafkaJS)
- **File**: `kafkajs_test.js`
- **Status**: ⏳ Placeholder (to be implemented)
- **Setup**: TBD
- **Run**: TBD

### Go (Sarama)
- **File**: `sarama_test.go`
- **Status**: ⏳ Placeholder (to be implemented)
- **Setup**: TBD
- **Run**: TBD

## Running All Tests

Use the compatibility test runner script:

```bash
cd /path/to/kawasan
./scripts/run_compatibility_tests.sh
```

This script will:
1. Start a Kawasan broker
2. Run all available client tests
3. Report results
4. Clean up

## Test Coverage

Each compatibility test should verify:
- [ ] Connection to broker
- [ ] Topic creation
- [ ] Message production (100+ messages)
- [ ] Message consumption (verify all messages)
- [ ] Offset commit
- [ ] Consumer group functionality

## Prerequisites

- Kawasan broker built and available at `build/tools/kawasan-broker`
- Client libraries installed for each test
- Python 3.6+ for Python tests
