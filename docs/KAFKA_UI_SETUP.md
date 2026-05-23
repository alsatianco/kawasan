# Running Kawasan with Kafka UI

This guide explains how to configure Kawasan to work with Kafka UI and other Kafka admin tools.

## The Issue

Kafka clients need to discover brokers through the Metadata API. When a client connects, it:
1. Connects to the bootstrap server
2. Requests metadata to discover all brokers
3. Uses the broker addresses from metadata to connect directly

If the broker advertises an incorrect address (like `localhost` when running in Docker), clients can't connect.

## Solution

Configure Kawasan to advertise the correct host address based on your deployment scenario.

### Scenario 1: Kafka UI in Docker, Broker on Host (Recommended)

**Broker Configuration:**
```json
{
  "broker.id": 0,
  "host": "0.0.0.0",
  "advertised.host": "host.docker.internal",
  "port": 9092,
  "log.dirs": "/tmp/kawasan-logs"
}
```

Generate this config automatically with:

```bash
KAWASAN_PROFILE=kafka-ui bash scripts/reset.sh
```

**Start Kafka UI:**
```bash
docker-compose -f docker-compose-kafka-ui.yml up -d
```

Visit http://localhost:8088

### Scenario 2: Everything on Host (Local Development)

**Broker Configuration:**
```json
{
  "broker.id": 0,
  "host": "localhost",
  "port": 9092,
  "log.dirs": "/tmp/kawasan-logs"
}
```

No `advertised.host` needed - defaults to `host`.

**Run Kafka UI locally** (not in Docker):
```bash
java -jar kafka-ui.jar --spring.config.additional-location=kafka-ui-config.yml
```

### Scenario 3: Both in Docker

**Broker Configuration:**
```json
{
  "broker.id": 0,
  "host": "0.0.0.0",
  "advertised.host": "kawasan-broker",
  "port": 9092,
  "log.dirs": "/data/kawasan-logs"
}
```

**Docker Compose:**
```yaml
version: '3.8'

services:
  kawasan-broker:
    image: kawasan:latest
    container_name: kawasan-broker
    hostname: kawasan-broker
    ports:
      - "9092:9092"
    volumes:
      - kawasan-data:/data/kawasan-logs
    networks:
      - kafka-network

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    container_name: kafka-ui
    ports:
      - "8088:8080"
    environment:
      - KAFKA_CLUSTERS_0_NAME=kawasan
      - KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS=kawasan-broker:9092
    networks:
      - kafka-network
    depends_on:
      - kawasan-broker

networks:
  kafka-network:
    driver: bridge

volumes:
  kawasan-data:
```

## Quick Start Script Update

You can now start a Kafka-UI-friendly broker without hand-editing `/tmp/kawasan-server.properties`:

```bash
KAWASAN_PROFILE=kafka-ui \
KAWASAN_ADVERTISED_HOST=host.docker.internal \
bash scripts/reset.sh

# Then start the UI
docker-compose -f docker-compose-kafka-ui.yml up -d
```

The `kafka-ui` profile sets:
- `host=0.0.0.0` so the broker listens on all interfaces
- `advertised.host=host.docker.internal` by default (override with `KAWASAN_ADVERTISED_HOST`)
- `advertised.port` matches the listener port unless overridden via `KAWASAN_ADVERTISED_PORT`

## Verification

Test that the broker is advertising the correct address:

```bash
python3 scripts/debug_metadata.py localhost 9092
```

You should see:
```
Broker 0: id=0, host=host.docker.internal, port=9092, rack=None
```

## Troubleshooting

### "Timed out waiting for a node assignment"

This means the Kafka client can't connect to the advertised address. Check:

1. **Broker is advertising correct host:**
   ```bash
   python3 scripts/debug_metadata.py localhost 9092
   ```

2. **Network connectivity:**
   ```bash
   # From Docker container
   docker exec kafka-ui nc -zv host.docker.internal 9092
   ```

3. **Firewall rules:** Ensure port 9092 is open

### "Connection refused"

- Check broker is running: `ps aux | grep kawasan-broker`
- Check broker logs: `tail -f /tmp/kawasan-broker-logs/broker.log`
- Verify port binding: `lsof -i :9092`

### Kafka UI shows "offline"

1. Check Kafka UI logs: `docker logs kafka-ui`
2. Verify configuration in Kafka UI
3. Ensure broker is advertising reachable address
4. Test with command-line tools first:
   ```bash
   python3 scripts/test_kafka_ui_apis.py
   ```

## Configuration Reference

### Broker Config Options

- `host`: IP/hostname to bind to (use `0.0.0.0` to listen on all interfaces)
- `advertised.host`: Address clients should use to connect (defaults to `host`)
- `port`: Port to listen on
- `advertised.port`: Port clients should use (defaults to `port`)

### Why This Matters

Kafka's design requires brokers to advertise their own address. This is different from typical client-server protocols where the client just uses the address it connected to. This design enables:
- Multi-broker clusters where each broker has a different address
- Load balancing and partition-specific routing
- Clients to maintain direct connections to the broker hosting their partition leader

The tradeoff is that networking configuration must be correct for clients to connect.

## Additional Resources

- [Kafka UI Documentation](https://docs.kafka-ui.provectus.io/)
- [Kafka Protocol Guide](https://kafka.apache.org/protocol)
- [Docker Networking](https://docs.docker.com/network/)
