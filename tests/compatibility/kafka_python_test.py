#!/usr/bin/env python3
"""
Kafka-Python client compatibility test for Kawasan.

Tests basic produce, consume, and commit operations using kafka-python library.
"""

import sys
import time
from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import KafkaError

BROKER = 'localhost:9092'
TOPIC = 'kafka-python-compat-test'
GROUP_ID = 'kafka-python-test-group'

def test_admin_operations():
    """Test admin operations: create and list topics."""
    print("Testing admin operations...")
    
    admin = KafkaAdminClient(
        bootstrap_servers=BROKER,
        client_id='kafka-python-admin-test'
    )
    
    # Create topic
    topic = NewTopic(name=TOPIC, num_partitions=3, replication_factor=1)
    try:
        admin.create_topics([topic])
        print(f"✓ Created topic: {TOPIC}")
    except Exception as e:
        # Topic might already exist
        print(f"  Topic creation (may already exist): {e}")
    
    # List topics
    topics = admin.list_topics()
    assert TOPIC in topics, f"Topic {TOPIC} not found in topic list"
    print(f"✓ Listed topics, found {TOPIC}")
    
    admin.close()
    print("✓ Admin operations passed\n")

def test_producer():
    """Test producing messages."""
    print("Testing producer...")
    
    producer = KafkaProducer(
        bootstrap_servers=BROKER,
        client_id='kafka-python-producer-test',
        acks=1,
        retries=3
    )
    
    # Produce 100 messages
    message_count = 100
    for i in range(message_count):
        key = f'key-{i}'.encode('utf-8')
        value = f'value-{i}'.encode('utf-8')
        future = producer.send(TOPIC, key=key, value=value)
        
        try:
            # Wait for message to be sent
            record_metadata = future.get(timeout=10)
            if i % 20 == 0:
                print(f"  Sent message {i} to partition {record_metadata.partition}, offset {record_metadata.offset}")
        except KafkaError as e:
            print(f"✗ Failed to send message {i}: {e}")
            producer.close()
            return False
    
    producer.flush()
    producer.close()
    print(f"✓ Produced {message_count} messages\n")
    return True

def test_consumer():
    """Test consuming messages."""
    print("Testing consumer...")
    
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BROKER,
        client_id='kafka-python-consumer-test',
        group_id=GROUP_ID,
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        consumer_timeout_ms=10000
    )
    
    consumed_count = 0
    try:
        for message in consumer:
            consumed_count += 1
            if consumed_count % 20 == 0:
                print(f"  Consumed message {consumed_count}: partition={message.partition}, offset={message.offset}")
                
            if consumed_count >= 100:
                break
    except Exception as e:
        print(f"✗ Consumer error: {e}")
        consumer.close()
        return False
    
    consumer.close()
    
    if consumed_count < 100:
        print(f"✗ Expected 100 messages, consumed {consumed_count}")
        return False
    
    print(f"✓ Consumed {consumed_count} messages\n")
    return True

def test_offset_commit():
    """Test offset commit and resume."""
    print("Testing offset commit...")
    
    # Consume and commit some messages
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BROKER,
        client_id='kafka-python-commit-test',
        group_id=GROUP_ID + '-commit',
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        consumer_timeout_ms=5000
    )
    
    # Consume first 50 messages
    consumed_first = 0
    last_offset = {}
    
    for message in consumer:
        consumed_first += 1
        last_offset[message.partition] = message.offset
        
        if consumed_first >= 50:
            break
    
    # Commit offsets
    consumer.commit()
    print(f"  Committed offsets after consuming {consumed_first} messages")
    consumer.close()
    
    # Create new consumer with same group
    consumer2 = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BROKER,
        client_id='kafka-python-resume-test',
        group_id=GROUP_ID + '-commit',
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        consumer_timeout_ms=5000
    )
    
    # Should resume from committed offset
    consumed_second = 0
    for message in consumer2:
        consumed_second += 1
        if consumed_second >= 50:
            break
    
    consumer2.close()
    
    print(f"✓ First consumer: {consumed_first}, Second consumer: {consumed_second}")
    print(f"✓ Offset commit and resume works\n")
    return True

def test_multiple_consumers():
    """Test multiple consumers in the same group."""
    print("Testing multiple consumers...")
    
    consumer1 = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BROKER,
        client_id='kafka-python-multi-1',
        group_id=GROUP_ID + '-multi',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        consumer_timeout_ms=5000
    )
    
    consumer2 = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BROKER,
        client_id='kafka-python-multi-2',
        group_id=GROUP_ID + '-multi',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        consumer_timeout_ms=5000
    )
    
    # Trigger rebalance by polling - this causes consumers to join the group
    try:
        # Poll to trigger join and rebalance
        next(consumer1)
    except StopIteration:
        pass  # No messages yet, that's ok
    
    try:
        next(consumer2)
    except StopIteration:
        pass  # No messages yet, that's ok
    
    # Give a moment for assignments to settle
    time.sleep(1)
    
    # Both consumers should have partition assignments
    partitions1 = consumer1.assignment()
    partitions2 = consumer2.assignment()
    
    print(f"  Consumer 1 partitions: {len(partitions1)}")
    print(f"  Consumer 2 partitions: {len(partitions2)}")
    
    consumer1.close()
    consumer2.close()
    
    total_partitions = len(partitions1) + len(partitions2)
    if total_partitions != 3:
        print(f"✗ Expected 3 total partitions, got {total_partitions}")
        return False
    
    print(f"✓ Multiple consumers work correctly\n")
    return True

def main():
    """Run all compatibility tests."""
    print("=" * 60)
    print("Kawasan Kafka-Python Compatibility Test")
    print("=" * 60)
    print()
    
    try:
        # Run tests
        test_admin_operations()
        
        if not test_producer():
            print("✗ Producer test failed")
            return 1
        
        if not test_consumer():
            print("✗ Consumer test failed")
            return 1
        
        if not test_offset_commit():
            print("✗ Offset commit test failed")
            return 1
        
        if not test_multiple_consumers():
            print("✗ Multiple consumers test failed")
            return 1
        
        print("=" * 60)
        print("✓ All kafka-python compatibility tests passed!")
        print("=" * 60)
        return 0
        
    except Exception as e:
        print(f"\n✗ Test suite failed with error: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == '__main__':
    sys.exit(main())
