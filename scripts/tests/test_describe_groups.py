#!/usr/bin/env python3
"""
Test script for DescribeGroups API
Verifies that the broker properly returns group state, members, and assignments
"""

import sys
import time
from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import KafkaError

BOOTSTRAP_SERVERS = 'localhost:9092'
TEST_TOPIC = 'test-describe-groups'
TEST_GROUP = 'test-consumer-group'

def test_describe_groups():
    """Test DescribeGroups API with a real consumer group"""
    print("=" * 60)
    print("Testing DescribeGroups API")
    print("=" * 60)
    
    # Create admin client
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
    
    # Create test topic
    try:
        topic = NewTopic(name=TEST_TOPIC, num_partitions=3, replication_factor=1)
        admin.create_topics([topic])
        print(f"✓ Created topic: {TEST_TOPIC}")
        time.sleep(1)  # Wait for topic creation
    except KafkaError as e:
        print(f"Topic may already exist: {e}")
    
    # Produce some test messages
    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)
    for i in range(10):
        producer.send(TEST_TOPIC, value=f"message-{i}".encode())
    producer.flush()
    print(f"✓ Produced 10 messages to {TEST_TOPIC}")
    
    # Create a consumer and join the group
    consumer = KafkaConsumer(
        TEST_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=TEST_GROUP,
        auto_offset_reset='earliest',
        enable_auto_commit=False
    )
    
    # Trigger group join by polling
    print(f"✓ Consumer joining group: {TEST_GROUP}")
    consumer.poll(timeout_ms=5000)
    
    # Now describe the group
    print(f"\nDescribing group: {TEST_GROUP}")
    try:
        groups = admin.describe_consumer_groups([TEST_GROUP])
        
        if not groups:
            print("✗ No groups returned!")
            return False
        
        group_info = groups[0]
        print(f"\nGroup Description:")
        print(f"  Group ID: {group_info.group_id}")
        print(f"  State: {group_info.state}")
        print(f"  Protocol Type: {group_info.protocol_type}")
        print(f"  Protocol: {group_info.protocol}")
        print(f"  Members: {len(group_info.members)}")
        
        for member in group_info.members:
            print(f"\n  Member:")
            print(f"    Member ID: {member.member_id}")
            print(f"    Client ID: {member.client_id}")
            print(f"    Client Host: {member.client_host}")
            print(f"    Assignment: {len(member.member_assignment)} bytes")
        
        # Verify basic properties
        assert group_info.group_id == TEST_GROUP, f"Expected group_id={TEST_GROUP}, got {group_info.group_id}"
        assert group_info.state in ['Stable', 'PreparingRebalance', 'CompletingRebalance'], f"Unexpected state: {group_info.state}"
        assert group_info.protocol_type == 'consumer', f"Expected protocol_type='consumer', got {group_info.protocol_type}"
        assert len(group_info.members) >= 1, f"Expected at least 1 member, got {len(group_info.members)}"
        
        print("\n✓ DescribeGroups API test PASSED")
        return True
        
    except Exception as e:
        print(f"\n✗ DescribeGroups API test FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        consumer.close()
        producer.close()
        admin.close()

if __name__ == '__main__':
    success = test_describe_groups()
    sys.exit(0 if success else 1)
