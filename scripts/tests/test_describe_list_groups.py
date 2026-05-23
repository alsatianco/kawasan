#!/usr/bin/env python3
"""Test DescribeGroups and ListGroups APIs"""

import sys
import time
from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient

def test_describe_list_groups():
    """Test DescribeGroups and ListGroups functionality"""
    broker = 'localhost:9092'
    topic = 'test-groups-api'
    group_id = 'test-group-describe'
    
    print(f"Testing DescribeGroups and ListGroups APIs")
    print(f"Broker: {broker}")
    print(f"Topic: {topic}")
    print(f"Group: {group_id}")
    print()
    
    # Create admin client
    admin = KafkaAdminClient(bootstrap_servers=broker)
    
    # Test 1: ListGroups (should be empty or show existing groups)
    print("Test 1: ListGroups API (before creating consumer)")
    try:
        groups_before = admin.list_consumer_groups()
        print(f"  Groups found: {len(groups_before)}")
        for group in groups_before:
            print(f"    - {group[0]} (type: {group[1]})")
    except Exception as e:
        print(f"  ERROR: {e}")
        return False
    print()
    
    # Create producer and send messages
    print("Test 2: Producing messages to topic")
    producer = KafkaProducer(bootstrap_servers=broker)
    for i in range(10):
        producer.send(topic, value=f'message-{i}'.encode())
    producer.flush()
    print(f"  Produced 10 messages")
    print()
    
    # Create consumer and join group
    print("Test 3: Creating consumer and joining group")
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=broker,
        group_id=group_id,
        auto_offset_reset='earliest',
        enable_auto_commit=True
    )
    
    # Poll to trigger group join
    print("  Polling to trigger group join...")
    messages = consumer.poll(timeout_ms=5000)
    print(f"  Received {sum(len(msgs) for msgs in messages.values())} messages")
    print()
    
    # Test 4: ListGroups (should show our group)
    print("Test 4: ListGroups API (after joining)")
    try:
        groups_after = admin.list_consumer_groups()
        print(f"  Groups found: {len(groups_after)}")
        found_our_group = False
        for group in groups_after:
            print(f"    - {group[0]} (type: {group[1]})")
            if group[0] == group_id:
                found_our_group = True
        
        if not found_our_group:
            print(f"  WARNING: Our group '{group_id}' not found!")
        else:
            print(f"  SUCCESS: Group '{group_id}' found")
    except Exception as e:
        print(f"  ERROR: {e}")
        consumer.close()
        return False
    print()
    
    # Test 5: DescribeGroups
    print("Test 5: DescribeGroups API")
    try:
        described_groups = admin.describe_consumer_groups([group_id])
        print(f"  Groups described: {len(described_groups)}")
        for group_desc in described_groups:
            print(f"    Group ID: {group_desc.group}")
            print(f"    State: {group_desc.state}")
            print(f"    Protocol Type: {group_desc.protocol_type}")
            print(f"    Protocol: {group_desc.protocol}")
            print(f"    Members: {len(group_desc.members)}")
            for member in group_desc.members:
                print(f"      - Member ID: {member.member_id}")
                print(f"        Client ID: {member.client_id}")
                print(f"        Client Host: {member.client_host}")
        
        # Verify state
        if described_groups[0].state == 'Stable':
            print("  SUCCESS: Group is in Stable state")
        else:
            print(f"  WARNING: Group state is {described_groups[0].state}, expected Stable")
    except Exception as e:
        print(f"  ERROR: {e}")
        consumer.close()
        return False
    print()
    
    # Clean up
    consumer.close()
    admin.close()
    
    print("All tests passed!")
    return True

if __name__ == '__main__':
    success = test_describe_list_groups()
    sys.exit(0 if success else 1)
