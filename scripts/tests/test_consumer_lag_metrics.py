#!/usr/bin/env python3
"""Test consumer lag metrics"""

import sys
import time
from kafka import KafkaConsumer, KafkaProducer
import requests

def test_consumer_lag_metrics():
    """Test that consumer lag metrics appear in Prometheus endpoint"""
    broker = 'localhost:9092'
    metrics_url = 'http://localhost:9094/metrics'
    topic = 'test-lag-metrics'
    group_id = 'test-lag-group'
    
    print(f"Testing Consumer Lag Metrics")
    print(f"Broker: {broker}")
    print(f"Metrics URL: {metrics_url}")
    print(f"Topic: {topic}")
    print(f"Group: {group_id}")
    print()
    
    # Step 1: Produce 100 messages
    print("Step 1: Producing 100 messages...")
    producer = KafkaProducer(bootstrap_servers=broker)
    for i in range(100):
        producer.send(topic, value=f'message-{i}'.encode())
    producer.flush()
    producer.close()
    print("  ✓ Produced 100 messages")
    print()
    
    # Step 2: Consume 50 messages and commit
    print("Step 2: Consuming 50 messages and committing...")
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=broker,
        group_id=group_id,
        auto_offset_reset='earliest',
        enable_auto_commit=False
    )
    
    count = 0
    for message in consumer:
        count += 1
        if count == 50:
            consumer.commit()
            print(f"  ✓ Consumed 50 messages and committed offset")
            break
    
    consumer.close()
    print()
    
    # Step 3: Wait for lag computation thread to run (runs every 10 seconds)
    print("Step 3: Waiting 12 seconds for lag computation...")
    time.sleep(12)
    print("  ✓ Wait complete")
    print()
    
    # Step 4: Query metrics endpoint
    print("Step 4: Querying metrics endpoint...")
    response = requests.get(metrics_url)
    if response.status_code != 200:
        print(f"  ERROR: Metrics endpoint returned {response.status_code}")
        return False
    
    metrics_text = response.text
    print("  ✓ Metrics endpoint responded")
    print()
    
    # Step 5: Check for consumer lag metric
    print("Step 5: Checking for consumer_lag metric...")
    lag_lines = [line for line in metrics_text.split('\n') if 'kawasan_consumer_lag' in line and not line.startswith('#')]
    
    if not lag_lines:
        print("  ERROR: No consumer_lag metrics found!")
        print("  Available metrics:")
        for line in metrics_text.split('\n')[:20]:
            print(f"    {line}")
        return False
    
    print("  ✓ Found consumer_lag metrics:")
    for line in lag_lines:
        print(f"    {line}")
    
    # Verify lag value is approximately 50
    for line in lag_lines:
        if f'group="{group_id}"' in line and f'topic="{topic}"' in line:
            # Extract lag value (last token)
            lag_value = int(float(line.split()[-1]))
            print(f"\n  Lag value: {lag_value}")
            if 45 <= lag_value <= 55:  # Allow some tolerance
                print(f"  ✓ Lag is correct (expected ~50, got {lag_value})")
                return True
            else:
                print(f"  ERROR: Lag is incorrect (expected ~50, got {lag_value})")
                return False
    
    print("  ERROR: Could not find lag metric for our group and topic")
    return False

if __name__ == '__main__':
    try:
        success = test_consumer_lag_metrics()
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
