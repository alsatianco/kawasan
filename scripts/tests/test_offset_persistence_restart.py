#!/usr/bin/env python3
"""
Enhanced offset persistence test: produce messages, consume, commit, restart broker, verify.
This test validates that consumer offsets survive broker restarts (Milestone 3.1).
"""

import os
import signal
import subprocess
import sys
import time
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic

TOPIC = 'offset-persist-test'
GROUP = 'offset-persist-group'
BOOTSTRAP = 'localhost:9092'
BROKER_LOG_DIR = '/tmp/kawasan-offset-persist-test'

def start_broker():
    """Start the Kawasan broker in the background."""
    print("🚀 Starting Kawasan broker...")
    
    # Clean old logs but preserve data directory to test persistence
    if not os.path.exists(BROKER_LOG_DIR):
        os.makedirs(BROKER_LOG_DIR)
    
    # Start broker with explicit log directory
    broker_proc = subprocess.Popen(
        ['./build/tools/kawasan-broker', 
         '--broker-id', '0',
         '--host', 'localhost',
         '--port', '9092',
         '--log-dir', BROKER_LOG_DIR],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    
    # Wait for broker to be ready
    max_attempts = 30
    for attempt in range(max_attempts):
        try:
            # Try to connect
            admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP, request_timeout_ms=1000)
            admin.close()
            print(f"✓ Broker started (PID: {broker_proc.pid})")
            return broker_proc
        except Exception:
            if attempt == max_attempts - 1:
                print("❌ Broker failed to start!")
                broker_proc.kill()
                return None
            time.sleep(0.5)
    
    return broker_proc

def stop_broker(broker_proc):
    """Stop the Kawasan broker gracefully."""
    if broker_proc:
        print(f"🛑 Stopping broker (PID: {broker_proc.pid})...")
        broker_proc.send_signal(signal.SIGTERM)
        try:
            broker_proc.wait(timeout=5)
            print("✓ Broker stopped gracefully")
        except subprocess.TimeoutExpired:
            print("⚠️  Broker didn't stop gracefully, killing...")
            broker_proc.kill()
            broker_proc.wait()

def cleanup():
    """Clean up test data."""
    if os.path.exists(BROKER_LOG_DIR):
        import shutil
        shutil.rmtree(BROKER_LOG_DIR)
        print(f"🗑️  Cleaned up {BROKER_LOG_DIR}")

def test_offset_persistence_across_restart():
    """Main test: commit offsets, restart broker, verify persistence."""
    
    print("\n" + "="*60)
    print("MILESTONE 3.1: Offset Persistence Test")
    print("="*60 + "\n")
    
    # Clean up from previous runs
    cleanup()
    
    # Phase 1: Start broker, produce, consume, commit
    print("📝 PHASE 1: Initial offset commit")
    print("-" * 60)
    
    broker = start_broker()
    if not broker:
        print("❌ FAILED: Could not start broker")
        return False
    
    try:
        time.sleep(2)  # Let broker fully initialize
        
        # Create topic
        print("Creating topic...")
        admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP)
        try:
            admin.create_topics([NewTopic(TOPIC, num_partitions=1, replication_factor=1)])
            print(f"✓ Created topic: {TOPIC}")
        except Exception as e:
            print(f"Topic may already exist: {e}")
        finally:
            admin.close()
        
        # Produce 10 messages
        print("\nProducing 10 messages...")
        producer = KafkaProducer(bootstrap_servers=BOOTSTRAP)
        for i in range(10):
            producer.send(TOPIC, value=f"Message-{i}".encode())
        producer.flush()
        producer.close()
        print("✓ Produced 10 messages")
        
        # Consume and commit
        print("\nConsuming messages...")
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=BOOTSTRAP,
            group_id=GROUP,
            auto_offset_reset='earliest',
            enable_auto_commit=False,  # Manual commit for explicit control
            consumer_timeout_ms=5000,
        )
        
        messages = []
        for msg in consumer:
            messages.append(msg.offset)
            print(f"  Consumed offset={msg.offset}, value={msg.value.decode()}")
        
        print(f"✓ Consumed {len(messages)} messages")
        
        # Commit offset
        print("\nCommitting offsets...")
        consumer.commit()
        print("✓ Offsets committed")
        
        # Get committed offset
        from kafka import TopicPartition
        tp = TopicPartition(TOPIC, 0)
        committed = consumer.committed(tp)
        print(f"📌 Committed offset: {committed}")
        
        consumer.close()
        
        if committed is None or committed != 10:
            print(f"❌ FAILED: Expected committed offset 10, got {committed}")
            return False
            
    finally:
        stop_broker(broker)
    
    print("\n⏳ Waiting 2 seconds before restart...\n")
    time.sleep(2)
    
    # Phase 2: Restart broker, verify offset persisted
    print("📝 PHASE 2: Verify offset after restart")
    print("-" * 60)
    
    broker = start_broker()
    if not broker:
        print("❌ FAILED: Could not restart broker")
        return False
    
    try:
        time.sleep(2)  # Let broker fully initialize
        
        # Create new consumer with same group
        print("Creating new consumer with same group ID...")
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=BOOTSTRAP,
            group_id=GROUP,
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            consumer_timeout_ms=3000,
        )
        
        # Check committed offset
        from kafka import TopicPartition
        tp = TopicPartition(TOPIC, 0)
        committed_after_restart = consumer.committed(tp)
        
        print(f"📌 Committed offset after restart: {committed_after_restart}")
        
        # Try to consume - should get no messages if offset was persisted
        print("\nTrying to consume (should get nothing)...")
        messages_after_restart = []
        for msg in consumer:
            messages_after_restart.append(msg.offset)
            print(f"  ⚠️  Unexpected message: offset={msg.offset}, value={msg.value.decode()}")
        
        consumer.close()
        
        # Verify results
        print("\n" + "="*60)
        print("RESULTS")
        print("="*60)
        print(f"Committed offset before restart: 10")
        print(f"Committed offset after restart:  {committed_after_restart}")
        print(f"Messages consumed after restart: {len(messages_after_restart)}")
        
        if committed_after_restart == 10 and len(messages_after_restart) == 0:
            print("\n✅ SUCCESS: Offsets persisted across broker restart!")
            return True
        else:
            print("\n❌ FAILURE: Offsets NOT persisted!")
            if committed_after_restart != 10:
                print(f"   - Expected committed offset 10, got {committed_after_restart}")
            if len(messages_after_restart) > 0:
                print(f"   - Expected 0 messages, got {len(messages_after_restart)}")
            return False
            
    finally:
        stop_broker(broker)
        # Keep data directory for debugging if test failed
        # cleanup()

if __name__ == '__main__':
    try:
        success = test_offset_persistence_across_restart()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\n⚠️  Test interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ Test failed with exception: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
