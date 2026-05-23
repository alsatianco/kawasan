#!/usr/bin/env python3
"""Capture SyncGroup request/response to debug the issue"""

import socket
import struct
import time
from kafka import KafkaConsumer
from kafka.protocol.group import JoinGroupRequest, SyncGroupRequest
from kafka.protocol.types import Int16, Int32, String, Bytes, Array, Schema
import threading

def capture_traffic():
    """Simple packet capture"""
    print("Starting traffic capture...")
    # This is a placeholder - we'll use tcpdump instead
    
if __name__ == '__main__':
    # Start consumer in background
    def run_consumer():
        consumer = KafkaConsumer(
            'test-topic',
            bootstrap_servers='localhost:9092',
            group_id='test-group-debug',
            auto_offset_reset='latest',
            enable_auto_commit=True
        )
        try:
            for msg in consumer:
                print(f"Received: {msg}")
        except Exception as e:
            print(f"Consumer error: {e}")
        finally:
            consumer.close()
    
    t = threading.Thread(target=run_consumer)
    t.daemon = True
    t.start()
    
    time.sleep(10)
