#!/usr/bin/env python3
"""
Simple single-threaded producer that sends messages every N seconds.
Usage: python3 simple_producer.py [--interval SECONDS] [--topic TOPIC] [--bootstrap SERVER]
"""

import time
import argparse
from kafka import KafkaProducer
from kafka.errors import KafkaError

def main():
    parser = argparse.ArgumentParser(description='Simple Kafka producer')
    parser.add_argument('--interval', type=int, default=2, help='Interval between messages in seconds (default: 2)')
    parser.add_argument('--topic', type=str, default='test-topic', help='Topic name (default: test-topic)')
    parser.add_argument('--bootstrap', type=str, default='localhost:9092', help='Bootstrap server (default: localhost:9092)')
    parser.add_argument('--count', type=int, default=0, help='Number of messages to send (0 = infinite, default: 0)')
    args = parser.parse_args()

    print(f"Starting producer...")
    print(f"  Topic: {args.topic}")
    print(f"  Interval: {args.interval}s")
    print(f"  Bootstrap: {args.bootstrap}")
    print(f"  Count: {'infinite' if args.count == 0 else args.count}")
    print()

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        value_serializer=lambda v: v.encode('utf-8')
    )

    message_num = 0
    try:
        while args.count == 0 or message_num < args.count:
            message = f"Message {message_num} at {time.strftime('%Y-%m-%d %H:%M:%S')}"
            
            try:
                future = producer.send(args.topic, value=message)
                record_metadata = future.get(timeout=10)
                print(f"✓ Sent: {message} -> partition={record_metadata.partition}, offset={record_metadata.offset}")
            except KafkaError as e:
                print(f"✗ Failed to send message {message_num}: {e}")
            
            message_num += 1
            
            if args.count == 0 or message_num < args.count:
                time.sleep(args.interval)
                
    except KeyboardInterrupt:
        print("\n\nShutting down producer...")
    finally:
        producer.flush()
        producer.close()
        print(f"Producer stopped. Sent {message_num} messages total.")

if __name__ == '__main__':
    main()
