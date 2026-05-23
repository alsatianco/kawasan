#!/usr/bin/env python3
"""
Simple single-threaded consumer that reads messages from a topic.
Usage: python3 simple_consumer.py [--topic TOPIC] [--group GROUP] [--bootstrap SERVER]
"""

import argparse
from kafka import KafkaConsumer
from kafka.errors import KafkaError

def main():
    parser = argparse.ArgumentParser(description='Simple Kafka consumer')
    parser.add_argument('--topic', type=str, default='test-topic', help='Topic name (default: test-topic)')
    parser.add_argument('--group', type=str, default='simple-consumer-group', help='Consumer group (default: simple-consumer-group)')
    parser.add_argument('--bootstrap', type=str, default='localhost:9092', help='Bootstrap server (default: localhost:9092)')
    parser.add_argument('--offset', type=str, default='latest', choices=['earliest', 'latest'], 
                        help='Initial offset (default: latest)')
    parser.add_argument('--timeout', type=int, default=0, help='Stop after N seconds of inactivity (0 = never, default: 0)')
    args = parser.parse_args()

    print(f"Starting consumer...")
    print(f"  Topic: {args.topic}")
    print(f"  Group: {args.group}")
    print(f"  Bootstrap: {args.bootstrap}")
    print(f"  Auto offset reset: {args.offset}")
    print(f"  Timeout: {'never' if args.timeout == 0 else f'{args.timeout}s'}")
    print()

    timeout_ms = args.timeout * 1000 if args.timeout > 0 else None
    
    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap,
        group_id=args.group,
        auto_offset_reset=args.offset,
        enable_auto_commit=True,
        auto_commit_interval_ms=5000,
        consumer_timeout_ms=timeout_ms if timeout_ms else -1,
        value_deserializer=lambda m: m.decode('utf-8')
    )

    message_count = 0
    try:
        print("Waiting for messages... (Ctrl+C to stop)\n")
        
        for message in consumer:
            message_count += 1
            print(f"✓ Received [partition={message.partition}, offset={message.offset}]: {message.value}")
            
    except KeyboardInterrupt:
        print("\n\nShutting down consumer...")
    except StopIteration:
        print(f"\n\nTimeout reached ({args.timeout}s of inactivity). Shutting down...")
    except KafkaError as e:
        print(f"\n✗ Kafka error: {e}")
    finally:
        consumer.close()
        print(f"Consumer stopped. Received {message_count} messages total.")

if __name__ == '__main__':
    main()
