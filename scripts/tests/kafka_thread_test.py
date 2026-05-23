#!/usr/bin/env python3
import threading
import time
import argparse
import datetime
import signal
import sys

from kafka import KafkaProducer, KafkaConsumer

# ANSI color codes
COLOR_RESET = "\033[0m"
COLOR_PRODUCER = "\033[92m"   # bright green
COLOR_CONSUMER = "\033[96m"   # bright cyan


def now_str():
    """Return current timestamp as ISO string."""
    return datetime.datetime.now().isoformat(timespec="seconds")


def producer_thread_func(thread_id, producer, topic, interval, stop_event):
    """
    Producer thread function.
    Sends one message every `interval` seconds until stop_event is set.
    """
    i = 0
    while not stop_event.is_set():
        i += 1
        created_time = now_str()
        message = f"Message {i}-th from producer thread {thread_id}-th, created time: {created_time}"

        # Send message
        producer.send(topic, message.encode("utf-8"))

        # Optional: ensure it's actually sent quickly (good for a test tool)
        producer.flush()

        # Log producing
        print(f"{COLOR_PRODUCER}{now_str()} producer thread {thread_id}-th produces message {message}{COLOR_RESET}")

        # Wait before sending next message
        # Use stop_event.wait so we can exit promptly if stop_event is set
        if stop_event.wait(interval):
            break


def consumer_thread_func(thread_id, bootstrap_servers, topic, group_id, stop_event):
    """
    Consumer thread function.
    Continuously consumes messages and logs them until stop_event is set.
    """
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: v.decode("utf-8"),
    )

    try:
        for msg in consumer:
            if stop_event.is_set():
                break
            message_value = msg.value
            print(f"{COLOR_CONSUMER}{now_str()} consumer thread {thread_id}-th consume message {message_value}{COLOR_RESET}")
    finally:
        consumer.close()


def main():
    parser = argparse.ArgumentParser(description="Simple Kafka multi-threaded producer/consumer test script")
    parser.add_argument("--bootstrap-servers", "-b", default="localhost:9092",
                        help="Kafka bootstrap servers (default: localhost:9092)")
    parser.add_argument("--topic", "-t", default="test-topic",
                        help="Kafka topic name (default: test-topic)")
    parser.add_argument("--producer-threads", "-N", type=int, default=2,
                        help="Number of producer threads (default: 2)")
    parser.add_argument("--consumer-threads", "-M", type=int, default=2,
                        help="Number of consumer threads (default: 2)")
    parser.add_argument("--interval", "-T", type=float, default=2.0,
                        help="Interval in seconds between messages per producer thread (default: 2.0)")
    parser.add_argument("--group-id", "-g", default="kafka-test-group",
                        help="Consumer group id (default: kafka-test-group)")

    args = parser.parse_args()

    bootstrap_servers = args.bootstrap_servers
    topic = args.topic
    num_producers = args.producer_threads
    num_consumers = args.consumer_threads
    interval = args.interval
    group_id = args.group_id

    print(f"{now_str()} Starting Kafka test:")
    print(f"  bootstrap servers : {bootstrap_servers}")
    print(f"  topic             : {topic}")
    print(f"  producer threads  : {num_producers}")
    print(f"  consumer threads  : {num_consumers}")
    print(f"  interval (seconds): {interval}")
    print(f"  consumer group id : {group_id}")

    stop_event = threading.Event()

    # Handle Ctrl+C cleanly
    def handle_sigint(signum, frame):
        print(f"\n{now_str()} Caught interrupt, stopping...")
        stop_event.set()

    signal.signal(signal.SIGINT, handle_sigint)

    # Shared producer instance (KafkaProducer is thread-safe)
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

    producer_threads = []
    consumer_threads = []

    # Start producer threads
    for i in range(num_producers):
        th = threading.Thread(
            target=producer_thread_func,
            args=(i, producer, topic, interval, stop_event),
            daemon=True,
        )
        th.start()
        producer_threads.append(th)

    # Start consumer threads (each has its own consumer instance)
    for i in range(num_consumers):
        th = threading.Thread(
            target=consumer_thread_func,
            args=(i, bootstrap_servers, topic, group_id, stop_event),
            daemon=True,
        )
        th.start()
        consumer_threads.append(th)

    # Wait until stop_event is set (via Ctrl+C)
    try:
        while not stop_event.is_set():
            time.sleep(0.5)
    finally:
        # Ensure we signal threads to stop and wait a bit
        stop_event.set()
        print(f"{now_str()} Waiting for threads to finish...")

        for th in producer_threads:
            th.join(timeout=2.0)

        for th in consumer_threads:
            th.join(timeout=2.0)

        producer.close()
        print(f"{now_str()} Done.")


if __name__ == "__main__":
    main()
