"""
Kafka Performance Testing Script
Tests throughput, latency, and various performance scenarios

IMPORTANT: This script measures different types of latency:
- Producer latency: Time from send() to broker acknowledgment (network + broker processing)
- Consumer throughput: Does NOT measure latency (only throughput for pre-populated messages)
- E2E latency: Round-trip time from produce to consume (true end-to-end latency)

CRITICAL REQUIREMENTS FOR FAIR BENCHMARKS:
1. Topics are DELETED and RECREATED before each test (via delete_first=True)
2. Each test run uses a unique run_id to track its own messages
3. Consumer throughput tests read pre-populated messages (no latency measurement)
4. E2E latency tests measure true round-trip time (producer ready → send → consume)
5. For comparing different brokers:
   - Run both in the same environment (both Docker or both host)
   - Match durability settings (acks, replication, fsync)
   - Use the same hardware and resource limits
"""

import time
import threading
import statistics
import uuid
from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed
import argparse
import json
from datetime import datetime

try:
    from kafka import KafkaProducer, KafkaConsumer
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import KafkaError
except ImportError:
    print("Please install kafka-python: pip install kafka-python")
    exit(1)


@dataclass
class PerformanceMetrics:
    """Store performance test results"""
    test_name: str
    total_messages: int
    duration_seconds: float
    throughput_msg_per_sec: float
    throughput_mb_per_sec: float
    avg_latency_ms: float
    min_latency_ms: float
    max_latency_ms: float
    p50_latency_ms: float
    p95_latency_ms: float
    p99_latency_ms: float
    errors: int
    
    def to_dict(self):
        return {
            'test_name': self.test_name,
            'total_messages': self.total_messages,
            'duration_seconds': round(self.duration_seconds, 2),
            'throughput_msg_per_sec': round(self.throughput_msg_per_sec, 2),
            'throughput_mb_per_sec': round(self.throughput_mb_per_sec, 2),
            'avg_latency_ms': round(self.avg_latency_ms, 2),
            'min_latency_ms': round(self.min_latency_ms, 2),
            'max_latency_ms': round(self.max_latency_ms, 2),
            'p50_latency_ms': round(self.p50_latency_ms, 2),
            'p95_latency_ms': round(self.p95_latency_ms, 2),
            'p99_latency_ms': round(self.p99_latency_ms, 2),
            'errors': self.errors
        }


class KafkaPerformanceTester:
    def __init__(self, bootstrap_servers: str):
        self.bootstrap_servers = bootstrap_servers
        self.latencies: List[float] = []
        self.errors = 0
        self.lock = threading.Lock()
        self.run_id = str(uuid.uuid4())  # Unique ID for this test run
    
    def check_connectivity(self) -> bool:
        """Check if we can connect to the Kafka broker"""
        try:
            print(f"\n🔍 Checking connectivity to {self.bootstrap_servers}...")
            admin_client = KafkaAdminClient(
                bootstrap_servers=self.bootstrap_servers,
                client_id='perf-test-connectivity-check',
                request_timeout_ms=5000
            )
            
            # Try to list topics as a connectivity check
            topics = admin_client.list_topics()
            admin_client.close()
            
            print(f"✓ Successfully connected to broker")
            print(f"  Existing topics: {len(topics)}")
            return True
        except Exception as e:
            print(f"❌ Failed to connect to broker: {e}")
            print(f"\nTroubleshooting:")
            print(f"  • Check if broker is running")
            print(f"  • Verify bootstrap servers: {self.bootstrap_servers}")
            print(f"  • Check network connectivity and firewall rules")
            print(f"  • For Docker: ensure port is exposed and mapped correctly")
            return False
        
    def create_topic(self, topic_name: str, num_partitions: int = 3, replication_factor: int = 1,
                     delete_first: bool = True):
        """Create a test topic, optionally deleting it first to ensure clean state"""
        if delete_first:
            self.delete_topic(topic_name)

        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=self.bootstrap_servers,
                client_id='perf-test-admin',
                request_timeout_ms=10000
            )

            topic = NewTopic(
                name=topic_name,
                num_partitions=num_partitions,
                replication_factor=replication_factor
            )

            admin_client.create_topics([topic], validate_only=False)
            print(f"✓ Created topic: {topic_name} (partitions={num_partitions})")
            admin_client.close()
            time.sleep(3)  # Wait for topic creation to propagate
        except Exception as e:
            if "TopicExistsError" not in str(e):
                print(f"⚠ Topic creation warning: {e}")
            # If topic exists, that's fine - we already deleted it if delete_first=True
    
    def delete_topic(self, topic_name: str):
        """Delete a test topic if it exists"""
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=self.bootstrap_servers,
                client_id='perf-test-admin',
                request_timeout_ms=10000
            )
            
            # Try to delete the topic
            admin_client.delete_topics([topic_name], timeout_ms=10000)
            print(f"🗑️  Deleted existing topic: {topic_name}")
            admin_client.close()
            
            # Wait longer for deletion to complete and propagate
            time.sleep(5)
        except Exception as e:
            # Ignore errors if topic doesn't exist
            if "UnknownTopicOrPartitionError" not in str(e) and "UNKNOWN_TOPIC_OR_PARTITION" not in str(e):
                # Only warn on unexpected errors
                pass
    
    def generate_message(self, size_bytes: int, message_id: int, include_timestamp: bool = True) -> bytes:
        """Generate a message of specified size

        Args:
            size_bytes: Target message size
            message_id: Unique message identifier
            include_timestamp: If True, include timestamp (for E2E latency tests only)
        """
        payload = {
            'id': message_id,
            'run_id': self.run_id,
        }

        if include_timestamp:
            payload['timestamp'] = time.time()

        # Calculate remaining space for data padding
        base_size = len(json.dumps(payload).encode('utf-8'))
        padding_size = max(0, size_bytes - base_size - 20)  # Leave room for JSON overhead
        payload['data'] = 'x' * padding_size

        return json.dumps(payload).encode('utf-8')
    
    def test_producer_throughput(
        self,
        topic: str,
        num_messages: int,
        message_size: int,
        batch_size: int = 100,
        compression_type: str = None,
        acks: str = '1'
    ) -> PerformanceMetrics:
        """Test producer throughput"""
        print(f"\n🚀 Testing Producer Throughput")
        print(f"   Messages: {num_messages}, Size: {message_size}B, "
              f"Batch: {batch_size}, Compression: {compression_type}, ACKs: {acks}")
        
        producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            acks=int(acks) if acks != 'all' else acks,  # Convert '1' to 1
            compression_type=compression_type,
            batch_size=batch_size * 1024,  # Convert to bytes
            linger_ms=10,  # Allow some batching for better throughput
            buffer_memory=67108864,  # 64MB
            request_timeout_ms=30000  # Increase timeout for slower operations
        )
        
        latencies = []
        errors = 0
        futures = []
        send_times = []
        
        start_time = time.time()
        
        # Send all messages (non-blocking)
        for i in range(num_messages):
            # Don't include timestamp - we're measuring producer latency (send to ack), not message age
            message = self.generate_message(message_size, i, include_timestamp=False)
            send_start = time.time()

            try:
                future = producer.send(topic, message)
                futures.append(future)
                send_times.append(send_start)
            except KafkaError as e:
                errors += 1
                print(f"⚠ Send error: {e}")
        
        # Now wait for all futures and calculate latencies
        for i, future in enumerate(futures):
            try:
                record_metadata = future.get(timeout=30)
                latency_ms = (time.time() - send_times[i]) * 1000
                latencies.append(latency_ms)
            except KafkaError as e:
                errors += 1
                print(f"⚠ Future error: {e}")
        
        producer.flush()
        producer.close()
        
        duration = time.time() - start_time
        total_bytes = num_messages * message_size
        
        return self._calculate_metrics(
            "Producer Throughput",
            num_messages,
            duration,
            total_bytes,
            latencies,
            errors
        )
    
    def test_consumer_throughput(
        self,
        topic: str,
        expected_messages: int,
        timeout_seconds: int = 60
    ) -> PerformanceMetrics:
        """Test consumer throughput (reading pre-populated messages)

        Note: This test measures THROUGHPUT only, not latency.
        We're reading pre-populated messages, so timestamp-based latency
        would just measure "message age", which is meaningless.
        """
        print(f"\n📥 Testing Consumer Throughput")
        print(f"   Expected messages: {expected_messages}, Timeout: {timeout_seconds}s")
        print(f"   Note: Measuring throughput only (latency N/A for pre-populated messages)")

        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=self.bootstrap_servers,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id=f'consumer-throughput-{self.run_id}',  # Unique per test run
            fetch_min_bytes=1,  # Don't wait for batching in throughput test
            fetch_max_wait_ms=100,  # Reduced wait time
            max_partition_fetch_bytes=1048576  # 1MB per partition
        )

        messages_received = 0
        total_bytes = 0
        errors = 0
        messages_from_this_run = 0

        start_time = time.time()

        try:
            while messages_received < expected_messages:
                if time.time() - start_time > timeout_seconds:
                    print(f"⚠ Timeout reached. Received {messages_received}/{expected_messages}")
                    break

                msg_batch = consumer.poll(timeout_ms=1000)

                for topic_partition, messages in msg_batch.items():
                    for message in messages:
                        try:
                            payload = json.loads(message.value.decode('utf-8'))

                            # Verify this message is from our test run
                            if payload.get('run_id') == self.run_id:
                                messages_from_this_run += 1

                            total_bytes += len(message.value)
                            messages_received += 1
                        except Exception as e:
                            errors += 1
        finally:
            consumer.close()

        duration = time.time() - start_time

        if messages_from_this_run != messages_received:
            print(f"⚠ Warning: Received {messages_received} total messages, "
                  f"but only {messages_from_this_run} from this test run")

        # No latencies for consumer throughput test
        return self._calculate_metrics(
            "Consumer Throughput",
            messages_received,
            duration,
            total_bytes,
            [],  # No latency measurement for throughput test
            errors
        )
    
    def test_concurrent_producers(
        self,
        topic: str,
        num_producers: int,
        messages_per_producer: int,
        message_size: int
    ) -> PerformanceMetrics:
        """Test multiple concurrent producers"""
        print(f"\n⚡ Testing Concurrent Producers")
        print(f"   Producers: {num_producers}, Messages/producer: {messages_per_producer}, "
              f"Size: {message_size}B")
        
        def producer_worker(worker_id: int) -> Tuple[List[float], int]:
            producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                acks=1,
                request_timeout_ms=30000
            )

            worker_latencies = []
            worker_errors = 0

            for i in range(messages_per_producer):
                # Don't include timestamp - measuring producer latency (send to ack)
                message = self.generate_message(message_size, worker_id * 1000000 + i,
                                                include_timestamp=False)
                send_start = time.time()

                try:
                    future = producer.send(topic, message)
                    future.get(timeout=30)
                    latency_ms = (time.time() - send_start) * 1000
                    worker_latencies.append(latency_ms)
                except Exception as e:
                    worker_errors += 1

            producer.flush()
            producer.close()
            return worker_latencies, worker_errors
        
        start_time = time.time()
        all_latencies = []
        total_errors = 0
        
        with ThreadPoolExecutor(max_workers=num_producers) as executor:
            futures = [executor.submit(producer_worker, i) for i in range(num_producers)]
            
            for future in as_completed(futures):
                latencies, errors = future.result()
                all_latencies.extend(latencies)
                total_errors += errors
        
        duration = time.time() - start_time
        total_messages = num_producers * messages_per_producer
        total_bytes = total_messages * message_size
        
        return self._calculate_metrics(
            f"Concurrent Producers (x{num_producers})",
            total_messages,
            duration,
            total_bytes,
            all_latencies,
            total_errors
        )
    
    def test_concurrent_consumers(
        self,
        topic: str,
        num_consumers: int,
        expected_total_messages: int,
        timeout_seconds: int = 60
    ) -> PerformanceMetrics:
        """Test multiple concurrent consumers in same consumer group

        Note: This test measures THROUGHPUT only, not latency.
        Messages are pre-populated, so timestamp-based latency would be meaningless.
        """
        print(f"\n📊 Testing Concurrent Consumers")
        print(f"   Consumers: {num_consumers}, Expected messages: {expected_total_messages}")
        print(f"   Note: Measuring throughput only (latency N/A for pre-populated messages)")

        messages_count = {'total': 0, 'lock': threading.Lock()}
        total_bytes = [0]
        errors = [0]
        messages_from_this_run = [0]

        def consumer_worker(worker_id: int):
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=self.bootstrap_servers,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id=f'concurrent-consumers-{self.run_id}'  # Unique per test run
            )

            worker_bytes = 0
            worker_errors = 0
            worker_run_messages = 0

            start = time.time()

            while True:
                with messages_count['lock']:
                    if messages_count['total'] >= expected_total_messages:
                        break

                if time.time() - start > timeout_seconds:
                    break

                msg_batch = consumer.poll(timeout_ms=1000)

                for _, messages in msg_batch.items():
                    for message in messages:
                        try:
                            payload = json.loads(message.value.decode('utf-8'))

                            # Track messages from this run
                            if payload.get('run_id') == self.run_id:
                                worker_run_messages += 1

                            worker_bytes += len(message.value)

                            with messages_count['lock']:
                                messages_count['total'] += 1
                        except Exception as e:
                            worker_errors += 1

            consumer.close()

            with messages_count['lock']:
                total_bytes[0] += worker_bytes
                errors[0] += worker_errors
                messages_from_this_run[0] += worker_run_messages
        
        start_time = time.time()

        threads = []
        for i in range(num_consumers):
            thread = threading.Thread(target=consumer_worker, args=(i,))
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()

        duration = time.time() - start_time

        if messages_from_this_run[0] != messages_count['total']:
            print(f"⚠ Warning: Received {messages_count['total']} total messages, "
                  f"but only {messages_from_this_run[0]} from this test run")

        # No latencies for consumer throughput test
        return self._calculate_metrics(
            f"Concurrent Consumers (x{num_consumers})",
            messages_count['total'],
            duration,
            total_bytes[0],
            [],  # No latency measurement for throughput test
            errors[0]
        )
    
    def test_end_to_end_latency(
        self,
        topic: str,
        num_messages: int,
        message_size: int,
        delay_between_messages_ms: int = 10
    ) -> PerformanceMetrics:
        """Test end-to-end latency with synchronized producer/consumer

        This is the ONE test where timestamp-based latency measurement makes sense,
        because we're measuring the full round-trip time from produce to consume.
        """
        print(f"\n⏱️  Testing End-to-End Latency")
        print(f"   Messages: {num_messages}, Size: {message_size}B, "
              f"Delay: {delay_between_messages_ms}ms")
        print(f"   Note: Measuring full round-trip latency (produce to consume)")

        # Start consumer FIRST
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=self.bootstrap_servers,
            auto_offset_reset='latest',  # Only read new messages
            enable_auto_commit=True,
            group_id=f'e2e-test-{self.run_id}',
            consumer_timeout_ms=1000,
            session_timeout_ms=10000,
            heartbeat_interval_ms=3000
        )

        # CRITICAL: Poll multiple times to ensure partition assignment completes
        # Without this, the first messages may be missed
        print("   Waiting for consumer to be ready...")
        
        # Poll multiple times to trigger partition assignment
        for i in range(5):
            consumer.poll(timeout_ms=200)
            time.sleep(0.3)
        
        # Verify consumer has partition assignments
        assignments = consumer.assignment()
        if not assignments:
            print("   ⚠ Warning: Consumer has no partition assignments yet, waiting longer...")
            for i in range(5):
                consumer.poll(timeout_ms=200)
                time.sleep(0.5)
                assignments = consumer.assignment()
                if assignments:
                    break
        
        print(f"   ✓ Consumer ready with {len(assignments)} partition(s) assigned")
        
        # Give extra time for everything to stabilize
        time.sleep(1)

        producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            acks='all',
            request_timeout_ms=30000
        )

        latencies = []
        sent_messages = {}
        errors = 0

        # Track the actual wall-clock time for the entire test
        test_start_time = time.time()

        # Send messages (include timestamp for E2E latency measurement)
        print(f"   Sending {num_messages} messages...")
        for i in range(num_messages):
            message = self.generate_message(message_size, i, include_timestamp=True)
            try:
                future = producer.send(topic, message)
                future.get(timeout=30)
                sent_messages[i] = time.time()
                
                # Add delay between messages for E2E test
                if delay_between_messages_ms > 0:
                    time.sleep(delay_between_messages_ms / 1000.0)
            except Exception as e:
                errors += 1
                print(f"⚠ Send error: {e}")

        # Ensure all messages are flushed to broker
        producer.flush()
        producer.close()
        
        # Give broker a moment to make messages available
        print(f"   All messages sent, waiting for consumption...")
        time.sleep(0.5)

        # Consume messages
        start_consume = time.time()
        received = 0
        received_from_this_run = 0

        while received < num_messages and (time.time() - start_consume) < 30:
            msg_batch = consumer.poll(timeout_ms=1000)

            for _, messages in msg_batch.items():
                for message in messages:
                    try:
                        payload = json.loads(message.value.decode('utf-8'))

                        # Only count messages from this test run
                        if payload.get('run_id') == self.run_id:
                            msg_id = payload['id']
                            sent_time = payload['timestamp']

                            latency_ms = (time.time() - sent_time) * 1000
                            latencies.append(latency_ms)
                            received_from_this_run += 1

                        received += 1
                    except Exception as e:
                        errors += 1

        consumer.close()

        if received_from_this_run < num_messages:
            print(f"⚠ Warning: Only received {received_from_this_run}/{num_messages} messages from this test run")
            print(f"   Total received (including old messages): {received}")
            if received_from_this_run == 0:
                print(f"   ❌ CRITICAL: E2E test failed - no messages received!")
                print(f"   This likely means:")
                print(f"     • Consumer wasn't ready before producer started, OR")
                print(f"     • Producer/consumer are connecting to different brokers, OR")
                print(f"     • Network/connectivity issue")
            else:
                print(f"   This likely means consumer wasn't fully ready before producer started")

        # Use actual wall-clock time for throughput calculation
        total_duration = time.time() - test_start_time
        total_bytes = received_from_this_run * message_size

        return self._calculate_metrics(
            "End-to-End Latency",
            received_from_this_run,  # Only count messages from this run
            total_duration,
            total_bytes,
            latencies,
            errors
        )
    
    def test_large_messages(
        self,
        topic: str,
        num_messages: int,
        message_size: int
    ) -> PerformanceMetrics:
        """Test with large message sizes

        Measures producer latency for large messages (send to ack time).
        
        Note: Kafka has default limits (1MB message.max.bytes). We use a smaller
        size (100KB) that should work for most Kafka configurations.
        """
        # Use a safer size that works with default Kafka config
        safe_message_size = min(message_size, 100 * 1024)  # Cap at 100KB
        
        print(f"\n📦 Testing Large Messages")
        print(f"   Messages: {num_messages}, Size: {safe_message_size / 1024:.2f}KB")
        print(f"   Note: Measuring producer latency (send to ack)")
        if safe_message_size < message_size:
            print(f"   ⚠ Capped message size from {message_size / 1024 / 1024:.2f}MB to {safe_message_size / 1024}KB")
            print(f"     (Kafka default limit is ~1MB, requires broker config change for larger)")

        producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            max_request_size=safe_message_size * 2,  # 2x message size
            buffer_memory=134217728,  # 128MB
            acks=1,
            request_timeout_ms=60000,
            compression_type=None  # No compression for large message test
        )

        latencies = []
        errors = 0
        error_messages = []

        start_time = time.time()

        for i in range(num_messages):
            # Don't include timestamp - measuring producer latency
            message = self.generate_message(safe_message_size, i, include_timestamp=False)
            send_start = time.time()

            try:
                future = producer.send(topic, message)
                future.get(timeout=60)
                latency_ms = (time.time() - send_start) * 1000
                latencies.append(latency_ms)
            except Exception as e:
                errors += 1
                error_msg = str(e)
                if errors <= 3:  # Only print first few errors
                    print(f"⚠ Send error: {error_msg}")
                if error_msg not in error_messages:
                    error_messages.append(error_msg)

        producer.flush()
        producer.close()

        if errors > 0:
            print(f"⚠ Total errors: {errors}/{num_messages}")
            print(f"   Unique error types: {len(error_messages)}")

        duration = time.time() - start_time
        total_bytes = len(latencies) * safe_message_size  # Use successful messages only

        return self._calculate_metrics(
            "Large Messages",
            len(latencies),  # Only count successful messages
            duration,
            total_bytes,
            latencies,
            errors
        )
    
    def _calculate_metrics(
        self,
        test_name: str,
        total_messages: int,
        duration_seconds: float,
        total_bytes: int,
        latencies: List[float],
        errors: int
    ) -> PerformanceMetrics:
        """Calculate performance metrics from test results"""
        if duration_seconds == 0:
            duration_seconds = 0.001
        
        throughput_msg = total_messages / duration_seconds
        throughput_mb = (total_bytes / duration_seconds) / (1024 * 1024)
        
        if latencies:
            sorted_latencies = sorted(latencies)
            avg_latency = statistics.mean(latencies)
            min_latency = min(latencies)
            max_latency = max(latencies)
            p50_latency = sorted_latencies[int(len(sorted_latencies) * 0.50)]
            p95_latency = sorted_latencies[int(len(sorted_latencies) * 0.95)]
            p99_latency = sorted_latencies[int(len(sorted_latencies) * 0.99)]
        else:
            avg_latency = min_latency = max_latency = p50_latency = p95_latency = p99_latency = 0
        
        return PerformanceMetrics(
            test_name=test_name,
            total_messages=total_messages,
            duration_seconds=duration_seconds,
            throughput_msg_per_sec=throughput_msg,
            throughput_mb_per_sec=throughput_mb,
            avg_latency_ms=avg_latency,
            min_latency_ms=min_latency,
            max_latency_ms=max_latency,
            p50_latency_ms=p50_latency,
            p95_latency_ms=p95_latency,
            p99_latency_ms=p99_latency,
            errors=errors
        )
    
    def print_metrics(self, metrics: PerformanceMetrics):
        """Pretty print performance metrics"""
        print(f"\n{'='*70}")
        print(f"📈 {metrics.test_name} - Results")
        print(f"{'='*70}")
        print(f"Messages:        {metrics.total_messages:,}")
        print(f"Duration:        {metrics.duration_seconds:.2f}s")
        print(f"Throughput:      {metrics.throughput_msg_per_sec:,.2f} msg/s")
        print(f"                 {metrics.throughput_mb_per_sec:.2f} MB/s")
        print(f"Latency (avg):   {metrics.avg_latency_ms:.2f}ms")
        print(f"Latency (min):   {metrics.min_latency_ms:.2f}ms")
        print(f"Latency (max):   {metrics.max_latency_ms:.2f}ms")
        print(f"Latency (p50):   {metrics.p50_latency_ms:.2f}ms")
        print(f"Latency (p95):   {metrics.p95_latency_ms:.2f}ms")
        print(f"Latency (p99):   {metrics.p99_latency_ms:.2f}ms")
        print(f"Errors:          {metrics.errors}")
        print(f"{'='*70}\n")


def main():
    parser = argparse.ArgumentParser(description='Kafka Performance Testing Tool')
    parser.add_argument('--bootstrap-servers', default='localhost:9092',
                       help='Kafka bootstrap servers (default: localhost:9092)')
    parser.add_argument('--topic-prefix', default='perf-test',
                       help='Topic name prefix (default: perf-test)')
    parser.add_argument('--cleanup', action='store_true',
                       help='Delete test topics after completion')
    parser.add_argument('--output', help='Output JSON file for results')
    
    # Test selection
    parser.add_argument('--tests', nargs='+', 
                       choices=['producer', 'consumer', 'concurrent-producers', 
                               'concurrent-consumers', 'e2e-latency', 'large-messages', 'all'],
                       default=['all'],
                       help='Tests to run (default: all)')
    
    # Test parameters
    parser.add_argument('--messages', type=int, default=10000,
                       help='Number of messages per test (default: 10000)')
    parser.add_argument('--message-size', type=int, default=1024,
                       help='Message size in bytes (default: 1024)')
    parser.add_argument('--num-producers', type=int, default=5,
                       help='Number of concurrent producers (default: 5)')
    parser.add_argument('--num-consumers', type=int, default=3,
                       help='Number of concurrent consumers (default: 3)')
    parser.add_argument('--partitions', type=int, default=6,
                       help='Number of partitions for test topics (default: 6)')
    
    args = parser.parse_args()
    
    print("="*70)
    print("🔥 Kafka Performance Testing Suite")
    print("="*70)
    print(f"Bootstrap Servers: {args.bootstrap_servers}")
    print(f"Topic Prefix:      {args.topic_prefix}")
    print(f"Messages:          {args.messages:,}")
    print(f"Message Size:      {args.message_size} bytes")
    print(f"Partitions:        {args.partitions}")
    print("="*70)
    print("\n⚠️  IMPORTANT: Fair Benchmark Requirements")
    print("-" * 70)
    print("For a valid performance comparison between brokers:")
    print("1. Both systems MUST run in the same environment:")
    print("   • Both in Docker, OR both on host (not mixed)")
    print("   • Same resource limits (CPU, memory)")
    print("2. Both systems MUST have equivalent durability guarantees:")
    print("   • Same acks setting (e.g., acks=1 or acks=all)")
    print("   • Same replication factor")
    print("   • Same fsync/flush behavior")
    print("3. Both systems MUST be tested on the same hardware")
    print("4. Topics are automatically deleted/recreated for clean state")
    print("5. Each test run uses unique run_id to avoid measuring old messages")
    print("-" * 70)
    print("\nTest Methodology:")
    print("• Producer tests: Measure send() to broker ack latency")
    print("• Consumer tests: Measure throughput only (not latency)")
    print("• E2E tests: Measure full round-trip time (produce → consume)")
    print("-" * 70)
    
    tester = KafkaPerformanceTester(args.bootstrap_servers)
    
    # Check connectivity before running tests
    if not tester.check_connectivity():
        print("\n❌ Cannot proceed with tests - broker not accessible")
        return 1
    
    results = []
    failed_tests = []  # Track failed tests
    
    tests_to_run = args.tests
    if 'all' in tests_to_run:
        tests_to_run = ['producer', 'consumer', 'concurrent-producers', 
                       'concurrent-consumers', 'e2e-latency', 'large-messages']
    
    try:
        # Producer throughput test
        if 'producer' in tests_to_run:
            topic = f"{args.topic_prefix}-producer"
            tester.create_topic(topic, args.partitions)
            
            metrics = tester.test_producer_throughput(
                topic=topic,
                num_messages=args.messages,
                message_size=args.message_size,
                batch_size=100,
                compression_type='gzip',
                acks='1'
            )
            tester.print_metrics(metrics)
            results.append(metrics.to_dict())
        
        # Consumer throughput test
        if 'consumer' in tests_to_run:
            topic = f"{args.topic_prefix}-consumer"
            tester.create_topic(topic, args.partitions)

            # First produce messages (no timestamps - not measuring latency)
            print(f"\n📝 Pre-populating topic with {args.messages} messages...")
            producer = KafkaProducer(bootstrap_servers=args.bootstrap_servers)
            for i in range(args.messages):
                msg = tester.generate_message(args.message_size, i, include_timestamp=False)
                producer.send(topic, msg)
            producer.flush()
            producer.close()

            time.sleep(2)  # Wait for messages to be available
            
            metrics = tester.test_consumer_throughput(
                topic=topic,
                expected_messages=args.messages
            )
            tester.print_metrics(metrics)
            results.append(metrics.to_dict())
        
        # Concurrent producers test
        if 'concurrent-producers' in tests_to_run:
            topic = f"{args.topic_prefix}-concurrent-producers"
            tester.create_topic(topic, args.partitions)
            
            metrics = tester.test_concurrent_producers(
                topic=topic,
                num_producers=args.num_producers,
                messages_per_producer=args.messages // args.num_producers,
                message_size=args.message_size
            )
            tester.print_metrics(metrics)
            results.append(metrics.to_dict())
        
        # Concurrent consumers test
        if 'concurrent-consumers' in tests_to_run:
            topic = f"{args.topic_prefix}-concurrent-consumers"
            tester.create_topic(topic, args.partitions)

            # Pre-populate (no timestamps - not measuring latency)
            print(f"\n📝 Pre-populating topic with {args.messages} messages...")
            producer = KafkaProducer(bootstrap_servers=args.bootstrap_servers)
            for i in range(args.messages):
                msg = tester.generate_message(args.message_size, i, include_timestamp=False)
                producer.send(topic, msg)
            producer.flush()
            producer.close()

            time.sleep(2)
            
            metrics = tester.test_concurrent_consumers(
                topic=topic,
                num_consumers=args.num_consumers,
                expected_total_messages=args.messages
            )
            tester.print_metrics(metrics)
            results.append(metrics.to_dict())
        
        # End-to-end latency test
        if 'e2e-latency' in tests_to_run:
            topic = f"{args.topic_prefix}-e2e-latency"
            tester.create_topic(topic, args.partitions)
            
            metrics = tester.test_end_to_end_latency(
                topic=topic,
                num_messages=min(1000, args.messages),  # Limit for latency test
                message_size=args.message_size,
                delay_between_messages_ms=10
            )
            tester.print_metrics(metrics)
            results.append(metrics.to_dict())
        
        # Large messages test
        if 'large-messages' in tests_to_run:
            topic = f"{args.topic_prefix}-large-messages"
            tester.create_topic(topic, args.partitions)
            
            # Use 100KB messages (safe for default Kafka config)
            # 1MB messages require broker config: message.max.bytes and replica.fetch.max.bytes
            metrics = tester.test_large_messages(
                topic=topic,
                num_messages=min(100, args.messages // 10),  # Fewer large messages
                message_size=100 * 1024  # 100KB messages (safe for default config)
            )
            tester.print_metrics(metrics)
            results.append(metrics.to_dict())
        
        # Summary
        print("\n" + "="*70)
        print("📊 PERFORMANCE TEST SUMMARY")
        print("="*70)
        print("\nLatency Measurement Definitions:")
        print("  • Producer tests: Time from send() to broker acknowledgment")
        print("  • Consumer tests: N/A (throughput only for pre-populated messages)")
        print("  • E2E Latency:    Full round-trip time (produce → consume)")
        print("="*70)
        
        for result in results:
            print(f"\n{result['test_name']}:")
            
            # Check for test failures
            is_failed = False
            if result['total_messages'] == 0 or result['throughput_msg_per_sec'] == 0:
                is_failed = True
                failed_tests.append(result['test_name'])
                print(f"  ❌ TEST FAILED - No messages processed")
            elif result['errors'] > 0 and result['errors'] == result['total_messages']:
                is_failed = True
                failed_tests.append(result['test_name'])
                print(f"  ❌ TEST FAILED - All messages errored")
            
            if not is_failed:
                print(f"  ✓ Throughput: {result['throughput_msg_per_sec']:,.0f} msg/s "
                      f"({result['throughput_mb_per_sec']:.2f} MB/s)")
                if result['avg_latency_ms'] > 0:
                    print(f"  ✓ Latency:    avg={result['avg_latency_ms']:.2f}ms, "
                          f"p95={result['p95_latency_ms']:.2f}ms, "
                          f"p99={result['p99_latency_ms']:.2f}ms")
                else:
                    print(f"  ℹ Latency:    N/A (throughput test only)")
                
                if result['errors'] > 0:
                    error_rate = (result['errors'] / (result['total_messages'] + result['errors'])) * 100
                    print(f"  ⚠ Errors:     {result['errors']} ({error_rate:.1f}% error rate)")
                else:
                    print(f"  ✓ Errors:     {result['errors']}")
        
        if failed_tests:
            print("\n" + "="*70)
            print("⚠️  WARNING: Some tests failed:")
            for test_name in failed_tests:
                print(f"   • {test_name}")
            print("\nPossible causes:")
            print("  • Broker not running or not accessible")
            print("  • Network connectivity issues")
            print("  • Broker configuration limits (message size, etc.)")
            print("  • Consumer not ready before producer started (E2E test)")
            print("="*70)
        
        # Save to JSON if requested
        if args.output:
            output_data = {
                'timestamp': datetime.now().isoformat(),
                'config': {
                    'bootstrap_servers': args.bootstrap_servers,
                    'messages': args.messages,
                    'message_size': args.message_size,
                    'partitions': args.partitions
                },
                'results': results
            }
            with open(args.output, 'w') as f:
                json.dump(output_data, f, indent=2)
            print(f"\n✓ Results saved to {args.output}")
        
    finally:
        # Cleanup
        if args.cleanup:
            print("\n🧹 Cleaning up test topics...")
            for test in tests_to_run:
                if test != 'all':
                    topic = f"{args.topic_prefix}-{test}"
                    tester.delete_topic(topic)
    
    print("\n✅ Performance testing complete!")
    
    if failed_tests:
        return 1
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
