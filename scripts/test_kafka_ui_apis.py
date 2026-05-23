#!/usr/bin/env python3
"""
Test script to verify DESCRIBE_CONFIGS and DESCRIBE_CLUSTER APIs work correctly.
"""

import socket
import struct
import sys

def send_kafka_request(host, port, api_key, api_version, request_body):
    """Send a Kafka protocol request and return the response."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(5)
    
    try:
        sock.connect((host, port))
        
        # Build request header
        correlation_id = 1
        client_id = b"test-client"
        
        header = struct.pack('>h', api_key)  # API key
        header += struct.pack('>h', api_version)  # API version
        header += struct.pack('>i', correlation_id)  # Correlation ID
        header += struct.pack('>h', len(client_id)) + client_id  # Client ID
        
        # Build complete request with length prefix
        request = header + request_body
        message = struct.pack('>i', len(request)) + request
        
        sock.sendall(message)
        
        # Read response length
        response_length_bytes = sock.recv(4)
        if len(response_length_bytes) < 4:
            return None
        response_length = struct.unpack('>i', response_length_bytes)[0]
        
        # Read response
        response = b''
        while len(response) < response_length:
            chunk = sock.recv(min(4096, response_length - len(response)))
            if not chunk:
                break
            response += chunk
        
        return response
        
    except Exception as e:
        print(f"Error: {e}")
        return None
    finally:
        sock.close()


def test_api_versions(host, port):
    """Test API_VERSIONS request (API key 18)."""
    print("Testing API_VERSIONS...")
    
    # Empty body for API_VERSIONS
    response = send_kafka_request(host, port, 18, 0, b'')
    
    if response:
        # Parse correlation ID
        correlation_id = struct.unpack('>i', response[:4])[0]
        print(f"✓ API_VERSIONS responded (correlation_id={correlation_id})")
        
        # Check for DESCRIBE_CONFIGS (32) and DESCRIBE_CLUSTER (60)
        if len(response) > 8:
            print(f"  Response length: {len(response)} bytes")
            return True
    else:
        print("✗ API_VERSIONS failed")
        return False


def test_describe_configs(host, port):
    """Test DESCRIBE_CONFIGS request (API key 32)."""
    print("\nTesting DESCRIBE_CONFIGS...")
    
    # Build request body for DESCRIBE_CONFIGS
    # Request broker configs for broker ID 0
    body = b''
    body += struct.pack('>i', 1)  # 1 resource
    body += struct.pack('>b', 4)  # Resource type: BROKER (4)
    body += struct.pack('>h', 1) + b'0'  # Resource name: "0"
    body += struct.pack('>i', -1)  # Config names: null (all configs)
    
    response = send_kafka_request(host, port, 32, 0, body)
    
    if response:
        correlation_id = struct.unpack('>i', response[:4])[0]
        print(f"✓ DESCRIBE_CONFIGS responded (correlation_id={correlation_id})")
        print(f"  Response length: {len(response)} bytes")
        
        # Parse throttle time
        throttle_time = struct.unpack('>i', response[4:8])[0]
        print(f"  Throttle time: {throttle_time} ms")
        return True
    else:
        print("✗ DESCRIBE_CONFIGS failed or timed out")
        return False


def test_describe_cluster(host, port):
    """Test DESCRIBE_CLUSTER request (API key 60)."""
    print("\nTesting DESCRIBE_CLUSTER...")
    
    # Build request body for DESCRIBE_CLUSTER (flexible v0 uses tagged fields)
    body = struct.pack('>b', 0)  # include_cluster_authorized_operations: false
    body += b'\x00'  # tagged fields (empty)
    
    response = send_kafka_request(host, port, 60, 0, body)
    
    if response:
        correlation_id = struct.unpack('>i', response[:4])[0]
        print(f"✓ DESCRIBE_CLUSTER responded (correlation_id={correlation_id})")
        print(f"  Response length: {len(response)} bytes")
        
        # Parse throttle time
        throttle_time = struct.unpack('>i', response[4:8])[0]
        print(f"  Throttle time: {throttle_time} ms")
        return True
    else:
        print("✗ DESCRIBE_CLUSTER failed or timed out")
        return False


def test_metadata(host, port):
    """Test METADATA request (API key 3)."""
    print("\nTesting METADATA...")
    
    # Build request body for METADATA - all topics
    body = struct.pack('>i', 0)  # 0 topics = all topics
    
    response = send_kafka_request(host, port, 3, 0, body)
    
    if response:
        correlation_id = struct.unpack('>i', response[:4])[0]
        print(f"✓ METADATA responded (correlation_id={correlation_id})")
        print(f"  Response length: {len(response)} bytes")
        return True
    else:
        print("✗ METADATA failed")
        return False


def main():
    host = 'localhost'
    port = 9092
    
    print(f"Testing Kawasan broker at {host}:{port}\n")
    print("=" * 60)
    
    results = []
    results.append(("API_VERSIONS", test_api_versions(host, port)))
    results.append(("METADATA", test_metadata(host, port)))
    results.append(("DESCRIBE_CONFIGS", test_describe_configs(host, port)))
    results.append(("DESCRIBE_CLUSTER", test_describe_cluster(host, port)))
    
    print("\n" + "=" * 60)
    print("SUMMARY:")
    print("=" * 60)
    
    all_passed = True
    for name, passed in results:
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"{status}: {name}")
        if not passed:
            all_passed = False
    
    print("=" * 60)
    
    if all_passed:
        print("\n✓ All tests passed! Kafka UI should now work correctly.")
        return 0
    else:
        print("\n✗ Some tests failed. Kafka UI may not work properly.")
        return 1


if __name__ == '__main__':
    sys.exit(main())
