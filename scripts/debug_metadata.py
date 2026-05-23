#!/usr/bin/env python3
"""
Debug script to decode and display Metadata response details.
"""

import socket
import struct
import sys

def send_metadata_request(host, port):
    """Send Metadata request and decode response."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(5)
    
    try:
        sock.connect((host, port))
        
        # Build request header for METADATA (API 3)
        correlation_id = 1
        client_id = b"debug-client"
        
        header = struct.pack('>h', 3)  # API key: METADATA
        header += struct.pack('>h', 7)  # API version: 7
        header += struct.pack('>i', correlation_id)
        header += struct.pack('>h', len(client_id)) + client_id
        
        # Empty request body (all topics)
        body = struct.pack('>i', 0)  # 0 topics = all topics
        
        # Send request
        request = header + body
        message = struct.pack('>i', len(request)) + request
        sock.sendall(message)
        
        # Read response
        response_length_bytes = sock.recv(4)
        response_length = struct.unpack('>i', response_length_bytes)[0]
        
        response = b''
        while len(response) < response_length:
            chunk = sock.recv(min(4096, response_length - len(response)))
            if not chunk:
                break
            response += chunk
        
        # Parse response
        offset = 0
        
        # Correlation ID
        corr_id = struct.unpack('>i', response[offset:offset+4])[0]
        offset += 4
        print(f"Correlation ID: {corr_id}")
        
        # Throttle time (v1+)
        throttle_time = struct.unpack('>i', response[offset:offset+4])[0]
        offset += 4
        print(f"Throttle time: {throttle_time} ms")
        
        # Brokers array
        broker_count = struct.unpack('>i', response[offset:offset+4])[0]
        offset += 4
        print(f"\nBroker count: {broker_count}")
        
        for i in range(broker_count):
            # Node ID
            node_id = struct.unpack('>i', response[offset:offset+4])[0]
            offset += 4
            
            # Host
            host_len = struct.unpack('>h', response[offset:offset+2])[0]
            offset += 2
            host = response[offset:offset+host_len].decode('utf-8')
            offset += host_len
            
            # Port
            port = struct.unpack('>i', response[offset:offset+4])[0]
            offset += 4
            
            # Rack (nullable, v1+)
            rack_len = struct.unpack('>h', response[offset:offset+2])[0]
            offset += 2
            rack = None
            if rack_len >= 0:
                rack = response[offset:offset+rack_len].decode('utf-8')
                offset += rack_len
            
            print(f"  Broker {i}: id={node_id}, host={host}, port={port}, rack={rack}")
        
        # Cluster ID (v2+)
        cluster_id_len = struct.unpack('>h', response[offset:offset+2])[0]
        offset += 2
        cluster_id = None
        if cluster_id_len >= 0:
            cluster_id = response[offset:offset+cluster_id_len].decode('utf-8')
            offset += cluster_id_len
        print(f"\nCluster ID: {cluster_id}")
        
        # Controller ID (v1+)
        controller_id = struct.unpack('>i', response[offset:offset+4])[0]
        offset += 4
        print(f"Controller ID: {controller_id}")
        
        # Topics
        topic_count = struct.unpack('>i', response[offset:offset+4])[0]
        offset += 4
        print(f"\nTopic count: {topic_count}")
        
        return True
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        sock.close()


if __name__ == '__main__':
    host = sys.argv[1] if len(sys.argv) > 1 else 'localhost'
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 9092
    
    print(f"Fetching metadata from {host}:{port}\n")
    print("=" * 60)
    send_metadata_request(host, port)
    print("=" * 60)
