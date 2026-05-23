#!/usr/bin/env python3
"""Test ApiVersions request to kawasan broker"""

import socket
import struct

def send_api_versions_request():
    """Send an ApiVersions request (version 0) to the broker"""
    
    # Connect to broker
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(('localhost', 9092))
    
    # Build ApiVersions request (version 0)
    # Request Header:
    # - api_key: int16 = 18 (ApiVersions)
    # - api_version: int16 = 0
    # - correlation_id: int32 = 1
    # - client_id: string = "test"
    
    request_data = b''
    request_data += struct.pack('>h', 18)  # api_key (ApiVersions = 18)
    request_data += struct.pack('>h', 0)   # api_version
    request_data += struct.pack('>i', 1)   # correlation_id
    
    # client_id (string = length + data)
    client_id = b'test'
    request_data += struct.pack('>h', len(client_id))
    request_data += client_id
    
    # For version 0, there's no request body
    
    # Wrap in frame (size + data)
    frame = struct.pack('>i', len(request_data)) + request_data
    
    print(f"Sending {len(frame)} bytes...")
    print(f"Frame: {frame.hex()}")
    
    sock.sendall(frame)
    
    # Read response frame size
    response_size_bytes = sock.recv(4)
    if len(response_size_bytes) < 4:
        print(f"ERROR: Only got {len(response_size_bytes)} bytes for frame size")
        sock.close()
        return
        
    response_size = struct.unpack('>i', response_size_bytes)[0]
    print(f"Response size: {response_size} bytes")
    
    # Read response data
    response_data = b''
    while len(response_data) < response_size:
        chunk = sock.recv(response_size - len(response_data))
        if not chunk:
            break
        response_data += chunk
    
    print(f"Received {len(response_data)} bytes of {response_size} expected")
    print(f"Response data: {response_data.hex()}")
    
    if len(response_data) >= 4:
        correlation_id = struct.unpack('>i', response_data[0:4])[0]
        print(f"Correlation ID: {correlation_id}")
        
        if len(response_data) >= 6:
            error_code = struct.unpack('>h', response_data[4:6])[0]
            print(f"Error code: {error_code}")
            
            if len(response_data) >= 10:
                array_length = struct.unpack('>i', response_data[6:10])[0]
                print(f"API versions array length: {array_length}")
    
    sock.close()

if __name__ == '__main__':
    send_api_versions_request()
