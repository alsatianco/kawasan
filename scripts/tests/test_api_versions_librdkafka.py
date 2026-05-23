#!/usr/bin/env python3
"""Test ApiVersions request version 3 with librdkafka-like client info"""

import socket
import struct

def write_unsigned_varint(value):
    """Write an unsigned varint"""
    result = b''
    while value > 0x7F:
        result += bytes([((value & 0x7F) | 0x80)])
        value >>= 7
    result += bytes([value & 0x7F])
    return result

def write_compact_string(s):
    """Write a compact string (length+1 as varint, then bytes)"""
    data = s.encode('utf-8') if isinstance(s, str) else s
    length_plus_one = len(data) + 1
    return write_unsigned_varint(length_plus_one) + data

def send_api_versions_request_v3():
    """Send an ApiVersions request (version 3) to the broker"""
    
    # Connect to broker
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(('localhost', 9092))
    
    # Build ApiVersions request (version 3)
    request_data = b''
    request_data += struct.pack('>h', 18)  # api_key (ApiVersions = 18)
    request_data += struct.pack('>h', 3)   # api_version = 3
    request_data += struct.pack('>i', 2)   # correlation_id
    
    # client_id (string = length + data)
    client_id = b'rdkafka'
    request_data += struct.pack('>h', len(client_id))
    request_data += client_id
    
    # For version 3, we need:
    # - client_software_name: compact string
    # - client_software_version: compact string
    # - tagged_fields: unsigned varint (0 for no tagged fields)
    
    # client_software_name (compact string)
    request_data += write_compact_string("librdkafka")
    
    # client_software_version (compact string)
    request_data += write_compact_string("2.12.1")
    
    # tagged_fields
    request_data += write_unsigned_varint(0)  # no tagged fields
    
    # Wrap in frame (size + data)
    frame = struct.pack('>i', len(request_data)) + request_data
    
    print(f"Sending {len(frame)} bytes...")
    print(f"Frame: {frame.hex()}")
    print(f"Request breakdown:")
    print(f"  api_key=18, version=3, correlation_id=2, client_id='rdkafka'")
    print(f"  client_software_name='librdkafka'")
    print(f"  client_software_version='2.12.1'")
    print(f"  tagged_fields=0")
    
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
            if error_code == 0:
                print("SUCCESS!")
            else:
                print(f"ERROR: {error_code}")
    
    sock.close()

if __name__ == '__main__':
    send_api_versions_request_v3()
