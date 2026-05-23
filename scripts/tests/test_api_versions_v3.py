#!/usr/bin/env python3
"""Test ApiVersions request version 3 to kawasan broker"""

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

def send_api_versions_request_v3():
    """Send an ApiVersions request (version 3) to the broker"""
    
    # Connect to broker
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(('localhost', 9092))
    
    # Build ApiVersions request (version 3)
    # Request Header (FLEXIBLE version for API v3):
    # - api_key: int16 = 18 (ApiVersions)
    # - api_version: int16 = 3
    # - correlation_id: int32 = 2
    # - client_id: COMPACT_STRING = "test"
    # - tagged_fields: unsigned varint = 0
    
    request_data = b''
    request_data += struct.pack('>h', 18)  # api_key (ApiVersions = 18)
    request_data += struct.pack('>h', 3)   # api_version = 3
    request_data += struct.pack('>i', 2)   # correlation_id
    
    # client_id (COMPACT STRING - length+1 as unsigned varint, then data)
    client_id = b'test'
    request_data += write_unsigned_varint(len(client_id) + 1)  # length + 1
    request_data += client_id
    
    # tagged_fields for REQUEST HEADER
    request_data += write_unsigned_varint(0)  # no tagged fields in header
    
    # Request body for ApiVersions v3:
    # - client_software_name: compact string
    # - client_software_version: compact string
    # - tagged_fields: unsigned varint (0 for no tagged fields)
    
    # client_software_name (compact string - empty)
    request_data += write_unsigned_varint(1)  # length + 1 = 1 means empty string
    
    # client_software_version (compact string - empty)
    request_data += write_unsigned_varint(1)  # length + 1 = 1 means empty string
    
    # tagged_fields for REQUEST BODY
    request_data += write_unsigned_varint(0)  # no tagged fields
    
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
    
    sock.close()

if __name__ == '__main__':
    send_api_versions_request_v3()
