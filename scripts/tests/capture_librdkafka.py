#!/usr/bin/env python3
"""Capture what librdkafka sends for ApiVersions v3"""

import socket
import struct

def read_unsigned_varint(data, offset):
    """Read an unsigned varint and return (value, new_offset)"""
    value = 0
    shift = 0
    while offset < len(data):
        byte = data[offset]
        offset += 1
        value |= (byte & 0x7F) << shift
        if (byte & 0x80) == 0:
            break
        shift += 7
    return value, offset

def capture_request():
    """Listen and capture the first request"""
    
    # Create a server socket
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(('localhost', 19092))
    server.listen(1)
    
    print("Listening on localhost:19092...")
    print("Run: kcat -L -b localhost:19092")
    
    conn, addr = server.accept()
    print(f"\nConnected from {addr}")
    
    # Read frame size
    frame_size_bytes = conn.recv(4)
    if len(frame_size_bytes) == 4:
        frame_size = struct.unpack('>i', frame_size_bytes)[0]
        print(f"Frame size: {frame_size} bytes")
        
        # Read the frame
        frame_data = b''
        while len(frame_data) < frame_size:
            chunk = conn.recv(frame_size - len(frame_data))
            if not chunk:
                break
            frame_data += chunk
        
        print(f"Received {len(frame_data)} bytes")
        print(f"\nFull frame hex:\n{frame_data.hex()}\n")
        
        # Parse header
        offset = 0
        api_key = struct.unpack('>h', frame_data[offset:offset+2])[0]
        offset += 2
        api_version = struct.unpack('>h', frame_data[offset:offset+2])[0]
        offset += 2
        correlation_id = struct.unpack('>i', frame_data[offset:offset+4])[0]
        offset += 4
        client_id_len = struct.unpack('>h', frame_data[offset:offset+2])[0]
        offset += 2
        client_id = frame_data[offset:offset+client_id_len].decode('utf-8')
        offset += client_id_len
        
        print(f"Parsed header:")
        print(f"  API Key: {api_key}")
        print(f"  API Version: {api_version}")
        print(f"  Correlation ID: {correlation_id}")
        print(f"  Client ID: {client_id}")
        
        # Body for API version 3
        body = frame_data[offset:]
        print(f"\nRequest body ({len(body)} bytes):")
        print(f"  Hex: {body.hex()}")
        
        if api_version >= 3:
            print(f"\nParsing v3+ fields:")
            pos = 0
            
            # Read client_software_name (compact string)
            if pos < len(body):
                name_len_plus_one, pos = read_unsigned_varint(body, pos)
                print(f"  client_software_name length+1: {name_len_plus_one} (at offset {offset + pos - 1})")
                if name_len_plus_one > 0:
                    name_len = name_len_plus_one - 1
                    if pos + name_len <= len(body):
                        name = body[pos:pos+name_len].decode('utf-8')
                        print(f"  client_software_name: '{name}'")
                        pos += name_len
            
            # Read client_software_version (compact string)
            if pos < len(body):
                version_len_plus_one, pos = read_unsigned_varint(body, pos)
                print(f"  client_software_version length+1: {version_len_plus_one} (at offset {offset + pos - 1})")
                if version_len_plus_one > 0:
                    version_len = version_len_plus_one - 1
                    if pos + version_len <= len(body):
                        version = body[pos:pos+version_len].decode('utf-8')
                        print(f"  client_software_version: '{version}'")
                        pos += version_len
            
            # Read tagged fields count
            if pos < len(body):
                tagged_count, pos = read_unsigned_varint(body, pos)
                print(f"  tagged_fields count: {tagged_count} (at offset {offset + pos - 1})")
                
                for i in range(tagged_count):
                    if pos >= len(body):
                        print(f"    ERROR: No more data for tag {i}")
                        break
                    
                    tag_id, pos = read_unsigned_varint(body, pos)
                    print(f"    Tag {i}: id={tag_id}")
                    
                    if pos >= len(body):
                        print(f"      ERROR: No size for tag {tag_id}")
                        break
                    
                    tag_size, pos = read_unsigned_varint(body, pos)
                    print(f"      size={tag_size} bytes")
                    
                    if pos + tag_size <= len(body):
                        tag_data = body[pos:pos+tag_size]
                        print(f"      data: {tag_data.hex()}")
                        print(f"      data (raw): {tag_data}")
                        pos += tag_size
                    else:
                        print(f"      ERROR: Not enough data (need {tag_size}, have {len(body) - pos})")
                        break
    
    conn.close()
    server.close()

if __name__ == '__main__':
    capture_request()
