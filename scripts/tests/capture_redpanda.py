#!/usr/bin/env python3
"""Capture what Redpanda Console sends for ApiVersions v4"""

import socket
import struct

def capture_request():
    """Listen and capture"""
    
    # Create a server socket
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(('localhost', 19092))
    server.listen(1)
    
    print("Listening on localhost:19092...")
    print("Run: docker run --rm -e KAFKA_BROKERS=localhost:19092 docker.redpanda.com/redpandadata/console:latest")
    
    conn, addr = server.accept()
    print(f"Connected from {addr}")
    
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
        print(f"Full frame hex: {frame_data.hex()}")
        
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
        
        print(f"\nParsed header:")
        print(f"  API Key: {api_key}")
        print(f"  API Version: {api_version}")
        print(f"  Correlation ID: {correlation_id}")
        print(f"  Client ID: {client_id}")
        
        # Body
        body = frame_data[offset:]
        print(f"\nRequest body ({len(body)} bytes):")
        print(f"  Hex: {body.hex()}")
        print(f"\n  Bytes breakdown:")
        for i in range(min(len(body), 100)):
            print(f"    [{i:2d}] 0x{body[i]:02x} = {body[i]:3d} = varint? {body[i] & 0x7F if body[i] < 128 else 'multi-byte'}")
    
    conn.close()
    server.close()

if __name__ == '__main__':
    capture_request()
