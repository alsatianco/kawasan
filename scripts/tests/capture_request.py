#!/usr/bin/env python3
"""Capture what rdkafka actually sends"""

import socket

def capture_rdkafka_request():
    """Listen and capture what rdkafka sends"""
    
    # Create a server socket
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(('localhost', 19092))
    server.listen(1)
    
    print("Listening on localhost:19092...")
    print("Now run: kcat -L -b localhost:19092")
    
    conn, addr = server.accept()
    print(f"Connected from {addr}")
    
    # Read frame size
    frame_size_bytes = conn.recv(4)
    if len(frame_size_bytes) == 4:
        import struct
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
        print(f"Full frame: {frame_data.hex()}")
        
        # Parse the frame
        if len(frame_data) >= 10:
            api_key = struct.unpack('>h', frame_data[0:2])[0]
            api_version = struct.unpack('>h', frame_data[2:4])[0]
            correlation_id = struct.unpack('>i', frame_data[4:8])[0]
            
            print(f"API Key: {api_key}")
            print(f"API Version: {api_version}")
            print(f"Correlation ID: {correlation_id}")
            
            # Rest of the data
            print(f"Rest: {frame_data[8:].hex()}")
    
    conn.close()
    server.close()

if __name__ == '__main__':
    capture_rdkafka_request()
