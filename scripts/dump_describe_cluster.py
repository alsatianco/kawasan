#!/usr/bin/env python3
import socket
import struct

def send_describe_cluster():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(('localhost', 9092))
    
    # DESCRIBE_CLUSTER request (API Key 60, Version 0)
    api_key = 60
    api_version = 0
    correlation_id = 42
    client_id = "test"
    
    # Build request payload
    include_cluster_authorized_operations = 1  # true
    request_payload = struct.pack('>B', include_cluster_authorized_operations)
    
    # Build header
    header = struct.pack('>HHI', api_key, api_version, correlation_id)
    header += struct.pack('>H', len(client_id)) + client_id.encode()
    
    # Build complete message
    message = header + request_payload
    message_with_length = struct.pack('>I', len(message)) + message
    
    # Send
    sock.sendall(message_with_length)
    
    # Receive response
    response_length_bytes = sock.recv(4)
    response_length = struct.unpack('>I', response_length_bytes)[0]
    print(f"Response length: {response_length}")
    
    response = b''
    while len(response) < response_length:
        chunk = sock.recv(response_length - len(response))
        if not chunk:
            break
        response += chunk
    
    print(f"Response bytes ({len(response)}): {response.hex()}")
    print()
    
    # Parse response
    offset = 0
    
    correlation_id_response = struct.unpack('>I', response[offset:offset+4])[0]
    offset += 4
    print(f"Correlation ID: {correlation_id_response}")
    
    throttle_time = struct.unpack('>I', response[offset:offset+4])[0]
    offset += 4
    print(f"Throttle time: {throttle_time} ms")
    
    error_code = struct.unpack('>H', response[offset:offset+2])[0]
    offset += 2
    print(f"Error code: {error_code}")
    
    error_msg_len = struct.unpack('>H', response[offset:offset+2])[0]
    offset += 2
    error_msg = response[offset:offset+error_msg_len].decode() if error_msg_len > 0 else ""
    offset += error_msg_len
    print(f"Error message: '{error_msg}'")
    
    cluster_id_len = struct.unpack('>H', response[offset:offset+2])[0]
    offset += 2
    print(f"Cluster ID length: {cluster_id_len}")
    if cluster_id_len == 0xFFFF:  # -1 in signed
        print(f"Cluster ID: NULL")
    else:
        cluster_id = response[offset:offset+cluster_id_len].decode()
        offset += cluster_id_len
        print(f"Cluster ID: '{cluster_id}'")
    
    sock.close()

if __name__ == '__main__':
    send_describe_cluster()
