#!/usr/bin/env python3
import socket
import struct

def send_metadata():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(('localhost', 9092))
    
    # METADATA request (API Key 3, Version 7)
    api_key = 3
    api_version = 7
    correlation_id = 99
    client_id = "test-client"
    
    # Build request payload: empty topics array (request all topics)
    request_payload = b''
    request_payload += struct.pack('>i', 0)  # topics array length = 0 (all topics)
    request_payload += struct.pack('>B', 1)  # allow_auto_topic_creation = true
    
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
    
    print(f"\\nFull response hex ({len(response)} bytes):")
    for i in range(0, len(response), 32):
        hex_part = response[i:i+32].hex()
        print(f"  {i:04d}: {hex_part}")
    
    print("\\nParsing response...")
    offset = 0
    
    correlation_id_response = struct.unpack('>I', response[offset:offset+4])[0]
    offset += 4
    print(f"Correlation ID: {correlation_id_response}")
    
    # Version 3+ has throttle_time
    throttle_time = struct.unpack('>I', response[offset:offset+4])[0]
    offset += 4
    print(f"Throttle time: {throttle_time} ms")
    
    # Brokers array
    broker_count = struct.unpack('>I', response[offset:offset+4])[0]
    offset += 4
    print(f"Broker count: {broker_count}")
    
    for i in range(broker_count):
        broker_id = struct.unpack('>I', response[offset:offset+4])[0]
        offset += 4
        
        host_len = struct.unpack('>H', response[offset:offset+2])[0]
        offset += 2
        host = response[offset:offset+host_len].decode()
        offset += host_len
        
        port = struct.unpack('>I', response[offset:offset+4])[0]
        offset += 4
        
        # Version 1+ has rack (nullable)
        rack_len_signed = struct.unpack('>h', response[offset:offset+2])[0]
        offset += 2
        if rack_len_signed == -1:
            rack = None
        else:
            rack = response[offset:offset+rack_len_signed].decode()
            offset += rack_len_signed
        
        print(f"  Broker {i}: id={broker_id}, host={host}, port={port}, rack={rack}")
    
    # Version 2+ has cluster_id (nullable)
    print(f"\\nCluster ID at offset {offset}:")
    cluster_id_len_signed = struct.unpack('>h', response[offset:offset+2])[0]
    offset += 2
    print(f"  Cluster ID length (signed): {cluster_id_len_signed}")
    if cluster_id_len_signed == -1:
        cluster_id = None
        print(f"  Cluster ID: NULL")
    else:
        cluster_id = response[offset:offset+cluster_id_len_signed].decode()
        offset += cluster_id_len_signed
        print(f"  Cluster ID: '{cluster_id}'")
    
    # Version 1+ has controller_id
    controller_id = struct.unpack('>I', response[offset:offset+4])[0]
    offset += 4
    print(f"Controller ID: {controller_id}")
    
    print(f"\\nRemaining bytes: {len(response) - offset}")
    
    sock.close()

if __name__ == '__main__':
    send_metadata()
