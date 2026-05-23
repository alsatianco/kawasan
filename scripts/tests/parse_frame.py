#!/usr/bin/env python3

import struct

# The full frame from kcat
frame_hex = "0012000300000001000772646b61666b61000b6c696272646b61666b6107322e31322e3100"
frame_data = bytes.fromhex(frame_hex)

print(f"Total frame length: {len(frame_data)} bytes\n")

offset = 0

# Parse header
api_key = struct.unpack('>h', frame_data[offset:offset+2])[0]
offset += 2
print(f"API Key: {api_key}")

api_version = struct.unpack('>h', frame_data[offset:offset+2])[0]
offset += 2
print(f"API Version: {api_version}")

correlation_id = struct.unpack('>i', frame_data[offset:offset+4])[0]
offset += 4
print(f"Correlation ID: {correlation_id}")

client_id_len = struct.unpack('>h', frame_data[offset:offset+2])[0]
offset += 2
client_id = frame_data[offset:offset+client_id_len].decode('utf-8')
offset += client_id_len
print(f"Client ID: '{client_id}' (length={client_id_len})")

print(f"\nHeader size: {offset} bytes")
print(f"Request body ({len(frame_data) - offset} bytes): {frame_data[offset:].hex()}\n")

# Parse request body as hex bytes
body = frame_data[offset:]
print("Request body breakdown:")
for i, byte in enumerate(body):
    char = chr(byte) if 32 <= byte < 127 else '.'
    print(f"  [{i:2d}] 0x{byte:02x} = {byte:3d} '{char}'")
