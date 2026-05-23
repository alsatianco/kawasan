#!/usr/bin/env python3
import sys
sys.path.insert(0, 'scripts/env/lib/python3.9/site-packages')

from kafka.coordinator.protocol import ConsumerProtocol

# Create an empty assignment
empty = ConsumerProtocol.ASSIGNMENT.encode(ConsumerProtocol.ASSIGNMENT(0, []))
print(f"Empty assignment length: {len(empty)}")
print(f"Hex: {empty.hex()}")
print(f"Bytes: {list(empty)}")
print(f"\nBinary representation:")
for i, b in enumerate(empty):
    print(f"  Byte {i}: {b:3d} (0x{b:02x})")
