#!/usr/bin/env python3
"""
Simple client to test network task distribution
"""

import asyncio
import sys
import os
import numpy as np

ROOT = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from node.core import UnifiedNode

async def test_client():
    client = UnifiedNode("test_client", 50066, 0.0)
    await client.start()
    
    print("Client registering...")
    await client.network.register_with_coordinator()
    
    print("Discovering peers...")
    peers = await client.network.get_available_peers(num_nodes=5)
    print(f"Discovered peers: {peers}")
    
    if peers:
        print("Sending matrix multiplication task...")
        A = np.random.rand(64, 64)
        B = np.random.rand(64, 64)
        result = await client.compute_distributed(A, B)
        print(f"Task completed, result shape: {result.shape}")
    else:
        print("No peers available for task.")
    
    await client.shutdown()

if __name__ == "__main__":
    asyncio.run(test_client())