#!/usr/bin/env python3
"""
Simple test for node registration and discovery
"""

import asyncio
import sys
import os

ROOT = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from node.core import UnifiedNode

async def test_node(node_id, port, latency):
    node = UnifiedNode(node_id, port, latency)
    await node.start()
    
    print(f"Registering {node_id}...")
    await node.network.register_with_coordinator()
    
    print(f"Getting peers for {node_id}...")
    peers = await node.network.get_available_peers(num_nodes=5)
    print(f"Discovered peers: {peers}")
    
    print(f"{node_id} running... Press Ctrl+C to stop")
    try:
        await node.server.wait_for_termination()
    except KeyboardInterrupt:
        await node.shutdown()

if __name__ == "__main__":
    node_id = sys.argv[1] if len(sys.argv) > 1 else "test-node"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 50060
    latency = float(sys.argv[3]) if len(sys.argv) > 3 else 0.0
    
    asyncio.run(test_node(node_id, port, latency))