#!/usr/bin/env python3
"""
Node entrypoint script
"""
import asyncio
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from node.core import UnifiedNode

async def main():
    # Get arguments from command line or use defaults
    node_id = sys.argv[1] if len(sys.argv) > 1 else "worker1"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 50061
    latency = float(sys.argv[3]) if len(sys.argv) > 3 else 0.01

    print(f"Starting node {node_id} on port {port} with latency {latency}")

    node = UnifiedNode(node_id, port, latency)
    await node.start()

    print(f"Node {node_id} running... Press Ctrl+C to stop")

    try:
        await node.server.wait_for_termination()
    except KeyboardInterrupt:
        print(f"Shutting down node {node_id}...")
        await node.shutdown()

if __name__ == "__main__":
    asyncio.run(main())