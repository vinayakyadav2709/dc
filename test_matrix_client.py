#!/usr/bin/env python3
"""
Test client for distributed matrix multiplication
Spawns a client node and performs numpy-based matrix multiplication
"""

import asyncio
import sys
import os
import numpy as np

# Add src to path
ROOT = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from node.core import UnifiedNode

async def test_matrix_client():
    """Test client that performs distributed matrix multiplication"""

    # Create client with unique port (avoiding worker ports 50061-50070)
    client = UnifiedNode("matrix_client", 50071, 0.0)
    await client.start()

    print("Matrix client registering with coordinator...")
    await client.network.register_with_coordinator()

    print("Discovering available peers...")
    peers = await client.network.get_available_peers(num_nodes=10)  # Get up to 10 peers
    print(f"Discovered {len(peers)} peers: {[p['node_id'] for p in peers]}")

    if peers:
        print("\n" + "="*60)
        print("STARTING DISTRIBUTED MATRIX MULTIPLICATION TEST")
        print("="*60)

        # Create test matrices
        size = 128  # Larger matrix for better distributed test
        print(f"Creating {size}x{size} matrices...")
        A = np.random.rand(size, size).astype(np.float32)
        B = np.random.rand(size, size).astype(np.float32)

        print(f"Matrix A shape: {A.shape}, Matrix B shape: {B.shape}")
        print(f"Expected result shape: ({size}, {size})")

        # Perform distributed computation
        print("\nStarting distributed computation...")
        start_time = asyncio.get_event_loop().time()

        result = await client.compute_distributed(A, B)

        end_time = asyncio.get_event_loop().time()
        duration = end_time - start_time

        print("\n✓ Distributed computation completed!")
        print(f"Result shape: {result.shape}")
        print(f"Computation time: {duration:.2f} seconds")
        # Verify result correctness (compare with local computation)
        print("\nVerifying result correctness...")
        local_result = np.dot(A, B)

        # Check if results are close (allowing for floating point precision)
        if np.allclose(result, local_result, rtol=1e-5, atol=1e-5):
            print("✓ Results match! Distributed computation is correct.")
        else:
            print("✗ Results don't match! There might be an error.")
            max_diff = np.max(np.abs(result - local_result))
            print(f"Max difference: {max_diff:.2e}")
        print("\n" + "="*60)
        print("MATRIX MULTIPLICATION TEST COMPLETED")
        print("="*60)

    else:
        print("❌ No peers available for distributed computation.")
        print("Make sure the coordinator and workers are running.")

    # Clean shutdown
    await client.shutdown()
    print("\nMatrix client shutdown complete.")

if __name__ == "__main__":
    asyncio.run(test_matrix_client())