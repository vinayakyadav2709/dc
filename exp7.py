#!/usr/bin/env python3
"""Experiment 7: MapReduce - REAL implementation using UnifiedNode"""
import asyncio
import sys
import os
import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from node.core import UnifiedNode

async def main():
    print("="*70)
    print("EXPERIMENT 7: MAPREDUCE WITH REAL UNIFIED NODE")
    print("="*70)
    print()
    
    # Create a client node (UnifiedNode in CLIENT MODE)
    print("[CLIENT] Initializing UnifiedNode as client...")
    client = UnifiedNode("mapreduce-client", 50098, latency_simulation=0.0)
    await client._start_server()
    await client._register()
    
    print("[CLIENT] Node ready, registered with coordinator\n")
    
    # Create test matrices
    print("="*70)
    print("MAPREDUCE COMPUTATION")
    print("="*70)
    print()
    
    A = np.random.rand(128, 128)
    B = np.random.rand(128, 128)
    
    print(f"[CLIENT] Matrix A: {A.shape}")
    print(f"[CLIENT] Matrix B: {B.shape}")
    print()
    
    # THIS IS THE REAL MAPREDUCE IMPLEMENTATION
    # UnifiedNode.compute_distributed() handles:
    # - Discovery of available workers (via coordinator)
    # - Partitioning matrix A across workers
    # - Sending tasks to workers (MAP phase)
    # - Aggregating results from workers (REDUCE phase)
    
    print("[MAPREDUCE] Starting distributed computation...")
    print("[CLIENT] compute_distributed() will:")
    print("  1. Query coordinator for available nodes")
    print("  2. Check P2P availability of each node")
    print("  3. Partition matrix A into blocks")
    print("  4. Send blocks to workers (MAP)")
    print("  5. Aggregate results (REDUCE)")
    print()
    
    result = await client.compute_distributed(A, B)
    
    # Verify correctness
    expected = np.matmul(A, B)
    is_correct = np.allclose(result, expected)
    
    print()
    print("="*70)
    print("MAPREDUCE RESULTS")
    print("="*70)
    print(f"""
Result shape: {result.shape}
Expected shape: {expected.shape}
Correctness: {'✓ CORRECT' if is_correct else '✗ WRONG'}

WHAT HAPPENED UNDER THE HOOD:

1. UnifiedNode.compute_distributed() called
2. Got available nodes from coordinator
3. Performed P2P availability checks
4. Selected best nodes based on latency
5. Partitioned matrix A into row blocks
6. Sent each block to a worker (MAP PHASE)
7. Each worker computed: block @ B
8. Coordinator collected results
9. Stacked results together (REDUCE PHASE)
10. Returned final result

KEY FILES INVOLVED:
  ✓ src/node/core.py - UnifiedNode.compute_distributed()
  ✓ src/node/network.py - Peer discovery & latency measurement
  ✓ src/node/servicer.py - Worker receives & executes tasks
  ✓ src/coordinator.py - Tracks available nodes
  
THIS IS REAL MAPREDUCE, not simulation!
    """)
    
    await client.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
