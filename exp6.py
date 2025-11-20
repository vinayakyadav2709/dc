#!/usr/bin/env python3
"""Experiment 6 — Load Balancing Algorithm with Network and Bandwidth Consideration."""

import json
import asyncio
import sys
import os
import time
from pathlib import Path
from textwrap import dedent

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

RELEVANT_FILES = [
    ("src/node/network.py", "Measures latency, selects best nodes based on connectivity"),
    ("src/node/core.py", "Distributes load across selected nodes"),
    ("src/config.py", "GOOD_CONNECTION_LATENCY threshold"),
    ("src/node/servicer.py", "Tracks current_load per node for decisions"),
]

def summarize_experiment():
    """Print details about Experiment 6."""
    heading = dedent(
        """
        ╔═══════════════════════════════════════════════════════════╗
        ║         EXPERIMENT 6: LOAD BALANCING                      ║
        ╚═══════════════════════════════════════════════════════════╝
        
        OVERVIEW:
        Load balancing distributes computational work across nodes
        based on network latency, bandwidth, and current load.
        Faster nodes get more tasks; overloaded nodes are skipped.
        
        KEY CONCEPTS:
        • Latency-aware: Ping all nodes, measure round-trip time
        • Availability-aware: Skip busy/overloaded nodes
        • Quality thresholds: Only use nodes under GOOD_CONNECTION_LATENCY
        • Fallback strategy: If no good nodes, pick fastest K nodes
        """
    ).strip()
    print(heading)

def print_relevant_files():
    """Print all relevant files."""
    print("\n" + "="*60)
    print("RELEVANT FILES FOR EXPERIMENT 6")
    print("="*60)
    for path, description in RELEVANT_FILES:
        exists = Path(path).exists()
        status = "✓" if exists else "✗"
        print(f"\n[{status}] {path}")
        print(f"    {description}")

def simulate_load_balancing():
    """Simulate load balancing algorithm."""
    print("\n" + "="*60)
    print("LOAD BALANCING ALGORITHM SIMULATION")
    print("="*60)
    
    # Simulated nodes with different latencies and loads
    nodes = [
        {"id": "worker1", "latency": 0.010, "load": 0, "cpu_cores": 20},
        {"id": "worker2", "latency": 0.020, "load": 2, "cpu_cores": 20},
        {"id": "worker3", "latency": 0.030, "load": 1, "cpu_cores": 20},
        {"id": "worker4", "latency": 0.042, "load": 0, "cpu_cores": 20},
        {"id": "worker5", "latency": 0.053, "load": 3, "cpu_cores": 20},
    ]
    
    GOOD_CONNECTION_LATENCY = 0.05  # 50ms threshold
    
    print("\n[STEP 1] Node Status (Latency & Load):")
    print("-" * 60)
    for node in nodes:
        status = "🟢 GOOD" if node['latency'] < GOOD_CONNECTION_LATENCY else "🟡 FAIR"
        print(f"  {node['id']:10} | Latency: {node['latency']*1000:5.1f}ms | Load: {node['load']}/5 | {status}")
    
    print("\n[STEP 2] Filtering by Quality (< 50ms):")
    print("-" * 60)
    good_nodes = [n for n in nodes if n['latency'] < GOOD_CONNECTION_LATENCY]
    print(f"  Found {len(good_nodes)} nodes with good connectivity:")
    for node in good_nodes:
        print(f"    ✓ {node['id']:10} ({node['latency']*1000:5.1f}ms)")
    
    print("\n[STEP 3] Sorting by Latency (Best First):")
    print("-" * 60)
    good_nodes.sort(key=lambda x: x['latency'])
    for i, node in enumerate(good_nodes, 1):
        print(f"  {i}. {node['id']:10} ({node['latency']*1000:5.1f}ms, load {node['load']}/5)")
    
    print("\n[STEP 4] Load Distribution (Assign Tasks):")
    print("-" * 60)
    tasks_to_assign = 4
    assignment = []
    for i in range(tasks_to_assign):
        # Pick node with lowest latency that's not overloaded
        available = [n for n in good_nodes if n['load'] < 4]
        if available:
            selected = available[0]
            selected['load'] += 1
            assignment.append((f"Task-{i+1}", selected['id']))
            print(f"  Task-{i+1} → {selected['id']:10} (new load: {selected['load']}/5)")
        else:
            print(f"  Task-{i+1} → NO AVAILABLE NODES (all overloaded)")
    
    print("\n[STEP 5] Final Load State:")
    print("-" * 60)
    for node in nodes:
        bar_len = int(node['load'] / 5 * 20)
        bar = "█" * bar_len + "░" * (20 - bar_len)
        print(f"  {node['id']:10} | {bar} | {node['load']}/5")
    
    return assignment

def simulate_failover():
    """Simulate failover when best node becomes unavailable."""
    print("\n" + "="*60)
    print("FAILOVER SCENARIO")
    print("="*60)
    
    nodes = [
        {"id": "worker1", "latency": 0.010, "available": False},  # DEAD
        {"id": "worker2", "latency": 0.020, "available": True},
        {"id": "worker3", "latency": 0.030, "available": True},
    ]
    
    print("\n[SCENARIO] worker1 (best latency) is DOWN")
    print("-" * 60)
    
    print("\n[FALLBACK] Using next best available node:")
    for node in nodes:
        status = "✓ UP" if node['available'] else "✗ DOWN"
        print(f"  {node['id']} ({node['latency']*1000:.1f}ms) {status}")
    
    available_nodes = [n for n in nodes if n['available']]
    available_nodes.sort(key=lambda x: x['latency'])
    
    if available_nodes:
        selected = available_nodes[0]
        print(f"\n✓ Redirecting task to {selected['id']} ({selected['latency']*1000:.1f}ms)")

async def test_network_load_balancing():
    """Test actual load balancing via coordinator and nodes."""
    print("\n" + "="*60)
    print("NETWORK LOAD BALANCING TEST")
    print("="*60)
    
    try:
        import grpc.aio
        import compute_pb2
        import compute_pb2_grpc
        from config import COORDINATOR_ADDRESS, GRPC_OPTIONS
        
        async with grpc.aio.insecure_channel(COORDINATOR_ADDRESS, options=GRPC_OPTIONS) as channel:
            stub = compute_pb2_grpc.CoordinatorStub(channel)
            
            # Query available nodes
            response = await stub.GetAvailableNodes(
                compute_pb2.NodeRequest(
                    num_nodes=10,
                    requesting_node_id="exp6-client"
                )
            )
            
            if response.nodes:
                print(f"\n✓ Found {len(response.nodes)} nodes for load balancing:")
                for node in response.nodes[:5]:
                    print(f"   {node.node_id:10} at {node.address:20} ({node.status})")
                if len(response.nodes) > 5:
                    print(f"   ... and {len(response.nodes) - 5} more")
            else:
                print("  ⚠ No nodes available")
    except Exception as e:
        print(f"  ⚠ Network test skipped: {e}")
        print("     (Start coordinator + workers with: python run_exp5.py)")

def main():
    summarize_experiment()
    print_relevant_files()
    
    simulate_load_balancing()
    simulate_failover()
    
    try:
        asyncio.run(test_network_load_balancing())
    except:
        pass
    
    print("\n" + "="*60)
    print("SUMMARY: LOAD BALANCING")
    print("="*60)
    print("""
Algorithm:
  1. Query available nodes from coordinator
  2. Measure latency to each (via Ping RPC)
  3. Filter: Keep only nodes with latency < threshold
  4. Sort: Order by latency (best first)
  5. Assign: Give tasks to best available nodes
  6. Monitor: Track load per node, skip if overloaded
  7. Fallback: Use next best if preferred unavailable

Benefits:
  ✓ Faster task completion (low-latency nodes)
  ✓ Prevents overload (skip busy nodes)
  ✓ Fault tolerance (automatic failover)
  ✓ Network-aware (bandwidth considerations)
  ✓ Dynamic (latency measured in real-time)
    """)

if __name__ == "__main__":
    main()
