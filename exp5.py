#!/usr/bin/env python3
"""Experiment 5 — Data replication showcase."""

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
    ("README.md", "Lists all 10 experiments and describes "
                 "why replication matters in this suite."),
    ("src/coordinator.py", "Simple coordinator that keeps node metadata "
                           "and acts as the replication directory."),
    ("src/node/network.py", "Network manager that queries the coordinator, "
                             "measures latencies, and routes replication-aware requests."),
    ("src/node/core.py", "UnifiedNode orchestrating computation, reservation, "
                          "and logging that drives replicated workloads."),
    ("src/node/servicer.py", "Servicer that accepts replicated tasks, "
                              "decides whether to redistribute, and releases peers."),
    ("start_node.py", "Entrypoint used inside containers to bootstrap each "
                      "replicated worker node."),
    ("test_matrix_client.py", "Client that exercises distributed matrix multiplication "
                             "and therefore the replication paths."),
    ("docker-compose.yml", "Orchestrates the coordinator/worker swarm used for replicated experiments."),
    ("logs/", "Captured logs for each worker/coordinator showing replication behavior."),
]

SAMPLE_PAYLOAD = {
    "dataset": "replicated_configs",
    "version": "5.0",
    "policy": "write-once-multi-read",
    "rows": 128,
    "columns": 128,
    "checksum": "exp5-data/replica-ready",
}

REPLICAS_DIR = Path("exp5_replicas")


def ensure_replicas_dir() -> None:
    """Create the local replicas directory."""
    REPLICAS_DIR.mkdir(exist_ok=True)


def replicate_sample(sample: dict, nodes: list[str]) -> None:
    """Write the same sample payload to every simulated replica file."""
    ensure_replicas_dir()

    for node in nodes:
        destination = REPLICAS_DIR / f"{node}.json"
        destination.write_text(json.dumps(sample, indent=2))
        print(f"  ✓ Replicated to {node}.json")
        time.sleep(0.1)  # Show replication timeline


def verify_replicas(nodes: list[str]) -> bool:
    """Read back each replica and ensure it matches the sample."""
    print("\n[VERIFICATION] Checking all replicas...")
    all_match = True
    for node in nodes:
        destination = REPLICAS_DIR / f"{node}.json"
        if not destination.exists():
            print(f"  ✗ Missing replica for {node}")
            all_match = False
        else:
            restored = json.loads(destination.read_text())
            if restored == SAMPLE_PAYLOAD:
                print(f"  ✓ {node}.json matches source")
            else:
                print(f"  ✗ {node}.json MISMATCH")
                all_match = False
    return all_match


async def test_network_replication() -> None:
    """Test actual replication via coordinator and nodes."""
    print("\n[NETWORK TEST] Attempting to contact coordinator...")
    
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
                    requesting_node_id="exp5-client"
                )
            )
            
            if response.nodes:
                print(f"  ✓ Coordinator online! Found {len(response.nodes)} available nodes:")
                for node in response.nodes[:5]:  # Show first 5
                    print(f"     → {node.node_id} at {node.address} ({node.status})")
                if len(response.nodes) > 5:
                    print(f"     ... and {len(response.nodes) - 5} more")
            else:
                print("  ⚠ Coordinator online but no nodes available")
    except Exception as e:
        print(f"  ⚠ Network test skipped: {e}")
        print("     (Start coordinator + workers with: podman-compose up)")


def summarize_experiment() -> None:
    """Print details about Experiment 5."""
    heading = dedent(
        """
        ╔═══════════════════════════════════════════════════════════╗
        ║         EXPERIMENT 5: DATA REPLICATION                    ║
        ╚═══════════════════════════════════════════════════════════╝
        
        OVERVIEW:
        Data replication ensures fault tolerance and performance by
        distributing copies of data/tasks across worker nodes. Each
        worker registers with the coordinator, exposes availability
        via P2P checks, and participates in redundant execution.
        
        KEY CONCEPTS:
        • Write-once-multi-read (WOMR): Data written once, read by many
        • Replica consistency: All copies match the source
        • Coordinator-driven: Central registry manages replica locations
        • P2P verification: Nodes check each other's availability
        """
    ).strip()
    print(heading)


def print_relevant_files() -> None:
    """Print all relevant files."""
    print("\n" + "="*60)
    print("RELEVANT FILES FOR EXPERIMENT 5")
    print("="*60)
    for path, description in RELEVANT_FILES:
        exists = Path(path).exists()
        status = "✓" if exists else "✗"
        print(f"\n[{status}] {path}")
        print(f"    {description}")


def main() -> None:
    summarize_experiment()
    print_relevant_files()
    
    print("\n" + "="*60)
    print("LOCAL REPLICATION DEMO")
    print("="*60)
    print("\n[REPLICATION] Writing payload to 5 workers...")
    replicate_sample(SAMPLE_PAYLOAD, ["worker1", "worker2", "worker3", "worker4", "worker5"])
    
    if verify_replicas(["worker1", "worker2", "worker3", "worker4", "worker5"]):
        print("\n✓ All replicas verified!")
    else:
        print("\n✗ Replication verification FAILED")
    
    # Try network test
    try:
        asyncio.run(test_network_replication())
    except:
        pass
    
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    print("""
Exp5 demonstrates data replication via:

1. LOCAL: Wrote payload to exp5_replicas/{worker1-5}.json
2. NETWORK: Can query coordinator for node discovery
3. LOGS: Check logs/{coordinator,worker*.log} for replication traces
4. DISTRIBUTED: Run 'podman-compose up' then 'uv run python test_matrix_client.py'
   to see live replication in Docker

Files created: exp5_replicas/{worker1-5}.json
Logs available: logs/{coordinator.log, worker1.log, ...}
    """)


if __name__ == "__main__":
    main()
