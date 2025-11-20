#!/usr/bin/env python3
"""
Run Experiment 5 demo: Start coordinator, workers, and show replication logs
"""
import asyncio
import subprocess
import time
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

async def run_coordinator():
    """Start coordinator in background"""
    print("[COORDINATOR] Starting...")
    proc = await asyncio.create_subprocess_exec(
        "uv", "run", "python", "src/coordinator.py",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
        cwd="/home/falcon/Projects/Collage/DC"
    )
    return proc

async def run_worker(worker_id, port, latency):
    """Start worker in background"""
    print(f"[WORKER {worker_id}] Starting...")
    proc = await asyncio.create_subprocess_exec(
        "uv", "run", "python", "test_node.py", worker_id, str(port), str(latency),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
        cwd="/home/falcon/Projects/Collage/DC"
    )
    return proc

async def read_output(proc, name):
    """Read and print output from process"""
    while True:
        line = await proc.stdout.readline()
        if not line:
            break
        print(f"[{name}] {line.decode().strip()}")

async def main():
    print("="*70)
    print("EXPERIMENT 5: DATA REPLICATION - LIVE DEMO")
    print("="*70)
    print()
    
    # Start coordinator
    coord_proc = await run_coordinator()
    await asyncio.sleep(2)
    
    # Start workers
    workers = [
        ("worker1", 50061, 0.01),
        ("worker3", 50063, 0.03),
        ("worker4", 50064, 0.04),
        ("worker5", 50065, 0.05),
    ]
    
    worker_procs = []
    for worker_id, port, latency in workers:
        proc = await run_worker(worker_id, port, latency)
        worker_procs.append((worker_id, proc))
        await asyncio.sleep(0.5)
    
    # Wait for all to be ready
    await asyncio.sleep(3)
    
    print()
    print("="*70)
    print("RUNNING EXPERIMENT 5 - REPLICATION TEST")
    print("="*70)
    print()
    
    # Run exp5 to trigger replication
    result = subprocess.run(
        ["uv", "run", "python", "exp5.py"],
        cwd="/home/falcon/Projects/Collage/DC",
        capture_output=True,
        text=True
    )
    
    # Show only key lines
    for line in result.stdout.split('\n'):
        if any(x in line for x in ['REPLICATION', 'REPLICATED', 'VERIFICATION', 'SUMMARY', '✓', '✗', 'Coordinator', 'worker']):
            print(line)
    
    print()
    print("="*70)
    print("READING LIVE LOGS FROM RUNNING PROCESSES (30 seconds)...")
    print("="*70)
    print()
    
    # Read outputs
    tasks = []
    tasks.append(read_output(coord_proc, "COORDINATOR"))
    for worker_id, proc in worker_procs:
        tasks.append(read_output(proc, f"WORKER-{worker_id}"))
    
    try:
        await asyncio.wait_for(asyncio.gather(*tasks), timeout=30)
    except asyncio.TimeoutError:
        print("\n[DEMO] 30 second demo complete, terminating processes...")
    
    # Terminate all
    coord_proc.terminate()
    for _, proc in worker_procs:
        proc.terminate()
    
    await asyncio.sleep(1)
    
    print()
    print("="*70)
    print("EXP5 DEMO COMPLETE")
    print("="*70)
    print("\nKey files for replication:")
    print("  • src/coordinator.py - Central registry")
    print("  • src/node/core.py - Replication logic")
    print("  • src/node/servicer.py - Task execution")
    print("  • exp5_replicas/ - Created replica files")
    print()

if __name__ == "__main__":
    asyncio.run(main())
