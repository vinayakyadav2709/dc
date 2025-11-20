#!/usr/bin/env python3
"""Simple exp5: Start coordinator + workers, then trigger replication via test_matrix_client"""
import subprocess
import time
import sys

print("="*70)
print("EXPERIMENT 5: DATA REPLICATION")
print("="*70)
print()

# Start coordinator
print("[1/5] Starting coordinator...")
coord = subprocess.Popen(
    ["uv", "run", "python", "src/coordinator.py"],
    stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1
)
time.sleep(2)

# Start workers
workers = [
    ("worker1", 50061, 0.01),
    ("worker3", 50063, 0.03),
    ("worker4", 50064, 0.04),
    ("worker5", 50065, 0.05),
]

print("[2/5] Starting 4 workers...")
worker_procs = []
for worker_id, port, latency in workers:
    proc = subprocess.Popen(
        ["uv", "run", "python", "test_node.py", worker_id, str(port), str(latency)],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1
    )
    worker_procs.append(proc)
    time.sleep(0.5)

time.sleep(3)
print("[3/5] All workers started and registered")
print()
print("="*70)
print("NOW TRIGGERING REPLICATION VIA test_matrix_client...")
print("="*70)
print()

# Run test_matrix_client which triggers replication
subprocess.run(["uv", "run", "python", "test_matrix_client.py"])

print()
print("="*70)
print("REPLICATION DEMO COMPLETE")
print("="*70)
print()

# Cleanup
print("Terminating processes...")
coord.terminate()
for proc in worker_procs:
    proc.terminate()

sys.exit(0)
