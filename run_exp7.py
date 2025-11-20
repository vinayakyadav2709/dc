#!/usr/bin/env python3
"""Run Experiment 7: Start coordinator + workers + exp7"""
import subprocess
import time
import sys

print("[STARTING COORDINATOR & WORKERS]")
print()

# Start coordinator
coord = subprocess.Popen(
    ["uv", "run", "python", "src/coordinator.py"],
    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
)
time.sleep(1)

# Start 4 workers
workers = [
    ("worker1", 50061, 0.01),
    ("worker3", 50063, 0.03),
    ("worker4", 50064, 0.04),
    ("worker5", 50065, 0.05),
]

worker_procs = []
for worker_id, port, latency in workers:
    proc = subprocess.Popen(
        ["uv", "run", "python", "test_node.py", worker_id, str(port), str(latency)],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    worker_procs.append(proc)
    time.sleep(0.3)

time.sleep(2)

# Run exp7
print("="*70)
subprocess.run(["uv", "run", "python", "exp7.py"])
print("="*70)

# Cleanup
coord.terminate()
for proc in worker_procs:
    proc.terminate()

sys.exit(0)
