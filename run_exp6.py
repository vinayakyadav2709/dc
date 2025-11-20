#!/usr/bin/env python3
"""Run Experiment 6 demo: Load Balancing with network consideration"""
import subprocess
import time
import sys

print("="*70)
print("EXPERIMENT 6: LOAD BALANCING")
print("="*70)
print()

# Start coordinator
print("[1/4] Starting coordinator...")
coord = subprocess.Popen(
    ["uv", "run", "python", "src/coordinator.py"],
    stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1
)
time.sleep(2)

# Start workers with different latencies
workers = [
    ("worker1", 50061, 0.01),   # Best latency
    ("worker3", 50063, 0.03),   # Good latency
    ("worker4", 50064, 0.04),   # Fair latency
    ("worker5", 50065, 0.05),   # Threshold
]

print("[2/4] Starting 4 workers with different latencies...")
worker_procs = []
for worker_id, port, latency in workers:
    proc = subprocess.Popen(
        ["uv", "run", "python", "test_node.py", worker_id, str(port), str(latency)],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1
    )
    worker_procs.append(proc)
    time.sleep(0.5)

time.sleep(3)
print("[3/4] All workers started")
print()
print("="*70)
print("SHOWING LOAD BALANCING ALGORITHM")
print("="*70)
print()

# Run exp6 which shows load balancing
subprocess.run(["uv", "run", "python", "exp6.py"])

print()
print("="*70)
print("LOAD BALANCING DEMO COMPLETE")
print("="*70)
print()

# Cleanup
print("Terminating processes...")
coord.terminate()
for proc in worker_procs:
    proc.terminate()

sys.exit(0)
