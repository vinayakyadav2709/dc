#!/usr/bin/env python3
"""
Run Experiment 8: Fault Tolerance with Primary-Backup Replication

Starts coordinator + workers, then runs exp8 demo
"""

import subprocess
import time
import signal
import sys
import os

COORDINATOR_PORT = 50000
WORKER_PORTS = [50001, 50002, 50003, 50004]
COORDINATOR_ADDR = "localhost:50000"

processes = []


def signal_handler(sig, frame):
    """Handle graceful shutdown"""
    print("\n\nTerminating processes...")
    for p in processes:
        try:
            p.terminate()
            p.wait(timeout=2)
        except:
            p.kill()
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)


def start_coordinator():
    """Start coordinator"""
    print("[1/3] Starting coordinator...")
    env = os.environ.copy()
    env['PYTHONUNBUFFERED'] = '1'
    
    p = subprocess.Popen(
        ['python', '-u', 'src/coordinator.py'],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env=env
    )
    processes.append(p)
    time.sleep(1)


def start_workers(count=4):
    """Start worker nodes"""
    print(f"[2/3] Starting {count} workers...")
    env = os.environ.copy()
    env['PYTHONUNBUFFERED'] = '1'
    
    for i, port in enumerate(WORKER_PORTS[:count]):
        p = subprocess.Popen(
            ['python', '-u', 'start_node.py', str(port)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            env=env
        )
        processes.append(p)
    
    time.sleep(2)
    print(f"[2/3] All workers started")


def run_exp8():
    """Run exp8 demo"""
    print("[3/3] Running Experiment 8...\n")
    time.sleep(1)
    
    subprocess.run(['python', '-u', 'exp8.py'])


def main():
    print("\n" + "="*70)
    print("EXPERIMENT 8: FAULT TOLERANCE WITH PRIMARY-BACKUP REPLICATION")
    print("="*70 + "\n")
    
    try:
        start_coordinator()
        start_workers(4)
        run_exp8()
        
    except Exception as e:
        print(f"Error: {e}")
        raise
    finally:
        signal_handler(None, None)


if __name__ == '__main__':
    main()
