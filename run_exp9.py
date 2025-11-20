#!/usr/bin/env python3
"""
Run Experiment 9: MPI Collective Communication

Demonstrates MPI Broadcast, Scatter, and Gather operations
"""

import subprocess
import sys

def main():
    print("\n" + "="*70)
    print("EXPERIMENT 9: MPI COLLECTIVE COMMUNICATION")
    print("="*70 + "\n")
    
    print("[1/1] Running Experiment 9...\n")
    
    # Run exp9 directly (no separate processes needed)
    result = subprocess.run(['python', '-u', 'exp9.py'])
    
    sys.exit(result.returncode)


if __name__ == '__main__':
    main()
