#!/usr/bin/env python3
"""
Run Experiment 10: Parallel Matrix Multiplication using MPI

Demonstrates parallel 8x8 matrix multiplication with full results
"""

import subprocess
import sys

def main():
    print("\n" + "="*80)
    print("EXPERIMENT 10: PARALLEL MATRIX MULTIPLICATION USING MPI")
    print("="*80 + "\n")
    
    print("[1/1] Running Experiment 10...\n")
    
    # Run exp10 directly
    result = subprocess.run(['python', '-u', 'exp10.py'])
    
    sys.exit(result.returncode)


if __name__ == '__main__':
    main()
