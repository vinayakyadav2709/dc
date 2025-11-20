# Distributed Computing Experiments

This project implements 10 distributed computing experiments using Python, gRPC, NumPy, PySpark, and MPI.

## Experiments

1. Basic gRPC communication
2. Multithreading with async tasks
3. Clock synchronization
4. Bully election algorithm
5. Data replication
6. Load balancing
7. MapReduce with PySpark
8. Fault tolerance
9. MPI collectives
10. Parallel matrix multiplication

## Setup

Install dependencies:
```bash
pip install -e .
```

## Usage

Run coordinator:
```bash
python src/coordinator.py
```

Run nodes:
```bash
python test_node.py <node_name> <port> <latency>
```

Run client:
```bash
python test_client.py
```

## Docker

Use Podman Compose for isolated testing:
```bash
podman-compose up --build
```
# dc
