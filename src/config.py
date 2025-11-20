"""Central configuration"""

import os

# Coordinator
COORDINATOR_HOST = "localhost"
COORDINATOR_PORT = 50051
COORDINATOR_ADDRESS = os.environ.get('COORDINATOR_ADDRESS', f"localhost:{COORDINATOR_PORT}")

# Node ports
NODE_BASE_PORT = 50052

# System settings
DEFAULT_NUM_NODES = 10
ENABLE_RECURSION = True
MAX_RECURSION_DEPTH = 2

# Network thresholds
GOOD_CONNECTION_LATENCY = 0.05
POOR_CONNECTION_LATENCY = 0.2
REDISTRIBUTION_SIZE_THRESHOLD = 1_000_000
MIN_ROWS_FOR_REDISTRIBUTION = 50

# gRPC settings
GRPC_OPTIONS = [
    ('grpc.max_send_message_length', 100 * 1024 * 1024),
    ('grpc.max_receive_message_length', 100 * 1024 * 1024),
]

# Test node configurations (id, port, latency_seconds)
TEST_NODE_CONFIGS = [
    ("fast-1", NODE_BASE_PORT, 0.01),
    ("fast-2", NODE_BASE_PORT + 1, 0.01),
    ("fast-3", NODE_BASE_PORT + 2, 0.015),
    ("medium-1", NODE_BASE_PORT + 3, 0.05),
    ("medium-2", NODE_BASE_PORT + 4, 0.06),
    ("slow-1", NODE_BASE_PORT + 5, 0.15),
    ("slow-2", NODE_BASE_PORT + 6, 0.2),
    ("veryslow-1", NODE_BASE_PORT + 7, 0.3),
]
