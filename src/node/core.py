"""
Core UnifiedNode implementation
Async P2P node with CLIENT and WORKER modes
"""
import grpc.aio
import numpy as np
import pickle
import time
import uuid
import asyncio
import sys

sys.setrecursionlimit(10000)
import sys
import os

# Fix imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from config import *
import compute_pb2
import compute_pb2_grpc
from node.network import NetworkManager  # Changed from 'from network import'


class UnifiedNode:
    """Async network-aware P2P node"""
    
    def __init__(self, node_id, port, latency_simulation=0.0):
        self.node_id = node_id
        self.port = port
        self.status = "available"
        self.current_load = 0
        self.reserved_by = None
        self.server = None
        self.latency_simulation = latency_simulation
        
        # Network manager
        self.network = NetworkManager(self, latency_simulation)
        
        # Tracking
        self.peer_latencies = {}
        self.contribution_log = []
        
        print(f"[NODE {node_id}] Initializing async node (simulated latency: {latency_simulation*1000:.0f}ms)")
    
    async def start(self):
        """Async initialization"""
        await self._start_server()
        await asyncio.sleep(0.5)
        await self._register()
        print(f"[NODE {self.node_id}] Ready (async)")
    
    async def _start_server(self):
        """Start async gRPC server"""
        from .servicer import AsyncNodeServicer
        
        self.server = grpc.aio.server(options=GRPC_OPTIONS)
        compute_pb2_grpc.add_NodeServicer_to_server(
            AsyncNodeServicer(self), self.server
        )
        self.server.add_insecure_port(f'0.0.0.0:{self.port}')
        await self.server.start()
        print(f"[NODE {self.node_id}] Async server started on port {self.port}")
    
    async def _register(self):
        """Register with coordinator"""
        await self.network.register_with_coordinator()
    
    async def update_status(self, status):
        """Update status with coordinator"""
        self.status = status
        await self.network.update_status(status)
    
    async def compute_distributed(self, A, B, depth=0):
        """
        CLIENT MODE: Async distributed computation
        """
        print(f"\n[NODE {self.node_id}] CLIENT MODE [Depth {depth}]: Distributing A{A.shape} @ B{B.shape}")
        
        if depth >= MAX_RECURSION_DEPTH:
            result = np.matmul(A, B)
            return result
        
        # Mark busy
        await self.update_status("busy")
        self.current_load += 1
        
        # Get and check available nodes
        actually_available = await self.network.get_available_peers(DEFAULT_NUM_NODES)
        
        if not actually_available:
            self.current_load -= 1
            if self.current_load == 0:
                await self.update_status("available")
            return np.matmul(A, B)
        
        # Measure latencies and select best nodes
        good_nodes = await self.network.select_best_nodes(actually_available)
        
        if not good_nodes:
            self.current_load -= 1
            if self.current_load == 0:
                await self.update_status("available")
            return np.matmul(A, B)
        
        print(f"[NODE {self.node_id}] *** REPLICATION START: Replicating task to {len(good_nodes)} nodes ***")
        
        # Partition work
        A_blocks = np.array_split(A, len(good_nodes), axis=0)
        
        # Dispatch in parallel
        print(f"[NODE {self.node_id}] Dispatching {len(good_nodes)} tasks in PARALLEL (async)...")
        
        async def dispatch_to_node(node, A_block):
            task_id = f"task-{uuid.uuid4().hex[:8]}"
            
            if await self.network.reserve_peer(node['address'], node['node_id'], task_id):
                print(f"[NODE {self.node_id}] 📤 REPLICATE: Sending to {node['node_id']} (rows {A_block.shape[0]}) - {task_id}")
                
                try:
                    result = await self.network.send_task(node['address'], task_id, A_block, B, depth)
                    
                    self.contribution_log.append({
                        'node': self.node_id,
                        'delegated_to': node['node_id'],
                        'shape': A_block.shape,
                        'depth': depth,
                        'action': 'delegated'
                    })
                    
                    print(f"[NODE {self.node_id}] ✓ REPLICA COMPUTED: {task_id} completed by {node['node_id']}")
                    return result
                except Exception as e:
                    print(f"[NODE {self.node_id}] Failed: {e}")
                    return np.matmul(A_block, B)
            else:
                return np.matmul(A_block, B)
        
        dispatch_tasks = [dispatch_to_node(node, A_block) 
                         for node, A_block in zip(good_nodes, A_blocks)]
        
        results = await asyncio.gather(*dispatch_tasks)
        
        print(f"[NODE {self.node_id}] *** REPLICATION COMPLETE: All {len(results)} replicas computed ***")
        
        # Assemble
        final_result = np.vstack(results)
        
        # Mark available
        self.current_load -= 1
        if self.current_load == 0:
            await self.update_status("available")
        
        print(f"[NODE {self.node_id}] CLIENT MODE complete")
        return final_result
    
    def print_contribution_summary(self):
        """Print work distribution summary"""
        print(f"\n{'='*60}")
        print(f"Contribution Summary for {self.node_id}")
        print(f"{'='*60}")
        for entry in self.contribution_log:
            if entry['action'] == 'delegated':
                print(f"  Delegated {entry['shape']} to {entry['delegated_to']} [Depth {entry['depth']}]")
            else:
                print(f"  Computed {entry['shape']} locally [Depth {entry['depth']}]")
        print(f"{'='*60}\n")
    
    async def shutdown(self):
        """Async shutdown"""
        if self.server:
            await self.server.stop(0)


class MPINode(UnifiedNode):
    """MPI-enabled node inheriting from UnifiedNode"""
    
    def __init__(self, node_id, port, latency_simulation=0.0):
        super().__init__(node_id, port, latency_simulation)
        try:
            from mpi4py import MPI
            self.comm = MPI.COMM_WORLD
            self.rank = self.comm.Get_rank()
            self.size = self.comm.Get_size()
            print(f"[MPI-NODE {node_id}] Rank {self.rank} of {self.size}")
        except ImportError:
            print("[MPI-NODE] mpi4py not available")
            self.comm = None
    
    async def mpi_broadcast(self, data):
        """Broadcast data to all nodes"""
        if self.comm:
            return self.comm.bcast(data, root=0)
        return data
    
    async def mpi_scatter(self, data_list):
        """Scatter data across nodes"""
        if self.comm and self.size > 1:
            chunk_size = len(data_list) // self.size
            chunks = [data_list[i:i+chunk_size] for i in range(0, len(data_list), chunk_size)]
            return self.comm.scatter(chunks, root=0)
        return data_list
    
    async def mpi_gather(self, data):
        """Gather data from all nodes"""
        if self.comm:
            return self.comm.gather(data, root=0)
        return [data]


async def run_node_async(node_id, port, latency_sim=0.0):
    """Run async node"""
    node = UnifiedNode(node_id, port, latency_simulation=latency_sim)
    await node.start()
    
    try:
        await node.server.wait_for_termination()
    except KeyboardInterrupt:
        await node.shutdown()


def run_node(node_id, port, latency_sim=0.0):
    """Sync wrapper"""
    asyncio.run(run_node_async(node_id, port, latency_sim))


if __name__ == '__main__':
    import sys
    from config import NODE_BASE_PORT
    
    if len(sys.argv) > 1:
        node_id = sys.argv[1]
        port = int(sys.argv[2]) if len(sys.argv) > 2 else NODE_BASE_PORT
        latency = float(sys.argv[3]) if len(sys.argv) > 3 else 0.0
    else:
        node_id = "node-1"
        port = NODE_BASE_PORT
        latency = 0.0
    
    run_node(node_id, port, latency)