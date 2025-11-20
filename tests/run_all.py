"""
Unified Test Runner
Starts coordinator, spawns nodes, runs tests
"""
import subprocess
import time
import sys
import os
import signal
import atexit

# Add src to path FIRST
src_path = os.path.join(os.path.dirname(__file__), '..', 'src')
sys.path.insert(0, src_path)


class TestEnvironment:
    """Manages test environment"""
    
    def __init__(self):
        self.processes = []
        self.coordinator_proc = None
        self.src_dir = os.path.join(os.path.dirname(__file__), '..', 'src')
    
    def start_coordinator(self):
        """Start coordinator"""
        print("\n" + "="*60)
        print("1. Starting Coordinator...")
        print("="*60)
        
        proc = subprocess.Popen(
            [sys.executable, os.path.join(self.src_dir, 'coordinator.py')],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1
        )
        self.coordinator_proc = proc
        time.sleep(2)
        print("✓ Coordinator started on port 50051\n")
    
    def spawn_test_nodes(self):
        """Spawn all test nodes"""
        print("="*60)
        print("2. Spawning Test Nodes...")
        print("="*60)
        
        # Import config here after path is set
        from config import TEST_NODE_CONFIGS, NODE_BASE_PORT
        
        node_script = os.path.join(self.src_dir, 'node', 'core.py')
        
        for node_id, port, latency in TEST_NODE_CONFIGS:
            print(f"  Starting {node_id:12} port {port}  latency {latency*1000:>3.0f}ms")
            
            proc = subprocess.Popen(
                [sys.executable, node_script, node_id, str(port), str(latency)],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                env={**os.environ, 'PYTHONPATH': self.src_dir}
            )
            self.processes.append((node_id, proc))
            time.sleep(0.3)
        
        print(f"\n✓ All {len(TEST_NODE_CONFIGS)} nodes started\n")
        time.sleep(2)
    
    def run_pytest(self):
        """Run pytest suite"""
        print("="*60)
        print("3. Running Pytest Suite...")
        print("="*60 + "\n")
        
        tests_dir = os.path.dirname(__file__)
        
        result = subprocess.run(
            [sys.executable, "-m", "pytest", tests_dir, "-v", "--tb=short"],
            env={**os.environ, 'PYTHONPATH': self.src_dir}
        )
        
        return result.returncode
    
    def run_integration_tests(self):
        """Run integration tests"""
        print("\n" + "="*60)
        print("4. Running Integration Tests...")
        print("="*60 + "\n")
        
        import asyncio
        import numpy as np
        from node import UnifiedNode
        
        async def test_distributed_matmul():
            # Start test client on port 50080 (avoiding worker ports 50061-50070)
            print("Starting test client...")
            client = UnifiedNode("test-client", 50080, latency_simulation=0.0)
            await client.start()
            await asyncio.sleep(2)
            
            print("Running distributed matrix multiplication (2000x2000)...")
            A = np.random.rand(2000, 2000)
            B = np.random.rand(2000, 2000)
            
            start = time.time()
            result = await client.compute_distributed(A, B)
            elapsed = time.time() - start
            
            print(f"\n✓ Matrix multiplication complete!")
            print(f"  Shape: {result.shape}")
            print(f"  Time: {elapsed:.2f}s")
            print(f"  Verify shape is correct: {result.shape == (2000, 2000)}")
            
            client.print_contribution_summary()
            
            await client.shutdown()
            return result.shape == (2000, 2000)
        
        try:
            success = asyncio.run(test_distributed_matmul())
            if success:
                print("\n✓ Integration tests passed!\n")
                return 0
            else:
                print("\n✗ Integration test failed: wrong result shape\n")
                return 1
        except Exception as e:
            print(f"\n✗ Integration test failed: {e}\n")
            import traceback
            traceback.print_exc()
            return 1
    
    def cleanup(self):
        """Stop all processes"""
        print("\n" + "="*60)
        print("Cleaning up...")
        print("="*60)
        
        # Stop all node processes
        for node_id, proc in self.processes:
            try:
                proc.terminate()
                proc.wait(timeout=2)
            except:
                try:
                    proc.kill()
                except:
                    pass
        
        # Stop coordinator
        if self.coordinator_proc:
            try:
                self.coordinator_proc.terminate()
                self.coordinator_proc.wait(timeout=2)
            except:
                try:
                    self.coordinator_proc.kill()
                except:
                    pass
        
        print("✓ All processes stopped")


def main():
    """Main test runner"""
    print("\n" + "="*60)
    print("DISTRIBUTED COMPUTING TEST SUITE")
    print("="*60)
    
    env = TestEnvironment()
    
    # Register cleanup
    def cleanup_handler():
        try:
            env.cleanup()
        except:
            pass
    
    atexit.register(cleanup_handler)
    
    try:
        # Start environment
        env.start_coordinator()
        env.spawn_test_nodes()
        
        # Run pytest
        pytest_result = env.run_pytest()
        
        # Run integration tests
        integration_result = env.run_integration_tests()
        
        # Summary
        print("="*60)
        print("TEST SUMMARY")
        print("="*60)
        print(f"Pytest:      {'✓ PASSED' if pytest_result == 0 else '✗ FAILED'}")
        print(f"Integration: {'✓ PASSED' if integration_result == 0 else '✗ FAILED'}")
        print("="*60 + "\n")
        
        return pytest_result + integration_result
    
    except KeyboardInterrupt:
        print("\n\nTest interrupted by user")
        return 1
    
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    finally:
        cleanup_handler()


if __name__ == '__main__':
    sys.exit(main())
