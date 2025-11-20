"""Unified CLI"""
import sys


def main():
    """Main CLI entry point"""
    if len(sys.argv) < 2:
        print("Usage:")
        print("  dc coordinator              - Start coordinator")
        print("  dc node <id> <port> [lat]  - Start a node")
        print("  dc test                    - Run full test suite")
        return
    
    command = sys.argv[1]
    
    if command == "coordinator":
        from coordinator import serve
        serve()
    
    elif command == "node":
        if len(sys.argv) < 4:
            print("Usage: dc node <node_id> <port> [latency]")
            return
        
        node_id = sys.argv[2]
        port = int(sys.argv[3])
        latency = float(sys.argv[4]) if len(sys.argv) > 4 else 0.0
        
        from node.core import run_node
        run_node(node_id, port, latency)
    
    elif command == "test":
        import subprocess
        result = subprocess.run([sys.executable, "-m", "tests.run_all"])
        sys.exit(result.returncode)
    
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == '__main__':
    main()
