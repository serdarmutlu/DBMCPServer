
import fastmcp
import inspect

def inspect_fastmcp_context():
    print(f"FastMCP dir: {dir(fastmcp)}")
    if hasattr(fastmcp, 'Context'):
        print(f"Context dir: {dir(fastmcp.Context)}")
    
    # Try to find the context var
    # It might be in fastmcp.types or fastmcp.server.context
    pass

if __name__ == "__main__":
    inspect_fastmcp_context()
