
from fastmcp import FastMCP, Context
import asyncio

def test_context_injection():
    server = FastMCP(name="Test")
    
    @server.tool()
    async def add(a: int, b: int) -> int:
        return a + b

    async def run_checks():
        tool = await server.get_tool("add") if asyncio.iscoroutinefunction(server.get_tool) else server.get_tool("add")
        
        print("Trying with context...")
        try:
            # Manually setting context
            # NOTE: FastMCP implementation details might vary.
            # Checking if Context has set_current or similar
            # In some versions it uses contextvars directly.
            
            # Let's try internal call_tool if possible
            if hasattr(server, 'call_tool'):
                 print("Using server.call_tool...")
                 # result = await server.call_tool("add", arguments={"a": 1, "b": 2}) # unknown signature
                 pass

            # Try setting contextvar
            # Context is likely a pydantic model or dataclass, but the contextvar is internal to the module usually.
            # Check fastmcp dict for ContextVar
            
            # If we simply run it, we get "No active context found".
            # We need to find where "No active context found" is raised. Likely Context.get_current() or properties.
            pass

        except Exception as e:
            print(f"Error: {e}")

    asyncio.run(run_checks())

if __name__ == "__main__":
    test_context_injection()
