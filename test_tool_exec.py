
from fastmcp import FastMCP
import asyncio
from fastmcp import Context

def test_tool_execution():
    server = FastMCP(name="Test")
    
    @server.tool()
    async def add(a: int, b: int) -> int:
        return a + b

    async def run_checks():
        tool = await server.get_tool("add") if asyncio.iscoroutinefunction(server.get_tool) else server.get_tool("add")
        result = await tool.run({"a": 10, "b": 20})
        print(f"Result type: {type(result)}")
        print(f"Result dir: {dir(result)}")
        # Check standard MCP keys
        if hasattr(result, 'content'):
            print(f"Content: {result.content}")
        if hasattr(result, 'isError'):
            print(f"isError: {result.isError}")

    asyncio.run(run_checks())

if __name__ == "__main__":
    test_tool_execution()
