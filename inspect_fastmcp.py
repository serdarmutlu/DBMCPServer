
from fastmcp import FastMCP
import asyncio
from pydantic import BaseModel, Field

def inspect_fastmcp():
    server = FastMCP(name="Test")
    
    @server.tool()
    def my_tool(x: int, y: str = "default") -> str:
        """A test tool"""
        return f"{x} {y}"

    async def run_checks():
        print("Tools (Raw):")
        tools = await server.get_tools() if asyncio.iscoroutinefunction(server.get_tools) else server.get_tools()
        print(f"Type: {type(tools)}")
        print(f"Content: {tools}")
        
        if isinstance(tools, dict):
            for name, tool in tools.items():
                print(f"Tool Name: {name}")
                print(f"Tool Object: {tool}")
                print(f"Dir(tool): {dir(tool)}")
        elif isinstance(tools, list):
            for tool in tools:
                print(f"Tool Object: {tool}")
                print(f"Dir(tool): {dir(tool)}")

    asyncio.run(run_checks())

if __name__ == "__main__":
    inspect_fastmcp()
