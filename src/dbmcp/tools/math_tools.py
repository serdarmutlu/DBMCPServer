from fastmcp import FastMCP, Context

def register_math_tools(mcpserver: FastMCP):
    @mcpserver.tool(
        name="Add",
        description="Adds two numbers together",
        tags={"generic","math"}
    )
    def add(a: int, b: int, ctx: Context) -> int:
        return a + b

    @mcpserver.tool(
        name="Multiply",
        description="Multiplies two numbers",
        tags={"generic","math"}
    )
    def multiply(a: int, b: int) -> int:
        return a * b
