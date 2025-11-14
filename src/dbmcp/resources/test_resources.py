
from fastmcp import FastMCP

def register_test_resources(mcpserver: FastMCP):
    @mcpserver.resource("data://config")
    def get_config() -> dict:
        """Provides the application configuration."""
        return {"theme": "dark", "version": "1.0"}