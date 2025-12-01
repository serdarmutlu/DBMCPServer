from fastmcp import FastMCP
from db.metadata.metadata_repository_manager import repository_manager

def register_metadata_tools(mcpserver: FastMCP):
    @mcpserver.tool(
        name="list_all_connections_details",
        description="Gets list of all Postgresql connections defined in local database in MCP Server, returns in string format",
        tags={"postgresql", "metadata"}
    )
    async def list_all_connection_details():
        rows = await repository_manager.get_all_connections()
        return [dict(r) for r in rows] if rows else {"error": "Not found"}

    @mcpserver.tool(
        name="get_connection_detail",
        description="Gets detail of a single Postgresql connection defined in local database in MCP Server, returns in string format",
        tags={"postgresql", "metadata"}
    )
    async def get_connection_detail(connection_id: int):
        row = await repository_manager.get_connection(connection_id)
        return dict(row) if row else {"error": "Not found"}