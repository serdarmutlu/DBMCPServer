from fastmcp import FastMCP, Context
from db.metadata_manager import MetadataManager
from starlette.responses import JSONResponse
from db.metadata_manager import get_metadata_manager_instance

def register_metadata_tools(mcpserver: FastMCP):
    metadata_manager = get_metadata_manager_instance()
    @mcpserver.tool(
        name="list_all_connections_details",
        description="Gets list of all Postgresql connections defined in local database in MCP Server, returns in string format",
        tags={"postgresql", "metadata"}
    )
    async def list_all_connection_details():
        rows = await metadata_manager.get_all_connections()
        return [dict(r) for r in rows] if rows else {"error": "Not found"}

    @mcpserver.tool(
        name="get_connection_detail",
        description="Gets detail of a single Postgresql connection defined in local database in MCP Server, returns in string format",
        tags={"postgresql", "metadata"}
    )
    async def get_connection_detail(connection_id: int):
        row = await metadata_manager.get_connection(connection_id)
        return dict(row) if row else {"error": "Not found"}