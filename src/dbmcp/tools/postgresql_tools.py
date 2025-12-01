from db.postgresql.postgresql_manager import postgresql_manager

from fastmcp import FastMCP

def register_postgresql_tools(mcpserver: FastMCP):

    @mcpserver.tool(
        name="list-all-tables",
        description="List all tables in connected Postgresql",
        tags={"postgresql"}
    )
    async def list_all_tables(connection_id: int):
        return await postgresql_manager.find_all_tables(connection_id)

    @mcpserver.tool(
        name="list-all-tables-in-schema",
        description="List all tables in a given schema",
        tags={"postgresql"}
    )
    async def list_all_tables_in_schema(connection_id: int, schema_name: str):
        return await postgresql_manager.find_tables_by_schema_name(connection_id, schema_name)

    @mcpserver.tool(
        name="list-all-columns-in-table",
        description="List all columns in a specified table",
        tags={"postgresql"}
    )
    async def list_all_columns(connection_id: int, schema_name:str, table_name: str):
        return await postgresql_manager.find_columns_by_table_name(connection_id, schema_name, table_name)

    @mcpserver.tool(
        name="check-bloat",
        description="Checks bloating in a given table",
        tags={"postgresql"}
    )
    async def check_bloat(connection_id: int, schema_name:str, table_name: str):
        return await postgresql_manager.check_bloat(connection_id, schema_name, table_name)

    @mcpserver.tool(
        name="database-size",
        description="Find the size of a given Postgresql database",
        tags={"postgresql"}
    )
    async def check_bloat(connection_id: int, schema_name:str, table_name: str):
        return await postgresql_manager.check_bloat(connection_id, schema_name, table_name)

    @mcpserver.tool(
        name="query",
        description="Run a raw SQL query on the connected database",
        tags={"postgresql"}
    )
    async def run_query(connection_id: int, query: str):
        return await postgresql_manager.execute_custom_query(connection_id, query)
