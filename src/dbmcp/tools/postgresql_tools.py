import asyncio

from db.postgresql.postgresql_manager import postgresql_manager

from fastmcp import FastMCP


def register_postgresql_tools(mcpserver: FastMCP):

    @mcpserver.tool(
        name="list-all-tables",
        description="List all tables in connected Postgresql database",
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
        name="check-table-bloat",
        description="""
        Check PostgreSQL table bloat using pgstattuple.

        - If schema & table specified → single table
        - If only schema specified → all tables in schema
        - If none specified → all user tables
        Optionally evaluates against a bloat threshold.
        """,
    )
    async def check_table_bloat(connection_id: int, schema_name:str | None = None, table_name: str | None = None):
        use_pgstattuple = await postgresql_manager.has_pgstattuple(connection_id)

        tables = await postgresql_manager.find_schemas_and_tables(connection_id, schema_name, table_name)
        results = await asyncio.gather(
            *(
                postgresql_manager.check_single_table_bloat(
                    connection_id,
                    s, t,
                    use_pgstattuple
                )
                for s, t in tables
            )
        )

        return results


    @mcpserver.tool(
        name="database-size",
        description="Find the size of all accessible Postgresql databases",
        tags={"postgresql"}
    )
    async def check_database_size(connection_id: int):
        return await postgresql_manager.get_database_sizes(connection_id)


    @mcpserver.tool(
        name="collect-statistics",
        description="""
        Collect statistics for tables (row counts, vacuum info).
        - If schema & table specified → single table
        - If only schema specified → all tables in schema
        - If none specified → all user tables
        """,
        tags={"postgresql"}
    )
    async def collect_statistics(connection_id: int, schema_name: str | None = None, table_name: str | None = None):
        return await postgresql_manager.collect_stats(connection_id, schema_name, table_name)


    @mcpserver.tool(
        name="query",
        description="Run a raw SQL query on the connected database",
        tags={"postgresql"}
    )
    async def run_query(connection_id: int, query: str):
        return await postgresql_manager.execute_custom_query(connection_id, query)
