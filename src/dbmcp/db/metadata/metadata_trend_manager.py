import json
import logging
from typing import Optional, Any, List, Dict
from fastmcp import Client, FastMCP
from .metadata_connection import metadata_connection

logger = logging.getLogger(__name__)


class TrendManager:
    def __init__(self):
        pass

    async def initialize(self):
        # Metadata connection is managed globally
        pass

    async def close(self):
        pass

    # ✅ DATABASE CAPACITY SNAPSHOT
    async def add_database_capacity(
        self,
        dbname: str,
        size_bytes: int,
    ):
        sql = """
        INSERT INTO trends.pg_capacity_snapshots (
            scope, dbname, size_bytes
        ) VALUES (
            'db', $1, $2
        );
        """
        await metadata_connection.execute_query(sql, dbname, size_bytes)

    # ✅ TABLE CAPACITY SNAPSHOT
    async def add_table_capacity(
            self,
            dbname: str,
            schemaname: str,
            relname: str,
            size_bytes: int,
    ):
        sql = """
              INSERT INTO trends.pg_capacity_snapshots (scope, dbname, schemaname, relname, size_bytes) \
              VALUES ('table', $1, $2, $3, $4); \
              """
        await metadata_connection.execute_query(sql, dbname, schemaname, relname, size_bytes)

    async def execute_query(self, sql: str, *params) -> List[Dict[str, Any]]:
        # This was a generic helper, redirect to metadata_connection's execute_query
        return await metadata_connection.execute_query(sql, *params, fetch_all=True) or []

    async def execute_dml(self, sql: str, *params) -> List[Dict[str, Any]]:
        # This was a generic helper, redirect to metadata_connection's execute_query
        await metadata_connection.execute_query(sql, *params)
        return []

trend_manager = TrendManager()  # Singleton
