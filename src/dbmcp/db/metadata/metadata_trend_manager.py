import json

import asyncpg
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from typing import Optional, Any, List, Dict
from asyncpg.pool import Pool
from fastmcp import Client, FastMCP

from .metadata_connection import metadata_connection

import logging

logger = logging.getLogger(__name__)


class TrendManager:
    def __init__(self):
        self._metadata_connection_pool: Optional[Pool] = None

    async def initialize(self):
        self._metadata_connection_pool = metadata_connection.get_pool()

    async def close(self):
        self._metadata_connection_pool = None

    # ✅ DATABASE CAPACITY SNAPSHOT
    async def add_database_capacity(
        self,
        dbname: str,
        size_bytes: int,
    ):
        if not self._metadata_connection_pool:
            raise RuntimeError("TrendManager not initialized")

        sql = """
        INSERT INTO trends.pg_capacity_snapshots (
            scope, dbname, size_bytes
        ) VALUES (
            'db', $1, $2
        );
        """

        async with self._metadata_connection_pool.acquire() as conn:
            await conn.execute(sql, dbname, size_bytes)

    # ✅ TABLE CAPACITY SNAPSHOT
    async def add_table_capacity(
            self,
            dbname: str,
            schemaname: str,
            relname: str,
            size_bytes: int,
    ):
        if not self._metadata_connection_pool:
            raise RuntimeError("TrendManager not initialized")

        sql = """
              INSERT INTO trends.pg_capacity_snapshots (scope, dbname, schemaname, relname, size_bytes) \
              VALUES ('table', $1, $2, $3, $4); \
              """

        async with self._metadata_connection_pool.acquire() as conn:
            await conn.execute(
                sql,
                dbname,
                schemaname,
                relname,
                size_bytes
            )

    async def execute_query(self, sql: str, *params) -> List[Dict[str, Any]]:
        try:
            async with self._metadata_connection_pool.acquire() as conn:
                rows = await conn.fetch(sql, *params)
                return [dict(r) for r in rows]

        except (asyncpg.InterfaceError, asyncpg.ConnectionDoesNotExistError):
            # bir kez toparlanmayı deneyelim
            logger.warning("⚠️ Lost connection while running query. ")

    async def execute_dml(self, sql: str, *params) -> List[Dict[str, Any]]:
        try:
            async with self._metadata_connection_pool.acquire() as conn:
                # INSERT/UPDATE/DELETE/DDL
                await conn.execute(sql, *params)
                return []
        except (asyncpg.InterfaceError, asyncpg.ConnectionDoesNotExistError):
            # bir kez toparlanmayı deneyelim
            logger.warning("⚠️ Lost connection while running query. ")

trend_manager = TrendManager()  # Singleton


# CREATE TABLE IF NOT EXISTS trends.pg_capacity_snapshots (
#     snapshot_ts     timestamptz NOT NULL DEFAULT now(),
#     scope           text        NOT NULL, -- 'db' | 'table' | 'index'
#     dbname          text        NOT NULL,
#     schemaname      text,
#     relname         text,
#     size_bytes      bigint      NOT NULL
# );
#
# CREATE INDEX IF NOT EXISTS ix_pg_capacity_snapshots_ts
#     ON pg_capacity_snapshots(snapshot_ts);
#
# CREATE INDEX IF NOT EXISTS ix_pg_capacity_snapshots_scope
#     ON pg_capacity_snapshots(scope, dbname);
