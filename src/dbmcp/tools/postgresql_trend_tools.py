# tools/postgresql_observability_tools.py

import json
from typing import Optional

from fastmcp import FastMCP, Context
from fastmcp.tools.tool import ToolResult
from mcp import types as mt

from db.postgresql.postgresql_manager import postgresql_manager
from db.metadata.metadata_trend_manager import trend_manager


def _text_result(payload: dict | list, title: Optional[str] = None) -> ToolResult:
    """JSON çıktıyı TextContent olarak döndürmek için yardımcı fonksiyon."""
    text = json.dumps(payload, indent=2, default=str)
    if title:
        text = f"# {title}\n\n```json\n{text}\n```"
    else:
        text = f"```json\n{text}\n```"

    return ToolResult(
        content=[
            mt.TextContent(type="text", text=text)
        ]
    )

def register_postgresql_trend_tools(mcp: FastMCP) -> None:
    @mcp.tool(
        name="pg_capacity_growth_trend",
        description="Kapasite büyüme trendi (MB/gün), yüzde artış ve basit forecast üretir."
    )
    async def pg_capacity_growth_trend(
            ctx: Context,
            connection_id: int,
            scope: str = "db",  # 'db' | 'table'
            days: int = 30,
            limit: int = 10,
    ) -> ToolResult:
        # --- Güvenli param normalizasyonu
        days = max(1, min(days, 365))
        limit = max(1, min(limit, 100))

        if scope == "db":
            sql = """
                  WITH base AS (SELECT snapshot_ts::date AS d, max(size_bytes) AS size_bytes \
                                FROM trends.pg_capacity_snapshots \
                                WHERE scope = 'db' \
                                  AND dbname = current_database() \
                                  AND snapshot_ts >= now() - ($1 || ' days'):: interval
                  GROUP BY 1
                  ORDER BY 1
                      ),
                      agg AS (
                  SELECT
                      min (d) AS start_date, max (d) AS end_date, min (size_bytes) AS start_size, max (size_bytes) AS end_size, count (*) AS points
                  FROM base
                      )
                  SELECT start_date, \
                         end_date, \
                         start_size, \
                         end_size, \
                         points, \
                         round((end_size - start_size) / 1024.0 / 1024.0, 2)                           AS growth_mb, \
                         round(((end_size - start_size):: numeric / nullif (start_size,0))*100,2)      AS growth_pct, \
                         round(((end_size - start_size) / 1024.0 / 1024.0) / nullif(points - 1, 1), \
                               2)                                                                      AS growth_mb_per_day
                  FROM agg; \
                  """

            rows = await trend_manager.execute_query(sql, str(days))
            return _text_result(rows, title="Capacity Growth Trend (Database)")

        # ---- TABLE SCOPE ----
        sql = """
              WITH base AS (SELECT schemaname, \
                                   relname, \
                                   snapshot_ts::date AS d, max(size_bytes) AS size_bytes \
                            FROM trends.pg_capacity_snapshots \
                            WHERE scope = 'table' \
                              AND dbname = current_database() \
                              AND snapshot_ts >= now() - ($1 || ' days'):: interval
              GROUP BY 1, 2, 3
                  ),
                  agg AS (
              SELECT
                  schemaname, relname, min (d) AS start_date, max (d) AS end_date, min (size_bytes) AS start_size, max (size_bytes) AS end_size, count (*) AS points
              FROM base
              GROUP BY 1, 2
                  )
              SELECT schemaname, \
                     relname, \
                     start_date, \
                     end_date, \
                     round((end_size - start_size) / 1024.0 / 1024.0, 2)                           AS growth_mb, \
                     round(((end_size - start_size):: numeric / nullif (start_size,0))*100,2)      AS growth_pct, \
                     round(((end_size - start_size) / 1024.0 / 1024.0) / nullif(points - 1, 1), 2) AS growth_mb_per_day
              FROM agg
              ORDER BY growth_mb DESC
                  LIMIT $2; \
              """

        rows = await trend_manager.execute_query(sql, str(days), limit)
        return _text_result(rows, title=f"Capacity Growth Trend (Top {limit} Tables)")

    @mcp.tool(
        name="pg_capacity_growth_trend_database_capacity_insert",
        description="Veritabanı Kapasite bilgilerini kapasite tablosuna ekler"
    )
    async def pg_capacity_growth_trend_database_capacity_insert(
            ctx: Context,
            connection_id: int):
        snap_sql = """
        SELECT
            current_database() AS dbname,
            pg_database_size(current_database()) AS size_bytes;
        """
        rows = await postgresql_manager.execute_query(connection_id, snap_sql)

        if rows:
            await trend_manager.add_database_capacity(
                dbname=rows[0]["dbname"],
                size_bytes=rows[0]["size_bytes"]
            )

    @mcp.tool(
        name="pg_capacity_growth_trend_table_capacity_insert",
        description="Tablo Kapasite bilgilerini kapasite tablosuna ekler"
    )
    async def pg_capacity_growth_trend_table_capacity_insert(
            ctx: Context,
            connection_id: int):
        snap_tbl_sql = """
                       SELECT current_database()                                           AS dbname, \
                              schemaname, \
                              relname, \
                              pg_total_relation_size(format('%I.%I', schemaname, relname)) AS size_bytes
                       FROM pg_stat_user_tables; \
                       """

        rows = await postgresql_manager.execute_query(connection_id, snap_tbl_sql)

        for r in rows:
            await trend_manager.add_table_capacity(
                dbname=r["dbname"],
                schemaname=r["schemaname"],
                relname=r["relname"],
                size_bytes=r["size_bytes"]
            )