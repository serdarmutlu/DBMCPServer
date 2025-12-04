# postgresql_manager.py
import asyncio
import logging
import asyncpg
from typing import Any, Dict, List, Optional
from .postgresql_connection import PostgresqlConnection
from db.metadata.metadata_repository_manager import repository_manager

logger = logging.getLogger(__name__)

class PostgresqlManager:
    """Çoklu veritabanı bağlantısını yönetir ve tool'lara hizmet eder."""

    def __init__(self):
        self.repository_manager = repository_manager
        self.connections: Dict[int, PostgresqlConnection] = {}   # id -> DatabaseConnection
        self.active_connection: int | None = None
        self._lock = asyncio.Lock()
        self._initialized = False

    async def _activate_single_connection(self, row):
        conn_id = int(row["id"] if "id" in row else row.get("connection_id", 0))

        full = await self.repository_manager.get_connection_with_password(conn_id)
        if not full:
            return False

        dbc = PostgresqlConnection(dict(full))
        logger.info("Trying to activate connection id: %s", conn_id)

        ok = await dbc.connect()
        if ok:
            self.connections[conn_id] = dbc
            await self.repository_manager.activate_connection(conn_id)
            logger.info("%s is activated", conn_id)
            return True

        return False


    # ------------------------------------------------------------------ #
    # Startup
    # ------------------------------------------------------------------ #
    async def initialize(self):
        if self._initialized:
            return

        async with self._lock:
            await self.repository_manager.deactivate_all_connections()
            rows = await self.repository_manager.get_all_connections(connect_at_startup=True)

            tasks = [
                self._activate_single_connection(row)
                for row in rows
            ]

            results = await asyncio.gather(*tasks, return_exceptions=True)

            active_count = sum(1 for r in results if r is True)
            self._initialized = True

            logger.info("✅ DB Manager initialized. Active pools: %s", active_count)

    async def close(self):
        """Uygulama kapanırken çağrılır."""
        for conn_id in list(self.connections.keys()):
            await self.disconnect(conn_id)
        self.connections.clear()

    # ------------------------------------------------------------------ #
    # Bağlantı yaşam döngüsü
    # ------------------------------------------------------------------ #
    async def connect_by_id(self, connection_id: int) -> bool:
        """Metadata’dan tekil kaydı şifreli al, havuz kur."""
        full = await self.repository_manager.get_connection_with_password(connection_id)
        if not full:
            logger.warning("⚠️ No metadata for id=%s", connection_id)
            return False
        dbc = PostgresqlConnection(dict(full))
        ok = await dbc.connect()
        if ok:
            self.connections[connection_id] = dbc
        return ok

    async def disconnect(self, connection_id: int):
        dbc = self.connections.get(connection_id)
        if dbc:
            await dbc.disconnect()
            self.connections.pop(connection_id, None)
            if self.active_connection == connection_id:
                self.active_connection = None

    async def reconnect(self, connection_id: int) -> bool:
        await self.disconnect(connection_id)
        return await self.connect_by_id(connection_id)

    # ------------------------------------------------------------------ #
    # Havuz/bağlantı erişimi
    # ------------------------------------------------------------------ #
    async def acquire(self, connection_id: int) -> asyncpg.Connection:
        """Sağlık kontrolü + acquire. Kopmuşsa toparla."""
        dbc = self.connections.get(connection_id)
        if not dbc:
            # Havuz kurulmamışsa kurmayı dene
            ok = await self.connect_by_id(connection_id)
            if not ok:
                raise RuntimeError(f"Cannot connect id={connection_id}")
            dbc = self.connections[connection_id]

        # Havuz sağlık kontrolü
        try:
            async with dbc.get_connection() as conn:
                await conn.execute("SELECT 1;")
                return conn
        except (asyncpg.InterfaceError, asyncpg.ConnectionDoesNotExistError):
            logger.warning("⚠️ Pool unhealthy. Reconnecting id=%s ...", connection_id)
            ok = await self.reconnect(connection_id)
            if not ok:
                raise RuntimeError(f"Reconnect failed id={connection_id}")
            # yeni havuzdan dön
            dbc = self.connections[connection_id]
            async with dbc.get_connection() as conn:
                await conn.execute("SELECT 1;")
                return conn

    async def execute_query(self, connection_id: int, sql: str, *params) -> List[Dict[str, Any]]:
        """Tool’lar için ana giriş noktası: SELECT/DDL/DML hepsi."""
        dbc = self.connections.get(connection_id)
        if not dbc:
            ok = await self.connect_by_id(connection_id)
            if not ok:
                raise RuntimeError(f"Cannot connect id={connection_id}")
            dbc = self.connections[connection_id]

        try:
            async with dbc.get_connection() as conn:
                verb = sql.lstrip().split()[0].upper()
                if verb == "SELECT":
                    rows = await conn.fetch(sql, *params)
                    return [dict(r) for r in rows]
                else:
                    # INSERT/UPDATE/DELETE/DDL
                    await conn.execute(sql, *params)
                    return []
        except (asyncpg.InterfaceError, asyncpg.ConnectionDoesNotExistError):
            # bir kez toparlanmayı deneyelim
            logger.warning("⚠️ Lost connection while running query. Reconnecting id=%s ...", connection_id)
            ok = await self.reconnect(connection_id)
            if not ok:
                raise
            async with self.connections[connection_id].get_connection() as conn:
                verb = sql.lstrip().split()[0].upper()
                if verb == "SELECT":
                    rows = await conn.fetch(sql, *params)
                    return [dict(r) for r in rows]
                else:
                    await conn.execute(sql, *params)
                    return []

    async def has_pgstattuple(self, connection_id: int) -> bool:
        sql = """
              SELECT 1
              FROM pg_extension
              WHERE extname = 'pgstattuple' \
              """
        rows = await postgresql_manager.execute_query(connection_id, sql)
        return bool(rows)

    async def check_bloat_fallback(
            self,
            connection_id: int,
            schema: str,
            table: str,
    ):
        sql = """
              SELECT n_live_tup, \
                     n_dead_tup, \
                     last_vacuum, \
                     last_autovacuum
              FROM pg_stat_user_tables
              WHERE schemaname = $1
                AND relname = $2 \
              """
        rows = await postgresql_manager.execute_query(
            connection_id,
            sql,
            schema, table
        )

        if not rows:
            return 0.0, None, None

        live = rows[0]["n_live_tup"]
        dead = rows[0]["n_dead_tup"]

        total = live + dead
        dead_pct = (dead / total * 100) if total > 0 else 0.0

        return dead_pct, rows[0]["last_vacuum"], rows[0]["last_autovacuum"]

    async def check_single_table_bloat(
            self,
            connection_id: int,
            schema: str,
            table: str,
            use_pgstattuple: bool):
        if use_pgstattuple:
            sql = """
                  SELECT dead_tuple_percent
                  FROM pgstattuple(
                          format('%I.%I', $1::text, $2::text)
                       ) \
                  """
            rows = await postgresql_manager.execute_query(
                connection_id,
                sql,
                schema, table
            )

            dead_pct = rows[0]["dead_tuple_percent"] if rows else 0.0

            return {
                "schema": schema,
                "table": table,
                "dead_tuple_percent": dead_pct,
                "source": "pgstattuple",
            }

        else:
            dead_pct, last_vacuum, last_autovacuum = await check_bloat_fallback(
                connection_id, schema, table
            )

            return {
                "schema": schema,
                "table": table,
                "dead_tuple_percent": dead_pct,
                "source": "pg_stat_user_tables",
                "last_vacuum": last_vacuum,
                "last_autovacuum": last_autovacuum,
            }

    # --- READ COLUMNS ---
    async def find_columns_by_table_name(self, connection_id: int, schema_name: str, table_name: str):
        sql = """
            SELECT * FROM information_schema.columns
            WHERE table_schema= $1 and table_name = $2
            ORDER BY ordinal_position
        """
        return await self.execute_query(connection_id, sql,schema_name, table_name)

    async def find_tables_by_schema_name(self, connection_id: int, schema_name: str):
        sql = """
            SELECT * FROM information_schema.tables
            WHERE table_schema = $1
            ORDER BY table_name
        """
        return await self.execute_query(connection_id, sql, schema_name)

    async def find_all_tables(self, connection_id: int):
        sql = "SELECT * FROM information_schema.tables ORDER BY table_schema, table_name"
        return await self.execute_query(connection_id, sql)

    async def execute_custom_query(self, connection_id: int, sql: str):
        return await self.execute_query(connection_id, sql)

    async def execute_custom_update(self, connection_id: int, sql: str):
        await self.execute_query(connection_id, sql)
        return {"status": "ok"}

    async def count_rows_in_table(self, connection_id: int, table_name: str):
        sql = f"SELECT COUNT(*) FROM {table_name}"
        rows = await self.execute_query(connection_id, sql)
        return rows[0]["count"] if rows else 0

    async def find_schemas_and_tables(
            self,
            connection_id: int,
            schema_name: str | None = None,
            table_name: str | None = None
    ):
        results = []

        # ✅ 1) Tek tablo
        if schema_name and table_name:
            return [(schema_name, table_name)]

        # ✅ 2) Schema veya tüm DB
        if schema_name:
            sql = """
                  SELECT table_schema, table_name
                  FROM information_schema.tables
                  WHERE table_type = 'BASE TABLE'
                    AND table_schema = $1 \
                  """
            rows = await self.execute_query(connection_id, sql, schema_name)
        else:
            sql = """
                  SELECT table_schema, table_name
                  FROM information_schema.tables
                  WHERE table_type = 'BASE TABLE'
                    AND table_schema NOT IN ('pg_catalog', 'information_schema') \
                  """
            rows = await self.execute_query(connection_id, sql)

        return [(r["table_schema"], r["table_name"]) for r in rows]

    async def get_database_sizes(self, connection_id: int) -> List[Dict[str, Any]]:
        """
        Returns a list of databases and their sizes for the given connection.
        """
        sql = """
            SELECT datname as database_name,
                   pg_size_pretty(pg_database_size(datname)) as size_pretty,
                   pg_database_size(datname) as size_bytes
            FROM pg_database
            ORDER BY pg_database_size(datname) DESC;
        """
        return await self.execute_query(connection_id, sql)

    async def collect_stats(
            self,
            connection_id: int,
            schema: str | None = None,
            table: str | None = None
    ) -> List[Dict[str, Any]]:
        """
        Collects statistics from pg_stat_user_tables.
        """
        base_sql = """
                   SELECT schemaname, \
                          relname, \
                          n_live_tup, \
                          n_dead_tup,
                          last_vacuum, \
                          last_autovacuum, \
                          last_analyze, \
                          last_autoanalyze
                   FROM pg_stat_user_tables \
                   """

        if schema and table:
            sql = base_sql + " WHERE schemaname = $1 AND relname = $2"
            return await self.execute_query(connection_id, sql, schema, table)
        elif schema:
            sql = base_sql + " WHERE schemaname = $1"
            return await self.execute_query(connection_id, sql, schema)
        else:
            return await self.execute_query(connection_id, base_sql)

    # ------------------------------------------------------------------ #
    # Yardımcılar
    # ------------------------------------------------------------------ #
    def list_pools(self) -> List[Dict[str, Any]]:
        out = []
        for cid, dbc in self.connections.items():
            info = dbc.connection_info
            out.append({
                "id": cid,
                "host": info["host"],
                "port": info["port"],
                "database": info["database_name"],
                "username": info["username"],
                "connected": dbc.connected,
                "active": (cid == self.active_connection),
            })
        return out

postgresql_manager = PostgresqlManager() #Singleton