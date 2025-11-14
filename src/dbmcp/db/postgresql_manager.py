# postgresql_manager.py
import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional
from .metadata_manager import get_metadata_manager_instance

import asyncpg
from .encryption import decrypt_password   # <- mevcut dosyandaki fonksiyon
logger = logging.getLogger(__name__)


class DatabaseConnection:
    """Tek bir veritabanı bağlantısı için havuz sarmalayıcı."""

    def __init__(self, connection_info: Dict[str, Any]):
        # connection_info içinde: id, host, port, database_name, username, encrypted_password, ...
        self.connection_info = connection_info
        self.pool: Optional[asyncpg.Pool] = None
        self.connected: bool = False

    async def connect(self) -> bool:
        """Havuzu kur ve test et."""
        try:
            password = None
            enc = self.connection_info.get("encrypted_password")
            if enc:
                password = decrypt_password(enc)

            # DİKKAT: asyncpg'de database parametresi 'database' adıdır.
            self.pool = await asyncpg.create_pool(
                host=self.connection_info["host"],
                port=int(self.connection_info["port"]),
                database=self.connection_info["database_name"],
                user=self.connection_info["username"],
                password=password,
                min_size=1,
                max_size=5,
                timeout=10.0,
                statement_cache_size=1024,
            )

            # Basit sağlık kontrolü
            async with self.pool.acquire() as conn:
                await conn.execute("SELECT 1;")

            self.connected = True
            logger.info(
                "🔗 Connected: %s@%s:%s/%s [id=%s]",
                self.connection_info["username"],
                self.connection_info["host"],
                self.connection_info["port"],
                self.connection_info["database_name"],
                self.connection_info.get("id"),
            )
            return True
        except Exception as e:
            self.connected = False
            logger.error("❌ Connection failed for id=%s: %s", self.connection_info.get("id"), e)
            return False

    async def disconnect(self):
        if self.pool:
            await self.pool.close()
        self.pool = None
        self.connected = False
        logger.info("🔌 Disconnected id=%s", self.connection_info.get("id"))

    @asynccontextmanager
    async def get_connection(self):
        if not self.pool:
            raise RuntimeError("Pool not initialized")
        conn = await self.pool.acquire()
        try:
            yield conn
        finally:
            await self.pool.release(conn)


class PostgresqlManager:
    """Çoklu veritabanı bağlantısını yönetir ve tool'lara hizmet eder."""

    def __init__(self):
        self.metadata_manager = get_metadata_manager_instance()
        self.connections: Dict[int, DatabaseConnection] = {}   # id -> DatabaseConnection
        self.active_connection: Optional[int] = None
        self._lock = asyncio.Lock()
        self._initialized = False

    # ------------------------------------------------------------------ #
    # Startup
    # ------------------------------------------------------------------ #
    async def initialize(self):
        """Uygulama açılışında çağrılır: connect_at_startup olanlara bağlanır."""
        if self._initialized:
            return
        async with self._lock:
            await self.metadata_manager.deactivate_all_connections()
            rows = await self.metadata_manager.get_all_connections(connect_at_startup=True)
            # list_connections şifre getirmez; tekil çağrıda şifreli alanı alacağız
            for row in rows:
                conn_id = int(row["id"] if "id" in row else row.get("connection_id", 0))
                full = await self.metadata_manager.get_connection_with_password(conn_id)
                if not full:
                    continue
                # (İstersen db-type kontrolü ekle: only PostgreSQL)
                dbc = DatabaseConnection(dict(full))
                print(f"Trying to activate connection id:{conn_id}")
                ok = await dbc.connect()
                if ok:
                    self.connections[conn_id] = dbc
                    await self.metadata_manager.activate_connection(conn_id)
                    print(f"{conn_id} is activated")
            self._initialized = True
            logger.info("✅ DB Manager initialized. Active pools: %s", len(self.connections))

    async def cleanup(self):
        """Uygulama kapanırken çağrılır."""
        for conn_id in list(self.connections.keys()):
            await self.disconnect(conn_id)
        self.connections.clear()

    # ------------------------------------------------------------------ #
    # Bağlantı yaşam döngüsü
    # ------------------------------------------------------------------ #
    async def connect_by_id(self, connection_id: int) -> bool:
        """Metadata’dan tekil kaydı şifreli al, havuz kur."""
        full = await self.metadata_manager.get_connection_with_password(connection_id)
        if not full:
            logger.warning("⚠️ No metadata for id=%s", connection_id)
            return False
        dbc = DatabaseConnection(dict(full))
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

_postgresql_manager = PostgresqlManager()

def get_postgresql_manager_instance() -> PostgresqlManager:
    return _postgresql_manager

async def initialize_postgresql_manager():
    await _postgresql_manager.initialize()

async def close_postgresql_manager():
    await _postgresql_manager.cleanup()