import asyncpg
import logging
from contextlib import asynccontextmanager
from config.settings import get_settings
from db.encryption import decrypt_password   # <- mevcut dosyandaki fonksiyon
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

class PostgresqlConnection:
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
                min_size=get_settings().db_pool_min_size,
                max_size=get_settings().db_pool_max_size,
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