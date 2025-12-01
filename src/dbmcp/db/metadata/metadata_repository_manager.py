from typing import Optional

from asyncpg.pool import Pool
from config.settings import get_settings
from .metadata_connection import metadata_connection
import logging

logger = logging.getLogger(__name__)

class RepositoryManager:

    def __init__(self):
        self._metadata_connection_pool: Optional[Pool] = None

    async def initialize(self):
        self._metadata_connection_pool = metadata_connection.get_pool()

    async def close(self):
        self._metadata_connection_pool = None

    # --- database_types ---
    async def get_all_types(self):
        async with self._metadata_connection_pool.acquire() as conn:
            return await conn.fetch("SELECT * FROM repository.database_types ORDER BY id")

    async def get_type(self, id:int):
        async with self._metadata_connection_pool.acquire() as conn:
            return await conn.fetchrow("SELECT * FROM repository.database_types WHERE id=$1", id)

    async def add_type(self, name: str):
        async with self._metadata_connection_pool.acquire() as conn:
            return await conn.fetchrow(
                "INSERT INTO repository.database_types (name) VALUES ($1) RETURNING *", name
            )

    async def update_type(self, id: int, data: dict):
        async with self._metadata_connection_pool.acquire() as conn:
            return await conn.fetchrow("""
                UPDATE repository.database_types
                SET name=$1
                WHERE id=$2
                RETURNING *
            """,
            data["name"], id)

    async def delete_type(self, id: int):
        async with self._metadata_connection_pool.acquire() as conn:
            await conn.execute("DELETE FROM repository.database_types WHERE id = $1", id)


    # --- database_connections ---
    async def get_all_connections(self, connect_at_startup: bool = None):
        async with self._metadata_connection_pool.acquire() as conn:
            query = """
                SELECT c.id, c.database_type_id, t.name AS database_type_name, c.host, c.port, c.database_name,
                    c.username, c.is_active, c.description, c.connect_at_startup
                FROM repository.database_connections c
                LEFT JOIN repository.database_types t ON c.database_type_id = t.id
            """
            params = []
            if connect_at_startup is not None:
                query += " WHERE c.connect_at_startup = $1"
                params.append(connect_at_startup)
            query += " ORDER BY t.name, c.host, c.port, c.database_name"
            return await conn.fetch(query, *params)

    async def get_connection(self, connection_id: int):
        async with self._metadata_connection_pool.acquire() as conn:
            return await conn.fetchrow("""
                SELECT  c.id, c.database_type_id, t.name AS database_type_name, c.host, c.port, c.database_name,
                    c.username, c.is_active, c.description, c.connect_at_startup
                FROM repository.database_connections c
                LEFT JOIN repository.database_types t ON c.database_type_id = t.id
                WHERE c.id = $1
                ORDER BY t.name, c.host, c.port, c.database_name
            """, connection_id)

    async def get_connection_with_password(self, connection_id: int):
        async with self._metadata_connection_pool.acquire() as conn:
            return await conn.fetchrow("""
                SELECT  c.id, c.database_type_id, t.name AS database_type_name, c.host, c.port, c.database_name,
                    c.username, c.encrypted_password, c.is_active, c.description, c.connect_at_startup
                FROM repository.database_connections c
                LEFT JOIN repository.database_types t ON c.database_type_id = t.id
                WHERE c.id = $1
                ORDER BY t.name, c.host, c.port, c.database_name
            """, connection_id)

    async def add_connection(self, data: dict):
        async with self._metadata_connection_pool.acquire() as conn:
            return await conn.fetchrow("""
                INSERT INTO repository.database_connections
                (database_type_id, host, port, database_name, username,
                 encrypted_password, is_active, description, connect_at_startup)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
                RETURNING *
            """,
            data["database_type_id"], data["host"], data["port"],
            data["database_name"], data["username"], data["encrypted_password"],
            data.get("is_active", True), data.get("description"),
            data.get("connect_at_startup", True))

    async def update_connection(self, connection_id: int, data: dict):
        async with self._metadata_connection_pool.acquire() as conn:
            return await conn.fetchrow("""
                UPDATE repository.database_connections
                SET database_type_id=$1, host=$2, port=$3, database_name=$4,
                    username=$5, encrypted_password=$6, is_active=$7,
                    description=$8, connect_at_startup=$9
                WHERE id=$10
                RETURNING *
            """,
            data["database_type_id"], data["host"], data["port"],
            data["database_name"], data["username"], data["encrypted_password"],
            data.get("is_active", True), data.get("description"),
            data.get("connect_at_startup", False), connection_id)

    async def update_connection_no_password(self, connection_id: int, data: dict):
        async with self._metadata_connection_pool.acquire() as conn:
            return await conn.fetchrow("""
                UPDATE repository.database_connections
                SET database_type_id=$1, host=$2, port=$3, database_name=$4,
                    username=$5, is_active=$6,
                    description=$7, connect_at_startup=$8
                WHERE id=$9
                RETURNING *
            """,
            data["database_type_id"], data["host"], data["port"],
            data["database_name"], data["username"],
            data.get("is_active", True), data.get("description"),
            data.get("connect_at_startup", False), connection_id)

    async def delete_connection(self, connection_id: int):
        async with self._metadata_connection_pool.acquire() as conn:
            await conn.execute("DELETE FROM repository.database_connections WHERE id = $1", connection_id)

    async def deactivate_all_connections(self):
        """Tüm bağlantılarda is_active değerini False yapar."""
        async with self._metadata_connection_pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute("UPDATE repository.database_connections SET is_active = FALSE")
            return {"status": "ok", "message": "All connections deactivated."}

    async def activate_connection(self, connection_id: int):
        """Belirtilen connection_id için is_active değerini True yapar."""
        async with self._metadata_connection_pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow("""
                    UPDATE repository.database_connections
                    SET is_active = TRUE
                    WHERE id = $1
                    RETURNING *
                """, connection_id)
            if not row:
                return {"status": "error", "message": f"Connection {connection_id} not found."}
            return dict(row)

    async def deactivate_connection(self, connection_id: int):
        """Belirtilen connection_id için is_active değerini False yapar."""
        async with self._metadata_connection_pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow("""
                    UPDATE repository.database_connections
                    SET is_active = FALSE
                    WHERE id = $1
                    RETURNING *
                """, connection_id)
            if not row:
                return {"status": "error", "message": f"Connection {connection_id} not found."}
            return dict(row)


repository_manager = RepositoryManager() #Singleton


# CREATE TABLE repository.database_types (
#     id SERIAL PRIMARY KEY,
#     name TEXT NOT NULL UNIQUE
# );
#
# CREATE TABLE repository.database_connections (
#     id serial PRIMARY KEY,
#     database_type_id INT REFERENCES repository.database_types(id) ON DELETE CASCADE,
#     host TEXT NOT NULL,
#     port INT NOT NULL,
#     database_name TEXT NOT NULL,
#     username TEXT NOT NULL,
#     encrypted_password TEXT NOT NULL,
#     is_active BOOLEAN DEFAULT TRUE,
#     description TEXT,
#     connect_at_startup BOOLEAN DEFAULT TRUE,
#     UNIQUE(database_type_id, host, port, database_name)
# );