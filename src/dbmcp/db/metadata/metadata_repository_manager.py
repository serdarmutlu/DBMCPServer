import logging
import asyncio
from typing import Optional, List, Dict, Any, Tuple
from .metadata_connection import metadata_connection

logger = logging.getLogger(__name__)

class RepositoryManager:

    def __init__(self):
        pass

    async def initialize(self):
        pass

    async def close(self):
        pass

    # --- database_types ---
    async def get_all_types(self):
        return await metadata_connection.execute_query("SELECT * FROM repository.database_types ORDER BY id", fetch_all=True)

    async def get_type(self, id:int):
        return await metadata_connection.execute_query("SELECT * FROM repository.database_types WHERE id=$1", id, fetch_one=True)

    async def add_type(self, name: str):
        return await metadata_connection.execute_query(
            "INSERT INTO repository.database_types (name) VALUES ($1) RETURNING *", name, fetch_one=True
        )

    async def update_type(self, id: int, data: dict):
        return await metadata_connection.execute_query("""
            UPDATE repository.database_types
            SET name=$1
            WHERE id=$2
            RETURNING *
        """, data["name"], id, fetch_one=True)

    async def delete_type(self, id: int):
        await metadata_connection.execute_query("DELETE FROM repository.database_types WHERE id = $1", id)

    # --- database_connections ---
    async def get_all_connections(self, connect_at_startup: bool = None):
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
        return await metadata_connection.execute_query(query, *params, fetch_all=True)

    async def get_connection(self, connection_id: int):
        return await metadata_connection.execute_query("""
            SELECT  c.id, c.database_type_id, t.name AS database_type_name, c.host, c.port, c.database_name,
                c.username, c.is_active, c.description, c.connect_at_startup
            FROM repository.database_connections c
            LEFT JOIN repository.database_types t ON c.database_type_id = t.id
            WHERE c.id = $1
            ORDER BY t.name, c.host, c.port, c.database_name
        """, connection_id, fetch_one=True)

    async def get_connection_with_password(self, connection_id: int):
        return await metadata_connection.execute_query("""
            SELECT  c.id, c.database_type_id, t.name AS database_type_name, c.host, c.port, c.database_name,
                c.username, c.encrypted_password, c.is_active, c.description, c.connect_at_startup
            FROM repository.database_connections c
            LEFT JOIN repository.database_types t ON c.database_type_id = t.id
            WHERE c.id = $1
            ORDER BY t.name, c.host, c.port, c.database_name
        """, connection_id, fetch_one=True)

    async def add_connection(self, data: dict):
        return await metadata_connection.execute_query("""
            INSERT INTO repository.database_connections
            (database_type_id, host, port, database_name, username,
             encrypted_password, is_active, description, connect_at_startup)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
            RETURNING *
        """,
        data["database_type_id"], data["host"], data["port"],
        data["database_name"], data["username"], data["encrypted_password"],
        data.get("is_active", True), data.get("description"),
        data.get("connect_at_startup", True),
        fetch_one=True)

    async def update_connection(self, connection_id: int, data: dict):
        return await metadata_connection.execute_query("""
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
        data.get("connect_at_startup", False), connection_id,
        fetch_one=True)

    async def update_connection_no_password(self, connection_id: int, data: dict):
        return await metadata_connection.execute_query("""
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
        data.get("connect_at_startup", False), connection_id,
        fetch_one=True)

    async def delete_connection(self, connection_id: int):
        await metadata_connection.execute_query("DELETE FROM repository.database_connections WHERE id = $1", connection_id)

    async def deactivate_all_connections(self):
        """Tüm bağlantılarda is_active değerini False yapar."""
        await metadata_connection.execute_query("UPDATE repository.database_connections SET is_active = FALSE")
        return {"status": "ok", "message": "All connections deactivated."}

    async def activate_connection(self, connection_id: int):
        """Belirtilen connection_id için is_active değerini True yapar."""
        row = await metadata_connection.execute_query("""
            UPDATE repository.database_connections
            SET is_active = TRUE
            WHERE id = $1
            RETURNING *
        """, connection_id, fetch_one=True)
        
        if not row:
            return {"status": "error", "message": f"Connection {connection_id} not found."}
        return row

    async def deactivate_connection(self, connection_id: int):
        """Belirtilen connection_id için is_active değerini False yapar."""
        row = await metadata_connection.execute_query("""
            UPDATE repository.database_connections
            SET is_active = FALSE
            WHERE id = $1
            RETURNING *
        """, connection_id, fetch_one=True)
        
        if not row:
            return {"status": "error", "message": f"Connection {connection_id} not found."}
        return row


repository_manager = RepositoryManager() #Singleton