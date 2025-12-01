from typing import Optional

import asyncpg
from asyncpg.pool import Pool
from config.settings import get_settings
import logging

logger = logging.getLogger(__name__)

class MetadataConnection:

    def __init__(self):
        self._pool: Optional[Pool] = None

    async def initialize(self):
        settings = get_settings()
        """
        Initializes the connection pool. Safe to call multiple times; 
        will only create pool if it doesn't exist.
        """
        if self._pool is None:
            try:
                self._pool = await asyncpg.create_pool(
                    host=settings.metadata_db_host,
                    port=settings.metadata_db_port,
                    user=settings.metadata_db_username,
                    password=settings.metadata_db_password,
                    database=settings.metadata_database_name
                )
            except Exception as e:
                logger.error(f"Failed to create connection pool: {e}")
                raise
        else:
            logger.warning("Attempted to connect, but pool already exists.")

    async def close(self):
        """Closes the connection pool."""
        if self._pool:
            await self._pool.close()
            self._pool = None
            logger.info("Database connection pool closed.")

    def get_pool(self)->Pool:
        return self._pool

metadata_connection = MetadataConnection() #Singleton