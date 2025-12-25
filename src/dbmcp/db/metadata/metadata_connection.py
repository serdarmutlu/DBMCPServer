import logging
import duckdb
import asyncio
import re
from config.settings import get_settings
from typing import Optional, Any, Tuple, List, Dict

logger = logging.getLogger(__name__)

class MetadataConnection:

    def __init__(self):
        self._db_path: Optional[str] = None
        
    async def initialize(self):
        """
        Initializes the DuckDB database. 
        Ensures the database file exists and schemas are created.
        """
        settings = get_settings()
        self._db_path = settings.metadata_duckdb_path
        
        # Connect to ensure file exists and create tables if needed
        try:
             with duckdb.connect(self._db_path) as conn:
                self._create_schema(conn)
        except Exception as e:
            logger.error(f"Failed to initialize DuckDB metadata: {e}")
            raise

    def _create_schema(self, conn: duckdb.DuckDBPyConnection):
        """Creates the necessary tables if they don't exist."""
        
        # --- Repository Schema ---
        conn.execute("CREATE SCHEMA IF NOT EXISTS repository;")
        
        conn.execute("""
            CREATE SEQUENCE IF NOT EXISTS seq_database_types_id START 1;
            CREATE TABLE IF NOT EXISTS repository.database_types (
                id INTEGER PRIMARY KEY DEFAULT nextval('seq_database_types_id'),
                name VARCHAR NOT NULL UNIQUE
            );
        """)

        conn.execute("""
            CREATE SEQUENCE IF NOT EXISTS seq_database_connections_id START 1;
            CREATE TABLE IF NOT EXISTS repository.database_connections (
                id INTEGER PRIMARY KEY DEFAULT nextval('seq_database_connections_id'),
                database_type_id INTEGER,
                host VARCHAR NOT NULL,
                port INTEGER NOT NULL,
                database_name VARCHAR NOT NULL,
                username VARCHAR NOT NULL,
                encrypted_password VARCHAR NOT NULL,
                is_active BOOLEAN DEFAULT TRUE,
                description VARCHAR,
                connect_at_startup BOOLEAN DEFAULT TRUE,
                UNIQUE(database_type_id, host, port, database_name),
                FOREIGN KEY (database_type_id) REFERENCES repository.database_types(id)
            );
        """)

        # --- LLM Providers Settings ---
        conn.execute("""
            CREATE TABLE IF NOT EXISTS repository.llm_providers (
                provider_name VARCHAR PRIMARY KEY,
                base_url      VARCHAR NOT NULL,
                api_key       VARCHAR,
                updated_at    TIMESTAMP DEFAULT now()
            );
        """)
        
        # Insert defaults if not exists
        # DuckDB INSERT OR IGNORE / ON CONFLICT syntax
        conn.execute("""
            INSERT OR IGNORE INTO repository.llm_providers (provider_name, base_url, api_key) VALUES
            ('Ollama', 'http://localhost:11434', NULL),
            ('LM Studio', 'http://localhost:1234/v1', NULL),
            ('Llama.cpp', 'http://localhost:8080/v1', NULL);
        """)
        
        # --- Scheduler Schema ---
        conn.execute("CREATE SCHEMA IF NOT EXISTS scheduler;")
        
        conn.execute("""
            CREATE SEQUENCE IF NOT EXISTS seq_scheduled_jobs_id START 1;
            CREATE TABLE IF NOT EXISTS scheduler.scheduled_jobs (
                job_id            INTEGER PRIMARY KEY DEFAULT nextval('seq_scheduled_jobs_id'),
                job_name          VARCHAR NOT NULL UNIQUE,
                tool_name         VARCHAR NOT NULL,
                tool_params       JSON,
                trigger_type      VARCHAR NOT NULL CHECK (trigger_type IN ('interval','cron')),
                interval_seconds  INTEGER,
                cron_expression   VARCHAR,
                is_active         BOOLEAN DEFAULT TRUE,
                last_run_at       TIMESTAMP,
                created_at        TIMESTAMP DEFAULT now(),
                updated_at        TIMESTAMP DEFAULT now()
            );
        """)

        # --- Trends Schema ---
        conn.execute("CREATE SCHEMA IF NOT EXISTS trends;")
        
        conn.execute("""
            CREATE TABLE IF NOT EXISTS trends.pg_capacity_snapshots (
                snapshot_ts     TIMESTAMPTZ NOT NULL DEFAULT now(),
                scope           VARCHAR        NOT NULL, 
                dbname          VARCHAR        NOT NULL,
                schemaname      VARCHAR,
                relname         VARCHAR,
                size_bytes      BIGINT      NOT NULL
            );
        """)
        
        logger.info("DuckDB schema initialized.")

    async def close(self):
        pass

    def get_connection(self) -> duckdb.DuckDBPyConnection:
        """Returns a new synchronous connection to the DuckDB database."""
        if not self._db_path:
             raise RuntimeError("MetadataConnection not initialized. Call initialize() first.")
        return duckdb.connect(self._db_path)

    def _execute_duckdb_sync(self, query: str, params: Tuple = (), fetch_one: bool = False, fetch_all: bool = False) -> Any:
        conn = self.get_connection()
        try:
            # Replace $n with ?
            duck_query = re.sub(r'\$\d+', '?', query)
            
            cursor = conn.cursor()
            cursor.execute(duck_query, params)
            
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            
            if fetch_one:
                row = cursor.fetchone()
                if row:
                    return dict(zip(columns, row))
                return None
            
            if fetch_all:
                rows = cursor.fetchall()
                results = []
                for row in rows:
                    results.append(dict(zip(columns, row)))
                return results
                
            return None
            
        except Exception as e:
            logger.error(f"DuckDB Execution Error: {e}\nQuery: {query}\nParams: {params}")
            raise
        finally:
            conn.close()

    async def execute_query(self, query: str, *params, fetch_one: bool = False, fetch_all: bool = False):
        """
        Executes a query against DuckDB asynchronously (in a thread).
        Supports Postgres-style $n placeholders by converting them to ?.
        """
        return await asyncio.to_thread(self._execute_duckdb_sync, query, params, fetch_one, fetch_all)

metadata_connection = MetadataConnection() #Singleton