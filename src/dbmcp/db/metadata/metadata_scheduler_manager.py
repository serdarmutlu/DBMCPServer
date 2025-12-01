
from .metadata_connection import metadata_connection
import logging

logger = logging.getLogger(__name__)

class SchedulerManager:
    _metadata_connection_pool = None

    async def initialize(self):
        self._metadata_connection_pool = metadata_connection.get_pool()

    async def close(self):
        self._metadata_connection_pool = None

scheduler_manager = SchedulerManager() #Singleton


# CREATE TABLE scheduler.scheduled_jobs (
#     job_id            SERIAL PRIMARY KEY,
#     job_name          TEXT NOT NULL UNIQUE,
#     tool_name         TEXT NOT NULL,
#     tool_params       JSONB,
#     trigger_type      TEXT NOT NULL CHECK (trigger_type IN ('interval','cron')),
#     -- interval
#     interval_seconds  INTEGER,
#     -- cron
#     cron_expression   TEXT,
#     is_active         BOOLEAN DEFAULT TRUE,
#     last_run_at       TIMESTAMP,
#     created_at        TIMESTAMP DEFAULT now(),
#     updated_at        TIMESTAMP DEFAULT now()
# );