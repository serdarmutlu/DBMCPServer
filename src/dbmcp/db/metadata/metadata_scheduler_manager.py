import json

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from typing import Optional, Any
from asyncpg.pool import Pool
from fastmcp import Client, FastMCP

from .metadata_connection import metadata_connection

import logging

logger = logging.getLogger(__name__)


class SchedulerManager:
    def __init__(self):
        self._metadata_connection_pool: Optional[Pool] = None
        self._mcp_server: Optional[FastMCP] = None
        self._mcp_client: Optional[Client] = None
        self.scheduler = AsyncIOScheduler()

    async def initialize(self, mcpserver, mcpclient):
        self._metadata_connection_pool = metadata_connection.get_pool()
        self._mcp_server = mcpserver
        self._mcp_client = mcpclient

    async def close(self):
        self._metadata_connection_pool = None

    async def get_active_scheduled_jobs(self):
        async with self._metadata_connection_pool.acquire() as conn:
            rows = await conn.fetch("""
                                    SELECT *
                                    FROM scheduler.scheduled_jobs
                                    WHERE is_active = true
                                    """)
            return [dict(r) for r in rows]

    async def get_all_jobs(self):
        async with self._metadata_connection_pool.acquire() as conn:
            rows = await conn.fetch("""
                                    SELECT *
                                    FROM scheduler.scheduled_jobs
                                    ORDER BY job_id
                                    """)
            return [dict(r) for r in rows]

    async def get_job(self, job_id: int):
        async with self._metadata_connection_pool.acquire() as conn:
            row = await conn.fetchrow("""
                                      SELECT *
                                      FROM scheduler.scheduled_jobs
                                      WHERE job_id = $1
                                      """, job_id)
            return dict(row) if row else None

    async def add_job(self, job_data: dict):
        async with self._metadata_connection_pool.acquire() as conn:
            row = await conn.fetchrow("""
                                      INSERT INTO scheduler.scheduled_jobs (job_name, tool_name, tool_params,
                                                                            trigger_type,
                                                                            interval_seconds, cron_expression,
                                                                            is_active)
                                      VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING *
                                      """,
                                      job_data["job_name"],
                                      job_data["tool_name"],
                                      job_data.get("tool_params"),
                                      job_data["trigger_type"],
                                      job_data.get("interval_seconds"),
                                      job_data.get("cron_expression"),
                                      job_data.get("is_active", True)
                                      )
            return dict(row)

    async def update_job(self, job_id: int, job_data: dict):
        async with self._metadata_connection_pool.acquire() as conn:
            # Dynamically build the update query
            fields = []
            values = []
            idx = 1
            for key, value in job_data.items():
                if key in ['job_name', 'tool_name', 'tool_params', 'trigger_type', 'interval_seconds',
                           'cron_expression', 'is_active']:
                    fields.append(f"{key} = ${idx}")
                    values.append(value)
                    idx += 1

            if not fields:
                return await self.get_job(job_id)

            values.append(job_id)
            query = f"""
                UPDATE scheduler.scheduled_jobs
                SET {', '.join(fields)}, updated_at = now()
                WHERE job_id = ${idx}
                RETURNING *
            """
            row = await conn.fetchrow(query, *values)
            return dict(row) if row else None

    async def delete_job(self, job_id: int):
        async with self._metadata_connection_pool.acquire() as conn:
            await conn.execute("""
                               DELETE
                               FROM scheduler.scheduled_jobs
                               WHERE job_id = $1
                               """, job_id)

    async def update_job_last_run(self, job_id: int):
        async with self._metadata_connection_pool.acquire() as conn:
            await conn.execute("""
                               UPDATE scheduler.scheduled_jobs
                               SET last_run_at = now()
                               WHERE job_id = $1
                               """, job_id)

    async def execute_job(self, job: dict):
        logger.info(f"Executing scheduled job: {job['job_name']}")

        try:
            async with self._mcp_client:
                params = job.get("tool_params")

                if isinstance(params, str):
                    try:
                        params = json.loads(params)
                    except json.JSONDecodeError as e:
                        logger.error(
                            "Invalid tool_params JSON for job %s: %s",
                            job.get("id"),
                            params
                        )
                        params = {}

                result = await self._mcp_client.call_tool(
                    name=job["tool_name"],
                    arguments=params or {}
                )
                print(result)

            await self.update_job_last_run(job["job_id"])

        except Exception as e:
            logger.exception(f"Job failed: {job['job_name']} - {e}")

    async def load_jobs_from_db(self):
        jobs = await self.get_active_scheduled_jobs()

        for job in jobs:
            if job["trigger_type"] == "interval":
                trigger = IntervalTrigger(seconds=job["interval_seconds"])

            elif job["trigger_type"] == "cron":
                trigger = CronTrigger.from_crontab(job["cron_expression"])

            else:
                continue

            self.scheduler.add_job(
                self.execute_job,
                trigger=trigger,
                kwargs={"job": job},
                id=f"job_{job['job_id']}",
                replace_existing=True,
            )

            logger.info(f"Loaded job: {job['job_name']}")

    async def start(self):
        await self.load_jobs_from_db()
        self.scheduler.start()
        logger.info("Scheduler started")

    async def shutdown(self):
        self.scheduler.shutdown(wait=True)
        logger.info("Scheduler stopped")


scheduler_manager = SchedulerManager()  # Singleton

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