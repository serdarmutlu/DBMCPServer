import logging
import json

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from typing import Optional, Any
from fastmcp import Client, FastMCP
from .metadata_connection import metadata_connection

logger = logging.getLogger(__name__)

class SchedulerManager:
    def __init__(self):
        self._mcp_server: Optional[FastMCP] = None
        self._mcp_client: Optional[Client] = None
        self.scheduler = AsyncIOScheduler()

    async def initialize(self, mcpserver, mcpclient):
        # Pool initialization removed as it's handled in metadata_connection singleton
        self._mcp_server = mcpserver
        self._mcp_client = mcpclient

    async def close(self):
        pass

    async def get_active_scheduled_jobs(self):
        rows = await metadata_connection.execute_query("""
            SELECT *
            FROM scheduler.scheduled_jobs
            WHERE is_active = true
        """, fetch_all=True)
        return rows

    async def get_all_jobs(self):
        rows = await metadata_connection.execute_query("""
            SELECT *
            FROM scheduler.scheduled_jobs
            ORDER BY job_id
        """, fetch_all=True)
        return rows

    async def get_job(self, job_id: int):
        row = await metadata_connection.execute_query("""
            SELECT *
            FROM scheduler.scheduled_jobs
            WHERE job_id = $1
        """, job_id, fetch_one=True)
        return row

    async def add_job(self, job_data: dict):
        # DuckDB JSON type expects a string for insert usually if parameterized?
        # Or can handle dict if registered type. Safest to dump to string if using ? param.
        tool_params = job_data.get("tool_params")
        if isinstance(tool_params, dict) or isinstance(tool_params, list):
            tool_params = json.dumps(tool_params)

        row = await metadata_connection.execute_query("""
            INSERT INTO scheduler.scheduled_jobs (job_name, tool_name, tool_params,
                                                    trigger_type,
                                                    interval_seconds, cron_expression,
                                                    is_active)
            VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING *
            """,
            job_data["job_name"],
            job_data["tool_name"],
            tool_params,
            job_data["trigger_type"],
            job_data.get("interval_seconds"),
            job_data.get("cron_expression"),
            job_data.get("is_active", True),
            fetch_one=True
        )
        return row

    async def update_job(self, job_id: int, job_data: dict):
        # Dynamically build the update query
        fields = []
        values = []
        idx = 1
        for key, value in job_data.items():
            if key in ['job_name', 'tool_name', 'tool_params', 'trigger_type', 'interval_seconds',
                       'cron_expression', 'is_active']:
                
                if key == 'tool_params' and (isinstance(value, dict) or isinstance(value, list)):
                     value = json.dumps(value)

                fields.append(f"{key} = ${idx}")
                values.append(value)
                idx += 1

        if not fields:
            return await self.get_job(job_id)

        values.append(job_id)
        # Note: idx is now N+1, so $idx is the last param for WHERE
        query = f"""
            UPDATE scheduler.scheduled_jobs
            SET {', '.join(fields)}, updated_at = now()
            WHERE job_id = ${idx}
            RETURNING *
        """
        row = await metadata_connection.execute_query(query, *values, fetch_one=True)
        return row

    async def delete_job(self, job_id: int):
        await metadata_connection.execute_query("""
           DELETE
           FROM scheduler.scheduled_jobs
           WHERE job_id = $1
           """, job_id)

    async def update_job_last_run(self, job_id: int):
        await metadata_connection.execute_query("""
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
                elif params is None:
                    params = {}

                result = await self._mcp_client.call_tool(
                    name=job["tool_name"],
                    arguments=params
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
        # Initializing the connection is done in main via repository/metadata_connection check,
        # but self.load_jobs_from_db calls get_active_scheduled_jobs which calls execute_query
        # which calls metadata_connection.get_connection().
        # So we depend on metadata_connection being initialized globally (which it is called in mcp_server).
        self.scheduler.start()
        logger.info("Scheduler started")

    async def shutdown(self):
        self.scheduler.shutdown(wait=True)
        logger.info("Scheduler stopped")


scheduler_manager = SchedulerManager()  # Singleton