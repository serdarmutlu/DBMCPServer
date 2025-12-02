from datetime import datetime
from starlette.requests import Request
from starlette.responses import JSONResponse
from fastmcp import FastMCP

from db.metadata.metadata_scheduler_manager import SchedulerManager

def serialize_job(job):
    """Helper to serialize datetime objects in job dicts."""
    if not job:
        return None
    return {
        k: v.isoformat() if isinstance(v, datetime) else v
        for k, v in job.items()
    }

def register_job_routes(mcpserver: FastMCP, scheduler_manager):
    @mcpserver.custom_route("/job", methods=["GET"])
    async def list_jobs(request: Request):
        jobs = await scheduler_manager.get_all_jobs()
        # Serialize jobs to handle datetime objects
        serialized_jobs = [serialize_job(job) for job in jobs]
        return JSONResponse(content=serialized_jobs)

    @mcpserver.custom_route("/job/{job_id:int}", methods=["GET"])
    async def get_job(request: Request):
        job_id = int(request.path_params["job_id"])
        job = await scheduler_manager.get_job(job_id)
        serialized_job = serialize_job(job)
        return JSONResponse(
            content=serialized_job if serialized_job else {"error": "Not found"},
            status_code=200 if serialized_job else 404
        )

    @mcpserver.custom_route("/job", methods=["POST"])
    async def create_job(request: Request):
        data = await request.json()
        try:
            job = await scheduler_manager.add_job(data)
            return JSONResponse(content=serialize_job(job), status_code=201)
        except Exception as e:
            return JSONResponse(content={"error": str(e)}, status_code=400)

    @mcpserver.custom_route("/job/{job_id:int}", methods=["PUT"])
    async def update_job(request: Request):
        job_id = int(request.path_params["job_id"])
        data = await request.json()
        try:
            job = await scheduler_manager.update_job(job_id, data)
            serialized_job = serialize_job(job)
            return JSONResponse(
                content=serialized_job if serialized_job else {"error": "Not found"},
                status_code=200 if serialized_job else 404
            )
        except Exception as e:
            return JSONResponse(content={"error": str(e)}, status_code=400)

    @mcpserver.custom_route("/job/{job_id:int}", methods=["DELETE"])
    async def delete_job(request: Request):
        job_id = int(request.path_params["job_id"])
        await scheduler_manager.delete_job(job_id)

