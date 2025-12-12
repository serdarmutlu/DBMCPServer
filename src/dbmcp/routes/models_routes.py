from pydantic import BaseModel
from typing import List, Optional
from starlette.responses import JSONResponse
from starlette.requests import Request
import httpx

from fastmcp import FastMCP

class ModelInfo(BaseModel):
    name: str
    details: Optional[dict] = None

class ServerStatus(BaseModel):
    status: str
    models: List[ModelInfo]
    error: Optional[str] = None

# Configuration
OLLAMA_URL = "http://localhost:11434"
LMSTUDIO_URL = "http://localhost:1234"

async def get_ollama_status() -> ServerStatus:
    try:
        async with httpx.AsyncClient(timeout=2.0) as client:
            # Check basic connectivity (Ollama root usually returns 200 OK)
            root_resp = await client.get(OLLAMA_URL)
            if root_resp.status_code != 200:
                return ServerStatus(status="DOWN", models=[], error=f"Status code: {root_resp.status_code}")

            # Fetch models
            tags_resp = await client.get(f"{OLLAMA_URL}/api/tags")
            if tags_resp.status_code == 200:
                data = tags_resp.json()
                models_list = [
                    ModelInfo(name=model["name"], details=model.get("details"))
                    for model in data.get("models", [])
                ]
                return ServerStatus(status="UP", models=models_list)
            else:
                return ServerStatus(status="UP", models=[], error="Could not fetch tags")

    except httpx.RequestError as e:
        return ServerStatus(status="DOWN", models=[], error=str(e))

async def get_lmstudio_status() -> ServerStatus:
    try:
        async with httpx.AsyncClient(timeout=2.0) as client:
            # LM Studio usually acts like OpenAI API.
            # GET /v1/models is a standard check.
            models_resp = await client.get(f"{LMSTUDIO_URL}/v1/models")

            if models_resp.status_code == 200:
                data = models_resp.json()
                # OpenAI format: {"data": [{"id": "model-id", ...}], ...}
                models_list = [
                    ModelInfo(name=model["id"], details={"owned_by": model.get("owned_by")})
                    for model in data.get("data", [])
                ]
                return ServerStatus(status="UP", models=models_list)
            else:
                return ServerStatus(status="DOWN", models=[], error=f"Status code: {models_resp.status_code}")

    except httpx.RequestError as e:
        return ServerStatus(status="DOWN", models=[], error=str(e))


def register_model_routes(mcpserver: FastMCP, client_manager):

    @mcpserver.custom_route("/status/ollama", methods=["GET"])
    async def check_ollama(request: Request) -> ServerStatus:
        return await get_ollama_status()

    @mcpserver.custom_route("/status/lmstudio", methods=["GET"])
    async def check_lmstudio(request: Request) -> ServerStatus:
        return await get_lmstudio_status()