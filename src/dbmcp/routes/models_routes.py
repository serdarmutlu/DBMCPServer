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

from db.repository.llm_repository import llm_repository

# Configuration is now dynamic via LLMRepository
# Defaults are handled in DB initialization or repository fallback logic if we implemented it, 
# but here we'll just query the DB.

async def get_ollama_status() -> ServerStatus:
    try:
        config = await llm_repository.get_provider("Ollama")
        base_url = config.base_url if config else "http://localhost:11434"
        
        async with httpx.AsyncClient(timeout=2.0) as client:
            # Check basic connectivity (Ollama root usually returns 200 OK)
            root_resp = await client.get(base_url)
            if root_resp.status_code != 200:
                return ServerStatus(status="DOWN", models=[], error=f"Status code: {root_resp.status_code}")

            # Fetch models
            tags_resp = await client.get(f"{base_url}/api/tags")
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
    except Exception as e:
        return ServerStatus(status="DOWN", models=[], error=f"Unexpected error: {str(e)}")

async def get_lmstudio_status() -> ServerStatus:
    try:
        config = await llm_repository.get_provider("LM Studio")
        base_url = config.base_url if config else "http://localhost:1234/v1"

        async with httpx.AsyncClient(timeout=2.0) as client:
            # LM Studio usually acts like OpenAI API.
            # GET /v1/models is a standard check.
            models_resp = await client.get(f"{base_url}/models")

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
    except Exception as e:
        return ServerStatus(status="DOWN", models=[], error=f"Unexpected error: {str(e)}")


async def get_llamacpp_status() -> ServerStatus:
    try:
        config = await llm_repository.get_provider("Llama.cpp")
        base_url = config.base_url if config else "http://localhost:8080/v1"

        async with httpx.AsyncClient(timeout=2.0) as client:
            # llama.cpp server is OpenAI compatible.
            # GET /v1/models is supported.
            models_resp = await client.get(f"{base_url}/models")

            if models_resp.status_code == 200:
                data = models_resp.json()
                # OpenAI format: {"data": [{"id": "model-id", ...}], ...}
                models_list = [
                    ModelInfo(name=model["id"], details={"owned_by": model.get("owned_by")})
                    for model in data.get("data", [])
                ]
                # If no models returned (some versions of llama.cpp might be empty if just one model loaded),
                # we can try /props (if available) or assume "default" if UP.
                # simpler: just rely on /v1/models
                if not models_list:
                    # Fallback if list is empty but server acts up?
                    pass
                return ServerStatus(status="UP", models=models_list)
            else:
                return ServerStatus(status="DOWN", models=[], error=f"Status code: {models_resp.status_code}")

    except httpx.RequestError as e:
        return ServerStatus(status="DOWN", models=[], error=str(e))
    except Exception as e:
        return ServerStatus(status="DOWN", models=[], error=f"Unexpected error: {str(e)}")


def register_model_routes(mcpserver: FastMCP, client_manager):

    @mcpserver.custom_route("/status/ollama", methods=["GET"])
    async def check_ollama(request: Request):
        result = await get_ollama_status()
        return JSONResponse(content=result.model_dump())

    @mcpserver.custom_route("/status/lmstudio", methods=["GET"])
    async def check_lmstudio(request: Request):
        result = await get_lmstudio_status()
        return JSONResponse(content=result.model_dump())

    @mcpserver.custom_route("/status/llamacpp", methods=["GET"])
    async def check_llamacpp(request: Request):
        result = await get_llamacpp_status()
        return JSONResponse(content=result.model_dump())