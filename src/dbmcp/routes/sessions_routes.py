from starlette.responses import JSONResponse
from starlette.requests import Request
from fastmcp import FastMCP

def register_session_routes(mcpserver: FastMCP, client_manager):
    @mcpserver.custom_route("/sessions", methods=["GET"])
    async def list_sessions(request: Request):
        sessions = await client_manager.list_active_sessions()
        return JSONResponse(content=sessions)

    @mcpserver.custom_route("/sessions", methods=["POST"])
    async def create_session(request: Request):
        session_id = await client_manager.create_session()
        return JSONResponse(content={"session_id": session_id})
