from fastmcp.tools.tool import ToolResult
from mcp import types as mt

from config import Settings
from fastmcp import FastMCP, Context

from db.metadata_manager import get_metadata_manager_instance, initialize_metadata_manager, close_metadata_manager
from db.postgresql_manager import get_postgresql_manager_instance, initialize_postgresql_manager, close_postgresql_manager
from client_manager import ClientManager

from tools.math_tools import register_math_tools
from tools.metadata_tools import register_metadata_tools
from tools.postgresql_tools import register_postgresql_tools

from routes.sessions_routes import register_session_routes
from routes.metadata_connection_routes import register_connection_routes
from resources.test_resources import register_test_resources

from eunomia_mcp import create_eunomia_middleware, utils
from starlette.middleware.cors import CORSMiddleware
from starlette.applications import Starlette
from starlette.routing import Mount
import uvicorn

import logging
from fastmcp.server.middleware import Middleware, MiddlewareContext, CallNext

logger = logging.getLogger(__name__)

# Define routing structure
ROOT_URL = "http://localhost:8000"
MOUNT_PREFIX = "/api"
MCP_PATH = "/mcp"

class CustomMiddleware(Middleware):
    async def on_call_tool(
        self,
        context: MiddlewareContext[mt.CallToolRequestParams],
        call_next: CallNext[mt.CallToolRequestParams, ToolResult],
    ) -> ToolResult:
        print(context.fastmcp_context.session_id)
        return await call_next(context)


class MCPServer():
    def __init__(self, settings: Settings):
        self.settings = settings
        self.mcpserver = FastMCP(name="DBMCPServer 🚀",
                                instructions="""
                                -   This server provides data analysis on Postgresql databases
                                """,
                                stateless_http=True,
                                json_response=True)


        # Create MCP app
        mcp_app = self.mcpserver.http_app(path=MCP_PATH)

        # Add authorization eunomia middleware
        eunomia_middleware = create_eunomia_middleware(policy_file="mcp_policies.json")
        self.mcpserver.add_middleware(eunomia_middleware)

        self.mcpserver.add_middleware(CustomMiddleware())

        # Managerlar
        self.metadata_manager = get_metadata_manager_instance()
        self.db_manager = get_postgresql_manager_instance()
        # self.client_manager = ClientManager()

        # Tool ve route kayıtları
        register_math_tools(self.mcpserver)
        register_metadata_tools(self.mcpserver)
        register_postgresql_tools(self.mcpserver)
        register_test_resources(self.mcpserver)
        # register_session_routes(self.mcpserver, self.client_manager)
        register_connection_routes(self.mcpserver, self.metadata_manager, self.db_manager)


    async def start(self):
        """Start the MCP server."""
        logger.info("Starting MCP Database Server...")
        await initialize_metadata_manager()
        await initialize_postgresql_manager()
        # await self.mcpserver.run_async(transport="http", port=8000, path="/mcp") # path="/api/mcp" transport=http for new clients

        app = self.mcpserver.http_app()

        origins = [
            "http://localhost:3000",
            "http://127.0.0.1:3000"
        ]

        app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"], # origins,  # Allow all origins for development; restrict in production
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )
        config = uvicorn.Config(app, host="0.0.0.0", port=8000)
        server = uvicorn.Server(config)
        await server.serve()

        print(__name__)

    async def stop(self):
        """Stop the MCP server."""
        logger.info("Stopping MCP Database Server...")
        await close_postgresql_manager()
        await close_metadata_manager()


# Principal Extraction
# The middleware automatically extracts principals from these headers:
#
# Header	Principal URI	Attributes
# X-Agent-ID: claude	agent:claude	{"agent_id": "claude"}
# X-User-ID: user123		{"user_id": "user123"}
# User-Agent: Claude		{"user_agent": "Claude"}
# Authorization: Bearer xyz		{"api_key": "xyz"}