import asyncio

from fastmcp.client import StreamableHttpTransport
from fastmcp.tools.tool import ToolResult
from mcp import types as mt

from config.settings import get_settings
from fastmcp import FastMCP, Client

from db.metadata.metadata_connection import metadata_connection #Singleton
from db.metadata.metadata_repository_manager import repository_manager #Singleton
from db.metadata.metadata_scheduler_manager import scheduler_manager #Singleton
from db.postgresql.postgresql_manager import postgresql_manager #Singleton
from db.metadata.metadata_trend_manager import trend_manager #Singleton

from tools import math_tools
#from tools.math_tools import register_math_tools
from tools.repository_tools import register_metadata_tools
from tools.postgresql_tools import register_postgresql_tools
from tools.postgresql_observability_tools import register_postgresql_observability_tools
from tools.postgresql_trend_tools import register_postgresql_trend_tools
from routes.metadata_connection_routes import register_connection_routes
from routes.job_routes import register_job_routes
from routes.introspection_routes import register_introspection_routes

from resources.test_resources import register_test_resources

from eunomia_mcp import create_eunomia_middleware
from starlette.middleware.cors import CORSMiddleware
import uvicorn

import logging
from fastmcp.server.middleware import Middleware, MiddlewareContext, CallNext

BASE_URL = "http://127.0.0.1:8000"
API_PREFIX = "/metadata"
MCP_URL = "http://localhost:8000/mcp"
MCP_TOKEN = "serdar"

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


class MCPServer:
    def __init__(self):
        #self.mcp_app = None
        self.server = None
        #self.mcpserver = None
        #self.settings = None

    #def create_managers(self):
        #self.metadata_connection = metadata_connection
        #self.repository_manager = repository_manager_instance()
        #self.db_manager = get_postgresql_manager_instance()

    async def initialize_managers(self, mcpserver: FastMCP, mcpclient: Client):
        await metadata_connection.initialize()
        await repository_manager.initialize()
        await asyncio.gather(
            scheduler_manager.initialize(mcpserver, mcpclient),
            postgresql_manager.initialize(),
            trend_manager.initialize()
        )

    def close_managers(self):
        trend_manager.close()
        postgresql_manager.close()
        repository_manager.close()
        metadata_connection.close()

    # --- MCP Client Helpers ---
    def get_mcp_transport(self):
        return StreamableHttpTransport(
            url=MCP_URL,
            headers={"Authorization": f"Bearer {MCP_TOKEN}"}
        )

    async def initialize_server(self):
        settings = get_settings()
        mcpserver = FastMCP(name="DBMCPServer 🚀",
                                 instructions="""
                                -   This server provides data analysis on Postgresql databases
                                """,
                                 stateless_http=False,  # This is preferred mode for state management. Changing to true also raises ClosedResourceError warnings.
                                                        # Needs to be checked in future versions of FastMCP Server
                                 json_response=True)
        transport = self.get_mcp_transport()
        mcpclient = Client(transport=transport) # Not possible to make in-memory connection because of header authorization

        # Tool ve route kayıtları
        math_tools.register_math_tools(mcpserver)
        register_metadata_tools(mcpserver)
        register_postgresql_tools(mcpserver)
        register_postgresql_observability_tools(mcpserver)
        register_postgresql_trend_tools(mcpserver)
        register_test_resources(mcpserver)
        # register_session_routes(self.mcpserver, self.client_manager)
        register_job_routes(mcpserver, scheduler_manager)
        register_connection_routes(mcpserver)
        register_introspection_routes(mcpserver)

        # Create MCP app
        mcp_app = mcpserver.http_app(path=MCP_PATH, transport="streamable-http")

        # Add authorization eunomia middleware
        eunomia_middleware = create_eunomia_middleware(policy_file="mcp_policies.json")
        mcpserver.add_middleware(eunomia_middleware)

        mcpserver.add_middleware(CustomMiddleware())

        origins = [
            "http://localhost:3000",
            "http://127.0.0.1:3000"
        ]

        mcp_app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],  # origins,  # Allow all origins for development; restrict in production
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )
        config = uvicorn.Config(mcp_app, host="0.0.0.0", port=8000, log_level=logging.DEBUG)
        self.server = uvicorn.Server(config)

        await self.initialize_managers(mcpserver, mcpclient)

    async def start(self):
        """Start the MCP server."""
        logger.info("Starting MCP Database Server...")
        await self.initialize_server()
        await scheduler_manager.start()
        await self.server.serve()

    async def stop(self):
        """Stop the MCP server."""
        logger.info("Stopping MCP Database Server...")
        self.close_managers()
        scheduler_manager.stop()
        await self.server.shutdown()

mcp_handler = MCPServer() #Singleton

# Principal Extraction
# The middleware automatically extracts principals from these headers:
#
# Header	Principal URI	Attributes
# X-Agent-ID: claude	agent:claude	{"agent_id": "claude"}
# X-User-ID: user123		{"user_id": "user123"}
# User-Agent: Claude		{"user_agent": "Claude"}
# Authorization: Bearer xyz		{"api_key": "xyz"}