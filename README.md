# DBMCPServer

DBMCPServer is a FastMCP-powered Metadata Control Plane server for PostgreSQL databases. It exposes MCP tools for metadata management, job scheduling, observability, trend analysis, and simple math utilities over an HTTP interface. The server is designed to run as a long-lived MCP instance backed by PostgreSQL and secured with Eunomia policies.

## Features
- **FastMCP server** with HTTP transport and optional streamable client for chaining requests.
- **Metadata management** backed by PostgreSQL connection pools.
- **Job scheduling** via a dedicated scheduler manager.
- **Database tooling** for PostgreSQL access, observability, and trend reporting.
- **Pluggable middleware** including Eunomia policy enforcement and custom request logging.
- **Structured logging** to console and rotating files.

## Project layout
- `src/dbmcp/main.py` – entrypoint that configures logging and starts the MCP server singleton.
- `src/dbmcp/mcp_server.py` – server bootstrap, middleware wiring, and tool/route registration.
- `src/dbmcp/config/` – logging configuration and environment-driven settings management.
- `src/dbmcp/db/` – metadata and PostgreSQL managers plus connection handling.
- `src/dbmcp/tools/` – MCP tool registrations for math, metadata, PostgreSQL access, and observability.
- `src/dbmcp/routes/` – FastMCP route registration helpers.
- `src/dbmcp/resources/` – resource registrations (e.g., test resources).
- `src/dbmcp/logs/` – log output directory created automatically at runtime.
- Environment templates: `src/dbmcp/default.env`, `src/dbmcp/mac.env`, and `src/dbmcp/oci.env`.

## Prerequisites
- Python 3.11 or newer.
- PostgreSQL instance for metadata storage (defaults are set in `default.env`).
- Network access to any PostgreSQL databases you plan to query.

## Installation
1. Create and activate a virtual environment.
2. Install Python dependencies. A typical set based on the source imports is:
   ```bash
   pip install fastmcp asyncpg uvicorn starlette pydantic-settings eunomia-mcp
   ```
3. Ensure your `PYTHONPATH` includes `src` so local modules resolve:
   ```bash
   export PYTHONPATH="$PWD/src"
   ```

## Configuration
Settings are loaded via `pydantic_settings` from an environment file chosen by hostname:
- `default.env` – used by default.
- `mac.env` – used when the hostname matches `Serdars-MBP-M3`.
- `oci.env` – used when the hostname matches `db-mcp-server`.

You can override any value with environment variables. Key options include:
- `METADATA_DB_HOST`, `METADATA_DB_PORT`, `METADATA_DATABASE_NAME`, `METADATA_DB_USERNAME`, `METADATA_DB_PASSWORD` – metadata database connection.
- `SESSION_TIMEOUT_MINUTES` – session timeout for MCP clients.
- `EUNOMIA_POLICY_FILE` – path to the Eunomia policy JSON used by the middleware.

## Running the server
1. Confirm your configuration file (e.g., `src/dbmcp/default.env`) points at a reachable PostgreSQL instance.
2. Start the server from the project root:
   ```bash
   cd src
   python -m dbmcp.main
   ```
   Alternatively, run `python src/dbmcp/main.py` with `PYTHONPATH` set to include `src`.
3. The server listens on `0.0.0.0:8000` and exposes the MCP endpoint at `/mcp`.

## Logging
Logs are written to `src/dbmcp/logs/app.log` with daily rotation and also streamed to stdout using the format defined in `config/logging_config.py`.

## Stopping
Use standard process termination signals (Ctrl+C) to stop the server. Managers for scheduling, PostgreSQL access, and metadata connections are shut down gracefully in `MCPServer.stop()`.
