import logging.config
from config.logging_config import LOGGING_CONFIG

import asyncio

from mcp_server import MCPServer
from config import settings

logging.config.dictConfig(LOGGING_CONFIG)
logger = logging.getLogger(__name__)
async def main():
    logger.info('Started')
    #settings = get_settings()
    mcp_server = MCPServer(settings)
    await mcp_server.start()

if __name__ == "__main__":
    asyncio.run(main())
