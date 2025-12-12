import logging.config
from config.settings import Settings
from config.logging_config import LOGGING_CONFIG
import asyncio

from mcp_server import mcp_handler #Singleton

logging.config.dictConfig(LOGGING_CONFIG)
logger = logging.getLogger(__name__)
async def main():
    logger.info('Started')
    await mcp_handler.start()

if __name__ == "__main__":
    settings = Settings()
    asyncio.run(main())

# End of file