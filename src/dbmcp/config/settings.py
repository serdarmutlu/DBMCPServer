import logging
from typing import Optional
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
import socket

logger = logging.getLogger(__name__)

class Settings(BaseSettings):

    if socket.gethostname() == "Serdars-MBP-M3":
        model_config = SettingsConfigDict(env_file="mac.env", env_file_encoding="utf-8")
    elif socket.gethostname() == "db-mcp-server":
        model_config = SettingsConfigDict(env_file="oci.env", env_file_encoding="utf-8")
    else:
        model_config = SettingsConfigDict(env_file="default.env", env_file_encoding="utf-8")

    # Session
    session_timeout_minutes: int = Field(default=30)

    # Metadata DB
    metadata_db_host: str = Field(default="localhost")
    metadata_db_port: int = Field(default=5433)
    metadata_database_name: str = Field(default="mcp_metadata")
    metadata_db_username: str = Field(default="postgres")
    metadata_db_password: str = Field(default="postgres")
    metadata_db_url: Optional[str] = None

    # Encryption
    encryption_key: Optional[str] = None

    # Default DB
    default_database_host: Optional[str] = None
    default_database_port: int = Field(default=5432)
    default_database_name: Optional[str] = None
    default_database_username: Optional[str] = None
    default_database_password: Optional[str] = None

    # DB Pool
    db_pool_min_size: int = Field(default=1)
    db_pool_max_size: int = Field(default=5)

    eunomia_policy_file: Optional[str] = None

    def get_metadata_db_url(self) -> str:
        if self.metadata_db_url:
            return self.metadata_db_url
        return (
            f"postgresql://{self.metadata_db_username}:{self.metadata_db_password}"
            f"@{self.metadata_db_host}:{self.metadata_db_port}/{self.metadata_database_name}"
        )

def get_settings() -> Settings:
    """Singleton-style accessor to avoid reloading default.env multiple times."""
    if not hasattr(get_settings, "_instance"):
        get_settings._instance = Settings()
    return get_settings._instance
