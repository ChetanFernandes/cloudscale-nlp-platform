
from pydantic_settings import BaseSettings
from functools import lru_cache
from pydantic_settings import SettingsConfigDict
from pathlib import Path
'''
BaseSettings
This is a special class that:
Reads values from .env
Reads values from system environment
Automatically converts types
Validates required fields
'''
BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent

class Settings(BaseSettings):
    # -------------------
    # Application
    # -------------------
    app_name: str
    app_env: str = "development"

    # -------------------
    # Database
    # -------------------
    database_url: str

    # -------------------
    # Redis
    # -------------------
    redis_url: str

    # -------------------
    # Logging
    # -------------------
    log_level: str = "INFO"

    # -------------------
    # Azure storage ACCOUNT_NAME
    # -------------------
    storage_account_name: str = "chetanstore123"

    #--------------------
    # Azure bucket name
    #--------------------
    CONTAINER_NAME :str = "test"

    #--------------------
    # Azure storage account key
    #--------------------
    STORAGE_ACCOUNT_KEY :str 

    #--------------------
    # Azure storage connecting string
    #--------------------
    STORAGE_CONNECTION_STRING :str 

    # -------------------
    # Rate Limiting
    # -------------------
    rate_limit_per_minute: int = 10

    model_config = SettingsConfigDict(
    env_file=str(BASE_DIR / ".env"),
    case_sensitive=False

)

@lru_cache()
def get_settings():
    return Settings()


'''
lru_cache()

This means:

Config loads once

Not reloaded on every request

Efficient and production-safe
'''