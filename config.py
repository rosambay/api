from pathlib import Path
from pydantic_settings import BaseSettings
from functools import lru_cache
from typing import List

class Settings(BaseSettings):

    
    access_token_expire_minutes: int = 60
    secret_key: str
    algorithm: str = "HS256"
    url_fsm: str 
    usuario: str 
    senha: str 
    admin: str 
    passwd: str
    client_uid: int 

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        # env_file = str(Path(__file__).parent.parent.parent / ".env")
        # case_sensitive = True
        extra = "ignore"  # Ignora campos extras do .env que não estão definidos
@lru_cache()
def get_settings() -> Settings:
    settings = Settings()

    return settings

settings = get_settings()
