from functools import cache
from pydantic_settings import BaseSettings


class MainConfig(BaseSettings):
    # sqlalchemy dsn
    DB_DSN: str = "sqlite:///freelancers.db"
    OLLAMA_URL: str = "http://localhost:11434"


@cache
def get_config():
    return MainConfig()