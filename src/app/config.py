from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    database_url: str
    rabbitmq_url: str
    api_key: str
    log_level: str = "INFO"
    # сколько часов хранить уже опубликованные outbox-записи: они нужны только как
    # аудит-след, но без уборки таблица растёт бесконечно. 0 отключает очистку
    outbox_retention_hours: int = 24


@lru_cache
def get_settings() -> Settings:
    return Settings()
