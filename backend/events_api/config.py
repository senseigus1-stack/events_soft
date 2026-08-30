from functools import lru_cache
from typing import Annotated, Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    environment: Literal["development", "test", "production"] = "development"
    database_url: str = "sqlite:///./events.db"
    run_schema_on_startup: bool = True
    cors_origins: Annotated[list[str], NoDecode] = Field(
        default_factory=lambda: ["http://localhost:5173"]
    )
    public_api_url: str = "http://localhost:8000"
    app_secret: str = ""
    auth_token_days: int = Field(default=365, ge=1, le=3650)
    oauth_public_base_url: str = "http://localhost:8000"
    oauth_frontend_url: str = "http://localhost:5173"
    oauth_state_minutes: int = Field(default=10, ge=3, le=30)
    google_oauth_client_id: str = ""
    google_oauth_client_secret: str = ""
    yandex_oauth_client_id: str = ""
    yandex_oauth_client_secret: str = ""
    github_oauth_client_id: str = ""
    github_oauth_client_secret: str = ""
    sync_api_key: str = ""
    admin_api_key: str = ""
    kudago_base_url: str = "https://kudago.com/public-api/v1.4"
    timepad_base_url: str = "https://api.timepad.ru/v1"
    timepad_organization_ids: Annotated[list[int], NoDecode] = Field(default_factory=list)
    sync_city_slugs: Annotated[list[str], NoDecode] = Field(
        default_factory=lambda: ["msk", "spb", "ekb", "kzn", "nsk"]
    )
    sync_horizon_days: int = Field(default=120, ge=7, le=366)
    sync_page_size: int = Field(default=100, ge=20, le=100)
    sync_interval_minutes: int = Field(default=360, ge=15, le=1440)
    notification_interval_seconds: int = Field(default=60, ge=15, le=3600)
    telegram_token: str = ""
    log_level: str = "INFO"

    @field_validator("cors_origins", mode="before")
    @classmethod
    def parse_origins(cls, value: object) -> object:
        if isinstance(value, str):
            return [item.strip() for item in value.split(",") if item.strip()]
        return value

    @field_validator("timepad_organization_ids", mode="before")
    @classmethod
    def parse_organization_ids(cls, value: object) -> object:
        if isinstance(value, str):
            return [int(item.strip()) for item in value.split(",") if item.strip()]
        return value

    @field_validator("sync_city_slugs", mode="before")
    @classmethod
    def parse_city_slugs(cls, value: object) -> object:
        if isinstance(value, str):
            return [item.strip() for item in value.split(",") if item.strip()]
        return value


@lru_cache
def get_settings() -> Settings:
    return Settings()
