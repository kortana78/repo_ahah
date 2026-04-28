from functools import lru_cache
from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict

BASE_DIR = Path(__file__).resolve().parent.parent


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=BASE_DIR / ".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    app_name: str = "NyasaSport Repo API"
    app_env: str = "development"
    app_host: str = "127.0.0.1"
    app_port: int = 8000
    frontend_origin: str = "http://localhost:5173"
    frontend_origins: str = ""
    database_url: str | None = None
    init_db_on_startup: bool = True
    uploads_dir: Path = BASE_DIR / "uploads"
    
    secret_key: str = "super-secret-key-change-me"
    algorithm: str = "HS256"
    access_token_expire_minutes: int = 60 * 24 # 1 day

    openrouter_api_key: str | None = None
    openrouter_model: str = "openrouter/free"
    openrouter_vision_model: str = "openrouter/free"
    openrouter_vision_fallback_models: str = ""

    @property
    def frontend_origins_list(self) -> list[str]:
        value = self.frontend_origins
        if not value:
            return []
        return [item.strip() for item in value.split(",") if item.strip()]

    @property
    def openrouter_vision_fallback_models_list(self) -> list[str]:
        value = self.openrouter_vision_fallback_models
        if not value:
            return []
        return [item.strip() for item in value.split(",") if item.strip()]


@lru_cache
def get_settings() -> Settings:
    return Settings()
