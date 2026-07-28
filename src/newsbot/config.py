from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_prefix="NEWSBOT_", extra="ignore")

    database_url: str = "sqlite+aiosqlite:///newsbot.db"
    dry_run: bool = True
    log_level: str = "INFO"
    user_agent: str = "SaratovNewsBot/0.1 (+https://example.invalid/newsbot)"
    fetch_concurrency: int = Field(default=4, ge=1, le=20)
    fetch_timeout_seconds: float = Field(default=15, gt=0)
    fetch_retries: int = Field(default=3, ge=1, le=6)
    max_response_bytes: int = Field(default=5_000_000, ge=10_000)
    validate_public_source_ips: bool = True
    circuit_failure_threshold: int = Field(default=5, ge=1)
    circuit_cooldown_seconds: int = Field(default=1800, ge=60)
    collecting_ttl_hours: int = Field(default=24, ge=1)
    max_article_age_hours: int = Field(default=72, ge=1)
    event_match_window_hours: int = Field(default=48, ge=1, le=168)
    publication_retry_base_seconds: int = Field(default=30, ge=1)
    sending_stale_seconds: int = Field(default=300, ge=30)
    worker_interval_seconds: int = Field(default=300, ge=10)
    management_token: str = ""

    llm_base_url: str = "https://cheapvibecode.ru/v1"
    llm_api_key: str = ""
    llm_model: str = "deepseek-v4-flash"
    llm_retries: int = Field(default=2, ge=1, le=4)
    llm_retry_base_seconds: float = Field(default=0.5, ge=0, le=10)
    llm_timeout_seconds: float = Field(default=60, gt=0)
    publisher_timeout_seconds: float = Field(default=45, gt=0)

    telegram_api_id: int | None = None
    telegram_api_hash: str = ""
    telegram_session_string: str = ""
    telegram_chat_id: str = ""
    max_token: str = ""
    max_password: str = ""
    max_chat_id: int | None = None


@lru_cache
def get_settings() -> Settings:
    return Settings()
