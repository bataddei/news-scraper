"""Settings loaded from environment variables (via .env in development).

Validation is strict: a missing required var raises at import time rather than
surfacing later as a mysterious connection error mid-collection.
"""

from __future__ import annotations

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    supabase_db_url: SecretStr = Field(
        ...,
        description=(
            "Postgres connection string from Supabase → Project Settings → Database "
            "→ Connection string → Transaction pooler."
        ),
    )

    supabase_url: str | None = None
    supabase_service_role_key: SecretStr | None = None

    operator_contact_email: str = Field(
        default="unknown@example.com",
        description="Included in User-Agent. SEC EDGAR rejects requests without a contact.",
    )
    user_agent: str = Field(
        default="news-archive-pipeline/0.1 (+mailto:unknown@example.com)",
    )

    environment: str = Field(default="local", description='"local" or "droplet".')
    log_level: str = Field(default="INFO")

    telegram_bot_token: SecretStr | None = None
    telegram_chat_id: str | None = None
    discord_webhook_url: SecretStr | None = None
    healthchecks_url: str | None = None

    # Literature triage: OpenAI-powered. Optional at import time so collector
    # runs (which don't need it) can start without the key being set.
    openai_api_key: SecretStr | None = None
    literature_triage_model: str = Field(default="gpt-4o")


settings = Settings()  # type: ignore[call-arg]
