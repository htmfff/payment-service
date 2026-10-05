from __future__ import annotations

from functools import lru_cache

from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from app.core.enums import Environment
from app.core.retries import retry_tier_delays

INSECURE_DEFAULT_API_KEY = "local-dev-api-key"
INSECURE_DEFAULT_WEBHOOK_SECRET = "local-dev-webhook-secret"  # noqa: S105 - refused in production


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    application_name: str = "payment-service"
    environment: Environment = Environment.LOCAL
    log_level: str = "INFO"

    database_url: str = "postgresql+asyncpg://payments:payments@localhost:5432/payments"
    db_pool_size: int = Field(default=10, ge=1)
    db_max_overflow: int = Field(default=10, ge=0)

    api_key: SecretStr = SecretStr(INSECURE_DEFAULT_API_KEY)
    webhook_signing_secret: SecretStr = SecretStr(INSECURE_DEFAULT_WEBHOOK_SECRET)

    rabbitmq_url: str = "amqp://payments:payments@localhost:5672/"

    outbox_poll_interval_seconds: float = Field(default=1.0, gt=0)
    outbox_batch_size: int = Field(default=100, ge=1, le=1000)
    outbox_max_attempts: int = Field(default=20, ge=1)
    outbox_retry_base_seconds: float = Field(default=5.0, gt=0)

    gateway_min_latency_seconds: float = Field(default=2.0, ge=0)
    gateway_max_latency_seconds: float = Field(default=5.0, ge=0)
    gateway_success_rate: float = Field(default=0.9, ge=0, le=1)
    gateway_unavailable_rate: float = Field(default=0.0, ge=0, le=1)

    processing_max_attempts: int = Field(default=3, ge=1)
    processing_retry_base_seconds: int = Field(default=5, ge=0)
    processing_retry_multiplier: int = Field(default=3, ge=1)

    webhook_timeout_seconds: float = Field(default=5.0, gt=0)
    webhook_max_attempts: int = Field(default=3, ge=1)
    webhook_backoff_base_seconds: float = Field(default=1.0, ge=0)

    @field_validator("log_level")
    @classmethod
    def _normalize_log_level(cls, value: str) -> str:
        return value.upper()

    @model_validator(mode="after")
    def _validate_consistency(self) -> Settings:
        if self.gateway_min_latency_seconds > self.gateway_max_latency_seconds:
            raise ValueError(
                "GATEWAY_MIN_LATENCY_SECONDS must not exceed GATEWAY_MAX_LATENCY_SECONDS"
            )
        if self.environment is Environment.PRODUCTION:
            if self.api_key.get_secret_value() == INSECURE_DEFAULT_API_KEY:
                raise ValueError("API_KEY must be overridden in production")
            if self.webhook_signing_secret.get_secret_value() == INSECURE_DEFAULT_WEBHOOK_SECRET:
                raise ValueError("WEBHOOK_SIGNING_SECRET must be overridden in production")
        return self

    @property
    def sync_database_url(self) -> str:
        """Same database, synchronous driver, used by Alembic migrations."""
        return self.database_url.replace("+asyncpg", "+psycopg")

    @property
    def processing_retry_delays(self) -> tuple[int, ...]:
        """Delay tiers, in seconds, applied between payment processing attempts."""
        return retry_tier_delays(
            self.processing_max_attempts,
            base_seconds=self.processing_retry_base_seconds,
            multiplier=self.processing_retry_multiplier,
        )

    @property
    def is_production(self) -> bool:
        return self.environment is Environment.PRODUCTION


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
