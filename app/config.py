from functools import lru_cache

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

MIN_SECRET_LENGTH = 32


class Settings(BaseSettings):
    """Shared settings. Each process gets only the env vars it needs (see docker-compose.yml),
    so process-specific secrets are optional here and checked by `require_secret` at startup."""

    model_config = SettingsConfigDict(extra="ignore")

    database_url: SecretStr
    rabbitmq_url: SecretStr | None = None  # not needed by the API

    # API
    api_key: SecretStr | None = None
    docs_enabled: bool = False

    # Outbox relay
    outbox_poll_interval: float = Field(default=0.5, gt=0)
    outbox_batch_size: int = Field(default=100, gt=0)

    # Consumer
    consumer_prefetch: int = Field(default=10, gt=0)
    max_attempts: int = Field(default=3, ge=1)
    # Delay before attempt N+1 is retry_base_delay_ms * 2**(N-1): 2s, 4s, ...
    # Changing it requires deleting the retry queues (RabbitMQ refuses to redeclare a queue with other args).
    retry_base_delay_ms: int = Field(default=2000, gt=0)
    processing_min_seconds: float = Field(default=2.0, ge=0)
    processing_max_seconds: float = Field(default=5.0, ge=0)
    processing_success_rate: float = Field(default=0.9, ge=0, le=1)

    # Webhooks
    webhook_secret: SecretStr | None = None
    webhook_timeout_seconds: float = Field(default=10.0, gt=0)
    # Only for local development: allows webhooks to private/loopback/internal addresses.
    webhook_allow_private_networks: bool = False


@lru_cache
def get_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]


def require_value(value: SecretStr | None, name: str) -> str:
    if value is None or not value.get_secret_value():
        raise RuntimeError(f"{name} must be set")
    return value.get_secret_value()


def require_secret(value: SecretStr | None, name: str) -> str:
    if value is None or len(value.get_secret_value()) < MIN_SECRET_LENGTH:
        raise RuntimeError(f"{name} must be set and be at least {MIN_SECRET_LENGTH} characters long")
    return value.get_secret_value()
