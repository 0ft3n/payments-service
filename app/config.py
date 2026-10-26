from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    api_key: str

    database_url: str = "postgresql+asyncpg://payments:payments@localhost:5432/payments"
    rabbitmq_url: str = "amqp://guest:guest@localhost:5672/"

    outbox_poll_interval: float = 1.0
    outbox_batch_size: int = 100

    # Total number of processing attempts per message before it goes to the DLQ.
    max_attempts: int = 3
    # Delay before attempt N+1 is retry_base_delay * 2 ** (N - 1) seconds.
    retry_base_delay: float = 2.0
    consumer_prefetch: int = 10

    gateway_min_delay: float = 2.0
    gateway_max_delay: float = 5.0
    gateway_success_rate: float = 0.9

    webhook_timeout: float = 5.0

    log_level: str = "INFO"


@lru_cache
def get_settings() -> Settings:
    return Settings()
