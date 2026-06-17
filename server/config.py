from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    redis_url: str = "redis://localhost:6379/0"
    max_batch_size: int = 8
    max_wait_ms: int = 50
    request_timeout_ms: int = 10_000
    result_ttl_seconds: int = 60
    request_ttl_seconds: int = 120
    worker_batch_queue: str = "worker:batches"
    pending_queue_key: str = "requests:pending"
    payload_key_prefix: str = "request:payload:"
    result_key_prefix: str = "request:result:"
    max_cache_entries: int = 32
    metrics_port: int = 9101

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")


settings = Settings()
