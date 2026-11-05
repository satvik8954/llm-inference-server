from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    redis_url: str = "redis://localhost:6379/0"
    max_batch_size: int = 8
    max_wait_ms: int = 50
    # "eager": dispatch as soon as the size/age trigger fires (original behaviour).
    # "backpressure": additionally hold batches back while every worker is
    # busy or already has a batch waiting, so batches form at worker-free time.
    dispatch_mode: str = "eager"
    active_workers_key: str = "workers:active"
    # Each worker atomically moves the batch it is processing from
    # worker_batch_queue into its own in-flight list (BLMOVE), so a batch is
    # always counted in exactly one place and never "disappears" between pop
    # and busy-marking.
    inflight_key_prefix: str = "worker:inflight:"
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
