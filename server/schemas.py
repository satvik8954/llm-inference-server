from pydantic import BaseModel, Field


class InferRequest(BaseModel):
    prompt: str = Field(..., min_length=1)
    system_prompt: str = ""
    max_tokens: int = Field(default=256, ge=1, le=2048)
    priority: int = Field(default=1, ge=1, le=2)
    timeout_ms: int = Field(default=10_000, ge=100, le=120_000)


class QueuedRequest(InferRequest):
    request_id: str
    enqueue_time_ms: int


class InferResponse(BaseModel):
    request_id: str
    response: str
    latency_ms: float
    cache_hit: bool
    batch_size: int
