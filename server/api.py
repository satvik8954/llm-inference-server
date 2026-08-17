import time
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.responses import Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from redis.asyncio import Redis

from server.config import settings
from server.metrics import QUEUE_DEPTH, REQUEST_LATENCY, REQUEST_TIMEOUTS, THROUGHPUT
from server.queue_manager import QueueManager
from server.schemas import InferRequest, InferResponse


@asynccontextmanager
async def lifespan(app: FastAPI):
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    app.state.redis = redis
    app.state.queue = QueueManager(redis)
    yield
    await redis.aclose()


app = FastAPI(title="LLM Inference Server", version="0.1.0", lifespan=lifespan)


@app.get("/health")
async def health() -> dict[str, str]:
    await app.state.redis.ping()
    return {"status": "ok"}


@app.post("/infer", response_model=InferResponse)
async def infer(request: InferRequest) -> InferResponse:
    request_id = str(uuid.uuid4())
    start = time.perf_counter()
    await app.state.queue.enqueue(request_id, request)
    QUEUE_DEPTH.set(await app.state.queue.queue_depth())

    result = await app.state.queue.wait_for_result(request_id, request.timeout_ms)
    latency_ms = (time.perf_counter() - start) * 1000
    if result is None:
        REQUEST_TIMEOUTS.inc()
        raise HTTPException(status_code=408, detail={"request_id": request_id, "error": "timeout"})

    REQUEST_LATENCY.observe(latency_ms / 1000)
    THROUGHPUT.inc()
    QUEUE_DEPTH.set(await app.state.queue.queue_depth())
    return InferResponse(
        request_id=request_id,
        response=result["response"],
        latency_ms=latency_ms,
        cache_hit=bool(result["cache_hit"]),
        batch_size=int(result["batch_size"]),
    )


@app.get("/metrics")
async def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
