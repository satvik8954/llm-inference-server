import json

import pytest
from fakeredis.aioredis import FakeRedis

from server.queue_manager import QueueManager
from server.schemas import InferRequest
from tests.fakes import FakeModelDouble
from worker.kv_cache import KVCacheManager
from worker.worker import ModelWorker


@pytest.mark.asyncio
async def test_worker_processes_batch_and_clears_inflight() -> None:
    redis = FakeRedis(decode_responses=True)
    queue = QueueManager(redis)
    worker = ModelWorker(redis, "w1", model=FakeModelDouble(KVCacheManager(max_entries=4)))

    await queue.enqueue("one", InferRequest(prompt="one"))
    await queue.dispatch_batch(await queue.claim_batch(8))

    assert await worker.run_once("worker:inflight:w1") is True

    assert await redis.llen("worker:batches") == 0
    assert await redis.llen("worker:inflight:w1") == 0
    result = json.loads(await redis.get("request:result:one"))
    assert result["worker_id"] == "w1"


@pytest.mark.asyncio
async def test_worker_run_once_returns_false_when_queue_empty() -> None:
    redis = FakeRedis(decode_responses=True)
    worker = ModelWorker(redis, "w1", model=FakeModelDouble(KVCacheManager(max_entries=4)))

    assert await worker.run_once("worker:inflight:w1", timeout_s=1) is False


@pytest.mark.asyncio
async def test_worker_requeues_batch_left_inflight_by_previous_run() -> None:
    redis = FakeRedis(decode_responses=True)
    queue = QueueManager(redis)
    await queue.enqueue("stale", InferRequest(prompt="stale"))
    await queue.enqueue("new", InferRequest(prompt="new"))
    stale = await queue.claim_batch(1)
    await queue.dispatch_batch(await queue.claim_batch(1))
    # Simulate a crash: the stale batch was taken but never finished.
    await redis.lpush(
        "worker:inflight:w1",
        json.dumps({"batch_id": "stale-batch", "requests": [r.model_dump() for r in stale]}),
    )

    worker = ModelWorker(redis, "w1", model=FakeModelDouble(KVCacheManager(max_entries=4)))
    await worker.requeue_leftover_inflight("worker:inflight:w1")

    assert await redis.llen("worker:inflight:w1") == 0
    # Requeued at the consuming (right) end, so it runs before newer batches.
    next_batch = json.loads(await redis.rpop("worker:batches"))
    assert next_batch["batch_id"] == "stale-batch"
