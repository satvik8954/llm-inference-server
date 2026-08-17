import pytest
from fakeredis.aioredis import FakeRedis

from server.queue_manager import QueueManager
from server.schemas import InferRequest


@pytest.mark.asyncio
async def test_queue_priority_ordering() -> None:
    redis = FakeRedis(decode_responses=True)
    queue = QueueManager(redis)

    await queue.enqueue("normal", InferRequest(prompt="normal", priority=1))
    await queue.enqueue("high", InferRequest(prompt="high", priority=2))

    batch = await queue.claim_batch(2)

    assert [request.request_id for request in batch] == ["high", "normal"]


@pytest.mark.asyncio
async def test_result_store_round_trip() -> None:
    redis = FakeRedis(decode_responses=True)
    queue = QueueManager(redis)

    await queue.put_result("req", {"response": "ok", "cache_hit": False, "batch_size": 1})
    result = await queue.wait_for_result("req", timeout_ms=100)

    assert result == {"response": "ok", "cache_hit": False, "batch_size": 1}
