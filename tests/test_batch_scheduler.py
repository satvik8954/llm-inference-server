import pytest
from fakeredis.aioredis import FakeRedis

from server.batch_scheduler import BatchScheduler
from server.queue_manager import QueueManager
from server.schemas import InferRequest


@pytest.mark.asyncio
async def test_scheduler_dispatches_when_batch_full(monkeypatch) -> None:
    redis = FakeRedis(decode_responses=True)
    queue = QueueManager(redis)
    scheduler = BatchScheduler(queue)
    monkeypatch.setattr("server.config.settings.max_batch_size", 2)

    await queue.enqueue("one", InferRequest(prompt="one"))
    await queue.enqueue("two", InferRequest(prompt="two"))

    dispatched = await scheduler.run_once()

    assert dispatched == 2


@pytest.mark.asyncio
async def test_scheduler_waits_before_max_wait(monkeypatch) -> None:
    redis = FakeRedis(decode_responses=True)
    queue = QueueManager(redis)
    scheduler = BatchScheduler(queue)
    monkeypatch.setattr("server.config.settings.max_batch_size", 8)
    monkeypatch.setattr("server.config.settings.max_wait_ms", 10_000)

    await queue.enqueue("one", InferRequest(prompt="one"))

    dispatched = await scheduler.run_once()

    assert dispatched == 0
