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
async def test_backpressure_holds_batch_while_worker_busy(monkeypatch) -> None:
    redis = FakeRedis(decode_responses=True)
    queue = QueueManager(redis)
    scheduler = BatchScheduler(queue)
    monkeypatch.setattr("server.config.settings.max_batch_size", 2)
    monkeypatch.setattr("server.config.settings.dispatch_mode", "backpressure")
    await redis.sadd("workers:active", "w1")
    await redis.lpush("worker:inflight:w1", "batch-in-progress")

    await queue.enqueue("one", InferRequest(prompt="one"))
    await queue.enqueue("two", InferRequest(prompt="two"))

    assert await scheduler.run_once() == 0

    await redis.lrem("worker:inflight:w1", 1, "batch-in-progress")

    assert await scheduler.run_once() == 2


@pytest.mark.asyncio
async def test_backpressure_holds_batch_while_one_already_queued(monkeypatch) -> None:
    redis = FakeRedis(decode_responses=True)
    queue = QueueManager(redis)
    scheduler = BatchScheduler(queue)
    monkeypatch.setattr("server.config.settings.max_batch_size", 1)
    monkeypatch.setattr("server.config.settings.dispatch_mode", "backpressure")
    await redis.sadd("workers:active", "w1")

    await queue.enqueue("one", InferRequest(prompt="one"))
    await queue.enqueue("two", InferRequest(prompt="two"))

    assert await scheduler.run_once() == 1
    assert await scheduler.run_once() == 0


@pytest.mark.asyncio
async def test_backpressure_sees_batch_while_worker_takes_it(monkeypatch) -> None:
    """Regression: the batch must stay visible while the worker picks it up.

    The worker used to BRPOP the batch and only then mark itself busy, so a
    scheduler check in between saw an idle worker with nothing queued and
    dispatched an extra, smaller batch. BLMOVE makes the hand-off atomic.
    """
    redis = FakeRedis(decode_responses=True)
    queue = QueueManager(redis)
    scheduler = BatchScheduler(queue)
    monkeypatch.setattr("server.config.settings.max_batch_size", 1)
    monkeypatch.setattr("server.config.settings.dispatch_mode", "backpressure")
    await redis.sadd("workers:active", "w1")

    await queue.enqueue("one", InferRequest(prompt="one"))
    await queue.enqueue("two", InferRequest(prompt="two"))
    assert await scheduler.run_once() == 1

    # The worker's exact hand-off: queue -> its in-flight list, one command.
    payload = await redis.blmove("worker:batches", "worker:inflight:w1", 0, "RIGHT", "LEFT")
    assert await redis.llen("worker:batches") == 0
    assert await scheduler.run_once() == 0

    await redis.lrem("worker:inflight:w1", 1, payload)
    assert await scheduler.run_once() == 1


@pytest.mark.asyncio
async def test_eager_ignores_busy_worker(monkeypatch) -> None:
    redis = FakeRedis(decode_responses=True)
    queue = QueueManager(redis)
    scheduler = BatchScheduler(queue)
    monkeypatch.setattr("server.config.settings.max_batch_size", 1)
    monkeypatch.setattr("server.config.settings.dispatch_mode", "eager")
    await redis.sadd("workers:active", "w1")
    await redis.lpush("worker:inflight:w1", "batch-in-progress")

    await queue.enqueue("one", InferRequest(prompt="one"))

    assert await scheduler.run_once() == 1


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
