import asyncio
import logging

from prometheus_client import start_http_server
from redis.asyncio import Redis

from server.config import settings
from server.metrics import BATCH_SIZE, QUEUE_DEPTH, SCHEDULER_BATCHES
from server.queue_manager import QueueManager


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("batch_scheduler")


class BatchScheduler:
    def __init__(self, queue: QueueManager):
        self.queue = queue

    async def should_dispatch(self) -> bool:
        depth = await self.queue.queue_depth()
        QUEUE_DEPTH.set(depth)
        if depth == 0:
            return False
        # In backpressure mode, requests keep accumulating in the pending set
        # while the workers are occupied, so the next batch is claimed when a
        # worker frees up instead of being split into tiny early batches.
        if settings.dispatch_mode == "backpressure" and not await self.queue.worker_capacity_free():
            return False
        if depth >= settings.max_batch_size:
            return True
        oldest = await self.queue.peek_oldest(1)
        if not oldest:
            return False
        return self.queue.now_ms() - oldest[0].enqueue_time_ms >= settings.max_wait_ms

    async def run_once(self) -> int:
        if not await self.should_dispatch():
            return 0
        batch = await self.queue.claim_batch(settings.max_batch_size)
        if not batch:
            return 0
        batch_id = await self.queue.dispatch_batch(batch)
        BATCH_SIZE.observe(len(batch))
        SCHEDULER_BATCHES.inc()
        logger.info("dispatched batch_id=%s size=%s", batch_id, len(batch))
        return len(batch)

    async def run_forever(self) -> None:
        logger.info(
            "scheduler started max_batch_size=%s max_wait_ms=%s dispatch_mode=%s",
            settings.max_batch_size,
            settings.max_wait_ms,
            settings.dispatch_mode,
        )
        while True:
            dispatched = await self.run_once()
            await asyncio.sleep(0 if dispatched else 0.005)


async def main() -> None:
    start_http_server(settings.metrics_port)
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    try:
        await BatchScheduler(QueueManager(redis)).run_forever()
    finally:
        await redis.aclose()


if __name__ == "__main__":
    asyncio.run(main())
