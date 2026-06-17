import asyncio
import json
import logging
import os
import socket

from prometheus_client import start_http_server
from redis.asyncio import Redis

from server.config import settings
from server.metrics import CACHE_HITS, CACHE_MISSES, WORKER_BATCHES, WORKER_REQUESTS
from server.queue_manager import QueueManager
from server.schemas import QueuedRequest
from worker.kv_cache import KVCacheManager
from worker.model_loader import FakeModel


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("worker")


class ModelWorker:
    def __init__(self, redis: Redis, worker_id: str):
        self.redis = redis
        self.worker_id = worker_id
        self.queue = QueueManager(redis)
        self.model = FakeModel(KVCacheManager(settings.max_cache_entries))

    async def process_batch(self, batch_payload: dict[str, object]) -> None:
        requests = [
            QueuedRequest.model_validate(request)
            for request in batch_payload["requests"]
        ]
        results = await self.model.generate_batch(requests)
        for result in results:
            if result["cache_hit"]:
                CACHE_HITS.inc()
            else:
                CACHE_MISSES.inc()
            result["worker_id"] = self.worker_id
            await self.queue.put_result(str(result["request_id"]), result)
        WORKER_BATCHES.inc()
        WORKER_REQUESTS.inc(len(results))
        logger.info(
            "processed batch_id=%s size=%s worker_id=%s",
            batch_payload["batch_id"],
            len(results),
            self.worker_id,
        )

    async def run_forever(self) -> None:
        await self.redis.sadd("workers:active", self.worker_id)
        logger.info("worker started worker_id=%s", self.worker_id)
        try:
            while True:
                _, payload = await self.redis.brpop(settings.worker_batch_queue)
                await self.process_batch(json.loads(payload))
        finally:
            await self.redis.srem("workers:active", self.worker_id)


async def main() -> None:
    metrics_port = int(os.getenv("WORKER_METRICS_PORT", "9102"))
    start_http_server(metrics_port)
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    worker_id = os.getenv("WORKER_ID", socket.gethostname())
    try:
        await ModelWorker(redis, worker_id).run_forever()
    finally:
        await redis.aclose()


if __name__ == "__main__":
    asyncio.run(main())
