import asyncio
import json
import logging
import os
import socket
import time

from prometheus_client import start_http_server
from redis.asyncio import Redis

from server.config import settings
from server.metrics import (
    CACHE_HITS,
    CACHE_MISSES,
    REQUEST_TIMEOUTS,
    WORKER_BATCHES,
    WORKER_REQUESTS,
)
from server.queue_manager import QueueManager
from server.schemas import QueuedRequest
from worker.kv_cache import KVCacheManager


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("worker")


class ModelWorker:
    def __init__(self, redis: Redis, worker_id: str, model=None):
        self.redis = redis
        self.worker_id = worker_id
        self.queue = QueueManager(redis)
        if model is None:
            # Imported here so tests can drive the worker loop with a test
            # double without importing torch or loading weights.
            from worker.model_loader import RealModel

            model = RealModel(KVCacheManager(settings.max_cache_entries))
        self.model = model

    async def process_batch(self, batch_payload: dict[str, object]) -> None:
        requests = [
            QueuedRequest.model_validate(request)
            for request in batch_payload["requests"]
        ]

        # Drop requests whose client has already given up. A request enqueued
        # more than timeout_ms ago has almost certainly already received a
        # 408 from the API's own wait loop - running generation for it just
        # burns GPU time on a response nobody will read, and (worse) lets an
        # overload backlog keep consuming worker capacity long after the
        # traffic that caused it has stopped. This does not fix GPU
        # throughput; it stops wasting it on dead work.
        now_ms = int(time.time() * 1000)
        live: list[QueuedRequest] = []
        expired: list[QueuedRequest] = []
        for req in requests:
            if now_ms - req.enqueue_time_ms > req.timeout_ms:
                expired.append(req)
            else:
                live.append(req)

        for req in expired:
            REQUEST_TIMEOUTS.inc()
            logger.info(
                "dropped expired request_id=%s waited_ms=%s timeout_ms=%s",
                req.request_id,
                now_ms - req.enqueue_time_ms,
                req.timeout_ms,
            )

        results = await self.model.generate_batch(live) if live else []
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
            "processed batch_id=%s size=%s dropped=%s worker_id=%s",
            batch_payload["batch_id"],
            len(results),
            len(expired),
            self.worker_id,
        )

    async def run_forever(self) -> None:
        inflight_key = QueueManager.inflight_key(self.worker_id)
        await self.requeue_leftover_inflight(inflight_key)
        await self.redis.sadd(settings.active_workers_key, self.worker_id)
        logger.info("worker started worker_id=%s", self.worker_id)
        try:
            while True:
                await self.run_once(inflight_key)
        finally:
            await self.redis.srem(settings.active_workers_key, self.worker_id)

    async def run_once(self, inflight_key: str, timeout_s: int = 1) -> bool:
        """Take one batch (waiting up to timeout_s) and process it."""
        # BLMOVE pops the batch and records it as this worker's in-flight
        # batch in one atomic step. With a separate BRPOP + "mark busy", a
        # backpressure scheduler checking in between saw the worker as idle
        # with nothing queued and dispatched an extra, smaller batch.
        payload = await self.redis.blmove(
            settings.worker_batch_queue, inflight_key, timeout_s, "RIGHT", "LEFT"
        )
        if payload is None:  # timed out with nothing queued
            return False
        try:
            await self.process_batch(json.loads(payload))
        finally:
            await self.redis.lrem(inflight_key, 1, payload)
        return True

    async def requeue_leftover_inflight(self, inflight_key: str) -> None:
        """Return batches a previous run of this worker id never finished.

        RPUSH puts them at the consuming end of the batch queue so they run
        next; process_batch still drops any request that has since expired.
        """
        while True:
            payload = await self.redis.lmove(
                inflight_key, settings.worker_batch_queue, "RIGHT", "RIGHT"
            )
            if payload is None:
                return
            logger.info("requeued unfinished batch from previous run worker_id=%s", self.worker_id)


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