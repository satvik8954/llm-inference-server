import asyncio
import json
import time
from typing import Any

from redis.exceptions import ResponseError
from redis.asyncio import Redis

from server.config import settings
from server.schemas import InferRequest, QueuedRequest


class QueueManager:
    def __init__(self, redis: Redis):
        self.redis = redis

    @staticmethod
    def now_ms() -> int:
        return int(time.time() * 1000)

    @staticmethod
    def priority_score(priority: int, enqueue_time_ms: int) -> float:
        return -priority * 1_000_000_000_000 + enqueue_time_ms

    async def enqueue(self, request_id: str, request: InferRequest) -> QueuedRequest:
        queued = QueuedRequest(
            request_id=request_id,
            enqueue_time_ms=self.now_ms(),
            **request.model_dump(),
        )
        payload_key = self._payload_key(request_id)
        await self.redis.set(
            payload_key,
            queued.model_dump_json(),
            ex=settings.request_ttl_seconds,
        )
        await self.redis.zadd(
            settings.pending_queue_key,
            {request_id: self.priority_score(request.priority, queued.enqueue_time_ms)},
        )
        return queued

    async def queue_depth(self) -> int:
        return int(await self.redis.zcard(settings.pending_queue_key))

    async def worker_capacity_free(self) -> bool:
        """True if some worker is idle with nothing already queued for it."""
        pipe = self.redis.pipeline()
        pipe.llen(settings.worker_batch_queue)
        pipe.scard(settings.busy_workers_key)
        pipe.scard(settings.active_workers_key)
        queued, busy, active = await pipe.execute()
        return int(queued) + int(busy) < max(1, int(active))

    async def peek_oldest(self, count: int = 1) -> list[QueuedRequest]:
        request_ids = await self.redis.zrange(settings.pending_queue_key, 0, count - 1)
        if not request_ids:
            return []
        payloads = await self.redis.mget([self._payload_key(rid) for rid in request_ids])
        return [
            QueuedRequest.model_validate_json(payload)
            for payload in payloads
            if payload is not None
        ]

    async def claim_batch(self, max_batch_size: int) -> list[QueuedRequest]:
        script = """
        local ids = redis.call('ZRANGE', KEYS[1], 0, tonumber(ARGV[1]) - 1)
        if #ids == 0 then
            return {}
        end
        redis.call('ZREM', KEYS[1], unpack(ids))
        return ids
        """
        try:
            request_ids = await self.redis.eval(
                script,
                1,
                settings.pending_queue_key,
                max_batch_size,
            )
        except ResponseError as exc:
            if "unknown command" not in str(exc).lower():
                raise
            request_ids = await self.redis.zrange(
                settings.pending_queue_key,
                0,
                max_batch_size - 1,
            )
            if request_ids:
                await self.redis.zrem(settings.pending_queue_key, *request_ids)
        if not request_ids:
            return []
        payloads = await self.redis.mget([self._payload_key(rid) for rid in request_ids])
        requests: list[QueuedRequest] = []
        for request_id, payload in zip(request_ids, payloads):
            if payload is None:
                continue
            requests.append(QueuedRequest.model_validate_json(payload))
            await self.redis.delete(self._payload_key(request_id))
        return requests

    async def dispatch_batch(self, batch: list[QueuedRequest]) -> str:
        batch_id = f"batch-{self.now_ms()}-{batch[0].request_id}"
        payload = {
            "batch_id": batch_id,
            "requests": [request.model_dump() for request in batch],
        }
        await self.redis.lpush(settings.worker_batch_queue, json.dumps(payload))
        return batch_id

    async def wait_for_result(self, request_id: str, timeout_ms: int) -> dict[str, Any] | None:
        deadline = time.monotonic() + timeout_ms / 1000
        result_key = self._result_key(request_id)
        while time.monotonic() < deadline:
            payload = await self.redis.get(result_key)
            if payload is not None:
                await self.redis.delete(result_key)
                return json.loads(payload)
            await asyncio.sleep(0.01)
        return None

    async def put_result(self, request_id: str, result: dict[str, Any]) -> None:
        await self.redis.set(
            self._result_key(request_id),
            json.dumps(result),
            ex=settings.result_ttl_seconds,
        )

    def _payload_key(self, request_id: str) -> str:
        return f"{settings.payload_key_prefix}{request_id}"

    def _result_key(self, request_id: str) -> str:
        return f"{settings.result_key_prefix}{request_id}"
