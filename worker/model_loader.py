import asyncio
import hashlib

from server.schemas import QueuedRequest
from worker.kv_cache import KVCacheManager


class FakeModel:
    """Deterministic inference simulator with batching and KV-cache speedups."""

    def __init__(self, cache: KVCacheManager):
        self.cache = cache

    async def generate_batch(self, requests: list[QueuedRequest]) -> list[dict[str, object]]:
        cache_hits: list[bool] = []
        unique_misses: set[str] = set()

        for request in requests:
            prompt_hash = self.cache.hash_system_prompt(request.system_prompt)
            if self.cache.get(prompt_hash) is None:
                cache_hits.append(False)
                unique_misses.add(prompt_hash)
            else:
                cache_hits.append(True)

        prefill_seconds = 0.025 * len(unique_misses)
        decode_seconds = min(max(request.max_tokens for request in requests), 512) * 0.0004
        batch_overhead_seconds = 0.02 + 0.004 * len(requests)
        await asyncio.sleep(prefill_seconds + decode_seconds + batch_overhead_seconds)

        for prompt_hash in unique_misses:
            self.cache.put(prompt_hash, {"prefilled": True})

        return [
            {
                "request_id": request.request_id,
                "response": self._response_for(request),
                "cache_hit": cache_hit,
                "batch_size": len(requests),
            }
            for request, cache_hit in zip(requests, cache_hits)
        ]

    @staticmethod
    def _response_for(request: QueuedRequest) -> str:
        digest = hashlib.sha256(
            f"{request.system_prompt}\n{request.prompt}\n{request.max_tokens}".encode("utf-8")
        ).hexdigest()[:12]
        return f"simulated-response-{digest}"
