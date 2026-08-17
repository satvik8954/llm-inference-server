"""
Test doubles. FakeModelDouble exists ONLY for fast unit tests of the
caching/batching contract (queue_manager, batch_scheduler, worker plumbing)
without downloading real model weights or needing a GPU.

This must never be imported by production code (worker/, server/) - only
by tests/. Production generation goes through worker.model_loader.RealModel.
"""
import hashlib

from server.schemas import QueuedRequest
from worker.kv_cache import KVCacheManager


class FakeModelDouble:
    """Deterministic stand-in for RealModel, for testing plumbing only."""

    def __init__(self, cache: KVCacheManager):
        self.cache = cache

    async def generate_batch(self, requests: list[QueuedRequest]) -> list[dict[str, object]]:
        results = []
        for request in requests:
            key = self._cache_key(request)
            cached = self.cache.get(key)
            cache_hit = cached is not None
            if not cache_hit:
                response = self._response_for(request)
                self.cache.put(key, {"response": response})
            else:
                response = cached["response"]
            results.append({
                "request_id": request.request_id,
                "response": response,
                "cache_hit": cache_hit,
                "batch_size": len(requests),
            })
        return results

    @staticmethod
    def _cache_key(request: QueuedRequest) -> str:
        raw = f"{request.system_prompt}\n{request.prompt}\n{request.max_tokens}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    @staticmethod
    def _response_for(request: QueuedRequest) -> str:
        digest = hashlib.sha256(
            f"{request.system_prompt}\n{request.prompt}\n{request.max_tokens}".encode("utf-8")
        ).hexdigest()[:12]
        return f"fake-response-{digest}"
