import pytest

from server.schemas import QueuedRequest
from worker.kv_cache import KVCacheManager
from worker.model_loader import FakeModel


@pytest.mark.asyncio
async def test_fake_model_is_deterministic_and_reports_cache_hits() -> None:
    request = QueuedRequest(
        request_id="req-1",
        prompt="hello",
        system_prompt="system",
        max_tokens=8,
        priority=1,
        timeout_ms=1000,
        enqueue_time_ms=1,
    )
    model = FakeModel(KVCacheManager(max_entries=4))

    first = await model.generate_batch([request])
    second = await model.generate_batch([request])

    assert first[0]["response"] == second[0]["response"]
    assert first[0]["cache_hit"] is False
    assert second[0]["cache_hit"] is True
    assert second[0]["batch_size"] == 1
