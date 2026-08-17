import pytest

from server.schemas import QueuedRequest
from tests.fakes import FakeModelDouble
from worker.kv_cache import KVCacheManager


@pytest.mark.asyncio
async def test_fake_model_double_is_deterministic_and_reports_cache_hits() -> None:
    """Tests the queue/cache/batch plumbing contract - not real generation.
    Real generation correctness is covered by test_model_loader.py (slow,
    requires GPU/model download, skipped by default)."""
    request = QueuedRequest(
        request_id="req-1",
        prompt="hello",
        system_prompt="system",
        max_tokens=8,
        priority=1,
        timeout_ms=1000,
        enqueue_time_ms=1,
    )
    model = FakeModelDouble(KVCacheManager(max_entries=4))

    first = await model.generate_batch([request])
    second = await model.generate_batch([request])

    assert first[0]["response"] == second[0]["response"]
    assert first[0]["cache_hit"] is False
    assert second[0]["cache_hit"] is True
    assert second[0]["batch_size"] == 1
