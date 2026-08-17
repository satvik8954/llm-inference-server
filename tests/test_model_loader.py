"""
Slow integration test for the real model. Skipped by default - requires
downloading TinyLlama-1.1B (~2.2GB) and benefits from a CUDA GPU (will run
on CPU but slowly). Run explicitly with:

    pytest tests/test_model_loader.py -m slow --run-slow
"""
import pytest

from server.schemas import QueuedRequest
from worker.kv_cache import KVCacheManager

pytestmark = pytest.mark.slow


@pytest.fixture(scope="module")
def real_model():
    from worker.model_loader import RealModel
    return RealModel(KVCacheManager(max_entries=8))


@pytest.mark.asyncio
async def test_real_model_generates_and_caches(real_model) -> None:
    request = QueuedRequest(
        request_id="req-1",
        prompt="What is the capital of France?",
        system_prompt="",
        max_tokens=16,
        priority=1,
        timeout_ms=10_000,
        enqueue_time_ms=1,
    )

    first = await real_model.generate_batch([request])
    second = await real_model.generate_batch([request])

    assert first[0]["response"]  # non-empty real generation
    assert first[0]["cache_hit"] is False
    assert second[0]["response"] == first[0]["response"]
    assert second[0]["cache_hit"] is True


@pytest.mark.asyncio
async def test_real_model_batches_multiple_requests(real_model) -> None:
    requests = [
        QueuedRequest(
            request_id=f"req-{i}",
            prompt=prompt,
            system_prompt="",
            max_tokens=12,
            priority=1,
            timeout_ms=10_000,
            enqueue_time_ms=1,
        )
        for i, prompt in enumerate(["Say hi.", "Name a color.", "Count to three."])
    ]

    results = await real_model.generate_batch(requests)

    assert len(results) == 3
    assert all(r["batch_size"] == 3 for r in results)
    assert all(r["response"] for r in results)
