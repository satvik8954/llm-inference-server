import pytest
from httpx import ASGITransport, AsyncClient
from prometheus_client import REGISTRY

from server.api import app


@pytest.mark.asyncio
async def test_metrics_endpoint_exposes_expected_names() -> None:
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        response = await client.get("/metrics")

    assert response.status_code == 200
    assert "inference_requests_total" in response.text
    assert "queue_depth" in response.text


def test_metric_names_registered() -> None:
    names = set(REGISTRY._names_to_collectors)
    assert "inference_latency_seconds" in names
