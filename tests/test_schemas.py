from pydantic import ValidationError

from server.schemas import InferRequest


def test_infer_request_defaults() -> None:
    request = InferRequest(prompt="hello")

    assert request.system_prompt == ""
    assert request.max_tokens == 256
    assert request.priority == 1
    assert request.timeout_ms == 10_000


def test_infer_request_validation() -> None:
    try:
        InferRequest(prompt="", priority=3)
    except ValidationError as exc:
        assert "prompt" in str(exc)
        assert "priority" in str(exc)
    else:
        raise AssertionError("expected validation error")
