from server.schemas import QueuedRequest
from worker.prompting import chat_messages, render_chat_prompts


class ZephyrStyleTokenizer:
    """Mimics TinyLlama-Chat's template: <|role|> blocks, no BOS."""

    bos_token = "<s>"
    eos_token = "</s>"

    def apply_chat_template(self, messages, tokenize, add_generation_prompt):
        assert tokenize is False
        text = "".join(
            f"<|{m['role']}|>\n{m['content']}{self.eos_token}\n" for m in messages
        )
        return text + ("<|assistant|>\n" if add_generation_prompt else "")


class BosInTemplateTokenizer(ZephyrStyleTokenizer):
    def apply_chat_template(self, messages, tokenize, add_generation_prompt):
        return self.bos_token + super().apply_chat_template(
            messages, tokenize, add_generation_prompt
        )


def _req(prompt: str, system_prompt: str = "") -> QueuedRequest:
    return QueuedRequest(
        request_id="r",
        prompt=prompt,
        system_prompt=system_prompt,
        max_tokens=16,
        priority=1,
        timeout_ms=1000,
        enqueue_time_ms=1,
    )


def test_chat_messages_omit_empty_system_prompt() -> None:
    assert chat_messages(_req("hi")) == [{"role": "user", "content": "hi"}]
    assert chat_messages(_req("hi", "be brief")) == [
        {"role": "system", "content": "be brief"},
        {"role": "user", "content": "hi"},
    ]


def test_render_uses_template_and_generation_prompt() -> None:
    prompts, add_special = render_chat_prompts(
        ZephyrStyleTokenizer(), [_req("What is 2+2?", "Be brief."), _req("Hi")]
    )

    assert prompts[0] == (
        "<|system|>\nBe brief.</s>\n<|user|>\nWhat is 2+2?</s>\n<|assistant|>\n"
    )
    assert prompts[1] == "<|user|>\nHi</s>\n<|assistant|>\n"
    # Template has no BOS, so the tokenizer must add it.
    assert add_special is True


def test_render_does_not_double_bos() -> None:
    prompts, add_special = render_chat_prompts(BosInTemplateTokenizer(), [_req("Hi")])

    assert prompts[0].startswith("<s>")
    assert add_special is False
