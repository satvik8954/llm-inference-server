from typing import Any

from server.schemas import QueuedRequest


def chat_messages(req: QueuedRequest) -> list[dict[str, str]]:
    messages = []
    if req.system_prompt:
        messages.append({"role": "system", "content": req.system_prompt})
    messages.append({"role": "user", "content": req.prompt})
    return messages


def render_chat_prompts(tokenizer: Any, reqs: list[QueuedRequest]) -> tuple[list[str], bool]:
    """Format requests with the model's chat template.

    Without the template a chat model sees raw text, continues it (inventing
    follow-up questions) instead of answering, and rarely emits EOS, so it
    runs to max_new_tokens on every request.

    Returns the prompt strings and whether the tokenizer should still add
    special tokens: TinyLlama's template has no BOS, so the tokenizer must add
    it, but a template that already starts with BOS must not get a second one.
    """
    prompts = [
        tokenizer.apply_chat_template(
            chat_messages(req), tokenize=False, add_generation_prompt=True
        )
        for req in reqs
    ]
    bos = tokenizer.bos_token
    add_special_tokens = not (bos and prompts[0].startswith(bos))
    return prompts, add_special_tokens
