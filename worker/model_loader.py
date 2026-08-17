import hashlib
from typing import Any

import torch
from transformers import AutoModelForCausalLM, AutoTokenizer

from server.schemas import QueuedRequest
from worker.kv_cache import KVCacheManager

MODEL_NAME = "TinyLlama/TinyLlama-1.1B-Chat-v1.0"


class RealModel:
    """
    Actual generation via transformers - not a simulator.

    Two distinct performance features, kept honest and separate:

    1. Real batching: multiple requests are tokenized together (left-padded)
       and run through ONE model.generate() call. This is the throughput win
       validated in spike_04 (~60 tok/s batched vs ~35 tok/s sequential on a
       3050 running TinyLlama-1.1B).

    2. Response cache (NOT KV-cache reuse): identical (system_prompt, prompt,
       max_tokens) requests skip generation entirely. This is a real,
       measurable win for repeated queries, but it is NOT the same thing as
       transformer KV-cache / prefix reuse. True cross-request prefix caching
       needs paged-attention-style cache management (what vLLM/TGI implement)
       and is out of scope for this project - claiming it here would repeat
       the exact overclaim this project was rebuilt to fix.
    """

    def __init__(self, cache: KVCacheManager):
        self.cache = cache
        self.device = "cuda" if torch.cuda.is_available() else "cpu"

        self.tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
        self.tokenizer.padding_side = "left"
        if self.tokenizer.pad_token is None:
            self.tokenizer.pad_token = self.tokenizer.eos_token

        self.model = AutoModelForCausalLM.from_pretrained(
            MODEL_NAME,
            torch_dtype=torch.float16 if self.device == "cuda" else torch.float32,
        ).to(self.device)
        self.model.eval()

    async def generate_batch(self, requests: list[QueuedRequest]) -> list[dict[str, Any]]:
        # NOTE: this is a synchronous, blocking GPU call inside an async
        # method. Fine for one worker process reading off the Redis batch
        # queue; a production version would run this in an executor
        # (loop.run_in_executor) so it doesn't block other async work in
        # the same process while the GPU churns.
        results: list[dict[str, Any]] = [None] * len(requests)  # type: ignore
        to_generate: list[tuple[int, QueuedRequest]] = []

        # Step 1: check the response cache for exact-match hits.
        for i, req in enumerate(requests):
            key = self._cache_key(req)
            cached = self.cache.get(key)
            if cached is not None:
                results[i] = {
                    "request_id": req.request_id,
                    "response": cached["response"],
                    "cache_hit": True,
                    "batch_size": len(requests),
                }
            else:
                to_generate.append((i, req))

        # Step 2: real batched generation for everything that missed cache.
        if to_generate:
            indices = [i for i, _ in to_generate]
            reqs = [r for _, r in to_generate]
            generated = self._generate_real_batch(reqs)
            for i, req, text in zip(indices, reqs, generated):
                self.cache.put(self._cache_key(req), {"response": text})
                results[i] = {
                    "request_id": req.request_id,
                    "response": text,
                    "cache_hit": False,
                    "batch_size": len(requests),
                }

        return results  # type: ignore

    def _generate_real_batch(self, reqs: list[QueuedRequest]) -> list[str]:
        prompts = [
            (req.system_prompt + "\n" + req.prompt) if req.system_prompt else req.prompt
            for req in reqs
        ]
        max_new = min(max(req.max_tokens for req in reqs), 512)
        min_new = min(8, max_new)

        inputs = self.tokenizer(prompts, return_tensors="pt", padding=True).to(self.device)

        with torch.no_grad():
            output = self.model.generate(
                **inputs,
                max_new_tokens=max_new,
                min_new_tokens=min_new,
                do_sample=False,
                pad_token_id=self.tokenizer.pad_token_id,
            )

        input_len = inputs["input_ids"].shape[1]
        return [
            self.tokenizer.decode(output[i][input_len:], skip_special_tokens=True)
            for i in range(len(reqs))
        ]

    @staticmethod
    def _cache_key(req: QueuedRequest) -> str:
        raw = f"{req.system_prompt}\n{req.prompt}\n{req.max_tokens}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()
