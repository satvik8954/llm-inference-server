import hashlib
from collections import OrderedDict
from typing import Any


class KVCacheManager:
    def __init__(self, max_entries: int = 32):
        self.max_entries = max_entries
        self.cache: OrderedDict[str, Any] = OrderedDict()

    @staticmethod
    def hash_system_prompt(system_prompt: str) -> str:
        return hashlib.sha256(system_prompt.encode("utf-8")).hexdigest()

    def get(self, system_prompt_hash: str) -> Any | None:
        value = self.cache.get(system_prompt_hash)
        if value is None:
            return None
        self.cache.move_to_end(system_prompt_hash)
        return value

    def put(self, system_prompt_hash: str, kv_state: Any) -> None:
        self.cache[system_prompt_hash] = kv_state
        self.cache.move_to_end(system_prompt_hash)
        self.evict_lru()

    def evict_lru(self) -> None:
        while len(self.cache) > self.max_entries:
            self.cache.popitem(last=False)
