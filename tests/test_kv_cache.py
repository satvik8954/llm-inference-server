from worker.kv_cache import KVCacheManager


def test_cache_hit_and_lru_eviction() -> None:
    cache = KVCacheManager(max_entries=2)
    first = cache.hash_system_prompt("first")
    second = cache.hash_system_prompt("second")
    third = cache.hash_system_prompt("third")

    cache.put(first, "a")
    cache.put(second, "b")
    assert cache.get(first) == "a"

    cache.put(third, "c")

    assert cache.get(second) is None
    assert cache.get(first) == "a"
    assert cache.get(third) == "c"
