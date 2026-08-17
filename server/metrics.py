from prometheus_client import Counter, Gauge, Histogram


REQUEST_LATENCY = Histogram(
    "inference_latency_seconds",
    "End-to-end request latency.",
    buckets=[0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0],
)
THROUGHPUT = Counter("inference_requests_total", "Total requests served.")
REQUEST_TIMEOUTS = Counter("inference_timeouts_total", "Total timed out requests.")
QUEUE_DEPTH = Gauge("queue_depth", "Current requests waiting in the pending queue.")
BATCH_SIZE = Histogram(
    "batch_size",
    "Requests per dispatched batch.",
    buckets=[1, 2, 4, 8, 16, 32],
)
CACHE_HITS = Counter("kv_cache_hits_total", "KV cache hits.")
CACHE_MISSES = Counter("kv_cache_misses_total", "KV cache misses.")
WORKER_BATCHES = Counter("worker_batches_total", "Batches processed by workers.")
WORKER_REQUESTS = Counter("worker_requests_total", "Requests processed by workers.")
SCHEDULER_BATCHES = Counter("scheduler_batches_total", "Batches dispatched by scheduler.")
