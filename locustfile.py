"""
Load test for the LLM inference server.

Two user classes so you can run either scenario independently, or mix them
by weight:

- UniqueUser: sends a prompt nobody has sent before (per-run random suffix),
  so every request is a guaranteed cache miss. This measures real batching
  behavior under concurrent load.

- RepeatUser: sends from a small fixed pool of prompts, so many requests
  hit the response cache after the first pass. This measures cache-hit-rate
  behavior under concurrent load.

Usage:
    pip install locust

    # headless run, 20 concurrent users, 60s, unique-prompt traffic only:
    locust -f locustfile.py --headless -u 20 -r 5 -t 60s --host http://localhost:8000 UniqueUser

    # mixed traffic (both classes weighted, default if no class named):
    locust -f locustfile.py --headless -u 20 -r 5 -t 60s --host http://localhost:8000

    # with a live web UI instead of --headless:
    locust -f locustfile.py --host http://localhost:8000
    # then open http://localhost:8089
"""

import random
import uuid

from locust import HttpUser, task, between, events

# Fixed pool of prompts for the repeated-prompt (cache-hit) scenario.
REPEATED_PROMPTS = [
    "What is the capital of France?",
    "What is the capital of Japan?",
    "What is the capital of Germany?",
    "Explain what a KV cache is in one sentence.",
    "What does CUDA stand for?",
]

RUN_ID = uuid.uuid4().hex[:8]  # unique per test run, so unique-prompt traffic
                                # never collides with a previous run's cache


class UniqueUser(HttpUser):
    """Every request is a fresh prompt -> guaranteed cache miss.
    Use this to measure real batching/throughput under concurrent load."""

    wait_time = between(0.1, 0.5)
    weight = 1

    @task
    def infer_unique(self):
        prompt = f"Tell me a fact about the number {random.randint(1, 10_000_000)} run {RUN_ID}."
        self.client.post(
            "/infer",
            json={"prompt": prompt, "max_tokens": 50, "priority": 1},
            name="/infer [unique]",
        )


class RepeatUser(HttpUser):
    """Draws from a small fixed prompt pool -> real cache hit rate under load.
    Use this to measure the response-cache win under concurrency."""

    wait_time = between(0.1, 0.5)
    weight = 1

    @task
    def infer_repeated(self):
        prompt = random.choice(REPEATED_PROMPTS)
        self.client.post(
            "/infer",
            json={"prompt": prompt, "max_tokens": 50, "priority": 1},
            name="/infer [repeated]",
        )


class PriorityUser(HttpUser):
    """Sends a mix of high/low priority requests -> use this to verify
    priority scheduling actually changes latency ordering under congestion.
    Not included by default; run explicitly with the PriorityUser class."""

    wait_time = between(0.05, 0.2)
    weight = 0  # excluded from default mixed run; invoke by name explicitly

    @task
    def infer_priority(self):
        priority = random.choice([1, 1, 1, 2])  # 25% high priority
        prompt = f"Priority test prompt {random.randint(1, 10_000_000)} run {RUN_ID}."
        self.client.post(
            "/infer",
            json={"prompt": prompt, "max_tokens": 50, "priority": priority},
            name=f"/infer [priority={priority}]",
        )


@events.quitting.add_listener
def _summary(environment, **kwargs):
    stats = environment.stats
    print("\n=== Run summary ===")
    for name, entry in stats.entries.items():
        print(
            f"{name}: requests={entry.num_requests} fails={entry.num_failures} "
            f"p50={entry.get_response_time_percentile(0.5):.0f}ms "
            f"p95={entry.get_response_time_percentile(0.95):.0f}ms "
            f"p99={entry.get_response_time_percentile(0.99):.0f}ms "
            f"avg={entry.avg_response_time:.0f}ms"
        )
