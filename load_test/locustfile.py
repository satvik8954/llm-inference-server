from locust import HttpUser, between, task


SYSTEM_PROMPTS = [
    "You are a concise coding assistant.",
    "You explain distributed systems clearly.",
    "You write practical benchmark summaries.",
]


class InferenceUser(HttpUser):
    wait_time = between(0.1, 0.5)

    @task
    def infer(self) -> None:
        idx = self.environment.runner.user_count % len(SYSTEM_PROMPTS)
        self.client.post(
            "/infer",
            json={
                "system_prompt": SYSTEM_PROMPTS[idx],
                "prompt": "Summarize dynamic batching in one sentence.",
                "max_tokens": 128,
                "priority": 1,
                "timeout_ms": 10_000,
            },
            timeout=15,
        )
