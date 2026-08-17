# PRD: LLM Inference Server
**Owner:** Golla Satvik Kumar  
**Target:** Resume project for Google SWE Intern 2027  
**Timeline:** 2 weeks  
**Status:** Planning

---

## 1. Problem Statement

Serving LLMs naively (one request at a time, synchronously) is catastrophically inefficient. Real inference systems handle thousands of concurrent requests by batching, caching, and queuing intelligently. This project builds a production-inspired LLM inference server from scratch — demonstrating mastery of concurrency, distributed systems design, and systems-level thinking that Google explicitly evaluates.

---

## 2. Goals

### Primary Goals
- Build a server that accepts concurrent LLM inference requests and serves them efficiently
- Implement dynamic request batching to maximize GPU/CPU utilization
- Implement KV cache reuse to avoid redundant prefill computation for repeated prompts
- Expose Prometheus metrics (latency p50/p95/p99, throughput, queue depth)
- Containerize with Docker Compose for multi-worker deployment

### Success Metrics (Resume Numbers)
- Demonstrate **≥3x throughput improvement** over naive sequential serving under load
- **p95 latency < 2s** for standard prompt lengths under moderate concurrency
- Support **≥20 concurrent clients** without dropping requests
- Cache hit rate **≥40%** on repeated system prompts in benchmark

---

## 3. Non-Goals
- Not building a training framework
- Not supporting model fine-tuning
- Not building a full-scale production system (no Kubernetes, no autoscaling)
- Not supporting streaming token output (can be a stretch goal)

---

## 4. System Architecture

```
Clients (load test / API consumers)
        │
        ▼
┌─────────────────────────────┐
│       FastAPI Frontend       │  ← HTTP entry point, request validation
│   /infer  /health  /metrics  │
└────────────┬────────────────┘
             │
             ▼
┌─────────────────────────────┐
│       Request Queue          │  ← Redis-backed priority queue
│   (priority + timeout mgmt)  │
└────────────┬────────────────┘
             │
             ▼
┌─────────────────────────────┐
│     Batch Scheduler          │  ← Dynamic batching engine
│  (wait_time / batch_size     │     groups requests → single forward pass
│   threshold logic)           │
└────────────┬────────────────┘
             │
             ▼
┌─────────────────────────────┐
│      KV Cache Manager        │  ← Hash system prompt → cache prefill KV
│  (prompt hash → cached state)│
└────────────┬────────────────┘
             │
             ▼
┌─────────────────────────────┐
│     Model Worker Pool        │  ← Multiple worker processes
│  Worker 1 │ Worker 2 │ ...  │     each loads model, processes batches
└────────────┬────────────────┘
             │
             ▼
┌─────────────────────────────┐
│    Metrics Collector         │  ← Prometheus exporter
│  (latency, throughput, cache)│
└─────────────────────────────┘
```

---

## 5. Component Breakdown

### 5.1 FastAPI Frontend
**File:** `server/api.py`

Endpoints:
- `POST /infer` — accepts `{prompt, system_prompt, max_tokens, priority}`, returns `{request_id, response, latency_ms}`
- `GET /health` — liveness check
- `GET /metrics` — Prometheus scrape endpoint

Request schema:
```python
class InferRequest(BaseModel):
    prompt: str
    system_prompt: str = ""
    max_tokens: int = 256
    priority: int = 1          # 1 = normal, 2 = high
    timeout_ms: int = 10000
```

Key behavior:
- Assign each request a UUID
- Push to Redis queue with priority and timestamp
- Poll for result with timeout
- Return 408 if timeout exceeded

---

### 5.2 Request Queue
**File:** `server/queue_manager.py`

- Use Redis Sorted Set with score = `priority * 1e12 - enqueue_timestamp` (higher priority + earlier = lower score = dequeued first)
- Separate result store: `Redis Hash` keyed by `request_id`
- TTL on results: 60 seconds
- Queue depth exposed as metric

---

### 5.3 Batch Scheduler
**File:** `server/batch_scheduler.py`

Dynamic batching logic:
```
while True:
    wait until:
        - batch_size >= MAX_BATCH_SIZE (default: 8), OR
        - time_since_first_item >= MAX_WAIT_MS (default: 50ms)
    
    pull batch from queue
    send to worker
```

Config (environment variables):
```
MAX_BATCH_SIZE=8
MAX_WAIT_MS=50
WORKER_COUNT=2
```

This is the core systems component — the tradeoff between latency (don't wait too long) and throughput (bigger batches = more efficient) is what makes this interesting to talk about in interviews.

---

### 5.4 KV Cache Manager
**File:** `server/kv_cache.py`

Strategy:
- Hash the `system_prompt` using SHA-256
- On first request with that hash: run prefill, store KV state in memory (per worker)
- On subsequent requests with same hash: skip prefill, reuse cached KV
- LRU eviction when cache exceeds `MAX_CACHE_ENTRIES` (default: 32)

```python
class KVCacheManager:
    def __init__(self, max_entries=32):
        self.cache = OrderedDict()   # hash → kv_state
        self.max_entries = max_entries
    
    def get(self, system_prompt_hash): ...
    def put(self, system_prompt_hash, kv_state): ...
    def evict_lru(self): ...
```

Note: For the actual model, use HuggingFace `transformers` with a small open model (e.g., `microsoft/phi-2` or `TinyLlama/TinyLlama-1.1B`) — no GPU required, runs fine on CPU for demo purposes.

---

### 5.5 Model Worker
**File:** `worker/worker.py`

- Loads model once at startup
- Maintains its own `KVCacheManager` instance
- Receives batches from scheduler via Redis pub/sub or queue
- Processes batch → writes results back to Redis result store
- Registers itself on startup, deregisters on shutdown (simple worker registry in Redis)

```python
class ModelWorker:
    def __init__(self, worker_id, model_name):
        self.model = load_model(model_name)
        self.tokenizer = load_tokenizer(model_name)
        self.kv_cache = KVCacheManager()
    
    def process_batch(self, batch: List[InferRequest]) -> List[str]: ...
    def run(self): ...   # main loop
```

---

### 5.6 Metrics Collector
**File:** `server/metrics.py`

Prometheus metrics to expose:
```python
REQUEST_LATENCY = Histogram(
    'inference_latency_seconds',
    'End-to-end request latency',
    buckets=[0.1, 0.25, 0.5, 1.0, 2.0, 5.0]
)
THROUGHPUT = Counter('inference_requests_total', 'Total requests served')
QUEUE_DEPTH = Gauge('queue_depth', 'Current requests in queue')
BATCH_SIZE = Histogram('batch_size', 'Requests per batch')
CACHE_HIT_RATE = Counter('kv_cache_hits_total', 'KV cache hits')
CACHE_MISS_RATE = Counter('kv_cache_misses_total', 'KV cache misses')
```

Grafana dashboard with 4 panels:
- Request rate over time
- Latency p50/p95/p99
- Batch size distribution
- Cache hit rate

---

## 6. Tech Stack

| Component | Technology |
|-----------|-----------|
| API Server | FastAPI + Uvicorn |
| Queue | Redis (Sorted Sets + Hashes) |
| Worker Communication | Redis pub/sub |
| Model | HuggingFace Transformers (TinyLlama or Phi-2) |
| Metrics | Prometheus + Grafana |
| Containerization | Docker + Docker Compose |
| Load Testing | Locust |
| Language | Python 3.11 |

---

## 7. Repo Structure

```
llm-inference-server/
├── server/
│   ├── api.py               # FastAPI app
│   ├── queue_manager.py     # Redis queue logic
│   ├── batch_scheduler.py   # Dynamic batching engine
│   ├── kv_cache.py          # KV cache manager
│   └── metrics.py           # Prometheus metrics
├── worker/
│   ├── worker.py            # Model worker
│   └── model_loader.py      # HuggingFace model init
├── load_test/
│   └── locustfile.py        # Locust load test scenarios
├── monitoring/
│   ├── prometheus.yml       # Scrape config
│   └── grafana_dashboard.json
├── docker-compose.yml
├── Dockerfile.server
├── Dockerfile.worker
├── requirements.txt
└── README.md                # Architecture diagram + benchmark results
```

---

## 8. 14-Day Build Plan

### Week 1 — Core Server + Batching

**Day 1 — Project scaffold**
- Set up repo, Docker Compose skeleton (FastAPI + Redis containers)
- Write `InferRequest` schema, `/health` endpoint
- Verify containers talk to each other

**Day 2 — Queue Manager**
- Implement Redis Sorted Set queue with priority scoring
- Write unit tests: enqueue, dequeue, priority ordering
- Implement result store with TTL

**Day 3 — Naive worker (no batching)**
- Load TinyLlama in `worker.py`
- Process one request at a time from queue
- End-to-end: POST /infer → queue → worker → result → API response

**Day 4 — Batch Scheduler**
- Implement `MAX_BATCH_SIZE` + `MAX_WAIT_MS` dual-trigger logic
- Group requests into batches, send to worker
- Worker processes batch via HuggingFace batch inference

**Day 5 — Benchmark naive vs batched**
- Write basic Locust load test (20 concurrent users, 60s)
- Measure throughput and latency for: sequential, batch_size=4, batch_size=8
- Record numbers — this is your resume bullet

**Day 6 — KV Cache Manager**
- Implement `KVCacheManager` with LRU eviction
- Integrate into worker: hash system_prompt, skip prefill on hit
- Unit test: cache hit/miss, eviction

**Day 7 — Buffer / review day**
- Fix bugs from integration
- Clean up code, add docstrings
- Ensure Docker Compose `up` works cleanly from scratch

---

### Week 2 — Metrics, Polish, README

**Day 8 — Prometheus metrics**
- Instrument all 6 metrics
- Expose `/metrics` endpoint
- Verify Prometheus scrapes correctly

**Day 9 — Grafana dashboard**
- Import `grafana_dashboard.json`
- Set up 4 panels (latency, throughput, batch size, cache hit rate)
- Screenshot for README

**Day 10 — Timeout + error handling**
- Implement 408 timeout on `/infer`
- Handle worker crashes gracefully (dead letter queue or retry)
- Add request ID to all log lines

**Day 11 — Multi-worker Docker Compose**
- Scale to 2 workers: `docker-compose up --scale worker=2`
- Verify load distribution across workers
- Benchmark: 1 worker vs 2 workers throughput

**Day 12 — Load test + final benchmarks**
- Run full Locust suite: ramp from 1 to 50 concurrent users
- Capture: p50, p95, p99 latency; peak throughput; cache hit rate
- These numbers go directly on your resume

**Day 13 — README**
- Architecture diagram (draw.io or Excalidraw, export as PNG)
- Benchmark results table
- How to run locally (3 commands: clone, docker-compose up, curl)

**Day 14 — Buffer + GitHub polish**
- Clean commit history
- Add GitHub Actions CI (lint + basic smoke test)
- Pin to top of GitHub profile

---

## 9. Resume Bullets (fill in your actual numbers)

```
LLM Inference Server  |  Python, FastAPI, Redis, Docker, Prometheus, HuggingFace

• Built a batched LLM inference server with dynamic request batching
  (dual-trigger: batch size threshold + wait time), achieving Xx throughput
  over naive sequential serving under 20 concurrent clients.

• Implemented a KV cache manager with LRU eviction for system prompt 
  prefill reuse, achieving ~X% cache hit rate and reducing mean latency 
  by Xms on repeated-context workloads.

• Deployed multi-worker architecture via Docker Compose with Prometheus 
  metrics (p50/p95/p99 latency, queue depth, batch size distribution) 
  and Grafana dashboards; p95 latency < 2s at 20 RPS.
```

---

## 10. Interview Talking Points

These are the questions Google will ask about this project:

**"Walk me through your batching logic."**
→ Talk about the dual-trigger: you don't want to wait forever (hurts latency) but you want full batches (hurts throughput if too small). MAX_WAIT_MS is the latency SLA; MAX_BATCH_SIZE is the throughput ceiling. Tradeoff is tunable.

**"How does your KV cache work?"**
→ SHA-256 hash of system prompt → LRU dict of KV states per worker. Prefill is O(n²) in sequence length, so reusing it for identical system prompts is a real win. Hit rate depends on how much prompt reuse there is in your workload.

**"What happens if a worker dies?"**
→ Currently: requests in that worker's in-flight batch are lost. Mitigation: move to a "claimed" queue state, timeout unclaimed results, requeue. Talk about this as a known limitation and how you'd fix it.

**"How did you measure improvement?"**
→ Locust load test, 20 concurrent users, 60s run. Compared sequential vs batch=4 vs batch=8 at fixed concurrency. Throughput metric is requests/second from Prometheus counter.

---

## 11. Stretch Goals (if time permits)

- **Streaming output** — SSE endpoint that streams tokens as they're generated
- **Model routing** — route requests to different models based on prompt length or task type
- **Adaptive batching** — dynamically adjust MAX_BATCH_SIZE based on current queue depth
- **gRPC frontend** — replace or augment FastAPI with gRPC for lower overhead

---

*Build it. Benchmark it. Put the numbers on your resume.*
