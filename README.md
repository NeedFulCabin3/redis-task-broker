# redis-task-broker

> Reliable asynchronous task execution and background job orchestration powered by Python asyncio and Redis atomic list primitives.

## Overview / Value Proposition

Background job orchestration in Python often forces developers into heavy external dependencies or full-blown frameworks like Celery or RQ when only reliable, lightweight task processing is required. Standard queue implementations frequently suffer from job loss during sudden process crashes or worker node failures.

`redis-task-broker` solves this operational friction by implementing a minimal, zero-framework job queue architecture. Utilizing Redis atomic operations (`BRPOPLPUSH`) and Python's native `asyncio` event loop alongside multi-process isolation, it guarantees reliable job delivery, transparent execution of both synchronous blocking tasks and native coroutines, exponential backoff retries, dead-letter routing, and real-time state inspection.

## How It Works

```bash
[ Task Queue Enqueue ]
                              │
                              ▼
                 ┌─────────────────────────┐
                 │   Redis pipeline()      │
                 │  - Hash/JSON Metadata   │
                 │  - RPUSH queue:pending  │
                 └────────────┬────────────┘
                              │
                              ▼
                    ( Redis queue:pending )
                              │
                              ▼
           ┌─────────────────────────────────────┐
           │    Worker BRPOPLPUSH (Atomic)       │
           └──────────────────┬──────────────────┘
                              │
                              ▼
                ( queue:processing:worker-X )
                              │
     ┌────────────────────────┴────────────────────────┐
     │                                                 │
     [ Sync Callable ]                                [ Async Callable ]
    │                                                 │
    ▼                                                 ▼
    asyncio.to_thread()                              await coroutine()
    │                                                 │
    └────────────────────────┬────────────────────────┘
    │
    ┌────────────┴────────────┐
    │    Execution Status     │
    └────────────┬────────────┘
    │
    ┌─────────────────────┴─────────────────────┐
    │                                           │
    [ SUCCESS ]                                 [ FAILURE ]
    │                                           │
    ▼                                           ▼

    LREM queue:processing                     - Increment job retries

    Set status = COMPLETED                    - Calculate backoff (delay * 2^(retries-1))
    - Retries <= max_retries?
    ├── YES ➔ Sleep & RPUSH queue:pending
    └── NO  ➔ RPUSH queue:dead_letter
```

### Execution Mechanics

1. **Atomic Job Dispatching:** Jobs are registered using dataclasses, serialized to JSON, and persisted into Redis via pipeline transactions (`SET job:<id>` + `RPUSH queue:pending`).
2. **Reliable Queue Pattern:** Workers issue `BRPOPLPUSH queue:pending queue:processing:<worker_id>` commands. This atomically shifts the job reference out of the pending queue and into a worker-specific processing queue, preventing job loss if worker processes are killed mid-task.
3. **Dual Execution Engine:** Workers inspect registered callables via `TaskRegistry`. Async coroutines are directly awaited on the worker's event loop, while synchronous blocking functions are automatically offloaded to thread pools using `asyncio.to_thread` to maintain loop responsiveness.
4. **Fault Recovery & DLQ:** Failed executions trigger exponential backoff calculation ($delay \times 2^{retry\_count - 1}$). If the failure count exceeds `max_retries`, the job is marked as `DEAD` and routed to `queue:dead_letter`.
5. **Heartbeat & Monitoring:** Each worker maintains an expiring heartbeat key (`worker:<id>:heartbeat`). A terminal dashboard pulls queue depths and heartbeat keys every second to provide real-time state visibility.

## Key Features

* **Atomic Processing Pipeline:** Prevents unacknowledged job loss during process termination using reliable queue primitives.
* **Transparent Execution Pipeline:** Executes both synchronous functions and asynchronous coroutines without caller-side modification.
* **Exponential Backoff & Retries:** Configurable retry limits with dynamic delay computation.
* **Dead Letter Queue (DLQ):** Failed jobs beyond maximum retry thresholds are isolated for auditability.
* **Process-Level Isolation:** Spawn isolated OS processes via `multiprocessing.Process` running dedicated event loops.
* **Live Terminal Dashboard:** Real-time state visualization of queue depths and active worker heartbeats.

## Tech Stack & Core Dependencies Breakdown

* **Python 3.10+:** Utilizes `asyncio.to_thread`, modern type hint syntax, and structural patterns.
* **`redis.asyncio` (`redis-py`):** Non-blocking asynchronous I/O driver for Redis interactions and pipelining.
* **`dataclasses` & `enum`:** Low-overhead job serialization and state representation.
* **`multiprocessing`:** Isolated OS process management to bypass GIL bottlenecks across worker nodes.

## Environment & Web-Based Quick Start

### Quick Start via GitHub Codespaces

1. Click **Code** -> **Codespaces** -> **Create codespace on main**.
2. Start a Redis server instance:
   ```bash
   sudo apt-get update && sudo apt-get install -y redis-server
   sudo service redis-server start
   ```

### Virtual Environment Setup

```bash

# Clone repository
git clone [https://github.com/](https://github.com/)<your-username>/redis-task-broker.git
cd redis-task-broker

# Create and activate virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install redis

```

## Execution

Run the demonstration pipeline containing worker processes, enqueued tasks, and the monitoring dashboard:

```bash
    python main.py
```

## Repository Structure

```bash
.
├── .github/
│   └── workflows/
│       └── ci.yml             # Automated syntax validation, linting, and type checking
├── .gitignore                 # Python, virtualenv, and Redis cache exclusions
├── LICENSE                    # MIT Open-Source License
├── README.md                  # System architecture documentation
└── main.py                    # Complete core engine implementation and demonstration
```

## Roadmap

[ ] Graceful Shutdown Handlers: Implement signal handlers (SIGTERM, SIGINT) to re-queue jobs currently in worker queue:processing lists prior to process exit.

[ ] Structured JSON Logging & Telemetry: Replace standard logging with structured JSON output and OpenTelemetry trace propagation across worker process boundaries.

[ ] Persistent Recovery Process: Implement a orphan recovery supervisor process to scan inactive queue:processing:<worker_id> keys based on expired worker heartbeats and push orphaned jobs back into queue:pending.