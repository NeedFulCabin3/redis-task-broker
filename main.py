import asyncio
import json
import logging
import os
import time
import uuid
from dataclasses import asdict, dataclass, field
from enum import Enum
from multiprocessing import Process
from typing import Any, Callable, Dict, Optional

import redis.asyncio as aioredis

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(processName)s] %(message)s"
)


class JobStatus(str, Enum):
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    DEAD = "DEAD"


@dataclass
class Job:
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    func_name: str = ""
    args: list = field(default_factory=list)
    kwargs: dict = field(default_factory=dict)
    status: JobStatus = JobStatus.PENDING
    retries: int = 0
    max_retries: int = 3
    retry_delay: float = 1.0  # Base delay in seconds
    error: Optional[str] = None
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)

    def to_json(self) -> str:
        data = asdict(self)
        data["status"] = self.status.value
        return json.dumps(data)

    @classmethod
    def from_json(cls, raw: str) -> "Job":
        data = json.loads(raw)
        data["status"] = JobStatus(data["status"])
        return cls(**data)


class TaskRegistry:
    """Registry mapping task names to callable Python functions."""
    _tasks: Dict[str, Callable] = {}

    @classmethod
    def register(cls, name: Optional[str] = None):
        def decorator(func: Callable):
            task_name = name or func.__name__
            cls._tasks[task_name] = func
            return func
        return decorator

    @classmethod
    def get(cls, name: str) -> Callable:
        if name not in cls._tasks:
            raise KeyError(f"Task '{name}' is not registered.")
        return cls._tasks[name]


class TaskQueue:
    """Redis-backed queue interface for pushing and tracking jobs."""
    def __init__(self, redis_url: str = "redis://localhost:6379"):
        self.redis_url = redis_url

    async def enqueue(self, func: Callable, *args, max_retries: int = 3, retry_delay: float = 1.0, **kwargs) -> Job:
        job = Job(
            func_name=func.__name__,
            args=list(args),
            kwargs=kwargs,
            max_retries=max_retries,
            retry_delay=retry_delay
        )
        client = aioredis.from_url(self.redis_url)
        try:
            # Store metadata and add to pending list atomically via Redis transaction
            async with client.pipeline(transaction=True) as pipe:
                pipe.set(f"job:{job.id}", job.to_json())
                pipe.rpush("queue:pending", job.id)
                await pipe.execute()
            logging.info(f"Enqueued job {job.id} ({job.func_name})")
            return job
        finally:
            await client.aclose()


class Worker:
    """Async Worker process that pulls jobs, executes them, and handles retries."""
    def __init__(self, worker_id: str, redis_url: str = "redis://localhost:6379"):
        self.worker_id = worker_id
        self.redis_url = redis_url
        self.running = False

    async def start(self):
        self.running = True
        client = aioredis.from_url(self.redis_url)
        logging.info(f"Worker {self.worker_id} initialized.")

        try:
            while self.running:
                # Update heartbeat every cycle
                await client.set(f"worker:{self.worker_id}:heartbeat", time.time(), ex=10)

                # Reliable Queue Pattern: RPOPLPUSH moves item to processing queue atomically
                job_id = await client.brpoplpush("queue:pending", f"queue:processing:{self.worker_id}", timeout=2)
                if not job_id:
                    continue

                job_id = job_id.decode("utf-8") if isinstance(job_id, bytes) else job_id
                await self._process_job(client, job_id)

        except asyncio.CancelledError:
            logging.info(f"Worker {self.worker_id} shutting down gracefully...")
        finally:
            await client.delete(f"worker:{self.worker_id}:heartbeat")
            await client.aclose()

    async def _process_job(self, client: aioredis.Redis, job_id: str):
        raw_job = await client.get(f"job:{job_id}")
        if not raw_job:
            await client.lrem(f"queue:processing:{self.worker_id}", 1, job_id)
            return

        job = Job.from_json(raw_job.decode("utf-8") if isinstance(raw_job, bytes) else raw_job)
        job.status = JobStatus.PROCESSING
        job.updated_at = time.time()
        await client.set(f"job:{job.id}", job.to_json())

        try:
            func = TaskRegistry.get(job.func_name)
            # Support both sync and async tasks transparently
            if asyncio.iscoroutinefunction(func):
                await func(*job.args, **job.kwargs)
            else:
                await asyncio.to_thread(func, *job.args, **job.kwargs)

            job.status = JobStatus.COMPLETED
            job.updated_at = time.time()
            await client.set(f"job:{job.id}", job.to_json())
            await client.lrem(f"queue:processing:{self.worker_id}", 1, job.id)
            logging.info(f"Successfully finished job {job.id}")

        except Exception as exc:
            logging.error(f"Execution failed for job {job.id}: {exc}")
            job.retries += 1
            job.error = str(exc)
            job.updated_at = time.time()

            await client.lrem(f"queue:processing:{self.worker_id}", 1, job.id)

            if job.retries <= job.max_retries:
                job.status = JobStatus.PENDING
                await client.set(f"job:{job.id}", job.to_json())
                
                # Exponential backoff: base_delay * (2 ^ (retry_count - 1))
                backoff = job.retry_delay * (2 ** (job.retries - 1))
                logging.info(f"Re-queueing job {job.id} in {backoff:.2f}s (Attempt {job.retries}/{job.max_retries})")
                
                await asyncio.sleep(backoff)
                await client.rpush("queue:pending", job.id)
            else:
                job.status = JobStatus.DEAD
                await client.set(f"job:{job.id}", job.to_json())
                await client.rpush("queue:dead_letter", job.id)
                logging.error(f"Job {job.id} exceeded retries. Moved to Dead Letter Queue (DLQ).")


def run_worker_process(worker_id: str, redis_url: str):
    """Entry point for isolated worker OS processes."""
    worker = Worker(worker_id=worker_id, redis_url=redis_url)
    try:
        asyncio.run(worker.start())
    except KeyboardInterrupt:
        pass


class MonitorDashboard:
    """Real-time terminal dashboard for tracking system state."""
    def __init__(self, redis_url: str = "redis://localhost:6379"):
        self.redis_url = redis_url

    async def start(self):
        client = aioredis.from_url(self.redis_url)
        try:
            while True:
                pending = await client.llen("queue:pending")
                dlq = await client.llen("queue:dead_letter")
                worker_keys = await client.keys("worker:*:heartbeat")
                
                # Render state dashboard
                os.system("cls" if os.name == "nt" else "clear")
                print("==================================================")
                print("        DISTRIBUTED TASK QUEUE MONITOR            ")
                print("==================================================")
                print(f" Active Workers : {len(worker_keys)}")
                print(f" Pending Jobs   : {pending}")
                print(f" Dead Letter Q  : {dlq}")
                print("--------------------------------------------------")
                
                # Print Active Worker IDs
                for key in worker_keys:
                    w_id = key.decode("utf-8").split(":")[1]
                    print(f"  • Worker ID: {w_id} [ALIVE]")
                print("==================================================")
                print("Press Ctrl+C to stop dashboard monitoring.\n")
                
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            pass
        finally:
            await client.aclose()


# -------------------------------------------------------------------
# Example Setup: Registering Tasks and Running the Queue Pipeline
# -------------------------------------------------------------------

@TaskRegistry.register("send_email")
def send_email_task(recipient: str, body: str):
    time.sleep(1)  # Simulate I/O work
    print(f"\n[TASK] Email sent to {recipient} with message: '{body}'")

@TaskRegistry.register("flaky_api_call")
def flaky_api_call_task():
    print("\n[TASK] Executing API call...")
    raise ConnectionResetError("API target unreachable!")


async def main():
    redis_url = "redis://localhost:6379"
    
    # 1. Start worker pool using OS processes
    num_workers = 2
    processes = []
    for i in range(num_workers):
        p = Process(target=run_worker_process, args=(f"worker-{i+1}", redis_url), daemon=True)
        p.start()
        processes.append(p)

    # 2. Dispatch jobs to queue
    tq = TaskQueue(redis_url=redis_url)
    await tq.enqueue(send_email_task, recipient="dev@example.com", body="Hello World!")
    await tq.enqueue(flaky_api_call_task, max_retries=2, retry_delay=0.5)

    # 3. Launch live dashboard monitor in main process
    dashboard = MonitorDashboard(redis_url=redis_url)
    dashboard_task = asyncio.create_task(dashboard.start())

    # Run demonstration for 10 seconds before terminating
    await asyncio.sleep(10)
    
    dashboard_task.cancel()
    for p in processes:
        p.terminate()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass