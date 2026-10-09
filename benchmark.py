# benchmark.py - Measures enqueue rate, processing throughput, and claim
# overhead against a real PostgreSQL instance. Handlers are no-ops, so the
# numbers reflect queue/DB overhead, not handler work.
# Run: python benchmark.py

from __future__ import annotations

import asyncio
import logging
import statistics
import time
import uuid

from config import config
from database import apply_schema, close_pool, execute, fetch_all, fetch_one, init_pool
from models import Job
from queue_manager import QueueManager
from worker import Worker

logging.basicConfig(level=logging.WARNING)
config.db.max_connections = 40

NUM_JOBS = 2000
WORKER_COUNTS = [1, 2, 4, 8, 16]


async def clear(queue: str):
    await execute(f"DELETE FROM job_events WHERE job_id IN (SELECT id FROM jobs WHERE queue_name='{queue}')")
    await execute(f"DELETE FROM jobs WHERE queue_name='{queue}'")


async def bench_enqueue(qm: QueueManager) -> float:
    queue = "bench_enqueue"
    await clear(queue)
    start = time.perf_counter()
    for i in range(NUM_JOBS):
        await qm.enqueue(Job(job_type="noop", payload={"i": i}, queue_name=queue,
                             idempotency_key=f"bq-{uuid.uuid4().hex}"))
    return NUM_JOBS / (time.perf_counter() - start)


async def bench_process(qm: QueueManager, n_workers: int) -> dict:
    queue = f"bench_w{n_workers}"
    await clear(queue)
    for i in range(NUM_JOBS):
        await qm.enqueue(Job(job_type="noop", payload={"i": i}, queue_name=queue,
                             idempotency_key=f"bp-{uuid.uuid4().hex}"))

    async def noop(payload):
        return {}

    workers = [Worker(qm, queue_names=[queue], worker_id=f"bench-{n_workers}-{i}")
               for i in range(n_workers)]
    for w in workers:
        w.register_handler("noop", noop)

    start = time.perf_counter()
    tasks = [asyncio.create_task(w.start()) for w in workers]
    while True:
        row = await fetch_one(
            f"SELECT COUNT(*) AS n FROM jobs WHERE queue_name='{queue}' AND status='completed'")
        if row["n"] >= NUM_JOBS:
            break
        await asyncio.sleep(0.05)
    elapsed = time.perf_counter() - start
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

    dup = await fetch_one(
        f"""SELECT COUNT(*) AS n FROM (
              SELECT job_id FROM job_events e JOIN jobs j ON j.id = e.job_id
              WHERE j.queue_name='{queue}' AND e.event_type='completed'
              GROUP BY job_id HAVING COUNT(*) > 1) d""")
    return {"workers": n_workers, "jobs_per_sec": NUM_JOBS / elapsed, "duplicates": dup["n"]}


async def bench_claim_latency(qm: QueueManager, samples: int = 1000) -> dict:
    queue = "bench_claim"
    await clear(queue)
    for i in range(samples):
        await qm.enqueue(Job(job_type="noop", payload={}, queue_name=queue,
                             idempotency_key=f"bc-{uuid.uuid4().hex}"))
    lat = []
    for _ in range(samples):
        t0 = time.perf_counter()
        await qm.claim_job("bench-claimer", [queue])
        lat.append((time.perf_counter() - t0) * 1000)
    lat.sort()
    return {"p50": statistics.median(lat), "p99": lat[int(len(lat) * 0.99) - 1]}


async def main():
    await init_pool()
    await apply_schema()
    qm = QueueManager()

    enq = await bench_enqueue(qm)
    print(f"Enqueue (single producer, sequential): {enq:,.0f} jobs/s")

    claim = await bench_claim_latency(qm)
    print(f"Claim latency: p50 {claim['p50']:.2f} ms, p99 {claim['p99']:.2f} ms")

    print(f"\nProcessing {NUM_JOBS} no-op jobs:")
    for n in WORKER_COUNTS:
        r = await bench_process(qm, n)
        print(f"  {r['workers']:>2} workers: {r['jobs_per_sec']:>7,.0f} jobs/s  duplicates={r['duplicates']}")

    await close_pool()


if __name__ == "__main__":
    asyncio.run(main())
