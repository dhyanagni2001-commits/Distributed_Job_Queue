# benchmark.py - Measures enqueue rate, processing throughput, and claim
# latency against a real PostgreSQL instance. Handlers are no-ops, so the
# numbers reflect queue/DB overhead, not handler work.
# Run: python benchmark.py

from __future__ import annotations

import asyncio
import logging
import multiprocessing as mp
import statistics
import time
import uuid

from config import config
from database import apply_schema, close_pool, execute, fetch_one, init_pool
from models import Job
from queue_manager import QueueManager
from worker import Worker

logging.basicConfig(level=logging.WARNING)


def make_jobs(queue: str, n: int) -> list[Job]:
    return [Job(job_type="noop", payload={"i": i}, queue_name=queue,
                idempotency_key=f"{queue}-{uuid.uuid4().hex}") for i in range(n)]


async def clear(queue: str):
    await execute(f"DELETE FROM jobs WHERE queue_name='{queue}'")


async def bench_enqueue(qm: QueueManager) -> tuple[float, float]:
    await clear("bench_enq")
    jobs = make_jobs("bench_enq", 2000)
    t0 = time.perf_counter()
    for j in jobs:
        await qm.enqueue(j)
    single = len(jobs) / (time.perf_counter() - t0)

    jobs = make_jobs("bench_enq", 50_000)
    t0 = time.perf_counter()
    for i in range(0, len(jobs), 1000):
        await qm.enqueue_many(jobs[i:i + 1000])
    bulk = len(jobs) / (time.perf_counter() - t0)
    await clear("bench_enq")
    return single, bulk


async def bench_claim_latency(qm: QueueManager, samples: int = 1000) -> dict:
    await clear("bench_claim")
    await qm.enqueue_many(make_jobs("bench_claim", samples))
    lat = []
    for _ in range(samples):
        t0 = time.perf_counter()
        await qm.claim_jobs("bench-claimer", ["bench_claim"], limit=1)
        lat.append((time.perf_counter() - t0) * 1000)
    lat.sort()
    await clear("bench_claim")
    return {"p50": statistics.median(lat), "p99": lat[int(len(lat) * 0.99) - 1]}


# ── worker processes ─────────────────────────────────────────────────────────
async def _run_workers(queue: str, n_workers: int, batch: int, stop):
    config.queue.batch_size = batch
    config.queue.poll_interval_sec = 0.05
    config.db.max_connections = n_workers + 4
    await init_pool()
    qm = QueueManager()

    async def noop(payload):
        return {}

    workers = []
    for _ in range(n_workers):
        w = Worker(qm, queue_names=[queue])
        w.register_handler("noop", noop)
        workers.append(w)
    tasks = [asyncio.create_task(w.start()) for w in workers]
    while not stop.is_set():
        await asyncio.sleep(0.05)
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    await close_pool()


def _proc_main(queue, n_workers, batch, stop):
    asyncio.run(_run_workers(queue, n_workers, batch, stop))


async def bench_process(qm: QueueManager, procs: int, workers_per_proc: int,
                        batch: int, n_jobs: int) -> dict:
    queue = f"bench_p{procs}_w{workers_per_proc}_b{batch}"
    await clear(queue)
    for i in range(0, n_jobs, 1000):
        await qm.enqueue_many(make_jobs(queue, min(1000, n_jobs - i)))

    ctx = mp.get_context("spawn")
    stop = ctx.Event()
    ps = [ctx.Process(target=_proc_main, args=(queue, workers_per_proc, batch, stop))
          for _ in range(procs)]
    for p in ps:
        p.start()

    # Start the clock at the first claimed job so process spawn time is excluded.
    while (await fetch_one(f"SELECT 1 FROM jobs WHERE queue_name='{queue}' "
                           "AND status <> 'pending' LIMIT 1")) is None:
        await asyncio.sleep(0.01)
    t0 = time.perf_counter()
    while (await fetch_one(f"SELECT COUNT(*) AS n FROM jobs WHERE queue_name='{queue}' "
                           "AND status='completed'"))["n"] < n_jobs:
        await asyncio.sleep(0.02)
    elapsed = time.perf_counter() - t0
    stop.set()
    for p in ps:
        p.join()

    dup = await fetch_one(
        f"""SELECT COUNT(*) AS n FROM (
              SELECT e.job_id FROM job_events e JOIN jobs j ON j.id = e.job_id
              WHERE j.queue_name='{queue}' AND e.event_type='completed'
              GROUP BY e.job_id HAVING COUNT(*) > 1) d""")
    await clear(queue)
    return {"jobs_per_sec": n_jobs / elapsed, "duplicates": dup["n"]}


async def main():
    await init_pool()
    await apply_schema()
    await execute("DELETE FROM workers")
    qm = QueueManager()

    single, bulk = await bench_enqueue(qm)
    print(f"Enqueue: {single:,.0f} jobs/s single-row, {bulk:,.0f} jobs/s bulk (1,000/batch)")

    claim = await bench_claim_latency(qm)
    print(f"Claim latency: p50 {claim['p50']:.2f} ms, p99 {claim['p99']:.2f} ms")

    print("\nThroughput (no-op handlers):")
    print(f"  {'procs':>5} {'workers':>8} {'batch':>6} {'jobs':>7} {'jobs/s':>9} dup")
    configs = [
        (1, 1, 1, 2_000),
        (1, 16, 1, 10_000),
        (1, 16, 10, 20_000),
        (1, 16, 50, 50_000),
        (4, 4, 50, 50_000),
        (1, 16, 50, 100_000),
    ]
    for procs, wpp, batch, n in configs:
        r = await bench_process(qm, procs, wpp, batch, n)
        print(f"  {procs:>5} {procs * wpp:>8} {batch:>6} {n:>7,} "
              f"{r['jobs_per_sec']:>9,.0f} {r['duplicates']}")

    await close_pool()


if __name__ == "__main__":
    asyncio.run(main())
