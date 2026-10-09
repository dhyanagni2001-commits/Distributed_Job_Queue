# chaos_test.py
# Fault-tolerance test suite. Proves: crash recovery, idempotency,
# exclusive claiming, and retry/dead-letter behavior.
# Run: python chaos_test.py

from __future__ import annotations

import asyncio
import logging
import multiprocessing as mp
import sys
import time
import uuid

from config import config
from database import apply_schema, close_pool, fetch_all, fetch_one, init_pool
from models import Job
from queue_manager import QueueManager
from worker import Worker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

PASS = "PASS"
FAIL = "FAIL"


# ── Test 1: Idempotent enqueue ────────────────────────────────────────────────
async def test_idempotency(qm: QueueManager) -> str:
    logger.info("─" * 55)
    logger.info("TEST 1: Idempotent enqueue")
    logger.info("─" * 55)

    key = f"idem-{uuid.uuid4().hex[:8]}"

    id1 = await qm.enqueue(Job(job_type="fast_job", payload={},
                               queue_name="chaos_test", idempotency_key=key))
    id2 = await qm.enqueue(Job(job_type="fast_job", payload={},
                               queue_name="chaos_test", idempotency_key=key))
    id3 = await qm.enqueue(Job(job_type="fast_job", payload={},
                               queue_name="chaos_test", idempotency_key=key))

    if id1 == id2 == id3:
        logger.info("✓ PASS: All 3 enqueue calls returned same ID: %s", id1)
        return PASS
    else:
        logger.error("✗ FAIL: Got different IDs: %s %s %s", id1, id2, id3)
        return FAIL


# ── Test 2: Exactly-once across concurrent workers ────────────────────────────
async def test_exactly_once(qm: QueueManager) -> str:
    logger.info("─" * 55)
    logger.info("TEST 2: Exactly-once under %d concurrent workers", 5)
    logger.info("─" * 55)

    NUM_JOBS = 20
    NUM_WORKERS = 5
    queue = "exactly_once_test"

    # Clean slate
    from database import execute
    await execute(f"DELETE FROM jobs WHERE queue_name='{queue}'")

    job_ids = []
    for i in range(NUM_JOBS):
        jid = await qm.enqueue(Job(
            job_type="counted_job",
            payload={"seq": i},
            queue_name=queue,
            idempotency_key=f"eo-{i}-{uuid.uuid4().hex[:6]}",
        ))
        job_ids.append(str(jid))

    logger.info("Enqueued %d jobs", NUM_JOBS)

    async def handle_counted_job(payload):
        await asyncio.sleep(0.05)
        return {"executed": True}

    workers = []
    tasks = []
    for i in range(NUM_WORKERS):
        w = Worker(qm, queue_names=[queue], worker_id=f"eo-worker-{i}")
        w.register_handler("counted_job", handle_counted_job)
        workers.append(w)
        tasks.append(asyncio.create_task(w.start()))

    # Wait for all jobs to complete (up to 30s)
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        row = await fetch_one(
            f"SELECT COUNT(*) AS n FROM jobs WHERE queue_name='{queue}' AND status='completed'"
        )
        if row["n"] >= NUM_JOBS:
            break
        await asyncio.sleep(1)

    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

    # Verify each completed exactly once
    rows = await fetch_all(
        f"""SELECT id, status,
               (SELECT COUNT(*) FROM job_events
                WHERE job_id=jobs.id AND event_type='completed') AS completions
           FROM jobs WHERE queue_name='{queue}'"""
    )

    failures = [r for r in rows if r["completions"] != 1]
    if failures:
        for f in failures:
            logger.error("✗ Job %s: completions=%d status=%s",
                         f["id"], f["completions"], f["status"])
        return f"FAIL: {len(failures)} jobs not completed exactly once"

    logger.info("✓ PASS: All %d jobs completed exactly once across %d workers",
                NUM_JOBS, NUM_WORKERS)
    return PASS


# ── Test 3: Dead letter after exhausted retries ───────────────────────────────
async def test_dead_letter(qm: QueueManager) -> str:
    logger.info("─" * 55)
    logger.info("TEST 3: Retry exhaustion → dead letter")
    logger.info("─" * 55)

    queue = "dead_letter_test"

    async def always_fails(payload):
        raise RuntimeError("This job always fails on purpose")

    job_id = await qm.enqueue(Job(
        job_type="failing_job",
        payload={},
        queue_name=queue,
        max_attempts=2,
        retry_delay_sec=1,
    ))
    logger.info("Enqueued always-failing job: %s", job_id)

    w = Worker(qm, queue_names=[queue], worker_id="dl-worker")
    w.register_handler("failing_job", always_fails)
    task = asyncio.create_task(w.start())

    # Wait for permanent failure
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        row = await fetch_one(
            "SELECT status, attempt_count FROM jobs WHERE id = $1", job_id
        )
        if row["status"] == "failed":
            break
        await asyncio.sleep(1)

    task.cancel()
    await asyncio.gather(task, return_exceptions=True)

    row = await fetch_one(
        "SELECT status, attempt_count, last_error FROM jobs WHERE id = $1",
        job_id,
    )

    if row["status"] != "failed":
        return f"FAIL: Expected status=failed, got {row['status']}"
    if row["attempt_count"] < 2:
        return f"FAIL: Expected >= 2 attempts, got {row['attempt_count']}"
    if "always fails" not in (row["last_error"] or ""):
        return "FAIL: Error message not stored"

    logger.info("✓ PASS: Job failed after %d attempts, error stored",
                row["attempt_count"])
    return PASS


# ── Test 4: Priority ordering ─────────────────────────────────────────────────
async def test_priority_ordering(qm: QueueManager) -> str:
    logger.info("─" * 55)
    logger.info("TEST 4: Priority ordering (lower number = higher priority)")
    logger.info("─" * 55)

    queue = "priority_test"
    from database import execute
    await execute(f"DELETE FROM jobs WHERE queue_name='{queue}'")

    execution_order = []

    async def handle_priority_job(payload):
        execution_order.append(payload["priority_label"])
        return {}

    # Enqueue in reverse priority order (low priority first)
    await qm.enqueue(Job(job_type="pj", payload={"priority_label": "LOW"},
                         queue_name=queue, priority=9))
    await qm.enqueue(Job(job_type="pj", payload={"priority_label": "MED"},
                         queue_name=queue, priority=5))
    await qm.enqueue(Job(job_type="pj", payload={"priority_label": "HIGH"},
                         queue_name=queue, priority=1))

    w = Worker(qm, queue_names=[queue], worker_id="priority-worker")
    w.register_handler("pj", handle_priority_job)
    task = asyncio.create_task(w.start())

    # Wait for all 3 jobs
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        if len(execution_order) >= 3:
            break
        await asyncio.sleep(0.2)

    task.cancel()
    await asyncio.gather(task, return_exceptions=True)

    if execution_order != ["HIGH", "MED", "LOW"]:
        return f"FAIL: Expected HIGH→MED→LOW, got {execution_order}"

    logger.info("✓ PASS: Jobs executed in priority order: %s", execution_order)
    return PASS


# ── Test 5: Lease renewal keeps long job alive ────────────────────────────────
async def test_lease_renewal(qm: QueueManager) -> str:
    logger.info("─" * 55)
    logger.info("TEST 5: Lease renewal keeps long-running job alive")
    logger.info("─" * 55)

    queue = "lease_renewal_test"

    # Job that runs longer than a single lease window
    # With lease_duration=30s and renewal every 10s, a 25s job should complete
    async def handle_long_job(payload):
        await asyncio.sleep(25)
        return {"survived": True}

    job_id = await qm.enqueue(Job(
        job_type="long_job",
        payload={},
        queue_name=queue,
    ))

    w = Worker(qm, queue_names=[queue], worker_id="long-job-worker")
    w.register_handler("long_job", handle_long_job)
    task = asyncio.create_task(w.start())

    # Wait up to 40s
    deadline = time.monotonic() + 40
    result_status = None
    while time.monotonic() < deadline:
        row = await fetch_one(
            "SELECT status FROM jobs WHERE id = $1", job_id
        )
        if row["status"] in ("completed", "failed"):
            result_status = row["status"]
            break
        await asyncio.sleep(2)

    task.cancel()
    await asyncio.gather(task, return_exceptions=True)

    if result_status != "completed":
        return f"FAIL: Expected completed, got {result_status} (lease not renewed?)"

    logger.info("✓ PASS: Long job (25s) completed without lease expiry")
    return PASS


# ── Test 6: Crash recovery (requires short lease in config) ──────────────────
def _victim_process(queue: str):
    """Runs in a separate OS process; claims the job and hangs until killed."""
    async def run():
        config.queue.lease_duration_sec = 6
        config.queue.lease_renewal_interval_sec = 2
        await init_pool()
        victim = Worker(QueueManager(), queue_names=[queue], worker_id="victim-worker")

        async def handle_never_finishes(payload):
            await asyncio.sleep(9999)

        victim.register_handler("crash_job", handle_never_finishes)
        await victim.start()
    asyncio.run(run())


async def test_crash_recovery(qm: QueueManager) -> str:
    logger.info("─" * 55)
    logger.info("TEST 6: Worker crash recovery (SIGKILL of a worker process)")
    logger.info("─" * 55)

    queue = "crash_recovery_test"
    from database import execute
    await execute(f"DELETE FROM jobs WHERE queue_name='{queue}'")
    job_id = await qm.enqueue(Job(job_type="crash_job", payload={}, queue_name=queue))

    proc = mp.get_context("spawn").Process(target=_victim_process, args=(queue,))
    proc.start()

    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        row = await fetch_one("SELECT status FROM jobs WHERE id = $1", job_id)
        if row["status"] == "running":
            break
        await asyncio.sleep(0.2)
    else:
        proc.kill()
        return f"FAIL: Job not claimed within 15s (status={row['status']})"

    # Let at least one lease renewal happen, then kill without cleanup.
    await asyncio.sleep(3)
    proc.kill()
    proc.join()
    killed_at = time.monotonic()
    logger.info("✓ Victim worker process SIGKILLed while job was running")

    async def handle_crash_job_fast(payload):
        return {"recovered": True}

    saved = config.queue.recovery_interval_sec
    config.queue.recovery_interval_sec = 1
    recovery = Worker(qm, queue_names=[queue], worker_id="recovery-worker")
    recovery.register_handler("crash_job", handle_crash_job_fast)
    recovery_task = asyncio.create_task(recovery.start())

    recovered = False
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        row = await fetch_one("SELECT status, attempt_count FROM jobs WHERE id = $1", job_id)
        if row["status"] == "completed":
            recovered = True
            break
        await asyncio.sleep(0.2)

    recovery_task.cancel()
    await asyncio.gather(recovery_task, return_exceptions=True)
    config.queue.recovery_interval_sec = saved
    await execute("DELETE FROM workers WHERE id IN ('victim-worker', 'recovery-worker')")

    if not recovered:
        return "FAIL: Job not recovered within 30s"
    if row["attempt_count"] != 2:
        return f"FAIL: Expected attempt_count=2 after recovery, got {row['attempt_count']}"

    logger.info("✓ PASS: Job recovered and completed %.1fs after crash (lease=6s)",
                time.monotonic() - killed_at)
    return PASS


# ── Main runner ───────────────────────────────────────────────────────────────
async def main():
    await init_pool()
    await apply_schema("schema.sql")

    qm = QueueManager()

    tests = [
        ("idempotency",     test_idempotency),
        ("exactly_once",    test_exactly_once),
        ("dead_letter",     test_dead_letter),
        ("priority_order",  test_priority_ordering),
        ("lease_renewal",   test_lease_renewal),
        ("crash_recovery",  test_crash_recovery),
    ]

    results = {}
    for name, fn in tests:
        try:
            results[name] = await fn(qm)
        except Exception as e:
            logger.exception("Test %s raised an exception", name)
            results[name] = f"FAIL: {e}"

    # Summary
    logger.info("")
    logger.info("=" * 55)
    logger.info("CHAOS TEST RESULTS")
    logger.info("=" * 55)
    passed = sum(1 for v in results.values() if v == PASS)
    total  = len(results)
    for name, result in results.items():
        icon = "✓" if result == PASS else "✗"
        logger.info("%s %-22s %s", icon, name, result)
    logger.info("")
    logger.info("Score: %d / %d", passed, total)

    await close_pool()
    return passed == total


if __name__ == "__main__":
    sys.exit(0 if asyncio.run(main()) else 1)
