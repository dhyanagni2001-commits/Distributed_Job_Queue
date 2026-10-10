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


# ── Test 7: Fencing (a stalled worker cannot finish a reclaimed job) ─────────
async def test_zombie_fencing(qm: QueueManager) -> str:
    logger.info("─" * 55)
    logger.info("TEST 7: Fencing — stale worker cannot complete/fail a reclaimed job")
    logger.info("─" * 55)

    from database import execute
    from models import JobResult
    queue = "fencing_test"
    await execute("DELETE FROM jobs WHERE queue_name = $1", queue)
    job_id = await qm.enqueue(Job(job_type="fence_job", payload={}, queue_name=queue))

    # Worker A claims with a 1s lease, then "stalls" past it.
    [a_job] = await qm.claim_jobs("zombie-a", [queue], lease_sec=1, limit=1)
    await asyncio.sleep(1.5)
    await qm.recover_expired_leases()
    [b_job] = await qm.claim_jobs("fresh-b", [queue], lease_sec=30, limit=1)
    if b_job.job_id != job_id:
        return "FAIL: Worker B did not reclaim the job"

    # Worker A wakes up and tries to renew, complete and fail. All must be rejected.
    renewed = await qm.renew_leases([job_id], "zombie-a")
    a_completed = await qm.complete_jobs("zombie-a", [JobResult(job_id, True, {"by": "a"})])
    a_failed = await qm.fail_job(JobResult(job_id, False, error="late"), 3, "zombie-a")
    if renewed or a_completed or a_failed:
        return (f"FAIL: Stale worker was not fenced (renewed={bool(renewed)}, "
                f"completed={a_completed}, failed={a_failed})")

    b_completed = await qm.complete_jobs("fresh-b", [JobResult(job_id, True, {"by": "b"})])
    row = await fetch_one(
        """SELECT status, result,
                  (SELECT COUNT(*) FROM job_events
                   WHERE job_id = $1 AND event_type = 'completed') AS completions
           FROM jobs WHERE id = $1""", job_id)
    if b_completed != 1 or row["status"] != "completed" or row["completions"] != 1:
        return f"FAIL: Owner completion wrong (status={row['status']}, completions={row['completions']})"
    if row["result"] != {"by": "b"}:
        return f"FAIL: Result written by stale worker: {row['result']}"

    logger.info("✓ PASS: Stale worker's renew/complete/fail rejected; owner completed once")
    return PASS


# ── Test 8: Poison pill is dead-lettered by lease recovery ───────────────────
async def test_poison_pill_recovery(qm: QueueManager) -> str:
    logger.info("─" * 55)
    logger.info("TEST 8: Job that keeps losing its worker is dead-lettered")
    logger.info("─" * 55)

    from database import execute
    queue = "poison_pill_test"
    await execute("DELETE FROM jobs WHERE queue_name = $1", queue)
    job_id = await qm.enqueue(Job(job_type="poison", payload={}, queue_name=queue,
                                  max_attempts=2))

    # Two attempts, each "crashing" (lease left to expire without completing).
    for attempt in (1, 2):
        claimed = await qm.claim_jobs(f"doomed-{attempt}", [queue], lease_sec=1, limit=1)
        if not claimed:
            return f"FAIL: Job not claimable on attempt {attempt}"
        await asyncio.sleep(1.5)
        await qm.recover_expired_leases()

    row = await fetch_one("SELECT status, attempt_count, last_error FROM jobs WHERE id = $1",
                          job_id)
    if row["status"] != "dead_letter":
        return f"FAIL: Expected dead_letter after 2 lost leases, got {row['status']}"
    events = await fetch_all(
        "SELECT event_type FROM job_events WHERE job_id = $1 "
        "AND event_type IN ('lease_expired', 'dead_lettered') ORDER BY id", job_id)
    if [e["event_type"] for e in events] != ["lease_expired", "dead_lettered"]:
        return f"FAIL: Recovery events not logged: {[e['event_type'] for e in events]}"

    logger.info("✓ PASS: Dead-lettered after %d lost leases, recovery events logged",
                row["attempt_count"])
    return PASS


# ── Test 9: Batch renewal notices a lost lease and cancels the handler ───────
async def test_lost_lease_cancels_handler(qm: QueueManager) -> str:
    logger.info("─" * 55)
    logger.info("TEST 9: Lost lease in a batch cancels that job's handler")
    logger.info("─" * 55)

    from database import execute
    queue = "lost_lease_test"
    await execute("DELETE FROM jobs WHERE queue_name = $1", queue)
    stolen_id = await qm.enqueue(Job(job_type="slow", payload={"stolen": True}, queue_name=queue))
    kept_id = await qm.enqueue(Job(job_type="slow", payload={"stolen": False}, queue_name=queue))

    cancelled = []

    async def handle_slow(payload):
        try:
            await asyncio.sleep(4)
        except asyncio.CancelledError:
            cancelled.append(payload["stolen"])
            raise
        return {"done": True}

    saved = config.queue.lease_renewal_interval_sec
    config.queue.lease_renewal_interval_sec = 1
    w = Worker(qm, queue_names=[queue], worker_id="lease-loser")
    w.register_handler("slow", handle_slow)
    task = asyncio.create_task(w.start())
    try:
        # Wait until both jobs are running, then simulate another worker taking one.
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            row = await fetch_one("SELECT COUNT(*) AS n FROM jobs WHERE queue_name = $1 "
                                  "AND status = 'running'", queue)
            if row["n"] == 2:
                break
            await asyncio.sleep(0.1)
        await execute("UPDATE jobs SET worker_id = 'thief' WHERE id = $1", stolen_id)

        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            row = await fetch_one("SELECT status FROM jobs WHERE id = $1", kept_id)
            if row["status"] == "completed":
                break
            await asyncio.sleep(0.2)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        config.queue.lease_renewal_interval_sec = saved
        await execute("DELETE FROM workers WHERE id = 'lease-loser'")

    stolen = await fetch_one("SELECT status, worker_id FROM jobs WHERE id = $1", stolen_id)
    kept = await fetch_one("SELECT status FROM jobs WHERE id = $1", kept_id)
    if cancelled != [True]:
        return f"FAIL: Expected only the stolen job's handler cancelled, got {cancelled}"
    if stolen["status"] != "running" or stolen["worker_id"] != "thief":
        return f"FAIL: Stolen job was modified by the loser: {dict(stolen)}"
    if kept["status"] != "completed":
        return f"FAIL: Job that kept its lease did not complete: {kept['status']}"

    logger.info("✓ PASS: Lost-lease handler cancelled; the other job completed")
    return PASS


# ── Test 10: Finished job keeps its lease while the batch is still running ───
async def test_batch_holds_finished_leases(qm: QueueManager) -> str:
    logger.info("─" * 55)
    logger.info("TEST 10: Fast job in a slow batch is not reclaimed before completion")
    logger.info("─" * 55)

    from database import execute
    queue = "batch_lease_test"
    await execute("DELETE FROM jobs WHERE queue_name = $1", queue)
    fast_id = await qm.enqueue(Job(job_type="timed", payload={"sec": 0}, queue_name=queue))
    await qm.enqueue(Job(job_type="timed", payload={"sec": 5}, queue_name=queue))

    async def handle_timed(payload):
        await asyncio.sleep(payload["sec"])
        return {}

    saved = (config.queue.lease_duration_sec, config.queue.lease_renewal_interval_sec)
    config.queue.lease_duration_sec, config.queue.lease_renewal_interval_sec = 2, 0.5
    w = Worker(qm, queue_names=[queue], worker_id="batch-holder")
    w.register_handler("timed", handle_timed)
    task = asyncio.create_task(w.start())
    try:
        # The slow job outlives the 2s lease several times over; keep running
        # recovery the way other workers would.
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            await qm.recover_expired_leases()
            row = await fetch_one("SELECT COUNT(*) AS n FROM jobs WHERE queue_name = $1 "
                                  "AND status = 'completed'", queue)
            if row["n"] == 2:
                break
            await asyncio.sleep(0.3)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        config.queue.lease_duration_sec, config.queue.lease_renewal_interval_sec = saved
        await execute("DELETE FROM workers WHERE id = 'batch-holder'")

    row = await fetch_one(
        """SELECT status, attempt_count,
                  (SELECT COUNT(*) FROM job_events WHERE job_id = $1
                   AND event_type = 'lease_expired') AS expiries
           FROM jobs WHERE id = $1""", fast_id)
    if row["status"] != "completed" or row["attempt_count"] != 1 or row["expiries"]:
        return (f"FAIL: Fast job lost its lease while waiting for the batch "
                f"(status={row['status']}, attempts={row['attempt_count']}, expiries={row['expiries']})")

    logger.info("✓ PASS: Fast job kept its lease and completed on the first attempt")
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
        ("zombie_fencing",  test_zombie_fencing),
        ("poison_pill",     test_poison_pill_recovery),
        ("lost_lease",      test_lost_lease_cancels_handler),
        ("batch_lease",     test_batch_holds_finished_leases),
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
