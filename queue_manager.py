# queue_manager.py - Enqueue, claim, complete, fail, and maintenance operations

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import asyncpg

from database import acquire
from models import (
    ClaimedJob, EventType, Job, JobResult, JobStatus, QueueStats,
)

logger = logging.getLogger(__name__)


class QueueManager:
    """All SQL-backed queue operations."""

    # ------------------------------------------------------------------
    # Enqueue
    # ------------------------------------------------------------------
    async def enqueue(self, job: Job) -> uuid.UUID:
        async with acquire() as conn:
            try:
                row = await conn.fetchrow(
                    """
                    INSERT INTO jobs (
                        queue_name, job_type, payload, priority,
                        max_attempts, retry_delay_sec,
                        idempotency_key, tags, metadata, scheduled_at
                    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,
                              COALESCE($10, NOW()))
                    ON CONFLICT (idempotency_key)
                        DO UPDATE SET updated_at = NOW()
                    RETURNING id, (xmax = 0) AS inserted
                    """,
                    job.queue_name,
                    job.job_type,
                    job.payload,
                    job.priority,
                    job.max_attempts,
                    job.retry_delay_sec,
                    job.idempotency_key,
                    job.tags,
                    job.metadata,
                    job.scheduled_at,
                )
            except asyncpg.UniqueViolationError:
                row = await conn.fetchrow(
                    "SELECT id FROM jobs WHERE idempotency_key = $1",
                    job.idempotency_key,
                )
                logger.debug("Idempotent enqueue, returning existing job %s", row["id"])
                return row["id"]

            job_id = row["id"]

            if row["inserted"]:
                await self._log_event(
                    conn, job_id, None, EventType.ENQUEUED,
                    f"Job enqueued to queue '{job.queue_name}'"
                )
                logger.debug("Enqueued job %s type=%s queue=%s",
                             job_id, job.job_type, job.queue_name)
            else:
                logger.debug("Idempotent re-enqueue for key=%s, job=%s",
                             job.idempotency_key, job_id)

            return job_id

    async def enqueue_batch(self, jobs: List[Job]) -> List[uuid.UUID]:
        ids = []
        for job in jobs:
            ids.append(await self.enqueue(job))
        return ids

    async def enqueue_many(self, jobs: List[Job]) -> int:
        """Bulk insert in one round trip. Duplicate idempotency keys are skipped.
        Returns the number of newly inserted jobs."""
        if not jobs:
            return 0
        async with acquire() as conn:
            row = await conn.fetchrow(
                """
                WITH ins AS (
                    INSERT INTO jobs (
                        queue_name, job_type, payload, priority,
                        max_attempts, retry_delay_sec, idempotency_key
                    )
                    SELECT * FROM unnest(
                        $1::text[], $2::text[], $3::jsonb[], $4::int[],
                        $5::int[], $6::int[], $7::text[]
                    )
                    ON CONFLICT (idempotency_key) DO NOTHING
                    RETURNING id, queue_name
                ), ev AS (
                    INSERT INTO job_events (job_id, event_type, message)
                    SELECT id, 'enqueued', 'Job enqueued to queue ''' || queue_name || ''''
                    FROM ins
                )
                SELECT COUNT(*) AS n FROM ins
                """,
                [j.queue_name for j in jobs],
                [j.job_type for j in jobs],
                [j.payload for j in jobs],
                [j.priority for j in jobs],
                [j.max_attempts for j in jobs],
                [j.retry_delay_sec for j in jobs],
                [j.idempotency_key for j in jobs],
            )
            return row["n"]

    # ------------------------------------------------------------------
    # Claim
    # ------------------------------------------------------------------
    async def claim_job(
        self,
        worker_id: str,
        queue_names: List[str],
        lease_sec: int = 30,
    ) -> Optional[ClaimedJob]:
        async with acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM claim_next_job($1, $2, $3)",
                worker_id,
                queue_names,
                lease_sec,
            )

            if row is None:
                return None

            await self._log_event(
                conn, row["out_job_id"], worker_id, EventType.CLAIMED,
                f"Claimed by worker {worker_id} (attempt {row['out_attempt_count']})"
            )

            return ClaimedJob(
                job_id=row["out_job_id"],
                job_type=row["out_job_type"],
                payload=row["out_payload"],
                attempt_count=row["out_attempt_count"],
                max_attempts=row["out_max_attempts"],
                worker_id=worker_id,
            )

    async def claim_jobs(
        self,
        worker_id: str,
        queue_names: List[str],
        lease_sec: int = 30,
        limit: int = 10,
    ) -> List[ClaimedJob]:
        """Claim up to `limit` jobs and mark them running in one round trip."""
        async with acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM claim_jobs($1, $2, $3, $4)",
                worker_id, queue_names, lease_sec, limit,
            )
        return [
            ClaimedJob(
                job_id=r["out_job_id"],
                job_type=r["out_job_type"],
                payload=r["out_payload"],
                attempt_count=r["out_attempt_count"],
                max_attempts=r["out_max_attempts"],
                worker_id=worker_id,
            )
            for r in rows
        ]

    async def complete_jobs(self, worker_id: str, results: List[JobResult]) -> int:
        """Mark a batch of jobs completed in one round trip."""
        if not results:
            return 0
        async with acquire() as conn:
            return await conn.fetchval(
                "SELECT complete_jobs($1, $2, $3, $4)",
                worker_id,
                [r.job_id for r in results],
                [r.result or {} for r in results],
                [r.duration_sec for r in results],
            )

    async def renew_leases(self, job_ids: List[uuid.UUID], worker_id: str,
                           lease_sec: int = 30) -> set[uuid.UUID]:
        """Extend leases on jobs this worker still owns. Returns the IDs that
        were renewed; any ID missing from the result has been lost."""
        async with acquire() as conn:
            rows = await conn.fetch(
                """
                UPDATE jobs SET lease_expires_at = NOW() + make_interval(secs => $3),
                                updated_at = NOW()
                WHERE id = ANY($1) AND worker_id = $2 AND status = 'running'
                RETURNING id
                """,
                job_ids, worker_id, lease_sec,
            )
            return {r["id"] for r in rows}

    # ------------------------------------------------------------------
    # Mark running
    # ------------------------------------------------------------------
    async def mark_running(self, job_id: uuid.UUID, worker_id: str):
        async with acquire() as conn:
            await conn.execute(
                """
                UPDATE jobs SET status = 'running', started_at = NOW(), updated_at = NOW()
                WHERE id = $1 AND worker_id = $2 AND status = 'claimed'
                """,
                job_id, worker_id,
            )
            await self._log_event(conn, job_id, worker_id, EventType.STARTED, "Execution started")

    # ------------------------------------------------------------------
    # Complete
    # ------------------------------------------------------------------
    async def complete_job(self, result: JobResult, worker_id: str) -> bool:
        """Returns False if the worker no longer owns the job (lease lost)."""
        async with acquire() as conn:
            res = await conn.execute(
                """
                UPDATE jobs SET
                    status       = 'completed',
                    result       = $1,
                    completed_at = NOW(),
                    updated_at   = NOW(),
                    worker_id    = NULL
                WHERE id = $2 AND worker_id = $3 AND status IN ('claimed', 'running')
                """,
                result.result or {},
                result.job_id,
                worker_id,
            )
            if res.split()[-1] == "0":
                logger.warning("Job %s not completed: lease lost by %s",
                               result.job_id, worker_id)
                return False
            await self._log_event(
                conn, result.job_id, None, EventType.COMPLETED,
                f"Completed in {result.duration_sec:.3f}s",
                {"duration_sec": result.duration_sec},
            )
            return True

    # ------------------------------------------------------------------
    # Fail / retry
    # ------------------------------------------------------------------
    async def fail_job(self, result: JobResult, max_attempts: int, worker_id: str) -> bool:
        """Retry or permanently fail a job in one statement. Only applies if
        `worker_id` still owns the job; returns False if the lease was lost."""
        async with acquire() as conn:
            row = await conn.fetchrow(
                """
                WITH upd AS (
                    UPDATE jobs SET
                        status           = CASE WHEN attempt_count < $2
                                                THEN 'pending'::job_status
                                                ELSE 'failed'::job_status END,
                        scheduled_at     = CASE WHEN attempt_count < $2
                                                THEN NOW() + make_interval(secs => retry_delay_sec)
                                                ELSE scheduled_at END,
                        worker_id        = NULL,
                        lease_expires_at = NULL,
                        last_error       = $3,
                        last_error_at    = NOW(),
                        updated_at       = NOW()
                    WHERE id = $1 AND worker_id = $4 AND status IN ('claimed', 'running')
                    RETURNING id, status, attempt_count, retry_delay_sec
                ), ev AS (
                    INSERT INTO job_events (job_id, worker_id, event_type, message)
                    SELECT id, $4,
                           CASE WHEN status = 'pending' THEN 'retried' ELSE 'failed' END,
                           CASE WHEN status = 'pending'
                                THEN 'Attempt ' || attempt_count || '/' || $2 || ' failed; retry in '
                                     || retry_delay_sec || 's. Error: ' || $3
                                ELSE 'Permanently failed after ' || attempt_count
                                     || ' attempts. Error: ' || $3 END
                    FROM upd
                )
                SELECT status::text AS status, attempt_count, retry_delay_sec FROM upd
                """,
                result.job_id, max_attempts, result.error, worker_id,
            )
            if row is None:
                logger.warning("Failure for job %s not recorded: lease lost by %s",
                               result.job_id, worker_id)
                return False

            if row["status"] == "pending":
                logger.warning("Job %s attempt %d/%d failed, retrying in %ds",
                               result.job_id, row["attempt_count"], max_attempts,
                               row["retry_delay_sec"])
            else:
                logger.error("Job %s permanently failed after %d attempts",
                             result.job_id, row["attempt_count"])
            return True

    # ------------------------------------------------------------------
    # Lease renewal
    # ------------------------------------------------------------------
    async def renew_lease(
        self, job_id: uuid.UUID, worker_id: str, lease_sec: int = 30
    ) -> bool:
        async with acquire() as conn:
            row = await conn.fetchrow(
                "SELECT renew_job_lease($1, $2, $3) AS ok",
                job_id, worker_id, lease_sec,
            )
            renewed = row["ok"] if row else False
            if renewed:
                await self._log_event(
                    conn, job_id, worker_id, EventType.LEASE_RENEWED,
                    f"Lease renewed for {lease_sec}s",
                )
            return renewed

    # ------------------------------------------------------------------
    # Maintenance
    # ------------------------------------------------------------------
    async def recover_expired_leases(self) -> int:
        async with acquire() as conn:
            row = await conn.fetchrow("SELECT recover_expired_leases() AS n")
            count = row["n"] if row else 0
            if count:
                logger.info("Recovered %d expired leases", count)
            return count

    async def process_dead_letters(self) -> int:
        async with acquire() as conn:
            row = await conn.fetchrow("SELECT process_dead_letters() AS n")
            count = row["n"] if row else 0
            if count:
                logger.info("Moved %d jobs to dead_letter", count)
            return count

    # ------------------------------------------------------------------
    # Worker registry
    # ------------------------------------------------------------------
    async def register_worker(
        self,
        worker_id: str,
        queue_names: List[str],
        hostname: str,
        pid: int,
    ):
        async with acquire() as conn:
            await conn.execute(
                """
                INSERT INTO workers (id, queue_names, hostname, pid)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (id) DO UPDATE SET
                    last_heartbeat = NOW(),
                    status         = 'idle'
                """,
                worker_id, queue_names, hostname, pid,
            )

    async def heartbeat(self, worker_id: str, status: str, current_job_id=None):
        async with acquire() as conn:
            await conn.execute(
                """
                UPDATE workers SET
                    last_heartbeat = NOW(),
                    status         = $1,
                    current_job_id = $2
                WHERE id = $3
                """,
                status, current_job_id, worker_id,
            )

    async def deregister_worker(self, worker_id: str):
        async with acquire() as conn:
            await conn.execute("DELETE FROM workers WHERE id = $1", worker_id)

    async def increment_worker_stats(self, worker_id: str, success: bool):
        async with acquire() as conn:
            if success:
                await conn.execute(
                    "UPDATE workers SET jobs_processed = jobs_processed + 1 WHERE id = $1",
                    worker_id,
                )
            else:
                await conn.execute(
                    "UPDATE workers SET jobs_failed = jobs_failed + 1 WHERE id = $1",
                    worker_id,
                )

    # ------------------------------------------------------------------
    # Stats
    # ------------------------------------------------------------------
    async def get_queue_stats(self, queue_name: str = "default") -> QueueStats:
        async with acquire() as conn:
            counts = await conn.fetchrow(
                """
                SELECT
                    COUNT(*) FILTER (WHERE status='pending')     AS pending,
                    COUNT(*) FILTER (WHERE status='claimed')     AS claimed,
                    COUNT(*) FILTER (WHERE status='running')     AS running,
                    COUNT(*) FILTER (WHERE status='completed')   AS completed,
                    COUNT(*) FILTER (WHERE status='failed')      AS failed,
                    COUNT(*) FILTER (WHERE status='dead_letter') AS dead_letter
                FROM jobs WHERE queue_name = $1
                """,
                queue_name,
            )
            workers = await conn.fetchrow(
                """
                SELECT COUNT(*) AS n FROM workers
                WHERE $1 = ANY(queue_names)
                  AND last_heartbeat > NOW() - INTERVAL '30 seconds'
                """,
                queue_name,
            )
            throughput = await conn.fetchrow(
                """
                SELECT COUNT(*) AS n FROM jobs
                WHERE queue_name = $1
                  AND status = 'completed'
                  AND completed_at > NOW() - INTERVAL '1 minute'
                """,
                queue_name,
            )
            timing = await conn.fetchrow(
                """
                SELECT
                    AVG(EXTRACT(EPOCH FROM (claimed_at - created_at)))           AS avg_wait,
                    AVG(EXTRACT(EPOCH FROM (completed_at - started_at)))         AS avg_exec,
                    PERCENTILE_CONT(0.95) WITHIN GROUP (
                        ORDER BY EXTRACT(EPOCH FROM (completed_at - started_at))
                    )                                                            AS p95_exec,
                    PERCENTILE_CONT(0.99) WITHIN GROUP (
                        ORDER BY EXTRACT(EPOCH FROM (completed_at - started_at))
                    )                                                            AS p99_exec
                FROM jobs
                WHERE queue_name = $1
                  AND status = 'completed'
                  AND completed_at > NOW() - INTERVAL '5 minutes'
                """,
                queue_name,
            )

            return QueueStats(
                queue_name=queue_name,
                pending=counts["pending"],
                claimed=counts["claimed"],
                running=counts["running"],
                completed=counts["completed"],
                failed=counts["failed"],
                dead_letter=counts["dead_letter"],
                active_workers=workers["n"],
                throughput_per_min=float(throughput["n"]),
                avg_wait_sec=timing["avg_wait"],
                avg_exec_sec=timing["avg_exec"],
                p95_exec_sec=timing["p95_exec"],
                p99_exec_sec=timing["p99_exec"],
            )

    async def get_all_workers(self):
        async with acquire() as conn:
            return await conn.fetch(
                """
                SELECT id, queue_names, hostname, pid, status,
                       started_at, last_heartbeat, jobs_processed,
                       jobs_failed, current_job_id
                FROM workers
                ORDER BY started_at
                """
            )

    async def get_recent_events(self, limit: int = 50):
        async with acquire() as conn:
            return await conn.fetch(
                """
                SELECT je.*, j.job_type, j.queue_name
                FROM job_events je
                JOIN jobs j ON j.id = je.job_id
                ORDER BY je.occurred_at DESC
                LIMIT $1
                """,
                limit,
            )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    async def _log_event(
        self,
        conn: asyncpg.Connection,
        job_id: uuid.UUID,
        worker_id: Optional[str],
        event_type: EventType,
        message: str = "",
        metadata: Optional[Dict[str, Any]] = None,
    ):
        await conn.execute(
            """
            INSERT INTO job_events (job_id, worker_id, event_type, message, metadata)
            VALUES ($1, $2, $3, $4, $5)
            """,
            job_id, worker_id, event_type.value, message, metadata or {},
        )