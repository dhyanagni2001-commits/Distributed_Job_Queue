# abstract_backend.py
# Defines the interface all queue backends must implement.
# Swap PostgresBackend for KafkaBackend or SQSBackend with zero worker changes.

from __future__ import annotations

import uuid
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from models import ClaimedJob, Job, JobResult, QueueStats


class AbstractQueueBackend(ABC):
    """
    All queue operations any backend must support.
    Workers only call methods on this interface -- they never touch SQL or Kafka directly.
    """

    # ------------------------------------------------------------------
    # Job lifecycle
    # ------------------------------------------------------------------

    @abstractmethod
    async def enqueue(self, job: Job) -> uuid.UUID:
        """
        Insert a job into the queue. Return its UUID.
        Must be idempotent when job.idempotency_key is set.
        """

    @abstractmethod
    async def enqueue_batch(self, jobs: List[Job]) -> List[uuid.UUID]:
        """Enqueue multiple jobs. Return IDs in the same order."""

    @abstractmethod
    async def claim_job(
        self,
        worker_id: str,
        queue_names: List[str],
        lease_sec: int = 30,
    ) -> Optional[ClaimedJob]:
        """
        Atomically claim the next available job.
        Returns None if the queue is empty.
        Guarantee: no two workers ever receive the same ClaimedJob.
        """

    @abstractmethod
    async def mark_running(self, job_id: uuid.UUID, worker_id: str):
        """Transition a claimed job to running status."""

    @abstractmethod
    async def complete_job(self, result: JobResult, worker_id: str) -> bool:
        """
        Mark a job as completed and store the result.
        Only succeeds if worker_id still owns the job; returns False otherwise.
        """

    @abstractmethod
    async def fail_job(self, result: JobResult, max_attempts: int, worker_id: str) -> bool:
        """
        Mark a job attempt as failed.
        If attempt_count < max_attempts: re-queue for retry.
        If attempt_count >= max_attempts: mark permanently failed.
        Only applies if worker_id still owns the job; returns False otherwise.
        """

    # ------------------------------------------------------------------
    # Lease management
    # ------------------------------------------------------------------

    @abstractmethod
    async def renew_lease(
        self,
        job_id: uuid.UUID,
        worker_id: str,
        lease_sec: int = 30,
    ) -> bool:
        """
        Extend the lease on an in-progress job.
        Returns False if the lease was lost (job recovered by another worker).
        """

    # ------------------------------------------------------------------
    # Maintenance (fault tolerance)
    # ------------------------------------------------------------------

    @abstractmethod
    async def recover_expired_leases(self) -> int:
        """
        Reset jobs with expired leases back to pending, or to dead_letter if
        their attempts are used up. Safe to call concurrently from multiple workers.
        Returns count of recovered jobs.
        """

    @abstractmethod
    async def process_dead_letters(self) -> int:
        """
        Move permanently failed jobs to dead_letter status.
        Returns count of jobs moved.
        """

    # ------------------------------------------------------------------
    # Worker registry
    # ------------------------------------------------------------------

    @abstractmethod
    async def register_worker(
        self,
        worker_id: str,
        queue_names: List[str],
        hostname: str,
        pid: int,
    ):
        """Register a worker. Called at startup."""

    @abstractmethod
    async def heartbeat(
        self,
        worker_id: str,
        status: str,
        current_job_id: Optional[uuid.UUID] = None,
    ):
        """Update worker liveness. Called every heartbeat_interval_sec."""

    @abstractmethod
    async def deregister_worker(self, worker_id: str):
        """Remove a worker from the registry. Called at shutdown."""

    @abstractmethod
    async def increment_worker_stats(self, worker_id: str, success: bool):
        """Increment jobs_processed or jobs_failed counter for a worker."""

    # ------------------------------------------------------------------
    # Observability
    # ------------------------------------------------------------------

    @abstractmethod
    async def get_queue_stats(self, queue_name: str = "default") -> QueueStats:
        """Return current queue statistics including latency percentiles."""

    @abstractmethod
    async def get_all_workers(self) -> List[Any]:
        """Return all registered workers with their current status."""

    @abstractmethod
    async def get_recent_events(self, limit: int = 50) -> List[Any]:
        """Return recent job lifecycle events for the dashboard."""
