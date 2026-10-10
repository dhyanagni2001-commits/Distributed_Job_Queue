# Scale Story: When PostgreSQL Breaks and What to Do Next

## Current Architecture Limits

This system uses PostgreSQL as its job queue backbone.
It works well up to a point. Below are the likely breaking points, the
symptoms, and the migration path for each.

Only the numbers that come from `benchmark.py` (see the README) are measured:
about 17,800 jobs/s sustained with 16 workers and no-op handlers on one
machine, with PostgreSQL CPU as the limit. Worker counts and thresholds in
this document are **estimates** that have not been benchmarked.

---

## Breaking Point 1: Connection Saturation

### What happens
Each worker *process* holds an asyncpg pool of 2-10 connections, shared by
all workers in that process. PostgreSQL's default max_connections is 100, so
about 10 worker processes at full pool size exhaust it. Workers then start
timing out on acquire().

### Symptoms
```
asyncpg.exceptions.TooManyConnectionsError
connection pool exhausted, timeout acquiring connection
```

### Fix (estimated to reach thousands of worker connections)
Add PgBouncer as a connection pooler in front of PostgreSQL.

```ini
# pgbouncer.ini
[databases]
jobqueue = host=localhost port=5432 dbname=jobqueue

[pgbouncer]
pool_mode = transaction
max_client_conn = 10000
default_pool_size = 50
```

```python
# config.py — point at PgBouncer, not Postgres directly
host: str = os.getenv("DB_HOST", "pgbouncer")   # port 6432
```

---

## Breaking Point 2: Claim Hot Spot and Database CPU

### What was measured
- Single-job claim latency: p50 about 1.05 ms, p99 1.32-1.45 ms (one claimer).
- With 16 workers and batch size 50, PostgreSQL used about 470% CPU and
  throughput was the same whether workers ran in 1 or 4 processes, so the
  database is the bottleneck at that point.
- Claim latency under hundreds of concurrent workers has **not** been measured.
  `SKIP LOCKED` avoids lock waits, so the expected symptom is rising DB CPU
  and claim latency rather than blocking.

### Fix: Queue partitioning (estimate, not benchmarked)

```sql
CREATE TABLE jobs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    queue_name      TEXT NOT NULL DEFAULT 'default',
    job_type        TEXT NOT NULL,
    payload         JSONB NOT NULL DEFAULT '{}',
    priority        INTEGER NOT NULL DEFAULT 5,
    status          job_status NOT NULL DEFAULT 'pending',
    scheduled_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    worker_id       TEXT,
    lease_expires_at TIMESTAMPTZ,
    claimed_at      TIMESTAMPTZ,
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    attempt_count   INTEGER NOT NULL DEFAULT 0,
    max_attempts    INTEGER NOT NULL DEFAULT 3,
    retry_delay_sec INTEGER NOT NULL DEFAULT 5,
    last_error      TEXT,
    last_error_at   TIMESTAMPTZ,
    idempotency_key TEXT UNIQUE,
    result          JSONB,
    tags            TEXT[] DEFAULT '{}',
    metadata        JSONB DEFAULT '{}'
) PARTITION BY LIST (queue_name);

CREATE TABLE jobs_default  PARTITION OF jobs FOR VALUES IN ('default');
CREATE TABLE jobs_email    PARTITION OF jobs FOR VALUES IN ('email');
CREATE TABLE jobs_critical PARTITION OF jobs FOR VALUES IN ('critical');
CREATE TABLE jobs_bulk     PARTITION OF jobs FOR VALUES IN ('bulk');
```

---

## Breaking Point 3: Table Bloat (>10M rows)

### Fix: Archival + vacuum tuning

```sql
-- Archive completed jobs older than 7 days
CREATE TABLE jobs_archive (LIKE jobs INCLUDING ALL);

INSERT INTO jobs_archive
SELECT * FROM jobs
WHERE status IN ('completed', 'dead_letter')
  AND updated_at < NOW() - INTERVAL '7 days';

DELETE FROM jobs
WHERE status IN ('completed', 'dead_letter')
  AND updated_at < NOW() - INTERVAL '7 days';

-- Aggressive autovacuum
ALTER TABLE jobs SET (
    autovacuum_vacuum_scale_factor = 0.01,
    autovacuum_analyze_scale_factor = 0.01,
    autovacuum_vacuum_cost_delay = 2
);
```

---

## Breaking Point 4: Single Region / No HA

### Fix options
- PostgreSQL streaming replication + Patroni for automatic failover
- Managed service: AWS RDS Multi-AZ, Google Cloud SQL HA (~30s failover)

---

## When to Migrate Away from PostgreSQL

Rough guidance (estimates; real handlers, payload sizes and the rest of the
database's workload matter more than these numbers):

| Situation                                              | Recommended stack                      |
|--------------------------------------------------------|----------------------------------------|
| Hundreds to a few thousand real jobs/s, Postgres already in the stack | This system as-is             |
| Many worker processes, connection pressure             | This system + PgBouncer (transaction mode) |
| Queue traffic hurting the main database                | Dedicated Postgres for the queue, partitioning |
| Fan-out, replay, retention, or isolation needed        | Redis Streams, RabbitMQ, or SQS        |
| Sustained very high volume across many consumers       | Kafka + consumer groups                |

---

## Migration Path: AbstractQueueBackend

The worker code is decoupled from the queue backend via QueueManager.
Swapping backends requires implementing one abstract class:

```python
# abstract_backend.py
from abc import ABC, abstractmethod
from typing import Optional, List
from models import ClaimedJob, Job, JobResult
import uuid

class AbstractQueueBackend(ABC):

    @abstractmethod
    async def enqueue(self, job: Job) -> uuid.UUID:
        """Insert a job. Return its ID."""

    @abstractmethod
    async def claim_job(
        self,
        worker_id: str,
        queue_names: List[str],
        lease_sec: int = 30,
    ) -> Optional[ClaimedJob]:
        """Atomically claim the next available job."""

    @abstractmethod
    async def complete_job(self, result: JobResult, worker_id: str) -> bool: ...

    @abstractmethod
    async def fail_job(self, result: JobResult, max_attempts: int, worker_id: str) -> bool: ...

    @abstractmethod
    async def renew_lease(
        self, job_id: uuid.UUID, worker_id: str, lease_sec: int
    ) -> bool: ...
```

Zero worker code changes — just swap the backend in main.py:
```python
# from queue_manager import QueueManager as Backend  # PostgreSQL
from kafka_backend import KafkaBackend as Backend    # Kafka

qm = Backend(bootstrap_servers="kafka:9092")
```

---

## PostgreSQL vs Industry Systems

| Dimension          | This system       | AWS SQS           | Apache Kafka     | Celery + Redis   |
|--------------------|-------------------|-------------------|------------------|------------------|
| Delivery           | at-least-once (exclusive claiming via row locks) | at-least-once | at-least-once (exactly-once only within Kafka transactions) | at-least-once (with late acks) |
| Throughput         | ~17.8k jobs/s measured (no-op handlers, 1 machine) | very high (managed) | millions/sec | high |
| Operational cost   | low (1 DB)        | zero (managed)    | high (cluster)   | medium (Redis)   |
| Query/debug jobs   | YES (SQL)         | no SQL            | no SQL           | limited          |
| Ordered delivery   | priority, best effort | no order (FIFO queues available) | per partition | no order |
| Scheduled jobs     | YES (scheduled_at)| delay queues      | external needed  | YES (eta param)  |
| Dead letter        | YES (same DB)     | DLQ native        | separate topic   | native           |
| Multi-region       | needs replication | built-in          | built-in         | needs setup      |
| Extra infra needed | none              | none (managed)    | Zookeeper/KRaft  | Redis            |
