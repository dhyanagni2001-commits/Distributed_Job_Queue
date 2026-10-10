-- ============================================================
-- Distributed Job Queue - PostgreSQL Schema
-- ============================================================

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- Job status enum
DO $$ BEGIN
    CREATE TYPE job_status AS ENUM (
        'pending',
        'claimed',
        'running',
        'completed',
        'failed',
        'dead_letter'
    );
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

-- ============================================================
-- Core jobs table
-- ============================================================
CREATE TABLE IF NOT EXISTS jobs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    queue_name      TEXT NOT NULL DEFAULT 'default',
    job_type        TEXT NOT NULL,
    payload         JSONB NOT NULL DEFAULT '{}',
    priority        INTEGER NOT NULL DEFAULT 5,        -- 1=highest, 10=lowest
    status          job_status NOT NULL DEFAULT 'pending',

    -- Scheduling
    scheduled_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Worker leasing
    worker_id       TEXT,
    lease_expires_at TIMESTAMPTZ,
    claimed_at      TIMESTAMPTZ,
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,

    -- Retry tracking
    attempt_count   INTEGER NOT NULL DEFAULT 0,
    max_attempts    INTEGER NOT NULL DEFAULT 3,
    retry_delay_sec INTEGER NOT NULL DEFAULT 5,
    last_error      TEXT,
    last_error_at   TIMESTAMPTZ,

    -- Idempotency
    idempotency_key TEXT UNIQUE,

    -- Result storage
    result          JSONB,

    -- Metadata
    tags            TEXT[] DEFAULT '{}',
    metadata        JSONB DEFAULT '{}'
);

-- ============================================================
-- Indexes for efficient polling and claiming
-- ============================================================
CREATE INDEX IF NOT EXISTS idx_jobs_claimable ON jobs (
    queue_name, priority ASC, scheduled_at ASC
)
WHERE status = 'pending';

CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs (status, updated_at);
CREATE INDEX IF NOT EXISTS idx_jobs_worker ON jobs (worker_id) WHERE worker_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_jobs_lease_expiry ON jobs (lease_expires_at) WHERE status = 'claimed' OR status = 'running';
CREATE INDEX IF NOT EXISTS idx_jobs_queue_name ON jobs (queue_name, status);

-- ============================================================
-- Workers registry table
-- ============================================================
CREATE TABLE IF NOT EXISTS workers (
    id              TEXT PRIMARY KEY,
    queue_names     TEXT[] NOT NULL DEFAULT '{default}',
    hostname        TEXT NOT NULL,
    pid             INTEGER NOT NULL,
    status          TEXT NOT NULL DEFAULT 'idle',   -- idle, busy, draining, dead
    started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_heartbeat  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    jobs_processed  INTEGER NOT NULL DEFAULT 0,
    jobs_failed     INTEGER NOT NULL DEFAULT 0,
    current_job_id  UUID REFERENCES jobs(id) ON DELETE SET NULL,
    metadata        JSONB DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_workers_heartbeat ON workers (last_heartbeat);
CREATE INDEX IF NOT EXISTS idx_workers_status ON workers (status);

-- ============================================================
-- Job events / audit log
-- ============================================================
CREATE TABLE IF NOT EXISTS job_events (
    id              BIGSERIAL PRIMARY KEY,
    job_id          UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    worker_id       TEXT,
    event_type      TEXT NOT NULL,   -- claimed, started, completed, failed, retried, dead_lettered
    message         TEXT,
    metadata        JSONB DEFAULT '{}',
    occurred_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_job_events_job_id ON job_events (job_id, occurred_at DESC);
CREATE INDEX IF NOT EXISTS idx_job_events_type ON job_events (event_type, occurred_at DESC);

-- ============================================================
-- Metrics snapshots (time-series for dashboard)
-- ============================================================
CREATE TABLE IF NOT EXISTS metrics_snapshots (
    id              BIGSERIAL PRIMARY KEY,
    captured_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    queue_name      TEXT NOT NULL DEFAULT 'default',
    pending_count   INTEGER NOT NULL DEFAULT 0,
    running_count   INTEGER NOT NULL DEFAULT 0,
    completed_count INTEGER NOT NULL DEFAULT 0,
    failed_count    INTEGER NOT NULL DEFAULT 0,
    dead_letter_count INTEGER NOT NULL DEFAULT 0,
    active_workers  INTEGER NOT NULL DEFAULT 0,
    throughput_per_min FLOAT NOT NULL DEFAULT 0,
    avg_wait_sec    FLOAT,
    avg_exec_sec    FLOAT,
    p95_exec_sec    FLOAT,
    p99_exec_sec    FLOAT
);

CREATE INDEX IF NOT EXISTS idx_metrics_time ON metrics_snapshots (captured_at DESC, queue_name);

-- ============================================================
-- Atomic claim function (exactly-once guarantee)
-- ============================================================
CREATE OR REPLACE FUNCTION claim_next_job(
    p_worker_id     TEXT,
    p_queue_names   TEXT[],
    p_lease_sec     INTEGER DEFAULT 30
)
RETURNS TABLE (
    out_job_id        UUID,
    out_job_type      TEXT,
    out_payload       JSONB,
    out_attempt_count INTEGER,
    out_max_attempts  INTEGER
)
LANGUAGE plpgsql AS $$
DECLARE
    v_job_id        UUID;
    v_job_type      TEXT;
    v_payload       JSONB;
    v_attempt_count INTEGER;
    v_max_attempts  INTEGER;
BEGIN
    SELECT j.id INTO v_job_id
    FROM jobs j
    WHERE j.queue_name = ANY(p_queue_names)
      AND j.status = 'pending'
      AND j.scheduled_at <= NOW()
    ORDER BY j.priority ASC, j.scheduled_at ASC
    LIMIT 1
    FOR UPDATE SKIP LOCKED;

    IF v_job_id IS NULL THEN
        RETURN;
    END IF;

    UPDATE jobs
    SET
        status           = 'claimed',
        worker_id        = p_worker_id,
        lease_expires_at = NOW() + (p_lease_sec || ' seconds')::INTERVAL,
        claimed_at       = NOW(),
        attempt_count    = jobs.attempt_count + 1,
        updated_at       = NOW()
    WHERE id = v_job_id
    RETURNING
        jobs.job_type,
        jobs.payload,
        jobs.attempt_count,
        jobs.max_attempts
    INTO v_job_type, v_payload, v_attempt_count, v_max_attempts;

    out_job_id        := v_job_id;
    out_job_type      := v_job_type;
    out_payload       := v_payload;
    out_attempt_count := v_attempt_count;
    out_max_attempts  := v_max_attempts;
    RETURN NEXT;
END;
$$;

-- ============================================================
-- Lease renewal function
-- ============================================================
CREATE OR REPLACE FUNCTION renew_job_lease(
    p_job_id    UUID,
    p_worker_id TEXT,
    p_lease_sec INTEGER DEFAULT 30
)
RETURNS BOOLEAN LANGUAGE plpgsql AS $$
DECLARE
    updated_count INTEGER;
BEGIN
    UPDATE jobs SET
        lease_expires_at = NOW() + (p_lease_sec || ' seconds')::INTERVAL,
        updated_at       = NOW()
    WHERE id = p_job_id
      AND worker_id = p_worker_id
      AND status IN ('claimed', 'running');

    GET DIAGNOSTICS updated_count = ROW_COUNT;
    RETURN updated_count > 0;
END;
$$;

-- ============================================================
-- Recover expired leases (called by any worker periodically).
-- A job whose attempts are used up goes straight to dead_letter, so a job
-- that crashes its worker every time cannot loop forever.
-- ============================================================
CREATE OR REPLACE FUNCTION recover_expired_leases()
RETURNS INTEGER LANGUAGE plpgsql AS $$
DECLARE
    recovered INTEGER;
BEGIN
    WITH expired AS (
        UPDATE jobs j SET
            status           = CASE WHEN j.attempt_count >= j.max_attempts
                                    THEN 'dead_letter'::job_status
                                    ELSE 'pending'::job_status END,
            last_error       = CASE WHEN j.attempt_count >= j.max_attempts
                                    THEN 'Lease expired on final attempt (worker lost)'
                                    ELSE j.last_error END,
            last_error_at    = CASE WHEN j.attempt_count >= j.max_attempts
                                    THEN NOW() ELSE j.last_error_at END,
            worker_id        = NULL,
            lease_expires_at = NULL,
            claimed_at       = NULL,
            updated_at       = NOW()
        FROM (
            SELECT id, worker_id AS old_worker_id FROM jobs
            WHERE status IN ('claimed', 'running')
              AND lease_expires_at < NOW()
            FOR UPDATE SKIP LOCKED
        ) old
        WHERE j.id = old.id
        RETURNING j.id, j.status, j.attempt_count, old.old_worker_id
    ), ev AS (
        INSERT INTO job_events (job_id, worker_id, event_type, message)
        SELECT id, old_worker_id,
               CASE WHEN status = 'dead_letter' THEN 'dead_lettered' ELSE 'lease_expired' END,
               'Lease held by ' || COALESCE(old_worker_id, '?') || ' expired on attempt '
                   || attempt_count
                   || CASE WHEN status = 'dead_letter' THEN '; attempts exhausted' ELSE '; requeued' END
        FROM expired
    )
    SELECT COUNT(*) INTO recovered FROM expired;

    RETURN recovered;
END;
$$;

-- ============================================================
-- Move exhausted jobs to dead letter
-- ============================================================
CREATE OR REPLACE FUNCTION process_dead_letters()
RETURNS INTEGER LANGUAGE plpgsql AS $$
DECLARE
    moved INTEGER;
BEGIN
    WITH dead AS (
        UPDATE jobs SET
            status     = 'dead_letter',
            updated_at = NOW()
        WHERE status = 'failed'
          AND attempt_count >= max_attempts
        RETURNING id
    )
    SELECT COUNT(*) INTO moved FROM dead;

    RETURN moved;
END;
$$;
-- ─────────────────────────────────────────────────────────────────────────────
-- Batched claim: claims up to p_limit jobs, marks them running, and writes
-- their claimed/started events in a single round trip.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION claim_jobs(
    p_worker_id   TEXT,
    p_queue_names TEXT[],
    p_lease_sec   INTEGER,
    p_limit       INTEGER
)
RETURNS TABLE (
    out_job_id        UUID,
    out_job_type      TEXT,
    out_payload       JSONB,
    out_attempt_count INTEGER,
    out_max_attempts  INTEGER
)
LANGUAGE sql AS $$
    WITH picked AS (
        SELECT id FROM jobs
        WHERE queue_name = ANY(p_queue_names)
          AND status = 'pending'
          AND scheduled_at <= NOW()
        ORDER BY priority ASC, scheduled_at ASC
        LIMIT p_limit
        FOR UPDATE SKIP LOCKED
    ), upd AS (
        UPDATE jobs j SET
            status           = 'running',
            worker_id        = p_worker_id,
            lease_expires_at = NOW() + make_interval(secs => p_lease_sec),
            claimed_at       = NOW(),
            started_at       = NOW(),
            attempt_count    = j.attempt_count + 1,
            updated_at       = NOW()
        FROM picked WHERE j.id = picked.id
        RETURNING j.id, j.job_type, j.payload, j.attempt_count, j.max_attempts,
                  j.priority, j.scheduled_at
    ), ev AS (
        INSERT INTO job_events (job_id, worker_id, event_type, message)
        SELECT id, p_worker_id, 'claimed',
               'Claimed by worker ' || p_worker_id || ' (attempt ' || attempt_count || ')'
        FROM upd
        UNION ALL
        SELECT id, p_worker_id, 'started', 'Execution started' FROM upd
    )
    SELECT id, job_type, payload, attempt_count, max_attempts
    FROM upd ORDER BY priority ASC, scheduled_at ASC;
$$;

-- ─────────────────────────────────────────────────────────────────────────────
-- Batched completion: marks jobs completed, writes completed events, and
-- updates the worker's counter in a single round trip. Only jobs this worker
-- still owns are completed; a job whose lease expired and was reclaimed is
-- skipped, so a stalled worker cannot complete another worker's job.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION complete_jobs(
    p_worker_id TEXT,
    p_job_ids   UUID[],
    p_results   JSONB[],
    p_durations FLOAT8[]
)
RETURNS INTEGER
LANGUAGE sql AS $$
    WITH input AS (
        SELECT * FROM unnest(p_job_ids, p_results, p_durations) AS t(id, result, duration)
    ), upd AS (
        UPDATE jobs j SET
            status       = 'completed',
            result       = input.result,
            completed_at = NOW(),
            updated_at   = NOW(),
            worker_id    = NULL
        FROM input
        WHERE j.id = input.id
          AND j.worker_id = p_worker_id
          AND j.status = 'running'
        RETURNING j.id, input.duration
    ), ev AS (
        INSERT INTO job_events (job_id, event_type, message, metadata)
        SELECT id, 'completed', 'Completed in ' || round(duration::numeric, 3) || 's',
               jsonb_build_object('duration_sec', duration)
        FROM upd
    ), stats AS (
        UPDATE workers SET jobs_processed = jobs_processed + (SELECT COUNT(*) FROM upd)
        WHERE id = p_worker_id
    )
    SELECT COUNT(*)::INTEGER FROM upd;
$$;
