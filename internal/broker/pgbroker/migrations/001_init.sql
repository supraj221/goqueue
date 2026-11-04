CREATE TABLE jobs (
    id               text        PRIMARY KEY,
    queue            text        NOT NULL,
    type             text        NOT NULL,
    payload          json,
    priority         integer     NOT NULL DEFAULT 0,
    state            text        NOT NULL CHECK (state IN ('pending','scheduled','running','succeeded','dead')),
    attempts         integer     NOT NULL DEFAULT 0,
    max_attempts     integer     NOT NULL,
    run_at           timestamptz NOT NULL,
    lease_owner      text        NOT NULL DEFAULT '',
    lease_token      bigint      NOT NULL DEFAULT 0,
    lease_expires_at timestamptz,
    last_error       text        NOT NULL DEFAULT '',
    idempotency_key  text,
    created_at       timestamptz NOT NULL,
    updated_at       timestamptz NOT NULL,
    completed_at     timestamptz
);

-- Claimable rows: pending, or scheduled and waiting for run_at. The claim query
-- scans this index in claim order and stops after LIMIT rows.
CREATE INDEX jobs_claimable_idx ON jobs (queue, priority DESC, run_at, id)
    WHERE state IN ('pending', 'scheduled');

-- Running rows by lease expiry, for reclaim and the reaper.
CREATE INDEX jobs_lease_idx ON jobs (lease_expires_at)
    WHERE state = 'running';

-- Exactly-once enqueue.
CREATE UNIQUE INDEX jobs_idempotency_idx ON jobs (idempotency_key)
    WHERE idempotency_key IS NOT NULL;
