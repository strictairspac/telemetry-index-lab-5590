# Schema v2 — cancel & execution time limit

`experiments` gains: `time_limit_seconds INT NOT NULL DEFAULT 120`,
`phase VARCHAR(20) NOT NULL DEFAULT 'queued'`, `reason VARCHAR(500) NULL`,
`stop_kind VARCHAR(20) NULL` (`user`/`timeout`), `started_at DATETIME NULL`,
`finished_at DATETIME NULL`, `connection_id INT NULL`.

Fresh installs get these from `Base.metadata.create_all`. Existing volumes are
upgraded idempotently by `src/config/migrate.py` (`ensure_schema()`), which runs
at backend lifespan start and worker startup; it only issues `ALTER TABLE` for
columns missing from `information_schema.COLUMNS`.

Status machine: `queued → running → completed/failed`; `queued → cancelled`
(never executes); `running → cancelling → cancelled/timeout` (terminal state is
published only after DB execution stop, probe rollback and temp-index cleanup
are verified); cleanup failure → `cleanup_pending`, retried by the worker
janitor. All transitions are conditional UPDATEs so cancel vs. completion keeps
exactly one terminal state. Worker crash orphans (`running`/`cancelling`) are
recovered at worker startup under the serial GET_LOCK, which guarantees the
leftover `trial_candidate` index belongs to the interrupted trial.
