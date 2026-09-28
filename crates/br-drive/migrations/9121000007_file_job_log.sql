-- The processing state of a file is no longer stored: it is computed from a
-- per-file log of the Jobs jobs the library created for it (one per chain
-- step) and from whether its upload was confirmed.
--
-- `drive.file_job` holds one row per job. `trigger` is the gesture that
-- started the chain the job belongs to (`upload`, `reprocess`,
-- `regenerate_page`; NULL on a job carried over from 0.1). `events` is
-- append-only, in arrival order, each entry `{kind, at, ...payload}`:
--   * every Jobs fact about the job, as received (`queued`,
--     `creation_rejected`, `started`, `plan_declared`, `step_started`,
--     `completed`, `failed`, `cancelled`);
--   * what the library writes itself: `reported_done` (the runner's final
--     report), `reported_failed` (the runner's declared failure, with its
--     `reason_code`), `cancel_requested` (a user's cancel), and the
--     `cancelled` of a step never started because the user's cancel crossed
--     the previous step's end (a row of its own, never sent to Jobs).
-- The backfill below also writes `reported_done` and `failed` entries (with a
-- `reason`) for what 0.1 had stored.
--
-- `outcome` is the FIRST terminal entry of the log (`reported_done`,
-- `reported_failed`, `failed`, `cancelled`, `creation_rejected`), maintained
-- by Postgres on every write: a job settles once. Jobs' `completed` is not
-- terminal: it only confirms the library's own `job.finish`, sent with the
-- runner's final report, and is logged as information. Jobs' facts arrive on
-- separate durables, so a non-terminal fact may land after the terminal one,
-- and a redelivered or stray terminal fact after it; neither reopens nor
-- changes a settled job. The log is append-only, so once set, `outcome`
-- never changes: "at most one live job per file" is a partial unique index
-- on it, checked by Postgres at insert time, so it holds under concurrency (a
-- second concurrent insert waits for the first and then fails), whatever the
-- gestures' own row locks do.
CREATE TABLE drive.file_job (
    job_id       uuid        PRIMARY KEY,
    -- The order jobs were recorded in, assigned by Postgres: "last job" never
    -- depends on the pods' wall clocks.
    seq          bigint      GENERATED ALWAYS AS IDENTITY,
    file_id      uuid        NOT NULL REFERENCES drive.file (id) ON DELETE CASCADE,
    step_index   integer     NOT NULL CHECK (step_index >= 0),
    trigger      text        CHECK (trigger IS NULL OR trigger IN ('upload', 'reprocess', 'regenerate_page')),
    triggered_by jsonb,
    events       jsonb       NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(events) = 'array'),
    outcome      jsonb       GENERATED ALWAYS AS (
        jsonb_path_query_first(
            events,
            '$[*] ? (@.kind == "reported_done" || @.kind == "reported_failed" || @.kind == "failed" || @.kind == "cancelled" || @.kind == "creation_rejected")'
        )
    ) STORED,
    created_at   timestamptz NOT NULL
);

CREATE INDEX file_job_file_idx ON drive.file_job (file_id, seq);
CREATE UNIQUE INDEX file_job_one_live_idx ON drive.file_job (file_id) WHERE outcome IS NULL;

-- NULL while the upload is not confirmed.
ALTER TABLE drive.file ADD COLUMN committed_at timestamptz;

-- Backfill from 0.1's stored state. A file past PENDING was committed. A job
-- in flight becomes its file's log:
--   * no final report yet: an empty log — the file stays PROCESSING and the
--     runner's next report (or Jobs' next fact) lands in it;
--   * the runner's final report received (`done_at`): the job ended there,
--     `reported_done`. On the last step of its chain that is the end (READY);
--     on an earlier step the next step was never asked of Jobs — a migration
--     cannot stage a command — so the chain is recorded as interrupted before
--     that step (FAILED `interrupted`, open to a reprocess);
--   * Jobs' `completed` received without the final report (`completed_at`
--     alone; unreachable with the real Jobs): the runner is gone — `completed`
--     is logged, then the job is recorded `failed` `interrupted`.
-- A failed file keeps its error as a synthetic `failed` entry of a synthetic
-- job, never sent to Jobs. A PROCESSING file without a job cannot come out of
-- 0.1; should one exist, it lands FAILED `interrupted`. Synthetic ids are v4:
-- never sent to Jobs, never compared with a Jobs id.
UPDATE drive.file SET committed_at = updated_at WHERE processing_state <> 'pending';

INSERT INTO drive.file_job (job_id, file_id, step_index, triggered_by, events, created_at)
SELECT job_id, id, COALESCE(step_index, 0), triggered_by,
       CASE
           WHEN done_at IS NOT NULL THEN jsonb_build_array(jsonb_build_object(
               'kind', 'reported_done',
               'at', to_char(done_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')))
           WHEN completed_at IS NOT NULL THEN jsonb_build_array(
               jsonb_build_object(
                   'kind', 'completed',
                   'at', to_char(completed_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
                   'job_id', job_id),
               jsonb_build_object(
                   'kind', 'failed',
                   'at', to_char(completed_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
                   'reason', 'interrupted'))
           ELSE '[]'::jsonb
       END,
       updated_at
FROM drive.file
WHERE processing_state = 'processing' AND job_id IS NOT NULL;

INSERT INTO drive.file_job (job_id, file_id, step_index, triggered_by, events, created_at)
SELECT gen_random_uuid(), id, COALESCE(step_index, 0) + 1, triggered_by,
       jsonb_build_array(jsonb_build_object(
           'kind', 'failed',
           'at', to_char(done_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
           'reason', 'interrupted')),
       updated_at
FROM drive.file
WHERE processing_state = 'processing' AND job_id IS NOT NULL AND done_at IS NOT NULL
  AND COALESCE(step_index, 0) + 1 < COALESCE(jsonb_array_length(steps), 0);

INSERT INTO drive.file_job (job_id, file_id, step_index, triggered_by, events, created_at)
SELECT gen_random_uuid(), id, COALESCE(step_index, 0), triggered_by,
       jsonb_build_array(jsonb_build_object(
           'kind', 'failed',
           'at', to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
           'reason', CASE
               WHEN processing_state = 'failed' THEN COALESCE(processing_error, 'failed')
               ELSE 'interrupted'
           END)),
       updated_at
FROM drive.file
WHERE processing_state = 'failed'
   OR (processing_state = 'processing' AND job_id IS NULL);

ALTER TABLE drive.file
    DROP CONSTRAINT file_job_only_while_processing,
    DROP COLUMN processing_state,
    DROP COLUMN processing_error,
    DROP COLUMN step_index,
    DROP COLUMN step_count,
    DROP COLUMN step_runner_type,
    DROP COLUMN job_id,
    DROP COLUMN plan,
    DROP COLUMN progress_index,
    DROP COLUMN progress_label,
    DROP COLUMN progress_at,
    DROP COLUMN triggered_by,
    DROP COLUMN done_at,
    DROP COLUMN completed_at;

-- The runner-type catalogue copy stays, as information only (a save's warning,
-- the known-types read); nothing waits for its first scan any more.
DROP TABLE drive.catalogue_scan;

-- The processing status of every file, from its LAST job:
--   no job, upload not confirmed                  -> pending
--   no job, upload confirmed                      -> ready (stored, unprocessed)
--   last job's outcome `reported_done`            -> ready (the chain appends
--                                                    the next step's job in the
--                                                    same transaction, so a
--                                                    done last job is the end)
--   `reported_failed` / `failed` / `cancelled` / `creation_rejected`
--                                                 -> failed, the error read
--                                                    from that entry
--   no outcome yet (any other entry, or none)     -> processing
-- The file's drive rides along (the per-drive counts filter on it, so the
-- predicate reaches `file_drive_idx`), as do the last job's own columns for
-- the library's reads (its identity, step, trigger, initiator, and its log
-- while it runs — a settled job's log is never read back through the view).
CREATE VIEW drive.file_status AS
SELECT f.id AS file_id,
       f.drive_id,
       CASE
           WHEN j.job_id IS NULL AND f.committed_at IS NULL THEN 'pending'
           WHEN j.job_id IS NULL THEN 'ready'
           WHEN j.outcome ->> 'kind' = 'reported_done' THEN 'ready'
           WHEN j.outcome IS NOT NULL THEN 'failed'
           ELSE 'processing'
       END AS processing_state,
       CASE j.outcome ->> 'kind'
           WHEN 'reported_failed' THEN COALESCE(j.outcome ->> 'reason_code', 'failed')
           WHEN 'failed' THEN COALESCE(
               j.outcome ->> 'reason',
               j.outcome -> 'failure_report' ->> 'reason_code',
               j.outcome ->> 'failure_cause',
               'failed')
           WHEN 'cancelled' THEN 'cancelled'
           WHEN 'creation_rejected' THEN COALESCE(j.outcome ->> 'reason_code', 'creation_rejected')
       END AS processing_error,
       j.job_id AS last_job_id,
       j.step_index AS last_job_step,
       j.trigger AS last_job_trigger,
       j.triggered_by AS last_job_triggered_by,
       CASE WHEN j.outcome IS NULL THEN j.events END AS last_job_events,
       j.created_at AS last_job_created_at
FROM drive.file f
LEFT JOIN LATERAL (
    SELECT fj.job_id, fj.step_index, fj.trigger, fj.triggered_by, fj.events, fj.outcome,
           fj.created_at
    FROM drive.file_job fj
    WHERE fj.file_id = f.id
    ORDER BY fj.seq DESC
    LIMIT 1
) j ON true;
