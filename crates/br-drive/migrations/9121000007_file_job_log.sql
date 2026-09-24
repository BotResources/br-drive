-- The processing state of a file is no longer stored: it is computed from a
-- per-file log of the Jobs jobs the library created for it (one per chain
-- step) and from whether its upload was confirmed.
--
-- `drive.file_job` holds one row per job. `events` is append-only, in arrival
-- order, each entry `{kind, at, ...the fact's payload as received}`: every Jobs
-- fact about the job (`queued`, `creation_rejected`, `started`,
-- `plan_declared`, `step_started`, `completed`, `failed`, `cancelled`), plus
-- the two entries the library writes itself where Jobs says nothing
-- (`cancel_requested`, and a `failed` carrying a `reason`).
--
-- `outcome` is the FIRST terminal entry of the log (`completed`, `failed`,
-- `cancelled`, `creation_rejected`), maintained by Postgres on every write:
-- a job settles once. Jobs' facts arrive on separate durables, so a
-- non-terminal fact may land after the terminal one, and a redelivered or
-- stray terminal fact after it; neither reopens nor changes a settled job.
-- The log is append-only, so once set, `outcome` never changes: "at most one live job per file" is a
-- partial unique index on it, checked by Postgres at insert time, so it holds
-- under concurrency (a second concurrent insert waits for the first and then
-- fails), whatever the gestures' own row locks do.
CREATE TABLE drive.file_job (
    job_id       uuid        PRIMARY KEY,
    file_id      uuid        NOT NULL REFERENCES drive.file (id) ON DELETE CASCADE,
    step_index   integer     NOT NULL CHECK (step_index >= 0),
    triggered_by jsonb,
    events       jsonb       NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(events) = 'array'),
    outcome      jsonb       GENERATED ALWAYS AS (
        jsonb_path_query_first(
            events,
            '$[*] ? (@.kind == "completed" || @.kind == "failed" || @.kind == "cancelled" || @.kind == "creation_rejected")'
        )
    ) STORED,
    created_at   timestamptz NOT NULL
);

CREATE INDEX file_job_file_idx ON drive.file_job (file_id, created_at, job_id);
CREATE UNIQUE INDEX file_job_one_live_idx ON drive.file_job (file_id) WHERE outcome IS NULL;

-- NULL while the upload is not confirmed.
ALTER TABLE drive.file ADD COLUMN committed_at timestamptz;

-- Backfill from the stored state: a file past PENDING was committed; a job in
-- flight becomes its file's log (empty: the file stays PROCESSING and the next
-- fact of that job lands in it); a failed file keeps its error as a synthetic
-- `failed` entry of a synthetic job, never sent to Jobs. A PROCESSING file
-- without a job cannot come out of 0.1; should one exist, it lands FAILED
-- `interrupted` and can be reprocessed.
UPDATE drive.file SET committed_at = updated_at WHERE processing_state <> 'pending';

INSERT INTO drive.file_job (job_id, file_id, step_index, triggered_by, events, created_at)
SELECT job_id, id, COALESCE(step_index, 0), triggered_by, '[]'::jsonb, updated_at
FROM drive.file
WHERE processing_state = 'processing' AND job_id IS NOT NULL;

INSERT INTO drive.file_job (job_id, file_id, step_index, triggered_by, events, created_at)
SELECT gen_random_uuid(), id, COALESCE(step_index, 0), triggered_by,
       jsonb_build_array(jsonb_build_object(
           'kind', 'failed',
           'at', to_jsonb(updated_at),
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

-- The runner-type catalogue copy is gone: Jobs judges a runner type itself.
DROP TABLE drive.known_runner_type;
DROP TABLE drive.catalogue_scan;

-- The per-drive counts (`br_drive::file_counts`) read the computed status: the
-- drive's files by this index, their last job by `file_job_file_idx`.
DROP INDEX drive.file_drive_idx;
CREATE INDEX file_drive_committed_idx ON drive.file (drive_id, committed_at);

-- The processing status of every file, from its LAST job:
--   no job, upload not confirmed        -> pending
--   no job, upload confirmed            -> ready
--   last job's outcome `completed`      -> ready (the chain appends the next
--                                          step's job in the same transaction,
--                                          so a completed last job is the end)
--   `failed` / `cancelled` / `creation_rejected` -> failed, the error read from
--                                          that entry
--   no outcome yet (any other entry, or none)    -> processing
-- The last job's own columns ride along for the library's reads (its
-- identity, step, initiator and log).
CREATE VIEW drive.file_status AS
SELECT f.id AS file_id,
       CASE
           WHEN j.job_id IS NULL AND f.committed_at IS NULL THEN 'pending'
           WHEN j.job_id IS NULL THEN 'ready'
           WHEN j.outcome ->> 'kind' = 'completed' THEN 'ready'
           WHEN j.outcome IS NOT NULL THEN 'failed'
           ELSE 'processing'
       END AS processing_state,
       CASE j.outcome ->> 'kind'
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
       j.triggered_by AS last_job_triggered_by,
       j.events AS last_job_events,
       j.created_at AS last_job_created_at
FROM drive.file f
LEFT JOIN LATERAL (
    SELECT fj.job_id, fj.step_index, fj.triggered_by, fj.events, fj.outcome, fj.created_at
    FROM drive.file_job fj
    WHERE fj.file_id = f.id
    ORDER BY fj.created_at DESC, fj.job_id DESC
    LIMIT 1
) j ON true;
