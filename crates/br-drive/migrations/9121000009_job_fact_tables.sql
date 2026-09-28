-- The job log becomes typed, insert-only fact tables, one per fact the library
-- reads — the shape Jobs gives its own ledger (runs, run_terminals, steps,
-- run_plan_declarations). Nothing is stored that is a state or derives one:
-- 0.2.0's global `seq`, its `events` array (rewritten on every fact) and its
-- generated `outcome` go. A file's status is only ever computed from which
-- fact rows exist:
--
--   drive.file_job         the job was created (one per chain step), numbered
--                          1, 2, 3… per file; the file's highest number is its
--                          last job
--   drive.file_job_end     the job ended — the runner's final report or its
--                          declared failure, or Jobs' failed / cancelled /
--                          creation_rejected. One row per job: the first end
--                          wins by the primary key, a later one is not stored
--   drive.file_job_cancel  a user asked to cancel the job
--   drive.file_job_plan    the runner declared its plan (the latest is current)
--   drive.file_job_step    the runner started a step of its plan
--
-- Jobs' `queued`, `started` and `completed` are not stored: nothing reads them
-- (`completed` only confirms the library's own `job.finish`).

DROP VIEW drive.file_status;

-- The job was created. `number` is assigned max + 1 under the file's row
-- lock, which every writer of a file's jobs takes first (the gestures through
-- the engine's load-for-update, the reactions through the same load): two
-- writers never compute the same number, and should one ever skip the lock,
-- the primary key refuses the second insert. With every new job created only
-- while the file's last job has ended (a gesture on a READY or FAILED file,
-- or the chain's next step in the transaction that ends the previous one),
-- this is what keeps at most one running job per file.
ALTER TABLE drive.file_job
    ADD COLUMN number            integer,
    ADD COLUMN triggered_by_id   uuid,
    ADD COLUMN triggered_by_name text;

UPDATE drive.file_job j
SET number = n.number
FROM (
    SELECT job_id, row_number() OVER (PARTITION BY file_id ORDER BY seq) AS number
    FROM drive.file_job
) n
WHERE n.job_id = j.job_id;

UPDATE drive.file_job
SET triggered_by_id = (triggered_by ->> 'id')::uuid,
    triggered_by_name = triggered_by ->> 'display_name'
WHERE triggered_by IS NOT NULL;

ALTER TABLE drive.file_job
    ALTER COLUMN number SET NOT NULL,
    ADD CONSTRAINT file_job_number_positive CHECK (number >= 1),
    ADD CONSTRAINT file_job_name_needs_id CHECK (triggered_by_name IS NULL OR triggered_by_id IS NOT NULL);

-- The job ended. The first terminal entry of 0.2.0's log is the end; the
-- entries after it changed nothing and are not kept.
CREATE TABLE drive.file_job_end (
    job_id      uuid        PRIMARY KEY REFERENCES drive.file_job (job_id) ON DELETE CASCADE,
    kind        text        NOT NULL CHECK (kind IN ('reported_done', 'reported_failed', 'failed', 'cancelled', 'creation_rejected')),
    reason_code text,
    message     text,
    at          timestamptz NOT NULL,
    CHECK (
        (kind IN ('reported_failed', 'failed', 'creation_rejected')
            AND reason_code IS NOT NULL AND reason_code <> '')
        OR
        (kind IN ('reported_done', 'cancelled')
            AND reason_code IS NULL AND message IS NULL)
    )
);

INSERT INTO drive.file_job_end (job_id, kind, reason_code, message, at)
SELECT job_id,
       outcome ->> 'kind',
       CASE outcome ->> 'kind'
           WHEN 'reported_failed' THEN COALESCE(NULLIF(outcome ->> 'reason_code', ''), 'failed')
           WHEN 'failed' THEN COALESCE(
               NULLIF(outcome ->> 'reason', ''),
               NULLIF(outcome -> 'failure_report' ->> 'reason_code', ''),
               NULLIF(outcome ->> 'failure_cause', ''),
               'failed')
           WHEN 'creation_rejected' THEN COALESCE(NULLIF(outcome ->> 'reason_code', ''), 'creation_rejected')
       END,
       CASE outcome ->> 'kind'
           WHEN 'reported_failed' THEN outcome ->> 'message'
           WHEN 'failed' THEN outcome ->> 'note'
       END,
       COALESCE((outcome ->> 'at')::timestamptz, created_at)
FROM drive.file_job
WHERE outcome IS NOT NULL;

-- A user asked to cancel the job. 0.2.0 did not record who: those rows carry
-- no requester.
CREATE TABLE drive.file_job_cancel (
    job_id       uuid        NOT NULL REFERENCES drive.file_job (job_id) ON DELETE CASCADE,
    number       integer     NOT NULL CHECK (number >= 1),
    requested_by uuid,
    at           timestamptz NOT NULL,
    PRIMARY KEY (job_id, number)
);

CREATE INDEX file_job_cancel_requested_by_idx ON drive.file_job_cancel (requested_by)
    WHERE requested_by IS NOT NULL;

INSERT INTO drive.file_job_cancel (job_id, number, requested_by, at)
SELECT j.job_id,
       row_number() OVER (PARTITION BY j.job_id ORDER BY e.ordinality),
       NULL,
       COALESCE((e.entry ->> 'at')::timestamptz, j.created_at)
FROM drive.file_job j
CROSS JOIN LATERAL jsonb_array_elements(j.events) WITH ORDINALITY AS e (entry, ordinality)
WHERE e.entry ->> 'kind' = 'cancel_requested';

-- The runner declared its plan, in a run of the job; the highest number is
-- the current plan.
CREATE TABLE drive.file_job_plan (
    job_id      uuid        NOT NULL REFERENCES drive.file_job (job_id) ON DELETE CASCADE,
    number      integer     NOT NULL CHECK (number >= 1),
    run_id      uuid        NOT NULL,
    labels      text[]      NOT NULL,
    declared_at timestamptz NOT NULL,
    PRIMARY KEY (job_id, number)
);

INSERT INTO drive.file_job_plan (job_id, number, run_id, labels, declared_at)
SELECT j.job_id,
       row_number() OVER (PARTITION BY j.job_id ORDER BY e.ordinality),
       (e.entry ->> 'run_id')::uuid,
       ARRAY(SELECT jsonb_array_elements_text(COALESCE(e.entry -> 'steps', '[]'::jsonb))),
       COALESCE((e.entry ->> 'at')::timestamptz, j.created_at)
FROM drive.file_job j
CROSS JOIN LATERAL jsonb_array_elements(j.events) WITH ORDINALITY AS e (entry, ordinality)
WHERE e.entry ->> 'kind' = 'plan_declared'
  AND e.entry ->> 'run_id' IS NOT NULL;

-- The runner started a step of its plan, in a run of the job. `plan_index` is
-- the step's index in the runner's plan — not the chain's `step_index`. A run
-- starts each of its steps once (Jobs' `steps` key); a retry run starts them
-- again, under its own run id.
CREATE TABLE drive.file_job_step (
    job_id     uuid        NOT NULL REFERENCES drive.file_job (job_id) ON DELETE CASCADE,
    run_id     uuid        NOT NULL,
    plan_index integer     NOT NULL CHECK (plan_index >= 0),
    label      text        NOT NULL,
    started_at timestamptz NOT NULL,
    PRIMARY KEY (job_id, run_id, plan_index)
);

INSERT INTO drive.file_job_step (job_id, run_id, plan_index, label, started_at)
SELECT j.job_id,
       (e.entry ->> 'run_id')::uuid,
       (e.entry ->> 'index')::integer,
       COALESCE(e.entry ->> 'label', ''),
       (e.entry ->> 'started_at')::timestamptz
FROM drive.file_job j
CROSS JOIN LATERAL jsonb_array_elements(j.events) AS e (entry)
WHERE e.entry ->> 'kind' = 'step_started'
  AND e.entry ->> 'run_id' IS NOT NULL
  AND e.entry ->> 'index' IS NOT NULL
  AND e.entry ->> 'started_at' IS NOT NULL
ON CONFLICT (job_id, run_id, plan_index) DO NOTHING;

-- The stored log goes: its global order, its array, its derived outcome, and
-- the index that relied on the outcome.
DROP INDEX drive.file_job_one_live_idx;
DROP INDEX drive.file_job_file_idx;
ALTER TABLE drive.file_job
    DROP COLUMN outcome,
    DROP COLUMN events,
    DROP COLUMN seq,
    DROP COLUMN triggered_by;

-- The job's id stays unique (the facts reference it); the file's jobs are
-- keyed by their number.
ALTER TABLE drive.file_job DROP CONSTRAINT file_job_pkey CASCADE;
ALTER TABLE drive.file_job
    ADD CONSTRAINT file_job_pkey PRIMARY KEY (file_id, number),
    ADD CONSTRAINT file_job_job_id_key UNIQUE (job_id);
ALTER TABLE drive.file_job_end
    ADD CONSTRAINT file_job_end_job_id_fkey
        FOREIGN KEY (job_id) REFERENCES drive.file_job (job_id) ON DELETE CASCADE;
ALTER TABLE drive.file_job_cancel
    ADD CONSTRAINT file_job_cancel_job_id_fkey
        FOREIGN KEY (job_id) REFERENCES drive.file_job (job_id) ON DELETE CASCADE;
ALTER TABLE drive.file_job_plan
    ADD CONSTRAINT file_job_plan_job_id_fkey
        FOREIGN KEY (job_id) REFERENCES drive.file_job (job_id) ON DELETE CASCADE;
ALTER TABLE drive.file_job_step
    ADD CONSTRAINT file_job_step_job_id_fkey
        FOREIGN KEY (job_id) REFERENCES drive.file_job (job_id) ON DELETE CASCADE;

CREATE INDEX file_job_triggered_by_idx ON drive.file_job (triggered_by_id)
    WHERE triggered_by_id IS NOT NULL;

-- The processing status of every file, from its LAST job (its highest
-- number) and that job's end, if any:
--   no job, upload not confirmed          -> pending
--   no job, upload confirmed              -> ready (stored, unprocessed)
--   no end yet                            -> processing
--   end `reported_done`                   -> ready (the next step's job is
--                                            created in the transaction that
--                                            records the end, so a done last
--                                            job is the end of the chain)
--   any other end                         -> failed, with its reason
-- While the last job runs, the view also reads where its runner says it is
-- (the current plan, the latest step started) and whether a user asked to
-- cancel it. The file's drive rides along for the per-drive counts.
CREATE VIEW drive.file_status AS
SELECT f.id AS file_id,
       f.drive_id,
       CASE
           WHEN j.job_id IS NULL AND f.committed_at IS NULL THEN 'pending'
           WHEN j.job_id IS NULL THEN 'ready'
           WHEN e.kind IS NULL THEN 'processing'
           WHEN e.kind = 'reported_done' THEN 'ready'
           ELSE 'failed'
       END AS processing_state,
       CASE e.kind
           WHEN 'cancelled' THEN 'cancelled'
           WHEN 'reported_done' THEN NULL
           ELSE e.reason_code
       END AS processing_error,
       j.job_id AS last_job_id,
       j.step_index AS last_job_step,
       j.trigger AS last_job_trigger,
       j.triggered_by_id AS last_job_triggered_by_id,
       j.triggered_by_name AS last_job_triggered_by_name,
       j.created_at AS last_job_created_at,
       CASE WHEN e.kind IS NULL THEN plan.labels END AS last_job_plan,
       CASE WHEN e.kind IS NULL THEN step.plan_index END AS last_job_plan_index,
       CASE WHEN e.kind IS NULL THEN step.label END AS last_job_plan_label,
       CASE WHEN e.kind IS NULL THEN step.started_at END AS last_job_plan_at,
       CASE WHEN e.kind IS NULL THEN cancel.at END AS last_job_cancel_requested_at
FROM drive.file f
LEFT JOIN LATERAL (
    SELECT fj.job_id, fj.step_index, fj.trigger, fj.triggered_by_id, fj.triggered_by_name,
           fj.created_at
    FROM drive.file_job fj
    WHERE fj.file_id = f.id
    ORDER BY fj.number DESC
    LIMIT 1
) j ON true
LEFT JOIN drive.file_job_end e ON e.job_id = j.job_id
LEFT JOIN LATERAL (
    SELECT p.labels
    FROM drive.file_job_plan p
    WHERE p.job_id = j.job_id
    ORDER BY p.number DESC
    LIMIT 1
) plan ON true
LEFT JOIN LATERAL (
    SELECT s.plan_index, s.label, s.started_at
    FROM drive.file_job_step s
    WHERE s.job_id = j.job_id
    ORDER BY s.started_at DESC, s.plan_index DESC
    LIMIT 1
) step ON true
LEFT JOIN LATERAL (
    SELECT c.at
    FROM drive.file_job_cancel c
    WHERE c.job_id = j.job_id
    ORDER BY c.number
    LIMIT 1
) cancel ON true;
